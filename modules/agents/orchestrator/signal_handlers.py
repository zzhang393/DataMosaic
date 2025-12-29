"""信号处理器模块"""
import asyncio
import time
import os
from typing import List, Optional, Any
import logging

from .context import Doc2DBContext, Doc2DBProcessingState
from ...memory import TableSnapshot, Violation, Fix
from ...signals.core import SignalType
from ...signals.handlers import BaseSignalHandler
from .utils import DocumentUtils


class OrchestratorSignalHandler(BaseSignalHandler):
    """协调器信号处理器"""
    
    def __init__(self, orchestrator):
        super().__init__('unified_orchestrator')
        self.orchestrator = orchestrator
        self.logger = logging.getLogger('orchestrator.signal_handler')
        
        self.add_supported_signal(SignalType.EXTRACTION_COMPLETE)
        self.add_supported_signal(SignalType.VERIFICATION_COMPLETE)
        self.add_supported_signal(SignalType.FIXING_COMPLETE)
        self.add_supported_signal(SignalType.EXTRACTION_ERROR)
        self.add_supported_signal(SignalType.VERIFICATION_ERROR)
        self.add_supported_signal(SignalType.FIXING_ERROR)
        self.add_supported_signal(SignalType.WARM_START_REQUEST)
        
    async def _process_signal(self, signal) -> Optional[Any]:
        """处理协调器相关信号"""
        if signal.signal_type == SignalType.EXTRACTION_COMPLETE:
            return await self.orchestrator.handle_extraction_complete(signal)
        elif signal.signal_type == SignalType.VERIFICATION_COMPLETE:
            return await self.orchestrator.handle_verification_complete(signal)
        elif signal.signal_type == SignalType.FIXING_COMPLETE:
            return await self.orchestrator.handle_fixing_complete(signal)
        elif signal.signal_type == SignalType.WARM_START_REQUEST:
            return await self.orchestrator.handle_warm_start_request(signal)
        elif signal.signal_type in [SignalType.EXTRACTION_ERROR, SignalType.VERIFICATION_ERROR, SignalType.FIXING_ERROR]:
            return await self.orchestrator.handle_component_error(signal)
        
        return None


class SignalHandlerMixin:
    """信号处理器混入类，提供信号处理方法"""
    
    async def handle_extraction_complete(self, signal):
        """处理提取完成信号（支持片段处理和batch处理）"""
        data = signal.data
        table_name = data.get('table_name')
        snapshots = data.get('snapshots', [])
        context_data = data.get('context', {})
        segment_info = data.get('segment_info')
        batch_info = data.get('batch_info')
        if self.current_context:
            signal_run_id = context_data.get('run_id') or signal.correlation_id.split('_')[0] if hasattr(signal, 'correlation_id') else None
            current_run_id = self.current_context.run_id
            current_state = getattr(self.current_context, 'current_state', None)
            
            if signal_run_id and signal_run_id != current_run_id:
                self.logger.warning(f'⛔ 拒绝不匹配的信号：信号run_id={signal_run_id}, 当前run_id={current_run_id}')
                return
            
            if current_state and str(current_state).endswith('COMPLETED'):
                self.logger.warning(f'⛔ 任务已完成（状态：{current_state}），忽略来自表 {table_name} 的提取完成信号')
                return
        
        self.logger.info(f' 收到提取完成信号 - 表: {table_name}, batch_info: {batch_info is not None}, segment_info: {segment_info is not None}')
        
        is_warm_start = context_data.get('warm_start', False) or (hasattr(self.current_context, 'warm_start_attempted') and table_name in self.current_context.warm_start_attempted)
        
        if is_warm_start and self.current_context and hasattr(self.current_context, 'step_outputs'):
            from ...core.io import IOManager
            for step in self.current_context.step_outputs:
                if step.get('step') == f'warm_start_extraction_{table_name}' and step.get('status') == 'in_progress':
                    step['status'] = 'completed'
                    step['description'] = f'Warm Start 完成: 重新提取了 {len(snapshots[0].rows) if snapshots and len(snapshots) > 0 else 0} 行数据'
                    step['details']['rows_extracted'] = len(snapshots[0].rows) if snapshots and len(snapshots) > 0 else 0
                    step['timestamp_completed'] = IOManager.get_timestamp()
                    self.logger.info(f' 已更新 warm start 步骤状态为完成')
                    break
            
            self.logger.info(f' Warm start提取完成: {table_name}, 等待验证完成...')
        
        if self.current_context and hasattr(self.current_context, 'warm_start_in_progress') and table_name in self.current_context.warm_start_in_progress:
            self.current_context.warm_start_in_progress.remove(table_name)
        
        if batch_info:
            await self._handle_batch_extraction_complete(
                table_name, snapshots, batch_info, context_data
            )
            return
        
        if segment_info:
            await self._handle_segment_extraction_complete(
                table_name, snapshots, segment_info, context_data
            )
            return
        if self.current_context and snapshots:
            for snapshot in snapshots:
                self.logger.info(f'📥 收到提取完成信号 - 表: {table_name}, 新数据: {len(snapshot.rows)}行')
                self.logger.info(f' 当前context.all_snapshots中的表: {list(self.current_context.all_snapshots.keys())}')
                
                if table_name in self.current_context.all_snapshots:
                    existing_snapshot = self.current_context.all_snapshots[table_name]
                    self.logger.info(f' 表 {table_name} 已存在snapshot ({len(existing_snapshot.rows)}行)，新数据 ({len(snapshot.rows)}行)')
                    
                    existing_desc = getattr(existing_snapshot, 'stage_description', '')
                    is_batch_merged = '分batch提取完成' in existing_desc or '分片段提取完成' in existing_desc
                    
                    if not is_batch_merged and hasattr(existing_snapshot, 'is_batch_merged'):
                        is_batch_merged = existing_snapshot.is_batch_merged
                    if not is_batch_merged and hasattr(self.current_context, 'batch_merged_tables') and table_name in self.current_context.batch_merged_tables:
                        is_batch_merged = True
                    
                    is_warm_start = context_data.get('warm_start', False) or context_data.get('is_warm_start', False) if context_data else False
                    
                    if is_batch_merged:
                        self.logger.warning(f'⛔ 检测到batch/segment合并结果，跳过覆盖！保持现有 {len(existing_snapshot.rows)} 行数据')
                        
                        if is_warm_start and len(snapshot.rows) > 0:
                            self.logger.info(f' Warm start模式：执行cell级别修复合并')
                            existing_snapshot = self._merge_warm_start_cells(existing_snapshot, snapshot, table_name)
                        
                        snapshot = existing_snapshot
                    else:
                        is_warm_start = context_data.get('warm_start', False) if context_data else False
                        warm_start_failed = False
                        
                        if is_warm_start:
                            if len(snapshot.rows) == 0:
                                self.logger.warning(f' Warm start重提取返回空数据，保留原有数据并直接标记表为完成')
                                snapshot = existing_snapshot
                                warm_start_failed = True
                            elif len(snapshot.rows) < len(existing_snapshot.rows) * 0.5:
                                self.logger.warning(f' Warm start重提取数据量显著减少 ({len(snapshot.rows)} < {len(existing_snapshot.rows)}*0.5)，保留原有数据并直接标记表为完成')
                                snapshot = existing_snapshot
                                warm_start_failed = True
                            else:
                                self.logger.info(f' Warm start重提取成功，使用新数据 ({len(snapshot.rows)}行)')
                            
                            if warm_start_failed:
                                self.logger.info(f'🏁 Warm start失败，直接标记表 {table_name} 为完成')
                                self._set_table_force_completed(table_name)
                                
                                if hasattr(self.current_context, 'warm_start_tracking') and table_name in self.current_context.warm_start_tracking:
                                    self.current_context.warm_start_tracking.remove(table_name)
                                    self.logger.info(f' 已从warm start追踪中移除: {table_name}')
                        else:
                            if hasattr(existing_snapshot, 'rows') and hasattr(snapshot, 'rows'):
                                existing_snapshot.rows.extend(snapshot.rows)
                                snapshot = existing_snapshot
                
                self.current_context.all_snapshots[table_name] = snapshot
                if '分batch提取完成' not in getattr(snapshot, 'stage_description', '') and '分片段提取完成' not in getattr(snapshot, 'stage_description', ''):
                    snapshot.processing_stage = 'extraction'
                    snapshot.stage_description = f'数据提取完成 - 表 {table_name}，共 {len(snapshot.rows)} 行数据'
                
                self.current_context.io_manager.append_snapshot(snapshot)
                
                self._sync_current_snapshot_to_context()
        
        self.logger.info(f' [提取完成] 开始基础验证: {table_name}')
        basic_violations_triggered_fix = await self._auto_basic_verification(table_name, snapshots, context_data)
        self.logger.info(f' [提取完成] 基础验证返回: triggered_fix={basic_violations_triggered_fix}')
        
        if not basic_violations_triggered_fix:
            self.logger.info(f' [提取完成] 未触发修复，开始完整验证: {table_name}')
            await self._signal_verify_data(table_name, snapshots, context_data)
        else:
            self.logger.info(f'⏳ [提取完成] 已触发修复，等待修复完成后自动验证: {table_name}')
    
    async def _handle_batch_extraction_complete(self, table_name: str, snapshots: List, 
                                                batch_info: dict, context_data: dict):
        """处理 batch 提取完成"""
        batch_index = batch_info.get('batch_index')
        total_batches = batch_info.get('total_batches')
        is_warm_start = batch_info.get('is_warm_start', False)
        
        warm_start_label = " (warm start)" if is_warm_start else ""
        self.logger.info(f' 表 {table_name} - Batch {batch_index}/{total_batches}{warm_start_label} 提取完成')
        
        if hasattr(self, 'coordinator'):
            self.coordinator.mark_batch_completed(table_name, batch_index, is_warm_start=is_warm_start)
        
        if not hasattr(self.current_context, 'batch_tracking'):
            self.current_context.batch_tracking = {}
        
        if table_name not in self.current_context.batch_tracking:
            self.logger.warning(f'表 {table_name} 没有 batch 追踪信息，初始化中...')
            self.current_context.batch_tracking[table_name] = {
                'total_batches': total_batches,
                'completed_batches': 0,
                'snapshots': []
            }
        
        tracking = self.current_context.batch_tracking[table_name]
        
        if snapshots:
            tracking['snapshots'].extend(snapshots)
            tracking['completed_batches'] += 1
            
            self.logger.debug(
                f'Batch收集进度：{tracking["completed_batches"]}/{tracking["total_batches"]} '
                f'(已收集 {len(tracking["snapshots"])} 个 snapshots)'
            )
        
        if snapshots:
            self.logger.info(f'开始对 Batch {batch_index} 的结果进行验证')
            
            context_data['is_batch_verification'] = True
            context_data['batch_index'] = batch_index
            context_data['batch_total'] = total_batches
            context_data['is_warm_start'] = is_warm_start  #  传递 warm start 标志
            
            basic_violations_triggered_fix = await self._auto_basic_verification(
                table_name, snapshots, context_data
            )
            
            if not basic_violations_triggered_fix:
                await self._signal_verify_data(table_name, snapshots, context_data)
        
        if tracking['completed_batches'] >= tracking['total_batches']:
            self.logger.info(f'🎉 表 {table_name} - 所有 {total_batches} 个 batch 处理完成，开始合并...')
            
            merged_snapshot = self._merge_batch_snapshots(table_name, tracking['snapshots'])
            
            if merged_snapshot:
                self.current_context.all_snapshots[table_name] = merged_snapshot
                merged_snapshot.processing_stage = 'extraction'
                merged_snapshot.stage_description = (
                    f'分batch提取完成 - 表 {table_name}，'
                    f'{total_batches} 个batch，共 {len(merged_snapshot.rows)} 行数据'
                )
                
                merged_snapshot.is_batch_merged = True
                merged_snapshot.merged_row_count = len(merged_snapshot.rows)
                
                if not hasattr(self.current_context, 'batch_merged_tables'):
                    self.current_context.batch_merged_tables = {}
                self.current_context.batch_merged_tables[table_name] = len(merged_snapshot.rows)
                self.logger.info(f'🔒 标记表 {table_name} 为batch合并结果，共 {len(merged_snapshot.rows)} 行，防止被覆盖')
                
                self.current_context.io_manager.append_snapshot(merged_snapshot)
                self._sync_current_snapshot_to_context()
                
                del self.current_context.batch_tracking[table_name]
                
                if hasattr(self, 'coordinator') and self.coordinator:
                    from .signal_coordinator import TableLifecycleState
                    self.coordinator.update_table_state(table_name, TableLifecycleState.EXTRACTED)
                
                self.logger.info(f' 表 {table_name} 所有 batch 已合并完成')
                
                if total_batches == 1:
                    self.logger.info(f' 表 {table_name} 只有1个batch，已在单个batch时验证过，跳过合并后的验证')
                else:
                    if 'is_batch_verification' in context_data:
                        del context_data['is_batch_verification']
                    
                    basic_violations_triggered_fix = await self._auto_basic_verification(
                        table_name, [merged_snapshot], context_data
                    )
                    
                    if not basic_violations_triggered_fix:
                        await self._signal_verify_data(table_name, [merged_snapshot], context_data)
            else:
                self.logger.error(f'表 {table_name} batch 合并失败')
    
    def _merge_batch_snapshots(self, table_name: str, snapshots: List) -> Optional[Any]:
        """合并多个 batch 的 snapshots
        
        策略：智能去重
        - 只删除完全相同的行
        - 或删除是其他行子集的行
        """
        if not snapshots:
            return None
        
        if len(snapshots) == 1:
            return snapshots[0]
        
        try:
            from ...memory import TableSnapshot
            from ...core.io import IOManager
            
            base_snapshot = snapshots[0]
            
            all_rows = []
            for snapshot in snapshots:
                if hasattr(snapshot, 'rows') and snapshot.rows:
                    all_rows.extend(snapshot.rows)
            
            if not all_rows:
                return base_snapshot
            
            unique_rows = []
            removed_count = 0
            
            for i, row1 in enumerate(all_rows):
                should_keep = True
                
                for j, row2 in enumerate(all_rows):
                    if i == j:
                        continue
                    
                    relation = self._compare_rows(row1, row2)
                    
                    if relation == 'identical':
                        if j < i:
                            should_keep = False
                            self.logger.debug(f' 去重: 行 {i} 与行 {j} 完全相同，删除行 {i}')
                            removed_count += 1
                            break
                    elif relation == 'row1_subset':
                        should_keep = False
                        self.logger.debug(f' 去重: 行 {i} 是行 {j} 的子集，删除行 {i}')
                        removed_count += 1
                        break
                
                if should_keep:
                    unique_rows.append(row1)
            
            if removed_count > 0:
                self.logger.info(f' Batch合并时去重了 {removed_count} 行数据（完全相同或子集关系）')
            
            merged_snapshot = TableSnapshot(
                run_id=base_snapshot.run_id if hasattr(base_snapshot, 'run_id') else None,
                table=table_name,
                rows=unique_rows,
                created_at=IOManager.get_timestamp(),
                table_id=base_snapshot.table_id if hasattr(base_snapshot, 'table_id') else table_name
            )
            
            self.logger.info(
                f' Batch合并完成：{len(snapshots)} 个batch → {len(unique_rows)} 行数据（去重后）'
            )
            
            return merged_snapshot
            
        except Exception as e:
            self.logger.error(f'合并 batch snapshots 失败: {e}', exc_info=True)
            return None
    
    def _compare_rows(self, row1, row2) -> str:
        """比较两行数据的关系
        
        Returns:
            'identical': 完全相同
            'row1_subset': row1是row2的子集
            'row2_subset': row2是row1的子集
            'different': 不同
        """
        if not hasattr(row1, 'cells') or not hasattr(row2, 'cells'):
            return 'different'
        
        cells1 = row1.cells if isinstance(row1.cells, dict) else {}
        cells2 = row2.cells if isinstance(row2.cells, dict) else {}
        
        values1 = {}
        values2 = {}
        
        for field, cell in cells1.items():
            value = cell.value if hasattr(cell, 'value') else cell
            if value not in [None, '', 'null', 'NULL']:
                values1[field] = str(value).strip()
        
        for field, cell in cells2.items():
            value = cell.value if hasattr(cell, 'value') else cell
            if value not in [None, '', 'null', 'NULL']:
                values2[field] = str(value).strip()
        
        if values1 == values2:
            return 'identical'
        
        if values1 and values2:
            if all(field in values2 and values1[field] == values2[field] for field in values1):
                if len(values1) < len(values2):
                    return 'row1_subset'
                elif len(values1) == len(values2):
                    return 'identical'
            
            if all(field in values1 and values2[field] == values1[field] for field in values2):
                if len(values2) < len(values1):
                    return 'row2_subset'
        
        return 'different'
    
    async def _handle_segment_extraction_complete(self, table_name: str, snapshots: List, 
                                                  segment_info: dict, context_data: dict):
        """处理片段提取完成（收集并合并）"""
        segment_index = segment_info.get('segment_index')
        total_segments = segment_info.get('total_segments')
        
        self.logger.info(f' 表 {table_name} - 片段 {segment_index}/{total_segments} 提取完成')
        
        if not hasattr(self.current_context, 'segment_tracking'):
            self.current_context.segment_tracking = {}
        
        if table_name not in self.current_context.segment_tracking:
            self.logger.warning(f'表 {table_name} 没有片段追踪信息，初始化中...')
            self.current_context.segment_tracking[table_name] = {
                'total_segments': total_segments,
                'completed_segments': 0,
                'snapshots': []
            }
        
        tracking = self.current_context.segment_tracking[table_name]
        
        if snapshots:
            tracking['snapshots'].extend(snapshots)
            tracking['completed_segments'] += 1
            
            self.logger.debug(
                f'片段收集进度：{tracking["completed_segments"]}/{tracking["total_segments"]} '
                f'(已收集 {len(tracking["snapshots"])} 个 snapshots)'
            )
        
        if tracking['completed_segments'] >= tracking['total_segments']:
            self.logger.info(f'🎉 表 {table_name} - 所有 {total_segments} 个片段提取完成，开始合并...')
            
            merged_snapshot = self._merge_segment_snapshots(table_name, tracking['snapshots'])
            
            if merged_snapshot:
                self.current_context.all_snapshots[table_name] = merged_snapshot
                merged_snapshot.processing_stage = 'extraction'
                merged_snapshot.stage_description = (
                    f'分片段提取完成 - 表 {table_name}，'
                    f'{total_segments} 个片段，共 {len(merged_snapshot.rows)} 行数据'
                )
                self.current_context.io_manager.append_snapshot(merged_snapshot)
                self._sync_current_snapshot_to_context()
                
                del self.current_context.segment_tracking[table_name]
                
                self.logger.info(f'开始对合并后的表 {table_name} 进行验证')
                
                basic_violations_triggered_fix = await self._auto_basic_verification(
                    table_name, [merged_snapshot], context_data
                )
                
                if not basic_violations_triggered_fix:
                    await self._signal_verify_data(table_name, [merged_snapshot], context_data)
            else:
                self.logger.error(f'表 {table_name} 片段合并失败')
    
    def _merge_warm_start_cells(self, existing_snapshot, warm_start_snapshot, table_name: str):
        """
        Warm start的cell级别合并：用warm start修复的cell更新batch合并结果
        
        Args:
            existing_snapshot: batch合并的完整快照
            warm_start_snapshot: warm start重新提取的部分快照
            table_name: 表名
            
        Returns:
            合并后的snapshot
        """
        try:
            warm_start_rows = {row.tuple_id: row for row in warm_start_snapshot.rows}
            
            updated_cells_count = 0
            
            for existing_row in existing_snapshot.rows:
                if existing_row.tuple_id in warm_start_rows:
                    warm_row = warm_start_rows[existing_row.tuple_id]
                    
                    for attr_name, warm_cell in warm_row.cells.items():
                        if attr_name in existing_row.cells:
                            existing_cell = existing_row.cells[attr_name]
                            warm_value = warm_cell.value if hasattr(warm_cell, 'value') else None
                            existing_value = existing_cell.value if hasattr(existing_cell, 'value') else None
                            
                            if warm_value != existing_value:
                                if hasattr(existing_cell, 'value'):
                                    existing_cell.value = warm_value
                                
                                if hasattr(warm_cell, 'evidences') and hasattr(existing_cell, 'evidences'):
                                    warm_evidences = warm_cell.evidences if isinstance(warm_cell.evidences, list) else [warm_cell.evidences]
                                    existing_evidences = existing_cell.evidences if isinstance(existing_cell.evidences, list) else [existing_cell.evidences]
                                    
                                    warm_evidences_marked = [f"[Warm Start Fix] {e}" if not e.startswith("[Warm Start Fix]") else e for e in warm_evidences]
                                    
                                    all_evidences = existing_evidences + warm_evidences_marked
                                    existing_cell.evidences = list(set(all_evidences))
                                
                                updated_cells_count += 1
            
            self.logger.info(f' Warm start cell级别合并完成: {table_name}, 更新了 {updated_cells_count} 个cell')
            return existing_snapshot
            
        except Exception as e:
            self.logger.error(f'Warm start cell合并失败: {e}', exc_info=True)
            return existing_snapshot
    
    def _merge_segment_snapshots(self, table_name: str, snapshots: List) -> Optional[Any]:
        """合并多个片段的 snapshots
        
        策略：简单拼接所有 snapshot 的 rows，verifier 会负责去重和一致性检查
        
        Returns:
            合并后的 TableSnapshot，如果失败返回 None
        """
        if not snapshots:
            return None
        
        if len(snapshots) == 1:
            return snapshots[0]
        
        try:
            from ...memory import TableSnapshot
            from ...core.io import IOManager
            
            base_snapshot = snapshots[0]
            
            all_rows = []
            for snapshot in snapshots:
                if hasattr(snapshot, 'rows') and snapshot.rows:
                    all_rows.extend(snapshot.rows)
            
            merged_snapshot = TableSnapshot(
                run_id=base_snapshot.run_id if hasattr(base_snapshot, 'run_id') else None,
                table=table_name,
                rows=all_rows,
                created_at=IOManager.get_timestamp(),
                table_id=base_snapshot.table_id if hasattr(base_snapshot, 'table_id') else table_name
            )
            
            self.logger.info(
                f' 合并完成：{len(snapshots)} 个片段 → {len(all_rows)} 行数据'
            )
            
            return merged_snapshot
            
        except Exception as e:
            self.logger.error(f'合并片段 snapshots 失败: {e}', exc_info=True)
            return None
    
    async def handle_verification_complete(self, signal):
        """处理验证完成信号"""
        data = signal.data
        table_name = data.get('table_name')
        violations = data.get('violations', [])
        
        if self.current_context:
            current_state = getattr(self.current_context, 'current_state', None)
            if current_state and str(current_state).endswith('COMPLETED'):
                self.logger.warning(f'⛔ 任务已完成（状态：{current_state}），忽略来自表 {table_name} 的验证完成信号')
                return
        
        context_data = data.get('context', {})
        batch_index = context_data.get('batch_index')
        if hasattr(self, 'coordinator') and self.coordinator:
            tracker = self.coordinator.get_table_tracker(table_name)
            if tracker:
                current_round = tracker.verification_round
                max_rounds = tracker.max_verification_rounds
                if current_round >= max_rounds:
                    self.logger.warning(
                        f'🛑 强制停止：表 {table_name} 已完成 {current_round} 轮验证，'
                        f'达到最大轮次 ({max_rounds})'
                    )
                    self._mark_final(table_name, batch_index)
                    return

        
        if hasattr(self, 'coordinator') and self.coordinator:
            from .signal_coordinator import TableLifecycleState
            if self.coordinator.is_table_in_state(table_name, TableLifecycleState.FINAL):
                self.logger.warning(f'⛔ 表 {table_name} 已标记为FINAL，忽略验证完成信号')
                
                if hasattr(self.current_context, 'warm_start_attempted') and table_name in self.current_context.warm_start_attempted:
                    if hasattr(self.current_context, 'warm_start_tracking') and table_name in self.current_context.warm_start_tracking:
                        self.current_context.warm_start_tracking.remove(table_name)
                        self.logger.info(f' 已从 warm_start_tracking 中移除 FINAL 表: {table_name}')
                
                context_data = data.get('context', {})
                batch_index = context_data.get('batch_index')
                if batch_index is not None:
                    tracker = self.coordinator.get_table_tracker(table_name)
                    if tracker:
                        is_warm_start = context_data.get('is_warm_start_batch', False)
                        batch_key = self.coordinator._get_batch_key(batch_index, is_warm_start)
                        
                        if batch_key in tracker.batch_trackers:
                            batch_tracker = tracker.batch_trackers[batch_key]
                            if batch_tracker.state != 'final':
                                self.logger.info(f' 强制标记 Batch {batch_index} (warm_start={is_warm_start}) 为 final，因为表已经是 FINAL 状态')
                                self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start)
                
                return
        
        snapshot = data.get('snapshot')
        
        is_warm_start_verification = (
            hasattr(self.current_context, 'warm_start_attempted') and 
            table_name in self.current_context.warm_start_attempted
        )
        
        if is_warm_start_verification:
            error_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'error']
            
            if hasattr(self.current_context, 'warm_start_tracking') and table_name in self.current_context.warm_start_tracking:
                self.current_context.warm_start_tracking.remove(table_name)
                if len(error_violations) == 0:
                    self.logger.info(
                        f' Warm start验证完成且无error: {table_name}, 已从追踪中移除, '
                        f'剩余: {list(self.current_context.warm_start_tracking) if self.current_context.warm_start_tracking else []}'
                    )
                else:
                    self.logger.info(
                        f' Warm start验证完成但仍有 {len(error_violations)} 个error, '
                        f'已从追踪中移除（避免阻塞）, 剩余: {list(self.current_context.warm_start_tracking) if self.current_context.warm_start_tracking else []}'
                    )
        
        is_batch_verification = context_data.get('is_batch_verification', False)
        
        if hasattr(self.current_context, 'force_format_check_tables') and table_name in self.current_context.force_format_check_tables:
        if hasattr(self.current_context, 'force_format_check_tables') and table_name in self.current_context.force_format_check_tables:
            format_violations = [v for v in violations if v.constraint_type == 'FORMAT']
            if not format_violations:
                self.current_context.force_format_check_tables.remove(table_name)
            else:
                self.current_context.force_format_check_tables.remove(table_name)
        
        if self.current_context:
            if table_name not in self.current_context.all_violations:
                self.current_context.all_violations[table_name] = []
            self.current_context.all_violations[table_name].extend(violations)
            
            if snapshot:
                if hasattr(self.current_context, 'batch_merged_tables') and table_name in self.current_context.batch_merged_tables:
                    merged_row_count = self.current_context.batch_merged_tables[table_name]
                    
                    if len(snapshot.rows) < merged_row_count:
                        self.logger.warning(
                            f'检测到验证后snapshot ({len(snapshot.rows)}行) 少于batch合并结果 ({merged_row_count}行)，'
                            f'这可能是单个batch的验证，拒绝覆盖合并结果。'
                        )
                        
                        if table_name in self.current_context.all_snapshots:
                            existing_snapshot = self.current_context.all_snapshots[table_name]
                            snapshot = existing_snapshot
                            self.logger.info(f'已保持batch合并结果的完整性，继续使用 {len(snapshot.rows)} 行数据')
                
                snapshot.processing_stage = 'verification'
                
                if is_batch_verification:
                    snapshot.stage_description = f'Batch验证完成 - 表 {table_name}，发现 {len(violations)} 个质量问题'
                else:
                    should_update_all_snapshots = True
                    
                    if hasattr(self.current_context, 'batch_merged_tables') and table_name in self.current_context.batch_merged_tables:
                        merged_row_count = self.current_context.batch_merged_tables[table_name]
                        if len(snapshot.rows) < merged_row_count:
                            should_update_all_snapshots = False
                            self.logger.warning(
                                f' 跳过更新all_snapshots：验证后snapshot ({len(snapshot.rows)}行) < batch合并结果 ({merged_row_count}行)，保护batch合并数据'
                            )
                    
                    if should_update_all_snapshots:
                        snapshot.stage_description = f'数据验证完成 - 表 {table_name}，发现 {len(violations)} 个质量问题'
                        self.current_context.all_snapshots[table_name] = snapshot
                    
                self.current_context.io_manager.append_snapshot(snapshot)
                
                self._sync_current_snapshot_to_context()
        
        error_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'error']
        warn_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'warn']
        
        
        
        if not violations:
            self.logger.info(f' 表 {table_name} 验证完成且无违规，标记为完成')
            
            if is_batch_verification and batch_index is not None:
                if hasattr(self, 'coordinator'):
                    is_warm_start = context_data.get('warm_start', False) or context_data.get('is_warm_start', False)
                    self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start=is_warm_start)
                    self.logger.info(f' Batch {batch_index} 无违规，已标记为final{" (warm start)" if is_warm_start else ""}')
                return  # batch完成，返回
            
            if hasattr(self, 'coordinator'):
                from .signal_coordinator import TableLifecycleState
                self.coordinator.update_table_state(table_name, TableLifecycleState.FINAL)
            self._set_table_force_completed(table_name)
            return
        
        total_fixes = len(self.current_context.all_fixes.get(table_name, [])) if self.current_context else 0
        error_count = len([v for v in violations if getattr(v, 'severity', 'warn') == 'error'])
        warn_count = len([v for v in violations if getattr(v, 'severity', 'warn') == 'warn'])
        
        if error_count == 0 and warn_count > 0:
            if total_fixes >= 1:  #  已经修复过，可以完成
                if is_batch_verification and batch_index is not None:
                    if hasattr(self, 'coordinator'):
                        is_warm_start = context_data.get('warm_start', False) or context_data.get('is_warm_start', False)
                        self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start=is_warm_start)
                        self.logger.info(f' Batch {batch_index} 只有warning且已修复，已标记为final{" (warm start)" if is_warm_start else ""}')
                    return  # batch完成，返回
                self._set_table_force_completed(table_name)
                return
        
        try:
            await asyncio.wait_for(
                self._detect_and_mark_repeated_violations(violations, table_name),
                timeout=10.0  # 10秒超时
            )
        except asyncio.TimeoutError:
            self.logger.warning(f' 检测重复违规超时（表 {table_name}），跳过此步骤')
        except Exception as e:
            self.logger.error(f' 检测重复违规失败（表 {table_name}）: {e}')
        
        try:
            filtered_violations = await asyncio.wait_for(
                self._filter_unfixable_violations(violations, table_name),
                timeout=10.0  # 10秒超时
            )
        except asyncio.TimeoutError:
            self.logger.warning(f' 过滤无法修复的违规超时（表 {table_name}），使用原始违规列表')
            filtered_violations = violations
        except Exception as e:
            self.logger.error(f' 过滤无法修复的违规失败（表 {table_name}）: {e}')
            filtered_violations = violations
        if len(filtered_violations) < len(violations):
            removed_count = len(violations) - len(filtered_violations)
            self.logger.info(f"过滤了 {removed_count} 个无法修复的违规")
        
        violations = filtered_violations
        
        if not violations:
            self.logger.info(f' 过滤后无违规，准备标记完成 - 表: {table_name}, is_batch_verification={is_batch_verification}, batch_index={batch_index}')
            if is_batch_verification and batch_index is not None:
                self.logger.info(f'  → 这是batch验证，将调用mark_batch_final')
                if hasattr(self, 'coordinator'):
                    is_warm_start = context_data.get('warm_start', False) or context_data.get('is_warm_start', False)
                    self.logger.info(f'  → 参数: table_name={table_name}, batch_index={batch_index}, is_warm_start={is_warm_start}')
                    self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start=is_warm_start)
                    self.logger.info(f' Batch {batch_index} 过滤后无违规，已标记为final{" (warm start)" if is_warm_start else ""}')
                return  # batch完成，返回
            else:
                self.logger.info(f'  → 这是表级验证（is_batch_verification={is_batch_verification}, batch_index={batch_index}），将调用_set_table_force_completed')
            self._set_table_force_completed(table_name)
            return
        
        if hasattr(self.current_context, 'coordinator'):
            coordinator = self.current_context.coordinator
        else:
            self.logger.warning(' Context 没有 coordinator，无法进行循环控制检查')
            coordinator = None
        
        tool_fixable = self._get_tool_fixable_violations(violations)
        requires_reextraction = self._get_reextraction_violations(violations)
        
        if coordinator:
            tracker = coordinator.get_table_tracker(table_name)
            current_round = tracker.verification_round if tracker else 0
            max_rounds = tracker.max_verification_rounds if tracker else 2
            self.logger.info(
                f' 表 {table_name} 验证完成分析: '
                f'验证轮次={current_round}/{max_rounds}, '
                f'工具修复={len(tool_fixable)}, '
                f'需要重提取={len(requires_reextraction)}, '
                f'总违规={len(violations)}'
            )
        else:
            self.logger.info(
                f'违规分类: 工具修复={len(tool_fixable)}, 需要重提取={len(requires_reextraction)}'
            )
        
        if tool_fixable:
            check_batch_index = batch_index if is_batch_verification else None
            
            self.logger.info(
                f' 循环控制检查: 表={table_name}, '
                f'is_batch_verification={is_batch_verification}, '
                f'batch_index={batch_index}, '
                f'check_batch_index={check_batch_index}'
            )
            
            if coordinator and not coordinator.can_verify_fix_iterate(table_name, check_batch_index):
                self.logger.warning(
                    f' 达到最大验证-修复轮次，停止修复 - 表 {table_name}, '
                    f'剩余 {len(tool_fixable)} 个可修复违规将被忽略'
                )
                
                if requires_reextraction:
                    self.logger.info(
                        f' 虽然达到最大修复轮次，但仍有 {len(requires_reextraction)} 个需要重提取的违规，'
                        f'尝试触发 Warm Start'
                    )
                    
                    if coordinator and coordinator.can_warm_start(table_name, check_batch_index):
                        coordinator.increment_warm_start_attempts(table_name, check_batch_index)
                        
                        if is_batch_verification and batch_index is not None:
                            self._mark_final(table_name, batch_index)
                        
                        if is_batch_verification:
                            batch_document_names = context_data.get('batch_document_names', [])
                            text_contents = context_data.get('text_contents', [])
                            batch_total = context_data.get('batch_total', 1)
                            
                            if batch_document_names and text_contents:
                                self.logger.info(
                                    f' Batch {batch_index}/{batch_total} warm start: 使用当前batch的文档 {batch_document_names}'
                                )
                                await self._trigger_batch_warm_start_extraction(
                                    table_name, requires_reextraction, snapshot, 
                                    text_contents, batch_document_names, batch_index, batch_total
                                )
                            else:
                                self.logger.warning(f' Batch {batch_index} 缺少文档信息，跳过warm start')
                                self._mark_final(table_name, batch_index)
                        else:
                            await self._trigger_smart_warm_start_extraction(
                                table_name, requires_reextraction, snapshot
                            )
                        return
                    else:
                        self.logger.warning(
                            f' 无法触发 Warm Start（已达最大尝试次数或其他限制）- 表 {table_name}'
                        )
                
                self._mark_final(table_name, batch_index)
                return
            
            self.logger.info(f' 准备修复 {len(tool_fixable)} 个违规...')
            
            if coordinator:
                coordinator.increment_verify_fix_iteration(table_name, check_batch_index)
            
            await self._signal_fix_data(table_name, tool_fixable, snapshot, context_data)
            return  # 修复完成后会自动触发重新验证
        
        if requires_reextraction:
            self.logger.info(f' 检测到 {len(requires_reextraction)} 个需要重提取的违规')
            
            check_batch_index = batch_index if is_batch_verification else None
            
            if coordinator and not coordinator.can_warm_start(table_name, check_batch_index):
                self.logger.warning(
                    f' 达到最大 Warm Start 尝试次数，停止重提取 - 表 {table_name}'
                )
                self._mark_final(table_name, batch_index)
                return
            
            if coordinator:
                coordinator.increment_warm_start_attempts(table_name, check_batch_index)
            
            if is_batch_verification and batch_index is not None:
                self._mark_final(table_name, batch_index)
            
            if is_batch_verification:
                batch_document_names = context_data.get('batch_document_names', [])
                text_contents = context_data.get('text_contents', [])
                batch_total = context_data.get('batch_total', 1)
                
                if batch_document_names and text_contents:
                    self.logger.info(
                        f' Batch {batch_index}/{batch_total} warm start: 使用当前batch的文档 {batch_document_names}'
                    )
                    await self._trigger_batch_warm_start_extraction(
                        table_name, requires_reextraction, snapshot, 
                        text_contents, batch_document_names, batch_index, batch_total
                    )
                else:
                    self.logger.warning(f' Batch {batch_index} 缺少文档信息，跳过warm start')
                    self._mark_final(table_name, batch_index)
            else:
                await self._trigger_smart_warm_start_extraction(
                    table_name, requires_reextraction, snapshot
                )
            return
        
        self.logger.info(
            f' 无可处理的violations，标记完成 - 表 {table_name}, '
            f'剩余 {len(violations)} 个违规（无法修复或需要人工处理）'
        )
        self._mark_final(table_name, batch_index)
    
    async def handle_fixing_complete(self, signal):
        """处理修复完成信号"""
        data = signal.data
        table_name = data.get('table_name')
        fixes = data.get('fixes', [])
        
        if self.current_context:
            current_state = getattr(self.current_context, 'current_state', None)
            if current_state and str(current_state).endswith('COMPLETED'):
                self.logger.warning(f'⛔ 任务已完成（状态：{current_state}），忽略来自表 {table_name} 的修复完成信号')
                return
        
        if hasattr(self, 'coordinator') and self.coordinator:
            from .signal_coordinator import TableLifecycleState
            if self.coordinator.is_table_in_state(table_name, TableLifecycleState.FINAL):
                self.logger.warning(f'⛔ 表 {table_name} 已标记为FINAL，忽略修复完成信号')
                return
        
        snapshot = data.get('snapshot')
        
        context_data = data.get('context', {})
        is_batch_verification = context_data.get('is_batch_verification', False)
        batch_index = context_data.get('batch_index')
        
        
        if self.current_context and fixes:
            fix_details = {
                'total_fixes': len(fixes),
                'fixes_by_type': {},
                'fixes_summary': []
            }
            
            for fix in fixes:
                fix_type = getattr(fix, 'fix_type', 'unknown')
                if fix_type not in fix_details['fixes_by_type']:
                    fix_details['fixes_by_type'][fix_type] = 0
                fix_details['fixes_by_type'][fix_type] += 1
                
                fix_details['fixes_summary'].append({
                    'id': getattr(fix, 'id', 'unknown'),
                    'tuple_id': getattr(fix, 'tuple_id', 'unknown'),
                    'attr': getattr(fix, 'attr', 'unknown'),
                    'old': getattr(fix, 'old', 'null'),
                    'new': getattr(fix, 'new', 'null'),
                    'fix_type': fix_type
                })
            
            fix_step = {
                'step': f'fixer_{table_name}_completed',
                'step_name': f'fixer_{table_name}_completed',
                'description': f'表 {table_name} 数据修复完成，应用了 {len(fixes)} 个修复',
                'status': 'completed',
                'timestamp': self.current_context.io_manager.get_timestamp(),
                'details': fix_details
            }
            
            self.current_context.step_outputs.append(fix_step)
        
        if self.current_context:
            if table_name not in self.current_context.all_fixes:
                self.current_context.all_fixes[table_name] = []
            self.current_context.all_fixes[table_name].extend(fixes)
            
            if len(fixes) == 0:
                self.logger.warning(f' 修复失败（0个修复），保留原始数据并设置表 {table_name} 强制完成')
                if snapshot and table_name in self.current_context.all_snapshots:
                    self.logger.info(f' 保留表 {table_name} 的原始数据 ({len(self.current_context.all_snapshots[table_name].rows)}行)')
                
                if is_batch_verification and batch_index is not None:
                    if hasattr(self, 'coordinator'):
                        is_warm_start = context_data.get('warm_start', False) or context_data.get('is_warm_start', False)
                        self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start=is_warm_start)
                        self.logger.info(f' Batch {batch_index} 修复失败，已标记为final{" (warm start)" if is_warm_start else ""}')
                    return  # batch完成，返回
                    
                self._set_table_force_completed(table_name)
                return
            
            if snapshot:
                should_update_snapshot = True
                
                if hasattr(self.current_context, 'batch_merged_tables') and table_name in self.current_context.batch_merged_tables:
                    merged_row_count = self.current_context.batch_merged_tables[table_name]
                    
                    if len(snapshot.rows) < merged_row_count:
                        self.logger.warning(
                            f' 检测到修复后snapshot ({len(snapshot.rows)}行) 少于batch合并结果 ({merged_row_count}行)！'
                            f'将应用修复到完整的batch合并数据上，而不是覆盖。'
                        )
                        
                        if table_name in self.current_context.all_snapshots:
                            existing_snapshot = self.current_context.all_snapshots[table_name]
                            
                            snapshot_dict = {getattr(row, 'tuple_id', None): row for row in snapshot.rows if hasattr(row, 'tuple_id')}
                            
                            updated_count = 0
                            for i, row in enumerate(existing_snapshot.rows):
                                tuple_id = getattr(row, 'tuple_id', None)
                                if tuple_id and tuple_id in snapshot_dict:
                                    existing_snapshot.rows[i] = snapshot_dict[tuple_id]
                                    updated_count += 1
                                    self.logger.debug(f'  更新row: {tuple_id}')
                            
                            snapshot = existing_snapshot
                            self.logger.info(
                                f' 已将 {updated_count} 个修复应用到batch合并结果上，'
                                f'保持完整性，最终 {len(snapshot.rows)} 行数据'
                            )
                            should_update_snapshot = True  # 需要更新，因为我们修改了existing_snapshot
                
                if should_update_snapshot:
                    self.current_context.all_snapshots[table_name] = snapshot
                snapshot.processing_stage = 'fixing'
                snapshot.stage_description = f'数据修复完成 - 表 {table_name}，应用了 {len(fixes)} 个修复'
                self.current_context.io_manager.append_snapshot(snapshot)
                
                self._sync_current_snapshot_to_context()
                
                if self._should_skip_reverification(table_name, len(fixes)):
                    self.logger.info(f' 跳过重新验证，表 {table_name} 已标记完成')
                    
                    if is_batch_verification and batch_index is not None:
                        if hasattr(self, 'coordinator'):
                            is_warm_start = context_data.get('warm_start', False) or context_data.get('is_warm_start', False)
                            self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start=is_warm_start)
                            self.logger.info(f' Batch {batch_index} 修复完成且无需重新验证，已标记为final{" (warm start)" if is_warm_start else ""}')
                        return  # batch完成，返回
                    
                    if hasattr(self, 'coordinator') and self.coordinator:
                        from .signal_coordinator import TableLifecycleState
                        self.coordinator.update_table_state(table_name, TableLifecycleState.FINAL)
                    self._set_table_force_completed(table_name)
                else:
                    check_batch_index = batch_index if is_batch_verification else None
                    
                    if hasattr(self, 'coordinator') and self.coordinator:
                        if not self.coordinator.can_verify_fix_iterate(table_name, check_batch_index):
                            self.logger.warning(
                                f' 修复完成，但已达到最大验证-修复轮次，停止重新验证 - 表 {table_name}'
                            )
                            if is_batch_verification and batch_index is not None:
                                is_warm_start = context_data.get('warm_start', False) or context_data.get('is_warm_start', False)
                                self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start=is_warm_start)
                                self.logger.info(f' Batch {batch_index} 已达最大轮次，标记为final{" (warm start)" if is_warm_start else ""}')
                            else:
                                from .signal_coordinator import TableLifecycleState
                                self.coordinator.update_table_state(table_name, TableLifecycleState.FINAL)
                                self._set_table_force_completed(table_name)
                            return  # 不再触发重新验证
                    
                    self.logger.info(f' 触发重新验证，表 {table_name}，已应用 {len(fixes)} 个修复')
                    await self._signal_verify_data(table_name, [snapshot], context_data)
            else:
                self.logger.warning(f' 修复完成但snapshot为空，设置表 {table_name} 强制完成')
                
                if is_batch_verification and batch_index is not None:
                    if hasattr(self, 'coordinator'):
                        is_warm_start = context_data.get('warm_start', False) or context_data.get('is_warm_start', False)
                        self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start=is_warm_start)
                        self.logger.info(f' Batch {batch_index} snapshot为空，已标记为final{" (warm start)" if is_warm_start else ""}')
                    return  # batch完成，返回
                    
                self._set_table_force_completed(table_name)
    
    def _sync_current_snapshot_to_context(self):
        """实时同步当前快照数据到context.snapshots，确保服务层能立即读取"""
        if not self.current_context or not self.current_context.all_snapshots:
            return
            
        self.current_context.snapshots = list(self.current_context.all_snapshots.values())
    
    async def handle_component_error(self, signal):
        """处理组件错误信号"""
        data = signal.data
        error = data.get('error')
        table_name = data.get('table_name', 'unknown')
        
        
        if hasattr(self, 'logger'):
            self.logger.error(f"组件错误 - 表 {table_name}: {error}")
        
    
    async def _trigger_smart_warm_start_extraction(self, table_name: str, violations_requiring_reextraction, snapshot):
        """触发 Warm Start 重提取 - 通过信号系统发送请求给 Orchestrator
        
        新架构：不再直接调用 orchestrator 方法，而是发送 WARM_START_REQUEST 信号
        """
        self.logger.info(f" 发送 Warm Start 请求: {table_name}, {len(violations_requiring_reextraction)} 个违规")
        
        if self.current_context and hasattr(self.current_context, 'step_outputs'):
            from ...core.io import IOManager
            warm_start_step = {
                'step': f'warm_start_extraction_{table_name}',
                'step_name': f'warm_start_extraction_{table_name}',
                'status': 'in_progress',
                'description': f'Warm Start: 检测到 {len(violations_requiring_reextraction)} 个需要重新提取的违规',
                'details': {
                    'table_name': table_name,
                    'violations_count': len(violations_requiring_reextraction),
                    'violation_types': list(set([v.constraint_type for v in violations_requiring_reextraction])),
                    'mode': 'warm_start_extraction'
                },
                'timestamp': IOManager.get_timestamp()
            }
            self.current_context.step_outputs.append(warm_start_step)
        
        try:
            from ...signals.core import SignalType
            
            signal_data = {
                'table_name': table_name,
                'violations': violations_requiring_reextraction,
                'snapshot': snapshot,
                'run_id': self.current_context.run_id if self.current_context else None,
                'schema': self.current_context.schema if self.current_context else None
            }
            
            await self.broadcaster.emit_simple_signal(
                SignalType.WARM_START_REQUEST,
                data=signal_data,
                correlation_id=f"{signal_data['run_id']}_{table_name}_warmstart"
            )
            
            self.logger.info(f" Warm Start 请求已发送，等待 Orchestrator 处理")
            
        except Exception as e:
            self.logger.error(f"发送 Warm Start 请求失败: {e}", exc_info=True)
            
            if self.current_context and hasattr(self.current_context, 'step_outputs'):
                from ...core.io import IOManager
                for step in self.current_context.step_outputs:
                    if step.get('step') == f'warm_start_extraction_{table_name}' and step.get('status') == 'in_progress':
                        step['status'] = 'failed'
                        step['description'] = f'Warm Start 请求失败: {str(e)}'
                        step['details']['error'] = str(e)
                        step['timestamp_completed'] = IOManager.get_timestamp()
                        break
    
    def _should_skip_reverification(self, table_name: str, fixes_applied: int) -> bool:
        """判断是否应该跳过重新验证以避免循环
        
        简化逻辑：
        1. 如果没有应用任何修复，跳过
        2. 如果是基础修复，不跳过（需要完整验证）
        3. 否则，总是触发重新验证（让 coordinator 控制循环次数）
        """
        
        if fixes_applied == 0:
            self.logger.info(f"跳过重新验证：未应用任何修复")
            return True
        
        if hasattr(self.current_context, 'basic_fix_tables') and table_name in self.current_context.basic_fix_tables:
            self.current_context.basic_fix_tables.remove(table_name)
            self.logger.info(f"不跳过重新验证：基础修复完成，需要完整验证")
            return False
        
        self.logger.info(f"不跳过重新验证：已应用 {fixes_applied} 个修复")
        return False
    
    async def _detect_and_mark_repeated_violations(self, violations: List, table_name: str):
        """
        检测并标记重复违规：单元格已经被修复过但violation还在，说明修复失败
        """
        if not violations:
            return
        
        try:
            if not hasattr(self, 'memory_manager') or not self.memory_manager:
                return
            
            run_id = getattr(self.current_context, 'run_id', None) if hasattr(self, 'current_context') else None
            if not run_id:
                return
            
            total_fixes = len(self.current_context.all_fixes.get(table_name, [])) if self.current_context else 0
            if total_fixes == 0:
                return
            
            repeated_violations = await self.memory_manager.check_repeated_violations(
                violations, run_id, table_name
            )
            
            if repeated_violations:
                pass  # Auto-fixed empty block
                
                for violation in repeated_violations:
                    try:
                        await self.memory_manager.mark_violation_unfixable(violation.id, run_id)
                        self.logger.info(f"标记重复违规为unfixable: {violation.id}")
                    except Exception as mark_error:
                        self.logger.error(f"标记违规 {violation.id} 为unfixable失败: {mark_error}")
                
        except Exception as e:
            self.logger.error(f"检测重复违规失败: {e}")
    
    async def _filter_unfixable_violations(self, violations: List, table_name: str) -> List:
        """过滤掉已标记为无法修复的违规"""
        if not violations:
            return violations
        
        try:
            if not hasattr(self, 'memory_manager') or not self.memory_manager:
                return violations
            
            run_id = getattr(self.current_context, 'run_id', None) if hasattr(self, 'current_context') else None
            if not run_id:
                return violations
            
            unfixable_ids = await self.memory_manager.get_unfixable_violations(run_id)
            
            if unfixable_ids:
                for vid in unfixable_ids:
                    pass  # Auto-fixed empty block
            else:
                pass  # Auto-fixed empty block
            
            filtered = [v for v in violations if v.id not in unfixable_ids]
            
            removed_count = len(violations) - len(filtered)
            if removed_count > 0:
                for v in violations:
                    if v.id in unfixable_ids:
                        pass  # Auto-fixed empty block
            
            return filtered
            
        except Exception as e:
            self.logger.error(f"过滤unfixable violations失败: {e}")
            import traceback
            traceback.print_exc()
            return violations
    
    def _extract_relevant_documents(self, violations: List, snapshot) -> List[str]:
        """从violations中提取相关文档（增强版：支持多种匹配策略）
        
        策略优先级：
        1. 优先使用 located_segments 缓存信息（最准确）
        2. 从 evidences 中提取文档名（模糊匹配）
        3. 如果都没有，返回空列表（回退到使用所有文档）
        
        Args:
            violations: 违规列表
            snapshot: 表快照
            
        Returns:
            相关文档名称列表（如 ['2024-CIRTRAN CORP-j.txt', ...]）
        """
        if not violations or not snapshot or not hasattr(snapshot, 'rows'):
            self.logger.debug('violations或snapshot为空，返回空文档列表')
            return []
        
        relevant_docs = set()
        violation_tuple_ids = set([v.tuple_id for v in violations])
        
        if hasattr(self, 'current_context') and self.current_context:
            if hasattr(self.current_context, 'located_segments') and self.current_context.located_segments:
                segment_docs = set([
                    seg.source_document for seg in self.current_context.located_segments 
                    if hasattr(seg, 'source_document') and seg.source_document
                ])
                if segment_docs:
                    relevant_docs.update(segment_docs)
                    self.logger.info(f' 策略1：从located_segments缓存中提取到 {len(segment_docs)} 个文档：{segment_docs}')
        
        evidence_based_docs = set()
        for row in snapshot.rows:
            if row.tuple_id in violation_tuple_ids:
                for cell_name, cell_data in row.cells.items():
                    if hasattr(cell_data, 'evidences') and cell_data.evidences:
                        for evidence in cell_data.evidences:
                            import os
                            doc_name = os.path.basename(evidence) if evidence else None
                            
                            if doc_name and ('.' in doc_name or doc_name.endswith('-j.txt')):
                                evidence_based_docs.add(doc_name)
        
        if evidence_based_docs:
            relevant_docs.update(evidence_based_docs)
            self.logger.info(f' 策略2：从evidences中提取到 {len(evidence_based_docs)} 个文档：{evidence_based_docs}')
        
        if not relevant_docs and hasattr(self, 'current_context') and self.current_context:
            if hasattr(self.current_context, 'documents') and self.current_context.documents:
                all_evidences = set()
                for row in snapshot.rows:
                    if row.tuple_id in violation_tuple_ids:
                        for cell_name, cell_data in row.cells.items():
                            if hasattr(cell_data, 'evidences') and cell_data.evidences:
                                all_evidences.update(cell_data.evidences)
                
                import os
                actual_docs = [os.path.basename(d) for d in self.current_context.documents]
                for evidence in all_evidences:
                    for actual_doc in actual_docs:
                        if evidence.lower() in actual_doc.lower() or actual_doc.lower() in evidence.lower():
                            relevant_docs.add(actual_doc)
                
                if relevant_docs:
                    self.logger.info(f' 策略3：模糊匹配提取到 {len(relevant_docs)} 个文档：{relevant_docs}')
        
        result = list(relevant_docs)
        if not result:
            self.logger.warning(f' 未能提取到相关文档，将回退到使用所有文档')
        else:
            self.logger.info(f' 最终识别到 {len(result)} 个相关文档：{result}')
        
        return result
    
    def _get_tool_fixable_violations(self, violations: List[Violation]) -> List[Violation]:
        """提取可工具修复的违规
        
        可工具修复的违规特征：
        - processing_category 包含 'tool_fixable' 或 'fixable'
        - 或者约束类型在可修复列表中（FORMAT, TYPE, VALUE等）
        """
        tool_fixable = []
        for v in violations:
            category = getattr(v, 'processing_category', None)
            if category:
                category_str = category.value if hasattr(category, 'value') else str(category)
                if 'tool_fixable' in category_str.lower() or 'fixable' in category_str.lower():
                    tool_fixable.append(v)
                    continue
            
            constraint_type = getattr(v, 'constraint_type', '').upper()
            if constraint_type in ['FORMAT', 'TYPE', 'VALUE', 'LOGIC', 'AGGREGATION']:
                tool_fixable.append(v)
        
        return tool_fixable
    
    def _get_reextraction_violations(self, violations: List[Violation]) -> List[Violation]:
        """提取需要重提取的违规
        
        需要重提取的违规特征：
        - processing_category 包含 'reextraction' 或 'reextract'
        - 或者是空值、缺失值等无法工具修复的问题
        """
        reextraction = []
        for v in violations:
            category = getattr(v, 'processing_category', None)
            if category:
                category_str = category.value if hasattr(category, 'value') else str(category)
                if 'reextract' in category_str.lower():
                    reextraction.append(v)
        
        return reextraction
    
    def _mark_final(self, table_name: str, batch_index: Optional[int] = None):
        """统一的标记完成方法
        
        Args:
            table_name: 表名
            batch_index: batch索引（None 表示表级）
        """
        if batch_index is not None:
            if hasattr(self, 'coordinator') and self.coordinator:
                is_warm_start = context_data.get('is_warm_start', False) if context_data else False
                self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start)
                self.logger.info(f' Batch {batch_index} 标记为 final')
        else:
            self._set_table_force_completed(table_name)
    
    def _set_table_force_completed(self, table_name: str):
        """设置表强制完成标记"""
        if not hasattr(self.current_context, 'force_completed_tables'):
            self.current_context.force_completed_tables = set()
        
        self.current_context.force_completed_tables.add(table_name)
        
        if hasattr(self, 'coordinator') and self.coordinator:
            from .signal_coordinator import TableLifecycleState
            self.coordinator.update_table_state(table_name, TableLifecycleState.FINAL)
            self.logger.info(f' 表 {table_name} 标记为FINAL')
            
            tracker = self.coordinator.get_table_tracker(table_name)
            if tracker and hasattr(tracker, 'batch_trackers') and tracker.batch_trackers:
                self.logger.info(f'   开始标记表 {table_name} 的所有batch为final，总共 {len(tracker.batch_trackers)} 个batch')
                for batch_key, batch_tracker in tracker.batch_trackers.items():
                    if batch_tracker.state != 'final':
                        batch_index = batch_tracker.batch_index
                        is_warm_start = batch_tracker.is_warm_start
                        self.logger.info(f'   标记 Batch {batch_index} (warm_start={is_warm_start}, 当前状态={batch_tracker.state})')
                        self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start)
                        self.logger.info(f'   Batch {batch_index} 已标记为final')
                    else:
                        self.logger.info(f'    Batch {batch_tracker.batch_index} 已经是final状态，跳过')
                self.logger.info(f'   表 {table_name} 的所有batch已标记为final')
            else:
                if hasattr(self.current_context, 'batch_tracking') and table_name in self.current_context.batch_tracking:
                    total_batches = self.current_context.batch_tracking[table_name].get('total_batches', 1)
                    self.logger.info(f'   未找到batch_trackers，但发现batch_tracking信息: total_batches={total_batches}')
                    if total_batches == 1:
                        try:
                            self.coordinator.mark_batch_final(table_name, 0, is_warm_start=False)
                            self.logger.info(f'   单batch模式：Batch 0 标记为final')
                        except Exception as e:
                            self.logger.warning(f'   标记batch 0为final失败: {e}')
                    else:
                        self.logger.warning(f'   多batch模式但没有batch_trackers，可能存在状态不一致！')
    
    def _is_table_force_completed(self, table_name: str) -> bool:
        """检查表是否已被强制完成"""
        if not hasattr(self.current_context, 'force_completed_tables'):
            return False
        
        is_completed = table_name in self.current_context.force_completed_tables
        if is_completed:
            pass  # Auto-fixed empty block
        return is_completed
    
    async def _auto_basic_verification(self, table_name: str, snapshots: List[TableSnapshot], context_data: dict) -> bool:
        """
        自动基础验证 - 就像编译器的语法检查
        
        由Orchestrator控制，每次提取后强制执行Format和Type验证，
        确保数据质量的基础保证
        
        Returns:
            bool: 是否触发了修复流程
        """
        if not snapshots:
            return False
        
        
        try:
            if not hasattr(self, '_basic_verifiers') or self._basic_verifiers is None:
                from ..mcp import FormatMCP, TypeMCP
                self._basic_verifiers = {
                    'format': FormatMCP(),
                    'type': TypeMCP()
                }
                self.logger.info("初始化基础验证器（Format + Type）")
            
            all_violations = []
            
            for snapshot in snapshots:
                format_violations = self._basic_verifiers['format'].verify(
                    snapshot=snapshot,
                    schema=context_data.get('schema'),
                    table_name=table_name
                )
                all_violations.extend(format_violations)
                
                type_violations = self._basic_verifiers['type'].verify(
                    snapshot=snapshot,
                    schema=context_data.get('schema'),
                    table_name=table_name
                )
                all_violations.extend(type_violations)
            
            if self.current_context:
                run_id = context_data.get('run_id')
                
                if all_violations:
                    if table_name not in self.current_context.all_violations:
                        self.current_context.all_violations[table_name] = []
                    self.current_context.all_violations[table_name].extend(all_violations)
                
                if self.memory_manager and run_id and all_violations:
                    try:
                        await self.memory_manager.store_violations(all_violations, run_id, table_name)
                    except Exception as store_error:
                        self.logger.warning(f"存储自动验证结果到memory失败: {store_error}")
            
            error_count = len([v for v in all_violations if getattr(v, 'severity', 'warn') == 'error'])
            warn_count = len([v for v in all_violations if getattr(v, 'severity', 'warn') == 'warn'])
            
            self.logger.info(
                f' [基础验证] 完成 - 表: {table_name}, '
                f'FORMAT违规: {len([v for v in all_violations if v.constraint_type == "FORMAT"])}, '
                f'TYPE违规: {len([v for v in all_violations if v.constraint_type == "TYPE"])}, '
                f'总计: {len(all_violations)} (错误: {error_count}, 警告: {warn_count})'
            )
            
            if all_violations:
                self.logger.info(f' [基础验证] 发现 {len(all_violations)} 个违规，开始分类...')
                
                violations_requiring_reextraction = []
                violations_for_tool_fixing = []
                
                for violation in all_violations:
                    processing_category = getattr(violation, 'processing_category', None)
                    if not processing_category:
                        from ..verifier.mcp_verifier import ViolationCategory
                        if violation.constraint_type in ['FORMAT', 'TYPE']:
                            processing_category = ViolationCategory.TOOL_FIXABLE
                            violation.processing_category = processing_category
                        else:
                            if violation.severity == 'error':
                                processing_category = ViolationCategory.REQUIRES_REEXTRACTION
                                violation.processing_category = processing_category
                            else:
                                processing_category = ViolationCategory.TOOL_FIXABLE
                                violation.processing_category = processing_category
                    
                    try:
                        from ..verifier.mcp_verifier import ViolationCategory
                        if processing_category == ViolationCategory.TOOL_FIXABLE:
                            violations_for_tool_fixing.append(violation)
                        else:
                            violations_requiring_reextraction.append(violation)
                    except:
                        if getattr(violation, 'severity', 'warn') == 'error':
                            violations_requiring_reextraction.append(violation)
                        else:
                            violations_for_tool_fixing.append(violation)
                
                reextraction_in_tool_fixable = [
                    v for v in violations_for_tool_fixing 
                    if getattr(v, 'processing_category', '') == 'requires_reextraction'
                ]
                
                actual_tool_fixable = [
                    v for v in violations_for_tool_fixing 
                    if v not in reextraction_in_tool_fixable
                ]
                
                if reextraction_in_tool_fixable:
                    violations_requiring_reextraction.extend(reextraction_in_tool_fixable)
                
                self.logger.info(
                    f' [基础验证] 违规分类完成: '
                    f'可工具修复={len(actual_tool_fixable)}, '
                    f'需要重提取={len(violations_requiring_reextraction)}'
                )
                
                if not hasattr(self.current_context, 'basic_fix_attempts'):
                    self.current_context.basic_fix_attempts = {}
                
                if table_name not in self.current_context.basic_fix_attempts:
                    self.current_context.basic_fix_attempts[table_name] = 0
                
                if self.current_context.basic_fix_attempts[table_name] >= 1:
                    self.logger.warning(f' 表 {table_name} 基础修复已尝试 {self.current_context.basic_fix_attempts[table_name]} 次，停止重试')
                    return False
                
                if actual_tool_fixable:
                    self.logger.info(f' [基础验证] 准备触发修复流程: {len(actual_tool_fixable)} 个违规')
                    if table_name in self.current_context.all_snapshots:
                        snapshot = self.current_context.all_snapshots[table_name]
                        
                        if not hasattr(self.current_context, 'basic_fix_tables'):
                            self.current_context.basic_fix_tables = set()
                        self.current_context.basic_fix_tables.add(table_name)
                        
                        self.current_context.basic_fix_attempts[table_name] += 1
                        self.logger.info(f' 触发基础修复（第 {self.current_context.basic_fix_attempts[table_name]} 次）: {table_name}，{len(actual_tool_fixable)} 个违规')
                        
                        try:
                            await asyncio.wait_for(
                                self._signal_fix_data(table_name, actual_tool_fixable, snapshot, context_data),
                                timeout=60.0  # 60秒超时
                            )
                            return True  # 表示触发了修复
                        except asyncio.TimeoutError:
                            self.logger.error(f' 基础修复超时: {table_name}，将继续完整验证')
                            return False  # 超时，返回False触发完整验证
                        except Exception as fix_error:
                            self.logger.error(f' 基础修复失败: {table_name}，错误: {fix_error}')
                            import traceback
                            traceback.print_exc()
                            return False  # 失败，返回False触发完整验证
                    else:
                        self.logger.warning(f' [基础验证] 表 {table_name} 不在 all_snapshots 中，无法修复')
                        self.logger.info(f' [基础验证] 将进行完整验证以建立快照')
                        return False  # 无法修复，返回False触发完整验证
                else:
                    self.logger.info(f'ℹ [基础验证] 没有可工具修复的违规')
                    return False  # 没有可修复的，返回False触发完整验证
                
            self.logger.info(f' [基础验证] 无违规，返回 False')
            return False
            
        except Exception as e:
            self.logger.error(f"自动基础验证失败: {e}")
            import traceback
            traceback.print_exc()
            return False


class TableProcessingWaiter:
    """表处理等待器，负责等待单个表的处理完成"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('table_processing_waiter')
        self.orchestrator = None  # 将在外部设置
    
    async def wait_for_table_processing_complete(self, context: Doc2DBContext, table_name: str):
        """等待单个表的处理完成（支持多轮验证-修复循环）"""
        max_wait_time = 100  # 恢复更长的等待时间
        wait_interval = 1.0  # 检查间隔（秒）
        waited_time = 0
        max_fix_attempts = 1  #  最大修复尝试次数
        max_iterations = 2  # 最大等待迭代次数（比coordinator的限制多一些）
        iteration_count = 0
        
        
        extraction_done = False
        verification_started = False  # 是否开始验证
        last_total_count = -1  # 跟踪总违规数量变化
        fix_attempt_count = 0  # 跟踪修复尝试次数
        last_violation_snapshot = None  # 记录上次违规快照，用于检测修复效果
        
        while waited_time < max_wait_time:
            if hasattr(context, 'force_completed_tables') and table_name in context.force_completed_tables:
                return True
            
            if not extraction_done and table_name in context.all_snapshots:
                if hasattr(context, 'warm_start_in_progress') and table_name in context.warm_start_in_progress:
                    pass  # Auto-fixed empty block
                else:
                    extraction_done = True
            
            if extraction_done:
                if table_name in context.all_violations:
                    verification_started = True
                    violations = context.all_violations[table_name]
                    current_total_count = len(violations)
                    error_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'error']
                    warn_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'warn']
                    
                    
                    if len(error_violations) > 0:
                        reextraction_violations = [
                            v for v in error_violations 
                            if getattr(v, 'processing_category', '') == 'requires_reextraction'
                        ]
                        
                        for i, v in enumerate(error_violations[:5]):  # 只显示前5个
                            pc = getattr(v, 'processing_category', 'None')
                        
                        if reextraction_violations:
                            if not hasattr(context, 'warm_start_attempted'):
                                context.warm_start_attempted = set()
                            
                            if table_name not in context.warm_start_attempted:
                                
                                context.warm_start_attempted.add(table_name)
                                fix_attempt_count += 1  # 标记已尝试
                                
                                if table_name in context.all_snapshots:
                                    old_snapshot = context.all_snapshots[table_name]
                                    if not hasattr(context, 'warm_start_in_progress'):
                                        context.warm_start_in_progress = set()
                                    context.warm_start_in_progress.add(table_name)
                                
                                extraction_done = False
                                verification_started = False  # 重置验证状态
                                
                                await self._trigger_warm_start_extraction(context, table_name)
                                waited_time = 0
                                last_total_count = -1  # 重置违规计数
                                continue  # 重新开始等待循环
                            else:
                                if not extraction_done:
                                    pass
                                else:
                                    return True
                    
                    violations_signature = f"{len(error_violations)}-{len(warn_violations)}"
                    progress_made = (last_total_count != current_total_count) or (last_total_count == -1)
                    
                    if progress_made:
                        last_total_count = current_total_count
                        iteration_count += 1
                        
                        if current_total_count == 0:
                            return True
                        
                        if len(error_violations) > 0:
                            if fix_attempt_count < max_fix_attempts:
                                fix_attempt_count += 1
                                waited_time = 0  # 重置等待时间
                            else:
                                if not hasattr(context, 'warm_start_attempted'):
                                    context.warm_start_attempted = set()
                                if table_name not in context.warm_start_attempted:
                                    context.warm_start_attempted.add(table_name)
                                    fix_attempt_count = 0
                                    await self._trigger_warm_start_extraction(context, table_name)
                                    waited_time = 0
                                else:
                                    return True
                        elif len(warn_violations) > 0:
                            total_fixes = len(context.all_fixes.get(table_name, []))
                            
                            min_wait_for_mcp = 3.0  # 至少等待3秒给MCP验证机会
                            
                            should_complete = (
                                (total_fixes >= 1 and waited_time >= min_wait_for_mcp) or  # 有修复且等待了足够时间
                                waited_time >= 15 or  # 等待时间超过15秒
                                fix_attempt_count >= 2  # 已经尝试修复过2次
                            )
                            
                            if should_complete:
                                return True
                            elif fix_attempt_count < max_fix_attempts:
                                fix_attempt_count += 1
                                waited_time = 0
                            else:
                                return True
                    else:
                        if waited_time > 30:
                            if len(error_violations) == 0:
                                return True
                            else:
                                return True
                        
                elif extraction_done and waited_time > 10:
                    if not verification_started and waited_time > 20:
                        return True
            elif extraction_done and waited_time > 30:
                return True
                    
            await asyncio.sleep(wait_interval)
            waited_time += wait_interval
        
        final_total_violations = 0
        final_error_count = 0
        final_warn_count = 0
        if table_name in context.all_violations:
            violations = context.all_violations[table_name]
            error_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'error']
            warn_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'warn']
            final_total_violations = len(violations)
            final_error_count = len(error_violations)
            final_warn_count = len(warn_violations)
        
        if iteration_count >= max_iterations:
            self.logger.warning(
                f' 表 {table_name} 已达到最大迭代次数 ({max_iterations})，'
                f'最终状态: {final_total_violations} 个违规 '
                f'(Error: {final_error_count}, Warning: {final_warn_count})，'
                f'强制终止处理'
            )
            if hasattr(context, 'force_completed_tables'):
                context.force_completed_tables.add(table_name)
            else:
                context.force_completed_tables = {table_name}
        else:
            self.logger.info(
                f' 表 {table_name} 处理完成或超时，'
                f'迭代次数: {iteration_count}/{max_iterations}，'
                f'最终违规: {final_total_violations} 个'
            )
        
        
        return True
    
    async def _trigger_warm_start_extraction(self, context: Doc2DBContext, table_name: str):
        """触发 warm start 重新提取（带有violations信息）
        
        注意：调用方应该已经检查过 warm_start_attempted 标记，
        这里不再重复检查和标记，避免逻辑混乱。
        """
        
        
        violations = context.all_violations.get(table_name, [])
        snapshot = context.all_snapshots.get(table_name)
        
        if snapshot:
            pass  # Auto-fixed empty block
        
        if not violations:
            return
        
        violations_requiring_reextraction = [
            v for v in violations 
            if getattr(v, 'processing_category', '') == 'requires_reextraction'
        ]
        
        relevant_documents = self._extract_relevant_documents_in_waiter(
            violations_requiring_reextraction, snapshot, context
        )
        
        if hasattr(self, 'orchestrator') and self.orchestrator:
            pass  # Auto-fixed empty block
            
            if hasattr(self.orchestrator, '_trigger_smart_warm_start_extraction'):
                await self.orchestrator._trigger_smart_warm_start_extraction(
                    table_name, violations_requiring_reextraction, snapshot
                )
            else:
                await self._trigger_filtered_extraction(context, table_name, relevant_documents)
        else:
            pass  # Auto-fixed empty block
        
    
    def _extract_relevant_documents_in_waiter(self, violations: List, snapshot, context) -> List[str]:
        """在waiter中提取相关文档（复用SignalHandlerMixin的逻辑）"""
        if not violations or not snapshot or not hasattr(snapshot, 'rows'):
            return []
        
        relevant_docs = set()
        violation_tuple_ids = set([v.tuple_id for v in violations])
        
        
        for row in snapshot.rows:
            if row.tuple_id in violation_tuple_ids:
                for cell_name, cell_data in row.cells.items():
                    if hasattr(cell_data, 'evidences') and cell_data.evidences:
                        for evidence in cell_data.evidences:
                            import os
                            doc_name = os.path.basename(evidence)
                            if doc_name:
                                relevant_docs.add(doc_name)
        
        result = list(relevant_docs)
        return result
    
    async def _trigger_filtered_extraction(self, context, table_name: str, relevant_documents: List[str]):
        """使用过滤后的文档触发提取"""
        if not relevant_documents:
            if hasattr(self.orchestrator, '_signal_extract_data'):
                await self.orchestrator._signal_extract_data(context, table_name)
            return
        
        
        all_documents = context.documents
        relevant_text_contents = []
        
        from .utils import DocumentUtils
        document_utils = DocumentUtils()
        
        for doc_path in all_documents:
            doc_basename = os.path.basename(doc_path)
            if doc_basename in relevant_documents:
                text_content = document_utils.convert_documents_to_text([doc_path])
                if text_content:
                    relevant_text_contents.extend(text_content)
        
        if relevant_text_contents:
            pass  # 暂未实现自定义text_contents功能
        else:
            if hasattr(self.orchestrator, '_signal_extract_data'):
                await self.orchestrator._signal_extract_data(context, table_name)
