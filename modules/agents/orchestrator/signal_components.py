"""信号驱动的组件适配器"""
import asyncio
import logging
import os
import sys
from typing import Dict, List, Any, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from modules.signals.core import SignalType, Signal, SignalBroadcaster, SignalReceiver

try:
    from modules.agents.extractor.base import BaseExtractor as BaseExtractor
    from modules.agents.verifier.mcp_verifier import MCPBasedVerifier
    from modules.agents.fixer.mcp_fixer import MCPBasedFixer
    from modules.memory import TableSnapshot, Violation, Fix
    from modules.core.io import IOManager
    from datetime import datetime
except ImportError:
    from ...extractor.base import BaseExtractor as BaseExtractor
    from ...verifier.mcp_verifier import MCPBasedVerifier
    from ...fixer.mcp_fixer import MCPBasedFixer
    from ...memory import TableSnapshot, Violation, Fix
    from ...core.io import IOManager


class SignalAwareExtractor:
    """支持信号的数据提取器 - 增强支持基于memory的数据操作"""
    
    def __init__(self, extractor: BaseExtractor = None, memory_manager = None, coordinator = None):
        self.extractor = extractor or BaseExtractor()
        self.memory_manager = memory_manager
        self.coordinator = coordinator  # 信号协调器
        self.logger = logging.getLogger('signal.extractor')
        self.broadcaster = SignalBroadcaster('extractor')
    
    async def extract_with_signals(self, signal: Signal) -> Signal:
        """基于信号的提取方法 - 支持基于memory的增量处理"""
        request_data = signal.data
        context = request_data.get('context', {})
        run_id = context.get('run_id')
        table_name = context.get('table_name')
        
        is_relation_table = request_data.get('is_relation_table', False)
        entity_snapshots = request_data.get('entity_snapshots', {})
        
        self.logger.debug(f'收到提取请求 - 表: {table_name}, Run ID: {run_id}')
        if is_relation_table:
            self.logger.debug(f'关系表提取 - {len(entity_snapshots)} 个实体表')
        
        try:
            text_contents = context.get('text_contents', [])
            
            if not text_contents or all(not tc for tc in text_contents):
                self.logger.error(f'表 {table_name} 没有收到任何文档内容！无法进行提取')
            
            await self.broadcaster.emit_simple_signal(
                SignalType.EXTRACTION_START,
                data={'table_name': table_name},
                correlation_id=signal.correlation_id
            )
            
            self.logger.info(f"开始提取数据 - 表: {table_name}")
            
            previous_snapshot = None
            violations_for_reextraction = []
            warm_start = request_data.get('warm_start', False)
            
            if 'violations_requiring_reextraction' in request_data:
                violations_dict_list = request_data.get('violations_requiring_reextraction', [])
                
                from ...memory import SuggestedFix
                for v_dict in violations_dict_list:
                    suggested_fix_data = v_dict.get('suggested_fix')
                    suggested_fix = None
                    if suggested_fix_data:
                        if isinstance(suggested_fix_data, dict):
                            suggested_fix = SuggestedFix(value=suggested_fix_data.get('value', ''))
                        elif isinstance(suggested_fix_data, str):
                            suggested_fix = SuggestedFix(value=suggested_fix_data)
                    
                    violation = Violation(
                        id=v_dict.get('id'),
                        table=table_name,
                        tuple_id=v_dict.get('tuple_id'),
                        attr=v_dict.get('attr'),
                        constraint_type=v_dict.get('constraint_type'),
                        description=v_dict.get('description'),
                        severity=v_dict.get('severity', 'error'),
                        suggested_fix=suggested_fix,
                        detector_id=v_dict.get('detector_id', 'unknown'),
                        timestamp=v_dict.get('timestamp', datetime.now().isoformat())
                    )
                    violation.processing_category = 'requires_reextraction'
                    violations_for_reextraction.append(violation)
                
                if 'previous_snapshot' in request_data and request_data['previous_snapshot']:
                    snapshot_dict = request_data['previous_snapshot']
                    previous_snapshot = TableSnapshot.from_dict(snapshot_dict)
                
                if violations_for_reextraction:
                    warm_start = True
                    self.logger.info(f"启用暖启动模式(信号数据) - 表: {table_name}, 待重提取: {len(violations_for_reextraction)}")
                    
            elif self.memory_manager and run_id:
                is_batch_extraction = 'batch_info' in request_data
                
                if is_batch_extraction:
                    self.logger.debug(f"Batch级提取 - 表: {table_name}, 使用cold start（不从memory加载）")
                    warm_start = False
                else:
                    try:
                        previous_snapshot = await self.memory_manager.get_snapshot(run_id, table_name)
                        
                        all_violations = await self.memory_manager.get_violations(run_id, table_name)
                        violations_for_reextraction = [v for v in all_violations if getattr(v, 'processing_category', '') == 'requires_reextraction']
                        
                        if previous_snapshot or violations_for_reextraction:
                            warm_start = True
                            self.logger.info(f"启用暖启动模式(memory) - 表: {table_name}, 待重提取: {len(violations_for_reextraction)}")
                        
                    except Exception as memory_error:
                        self.logger.warning(f"从memory读取数据失败，使用冷启动: {memory_error}")
                        warm_start = False
            
            processing_context = context.get('processing_context')
            
            if not processing_context:
                class SimpleContext:
                    def __init__(self):
                        self.entity_snapshots = {}
                        self.entity_tables = []
                        self.relation_tables = []
                        self.documents = []
                        self.document_mode = "single"  # 默认值
                
                processing_context = SimpleContext()
            
            if is_relation_table and entity_snapshots:
                processing_context.entity_snapshots = entity_snapshots
                
                if not hasattr(processing_context, 'entity_tables') or not processing_context.entity_tables:
                    processing_context.entity_tables = list(entity_snapshots.keys())
                
                self.logger.debug(f'已设置关系表提取上下文: {len(entity_snapshots)} 个实体表')
            
            if 'document_mode' in context:
                signal_document_mode = context.get('document_mode')
                if signal_document_mode:
                    processing_context.document_mode = signal_document_mode
                    self.logger.debug(f'更新 processing_context.document_mode = {signal_document_mode}')
            
            if 'batch_document_names' in context:
                batch_document_names = context.get('batch_document_names')
                if batch_document_names:
                    processing_context.batch_document_names = batch_document_names
                    self.logger.debug(f'设置 processing_context.batch_document_names = {batch_document_names}')
            
            if 'batch_info' in request_data:
                batch_info = request_data.get('batch_info')
                if batch_info and 'batch_index' in batch_info:
                    processing_context.batch_index = batch_info['batch_index']
                    self.logger.debug(f'设置 processing_context.batch_index = {batch_info["batch_index"]}')
            
            extracted_rows = self.extractor.extract(
                text_contents=context.get('text_contents'),
                schema=context.get('schema'),
                table_name=table_name,
                nl_prompt=context.get('nl_prompt'),
                context=processing_context,
                warm_start=warm_start,
                previous_snapshot=previous_snapshot,
                violations=violations_for_reextraction,
                full_schema=context.get('full_schema')  # 包含relations定义的完整schema
            )
            
            snapshot = TableSnapshot(
                run_id=run_id,
                table=table_name,
                rows=extracted_rows,
                created_at=IOManager.get_timestamp(),
                table_id=f"{table_name}_{IOManager.get_timestamp()}"
            )
            
            if self.memory_manager and run_id:
                try:
                    await self.memory_manager.store_snapshot(snapshot)
                    self.logger.debug(f"快照已存储 - 表: {table_name}")
                except Exception as store_error:
                    self.logger.warning(f"存储快照到memory失败: {store_error}")
                
                if warm_start and hasattr(processing_context, 'warm_start_fixes') and processing_context.warm_start_fixes:
                    try:
                        fix_records = processing_context.warm_start_fixes
                        await self.memory_manager.store_fixes(fix_records, run_id, table_name)
                        self.logger.debug(f"Warm start修复已存储 - 表: {table_name}, 数量: {len(fix_records)}")
                        
                        if hasattr(processing_context, 'all_fixes'):
                            if table_name not in processing_context.all_fixes:
                                processing_context.all_fixes[table_name] = []
                            processing_context.all_fixes[table_name].extend(fix_records)
                            self.logger.info(f"Warm start修复记录已添加到context - 表: {table_name}, 修复数: {len(fix_records)}")
                    except Exception as fix_store_error:
                        self.logger.warning(f"存储warm start修复记录失败: {fix_store_error}")
            
            processing_mode = "warm start" if warm_start else "cold start"
            self.logger.info(f"提取完成 - 表: {table_name}, 行数: {len(extracted_rows)} ({processing_mode})")
            
            
            response_data = {
                'context': context,
                'snapshots': [snapshot],
                'table_name': table_name,
                'rows_count': len(extracted_rows),
                'processing_mode': processing_mode,
                'warm_start': warm_start
            }
            
            if 'segment_info' in request_data:
                response_data['segment_info'] = request_data['segment_info']
                segment_info = request_data['segment_info']
                self.logger.info(
                    f"片段提取完成 - 表: {table_name}, "
                    f"片段 {segment_info.get('segment_index')}/{segment_info.get('total_segments')}, "
                    f"行数: {len(extracted_rows)}"
                )
            
            if 'batch_info' in request_data:
                response_data['batch_info'] = request_data['batch_info']
                batch_info = request_data['batch_info']
                self.logger.info(
                    f"Batch提取完成 - 表: {table_name}, "
                    f"Batch {batch_info.get('batch_index')}/{batch_info.get('total_batches')}, "
                    f"行数: {len(extracted_rows)}"
                )
            
            response_signal = Signal(
                signal_type=SignalType.EXTRACTION_COMPLETE,
                sender='extractor',
                data=response_data,
                correlation_id=signal.correlation_id
            )
            
            if self.coordinator:
                self.coordinator.notify_response(response_signal)
            
            return response_signal
            
        except Exception as e:
            self.logger.error(f"数据提取失败: {e}")
            
            error_signal = Signal(
                signal_type=SignalType.EXTRACTION_ERROR,
                sender='extractor',
                data={
                    'error': str(e),
                    'context': context,
                    'table_name': context.get('table_name')
                },
                correlation_id=signal.correlation_id
            )
            
            if self.coordinator:
                self.coordinator.notify_response(error_signal)
            
            return error_signal


class SignalAwareVerifier:
    """支持信号的数据验证器 - 增强支持基于memory的数据操作"""
    
    def __init__(self, verifier: MCPBasedVerifier = None, memory_manager = None, orchestrator = None, coordinator = None):
        self.verifier = verifier or MCPBasedVerifier()
        self.memory_manager = memory_manager
        self.orchestrator = orchestrator  #  添加orchestrator引用以同步更新context
        self.coordinator = coordinator  # 信号协调器
        self.logger = logging.getLogger('signal.verifier')
        self.broadcaster = SignalBroadcaster('verifier')
    
    async def verify_with_signals(self, signal: Signal) -> Signal:
        """基于信号的验证方法 - 支持基于memory的数据读取和存储"""
        request_data = signal.data
        context = request_data.get('context', {})
        run_id = context.get('run_id')
        table_name = context.get('table_name')
        snapshots = request_data.get('snapshots', [])
        
        try:
            current_snapshot = None
            if self.memory_manager and run_id and table_name:
                try:
                    current_snapshot = await self.memory_manager.get_snapshot(run_id, table_name)
                    if current_snapshot:
                        self.logger.debug(f"从memory读取快照 - 表: {table_name}, 行数: {len(current_snapshot.rows)}")
                        snapshots = [current_snapshot]  # 使用memory中的最新数据
                    else:
                        self.logger.debug(f"使用信号数据 - 表: {table_name}")
                except Exception as memory_error:
                    self.logger.warning(f"从memory读取快照失败，使用信号数据: {memory_error}")
            
            await self.broadcaster.emit_simple_signal(
                SignalType.VERIFICATION_START,
                data={
                    'table_name': table_name,
                    'snapshots_count': len(snapshots),
                    'data_source': 'memory' if current_snapshot else 'signal'
                },
                correlation_id=signal.correlation_id
            )
            
            self.logger.info(f"开始验证数据 - 表: {table_name}")
            
            all_violations = []
            verified_snapshot = None
            
            processing_context = context.get('processing_context')
            
            for snapshot in snapshots:
                violations = await self.verifier._async_verify(
                    snapshot=snapshot,
                    schema=context.get('schema'),
                    table_name=table_name,
                    context=processing_context
                )
                all_violations.extend(violations)
                verified_snapshot = snapshot
            
            if self.memory_manager and run_id and table_name and all_violations:
                try:
                    await self.memory_manager.store_violations(all_violations, run_id, table_name)
                    self.logger.debug(f"违规已存储 - 表: {table_name}, 数量: {len(all_violations)}")
                except Exception as store_error:
                    self.logger.warning(f"存储违规到memory失败: {store_error}")
            
            if self.orchestrator and hasattr(self.orchestrator, 'current_context') and self.orchestrator.current_context:
                if table_name not in self.orchestrator.current_context.all_violations:
                    self.orchestrator.current_context.all_violations[table_name] = []
                self.orchestrator.current_context.all_violations[table_name].extend(all_violations)
                self.logger.debug(f"同步更新context.all_violations - 表: {table_name}, {len(all_violations)} 个违规")
            
            error_violations = [v for v in all_violations if getattr(v, 'severity', 'warn') == 'error']
            warn_violations = [v for v in all_violations if getattr(v, 'severity', 'warn') == 'warn']
            
            self.logger.info(f"验证完成 - 表: {table_name}, 问题数: {len(all_violations)} (错误: {len(error_violations)}, 警告: {len(warn_violations)})")
            
            response_signal = Signal(
                signal_type=SignalType.VERIFICATION_COMPLETE,
                sender='verifier',
                data={
                    'context': context,
                    'table_name': table_name,
                    'violations': all_violations,
                    'snapshot': verified_snapshot,
                    'violations_count': len(all_violations),
                    'error_count': len(error_violations),
                    'warning_count': len(warn_violations),
                    'data_source': 'memory' if current_snapshot else 'signal'
                },
                correlation_id=signal.correlation_id
            )
            
            if self.coordinator:
                self.coordinator.notify_response(response_signal)
            
            return response_signal
            
        except Exception as e:
            self.logger.error(f"数据验证失败: {e}")
            
            error_signal = Signal(
                signal_type=SignalType.VERIFICATION_ERROR,
                sender='verifier',
                data={
                    'error': str(e),
                    'context': context
                },
                correlation_id=signal.correlation_id
            )
            
            if self.coordinator:
                self.coordinator.notify_response(error_signal)
            
            return error_signal


class SignalAwareFixer:
    """支持信号的数据修复器 - 增强支持基于memory的数据操作"""
    
    def __init__(self, fixer: MCPBasedFixer = None, memory_manager = None, coordinator = None):
        self.fixer = fixer or MCPBasedFixer()
        self.memory_manager = memory_manager
        self.coordinator = coordinator  # 信号协调器
        self.logger = logging.getLogger('signal.fixer')
        self.broadcaster = SignalBroadcaster('fixer')
    
    async def fix_with_signals(self, signal: Signal) -> Signal:
        """基于信号的修复方法 - 支持基于memory的数据读取和存储"""
        
        request_data = signal.data
        context = request_data.get('context', {})
        run_id = context.get('run_id')
        table_name = context.get('table_name')
        violations = request_data.get('violations', [])
        snapshot = request_data.get('snapshot')
        
        
        try:
            if self.memory_manager and run_id and table_name:
                try:
                    memory_violations = await self.memory_manager.get_violations(run_id, table_name)
                    if memory_violations:
                        fixable_violations = [v for v in memory_violations if getattr(v, 'category', '') != 'requires_reextraction']
                        if fixable_violations:
                            violations = fixable_violations
                            self.logger.debug(f"从memory读取违规 - 表: {table_name}, 可修复: {len(violations)}")
                        
                    memory_snapshot = await self.memory_manager.get_snapshot(run_id, table_name)
                    if memory_snapshot:
                        snapshot = memory_snapshot
                        self.logger.debug(f"从memory读取快照 - 表: {table_name}, 行数: {len(snapshot.rows)}")
                        
                except Exception as memory_error:
                    self.logger.warning(f"从memory读取数据失败，使用信号数据: {memory_error}")
            
            if self.memory_manager and run_id and violations:
                try:
                    unfixable_ids = await self.memory_manager.get_unfixable_violations(run_id)
                    if unfixable_ids:
                        original_count = len(violations)
                        violations = [v for v in violations if v.id not in unfixable_ids]
                        filtered_count = original_count - len(violations)
                        if filtered_count > 0:
                            self.logger.info(f"🔍 [Fixer] 已过滤 {filtered_count} 个标记为unfixable的违规，剩余 {len(violations)} 个")
                except Exception as filter_error:
                    self.logger.warning(f"过滤unfixable violations失败: {filter_error}")
            
            await self.broadcaster.emit_simple_signal(
                SignalType.FIXING_START,
                data={
                    'table_name': table_name,
                    'violations_count': len(violations),
                    'data_source': 'memory' if self.memory_manager else 'signal'
                },
                correlation_id=signal.correlation_id
            )
            
            
            all_fixes = []
            
            fix_context = context.get('processing_context') or {}
            if isinstance(fix_context, dict):
                fix_context['memory_manager'] = self.memory_manager
                fix_context['run_id'] = run_id
            else:
                fix_context = {
                    'memory_manager': self.memory_manager,
                    'run_id': run_id
                }
            
            if snapshot and hasattr(snapshot, 'rows') and snapshot.rows:
                original_row_count = len(snapshot.rows)
                snapshot.rows = [
                    row for row in snapshot.rows
                    if not any(
                        (hasattr(cell, 'value') and (cell.value == "__DELETED__" or str(cell.value) == "__DELETED__"))
                        for cell in row.cells.values()
                    )
                ]
                deleted_row_count = original_row_count - len(snapshot.rows)
                if deleted_row_count > 0:
                    self.logger.info(f'🧹 [修复前清理] 清理了 {deleted_row_count} 行__DELETED__标记的数据，剩余 {len(snapshot.rows)} 行')
            
            try:
                if hasattr(self.fixer, 'fix_batch') and len(violations) > 0:
                    all_fixes = self.fixer.fix_batch(violations, snapshot, fix_context)
                    self.logger.debug(f"批量修复完成: {len(all_fixes)}")
                else:
                    for violation in violations:
                        try:
                            fixes = self.fixer.fix(
                                violation=violation,
                                snapshot=snapshot,
                                context=fix_context
                            )
                            all_fixes.extend(fixes)
                            self.logger.debug(f"违规 {getattr(violation, 'id', 'unknown')} 修复完成: {len(fixes)} 个修复")
                        except Exception as fix_error:
                            self.logger.error(f"修复违规失败: {fix_error}")
            except Exception as batch_fix_error:
                self.logger.error(f"批量修复失败，尝试逐个修复: {batch_fix_error}")
                for violation in violations:
                    try:
                        fixes = self.fixer.fix(
                            violation=violation,
                            snapshot=snapshot,
                            context=fix_context
                        )
                        all_fixes.extend(fixes)
                    except Exception as fix_error:
                        self.logger.error(f"修复违规失败: {fix_error}")
            
            updated_snapshot = None
            if all_fixes and snapshot:
                updated_snapshot = self._apply_fixes_to_snapshot(snapshot, all_fixes)
            
            if self.memory_manager and run_id and table_name:
                try:
                    if all_fixes:
                        await self.memory_manager.store_fixes(all_fixes, run_id, table_name)
                        self.logger.debug(f"修复已存储 - 表: {table_name}, 数量: {len(all_fixes)}")
                    
                    if updated_snapshot:
                        await self.memory_manager.store_snapshot(updated_snapshot)
                        self.logger.debug(f"快照已更新 - 表: {table_name}")
                    
                    if violations and len(all_fixes) == 0:
                        self.logger.warning(f"⚠️ [Fixer] 本次修复返回0个修复，共有 {len(violations)} 个违规")
                        for violation in violations:
                            try:
                                await self.memory_manager.mark_violation_unfixable(violation.id, run_id)
                                self.logger.info(f"🔍 [Fixer] 标记违规 {violation.id} 为unfixable（零修复）")
                            except Exception as mark_error:
                                self.logger.error(f"标记违规unfixable失败: {mark_error}")
                    elif violations and all_fixes:
                        fixed_violation_ids = set()
                        for fix in all_fixes:
                            if hasattr(fix, 'tuple_id'):
                                for v in violations:
                                    if v.tuple_id == fix.tuple_id and v.attr == fix.attr:
                                        fixed_violation_ids.add(v.id)
                                        break
                        
                        unfixed_violations = [v for v in violations if v.id not in fixed_violation_ids]
                        if unfixed_violations:
                            self.logger.info(f"🔍 [Fixer] 发现 {len(unfixed_violations)}/{len(violations)} 个违规未被修复")
                            for violation in unfixed_violations:
                                try:
                                    await self.memory_manager.mark_violation_unfixable(violation.id, run_id)
                                    self.logger.debug(f"标记违规 {violation.id} 为unfixable（未生成修复）")
                                except Exception as mark_error:
                                    self.logger.error(f"标记违规unfixable失败: {mark_error}")
                        
                except Exception as store_error:
                    self.logger.warning(f"存储修复结果到memory失败: {store_error}")
            
            fix_types = {}
            for fix in all_fixes:
                fix_type = getattr(fix, 'fix_type', 'unknown')
                fix_types[fix_type] = fix_types.get(fix_type, 0) + 1
            
            
            response_signal = Signal(
                signal_type=SignalType.FIXING_COMPLETE,
                sender='fixer',
                data={
                    'context': context,
                    'fixes': all_fixes,
                    'snapshot': updated_snapshot or snapshot,
                    'table_name': table_name,
                    'fixes_count': len(all_fixes),
                    'fix_types': fix_types,
                    'data_source': 'memory' if self.memory_manager else 'signal'
                },
                correlation_id=signal.correlation_id
            )
            
            if self.coordinator:
                self.coordinator.notify_response(response_signal)
            else:
                self.logger.warning(f"⚠️ [Fixer] 协调器不存在，无法通知响应完成")
            
            return response_signal
            
        except Exception as e:
            self.logger.error(f"❌ [Fixer] 数据修复失败: {e}")
            import traceback
            traceback.print_exc()
            
            self.logger.error(f"📤 [Fixer] 创建修复错误响应信号: correlation_id={signal.correlation_id}")
            error_signal = Signal(
                signal_type=SignalType.FIXING_ERROR,
                sender='fixer',
                data={
                    'error': str(e),
                    'context': context
                },
                correlation_id=signal.correlation_id
            )
            
            if self.coordinator:
                self.logger.error(f"📤 [Fixer] 通知协调器修复失败: {signal.correlation_id}")
                self.coordinator.notify_response(error_signal)
            else:
                self.logger.error(f"⚠️ [Fixer] 协调器不存在，无法通知错误响应")
            
            return error_signal
    
    def _apply_fixes_to_snapshot(self, snapshot: TableSnapshot, fixes: List[Fix]) -> TableSnapshot:
        """应用修复到快照并返回更新后的快照"""
        applied_count = 0
        
        for fix in fixes:
            for row in snapshot.rows:
                if row.tuple_id == fix.tuple_id and fix.attr in row.cells:
                    row.cells[fix.attr].value = fix.new
                    applied_count += 1
        
        self.logger.debug(f'快照修复完成，应用数: {applied_count}')
        
        original_row_count = len(snapshot.rows)
        snapshot.rows = [
            row for row in snapshot.rows
            if not any(
                (hasattr(cell, 'value') and (cell.value == "__DELETED__" or str(cell.value) == "__DELETED__"))
                for cell in row.cells.values()
            )
        ]
        deleted_row_count = original_row_count - len(snapshot.rows)
        if deleted_row_count > 0:
            self.logger.info(f'🧹 清理了 {deleted_row_count} 行标记为__DELETED__的数据')
        
        snapshot.created_at = IOManager.get_timestamp()
        snapshot.processing_stage = 'fixing'
        snapshot.stage_description = f'数据修复完成 - 应用了 {applied_count} 个修复' + (f'，清理了 {deleted_row_count} 行删除标记' if deleted_row_count > 0 else '')
        
        return snapshot
    
    def get_unfixable_violations(self, fixes):
        """代理到底层fixer的get_unfixable_violations方法"""
        return self.fixer.get_unfixable_violations(fixes)
    
    def fix(self, violation, snapshot, context=None):
        """代理到底层fixer的fix方法"""
        return self.fixer.fix(violation, snapshot, context)


class SignalComponentManager:
    """信号组件管理器 - 管理所有信号组件的生命周期"""
    
    def __init__(self, memory_manager = None, orchestrator = None, coordinator = None):
        self.logger = logging.getLogger('signal.component_manager')
        self.memory_manager = memory_manager
        self.orchestrator = orchestrator  #  添加orchestrator引用
        self.coordinator = coordinator  # 信号协调器
        
        self.extractor = SignalAwareExtractor(
            memory_manager=memory_manager,
            coordinator=coordinator
        )
        self.verifier = SignalAwareVerifier(
            memory_manager=memory_manager,
            orchestrator=orchestrator,
            coordinator=coordinator
        )
        self.fixer = SignalAwareFixer(
            memory_manager=memory_manager,
            coordinator=coordinator
        )
        
        self.components = {
            'extractor': self.extractor,
            'verifier': self.verifier,
            'fixer': self.fixer
        }
        
        self.receivers = {}
        
        self.signal_processors = {
            'extractor': {
                SignalType.EXTRACTION_REQUEST: self.extractor.extract_with_signals
            },
            'verifier': {
                SignalType.VERIFICATION_REQUEST: self.verifier.verify_with_signals
            },
            'fixer': {
                SignalType.FIXING_REQUEST: self.fixer.fix_with_signals
            }
        }
    
    def setup_receivers(self, hub):
        """设置接收器"""
        from modules.signals.core import SignalReceiver
        
        for component_name, component in self.components.items():
            receiver = SignalReceiver(component_name)
            
            handler = SignalComponentHandler(
                component_name, 
                self.signal_processors[component_name]
            )
            
            receiver.add_handler(handler)
            self.receivers[component_name] = receiver
            
            hub.register_receiver(receiver)
            
            component.broadcaster.set_hub(hub)
    
    async def start_all(self):
        """启动所有组件"""
        self.logger.info("启动所有信号组件")
        
        tasks = []
        for name, receiver in self.receivers.items():
            task = asyncio.create_task(receiver.start_processing())
            tasks.append(task)
            self.logger.debug(f"启动组件: {name}")
        
        return tasks
    
    def stop_all(self):
        """停止所有组件"""
        self.logger.info("停止所有信号组件")
        
        for name, receiver in self.receivers.items():
            receiver.stop_processing()
            self.logger.debug(f"停止组件: {name}")


class SignalComponentHandler:
    """通用信号组件处理器"""
    
    def __init__(self, component_name: str, processors: Dict[SignalType, callable]):
        self.component_name = component_name
        self.processors = processors
        self.logger = logging.getLogger(f'signal.handler.{component_name}')
    
    def can_handle(self, signal: Signal) -> bool:
        """检查是否能处理该信号"""
        return signal.signal_type in self.processors
    
    async def handle_signal(self, signal: Signal) -> Optional[Signal]:
        """处理信号"""
        if signal.signal_type in self.processors:
            try:
                self.logger.info(f"📥 [Handler-{self.component_name}] 收到信号: {signal.signal_type.value}, correlation_id={signal.correlation_id}")
                
                processor = self.processors[signal.signal_type]
                self.logger.info(f"🔄 [Handler-{self.component_name}] 开始处理信号...")
                response_signal = await processor(signal)
                
                self.logger.info(f"✅ [Handler-{self.component_name}] 信号处理完成，响应类型: {response_signal.signal_type.value if response_signal else 'None'}")
                
                if response_signal:
                    from modules.signals.core import BroadcastHub
                    hub = BroadcastHub.get_instance()
                    self.logger.info(f"📤 [Handler-{self.component_name}] 广播响应信号: {response_signal.signal_type.value}")
                    await hub.broadcast_signal(response_signal)
                    self.logger.info(f"✅ [Handler-{self.component_name}] 响应信号已广播")
                
                return None  # 信号已经通过hub发送
                
            except Exception as e:
                self.logger.error(f"❌ [Handler-{self.component_name}] 处理信号失败: {e}")
                import traceback
                traceback.print_exc()
                
                error_signal = Signal(
                    signal_type=SignalType.SYSTEM_ERROR,
                    data={
                        'error': str(e),
                        'component': self.component_name,
                        'original_signal': signal.to_dict()
                    }
                )
                return error_signal
        
        return None
