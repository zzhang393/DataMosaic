"""并行表处理协调器 - 支持多表并行提取、验证和修复"""
import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor
import threading

from .unified_orchestrator import UnifiedSignalDrivenOrchestrator
from .context import Doc2DBContext, Doc2DBProcessingState
from .utils import SignalUtils, StepUtils, DataTransferUtils
from ...signals.core import SignalType
from ...memory import TableSnapshot, Violation, Fix


class ParallelTableProcessor:
    """并行表处理器 - 管理单个表的完整处理流程"""
    
    def __init__(self, table_name: str, orchestrator, logger=None):
        self.table_name = table_name
        self.orchestrator = orchestrator
        self.logger = logger or logging.getLogger(f'parallel.{table_name}')
        self.processing_state = 'pending'
        self.start_time = None
        self.end_time = None
        self.error = None
        
        self.snapshots = []
        self.violations = []
        self.fixes = []
        
    async def process_table(self, context: Doc2DBContext) -> Dict[str, Any]:
        """处理单个表的完整流程"""
        self.start_time = time.time()
        self.processing_state = 'processing'
        
        try:
            self.logger.info(f"🚀 开始并行处理表: {self.table_name}")
            
            await self._extract_data(context)
            
            await self._verify_and_fix_loop(context)
            
            self.processing_state = 'completed'
            self.end_time = time.time()
            
            self.logger.info(f"✅ 表 {self.table_name} 处理完成，耗时: {self.end_time - self.start_time:.2f}秒")
            
            return {
                'table_name': self.table_name,
                'status': 'completed',
                'processing_time': self.end_time - self.start_time,
                'snapshots': self.snapshots,
                'violations': self.violations,
                'fixes': self.fixes
            }
            
        except Exception as e:
            self.processing_state = 'failed'
            self.end_time = time.time()
            self.error = str(e)
            
            self.logger.error(f"❌ 表 {self.table_name} 处理失败: {e}")
            
            return {
                'table_name': self.table_name,
                'status': 'failed',
                'error': str(e),
                'processing_time': self.end_time - self.start_time if self.start_time else 0
            }
    
    async def _extract_data(self, context: Doc2DBContext):
        """提取数据"""
        self.logger.info(f"🤖 开始提取表 {self.table_name} 的数据...")
        
        table_specific_schema = self.orchestrator.schema_utils.extract_table_specific_schema(
            context.schema, self.table_name
        )
        if not table_specific_schema:
            table_specific_schema = context.schema
        
        signal_data = SignalUtils.create_extraction_signal_data(
            context, self.table_name, table_specific_schema
        )
        
        await self.orchestrator.broadcaster.emit_simple_signal(
            SignalType.EXTRACTION_REQUEST,
            data=signal_data,
            correlation_id=SignalUtils.get_correlation_id(
                context.run_id, self.table_name, "parallel_extract"
            )
        )
        
        await self._wait_for_extraction_complete(context)
    
    async def _wait_for_extraction_complete(self, context: Doc2DBContext, timeout: float = 30.0):
        """等待提取完成"""
        start_time = time.time()
        
        initial_snapshot_id = None
        if self.table_name in context.all_snapshots:
            initial_snapshot = context.all_snapshots[self.table_name]
            if hasattr(initial_snapshot, 'created_at'):
                initial_snapshot_id = initial_snapshot.created_at
            elif hasattr(initial_snapshot, 'table_id'):
                initial_snapshot_id = initial_snapshot.table_id
        
        while time.time() - start_time < timeout:
            if self.table_name in context.all_snapshots:
                snapshot = context.all_snapshots[self.table_name]
                
                is_new_snapshot = False
                if initial_snapshot_id is None:
                    is_new_snapshot = True
                elif hasattr(snapshot, 'created_at') and snapshot.created_at != initial_snapshot_id:
                    is_new_snapshot = True
                elif hasattr(snapshot, 'table_id') and snapshot.table_id != initial_snapshot_id:
                    is_new_snapshot = True
                
                if is_new_snapshot:
                    self.snapshots.append(snapshot)
                    return
            
            await asyncio.sleep(0.5)
        
        raise TimeoutError(f"表 {self.table_name} 提取超时")
    
    async def _verify_and_fix_loop(self, context: Doc2DBContext, max_iterations: int = 3):
        """验证和修复循环"""
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            self.logger.info(f"🔍 表 {self.table_name} 第 {iteration} 轮验证...")
            
            if self.table_name not in context.all_snapshots:
                self.logger.warning(f"表 {self.table_name} 没有可用快照，跳过验证")
                break
            
            snapshot = context.all_snapshots[self.table_name]
            
            await self._request_verification(context, snapshot)
            
            violations = await self._wait_for_verification_complete(context)
            
            if not violations:
                self.logger.info(f"✅ 表 {self.table_name} 验证通过，无违规")
                break
            
            violations_requiring_reextraction = []
            violations_for_tool_fixing = []
            
            for i, violation in enumerate(violations):
                processing_category = getattr(violation, 'processing_category', None)
                severity = getattr(violation, 'severity', 'warn')
                
                if processing_category == 'requires_reextraction':
                    violations_requiring_reextraction.append(violation)
                    self.logger.debug(f"需重提取: {violation.tuple_id}.{violation.attr}")
                elif processing_category == 'tool_fixable' or processing_category is None:
                    violations_for_tool_fixing.append(violation)
                else:
                    if severity == 'error':
                        violations_requiring_reextraction.append(violation)
                    else:
                        violations_for_tool_fixing.append(violation)
            
            error_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'error']
            warn_violations = [v for v in violations if getattr(v, 'severity', 'warn') == 'warn']
            
            self.logger.info(f"表 {self.table_name} 发现违规: Error {len(error_violations)}, Warning {len(warn_violations)}")
            self.logger.info(f"智能分类: 需重提取 {len(violations_requiring_reextraction)}, 可工具修复 {len(violations_for_tool_fixing)}")
            
            if violations_requiring_reextraction:
                self.logger.info(f"表 {self.table_name} 存在需要重新提取的violations，触发 warm start")
                await self._trigger_warm_start_extraction(context, violations_requiring_reextraction, snapshot)
                continue
            
            if not error_violations and len(self.fixes) > 0:
                self.logger.info(f"✅ 表 {self.table_name} 只有警告级别违规且已修复过，结束处理")
                break
            
            violations_to_fix = violations_for_tool_fixing[:10]  # 限制数量
            
            if violations_to_fix:
                await self._request_fixing(context, violations_to_fix, snapshot)
                
                fixes = await self._wait_for_fixing_complete(context)
                
                if fixes:
                    self.fixes.extend(fixes)
                    self.logger.info(f"表 {self.table_name} 修复完成，应用了 {len(fixes)} 个修复")
                else:
                    self.logger.warning(f"表 {self.table_name} 修复失败或无修复")
                    break
            else:
                break
        
        self.logger.info(f"表 {self.table_name} 验证修复循环完成，共 {iteration} 轮")
    
    async def _request_verification(self, context: Doc2DBContext, snapshot: TableSnapshot):
        """请求验证"""
        table_specific_schema = self.orchestrator.schema_utils.extract_table_specific_schema(
            context.schema, self.table_name
        )
        if not table_specific_schema:
            table_specific_schema = context.schema
        
        signal_data = SignalUtils.create_verification_signal_data(
            context, self.table_name, table_specific_schema, [snapshot]
        )
        
        timestamp = int(time.time() * 1000)
        await self.orchestrator.broadcaster.emit_simple_signal(
            SignalType.VERIFICATION_REQUEST,
            data=signal_data,
            correlation_id=f"{context.run_id}_{self.table_name}_parallel_verify_{timestamp}"
        )
    
    async def _wait_for_verification_complete(self, context: Doc2DBContext, timeout: float = 20.0) -> List[Violation]:
        """等待验证完成"""
        start_time = time.time()
        initial_violation_count = len(context.all_violations.get(self.table_name, []))
        
        while time.time() - start_time < timeout:
            if self.table_name in context.all_violations:
                current_violations = context.all_violations[self.table_name]
                if len(current_violations) > initial_violation_count:
                    new_violations = current_violations[initial_violation_count:]
                    self.violations.extend(new_violations)
                    return new_violations
            
            await asyncio.sleep(0.5)
        
        return []
    
    async def _request_fixing(self, context: Doc2DBContext, violations: List[Violation], snapshot: TableSnapshot):
        """请求修复"""
        table_specific_schema = self.orchestrator.schema_utils.extract_table_specific_schema(
            context.schema, self.table_name
        )
        if not table_specific_schema:
            table_specific_schema = context.schema
        
        signal_data = SignalUtils.create_fixing_signal_data(
            context, self.table_name, table_specific_schema, violations, snapshot
        )
        
        timestamp = int(time.time() * 1000)
        await self.orchestrator.broadcaster.emit_simple_signal(
            SignalType.FIXING_REQUEST,
            data=signal_data,
            correlation_id=f"{context.run_id}_{self.table_name}_parallel_fix_{timestamp}"
        )
    
    async def _wait_for_fixing_complete(self, context: Doc2DBContext, timeout: float = 20.0) -> List[Fix]:
        """等待修复完成"""
        start_time = time.time()
        initial_fix_count = len(context.all_fixes.get(self.table_name, []))
        
        while time.time() - start_time < timeout:
            if self.table_name in context.all_fixes:
                current_fixes = context.all_fixes[self.table_name]
                if len(current_fixes) > initial_fix_count:
                    new_fixes = current_fixes[initial_fix_count:]
                    return new_fixes
            
            await asyncio.sleep(0.5)
        
        return []
    
    async def _trigger_warm_start_extraction(self, context: Doc2DBContext, violations_requiring_reextraction: List[Violation], snapshot: TableSnapshot):
        """触发 warm start 重新提取（带有violations信息）"""
        self.logger.info(f"开始 warm start 重新提取表 {self.table_name} - 基于 {len(violations_requiring_reextraction)} 个需要重新提取的violations")
        
        table_specific_schema = self.orchestrator.schema_utils.extract_table_specific_schema(
            context.schema, self.table_name
        )
        if not table_specific_schema:
            table_specific_schema = context.schema
        
        signal_data = SignalUtils.create_extraction_signal_data(
            context, self.table_name, table_specific_schema
        )
        
        signal_data.update({
            'warm_start': True,
            'previous_snapshot': snapshot.to_dict() if snapshot else None,
            'violations_requiring_reextraction': [
                {
                    'id': v.id,
                    'tuple_id': v.tuple_id,
                    'attr': v.attr,
                    'constraint_type': v.constraint_type,
                    'description': v.description,
                    'severity': v.severity,
                    'suggested_fix': v.suggested_fix.value if hasattr(v, 'suggested_fix') and v.suggested_fix else None
                } 
                for v in violations_requiring_reextraction
            ]
        })
        
        await self.orchestrator.broadcaster.emit_simple_signal(
            SignalType.EXTRACTION_REQUEST,
            data=signal_data,
            correlation_id=SignalUtils.get_correlation_id(
                context.run_id, self.table_name, "parallel_warm_start_extract"
            )
        )
        
        await self._wait_for_extraction_complete(context, timeout=30.0)
        
        self.logger.info(f"warm start 重新提取完成：{self.table_name}")


class ParallelSignalDrivenOrchestrator(UnifiedSignalDrivenOrchestrator):
    """支持并行处理的信号驱动协调器"""
    
    def __init__(self, max_iterations: int = 3, max_concurrent_tables: int = 3):
        super().__init__(max_iterations)
        self.max_concurrent_tables = max_concurrent_tables
        self.logger = logging.getLogger('parallel.orchestrator')
    
    async def _run_signal_driven_pipeline(self, context: Doc2DBContext):
        """运行并行信号驱动的处理管道"""
        self.logger.info(f'并行处理管道启动 - 运行ID: {context.run_id}, 目标表: {len(context.target_tables)}')
        await self.broadcaster.emit_simple_signal(
            SignalType.PROCESSING_START,
            data={'context': context.to_dict()},
            correlation_id=context.run_id
        )
        
        await self._signal_analyze_tables(context)
        
        if len(context.target_tables) > 1:
            await self._process_tables_in_parallel(context)
        else:
            await super()._run_signal_driven_pipeline(context)
            return
        
        await self._signal_multi_table_verify(context)
        
        await self._finalize_parallel_processing(context)
    
    async def _process_tables_in_parallel(self, context: Doc2DBContext):
        """并行处理多个表"""
        self.logger.info(f"开始并行处理 {len(context.target_tables)} 个表")
        
        processors = []
        for table_name in context.target_tables:
            processor = ParallelTableProcessor(table_name, self, self.logger)
            processors.append(processor)
        
        semaphore = asyncio.Semaphore(self.max_concurrent_tables)
        
        async def process_with_semaphore(processor):
            async with semaphore:
                return await processor.process_table(context)
        
        tasks = [process_with_semaphore(processor) for processor in processors]
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful_tables = []
            failed_tables = []
            
            for i, result in enumerate(results):
                table_name = context.target_tables[i]
                
                if isinstance(result, Exception):
                    self.logger.error(f"表 {table_name} 处理异常: {result}")
                    failed_tables.append(table_name)
                elif isinstance(result, dict) and result.get('status') == 'completed':
                    self.logger.info(f"表 {table_name} 处理成功")
                    successful_tables.append(table_name)
                else:
                    self.logger.warning(f"表 {table_name} 处理状态未知: {result}")
                    failed_tables.append(table_name)
            
            parallel_step = StepUtils.create_custom_step(
                context,
                step_name="parallel_processing_complete",
                description=f"并行处理完成 - 成功: {len(successful_tables)}, 失败: {len(failed_tables)}",
                details={
                    'successful_tables': successful_tables,
                    'failed_tables': failed_tables,
                    'total_tables': len(context.target_tables),
                    'concurrent_limit': self.max_concurrent_tables,
                    'results': [r for r in results if not isinstance(r, Exception)]
                }
            )
            context.step_outputs.append(parallel_step)
            
            self.logger.info(f"并行处理完成 - 成功: {len(successful_tables)}, 失败: {len(failed_tables)}")
            
        except Exception as e:
            self.logger.error(f"并行处理失败: {e}")
            raise
    
    async def _finalize_parallel_processing(self, context: Doc2DBContext):
        """最终化并行处理结果"""
        total_snapshots = len(context.all_snapshots)
        total_violations = sum(len(v) for v in context.all_violations.values())
        total_fixes = sum(len(f) for f in context.all_fixes.values())
        
        self.logger.info(f'所有表处理完成 - 快照:{total_snapshots}, 违规:{total_violations}, 修复:{total_fixes}')
        
        DataTransferUtils.transfer_processing_results(context)
        
        self._sync_snapshots_to_context(context)
        
        self._setup_result_context(context)
        
        context.current_state = Doc2DBProcessingState.COMPLETED
        
        completion_step = StepUtils.create_completion_step(context)
        completion_step['details']['processing_mode'] = 'parallel'
        completion_step['details']['concurrent_tables'] = self.max_concurrent_tables
        context.step_outputs.append(completion_step)
        
        DataTransferUtils.save_final_snapshots(context)
        
        await self.broadcaster.emit_simple_signal(
            SignalType.PROCESSING_COMPLETE,
            data={
                'context': context.to_dict(),
                'processing_mode': 'parallel',
                'concurrent_tables': self.max_concurrent_tables
            },
            correlation_id=context.run_id
        )


def create_parallel_orchestrator(max_iterations: int = 3, max_concurrent_tables: int = 3) -> ParallelSignalDrivenOrchestrator:
    """创建并行信号驱动协调器"""
    return ParallelSignalDrivenOrchestrator(max_iterations=max_iterations, max_concurrent_tables=max_concurrent_tables)


async def run_parallel_signal_orchestrator(context: Doc2DBContext, max_concurrent_tables: int = 3) -> Doc2DBContext:
    """运行并行信号驱动协调器"""
    orchestrator = create_parallel_orchestrator(max_concurrent_tables=max_concurrent_tables)
    return await orchestrator.process_with_signals(context)
