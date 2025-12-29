"""内存管理操作模块"""
import asyncio
import concurrent.futures
import json
import os
import logging
from typing import Dict, Any, Optional, List

from .context import Doc2DBContext
from ...memory.manager import MemoryManager


class MemoryOperationsMixin:
    """内存操作混入类"""
    
    def __init__(self):
        self.memory_manager = MemoryManager.get_instance()
        self.logger = logging.getLogger('memory_operations')
    
    async def initialize_memory(self, context: Doc2DBContext):
        """初始化Memory Manager"""
        metadata = {
            'table_name': context.table_name,
            'user_query': context.user_query,
            'language': context.language,
            'model': context.model,
            'max_iterations': context.max_iterations,
            'mode': 'signal_driven'
        }
        
        await self.memory_manager.initialize_run(context.run_id, metadata)
        self.logger.info("Memory Manager 初始化完成")
    
    async def persist_results(self, context: Doc2DBContext):
        """持久化处理结果"""
        all_snapshots = list(context.all_snapshots.values())
        all_violations = []
        all_fixes = []
        
        for violations in context.all_violations.values():
            all_violations.extend(violations)
        
        for fixes in context.all_fixes.values():
            all_fixes.extend(fixes)
        
        all_violations.extend(context.multi_table_violations)
        
        context.snapshots = all_snapshots
        context.violations = all_violations
        context.fixes = all_fixes
        
        context.io_manager.write_violations(context.violations)
        context.io_manager.write_fixes(context.fixes)
        
        await self.memory_manager.persist_run_data(context.run_id, context.io_manager)
        
        self.logger.info("结果持久化完成 - 保留了阶段性快照记录")


class MemoryQueryMixin:
    """内存查询混入类，提供内存数据查询方法"""
    
    def __init__(self):
        self.memory_manager = MemoryManager.get_instance()
        self.current_context: Optional[Doc2DBContext] = None
    
    def get_memory_snapshots(self, run_id: str, table_name: Optional[str] = None) -> Dict[str, Any]:
        """获取内存中的快照数据"""
        try:
            if table_name:
                snapshot = self._sync_wrapper(self.memory_manager.get_snapshot(run_id, table_name))
                snapshot_dict = snapshot.to_dict() if snapshot else None
                if snapshot_dict:
                    snapshot_dict = self._reorder_snapshot_cells(run_id, snapshot_dict)
                return {
                    'table_name': table_name,
                    'snapshot': snapshot_dict
                }
            else:
                snapshots = self._sync_wrapper(self.memory_manager.get_all_snapshots(run_id))
                if snapshots:
                    snapshots_dict = {}
                    for name, snap in snapshots.items():
                        snap_dict = snap.to_dict()
                        snap_dict = self._reorder_snapshot_cells(run_id, snap_dict, name)
                        snapshots_dict[name] = snap_dict
                    return {
                        'snapshots': snapshots_dict
                    }
                else:
                    raise Exception('内存中无快照数据')
        except Exception as e:
            try:
                return self._get_snapshots_from_file(run_id)
            except Exception as file_e:
                return {'error': f'获取快照失败 - 内存: {e}, 文件: {file_e}'}
    
    def get_memory_violations(self, run_id: str, table_name: Optional[str] = None) -> Dict[str, Any]:
        """获取内存中的违规数据"""
        try:
            if table_name:
                violations = self._sync_wrapper(self.memory_manager.get_violations(run_id, table_name))
                return {
                    'table_name': table_name,
                    'violations': [v.to_dict() for v in violations]
                }
            else:
                if hasattr(self.current_context, 'target_tables') and self.current_context.target_tables:
                    all_violations = {}
                    for tbl in self.current_context.target_tables:
                        violations = self._sync_wrapper(self.memory_manager.get_violations(run_id, tbl))
                        all_violations[tbl] = [v.to_dict() for v in violations]
                    return {'violations_by_table': all_violations}
                else:
                    raise Exception('current_context无表列表信息')
        except Exception as e:
            try:
                return self._get_violations_from_file(run_id)
            except Exception as file_e:
                return {'error': f'获取违规失败 - 内存: {e}, 文件: {file_e}'}
    
    def get_memory_fixes(self, run_id: str, table_name: Optional[str] = None) -> Dict[str, Any]:
        """获取内存中的修复数据"""
        try:
            if table_name:
                fixes = self._sync_wrapper(self.memory_manager.get_fixes(run_id, table_name))
                return {
                    'table_name': table_name,
                    'fixes': [f.to_dict() for f in fixes]
                }
            else:
                if hasattr(self.current_context, 'target_tables') and self.current_context.target_tables:
                    all_fixes = {}
                    for tbl in self.current_context.target_tables:
                        fixes = self._sync_wrapper(self.memory_manager.get_fixes(run_id, tbl))
                        all_fixes[tbl] = [f.to_dict() for f in fixes]
                    return {'fixes_by_table': all_fixes}
                else:
                    raise Exception('current_context无表列表信息')
        except Exception as e:
            try:
                return self._get_fixes_from_file(run_id)
            except Exception as file_e:
                return {'error': f'获取修复失败 - 内存: {e}, 文件: {file_e}'}
    
    def get_memory_summary(self, run_id: str) -> Dict[str, Any]:
        """获取内存数据摘要"""
        try:
            summary = self._sync_wrapper(self.memory_manager.get_run_summary(run_id))
            return summary
        except Exception as e:
            return {'error': f'获取摘要失败: {e}'}
    
    def _reorder_snapshot_cells(self, run_id: str, snapshot_dict: Dict[str, Any], table_name: Optional[str] = None) -> Dict[str, Any]:
        """重排 snapshot 中 cells 的顺序以匹配 result.json 中的字段顺序"""
        try:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            output_dir = os.path.join(project_root, "backend", "output", run_id)
            result_file = os.path.join(output_dir, "result.json")
            
            if not os.path.exists(result_file):
                self.logger.warning(f"result.json 不存在，跳过字段重排")
                return snapshot_dict
            
            with open(result_file, 'r', encoding='utf-8') as f:
                result_data = json.load(f)
            
            target_table = table_name or snapshot_dict.get('table')
            
            if not target_table or target_table not in result_data.get('tables', {}):
                self.logger.warning(f"表 {target_table} 在 result.json 中不存在")
                return snapshot_dict
            
            table_rows = result_data['tables'][target_table]
            if not table_rows or len(table_rows) == 0:
                self.logger.warning(f"表 {target_table} 没有数据行")
                return snapshot_dict
            
            field_order = list(table_rows[0].keys())
            
            if 'rows' in snapshot_dict:
                for row in snapshot_dict['rows']:
                    if 'cells' in row and isinstance(row['cells'], dict):
                        reordered_cells = {}
                        for field in field_order:
                            if field in row['cells']:
                                reordered_cells[field] = row['cells'][field]
                        for field, value in row['cells'].items():
                            if field not in reordered_cells:
                                reordered_cells[field] = value
                        row['cells'] = reordered_cells
            
            return snapshot_dict
        except Exception as e:
            self.logger.error(f"重排 snapshot cells 顺序失败: {e}", exc_info=True)
            return snapshot_dict
    
    def _get_snapshots_from_file(self, run_id: str) -> Dict[str, Any]:
        """从文件读取快照数据"""
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        output_dir = os.path.join(project_root, "backend", "output", run_id)
        snapshots_file = os.path.join(output_dir, "snapshots.jsonl")
        
        if not os.path.exists(snapshots_file):
            return {'error': '快照文件不存在'}
        
        snapshots = []
        with open(snapshots_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    snapshot_dict = json.loads(line, object_pairs_hook=dict)
                    snapshot_dict = self._reorder_snapshot_cells(run_id, snapshot_dict)
                    snapshots.append(snapshot_dict)
        
        return {'snapshots_from_file': snapshots}
    
    def _get_violations_from_file(self, run_id: str) -> Dict[str, Any]:
        """从文件读取违规数据"""
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        output_dir = os.path.join(project_root, "backend", "output", run_id)
        violations_file = os.path.join(output_dir, "violations.jsonl")
        
        if not os.path.exists(violations_file):
            return {'error': '违规文件不存在'}
        
        violations = []
        with open(violations_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    violations.append(json.loads(line))
        
        return {'violations_from_file': violations}
    
    def _get_fixes_from_file(self, run_id: str) -> Dict[str, Any]:
        """从文件读取修复数据"""
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        output_dir = os.path.join(project_root, "backend", "output", run_id)
        fixes_file = os.path.join(output_dir, "fixes.jsonl")
        
        if not os.path.exists(fixes_file):
            return {'error': '修复文件不存在'}
        
        fixes = []
        with open(fixes_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    fixes.append(json.loads(line))
        
        return {'fixes_from_file': fixes}
    
    def _sync_wrapper(self, coro):
        """将异步协程转换为同步调用"""
        try:
            loop = asyncio.get_running_loop()
            def run_in_new_loop():
                return asyncio.run(coro)
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_new_loop)
                result = future.result(timeout=60)
                return result
        except RuntimeError:
            return asyncio.run(coro)
        except Exception as e:
            raise
