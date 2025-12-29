"""内存管理器 - 作为数据交换中心"""
import asyncio
import logging
import threading
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
import uuid
from concurrent.futures import ThreadPoolExecutor
import weakref

from .snapshot import TableSnapshot
from .violation import Violation
from .fix import Fix
from ..core.io import IOManager


@dataclass
class MemoryReference:
    """内存引用 - 用于在信号中传递的轻量级引用"""
    run_id: str
    table_name: str
    snapshot_id: Optional[str] = None
    violation_ids: List[str] = field(default_factory=list)
    fix_ids: List[str] = field(default_factory=list)
    operation_type: str = "read"  # read, write, update
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'run_id': self.run_id,
            'table_name': self.table_name,
            'snapshot_id': self.snapshot_id,
            'violation_ids': self.violation_ids,
            'fix_ids': self.fix_ids,
            'operation_type': self.operation_type,
            'timestamp': self.timestamp.isoformat()
        }


class MemoryManager:
    """内存管理器 - 中心化的数据存储和访问"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(MemoryManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('memory.manager')
            
            self._snapshots: Dict[str, TableSnapshot] = {}  # {snapshot_id: TableSnapshot}
            self._violations: Dict[str, Violation] = {}     # {violation_id: Violation}
            self._fixes: Dict[str, Fix] = {}                # {fix_id: Fix}
            
            self._snapshot_index: Dict[str, Dict[str, str]] = {}  # {run_id: {table_name: snapshot_id}}
            self._violation_index: Dict[str, Dict[str, List[str]]] = {}  # {run_id: {table_name: [violation_ids]}}
            self._fix_index: Dict[str, Dict[str, List[str]]] = {}  # {run_id: {table_name: [fix_ids]}}
            
            self._unfixable_violations: Dict[str, Set[str]] = {}  # {run_id: {violation_id}}
            
            self._run_metadata: Dict[str, Dict[str, Any]] = {}  # {run_id: metadata}
            
            self._rw_locks: Dict[str, asyncio.Lock] = {}  # {run_id: Lock}
            self._access_stats: Dict[str, Dict[str, int]] = {}  # {run_id: {operation: count}}
            
            self._observers: Set[weakref.ref] = set()
            self._cleanup_threshold = 100  # 自动清理阈值
            
            self._initialized = True
            self.logger.info("MemoryManager初始化完成")
    
    @classmethod
    def get_instance(cls) -> 'MemoryManager':
        """获取单例实例"""
        return cls()
    
    def _get_run_lock(self, run_id: str) -> asyncio.Lock:
        """获取run级别的锁"""
        if run_id not in self._rw_locks:
            self._rw_locks[run_id] = asyncio.Lock()
        return self._rw_locks[run_id]
    
    def _record_access(self, run_id: str, operation: str):
        """记录访问统计"""
        if run_id not in self._access_stats:
            self._access_stats[run_id] = {}
        self._access_stats[run_id][operation] = self._access_stats[run_id].get(operation, 0) + 1
    
    async def initialize_run(self, run_id: str, metadata: Dict[str, Any] = None):
        """初始化运行"""
        async with self._get_run_lock(run_id):
            self._run_metadata[run_id] = metadata or {}
            self._snapshot_index[run_id] = {}
            self._violation_index[run_id] = {}
            self._fix_index[run_id] = {}
            self._record_access(run_id, 'initialize')
            
            self.logger.info(f"初始化运行: {run_id}")
    
    
    async def store_snapshot(self, snapshot: TableSnapshot, run_id: str = None) -> str:
        """存储快照并返回ID - 增强支持阶段性快照管理"""
        if run_id is None:
            run_id = snapshot.run_id
        
        stage = getattr(snapshot, 'processing_stage', 'unknown')
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]  # 精确到毫秒
        snapshot_id = f"snapshot_{run_id}_{snapshot.table}_{stage}_{timestamp}"
        
        async with self._get_run_lock(run_id):
            self._snapshots[snapshot_id] = snapshot
            
            if run_id not in self._snapshot_index:
                self._snapshot_index[run_id] = {}
            if snapshot.table not in self._snapshot_index[run_id]:
                self._snapshot_index[run_id][snapshot.table] = {}
            
            if 'stages' not in self._snapshot_index[run_id][snapshot.table]:
                self._snapshot_index[run_id][snapshot.table]['stages'] = {}
            self._snapshot_index[run_id][snapshot.table]['stages'][stage] = snapshot_id
            
            self._snapshot_index[run_id][snapshot.table]['latest'] = snapshot_id
            self._snapshot_index[run_id][snapshot.table]['latest_stage'] = stage
            
            self._record_access(run_id, 'store_snapshot')
            
            self.logger.debug(f"存储阶段性快照: {snapshot_id} (表: {snapshot.table}, 阶段: {stage}, 行数: {len(snapshot.rows)})")
            
        return snapshot_id
    
    async def get_snapshot(self, run_id: str, table_name: str, stage: str = None) -> Optional[TableSnapshot]:
        """获取快照 - 增强支持按阶段获取"""
        async with self._get_run_lock(run_id):
            if (run_id in self._snapshot_index and 
                table_name in self._snapshot_index[run_id]):
                
                table_index = self._snapshot_index[run_id][table_name]
                
                if isinstance(table_index, dict):
                    if stage and 'stages' in table_index and stage in table_index['stages']:
                        snapshot_id = table_index['stages'][stage]
                    elif 'latest' in table_index:
                        snapshot_id = table_index['latest']
                    else:
                        return None
                else:
                    snapshot_id = table_index
                
                snapshot = self._snapshots.get(snapshot_id)
                self._record_access(run_id, 'get_snapshot')
                
                if snapshot:
                    current_stage = getattr(snapshot, 'processing_stage', 'unknown')
                    self.logger.debug(f"获取快照: {table_name} (阶段: {current_stage})")
                
                return snapshot
        
        return None
    
    async def get_snapshot_by_id(self, snapshot_id: str) -> Optional[TableSnapshot]:
        """通过ID获取快照"""
        return self._snapshots.get(snapshot_id)
    
    async def get_snapshots_by_stage(self, run_id: str, table_name: str) -> Dict[str, TableSnapshot]:
        """获取表的所有阶段快照"""
        result = {}
        async with self._get_run_lock(run_id):
            if (run_id in self._snapshot_index and 
                table_name in self._snapshot_index[run_id]):
                
                table_index = self._snapshot_index[run_id][table_name]
                
                if isinstance(table_index, dict) and 'stages' in table_index:
                    for stage, snapshot_id in table_index['stages'].items():
                        snapshot = self._snapshots.get(snapshot_id)
                        if snapshot:
                            result[stage] = snapshot
                
                self._record_access(run_id, 'get_snapshots_by_stage')
                
        return result
    
    async def get_snapshot_stages(self, run_id: str, table_name: str) -> List[str]:
        """获取表的所有快照阶段列表"""
        stages = []
        async with self._get_run_lock(run_id):
            if (run_id in self._snapshot_index and 
                table_name in self._snapshot_index[run_id]):
                
                table_index = self._snapshot_index[run_id][table_name]
                
                if isinstance(table_index, dict) and 'stages' in table_index:
                    stages = list(table_index['stages'].keys())
                
        return stages
    
    async def get_all_snapshots(self, run_id: str) -> Dict[str, TableSnapshot]:
        """获取运行的所有最新快照 - 增强支持阶段性快照"""
        result = {}
        async with self._get_run_lock(run_id):
            if run_id in self._snapshot_index:
                for table_name, table_index in self._snapshot_index[run_id].items():
                    if isinstance(table_index, dict):
                        if 'latest' in table_index:
                            snapshot_id = table_index['latest']
                            if snapshot_id in self._snapshots:
                                result[table_name] = self._snapshots[snapshot_id]
                    else:
                        snapshot_id = table_index
                        if snapshot_id in self._snapshots:
                            result[table_name] = self._snapshots[snapshot_id]
            
            self._record_access(run_id, 'get_all_snapshots')
            
        return result
    
    
    async def store_violations(self, violations: List[Violation], run_id: str, table_name: str) -> List[str]:
        """存储违规列表并返回ID列表"""
        violation_ids = []
        
        async with self._get_run_lock(run_id):
            for violation in violations:
                violation_id = f"violation_{run_id}_{table_name}_{uuid.uuid4().hex[:8]}"
                self._violations[violation_id] = violation
                violation_ids.append(violation_id)
            
            if run_id not in self._violation_index:
                self._violation_index[run_id] = {}
            if table_name not in self._violation_index[run_id]:
                self._violation_index[run_id][table_name] = []
            
            self._violation_index[run_id][table_name].extend(violation_ids)
            self._record_access(run_id, 'store_violations')
            
            self.logger.debug(f"存储违规: {len(violations)} 个 (表: {table_name})")
        
        return violation_ids
    
    async def get_violations(self, run_id: str, table_name: str) -> List[Violation]:
        """获取表的违规列表"""
        violations = []
        async with self._get_run_lock(run_id):
            if (run_id in self._violation_index and 
                table_name in self._violation_index[run_id]):
                
                violation_ids = self._violation_index[run_id][table_name]
                violations = [
                    self._violations[vid] for vid in violation_ids 
                    if vid in self._violations
                ]
                
                self._record_access(run_id, 'get_violations')
        
        return violations
    
    async def get_violations_by_ids(self, violation_ids: List[str]) -> List[Violation]:
        """通过ID列表获取违规"""
        return [
            self._violations[vid] for vid in violation_ids 
            if vid in self._violations
        ]
    
    async def clear_violations(self, run_id: str, table_name: str):
        """清除表的违规记录"""
        async with self._get_run_lock(run_id):
            if (run_id in self._violation_index and 
                table_name in self._violation_index[run_id]):
                
                violation_ids = self._violation_index[run_id][table_name]
                for vid in violation_ids:
                    if vid in self._violations:
                        del self._violations[vid]
                
                self._violation_index[run_id][table_name] = []
                self._record_access(run_id, 'clear_violations')
                
                self.logger.debug(f"清除违规: 表 {table_name}")
    
    async def mark_violation_unfixable(self, violation_id: str, run_id: str):
        """标记违规为无法修复"""
        async with self._get_run_lock(run_id):
            if run_id not in self._unfixable_violations:
                self._unfixable_violations[run_id] = set()
            self._unfixable_violations[run_id].add(violation_id)
            self.logger.info(f"标记违规 {violation_id} 为无法修复")
    
    async def is_violation_unfixable(self, violation_id: str, run_id: str) -> bool:
        """检查违规是否已标记为无法修复"""
        async with self._get_run_lock(run_id):
            if run_id not in self._unfixable_violations:
                return False
            return violation_id in self._unfixable_violations[run_id]
    
    async def get_unfixable_violations(self, run_id: str) -> Set[str]:
        """获取所有无法修复的违规ID"""
        async with self._get_run_lock(run_id):
            return self._unfixable_violations.get(run_id, set()).copy()
    
    async def check_repeated_violations(self, violations: List[Violation], run_id: str, 
                                       table_name: str) -> List[Violation]:
        """
        检测重复违规：如果单元格已经被修复过但violation还在，说明修复失败
        
        Args:
            violations: 当前的违规列表
            run_id: 运行ID
            table_name: 表名
            
        Returns:
            重复出现（修复失败）的violations列表
        """
        repeated = []
        
        async with self._get_run_lock(run_id):
            if (run_id not in self._fix_index or 
                table_name not in self._fix_index[run_id]):
                return repeated
            
            fix_ids = self._fix_index[run_id][table_name]
            fixes = [self._fixes[fid] for fid in fix_ids if fid in self._fixes]
            
            fixed_cells = set()
            for fix in fixes:
                if hasattr(fix, 'tuple_id') and hasattr(fix, 'attr'):
                    fixed_cells.add((fix.tuple_id, fix.attr))
            
            for violation in violations:
                cell_key = (violation.tuple_id, violation.attr)
                if cell_key in fixed_cells:
                    repeated.append(violation)
                    self.logger.info(f"检测到重复违规（修复失败）: {violation.table}.{violation.tuple_id}.{violation.attr}")
        
        return repeated
    
    
    async def store_fixes(self, fixes: List[Fix], run_id: str, table_name: str) -> List[str]:
        """存储修复列表并返回ID列表"""
        fix_ids = []
        
        async with self._get_run_lock(run_id):
            for fix in fixes:
                fix_id = f"fix_{run_id}_{table_name}_{uuid.uuid4().hex[:8]}"
                self._fixes[fix_id] = fix
                fix_ids.append(fix_id)
            
            if run_id not in self._fix_index:
                self._fix_index[run_id] = {}
            if table_name not in self._fix_index[run_id]:
                self._fix_index[run_id][table_name] = []
            
            self._fix_index[run_id][table_name].extend(fix_ids)
            self._record_access(run_id, 'store_fixes')
            
            self.logger.debug(f"存储修复: {len(fixes)} 个 (表: {table_name})")
        
        return fix_ids
    
    async def get_fixes(self, run_id: str, table_name: str) -> List[Fix]:
        """获取表的修复列表"""
        fixes = []
        async with self._get_run_lock(run_id):
            if (run_id in self._fix_index and 
                table_name in self._fix_index[run_id]):
                
                fix_ids = self._fix_index[run_id][table_name]
                fixes = [
                    self._fixes[fid] for fid in fix_ids 
                    if fid in self._fixes
                ]
                
                self._record_access(run_id, 'get_fixes')
        
        return fixes
    
    async def get_fixes_by_ids(self, fix_ids: List[str]) -> List[Fix]:
        """通过ID列表获取修复"""
        return [
            self._fixes[fid] for fid in fix_ids 
            if fid in self._fixes
        ]
    
    
    async def get_run_summary(self, run_id: str) -> Dict[str, Any]:
        """获取运行摘要"""
        summary = {
            'run_id': run_id,
            'tables': [],
            'total_snapshots': 0,
            'total_violations': 0,
            'total_fixes': 0,
            'access_stats': self._access_stats.get(run_id, {}),
            'metadata': self._run_metadata.get(run_id, {})
        }
        
        async with self._get_run_lock(run_id):
            if run_id in self._snapshot_index:
                for table_name in self._snapshot_index[run_id]:
                    snapshot = await self.get_snapshot(run_id, table_name)
                    violations = await self.get_violations(run_id, table_name)
                    fixes = await self.get_fixes(run_id, table_name)
                    
                    table_info = {
                        'table_name': table_name,
                        'rows': len(snapshot.rows) if snapshot else 0,
                        'violations': len(violations),
                        'fixes': len(fixes)
                    }
                    summary['tables'].append(table_info)
                    summary['total_snapshots'] += 1
                    summary['total_violations'] += len(violations)
                    summary['total_fixes'] += len(fixes)
        
        return summary
    
    async def create_memory_reference(self, run_id: str, table_name: str, 
                                     operation_type: str = "read") -> MemoryReference:
        """创建内存引用"""
        ref = MemoryReference(
            run_id=run_id,
            table_name=table_name,
            operation_type=operation_type
        )
        
        async with self._get_run_lock(run_id):
            if (run_id in self._snapshot_index and 
                table_name in self._snapshot_index[run_id]):
                ref.snapshot_id = self._snapshot_index[run_id][table_name]
            
            if (run_id in self._violation_index and 
                table_name in self._violation_index[run_id]):
                ref.violation_ids = self._violation_index[run_id][table_name][:]
            
            if (run_id in self._fix_index and 
                table_name in self._fix_index[run_id]):
                ref.fix_ids = self._fix_index[run_id][table_name][:]
        
        return ref
    
    
    async def persist_run_data(self, run_id: str, io_manager: IOManager):
        """持久化运行数据"""
        async with self._get_run_lock(run_id):
            try:
                snapshots = list((await self.get_all_snapshots(run_id)).values())
                
                all_violations = []
                all_fixes = []
                
                if run_id in self._violation_index:
                    for table_name in self._violation_index[run_id]:
                        violations = await self.get_violations(run_id, table_name)
                        all_violations.extend(violations)
                
                if run_id in self._fix_index:
                    for table_name in self._fix_index[run_id]:
                        fixes = await self.get_fixes(run_id, table_name)
                        all_fixes.extend(fixes)
                
                io_manager.write_violations(all_violations)
                io_manager.write_fixes(all_fixes)
                
                self.logger.info(f"持久化完成: {run_id} - 保留阶段性快照, {len(all_violations)}个违规, {len(all_fixes)}个修复")
                
            except Exception as e:
                self.logger.error(f"持久化失败: {run_id} - {e}")
                raise
    
    
    def add_observer(self, observer):
        """添加观察者"""
        self._observers.add(weakref.ref(observer))
    
    async def cleanup_run(self, run_id: str):
        """清理运行数据"""
        async with self._get_run_lock(run_id):
            try:
                if run_id in self._snapshot_index:
                    for snapshot_id in self._snapshot_index[run_id].values():
                        if snapshot_id in self._snapshots:
                            del self._snapshots[snapshot_id]
                    del self._snapshot_index[run_id]
                
                if run_id in self._violation_index:
                    for violation_ids in self._violation_index[run_id].values():
                        for vid in violation_ids:
                            if vid in self._violations:
                                del self._violations[vid]
                    del self._violation_index[run_id]
                
                if run_id in self._fix_index:
                    for fix_ids in self._fix_index[run_id].values():
                        for fid in fix_ids:
                            if fid in self._fixes:
                                del self._fixes[fid]
                    del self._fix_index[run_id]
                
                if run_id in self._run_metadata:
                    del self._run_metadata[run_id]
                
                if run_id in self._access_stats:
                    del self._access_stats[run_id]
                
                if run_id in self._rw_locks:
                    del self._rw_locks[run_id]
                
                self.logger.info(f"清理完成: {run_id}")
                
            except Exception as e:
                self.logger.error(f"清理失败: {run_id} - {e}")
    
    def get_memory_status(self) -> Dict[str, Any]:
        """获取内存状态"""
        return {
            'snapshots_count': len(self._snapshots),
            'violations_count': len(self._violations),
            'fixes_count': len(self._fixes),
            'active_runs': list(self._run_metadata.keys()),
            'total_access_stats': sum(
                sum(stats.values()) for stats in self._access_stats.values()
            )
        }
