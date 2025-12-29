"""信号协调器 - 管理信号的生命周期、批次和表状态"""
import asyncio
import logging
import time
from typing import Dict, Optional, Set, List, Any
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict

from ...signals.core import Signal, SignalType


class TableLifecycleState(Enum):
    """表的生命周期状态"""
    PENDING = "pending"  # 待处理
    EXTRACTING = "extracting"  # 提取中
    EXTRACTED = "extracted"  # 提取完成
    VERIFYING = "verifying"  # 验证中
    VERIFIED = "verified"  # 验证完成
    FIXING = "fixing"  # 修复中
    FIXED = "fixed"  # 修复完成
    WARM_START = "warm_start"  # 热启动中
    FINAL = "final"  # 最终完成
    ERROR = "error"  # 错误状态


@dataclass
class SignalWaiter:
    """信号等待器 - 用于等待特定信号的响应"""
    correlation_id: str
    signal_type: SignalType
    created_at: float
    timeout: float
    future: asyncio.Future = field(default_factory=asyncio.Future)
    response_signal: Optional[Signal] = None
    
    def is_expired(self) -> bool:
        """检查是否超时"""
        return time.time() - self.created_at > self.timeout
    
    def complete(self, signal: Signal):
        """完成等待"""
        if not self.future.done():
            self.response_signal = signal
            self.future.set_result(signal)
    
    def timeout_cancel(self):
        """超时取消"""
        if not self.future.done():
            self.future.set_exception(TimeoutError(f"Signal {self.correlation_id} timeout after {self.timeout}s"))


@dataclass
class BatchTracker:
    """批次追踪器"""
    table_name: str
    batch_index: int  # 原始的batch索引（如 1, 2, 3...）
    total_batches: int
    state: str = "pending"  # pending, extracting, extracted, verifying, verified, fixing, fixed, merged, final
    snapshot: Any = None
    violations: List = field(default_factory=list)
    fixes: List = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    is_warm_start: bool = False  # 是否为warm start batch
    warm_start_count: int = 0  # 该batch的warm start次数
    max_warm_start_attempts: int = 1  # 每个batch最多1次warm start
    verification_round: int = 0  # 该batch的验证轮次
    max_verification_rounds: int = 1  # 每个batch最多1轮验证
    
    def get_batch_key(self) -> str:
        """获取batch的唯一键"""
        prefix = "warm_start_" if self.is_warm_start else ""
        return f"{prefix}batch_{self.batch_index}"


@dataclass
class TableStateTracker:
    """表状态追踪器"""
    table_name: str
    current_state: TableLifecycleState = TableLifecycleState.PENDING
    batch_trackers: Dict[str, BatchTracker] = field(default_factory=dict)  # 🔥 改为字符串键
    total_batches: int = 1  # 默认1个batch（非多文档模式）
    completed_batches: int = 0
    snapshot: Any = None
    violations: List = field(default_factory=list)
    fixes: List = field(default_factory=list)
    verify_fix_iteration: int = 0  # 验证-修复迭代次数
    max_iterations: int = 1
    warm_start_attempted: bool = False
    is_relation_table: bool = False
    created_at: float = field(default_factory=time.time)
    verification_round: int = 0  # 验证轮次
    max_verification_rounds: int = 2  # 最大验证轮次（默认2轮）
    
    def can_transition_to(self, new_state: TableLifecycleState) -> bool:
        """检查是否可以转换到新状态"""
        valid_transitions = {
            TableLifecycleState.PENDING: [
                TableLifecycleState.EXTRACTING,
                TableLifecycleState.EXTRACTED,  # 允许从warm start直接进入extracted
                TableLifecycleState.VERIFYING,  # 允许从warm start直接进入验证
                TableLifecycleState.VERIFIED,   # 允许从warm start后直接标记已验证
                TableLifecycleState.FIXING,     # 允许从warm start后直接进入修复
                TableLifecycleState.FIXED,      # 允许从warm start后直接标记已修复
                TableLifecycleState.WARM_START, # 允许进入warm start状态
                TableLifecycleState.FINAL,      # 允许从warm start后直接完成
                TableLifecycleState.ERROR
            ],
            TableLifecycleState.EXTRACTING: [TableLifecycleState.EXTRACTED, TableLifecycleState.ERROR],
            TableLifecycleState.EXTRACTED: [TableLifecycleState.VERIFYING, TableLifecycleState.ERROR],
            TableLifecycleState.VERIFYING: [TableLifecycleState.VERIFIED, TableLifecycleState.ERROR],
            TableLifecycleState.VERIFIED: [
                TableLifecycleState.FIXING,
                TableLifecycleState.WARM_START,
                TableLifecycleState.FINAL,  # 如果没有违规，直接final
                TableLifecycleState.ERROR
            ],
            TableLifecycleState.FIXING: [
                TableLifecycleState.FIXED,
                TableLifecycleState.ERROR
            ],
            TableLifecycleState.FIXED: [
                TableLifecycleState.VERIFYING,  # 修复后重新验证
                TableLifecycleState.FINAL,
                TableLifecycleState.ERROR
            ],
            TableLifecycleState.WARM_START: [
                TableLifecycleState.EXTRACTED,  # warm start后回到extracted状态
                TableLifecycleState.VERIFYING,  # warm start后可以直接进入验证
                TableLifecycleState.ERROR
            ],
            TableLifecycleState.FINAL: [],  # 终态
            TableLifecycleState.ERROR: []  # 终态
        }
        
        return new_state in valid_transitions.get(self.current_state, [])
    
    def transition_to(self, new_state: TableLifecycleState) -> bool:
        """转换到新状态"""
        if self.can_transition_to(new_state):
            old_state = self.current_state
            self.current_state = new_state
            return True
        return False
    
    def all_batches_completed(self) -> bool:
        """检查是否所有batch都已完成"""
        return self.completed_batches >= self.total_batches
    
    def is_batch_mode(self) -> bool:
        """是否为batch模式"""
        return self.total_batches > 1


class SignalCoordinator:
    """信号协调器 - 统一管理信号生命周期和状态追踪
    
    核心功能：
    1. 信号级同步：发送信号并等待响应，支持超时
    2. Batch维度管理：追踪每个表的所有batch状态
    3. Table维度管理：管理表的完整生命周期状态机
    """
    
    def __init__(self, broadcaster, default_timeout: float = 600.0):
        """
        Args:
            broadcaster: 信号广播器
            default_timeout: 默认超时时间（秒）
        """
        self.broadcaster = broadcaster
        self.default_timeout = default_timeout
        self.logger = logging.getLogger('signal.coordinator')
        
        self.signal_waiters: Dict[str, SignalWaiter] = {}
        
        self.table_trackers: Dict[str, TableStateTracker] = {}
        
        self.response_signal_types = {
            SignalType.EXTRACTION_REQUEST: [
                SignalType.EXTRACTION_COMPLETE,
                SignalType.EXTRACTION_ERROR
            ],
            SignalType.VERIFICATION_REQUEST: [
                SignalType.VERIFICATION_COMPLETE,
                SignalType.VERIFICATION_ERROR
            ],
            SignalType.FIXING_REQUEST: [
                SignalType.FIXING_COMPLETE,
                SignalType.FIXING_ERROR
            ]
        }
        
        self._cleanup_task = None
    
    def start(self):
        """启动协调器"""
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_waiters())
        self.logger.info("信号协调器已启动")
    
    async def stop(self):
        """停止协调器"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self.logger.info("信号协调器已停止")
    
    async def _cleanup_expired_waiters(self):
        """清理超时的等待器"""
        while True:
            try:
                await asyncio.sleep(10)  # 每10秒检查一次
                
                expired_ids = []
                for correlation_id, waiter in self.signal_waiters.items():
                    if waiter.is_expired():
                        expired_ids.append(correlation_id)
                        waiter.timeout_cancel()
                        self.logger.warning(f"信号等待超时: {correlation_id}")
                
                for correlation_id in expired_ids:
                    del self.signal_waiters[correlation_id]
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"清理超时等待器异常: {e}")
    
    async def send_and_wait(
        self,
        signal_type: SignalType,
        data: Dict[str, Any],
        correlation_id: str,
        timeout: Optional[float] = None
    ) -> Optional[Signal]:
        """发送信号并等待响应
        
        Args:
            signal_type: 信号类型
            data: 信号数据
            correlation_id: 关联ID
            timeout: 超时时间（秒），None使用默认值
            
        Returns:
            响应信号，超时返回None
        """
        timeout = timeout or self.default_timeout
        
        
        waiter = SignalWaiter(
            correlation_id=correlation_id,
            signal_type=signal_type,
            created_at=time.time(),
            timeout=timeout
        )
        self.signal_waiters[correlation_id] = waiter
        
        try:
            await self.broadcaster.emit_simple_signal(
                signal_type=signal_type,
                data=data,
                correlation_id=correlation_id
            )
            
            response = await asyncio.wait_for(waiter.future, timeout=timeout)
            return response
            
        except asyncio.TimeoutError:
            self.logger.error(f"⏱️ [Coordinator] 信号等待超时: {correlation_id}, 超时时间: {timeout}s")
            self.logger.error(f"⏱️ [Coordinator] 期待的响应类型: {self.expected_responses.get(signal_type, '未知')}")
            return None
        except Exception as e:
            self.logger.error(f"❌ [Coordinator] 信号发送或等待异常: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            if correlation_id in self.signal_waiters:
                del self.signal_waiters[correlation_id]
    
    def notify_response(self, signal: Signal):
        """通知收到响应信号
        
        Args:
            signal: 响应信号
        """
        correlation_id = signal.correlation_id
        
        if correlation_id in self.signal_waiters:
            waiter = self.signal_waiters[correlation_id]
            waiter.complete(signal)
            self.logger.debug(f"响应信号已通知: {correlation_id}")
        else:
            self.logger.debug(f"收到未追踪的响应信号: {correlation_id}")
    
    
    def init_table_tracker(
        self,
        table_name: str,
        total_batches: int = 1,
        is_relation_table: bool = False,
        max_iterations: int = 1
    ) -> TableStateTracker:
        """初始化表追踪器
        
        Args:
            table_name: 表名
            total_batches: 总批次数
            is_relation_table: 是否为关系表
            max_iterations: 最大验证-修复迭代次数（默认1次，即验证修复一次）
            
        Returns:
            表状态追踪器
        """
        if table_name in self.table_trackers:
            self.logger.warning(f"表追踪器已存在: {table_name}，将被重置")
        
        tracker = TableStateTracker(
            table_name=table_name,
            total_batches=total_batches,
            is_relation_table=is_relation_table,
            max_iterations=max_iterations
        )
        
        self.table_trackers[table_name] = tracker
        self.logger.info(
            f"初始化表追踪器: {table_name}, "
            f"batches={total_batches}, "
            f"relation={is_relation_table}"
        )
        
        return tracker
    
    def get_table_tracker(self, table_name: str) -> Optional[TableStateTracker]:
        """获取表追踪器"""
        return self.table_trackers.get(table_name)
    
    def update_table_state(
        self,
        table_name: str,
        new_state: TableLifecycleState,
        force: bool = False
    ) -> bool:
        """更新表状态
        
        Args:
            table_name: 表名
            new_state: 新状态
            force: 是否强制更新（跳过状态转换检查）
            
        Returns:
            是否更新成功
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            self.logger.error(f"表追踪器不存在: {table_name}")
            return False
        
        if force:
            old_state = tracker.current_state
            tracker.current_state = new_state
            self.logger.info(f"强制更新表状态: {table_name} {old_state.value} -> {new_state.value}")
            return True
        
        if tracker.transition_to(new_state):
            self.logger.info(f"表状态转换: {table_name} -> {new_state.value}")
            return True
        else:
            self.logger.warning(
                f"无效的状态转换: {table_name} "
                f"{tracker.current_state.value} -> {new_state.value}"
            )
            return False
    
    def is_table_in_state(self, table_name: str, state: TableLifecycleState) -> bool:
        """检查表是否处于指定状态"""
        tracker = self.get_table_tracker(table_name)
        return tracker and tracker.current_state == state
    
    def wait_table_state(
        self,
        table_name: str,
        target_states: List[TableLifecycleState],
        timeout: float = 300.0
    ) -> asyncio.Future:
        """等待表达到目标状态（异步）
        
        Args:
            table_name: 表名
            target_states: 目标状态列表
            timeout: 超时时间
            
        Returns:
            Future对象
        """
        async def _wait():
            start_time = time.time()
            while time.time() - start_time < timeout:
                tracker = self.get_table_tracker(table_name)
                if tracker and tracker.current_state in target_states:
                    return tracker.current_state
                await asyncio.sleep(0.5)
            raise TimeoutError(f"等待表状态超时: {table_name}")
        
        return asyncio.create_task(_wait())
    
    
    @staticmethod
    def _get_batch_key(batch_index: int, is_warm_start: bool = False) -> str:
        """生成batch的唯一键
        
        Args:
            batch_index: 原始batch索引
            is_warm_start: 是否为warm start batch
            
        Returns:
            batch键，格式为 "batch_1" 或 "warm_start_batch_1"
        """
        prefix = "warm_start_" if is_warm_start else ""
        return f"{prefix}batch_{batch_index}"
    
    def init_batch_tracker(
        self,
        table_name: str,
        batch_index: int,
        total_batches: int,
        is_warm_start: bool = False
    ) -> BatchTracker:
        """初始化batch追踪器
        
        Args:
            table_name: 表名
            batch_index: 原始batch索引
            total_batches: 总batch数
            is_warm_start: 是否为warm start batch
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            tracker = self.init_table_tracker(table_name, total_batches)
        
        batch_key = self._get_batch_key(batch_index, is_warm_start)
        batch_tracker = BatchTracker(
            table_name=table_name,
            batch_index=batch_index,
            total_batches=total_batches,
            is_warm_start=is_warm_start
        )
        
        tracker.batch_trackers[batch_key] = batch_tracker
        warm_start_label = " (warm start)" if is_warm_start else ""
        self.logger.debug(f"初始化batch追踪器: {table_name} batch {batch_index}/{total_batches}{warm_start_label}")
        
        return batch_tracker
    
    def update_batch_state(
        self,
        table_name: str,
        batch_index: int,
        state: str,
        **kwargs
    ):
        """更新batch状态
        
        Args:
            table_name: 表名
            batch_index: batch索引
            state: 新状态
            **kwargs: 其他属性（snapshot, violations, fixes等）
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            self.logger.error(f"表追踪器不存在: {table_name}")
            return
        
        is_warm_start = kwargs.pop('is_warm_start', False)
        batch_key = self._get_batch_key(batch_index, is_warm_start)
        
        if batch_key not in tracker.batch_trackers:
            self.logger.warning(f"Batch追踪器不存在: {table_name} {batch_key}")
            return
        
        batch_tracker = tracker.batch_trackers[batch_key]
        batch_tracker.state = state
        
        for key, value in kwargs.items():
            if hasattr(batch_tracker, key):
                setattr(batch_tracker, key, value)
        
        self.logger.debug(f"更新batch状态: {table_name} batch {batch_index} -> {state}")
    
    def mark_batch_completed(self, table_name: str, batch_index: int, is_warm_start: bool = False):
        """标记batch完成（仅标记为completed，不增加计数）
        
        注意：这个方法只是标记batch状态为completed，
        真正的完成计数应该在batch达到final状态时才增加（由mark_batch_final处理）
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            return
        
        self.update_batch_state(table_name, batch_index, "completed", is_warm_start=is_warm_start)
        
        self.logger.debug(
            f"Batch标记为completed: {table_name} batch {batch_index}"
        )
    
    def all_batches_completed(self, table_name: str) -> bool:
        """检查表的所有batch是否都已完成"""
        tracker = self.get_table_tracker(table_name)
        return tracker and tracker.all_batches_completed()
    
    async def wait_all_batches(self, table_name: str, timeout: float = 600.0) -> bool:
        """等待表的所有batch完成
        
        Args:
            table_name: 表名
            timeout: 超时时间
            
        Returns:
            是否所有batch都完成
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if self.all_batches_completed(table_name):
                return True
            await asyncio.sleep(0.5)
        
        self.logger.warning(f"等待batch完成超时: {table_name}")
        return False
    
    
    def get_all_tables_status(self) -> Dict[str, str]:
        """获取所有表的状态"""
        return {
            table_name: tracker.current_state.value
            for table_name, tracker in self.table_trackers.items()
        }
    
    def all_tables_final(self) -> bool:
        """检查是否所有表都已完成"""
        if not self.table_trackers:
            return False
        
        return all(
            tracker.current_state == TableLifecycleState.FINAL
            for tracker in self.table_trackers.values()
        )
    
    async def wait_all_tables_final(self, timeout: float = 1800.0) -> bool:
        """等待所有表完成
        
        Args:
            timeout: 超时时间
            
        Returns:
            是否所有表都完成
        """
        start_time = time.time()
        last_log_time = 0
        
        while time.time() - start_time < timeout:
            if self.all_tables_final():
                self.logger.info("所有表处理完成")
                return True
            
            elapsed = time.time() - start_time
            if elapsed - last_log_time >= 10:
                status = self.get_all_tables_status()
                pending = [k for k, v in status.items() if v != 'final']
                self.logger.info(
                    f"等待表完成: {len(pending)}/{len(status)} 未完成, "
                    f"已等待 {elapsed:.0f}s"
                )
                last_log_time = elapsed
            
            await asyncio.sleep(1.0)
        
        status = self.get_all_tables_status()
        pending = {k: v for k, v in status.items() if v != 'final'}
        self.logger.warning(f"等待所有表完成超时: {pending}")
        return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {
            'total_tables': len(self.table_trackers),
            'total_waiters': len(self.signal_waiters),
            'tables_by_state': defaultdict(int),
            'total_batches': 0,
            'completed_batches': 0
        }
        
        for tracker in self.table_trackers.values():
            stats['tables_by_state'][tracker.current_state.value] += 1
            stats['total_batches'] += tracker.total_batches
            stats['completed_batches'] += tracker.completed_batches
        
        return dict(stats)
    
    
    def can_verify_fix_iterate(self, table_name: str, batch_index: Optional[int] = None) -> bool:
        """检查是否可以继续验证-修复循环
        
        Args:
            table_name: 表名
            batch_index: batch索引（None 表示表级）
            
        Returns:
            是否可以继续迭代
        """
        if batch_index is not None:
            self.logger.info(f'🔍 [循环控制] Batch级检查: 表={table_name}, batch_index={batch_index}')
            tracker = self.get_table_tracker(table_name)
            batch_key = self._get_batch_key(batch_index, is_warm_start=False)
            
            if not tracker:
                self.logger.warning(f'⚠️ [循环控制] 表追踪器不存在: {table_name}')
                return False
            
            if batch_key not in tracker.batch_trackers:
                self.logger.warning(
                    f'⚠️ [循环控制] Batch追踪器不存在: {table_name} batch_key={batch_key}, '
                    f'现有keys={list(tracker.batch_trackers.keys())}'
                )
                return False
            
            batch_tracker = tracker.batch_trackers[batch_key]
            can_iterate = batch_tracker.verification_round < batch_tracker.max_verification_rounds
            
            self.logger.info(
                f'📊 [循环控制] Batch级: 轮次={batch_tracker.verification_round}/{batch_tracker.max_verification_rounds}, '
                f'可继续={can_iterate}'
            )
            
            if not can_iterate:
                self.logger.warning(
                    f'⚠️ Batch {batch_index} 已达到最大验证-修复轮次 '
                    f'({batch_tracker.verification_round}/{batch_tracker.max_verification_rounds})'
                )
            
            return can_iterate
        else:
            self.logger.info(f'🔍 [循环控制] 表级检查: {table_name}')
            tracker = self.get_table_tracker(table_name)
            if not tracker:
                self.logger.warning(f'⚠️ [循环控制] 表追踪器不存在: {table_name}')
                return False
            
            can_iterate = tracker.verify_fix_iteration < tracker.max_iterations
            
            self.logger.info(
                f'📊 [循环控制] 表级: 轮次={tracker.verify_fix_iteration}/{tracker.max_iterations}, '
                f'可继续={can_iterate}'
            )
            
            if not can_iterate:
                self.logger.warning(
                    f'⚠️ 表 {table_name} 已达到最大验证-修复轮次 '
                    f'({tracker.verify_fix_iteration}/{tracker.max_iterations})'
                )
            
            return can_iterate
    
    def increment_verify_fix_iteration(self, table_name: str, batch_index: Optional[int] = None) -> int:
        """增加验证-修复迭代计数
        
        Args:
            table_name: 表名
            batch_index: batch索引（None 表示表级）
            
        Returns:
            当前迭代次数（增加后）
        """
        if batch_index is not None:
            tracker = self.get_table_tracker(table_name)
            batch_key = self._get_batch_key(batch_index, is_warm_start=False)
            if not tracker or batch_key not in tracker.batch_trackers:
                return 0
            
            batch_tracker = tracker.batch_trackers[batch_key]
            batch_tracker.verification_round += 1
            
            self.logger.info(
                f'🔄 Batch {batch_index} 验证-修复循环第 '
                f'{batch_tracker.verification_round}/{batch_tracker.max_verification_rounds} 轮'
            )
            
            return batch_tracker.verification_round
        else:
            tracker = self.get_table_tracker(table_name)
            if not tracker:
                return 0
            
            tracker.verify_fix_iteration += 1
            
            self.logger.info(
                f'🔄 表 {table_name} 验证-修复循环第 '
                f'{tracker.verify_fix_iteration}/{tracker.max_iterations} 轮'
            )
            
            return tracker.verify_fix_iteration
    
    def can_warm_start(self, table_name: str, batch_index: Optional[int] = None) -> bool:
        """检查是否可以执行 Warm Start 重提取
        
        Args:
            table_name: 表名
            batch_index: batch索引（None 表示表级）
            
        Returns:
            是否可以执行 Warm Start
        """
        if batch_index is not None:
            tracker = self.get_table_tracker(table_name)
            batch_key = self._get_batch_key(batch_index, is_warm_start=False)
            if not tracker or batch_key not in tracker.batch_trackers:
                return False
            
            batch_tracker = tracker.batch_trackers[batch_key]
            can_warm = batch_tracker.warm_start_count < batch_tracker.max_warm_start_attempts
            
            if not can_warm:
                self.logger.warning(
                    f'⚠️ Batch {batch_index} 已达到最大 Warm Start 尝试次数 '
                    f'({batch_tracker.warm_start_count}/{batch_tracker.max_warm_start_attempts})'
                )
            
            return can_warm
        else:
            tracker = self.get_table_tracker(table_name)
            if not tracker:
                return False
            
            can_warm = not tracker.warm_start_attempted
            
            if not can_warm:
                self.logger.warning(f'⚠️ 表 {table_name} 已尝试过 Warm Start')
            
            return can_warm
    
    def increment_warm_start_attempts(self, table_name: str, batch_index: Optional[int] = None) -> int:
        """增加 Warm Start 尝试次数
        
        Args:
            table_name: 表名
            batch_index: batch索引（None 表示表级）
            
        Returns:
            当前尝试次数（增加后）
        """
        if batch_index is not None:
            tracker = self.get_table_tracker(table_name)
            batch_key = self._get_batch_key(batch_index, is_warm_start=False)
            if not tracker or batch_key not in tracker.batch_trackers:
                return 0
            
            batch_tracker = tracker.batch_trackers[batch_key]
            batch_tracker.warm_start_count += 1
            
            self.logger.info(
                f'🔥 Batch {batch_index} Warm Start 尝试 '
                f'{batch_tracker.warm_start_count}/{batch_tracker.max_warm_start_attempts}'
            )
            
            return batch_tracker.warm_start_count
        else:
            tracker = self.get_table_tracker(table_name)
            if not tracker:
                return 0
            
            tracker.warm_start_attempted = True
            self.logger.info(f'🔥 表 {table_name} 开始 Warm Start 尝试')
            
            return 1
    
    
    
    def can_verify(self, table_name: str) -> bool:
        """检查是否可以继续执行验证
        
        Args:
            table_name: 表名
            
        Returns:
            是否可以继续验证
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            self.logger.warning(f"表追踪器不存在: {table_name}")
            return False
        
        if tracker.verification_round >= tracker.max_verification_rounds:
            self.logger.warning(
                f'⚠️ 表 {table_name} 已完成 {tracker.verification_round} 轮验证-修复循环，'
                f'达到最大轮次限制 ({tracker.max_verification_rounds})，'
                f'强制终止以避免无限循环'
            )
            return False
        
        return True
    
    def increment_verification_round(self, table_name: str) -> int:
        """增加验证轮次
        
        Args:
            table_name: 表名
            
        Returns:
            当前轮次（增加后）
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            self.logger.error(f"表追踪器不存在: {table_name}")
            return 0
        
        tracker.verification_round += 1
        self.logger.info(
            f'🔄 表 {table_name} 验证-修复循环第 '
            f'{tracker.verification_round}/{tracker.max_verification_rounds} 轮'
        )
        
        return tracker.verification_round
    
    
    def get_verification_round(self, table_name: str) -> int:
        """获取验证轮次"""
        tracker = self.get_table_tracker(table_name)
        return tracker.verification_round if tracker else 0
    
    
    def reset_verification_round(self, table_name: str):
        """重置验证轮次（用于特殊情况）"""
        tracker = self.get_table_tracker(table_name)
        if tracker:
            tracker.verification_round = 0
            self.logger.info(f"重置表 {table_name} 的验证轮次")
    
    
    def can_batch_warm_start(self, table_name: str, batch_index: int) -> bool:
        """检查指定batch是否可以继续执行 warm start
        
        Args:
            table_name: 表名
            batch_index: batch索引
            
        Returns:
            是否可以继续 warm start
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            self.logger.warning(f"表追踪器不存在: {table_name}")
            return False
        
        batch_key = self._get_batch_key(batch_index, is_warm_start=False)
        if batch_key not in tracker.batch_trackers:
            self.logger.warning(f"Batch追踪器不存在: {table_name} batch {batch_index}")
            return False
        
        batch_tracker = tracker.batch_trackers[batch_key]
        
        if batch_tracker.warm_start_count >= batch_tracker.max_warm_start_attempts:
            self.logger.warning(
                f'⚠️ 表 {table_name} batch {batch_index} 已达到最大 warm start 尝试次数 '
                f'({batch_tracker.warm_start_count}/{batch_tracker.max_warm_start_attempts})，'
                f'拒绝继续执行以避免无限循环'
            )
            return False
        
        return True
    
    def increment_batch_warm_start_count(self, table_name: str, batch_index: int) -> int:
        """增加指定batch的 warm start 计数
        
        Args:
            table_name: 表名
            batch_index: batch索引
            
        Returns:
            当前计数（增加后）
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            self.logger.error(f"表追踪器不存在: {table_name}")
            return 0
        
        batch_key = self._get_batch_key(batch_index, is_warm_start=False)
        if batch_key not in tracker.batch_trackers:
            self.logger.error(f"Batch追踪器不存在: {table_name} batch {batch_index}")
            return 0
        
        batch_tracker = tracker.batch_trackers[batch_key]
        batch_tracker.warm_start_count += 1
        
        self.logger.info(
            f'🔄 表 {table_name}  warm start 尝试 '
            f'{batch_tracker.warm_start_count}/{batch_tracker.max_warm_start_attempts}'
        )
        
        return batch_tracker.warm_start_count
    
    def get_batch_warm_start_count(self, table_name: str, batch_index: int) -> int:
        """获取指定batch的 warm start 计数"""
        tracker = self.get_table_tracker(table_name)
        batch_key = self._get_batch_key(batch_index, is_warm_start=False)
        if not tracker or batch_key not in tracker.batch_trackers:
            return 0
        return tracker.batch_trackers[batch_key].warm_start_count
    
    
    def can_batch_verify(self, table_name: str, batch_index: int) -> bool:
        """检查指定batch是否可以继续执行验证
        
        Args:
            table_name: 表名
            batch_index: batch索引
            
        Returns:
            是否可以继续验证
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            self.logger.warning(f"表追踪器不存在: {table_name}")
            return False
        
        batch_key = self._get_batch_key(batch_index, is_warm_start=False)
        if batch_key not in tracker.batch_trackers:
            self.logger.warning(f"Batch追踪器不存在: {table_name} batch {batch_index}")
            return False
        
        batch_tracker = tracker.batch_trackers[batch_key]
        
        if batch_tracker.verification_round >= batch_tracker.max_verification_rounds:
            self.logger.warning(
                f'⚠️ 表 {table_name} batch {batch_index} 已达到最大验证轮次 '
                f'({batch_tracker.verification_round}/{batch_tracker.max_verification_rounds})，'
                f'强制终止以避免无限循环'
            )
            return False
        
        return True
    
    def increment_batch_verification_round(self, table_name: str, batch_index: int) -> int:
        """增加指定batch的验证轮次
        
        Args:
            table_name: 表名
            batch_index: batch索引
            
        Returns:
            当前轮次（增加后）
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            self.logger.error(f"表追踪器不存在: {table_name}")
            return 0
        
        batch_key = self._get_batch_key(batch_index, is_warm_start=False)
        if batch_key not in tracker.batch_trackers:
            self.logger.error(f"Batch追踪器不存在: {table_name} batch {batch_index}")
            return 0
        
        batch_tracker = tracker.batch_trackers[batch_key]
        batch_tracker.verification_round += 1
        
        self.logger.info(
            f'🔄 表 {table_name} batch {batch_index} 验证-修复循环第 '
            f'{batch_tracker.verification_round}/{batch_tracker.max_verification_rounds} 轮'
        )
        
        return batch_tracker.verification_round
    
    def mark_batch_final(self, table_name: str, batch_index: int, is_warm_start: bool = False):
        """标记batch为final状态（处理完成）
        
        Args:
            table_name: 表名
            batch_index: 原始batch索引
            is_warm_start: 是否为warm start batch
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            return
        
        batch_key = self._get_batch_key(batch_index, is_warm_start)
        
        if batch_key in tracker.batch_trackers:
            batch_tracker = tracker.batch_trackers[batch_key]
            was_final = (batch_tracker.state == "final")
            
            self.update_batch_state(table_name, batch_index, "final", is_warm_start=is_warm_start)
            
            if not was_final:
                if not is_warm_start:
                    tracker.completed_batches += 1
                warm_start_label = " (warm start)" if is_warm_start else ""
                self.logger.info(
                    f"✅ Batch已完成: {table_name} batch {batch_index}{warm_start_label}, "
                    f"进度: {tracker.completed_batches}/{tracker.total_batches}"
                )
                
                if tracker.all_batches_completed():
                    self.logger.info(f"🎉 所有batch已完成: {table_name}")
                    if tracker.current_state != TableLifecycleState.FINAL:
                        self.logger.info(f"📊 所有batch已完成，将表 {table_name} 状态更新为 FINAL")
                        self.update_table_state(table_name, TableLifecycleState.FINAL)
            else:
                self.logger.debug(
                    f"Batch {batch_index} 已经是final状态，跳过重复计数"
                )
        else:
            self.logger.warning(f"Batch追踪器不存在: {table_name} batch {batch_index}")
    
    def is_batch_final(self, table_name: str, batch_index: int, is_warm_start: bool = False) -> bool:
        """检查batch是否已达到final状态
        
        Args:
            table_name: 表名
            batch_index: 原始batch索引
            is_warm_start: 是否为warm start batch
            
        Returns:
            是否已达到final状态
        """
        tracker = self.get_table_tracker(table_name)
        if not tracker:
            return False
        
        batch_key = self._get_batch_key(batch_index, is_warm_start)
        if batch_key not in tracker.batch_trackers:
            return False
        
        batch_tracker = tracker.batch_trackers[batch_key]
        return batch_tracker.state == "final"
    
    async def wait_batch_final(self, table_name: str, batch_index: int, is_warm_start: bool = False, timeout: float = 600.0) -> bool:
        """等待batch达到final状态
        
        Args:
            table_name: 表名
            batch_index: 原始batch索引
            is_warm_start: 是否为warm start batch
            timeout: 超时时间（秒）
            
        Returns:
            是否成功达到final状态（False表示超时）
        """
        if self.is_table_in_state(table_name, TableLifecycleState.FINAL):
            self.logger.info(f"✅ 表 {table_name} 已经是 FINAL 状态，无需等待 batch {batch_index}")
            if not self.is_batch_final(table_name, batch_index, is_warm_start):
                self.logger.info(f"📌 同时标记 Batch {batch_index} (warm_start={is_warm_start}) 为 final")
                self.mark_batch_final(table_name, batch_index, is_warm_start)
            return True
        
        start_time = time.time()
        last_log_time = 0
        
        warm_start_label = " (warm start)" if is_warm_start else ""
        self.logger.info(f"⏳ 等待 batch {batch_index}{warm_start_label} 完成处理: {table_name}")
        
        while time.time() - start_time < timeout:
            if self.is_batch_final(table_name, batch_index, is_warm_start):
                elapsed = time.time() - start_time
                self.logger.info(f"✅ Batch {batch_index}{warm_start_label} 已完成: {table_name}, 耗时 {elapsed:.1f}s")
                return True
            
            elapsed = time.time() - start_time
            if elapsed - last_log_time >= 10:
                tracker = self.get_table_tracker(table_name)
                batch_key = self._get_batch_key(batch_index, is_warm_start)
                if tracker and batch_key in tracker.batch_trackers:
                    batch_tracker = tracker.batch_trackers[batch_key]
                    self.logger.info(
                        f"⏳ Batch {batch_index}{warm_start_label} 处理中: {table_name}, "
                        f"状态={batch_tracker.state}, "
                        f"已等待 {elapsed:.0f}s"
                    )
                last_log_time = elapsed
            
            await asyncio.sleep(0.5)
        
        if is_warm_start:
            self.logger.warning(
                f"⚠️ Warm start batch {batch_index} 超时: {table_name}, "
                f"已等待 {timeout}s，将跳过此batch"
            )
        else:
            self.logger.warning(
                f"⚠️ 等待 batch {batch_index} 超时: {table_name}, "
                f"超时时间 {timeout}s"
            )
        return False
    


