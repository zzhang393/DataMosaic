"""信号系统核心 - 广播机制实现"""
import asyncio
import logging
import threading
import uuid
from enum import Enum
from typing import Dict, List, Any, Callable, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import weakref


class SignalType(Enum):
    """信号类型枚举"""
    SYSTEM_START = "system_start"
    SYSTEM_STOP = "system_stop" 
    SYSTEM_ERROR = "system_error"
    
    PROCESSING_START = "processing_start"
    PROCESSING_COMPLETE = "processing_complete"
    PROCESSING_ERROR = "processing_error"
    
    ANALYSIS_START = "analysis_start"
    ANALYSIS_COMPLETE = "analysis_complete"
    ANALYSIS_ERROR = "analysis_error"
    
    EXTRACTION_REQUEST = "extraction_request"
    EXTRACTION_START = "extraction_start"
    EXTRACTION_PROGRESS = "extraction_progress"
    EXTRACTION_COMPLETE = "extraction_complete"
    EXTRACTION_ERROR = "extraction_error"
    
    VERIFICATION_REQUEST = "verification_request"
    VERIFICATION_START = "verification_start"
    VERIFICATION_PROGRESS = "verification_progress"
    VERIFICATION_COMPLETE = "verification_complete"
    VERIFICATION_ERROR = "verification_error"
    
    AUTO_BASIC_VERIFICATION_COMPLETE = "auto_basic_verification_complete"
    
    MULTI_TABLE_VERIFICATION_START = "multi_table_verification_start"
    MULTI_TABLE_VERIFICATION_COMPLETE = "multi_table_verification_complete"
    MULTI_TABLE_VERIFICATION_ERROR = "multi_table_verification_error"
    
    FIXING_REQUEST = "fixing_request"
    FIXING_START = "fixing_start"
    FIXING_PROGRESS = "fixing_progress"
    FIXING_COMPLETE = "fixing_complete"
    FIXING_ERROR = "fixing_error"
    
    WARM_START_REQUEST = "warm_start_request"
    WARM_START_START = "warm_start_start"
    WARM_START_COMPLETE = "warm_start_complete"
    WARM_START_ERROR = "warm_start_error"
    
    STATE_CHANGED = "state_changed"
    PROGRESS_UPDATE = "progress_update"
    PROCESS_TERMINATED = "process_terminated"  # 流程终止信号
    
    DECISION_REQUEST = "decision_request"
    DECISION_RESPONSE = "decision_response"
    
    RESOURCE_AVAILABLE = "resource_available"
    RESOURCE_UNAVAILABLE = "resource_unavailable"


@dataclass
class Signal:
    """信号数据结构"""
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    signal_type: SignalType = SignalType.SYSTEM_START
    sender: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    data: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0  # 0=normal, 1=high, 2=urgent
    broadcast: bool = True  # 是否广播给所有组件
    target_components: List[str] = field(default_factory=list)  # 特定目标组件
    correlation_id: Optional[str] = None  # 用于关联请求和响应
    retry_count: int = 0
    max_retries: int = 3
    
    memory_ref: Optional[Dict[str, Any]] = None  # MemoryReference.to_dict()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'signal_id': self.signal_id,
            'signal_type': self.signal_type.value,
            'sender': self.sender,
            'timestamp': self.timestamp.isoformat(),
            'data': self.data,
            'priority': self.priority,
            'broadcast': self.broadcast,
            'target_components': self.target_components,
            'correlation_id': self.correlation_id,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'memory_ref': self.memory_ref
        }


class SignalHandler:
    """信号处理器接口"""
    
    def can_handle(self, signal: Signal) -> bool:
        """检查是否能处理该信号"""
        raise NotImplementedError
    
    async def handle_signal(self, signal: Signal) -> Optional[Signal]:
        """处理信号，可选返回响应信号"""
        raise NotImplementedError


class SignalReceiver:
    """信号接收器"""
    
    def __init__(self, component_name: str):
        self.component_name = component_name
        self.handlers: List[SignalHandler] = []
        self.logger = logging.getLogger(f'signal.receiver.{component_name}')
        self._running = False
        self._signal_queue = asyncio.Queue()
        
    def add_handler(self, handler: SignalHandler):
        """添加信号处理器"""
        self.handlers.append(handler)
        self.logger.debug(f"添加处理器: {handler.__class__.__name__}")
    
    def remove_handler(self, handler: SignalHandler):
        """移除信号处理器"""
        if handler in self.handlers:
            self.handlers.remove(handler)
    
    async def receive_signal(self, signal: Signal):
        """接收信号并加入处理队列"""
        if signal.signal_type.value == 'extraction_request':
            context = signal.data.get('context', {})
            table_name = context.get('table_name', 'N/A')
            text_contents = context.get('text_contents', [])
        await self._signal_queue.put(signal)
    
    async def start_processing(self):
        """开始处理信号 - 支持并发处理多个信号"""
        self._running = True
        self.logger.info(f"信号接收器 {self.component_name} 开始处理")
        
        processing_tasks = set()
        
        while self._running:
            try:
                signal = await asyncio.wait_for(self._signal_queue.get(), timeout=1.0)
                
                
                task = asyncio.create_task(self._process_signal_async(signal))
                processing_tasks.add(task)
                task.add_done_callback(processing_tasks.discard)
                
                self._signal_queue.task_done()
                
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"接收信号失败: {e}")
        
        if processing_tasks:
            self.logger.info(f"等待 {len(processing_tasks)} 个处理任务完成...")
            await asyncio.gather(*processing_tasks, return_exceptions=True)
    
    async def _process_signal_async(self, signal: Signal):
        """异步处理单个信号（作为后台任务运行）"""
        try:
            await self._process_signal(signal)
        except Exception as e:
            self.logger.error(f"处理信号异常: {signal.signal_type.value}, 错误: {e}")
    
    async def _process_signal(self, signal: Signal):
        """处理单个信号"""
        self.logger.debug(f"接收信号: {signal.signal_type.value} from {signal.sender}")
        
        if not signal.broadcast and signal.target_components:
            if self.component_name not in signal.target_components:
                return
        
        for handler in self.handlers:
            try:
                if handler.can_handle(signal):
                    response_signal = await handler.handle_signal(signal)
                    
                    if response_signal:
                        from .core import BroadcastHub
                        hub = BroadcastHub.get_instance()
                        await hub.broadcast_signal(response_signal)
                    break
                    
            except Exception as e:
                self.logger.error(f"处理器 {handler.__class__.__name__} 处理信号失败: {e}")
    
    def stop_processing(self):
        """停止处理信号"""
        self._running = False
        self.logger.info(f"信号接收器 {self.component_name} 停止处理")


class SignalBroadcaster:
    """信号发射器"""
    
    def __init__(self, component_name: str):
        self.component_name = component_name
        self.logger = logging.getLogger(f'signal.broadcaster.{component_name}')
        self._hub = None
    
    def set_hub(self, hub):
        """设置广播中心"""
        self._hub = hub
    
    async def emit_signal(self, signal: Signal):
        """发射信号"""
        signal.sender = self.component_name
        
        if self._hub is None:
            self._hub = BroadcastHub.get_instance()
        
        if self._hub:
            await self._hub.broadcast_signal(signal)
            self.logger.debug(f"发射信号: {signal.signal_type.value}")
        else:
            self.logger.error("未设置广播中心，无法发射信号")
    
    async def emit_simple_signal(self, signal_type: SignalType, data: Dict[str, Any] = None, 
                                priority: int = 0, broadcast: bool = True, 
                                target_components: List[str] = None,
                                correlation_id: str = None):
        """快速发射简单信号"""
        signal = Signal(
            signal_type=signal_type,
            data=data or {},
            priority=priority,
            broadcast=broadcast,
            target_components=target_components or [],
            correlation_id=correlation_id
        )
        await self.emit_signal(signal)
    
    async def emit_request_signal(self, signal_type: SignalType, data: Dict[str, Any] = None,
                                 target_components: List[str] = None) -> str:
        """发射请求信号并返回关联ID"""
        correlation_id = str(uuid.uuid4())
        signal = Signal(
            signal_type=signal_type,
            data=data or {},
            broadcast=False if target_components else True,
            target_components=target_components or [],
            correlation_id=correlation_id
        )
        await self.emit_signal(signal)
        return correlation_id
    
    async def emit_response_signal(self, original_signal: Signal, response_type: SignalType, 
                                  data: Dict[str, Any] = None):
        """发射响应信号"""
        response_signal = Signal(
            signal_type=response_type,
            data=data or {},
            correlation_id=original_signal.correlation_id,
            broadcast=False,
            target_components=[original_signal.sender]
        )
        await self.emit_signal(response_signal)
    
    async def emit_memory_signal(self, signal_type: SignalType, memory_ref: Dict[str, Any], 
                                data: Dict[str, Any] = None, priority: int = 0, 
                                broadcast: bool = True, target_components: List[str] = None,
                                correlation_id: str = None):
        """发射带内存引用的信号"""
        signal = Signal(
            signal_type=signal_type,
            data=data or {},
            memory_ref=memory_ref,
            priority=priority,
            broadcast=broadcast,
            target_components=target_components or [],
            correlation_id=correlation_id
        )
        await self.emit_signal(signal)


class BroadcastHub:
    """广播中心 - 负责信号的分发和路由"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(BroadcastHub, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.receivers: Dict[str, SignalReceiver] = {}
            self.logger = logging.getLogger('signal.hub')
            self._signal_history: List[Signal] = []
            self._max_history = 1000
            self._running = False
            self._initialized = True
    
    @classmethod
    def get_instance(cls):
        """获取单例实例"""
        return cls()
    
    def register_receiver(self, receiver: SignalReceiver):
        """注册接收器"""
        self.receivers[receiver.component_name] = receiver
        self.logger.info(f"注册接收器: {receiver.component_name}")
    
    def unregister_receiver(self, component_name: str):
        """注销接收器"""
        if component_name in self.receivers:
            del self.receivers[component_name]
            self.logger.info(f"注销接收器: {component_name}")
    
    async def broadcast_signal(self, signal: Signal):
        """广播信号"""
        
        if signal.signal_type.value == 'extraction_request':
            context = signal.data.get('context', {})
            table_name = context.get('table_name', 'N/A')
            text_contents = context.get('text_contents', [])
        
        self._add_to_history(signal)
        
        if signal.broadcast:
            tasks = []
            for receiver_name, receiver in self.receivers.items():
                if receiver_name != signal.sender:  # 不发送给发送者
                    tasks.append(receiver.receive_signal(signal))
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        
        elif signal.target_components:
            tasks = []
            for component_name in signal.target_components:
                if component_name in self.receivers:
                    receiver = self.receivers[component_name]
                    tasks.append(receiver.receive_signal(signal))
                else:
                    self.logger.warning(f"目标组件 {component_name} 未找到")
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    
    def _add_to_history(self, signal: Signal):
        """添加信号到历史记录"""
        self._signal_history.append(signal)
        if len(self._signal_history) > self._max_history:
            self._signal_history.pop(0)
    
    def get_signal_history(self, signal_type: SignalType = None, 
                          sender: str = None, limit: int = 100) -> List[Signal]:
        """获取信号历史"""
        history = self._signal_history[-limit:]
        
        if signal_type:
            history = [s for s in history if s.signal_type == signal_type]
        
        if sender:
            history = [s for s in history if s.sender == sender]
        
        return history
    
    def cleanup_run_signals(self, run_id: str):
        """清理特定 run 的信号历史（避免任务间数据混淆）"""
        original_count = len(self._signal_history)
        self._signal_history = [
            signal for signal in self._signal_history
            if not (signal.data and signal.data.get('run_id') == run_id)
        ]
        removed_count = original_count - len(self._signal_history)
        if removed_count > 0:
            self.logger.info(f"清理 run {run_id} 的信号历史: 移除 {removed_count} 条信号")
    
    def clear_signal_history(self):
        """清空所有信号历史"""
        self._signal_history.clear()
        self.logger.info("已清空所有信号历史")
    
    async def start_all_receivers(self):
        """启动所有接收器"""
        self._running = True
        self.logger.info("启动所有信号接收器")
        
        tasks = []
        for receiver in self.receivers.values():
            task = asyncio.create_task(receiver.start_processing())
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def stop_all_receivers(self):
        """停止所有接收器"""
        self._running = False
        self.logger.info("停止所有信号接收器")
        
        for receiver in self.receivers.values():
            receiver.stop_processing()
    
    def get_component_status(self) -> Dict[str, Any]:
        """获取所有组件状态"""
        status = {}
        for name, receiver in self.receivers.items():
            status[name] = {
                'running': receiver._running,
                'handlers_count': len(receiver.handlers),
                'queue_size': receiver._signal_queue.qsize()
            }
        return status
