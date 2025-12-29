"""信号处理器实现"""
import asyncio
import logging
from typing import Dict, List, Any, Optional, Set
from abc import ABC, abstractmethod
from .core import SignalHandler, Signal, SignalType


class BaseSignalHandler(SignalHandler):
    """基础信号处理器"""
    
    def __init__(self, component_name: str):
        self.component_name = component_name
        self.logger = logging.getLogger(f'signal.handler.{component_name}')
        self.supported_signals: Set[SignalType] = set()
    
    def add_supported_signal(self, signal_type: SignalType):
        """添加支持的信号类型"""
        self.supported_signals.add(signal_type)
    
    def can_handle(self, signal: Signal) -> bool:
        """检查是否能处理该信号"""
        return signal.signal_type in self.supported_signals
    
    async def handle_signal(self, signal: Signal) -> Optional[Signal]:
        """处理信号的模板方法"""
        self.logger.debug(f"处理信号: {signal.signal_type.value}")
        
        try:
            response = await self._process_signal(signal)
            
            self.logger.debug(f"信号处理完成: {signal.signal_type.value}")
            return response
            
        except Exception as e:
            self.logger.error(f"信号处理失败: {signal.signal_type.value}, 错误: {e}")
            
            error_signal = Signal(
                signal_type=SignalType.SYSTEM_ERROR,
                data={
                    'error': str(e),
                    'original_signal': signal.to_dict(),
                    'component': self.component_name
                },
                correlation_id=signal.correlation_id
            )
            return error_signal
    
    @abstractmethod
    async def _process_signal(self, signal: Signal) -> Optional[Signal]:
        """具体的信号处理逻辑"""
        pass


class OrchestratorSignalHandler(BaseSignalHandler):
    """Orchestrator信号处理器"""
    
    def __init__(self, orchestrator):
        super().__init__('orchestrator')
        self.orchestrator = orchestrator
        
        self.add_supported_signal(SignalType.EXTRACTION_COMPLETE)
        self.add_supported_signal(SignalType.EXTRACTION_ERROR)
        self.add_supported_signal(SignalType.VERIFICATION_COMPLETE)
        self.add_supported_signal(SignalType.VERIFICATION_ERROR)
        self.add_supported_signal(SignalType.FIXING_COMPLETE)
        self.add_supported_signal(SignalType.FIXING_ERROR)
        self.add_supported_signal(SignalType.DECISION_RESPONSE)
    
    async def _process_signal(self, signal: Signal) -> Optional[Signal]:
        """处理orchestrator相关信号"""
        
        if signal.signal_type == SignalType.EXTRACTION_COMPLETE:
            return await self._handle_extraction_complete(signal)
            
        elif signal.signal_type == SignalType.VERIFICATION_COMPLETE:
            return await self._handle_verification_complete(signal)
            
        elif signal.signal_type == SignalType.FIXING_COMPLETE:
            return await self._handle_fixing_complete(signal)
            
        elif signal.signal_type in [SignalType.EXTRACTION_ERROR, SignalType.VERIFICATION_ERROR, SignalType.FIXING_ERROR]:
            return await self._handle_component_error(signal)
        
        return None
    
    async def _handle_extraction_complete(self, signal: Signal) -> Optional[Signal]:
        """处理提取完成信号"""
        self.logger.info("数据提取完成，开始验证")
        
        await self.orchestrator.handle_extraction_complete(signal)
        return None  # 由orchestrator内部发送后续信号
    
    async def _handle_verification_complete(self, signal: Signal) -> Optional[Signal]:
        """处理验证完成信号"""
        await self.orchestrator.handle_verification_complete(signal)
        return None  # 由orchestrator内部发送后续信号
    
    async def _handle_fixing_complete(self, signal: Signal) -> Optional[Signal]:
        """处理修复完成信号"""
        await self.orchestrator.handle_fixing_complete(signal)
        return None  # 由orchestrator内部发送后续信号
    
    async def _handle_component_error(self, signal: Signal) -> Optional[Signal]:
        """处理组件错误信号"""
        error_info = signal.data.get('error', 'Unknown error')
        component = signal.sender
        context = signal.data.get('context')
        
        self.logger.error(f"组件 {component} 报告错误: {error_info}")
        
        if signal.signal_type == SignalType.EXTRACTION_ERROR:
            self.logger.error("提取失败，停止后续验证流程")
            
            if hasattr(self, 'orchestrator') and hasattr(self.orchestrator, 'current_context'):
                self.orchestrator.current_context.current_state = 'FAILED'
            
            return Signal(
                signal_type=SignalType.PROCESS_TERMINATED,
                sender='error_handler',
                data={
                    'reason': f'Extraction failed: {error_info}',
                    'original_error': signal.data,
                    'component': component
                },
                correlation_id=signal.correlation_id
            )
        
        elif signal.signal_type == SignalType.VERIFICATION_ERROR:
            self.logger.warning("验证失败，但可以继续处理")
            
        elif signal.signal_type == SignalType.FIXING_ERROR:
            self.logger.warning("修复失败，记录但继续处理")
        
        return None
    
    async def _handle_table_complete(self, signal: Signal) -> Optional[Signal]:
        """处理单表完成"""
        context_data = signal.data.get('context', {})
        
        state_signal = Signal(
            signal_type=SignalType.STATE_CHANGED,
            data={
                'new_state': 'table_completed',
                'context': context_data
            }
        )
        return state_signal


class ExtractorSignalHandler(BaseSignalHandler):
    """Extractor信号处理器 - 使用信号组件适配器"""
    
    def __init__(self, extractor):
        super().__init__('extractor')
        from ..agents.orchestrator.signal_components import SignalAwareExtractor
        self.signal_extractor = SignalAwareExtractor(extractor)
        
        self.add_supported_signal(SignalType.EXTRACTION_REQUEST)
    
    async def _process_signal(self, signal: Signal) -> Optional[Signal]:
        """处理extractor相关信号"""
        
        if signal.signal_type == SignalType.EXTRACTION_REQUEST:
            return await self.signal_extractor.extract_with_signals(signal)
        
        return None


class VerifierSignalHandler(BaseSignalHandler):
    """Verifier信号处理器 - 使用信号组件适配器"""
    
    def __init__(self, verifier):
        super().__init__('verifier')
        from ..agents.orchestrator.signal_components import SignalAwareVerifier
        self.signal_verifier = SignalAwareVerifier(verifier)
        
        self.add_supported_signal(SignalType.VERIFICATION_REQUEST)
    
    async def _process_signal(self, signal: Signal) -> Optional[Signal]:
        """处理verifier相关信号"""
        
        if signal.signal_type == SignalType.VERIFICATION_REQUEST:
            return await self.signal_verifier.verify_with_signals(signal)
        
        return None


class FixerSignalHandler(BaseSignalHandler):
    """Fixer信号处理器 - 使用信号组件适配器"""
    
    def __init__(self, fixer):
        super().__init__('fixer')
        from ..agents.orchestrator.signal_components import SignalAwareFixer
        self.signal_fixer = SignalAwareFixer(fixer)
        
        self.add_supported_signal(SignalType.FIXING_REQUEST)
    
    async def _process_signal(self, signal: Signal) -> Optional[Signal]:
        """处理fixer相关信号"""
        
        if signal.signal_type == SignalType.FIXING_REQUEST:
            return await self.signal_fixer.fix_with_signals(signal)
        
        return None
