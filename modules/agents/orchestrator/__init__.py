"""统一的信号驱动协调器模块"""

from .context import Doc2DBContext, Doc2DBProcessingState, create_doc2db_context
from .unified_orchestrator import (
    UnifiedSignalDrivenOrchestrator,
    create_unified_orchestrator,
    run_unified_signal_orchestrator
)
from .signal_components import SignalComponentManager, SignalAwareExtractor, SignalAwareVerifier, SignalAwareFixer
from .signal_handlers import OrchestratorSignalHandler, SignalHandlerMixin, TableProcessingWaiter
from .utils import SchemaUtils, DocumentUtils, SignalUtils, StepUtils, DataTransferUtils, DebugUtils
from .memory_operations import MemoryOperationsMixin, MemoryQueryMixin

Doc2DBOrchestrator = UnifiedSignalDrivenOrchestrator

def create_signal_driven_orchestrator(max_iterations: int = 3) -> UnifiedSignalDrivenOrchestrator:
    """创建统一的信号驱动协调器"""
    return create_unified_orchestrator(max_iterations=max_iterations)

def create_doc2db_context_with_signals(run_id: str, table_name: str, schema: dict, 
                                     documents: list, nl_prompt: str, io_manager,
                                     user_query: str = "", model: str = "gpt-4o",
                                     document_mode: str = "single") -> Doc2DBContext:
    """创建支持信号系统的Doc2DB上下文
    
    Args:
        document_mode: 文档处理模式，"single" (单文档逐一处理) 或 "multi" (多文档拼接处理)
    """
    return create_doc2db_context(
        run_id=run_id,
        table_name=table_name, 
        schema=schema,
        documents=documents,
        nl_prompt=nl_prompt,
        io_manager=io_manager,
        user_query=user_query,
        model=model,
        document_mode=document_mode
    )

async def run_orchestrator(context: Doc2DBContext) -> Doc2DBContext:
    """运行统一的信号驱动协调器"""
    return await run_unified_signal_orchestrator(context)

__all__ = [
    'UnifiedSignalDrivenOrchestrator', 'Doc2DBOrchestrator', 'Doc2DBContext', 'Doc2DBProcessingState',
    'SignalComponentManager', 'SignalAwareExtractor', 'SignalAwareVerifier', 'SignalAwareFixer',
    'create_unified_orchestrator', 'create_signal_driven_orchestrator',
    'create_doc2db_context', 'create_doc2db_context_with_signals',
    'run_unified_signal_orchestrator', 'run_orchestrator'
]
