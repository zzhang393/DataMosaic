"""统一的信号驱动协调器 - 重构版本"""
import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Any, Optional

from .context import Doc2DBContext, Doc2DBProcessingState, create_doc2db_context
from .signal_handlers import OrchestratorSignalHandler, SignalHandlerMixin, TableProcessingWaiter
from .utils import SchemaUtils, DocumentUtils, SignalUtils, StepUtils, DataTransferUtils, DebugUtils
from .memory_operations import MemoryOperationsMixin, MemoryQueryMixin
from .signal_coordinator import SignalCoordinator, TableLifecycleState

from ...memory import TableSnapshot, Violation, Fix
from .signal_components import SignalComponentManager
from ...signals.core import SignalBroadcaster, SignalReceiver, BroadcastHub, SignalType
from ...utils.orchestrator_logger import OrchestratorLogger


class UnifiedSignalDrivenOrchestrator(SignalHandlerMixin, MemoryOperationsMixin, MemoryQueryMixin):
    """统一的信号驱动协调器 - 专注于信号通信和广播机制"""
    
    def __init__(self, max_iterations: int = 3):
        MemoryOperationsMixin.__init__(self)
        MemoryQueryMixin.__init__(self)
        
        self.max_iterations = max_iterations
        self.logger = logging.getLogger('unified.orchestrator')
        
        self.detailed_logger = None
        
        self.broadcaster = SignalBroadcaster('unified_orchestrator')
        self.receiver = SignalReceiver('unified_orchestrator')
        self.hub = BroadcastHub.get_instance()
        
        self.coordinator = SignalCoordinator(self.broadcaster, default_timeout=600.0)
        
        self.signal_component_manager = None
        
        self.schema_utils = SchemaUtils()
        self.document_utils = DocumentUtils(self.logger)
        self.table_waiter = TableProcessingWaiter(self.logger)
        self.table_waiter.orchestrator = self
        
        self.current_context: Optional[Doc2DBContext] = None
        self.processing_tasks: List[Any] = []
        
        self._setup_basic_signal_system()
        

    
    def _setup_basic_signal_system(self):
        """设置基础信号系统（不包含需要memory_manager的组件）"""
        self.broadcaster.set_hub(self.hub)
        
        self.hub.register_receiver(self.receiver)
        
        self._setup_orchestrator_signal_handlers()
        

    
    def _setup_signal_components(self):
        """设置信号组件（在memory_manager初始化后调用）"""
        if self.signal_component_manager is None:
            self.signal_component_manager = SignalComponentManager(
                memory_manager=self.memory_manager,
                orchestrator=self,
                coordinator=self.coordinator
            )
            
            self.signal_component_manager.setup_receivers(self.hub)
            

    
    def _setup_orchestrator_signal_handlers(self):
        """设置协调器的信号处理器"""
        handler = OrchestratorSignalHandler(self)
        self.receiver.add_handler(handler)
    
    async def process_with_signals(self, context: Doc2DBContext) -> Doc2DBContext:
        """使用信号系统处理Doc2DB流程"""
        self.logger.info(f"启动统一信号处理，运行ID: {context.run_id}")
        
        self.current_context = context
        context.current_state = Doc2DBProcessingState.INIT
        
        context.coordinator = self.coordinator
        
        self.detailed_logger = OrchestratorLogger(context.run_id)
        
        start_step = StepUtils.create_start_step(context)
        context.step_outputs.append(start_step)
        
        try:
            await self.initialize_memory(context)
            
            self._setup_signal_components()
            
            await self._start_signal_processing()
            
            await self._run_signal_driven_pipeline(context)
            
            if context.current_state == Doc2DBProcessingState.COMPLETED:
                await self.persist_results(context)
                self.logger.info("信号驱动处理成功完成")
            else:
                self.logger.error("信号驱动处理失败")
                
        except Exception as e:
            self.logger.error(f"信号驱动处理异常: {e}")
            context.current_state = Doc2DBProcessingState.FAILED
            
            error_step = StepUtils.create_error_step(context, e)
            context.step_outputs.append(error_step)
        
        finally:
            await self._stop_signal_processing()
        
        return context
    
    async def _start_signal_processing(self):
        """启动信号处理"""
        self.processing_tasks = []
        
        self.coordinator.start()
        component_tasks = await self.signal_component_manager.start_all()
        self.processing_tasks.extend(component_tasks)
        orchestrator_task = asyncio.create_task(self.receiver.start_processing())
        self.processing_tasks.append(orchestrator_task)
        
        self.logger.info("信号处理系统启动完成")
        await asyncio.sleep(0.1)
    
    async def _stop_signal_processing(self):
        """停止信号处理"""
        await self.coordinator.stop()
        self.signal_component_manager.stop_all()
        self.receiver.stop_processing()
        for task in self.processing_tasks:
            if not task.done():
                task.cancel()
        
        await asyncio.gather(*self.processing_tasks, return_exceptions=True)
        self.logger.info("信号处理系统已停止")
    
    async def _run_signal_driven_pipeline(self, context: Doc2DBContext):
        """运行信号驱动的处理管道"""
        await self.broadcaster.emit_simple_signal(
            SignalType.PROCESSING_START,
            data={'context': context.to_dict()},
            correlation_id=context.run_id
        )
        
        await self._signal_analyze_tables(context)
        
        for table_name in context.target_tables:
            is_relation = table_name in context.relation_tables
            self.coordinator.init_table_tracker(
                table_name=table_name,
                total_batches=1,
                is_relation_table=is_relation,
                max_iterations=self.max_iterations
            )
            self.logger.debug(f'初始化表追踪器: {table_name}')
        
        self.logger.info(f'分阶段提取策略: 实体表({len(context.entity_tables)}) -> 关系表({len(context.relation_tables)})')
        if context.entity_tables:
            self.logger.info(f'阶段1: 开始提取 {len(context.entity_tables)} 个实体表')
            
            for table_name in context.entity_tables:
                context.current_table_index = context.target_tables.index(table_name)
                
                await self._signal_extract_data(context, table_name, is_relation_table=False)
                
                processing_result = await self.table_waiter.wait_for_table_processing_complete(context, table_name)
                
                if not processing_result:
                    self.logger.warning(f'实体表 {table_name} 处理超时或失败，继续下一个表')
            
            self.logger.info(f'阶段1完成：所有实体表已提取')
        
        if context.relation_tables:
            self.logger.info(f'阶段2: 开始提取 {len(context.relation_tables)} 个关系表')
            
            for table_name in context.relation_tables:
                context.current_table_index = context.target_tables.index(table_name)
                
                await self._signal_extract_data(context, table_name, is_relation_table=True)
                
                processing_result = await self.table_waiter.wait_for_table_processing_complete(context, table_name)
                
                if not processing_result:
                    self.logger.warning(f'关系表 {table_name} 处理超时或失败，继续下一个表')
            
            self.logger.info(f'阶段2完成：所有关系表已提取')
        
        await self._wait_for_all_batches_complete(context)
        await self._wait_for_all_tables_processing_complete(context)
        await self._signal_multi_table_verify(context)
        
        total_violations = sum(len(v) for v in context.all_violations.values())
        total_fixes = sum(len(f) for f in context.all_fixes.values())
        self.logger.info(f'所有表处理完成 - 快照:{len(context.all_snapshots)}, 违规:{total_violations}, 修复:{total_fixes}')
        
        DataTransferUtils.transfer_processing_results(context)
        self._sync_snapshots_to_context(context)
        self._setup_result_context(context)
        context.current_state = Doc2DBProcessingState.COMPLETED
        completion_step = StepUtils.create_completion_step(context)
        context.step_outputs.append(completion_step)
        DataTransferUtils.save_final_snapshots(context)
        await self.broadcaster.emit_simple_signal(
            SignalType.PROCESSING_COMPLETE,
            data={'context': context.to_dict()},
            correlation_id=context.run_id
        )
    
    async def _signal_analyze_tables(self, context: Doc2DBContext):
        """通过信号分析表结构"""
        if isinstance(context.schema, str):
            context.schema = json.loads(context.schema)
        all_tables = self.schema_utils.extract_table_names_from_schema(context.schema)
        
        if context.table_name and context.table_name.strip() and context.table_name != 'orders':
            if context.table_name not in all_tables:
                self.logger.warning(f'指定的表 {context.table_name} 不在schema中，将处理所有表')
                target_tables = all_tables
            else:
                target_tables = [context.table_name]
                self.logger.info(f'只处理指定的表: {context.table_name}')
        else:
            target_tables = all_tables
            if len(all_tables) > 1:
                self.logger.info(f'将处理所有 {len(all_tables)} 个表: {", ".join(all_tables)}')
        
        context.target_tables = target_tables
        
        table_classification = self.schema_utils.classify_tables_by_type(context.schema)
        
        all_entity_tables = table_classification['entity_tables']
        all_relation_tables = table_classification['relation_tables']
        
        context.entity_tables = [t for t in all_entity_tables if t in target_tables]
        context.relation_tables = [t for t in all_relation_tables if t in target_tables]
        
        for rel_table in context.relation_tables:
            rel_extraction_config = self._get_relation_extraction_config(context.schema, rel_table)
            if rel_extraction_config and rel_extraction_config.get('enabled'):
                ref_tables = rel_extraction_config.get('reference_tables', [])
                ref_table_names = [ref.get('table') for ref in ref_tables if ref.get('table')]
                if ref_table_names:
                    self.logger.info(f'关系表 {rel_table} 将使用参考表: {", ".join(ref_table_names)}')
        
        self.logger.info(f'表分析完成: {len(target_tables)}个表 (实体表:{len(context.entity_tables)}, 关系表:{len(context.relation_tables)})')
        
        await self.broadcaster.emit_simple_signal(
            SignalType.ANALYSIS_COMPLETE,
            data={
                'target_tables': target_tables,
                'entity_tables': context.entity_tables,
                'relation_tables': context.relation_tables,
                'context': context
            },
            correlation_id=context.run_id
        )
    
    def _get_relation_extraction_config(self, schema: dict, table_name: str) -> dict:
        """从schema中获取指定表的 relation_extraction 配置
        
        Args:
            schema: 数据库schema
            table_name: 表名
            
        Returns:
            relation_extraction配置字典，如果不存在则返回None
        """
        tables = schema.get('tables', [])
        for table in tables:
            if table.get('name') == table_name:
                return table.get('relation_extraction')
        return None
    
    async def _signal_extract_data(self, context: Doc2DBContext, table_name: str, is_relation_table: bool = False):
        """通过信号请求数据提取（支持分 batch 处理）
        
        Args:
            context: 处理上下文
            table_name: 表名
            is_relation_table: 是否为关系表。如果True，将包含所有已提取的实体表数据
            
        流程：
        1. 检查是否为多文档模式
        2. 如果是，分成多个 batch
        3. 对每个 batch 按顺序执行：locate → extract → verify → fix
        4. 最后合并所有 batch 的结果
        """
        table_specific_schema = self.schema_utils.extract_table_specific_schema(context.schema, table_name)
        
        if not table_specific_schema:
            self.logger.warning(f'未找到表 {table_name} 的schema定义，使用完整schema')
            table_specific_schema = context.schema
        
        document_mode = context.document_mode if hasattr(context, 'document_mode') else "single"
        
        document_sources = self._get_document_sources_from_schema(context.schema, table_name)
        
        from .utils import DocumentUtils
        document_utils = DocumentUtils()
        
        if document_sources:
            self.logger.info(f'表 {table_name} 配置了 document_sources: {document_sources}')
            text_contents = self._load_documents_from_sources(document_sources, context)
            self.logger.info(f'从 document_sources 加载了 {len(text_contents)} 个文档')
        else:
            text_contents = document_utils.convert_documents_to_text(context.documents)
        
        self.logger.info(f'表 {table_name} - document_mode={document_mode}, 文档数={len(text_contents)}')
        
        if document_mode == "multi":
            await self._signal_extract_data_in_batches(
                context, table_name, table_specific_schema, text_contents, is_relation_table
            )
        else:
            await self._signal_extract_full_document(context, table_name, table_specific_schema, is_relation_table)
    
    async def _signal_extract_data_in_batches(self, context: Doc2DBContext, table_name: str, 
                                              table_specific_schema: dict, text_contents: List[str],
                                              is_relation_table: bool = False):
        """分 batch 处理多文档提取
        
        Args:
            context: 处理上下文
            table_name: 表名
            table_specific_schema: 表 schema
            text_contents: 所有文档内容
            is_relation_table: 是否为关系表
        """
        self.logger.info(f'表 {table_name} - 合并文档后分chunk处理')
        
        document_names = []
        if context and hasattr(context, 'documents') and context.documents:
            import os
            document_names = [os.path.basename(doc) for doc in context.documents]
        
        merged_text = self._merge_documents_langchain_style(text_contents, document_names)
        chunks = self._split_text_into_chunks_langchain_style(merged_text)
        batches = []
        for chunk_index, chunk in enumerate(chunks):
            batches.append({
                'documents': [chunk],
                'document_indices': [0],
                'document_sub_indices': [chunk_index],
                'total_chars': len(chunk),
                'total_words': self._estimate_word_count(chunk)
            })
        
        
        self.logger.info(f'表 {table_name}分成 {len(batches)} 个 batch 处理')
        
        tracker = self.coordinator.get_table_tracker(table_name)
        if tracker:
            tracker.total_batches = len(batches)
            self.logger.debug(f'更新表 {table_name} 的 batch 数量: {len(batches)}')
        
        for batch_index in range(1, len(batches) + 1):
            self.coordinator.init_batch_tracker(table_name, batch_index, len(batches))
        
        if not hasattr(context, 'batch_tracking'):
            context.batch_tracking = {}
        
        context.batch_tracking[table_name] = {
            'total_batches': len(batches),
            'completed_batches': 0,
            'snapshots': []
        }
        
        for batch_index, batch in enumerate(batches, 1):
            batch_document_indices = batch.get("document_indices", [])
            batch_document_sub_indices = batch.get("document_sub_indices", [0] * len(batch_document_indices))
            batch_document_names = []
            
            if context and hasattr(context, 'documents') and context.documents:
                import os
                for idx, sub_idx in zip(batch_document_indices, batch_document_sub_indices):
                    if idx < len(context.documents):
                        base_name = os.path.basename(context.documents[idx])
                        if sub_idx > 0:
                            name_parts = base_name.rsplit('.', 1)
                            if len(name_parts) == 2:
                                doc_name = f"{name_parts[0]} (part{sub_idx+1}).{name_parts[1]}"
                            else:
                                doc_name = f"{base_name} (part{sub_idx+1})"
                        else:
                            doc_name = base_name
                        batch_document_names.append(doc_name)
                    else:
                        batch_document_names.append(f"doc_{idx+1}")
            else:
                batch_document_names = [f"doc_{idx+1}" for idx in batch_document_indices]
            
            self.logger.info(
                f'启动 Batch {batch_index}/{len(batches)}: '
                f'{len(batch["documents"])} 个文档（{", ".join(batch_document_names)}），'
                f'共 {batch.get("total_words", 0)} 词'
            )
            
            await self._signal_extract_batch(
                context=context,
                table_name=table_name,
                table_specific_schema=table_specific_schema,
                batch_documents=batch["documents"],
                batch_document_names=batch_document_names,
                batch_index=batch_index,
                total_batches=len(batches),
                is_relation_table=is_relation_table
            )
            
            batch_timeout = 900.0
            batch_success = await self.coordinator.wait_batch_final(
                table_name, batch_index, timeout=batch_timeout
            )
            
            if not batch_success:
                self.logger.warning(
                    f'Batch {batch_index} 处理超时或失败，强制标记为final并继续下一个batch'
                )
                self.coordinator.mark_batch_final(table_name, batch_index)
            
            await asyncio.sleep(0.5)
    
    def _estimate_word_count(self, text_content: str) -> int:
        """
        估算文本的词数（中文字符数 + 英文单词数）
        
        Args:
            text_content: 文本内容
            
        Returns:
            int: 估算的词数
        """
        if not text_content:
            return 0
        
        import re
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text_content))
        english_words = len(re.findall(r'[a-zA-Z]+', text_content))
        return chinese_chars + english_words
    
    def _get_document_sources_from_schema(self, schema: dict, table_name: str) -> Optional[List[str]]:
        """从 schema 中获取表的 document_sources 配置
        
        Args:
            schema: 数据库 schema 定义
            table_name: 表名
            
        Returns:
            document_sources 列表，如果没有配置则返回 None
        """
        tables = schema.get('tables', [])
        for table in tables:
            if table.get('name') == table_name:
                relation_config = table.get('relation_extraction', {})
                if relation_config and relation_config.get('enabled', False):
                    document_sources = relation_config.get('document_sources', [])
                    if document_sources:
                        return document_sources
        return None
    
    def _load_documents_from_sources(self, document_sources: List[str], context: Doc2DBContext) -> List[str]:
        """从配置的 document_sources 加载文档内容
        
        Args:
            document_sources: 文档路径列表
            context: 处理上下文（用于解析相对路径）
            
        Returns:
            文档内容列表
        """
        from pathlib import Path
        text_contents = []
        
        project_root = self._find_project_root()
        
        for doc_source in document_sources:
            try:
                doc_path = Path(doc_source)
                
                if doc_path.is_absolute():
                    if doc_path.exists():
                        with open(doc_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            text_contents.append(content)
                            self.logger.info(f'   加载文档: {doc_path.name} ({len(content)} 字符)')
                    else:
                        self.logger.warning(f'    文档不存在: {doc_path}')
                else:
                    if project_root:
                        full_path = project_root / doc_source
                        if full_path.exists():
                            with open(full_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                                text_contents.append(content)
                                self.logger.info(f'   加载文档: {full_path.name} ({len(content)} 字符)')
                        else:
                            self.logger.warning(f'    文档不存在: {full_path}')
                    else:
                        self.logger.warning(f'    无法解析相对路径: {doc_source}（未找到项目根目录）')
            except Exception as e:
                self.logger.error(f'   加载文档失败 {doc_source}: {e}')
        
        return text_contents
    
    def _find_project_root(self) -> Optional[Path]:
        """查找项目根目录
        
        通过查找包含 backend/、dataset/ 等特征目录的父目录来定位项目根目录
        
        Returns:
            项目根目录的 Path 对象，如果未找到则返回 None
        """
        from pathlib import Path
        
        current = Path(__file__).resolve()
        
        for _ in range(10):
            parent = current.parent
            
            if (parent / 'backend').exists() and (parent / 'dataset').exists():
                return parent
            
            if parent == current:
                break
            
            current = parent
        
        return None
    
    def _split_text_into_chunks_langchain_style(self, text: str, 
                                                chunk_size: int = 100000, 
                                                chunk_overlap: int = 500) -> List[str]:
        """将文本分割成多个chunks
        
        策略：
        1. 如果文本长度 <= chunk_size，不分块
        2. 否则按照chunk_size分块，带overlap
        3. 优先在段落边界断开，其次在句子边界
        
        Args:
            text: 待分割的文本
            chunk_size: 每个chunk的大小（字符数，默认100000）
            chunk_overlap: chunk之间的重叠部分（字符数，默认500）
            
        Returns:
            List[str]: 分割后的chunk列表
        """
        if len(text) <= chunk_size:
            self.logger.info(f"   文本长度 {len(text)} 字符，无需分chunk")
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            if end < len(text):
                paragraph_break = text.rfind('\n\n', start, end)
                if paragraph_break > start + chunk_size // 2:
                    end = paragraph_break + 2
                else:
                    sentence_break = max(
                        text.rfind('。', start, end),
                        text.rfind('！', start, end),
                        text.rfind('？', start, end),
                        text.rfind('.', start, end),
                        text.rfind('!', start, end),
                        text.rfind('?', start, end)
                    )
                    if sentence_break > start + chunk_size // 2:
                        end = sentence_break + 1
            
            chunk = text[start:end]
            chunks.append(chunk)
            
            start = end - chunk_overlap if end < len(text) else end
        
        self.logger.info(f"    文本已分割成 {len(chunks)} 个chunks")
        for i, chunk in enumerate(chunks, 1):
            self.logger.info(f"     Chunk {i}: {len(chunk)} 字符")
        
        return chunks
    
    def _merge_documents_langchain_style(self, text_contents: List[str], 
                                        document_names: List[str] = None) -> str:
        """合并多个文档内容
        
        Args:
            text_contents: 文档内容列表
            document_names: 文档名称列表（可选）
            
        Returns:
            str: 合并后的文本
        """
        if not document_names:
            document_names = [f"doc_{i+1}" for i in range(len(text_contents))]
        
        merged_content = []
        
        for i, (content, doc_name) in enumerate(zip(text_contents, document_names), 1):
            if content:
                merged_content.append(f"=== Document {i}: {doc_name} ===\n")
                merged_content.append(content)
                merged_content.append(f"\n=== End of Document {i} ===\n\n")
        
        final_content = "\n".join(merged_content)
        self.logger.info(f"   合并完成: {len(text_contents)} 个文档 → {len(final_content)} 字符")
        return final_content
    
    def _split_documents_into_batches(self, text_contents: List[str], context) -> List[dict]:
        """将文档分成多个 batch - 智能分批策略
        
        核心策略：
        1. 优先保持文档完整性：同一文档不会被拆分到不同batch
        2. 超长文档处理：单个文档超过阈值时，单独作为一个batch（会在后续被拆分处理）
        3. 最优填充：在不超过限制的前提下，尽量让每个batch包含更多完整文档
        4. 双重限制：同时考虑词数和字符数，任一超限即分batch
        
        Args:
            text_contents: 文档内容列表
            context: 处理上下文
        
        Returns:
            List[dict]: 每个 dict 包含 {
                'documents': [...],  # 文本内容列表
                'document_indices': [...],  # 对应的原始文档索引
                'total_chars': int, 
                'total_words': int
            }
        """
        MAX_BATCH_WORDS = 50000
        MAX_BATCH_CHARS = 100000
        MAX_SINGLE_DOC_WORDS = 50000
        MAX_SINGLE_DOC_CHARS = 100000
        
        batches = []
        current_batch = []
        current_batch_indices = []
        current_chars = 0
        current_words = 0
        
        self.logger.info(f'开始智能分批：共 {len(text_contents)} 个文档')
        
        processed_documents = []
        for i, doc_content in enumerate(text_contents):
            doc_chars = len(doc_content)
            doc_words = self._estimate_word_count(doc_content)
            
            self.logger.debug(f'文档 {i+1}: {doc_chars} 字符, {doc_words} 词')
            
            if doc_words > MAX_SINGLE_DOC_WORDS or doc_chars > MAX_SINGLE_DOC_CHARS:
                self.logger.info(f'  文档 {i+1} 超长 ({doc_chars} 字符, {doc_words} 词)，需要拆分')
                sub_documents = self._split_long_document(doc_content, max_words=MAX_SINGLE_DOC_WORDS, max_chars=MAX_SINGLE_DOC_CHARS)
                self.logger.info(f'   拆分为 {len(sub_documents)} 个子文档')
                for sub_idx, sub_doc in enumerate(sub_documents):
                    processed_documents.append((sub_doc, i, sub_idx))
            else:
                processed_documents.append((doc_content, i, 0))
        
        self.logger.info(f'文档预处理完成：原始 {len(text_contents)} 个文档 → 处理后 {len(processed_documents)} 个文档')
        
        current_batch_sub_indices = []
        
        for doc_content, original_index, sub_index in processed_documents:
            doc_chars = len(doc_content)
            doc_words = self._estimate_word_count(doc_content)
            
            if current_batch and (
                current_words + doc_words > MAX_BATCH_WORDS or
                current_chars + doc_chars > MAX_BATCH_CHARS
            ):
                self.logger.debug(f'Batch 已满 ({current_chars} 字符, {current_words} 词)，开始新batch')
                batches.append({
                    'documents': current_batch,
                    'document_indices': current_batch_indices,
                    'document_sub_indices': current_batch_sub_indices,
                    'total_chars': current_chars,
                    'total_words': current_words
                })
                current_batch = []
                current_batch_indices = []
                current_batch_sub_indices = []
                current_chars = 0
                current_words = 0
            
            current_batch.append(doc_content)
            current_batch_indices.append(original_index)
            current_batch_sub_indices.append(sub_index)
            current_chars += doc_chars
            current_words += doc_words
        
        if current_batch:
            batches.append({
                'documents': current_batch,
                'document_indices': current_batch_indices,
                'document_sub_indices': current_batch_sub_indices,
                'total_chars': current_chars,
                'total_words': current_words
            })
        self.logger.info(f'分批完成：共 {len(batches)} 个batch')
        for i, batch in enumerate(batches, 1):
            self.logger.info(
                f'  Batch {i}: {len(batch["documents"])} 个文档, '
                f'{batch["total_words"]} 词, {batch["total_chars"]} 字符'
            )
        
        return batches
    
    def _split_long_document(self, document: str, max_words: int = 40000, max_chars: int = 150000) -> List[str]:
        """将超长文档按段落边界拆分为多个子文档
        
        策略：
        - 按段落边界分割（保持段落完整性）
        - 相邻子文档之间有10%的重叠区域，避免边界信息丢失
        - 每个子文档不超过指定词数和字符数（任一超限即拆分）
        
        Args:
            document: 原始长文档
            max_words: 每个子文档最大词数（默认 40000）
            max_chars: 每个子文档最大字符数（默认 150000）
            
        Returns:
            List[str]: 拆分后的子文档列表
        """
        import re
        
        paragraphs = re.split(r'\n\s*\n', document)
        
        sub_documents = []
        current_sub_doc = []
        current_words = 0
        current_chars = 0
        overlap_ratio = 0.1
        
        self.logger.debug(f'开始拆分超长文档：{len(paragraphs)} 个段落，目标 {max_words} 词 / {max_chars} 字符/子文档')
        
        for para in paragraphs:
            para_words = self._estimate_word_count(para)
            para_chars = len(para)
            
            if (current_words + para_words > max_words or current_chars + para_chars > max_chars) and current_sub_doc:
                sub_doc_text = '\n\n'.join(current_sub_doc)
                sub_documents.append(sub_doc_text)
                
                overlap_words = int(current_words * overlap_ratio)
                overlap_chars = int(current_chars * overlap_ratio)
                overlap_paras = []
                overlap_word_count = 0
                overlap_char_count = 0
                
                for j in range(len(current_sub_doc) - 1, -1, -1):
                    overlap_paras.insert(0, current_sub_doc[j])
                    overlap_word_count += self._estimate_word_count(current_sub_doc[j])
                    overlap_char_count += len(current_sub_doc[j])
                    if overlap_word_count >= overlap_words or overlap_char_count >= overlap_chars:
                        break
                
                current_sub_doc = overlap_paras + [para]
                current_words = overlap_word_count + para_words
                current_chars = overlap_char_count + para_chars
            else:
                current_sub_doc.append(para)
                current_words += para_words
                current_chars += para_chars
        if current_sub_doc:
            sub_doc_text = '\n\n'.join(current_sub_doc)
            sub_documents.append(sub_doc_text)
        
        self.logger.debug(f'文档拆分完成：{len(sub_documents)} 个子文档')
        for i, sub_doc in enumerate(sub_documents, 1):
            sub_words = self._estimate_word_count(sub_doc)
            sub_chars = len(sub_doc)
            self.logger.debug(f'  子文档 {i}: {sub_chars} 字符, {sub_words} 词')
        
        return sub_documents
    
    async def _signal_extract_batch(self, context: Doc2DBContext, table_name: str,
                                    table_specific_schema: dict, batch_documents: List[str],
                                    batch_document_names: List[str],
                                    batch_index: int, total_batches: int,
                                    is_relation_table: bool = False):
        """为单个 batch 发送提取信号
        
        该 batch 内的文档会被 EntityExtractor 的 locate 功能处理
        
        Args:
            context: 处理上下文
            table_name: 表名
            table_specific_schema: 表schema
            batch_documents: batch中的文档内容列表
            batch_document_names: batch中的文档名称列表（与batch_documents一一对应）
            batch_index: batch索引
            total_batches: 总batch数
            is_relation_table: 是否为关系表
        """
        signal_data = {
            'context': {
                'run_id': context.run_id,
                'table_name': table_name,
                'schema': table_specific_schema,
                'full_schema': context.schema,
                'text_contents': batch_documents,
                'batch_document_names': batch_document_names,
                'document_mode': 'multi',
                'nl_prompt': context.nl_prompt or context.user_query,
                'processing_context': context
            }
        }
        
        signal_data['batch_info'] = {
            'batch_index': batch_index,
            'total_batches': total_batches,
            'documents_in_batch': len(batch_documents),
            'document_names': batch_document_names
        }
        
        if is_relation_table:
            entity_snapshots = {}
            for entity_table_name in context.entity_tables:
                if entity_table_name in context.all_snapshots:
                    entity_snapshots[entity_table_name] = context.all_snapshots[entity_table_name]
            signal_data['entity_snapshots'] = entity_snapshots
            signal_data['is_relation_table'] = True
        else:
            signal_data['is_relation_table'] = False
        
        correlation_id = SignalUtils.get_correlation_id(
            context.run_id, table_name, f"extract_batch{batch_index}"
        )
        
        self.coordinator.update_batch_state(table_name, batch_index, "extracting")
        await self.broadcaster.emit_simple_signal(
            signal_type=SignalType.EXTRACTION_REQUEST,
            data=signal_data,
            correlation_id=correlation_id
        )
        
        self.logger.info(f'Batch {batch_index} 提取请求已发送: {table_name}')
    
    async def _signal_extract_full_document(self, context: Doc2DBContext, table_name: str,
                                           table_specific_schema: dict, is_relation_table: bool = False):
        """发送完整文档提取信号（原有逻辑）"""
        signal_data = SignalUtils.create_extraction_signal_data(context, table_name, table_specific_schema)
        
        if is_relation_table:
            entity_snapshots = {}
            for entity_table_name in context.entity_tables:
                if entity_table_name in context.all_snapshots:
                    entity_snapshots[entity_table_name] = context.all_snapshots[entity_table_name]
            signal_data['entity_snapshots'] = entity_snapshots
            signal_data['is_relation_table'] = True
            self.logger.debug(f'关系表 {table_name} - 收集了 {len(entity_snapshots)} 个实体表的数据')
        else:
            signal_data['is_relation_table'] = False
        
        correlation_id = SignalUtils.get_correlation_id(context.run_id, table_name, "extract")
        self.coordinator.update_table_state(table_name, TableLifecycleState.EXTRACTING)
        response = await self.coordinator.send_and_wait(
            signal_type=SignalType.EXTRACTION_REQUEST,
            data=signal_data,
            correlation_id=correlation_id,
            timeout=600.0
        )
        
        if response:
            self.logger.info(f'表提取完成: {table_name}')
            self.coordinator.update_table_state(table_name, TableLifecycleState.EXTRACTED)
        else:
            self.logger.warning(f'表提取超时: {table_name}')
            self.coordinator.update_table_state(table_name, TableLifecycleState.ERROR)
    
    async def _signal_verify_data(self, table_name: str, snapshots: List[TableSnapshot], context_data: dict = None):
        """通过信号请求数据验证
        
        Args:
            table_name: 表名
            snapshots: 要验证的快照列表
            context_data: 额外的上下文数据（如batch信息）
        """
        actual_table_name = table_name
        if snapshots and len(snapshots) > 0 and hasattr(snapshots[0], 'table'):
            actual_table_name = snapshots[0].table
            if actual_table_name != table_name:
                self.logger.warning(f'检测到表名不一致！参数={table_name}, snapshot={actual_table_name}，使用snapshot中的表名')
        
        table_specific_schema = self.schema_utils.extract_table_specific_schema(self.current_context.schema, actual_table_name)
        if not table_specific_schema:
            self.logger.warning(f'未找到表 {actual_table_name} 的schema定义，使用完整schema')
            table_specific_schema = self.current_context.schema
        
        signal_data = SignalUtils.create_verification_signal_data(
            self.current_context, actual_table_name, table_specific_schema, snapshots
        )
        
        if context_data:
            signal_data['context'].update(context_data)
        
        timestamp = int(time.time() * 1000)
        correlation_id = f"{self.current_context.run_id}_{actual_table_name}_verify_{timestamp}"
        self.coordinator.update_table_state(actual_table_name, TableLifecycleState.VERIFYING)
        response = await self.coordinator.send_and_wait(
            signal_type=SignalType.VERIFICATION_REQUEST,
            data=signal_data,
            correlation_id=correlation_id,
            timeout=300.0
        )
        
        if response:
            self.logger.info(f'表验证完成: {actual_table_name}')
            self.coordinator.update_table_state(actual_table_name, TableLifecycleState.VERIFIED)
            return response
        else:
            self.logger.warning(f'表验证超时: {actual_table_name}')
            self.coordinator.update_table_state(actual_table_name, TableLifecycleState.ERROR)
            return None
    
    async def _signal_fix_data(self, table_name: str, violations: List[Violation], snapshot: TableSnapshot, 
                               context_data: dict = None):
        """通过信号请求数据修复
        
        Args:
            table_name: 表名
            violations: 违规列表
            snapshot: 快照
            context_data: 上下文数据，包含batch_index等信息
        """
        self.logger.info(f'准备发送修复请求信号 - 表: {table_name}, 违规数: {len(violations)}')
        
        actual_table_name = table_name
        if snapshot and hasattr(snapshot, 'table'):
            actual_table_name = snapshot.table
            if actual_table_name != table_name:
                self.logger.warning(f'修复时检测到表名不一致！参数={table_name}, snapshot={actual_table_name}，使用snapshot中的表名')
        
        error_count = len([v for v in violations if getattr(v, 'severity', 'warn') == 'error'])
        warn_count = len([v for v in violations if getattr(v, 'severity', 'warn') == 'warn'])
        
        self.logger.info(f'违规统计 - 错误: {error_count}, 警告: {warn_count}')
        
        table_specific_schema = self.schema_utils.extract_table_specific_schema(self.current_context.schema, actual_table_name)
        if not table_specific_schema:
            self.logger.warning(f'未找到表 {actual_table_name} 的schema定义，使用完整schema')
            table_specific_schema = self.current_context.schema
        
        signal_data = SignalUtils.create_fixing_signal_data(
            self.current_context, actual_table_name, table_specific_schema, violations, snapshot
        )
        
        if context_data:
            if 'context' not in signal_data:
                signal_data['context'] = {}
            if 'is_batch_verification' in context_data:
                signal_data['context']['is_batch_verification'] = context_data['is_batch_verification']
            if 'batch_index' in context_data:
                signal_data['context']['batch_index'] = context_data['batch_index']
            if 'batch_total' in context_data:
                signal_data['context']['batch_total'] = context_data['batch_total']
            if 'is_warm_start' in context_data:
                signal_data['context']['is_warm_start'] = context_data['is_warm_start']
            if 'warm_start' in context_data:
                signal_data['context']['warm_start'] = context_data['warm_start']
        
        timestamp = int(time.time() * 1000)
        correlation_id = f"{self.current_context.run_id}_{actual_table_name}_fix_{timestamp}"
        
        self.coordinator.update_table_state(actual_table_name, TableLifecycleState.FIXING)
        
        response = await self.coordinator.send_and_wait(
            signal_type=SignalType.FIXING_REQUEST,
            data=signal_data,
            correlation_id=correlation_id,
            timeout=300.0
        )
        
        if response:
            self.logger.info(f'收到修复完成响应 - 表: {actual_table_name}')
            self.coordinator.update_table_state(actual_table_name, TableLifecycleState.FIXED)
            return response
        else:
            self.logger.error(f'修复请求超时 - 表: {actual_table_name}')
            self.coordinator.update_table_state(actual_table_name, TableLifecycleState.ERROR)
            return None
    
    async def _wait_for_all_batches_complete(self, context: Doc2DBContext):
        """等待所有batch完成并合并
        
        使用 coordinator 的批次追踪机制，确保所有表的所有batch都已完成
        """
        if not hasattr(context, 'batch_tracking') or not context.batch_tracking:
            return
        
        max_wait_time = 2700
        start_time = time.time()
        last_log_time = 0
        
        tables_with_batches = list(context.batch_tracking.keys())
        
        self.logger.info(f'等待 {len(tables_with_batches)} 个表的所有batch完成')
        
        while time.time() - start_time < max_wait_time:
            all_completed = True
            pending_tables = []
            
            for table_name in tables_with_batches:
                if not self.coordinator.all_batches_completed(table_name):
                    all_completed = False
                    pending_tables.append(table_name)
            
            if all_completed:
                self.logger.info('所有表的batch都已完成')
                return
            
            elapsed = time.time() - start_time
            if elapsed - last_log_time >= 10:
                self.logger.info(
                    f'等待batch完成: {len(pending_tables)}/{len(tables_with_batches)} 个表待完成, '
                    f'已等待 {elapsed:.0f}s'
                )
                last_log_time = elapsed
            
            await asyncio.sleep(0.5)
        
        self.logger.warning(f'等待batch完成超时，部分表可能未完成')
    
    async def _wait_for_all_tables_processing_complete(self, context: Doc2DBContext):
        """等待所有表处理完成（包括验证-修复循环和warm start）
        
        使用 coordinator 的表生命周期状态管理，确保所有表都达到 FINAL 状态
        """
        if not context.target_tables:
            return
        
        max_wait_time = 1800
        
        self.logger.info(f'开始等待所有表处理完成: 总共 {len(context.target_tables)} 个表')
        
        success = await self.coordinator.wait_all_tables_final(timeout=max_wait_time)
        
        if success:
            self.logger.info('所有表处理完成')
        else:
            status = self.coordinator.get_all_tables_status()
            pending = {k: v for k, v in status.items() if v != 'final'}
            
            self.logger.warning(
                f'表处理等待超时: {len(pending)}/{len(status)} 个表未完成'
            )
            
            for table_name, state in pending.items():
                self.logger.warning(f'  - {table_name}: {state}')
                self.coordinator.update_table_state(
                    table_name, 
                    TableLifecycleState.FINAL,
                    force=True
                )
    
    async def _load_reference_tables_for_verification(self, context: Doc2DBContext):
        """为验证加载 relation_extraction 配置的参考表数据
        
        将参考表数据从文件加载并转换为 TableSnapshot 格式，添加到 context.all_snapshots
        这样验证器就可以使用这些参考数据进行外键验证
        """
        import json
        from pathlib import Path
        from ...memory import TableSnapshot, TableRow, CellData
        from ...core.ids import IdGenerator
        
        for rel_table in context.relation_tables:
            rel_config = self._get_relation_extraction_config(context.schema, rel_table)
            
            if not rel_config or not rel_config.get('enabled'):
                continue
            
            ref_tables_config = rel_config.get('reference_tables', [])
            
            for ref_config in ref_tables_config:
                ref_table_name = ref_config.get('table')
                data_source = ref_config.get('data_source')
                key_fields = ref_config.get('key_fields', [])
                
                if not ref_table_name or not data_source:
                    continue
                
                if ref_table_name in context.all_snapshots:
                    continue
                
                try:
                    data_path = Path(data_source)
                    if not data_path.is_absolute():
                        if Path.cwd().name == 'backend':
                            data_path = Path.cwd().parent / data_source
                        else:
                            data_path = Path.cwd() / data_source
                    
                    if not data_path.exists():
                        self.logger.warning(f" 参考表数据文件不存在: {data_path}")
                        continue
                    
                    with open(data_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if ref_table_name not in data:
                        self.logger.warning(f" 数据文件中未找到表 {ref_table_name}")
                        continue
                    
                    table_data = data[ref_table_name]
                    
                    rows = []
                    for idx, row_data in enumerate(table_data):
                        tuple_id = IdGenerator.generate_tuple_id(ref_table_name, idx, row_data)
                        cells = {}
                        
                        for field_name, field_value in row_data.items():
                            if key_fields and field_name not in key_fields:
                                continue
                            
                            cells[field_name] = CellData(
                                value=field_value,
                                evidences=[]
                            )
                        
                        rows.append(TableRow(
                            tuple_id=tuple_id,
                            cells=cells
                        ))
                    
                    snapshot = TableSnapshot(
                        run_id=context.run_id,
                        table=ref_table_name,
                        table_id=f"{ref_table_name}_{datetime.now().isoformat()}",
                        rows=rows,
                        created_at=datetime.now().isoformat()
                    )
                    
                    snapshot.processing_stage = "reference_data"
                    snapshot.stage_description = f"从 {data_source} 加载的参考表数据"
                    
                    context.all_snapshots[ref_table_name] = snapshot
                    
                    self.logger.info(f" 为验证加载参考表 {ref_table_name}: {len(rows)} 行（来自 {data_source}）")
                    
                except Exception as e:
                    self.logger.error(f" 加载参考表 {ref_table_name} 失败: {e}")
                    continue
    
    async def _signal_multi_table_verify(self, context: Doc2DBContext):
        """通过信号请求多表验证和修复"""
        context.multi_table_violations = []
        
        await self._load_reference_tables_for_verification(context)
        
        if not context.all_snapshots or len(context.all_snapshots) < 2:
            self.logger.debug('表数量不足2个，跳过多表验证')
            return
        
        try:
            from ..verifier.mcp_verifier import MCPBasedVerifier
            
            verifier = MCPBasedVerifier()
            
            multi_table_violations = verifier.verify_multi_table(
                all_snapshots=context.all_snapshots,
                schema=context.schema,
                context=context
            )
            
            context.multi_table_violations = multi_table_violations
            
            self.logger.info(f'多表验证完成，发现 {len(multi_table_violations)} 个跨表违规')
            
            if hasattr(context, 'step_outputs'):
                from ...core.io import IOManager
                
                violations_by_table = self._group_violations_by_table(multi_table_violations) if multi_table_violations else {}
                
                verification_details = {
                    'tables_verified': list(context.all_snapshots.keys()),
                    'total_tables': len(context.all_snapshots),
                    'total_violations': len(multi_table_violations),
                    'violations_by_table': violations_by_table,
                    'verification_status': 'passed' if len(multi_table_violations) == 0 else 'violations_found'
                }
                
                context.step_outputs.append({
                    'step': 'multi_table_verification',
                    'step_name': 'multi_table_verification_completed',
                    'status': 'completed',
                    'description': f'多表验证完成，发现 {len(multi_table_violations)} 个跨表质量问题',
                    'details': verification_details,
                    'timestamp': datetime.now().isoformat()
                })
                
            
            if multi_table_violations:
                self.logger.info(f' 开始修复 {len(multi_table_violations)} 个跨表违规...')
                
                await self._fix_multi_table_violations(context, multi_table_violations)
            
        except Exception as e:
            self.logger.error(f"多表验证异常: {e}", exc_info=True)
        
        await self.broadcaster.emit_simple_signal(
            SignalType.MULTI_TABLE_VERIFICATION_COMPLETE,
            data={
                'violations': context.multi_table_violations,
                'context': context
            },
            correlation_id=f"{context.run_id}_multi_table_verify"
        )
    
    def _group_violations_by_table(self, violations):
        """按表分组违规"""
        grouped = {}
        for v in violations:
            table = getattr(v, 'table', 'unknown')
            if table not in grouped:
                grouped[table] = 0
            grouped[table] += 1
        return grouped
    
    async def _fix_multi_table_violations(self, context: Doc2DBContext, violations: List[Violation]):
        """修复跨表违规
        
        Args:
            context: 处理上下文
            violations: 跨表违规列表
        """
        from ..fixer.mcp_fixer import MCPBasedFixer
        from ...core.io import IOManager
        
        violations_by_table = {}
        for violation in violations:
            table = getattr(violation, 'table', 'unknown')
            if table not in violations_by_table:
                violations_by_table[table] = []
            violations_by_table[table].append(violation)
        
        self.logger.info(f'按表分组：{len(violations_by_table)} 个表有跨表违规')
        
        fixer = MCPBasedFixer()
        total_fixes = 0
        
        for table_name, table_violations in violations_by_table.items():
            if table_name not in context.all_snapshots:
                self.logger.warning(f'表 {table_name} 没有快照，跳过修复')
                continue
            
            snapshot = context.all_snapshots[table_name]
            
            self.logger.info(f'修复表 {table_name} 的 {len(table_violations)} 个跨表违规')
            
            try:
                fixes = fixer.fix_batch(table_violations, snapshot, context)
                
                if fixes:
                    updated_snapshot = self._apply_fixes_to_snapshot(snapshot, fixes)
                    
                    context.all_snapshots[table_name] = updated_snapshot
                    
                    if table_name not in context.all_fixes:
                        context.all_fixes[table_name] = []
                    context.all_fixes[table_name].extend(fixes)
                    
                    total_fixes += len(fixes)
                    
                    self.logger.info(f'表 {table_name} 应用了 {len(fixes)} 个跨表修复')
                else:
                    self.logger.warning(f'表 {table_name} 未生成跨表修复')
                    
            except Exception as e:
                self.logger.error(f'修复表 {table_name} 的跨表违规失败: {e}', exc_info=True)
        
        if hasattr(context, 'step_outputs'):
            context.step_outputs.append({
                'step': 'multi_table_fixing',
                'status': 'completed',
                'description': f'跨表违规修复完成',
                'details': {
                    'tables_fixed': list(violations_by_table.keys()),
                    'total_violations': len(violations),
                    'total_fixes': total_fixes,
                    'fixes_by_table': {
                        table: len([f for f in context.all_fixes.get(table, []) if 'CROSS_TABLE' in getattr(f, 'id', '')])
                        for table in violations_by_table.keys()
                    }
                },
                'timestamp': datetime.now().isoformat()
            })
        
        self.logger.info(f'跨表违规修复完成，共应用 {total_fixes} 个修复')
        
        if total_fixes > 0:
            self.logger.info('跨表修复后重新验证')
            
            from ..verifier.mcp_verifier import MCPBasedVerifier
            verifier = MCPBasedVerifier()
            
            remaining_violations = verifier.verify_multi_table(
                all_snapshots=context.all_snapshots,
                schema=context.schema,
                context=context
            )
            
            context.multi_table_violations = remaining_violations
            
            if remaining_violations:
                self.logger.warning(f'修复后仍有 {len(remaining_violations)} 个跨表违规')
            else:
                self.logger.info('所有跨表违规已成功修复')
    
    def _apply_fixes_to_snapshot(self, snapshot: TableSnapshot, fixes: List[Fix]) -> TableSnapshot:
        """将修复应用到快照
        
        Args:
            snapshot: 原始快照
            fixes: 修复列表
            
        Returns:
            应用修复后的新快照
        """
        import copy
        updated_snapshot = copy.deepcopy(snapshot)
        
        for fix in fixes:
            tuple_id = getattr(fix, 'tuple_id', '')
            attr = getattr(fix, 'attr', '')
            new_value = getattr(fix, 'new', None)
            
            for row in updated_snapshot.rows:
                if row.tuple_id == tuple_id:
                    if attr in row.cells:
                        row.cells[attr].value = new_value
                        self.logger.debug(f'应用修复: {tuple_id}.{attr} = {new_value}')
                    break
        
        return updated_snapshot
    
    async def _trigger_batch_warm_start_extraction(self, table_name: str, violations_requiring_reextraction, 
                                                    snapshot, text_contents, document_names, 
                                                    batch_index, total_batches):
        """
        针对单个batch的warm start重提取
        直接使用当前batch的文档，不进行全局文档识别
        """
        try:
            self.logger.info(
                f' Batch {batch_index}/{total_batches} warm start: '
                f'使用当前batch文档进行重提取，文档: {document_names}'
            )
            
            signal_data = {
                'context': {
                    'run_id': self.current_context.run_id,
                    'table_name': table_name,
                    'schema': self.schema_utils.extract_table_specific_schema(
                        self.current_context.schema, table_name
                    ),
                    'full_schema': self.current_context.schema,
                    'text_contents': text_contents,
                    'batch_document_names': document_names,
                    'document_mode': 'multi',
                    'nl_prompt': self.current_context.nl_prompt or self.current_context.user_query,
                    'processing_context': self.current_context,
                    'batch_index': batch_index,
                    'batch_total': total_batches,
                    'is_batch_verification': True,
                    'is_warm_start': True,
                    'warm_start': True,
                    'previous_snapshot': snapshot.to_dict() if snapshot else None,
                    'violations': [
                        {
                            'id': v.id,
                            'tuple_id': v.tuple_id,
                            'attr': v.attr,
                            'constraint_type': v.constraint_type,
                            'description': v.description,
                            'severity': v.severity
                        }
                        for v in violations_requiring_reextraction
                    ]
                }
            }
            
            correlation_id = f"{self.current_context.run_id}_{table_name}_batch_{batch_index}_warm_start_extract"
            
            await self.broadcaster.emit_simple_signal(
                SignalType.EXTRACTION_REQUEST,
                data=signal_data,
                correlation_id=correlation_id
            )
            
            self.logger.info(f' Batch {batch_index} warm start提取请求已发送: {table_name}')
            
        except Exception as e:
            self.logger.error(f'Batch {batch_index} warm start请求处理失败: {e}', exc_info=True)
    
    async def _trigger_smart_warm_start_extraction(self, table_name: str, violations_requiring_reextraction, snapshot):
        """触发智能的 warm start 重新提取（委托给signal_handlers处理）
        
        注意：warm_start_attempted 的检查和标记由调用者（signal_handlers）负责。
        这个方法只是一个委托，实际逻辑在signal_handlers中。
        
         重要：Warm start也需要支持分batch处理，避免文档过长导致LLM调用失败
        """
        self.logger.info(f'执行 warm start 重新提取 - 表: {table_name}, {len(violations_requiring_reextraction)} 个违规')
        
        if not hasattr(self.current_context, 'warm_start_tracking'):
            self.current_context.warm_start_tracking = set()
        self.current_context.warm_start_tracking.add(table_name)
        self.logger.info(f' Warm start追踪: 添加 {table_name}, 当前进行中: {list(self.current_context.warm_start_tracking)}')
        
        try:
            table_specific_schema = self.schema_utils.extract_table_specific_schema(
                self.current_context.schema, table_name
            )
            if not table_specific_schema:
                table_specific_schema = self.current_context.schema
            
            relevant_documents = self._extract_relevant_documents(violations_requiring_reextraction, snapshot)
            
            self.logger.info(f' 从违规中识别到 {len(relevant_documents)} 个相关文档: {relevant_documents}')
            
            document_utils = DocumentUtils()
            if relevant_documents:
                all_documents = self.current_context.documents
                relevant_text_contents = []
                relevant_doc_names = []
                
                import os
                for doc_path in all_documents:
                    doc_basename = os.path.basename(doc_path)
                    is_relevant = False
                    for rel_doc in relevant_documents:
                        if doc_basename == rel_doc or rel_doc in doc_basename or doc_basename in rel_doc:
                            is_relevant = True
                            break
                    
                    if is_relevant:
                        text_content = document_utils.convert_documents_to_text([doc_path])
                        if text_content:
                            relevant_text_contents.extend(text_content)
                            relevant_doc_names.append(doc_basename)
                            self.logger.info(f'   匹配相关文档: {doc_basename}')
                
                if not relevant_text_contents:
                    self.logger.warning(f' 未匹配到任何相关文档，降级为使用所有文档')
                    warm_start_text_contents = document_utils.convert_documents_to_text(self.current_context.documents)
                    warm_start_doc_names = [os.path.basename(d) for d in self.current_context.documents]
                else:
                    warm_start_text_contents = relevant_text_contents
                    warm_start_doc_names = relevant_doc_names
                    self.logger.info(f' Warm start 将只处理 {len(warm_start_doc_names)} 个相关文档（跳过其他 {len(all_documents) - len(warm_start_doc_names)} 个文档）')
            else:
                self.logger.warning(f' 无法从evidences识别相关文档，将使用所有文档')
                warm_start_text_contents = document_utils.convert_documents_to_text(self.current_context.documents)
                warm_start_doc_names = [os.path.basename(d) for d in self.current_context.documents]
            
            total_chars = sum(len(doc) for doc in warm_start_text_contents)
            total_words = sum(self._estimate_word_count(doc) for doc in warm_start_text_contents)
            
            self.logger.info(f'WARM START 文档统计 - 表: {table_name}, 文档数: {len(warm_start_text_contents)}, '
                           f'总字符: {total_chars}, 总词数: {total_words}')
            
            MAX_BATCH_WORDS = 50000
            MAX_BATCH_CHARS = 100000
            
            if total_words > MAX_BATCH_WORDS or total_chars > MAX_BATCH_CHARS or len(warm_start_text_contents) > 2:
                self.logger.info(f' WARM START 文档超过限制，启用分batch处理')
                
                batches = self._split_documents_into_batches(warm_start_text_contents, self.current_context)
                
                self.logger.info(f' WARM START 分成 {len(batches)} 个batch处理')
                
                for batch_index, batch in enumerate(batches, 1):
                    batch_document_indices = batch.get("document_indices", [])
                    batch_text_contents = batch["documents"]
                    
                    batch_doc_names = []
                    for idx in batch_document_indices:
                        if idx < len(warm_start_doc_names):
                            batch_doc_names.append(warm_start_doc_names[idx])
                        else:
                            batch_doc_names.append(f"doc_{idx+1}")
                    
                    self.logger.info(
                        f' 启动 WARM START Batch {batch_index}/{len(batches)}: '
                        f'{len(batch_text_contents)} 个文档（{", ".join(batch_doc_names)}），'
                        f'共 {batch.get("total_words", 0)} 词，{batch.get("total_chars", 0)} 字符'
                    )
                    
                    self.coordinator.init_batch_tracker(table_name, batch_index, len(batches), is_warm_start=True)
                    
                    signal_data = {
                        'context': {
                            'run_id': self.current_context.run_id,
                            'table_name': table_name,
                            'schema': table_specific_schema,
                            'full_schema': self.current_context.schema,
                            'text_contents': batch_text_contents,  #  只包含当前batch的文档
                            'batch_document_names': batch_doc_names,
                            'document_mode': 'multi',
                            'nl_prompt': self.current_context.nl_prompt or self.current_context.user_query,
                            'processing_context': self.current_context
                        }
                    }
                    
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
                                'severity': getattr(v, 'severity', 'error'),
                                'suggested_fix': v.suggested_fix.value if hasattr(v, 'suggested_fix') and v.suggested_fix else None,
                                'detector_id': getattr(v, 'detector_id', 'unknown'),
                                'timestamp': getattr(v, 'timestamp', None)
                            } 
                            for v in violations_requiring_reextraction
                        ],
                        'batch_info': {
                            'batch_index': batch_index,  #  使用原始的 batch_index
                            'is_warm_start': True,  #  添加 warm start 标志
                            'total_batches': len(batches),
                            'documents_in_batch': len(batch_text_contents),
                            'document_names': batch_doc_names
                        }
                    })
                    
                    correlation_id = SignalUtils.get_correlation_id(
                        self.current_context.run_id, table_name, f"warm_start_batch{batch_index}"
                    )
                    
                    await self.broadcaster.emit_simple_signal(
                        SignalType.EXTRACTION_REQUEST,
                        data=signal_data,
                        correlation_id=correlation_id
                    )
                    
                    self.logger.info(f' WARM START Batch {batch_index} 提取请求信号已发送：{table_name}')
                    
                    batch_timeout = 60.0  # 每个warm start batch最多等待1分钟，超时则跳过
                    
                    batch_success = await self.coordinator.wait_batch_final(
                        table_name, batch_index, is_warm_start=True, timeout=batch_timeout
                    )
                    
                    if not batch_success:
                        self.logger.warning(
                            f' WARM START Batch {batch_index} 处理超时（超过{batch_timeout}秒），'
                            f'跳过此batch，强制标记为final并继续下一个batch'
                        )
                        self.coordinator.mark_batch_final(table_name, batch_index, is_warm_start=True)
                    
                    await asyncio.sleep(0.5)
            else:
                self.logger.info(f' WARM START 文档未超限，直接处理')
                
                signal_data = {
                    'context': {
                        'run_id': self.current_context.run_id,
                        'table_name': table_name,
                        'schema': table_specific_schema,
                        'full_schema': self.current_context.schema,
                        'text_contents': warm_start_text_contents,  #  只包含相关文档
                        'batch_document_names': warm_start_doc_names,
                        'document_mode': 'multi',
                        'nl_prompt': self.current_context.nl_prompt or self.current_context.user_query,
                        'processing_context': self.current_context
                    }
                }
                
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
                            'severity': getattr(v, 'severity', 'error'),
                            'suggested_fix': v.suggested_fix.value if hasattr(v, 'suggested_fix') and v.suggested_fix else None,
                            'detector_id': getattr(v, 'detector_id', 'unknown'),
                            'timestamp': getattr(v, 'timestamp', None)
                        } 
                        for v in violations_requiring_reextraction
                    ]
                })
                
                self.logger.info(f'WARM START 信号数据已准备: {len(violations_requiring_reextraction)} 个待重提取违规')
                
                await self.broadcaster.emit_simple_signal(
                    SignalType.EXTRACTION_REQUEST,
                    data=signal_data,
                    correlation_id=SignalUtils.get_correlation_id(
                        self.current_context.run_id, table_name, "warm_start_extract"
                    )
                )
                
                self.logger.info(f'WARM START 提取请求信号已发送：{table_name}')
            
        except Exception as e:
            self.logger.error(f"智能warm start提取失败: {e}", exc_info=True)
            await self._signal_fix_data(table_name, violations_requiring_reextraction, snapshot)
    
    def _extract_relevant_documents(self, violations: List, snapshot) -> List[str]:
        """从violations中提取相关文档（通过evidences）
        
         关键功能：从违规的 evidences 中识别所属batch的文档
        - 只返回违规数据所在的源文档名
        - 用于 warm start 时只重提取相关batch的文档，而不是所有文档
        
        Args:
            violations: 违规列表
            snapshot: 表快照
            
        Returns:
            相关文档名列表（例如：['doc1.txt', 'doc2.txt']）
        """
        if not violations or not snapshot or not hasattr(snapshot, 'rows'):
            return []
        
        import os
        import re
        
        relevant_docs = set()
        violation_tuple_ids = set([v.tuple_id for v in violations])
        
        self.logger.debug(f'开始从 {len(violations)} 个违规中提取相关文档，涉及 {len(violation_tuple_ids)} 个tuple_id')
        
        for row in snapshot.rows:
            if row.tuple_id in violation_tuple_ids:
                for cell_name, cell_data in row.cells.items():
                    if hasattr(cell_data, 'evidences') and cell_data.evidences:
                        for evidence in cell_data.evidences:
                            
                            clean_evidence = re.sub(r'\[.*?\]\s*', '', evidence)
                            
                            doc_name = os.path.basename(clean_evidence.strip())
                            
                            if doc_name and len(doc_name) > 0 and not doc_name.startswith('['):
                                relevant_docs.add(doc_name)
                                self.logger.debug(f'  从 {row.tuple_id}.{cell_name} 提取文档: {doc_name}')
        
        result = list(relevant_docs)
        self.logger.info(f' 从违规evidences中识别到 {len(result)} 个相关文档：{result}')
        return result
    
    async def handle_warm_start_request(self, signal):
        """处理 Warm Start 重提取请求
        
        当 Verifier 发现无法通过工具修复的违规时（如空值、缺失值），
        会发送 WARM_START_REQUEST 信号，Orchestrator 调度 Extractor 重新提取。
        
        Args:
            signal: WARM_START_REQUEST 信号，包含：
                - table_name: 表名
                - violations: 需要重提取的违规列表
                - snapshot: 当前快照
                - run_id: 运行ID
                - schema: Schema定义
        """
        data = signal.data
        table_name = data.get('table_name')
        violations = data.get('violations', [])
        snapshot = data.get('snapshot')
        
        self.logger.info(
            f" 收到 Warm Start 请求: {table_name}, "
            f"{len(violations)} 个违规需要重提取"
        )
        
        try:
            relevant_docs = self._extract_relevant_documents(violations, snapshot)
            
            if not relevant_docs:
                self.logger.warning(f"未能提取相关文档，使用所有文档")
                text_contents = self.document_utils.convert_documents_to_text(
                    self.current_context.documents
                )
            else:
                text_contents = []
                import os
                for doc_path in self.current_context.documents:
                    doc_name = os.path.basename(doc_path)
                    if any(rel_doc in doc_name or doc_name in rel_doc for rel_doc in relevant_docs):
                        doc_text = self.document_utils.convert_documents_to_text([doc_path])
                        if doc_text:
                            text_contents.extend(doc_text)
            
            if not text_contents:
                self.logger.error(f"Warm Start: 没有可用的文档内容")
                return
            
            signal_data = {
                'context': {
                    'run_id': self.current_context.run_id,
                    'table_name': table_name,
                    'schema': self.schema_utils.extract_table_specific_schema(
                        self.current_context.schema, table_name
                    ),
                    'full_schema': self.current_context.schema,
                    'text_contents': text_contents,
                    'document_mode': 'single',  # Warm Start 不分 batch
                    'nl_prompt': self.current_context.nl_prompt or self.current_context.user_query,
                    'processing_context': self.current_context,
                    'is_warm_start': True,
                    'previous_snapshot': snapshot,
                    'violations': violations
                }
            }
            
            correlation_id = f"{self.current_context.run_id}_{table_name}_warmstart_extract"
            
            await self.broadcaster.emit_simple_signal(
                SignalType.EXTRACTION_REQUEST,
                data=signal_data,
                correlation_id=correlation_id
            )
            
            self.logger.info(f" Warm Start 提取请求已发送: {table_name}")
            
        except Exception as e:
            self.logger.error(f"Warm Start 请求处理失败: {e}", exc_info=True)
    
    def _set_table_force_completed(self, table_name: str):
        """设置表强制完成标记"""
        if not hasattr(self.current_context, 'force_completed_tables'):
            self.current_context.force_completed_tables = set()
        
        self.current_context.force_completed_tables.add(table_name)
        
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
        
        self.logger.debug(f'设置表 {table_name} 强制完成标记')
    
    def _setup_result_context(self, context: Doc2DBContext):
        """设置result_context以支持前端显示查看结果按钮
        
         只输出用户请求的目标表，不包含参考表（用于关系抽取验证的辅助表）
        """
        total_rows = 0
        table_summaries = {}
        
        target_snapshots = {}
        
        for table_name in context.target_tables:
            if table_name in context.all_snapshots:
                snapshot = context.all_snapshots[table_name]
                
                if hasattr(snapshot, 'processing_stage') and snapshot.processing_stage == 'reference_data':
                    self.logger.debug(f'跳过参考表 {table_name}（不包含在输出结果中）')
                    continue
                
                target_snapshots[table_name] = snapshot
                
                if snapshot and hasattr(snapshot, 'rows') and snapshot.rows:
                    row_count = len(snapshot.rows)
                    total_rows += row_count
                    table_summaries[table_name] = {
                        'rows': row_count,
                        'violations': len(context.all_violations.get(table_name, [])),
                        'fixes': len(context.all_fixes.get(table_name, []))
                    }
        
        context.result_context = {
            'summary': {
                'total_rows': total_rows,
                'total_tables': len(target_snapshots),
                'total_violations': sum(len(v) for v in context.all_violations.values()),
                'total_fixes': sum(len(f) for f in context.all_fixes.values()),
                'table_summaries': table_summaries
            },
            'snapshots': target_snapshots,  #  只包含目标表
            'violations': context.all_violations,
            'fixes': context.all_fixes,
            'status': 'completed'
        }
        
        self.logger.info(
            f'result_context设置完成: {total_rows} 总行数, {len(target_snapshots)} 个目标表 '
        )
    
    def _sync_snapshots_to_context(self, context: Doc2DBContext):
        """强制同步快照数据到context以确保服务层能读取到数据
        
        注意：不覆盖已经存在的快照（可能是batch合并后的完整数据）
        """
        self.logger.debug('开始同步快照数据到context')
        
        try:
            for table_name in context.target_tables:
                if table_name in context.all_snapshots:
                    self.logger.debug(f'表 {table_name} 的快照已存在于context.all_snapshots，跳过memory同步')
                    continue
                
                snapshot = self._sync_wrapper(self.memory_manager.get_snapshot(context.run_id, table_name))
                if snapshot:
                    context.all_snapshots[table_name] = snapshot
                    self.logger.debug(f'从memory_manager同步表 {table_name} 的快照')
        except Exception as e:
            self.logger.error(f'memory同步异常: {e}')
        
        if context.all_snapshots:
            context.snapshots = list(context.all_snapshots.values())

                
    def process(self, context: Doc2DBContext) -> Doc2DBContext:
        """同步版本的处理方法 - 为了兼容现有服务"""
        DebugUtils.write_debug_log(context, f"重构后的orchestrator被调用: {context.run_id}")
            
        try:
            result = self._sync_wrapper(self.process_with_signals(context))
            
            if result.current_state == Doc2DBProcessingState.COMPLETED:
                DebugUtils.ensure_completion_step(result)
            
            DebugUtils.write_debug_log(context, f"处理完成，状态: {result.current_state}")
                
            return result
        except Exception as e:
            self.logger.error(f"同步调用异常: {e}", exc_info=True)
            
            error_step = StepUtils.create_error_step(context, e, 'orchestrator_sync_error')
            error_step['details']['sync_call'] = True
            context.step_outputs.append(error_step)
            
            DebugUtils.write_debug_log(context, f" 处理异常: {str(e)}")
                
            context.current_state = Doc2DBProcessingState.FAILED
            return context


def create_unified_orchestrator(max_iterations: int = 3) -> UnifiedSignalDrivenOrchestrator:
    """创建统一的信号驱动协调器"""
    return UnifiedSignalDrivenOrchestrator(max_iterations=max_iterations)


async def run_unified_signal_orchestrator(context: Doc2DBContext) -> Doc2DBContext:
    """运行统一的信号驱动协调器"""
    orchestrator = create_unified_orchestrator()
    return await orchestrator.process_with_signals(context)
