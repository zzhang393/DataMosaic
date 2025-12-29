import os
import re
import json
import logging
from typing import List, Dict, Any, Optional, Type
from pathlib import Path
from abc import ABC, abstractmethod
from datetime import datetime

from ...memory import TableRow, CellData, Fix
from ...core.ids import IdGenerator

from llm.main import get_answer

from .document_locator import DocumentLocator

from .entity_extractor import EntityExtractor
from .relation_extractor import RelationExtractor

from .utils import ExtractorUtils

try:
    from pydantic import BaseModel, Field, create_model
    from langchain_core.output_parsers import PydanticOutputParser
    from langchain_openai import ChatOpenAI
    from langchain.output_parsers import OutputFixingParser
    STRUCTURED_OUTPUT_AVAILABLE = True
except ImportError:
    STRUCTURED_OUTPUT_AVAILABLE = False
    BaseModel = None
    Field = None
    create_model = None


class BaseExtractor:
    
    def __init__(self, extractor_id: str = "BaseExtractor.v1",
                 enable_locate: bool = False,
                 locate_threshold: int = 50000):
        """
        初始化提取器
        
        Args:
            extractor_id: 提取器标识符
            enable_locate: 是否启用文档定位功能（默认关闭）
            locate_threshold: 文档定位的词数阈值（默认50000词）
        """
        self.extractor_id = extractor_id
        self.logger = logging.getLogger('doc2db.extractor')
        self.enable_locate = enable_locate
        self.locator = DocumentLocator(
            word_threshold=locate_threshold
        ) if enable_locate else None
        
        self.entity_extractor = EntityExtractor(logger=self.logger)
        self.relation_extractor = RelationExtractor(logger=self.logger)
        
        self.utils = ExtractorUtils(logger=self.logger)
    
    def _get_model_api_config(self, model: str) -> Dict[str, Optional[str]]:
        """根据模型名称获取对应的API配置
        
        Args:
            model: 模型名称
            
        Returns:
            包含 api_base 和 api_key 的字典
        """
        model_lower = model.lower()
        
        if 'deepseek' in model_lower:
            return {
                'api_base': os.getenv("DEEPSEEK_URL"),
                'api_key': os.getenv("DEEPSEEK_KEY")
            }
        
        elif 'qwen' in model_lower:
            return {
                'api_base': os.getenv("QWEN_URL"),
                'api_key': os.getenv("QWEN_KEY")
            }
        
        else:
            return {
                'api_base': os.getenv("API_URL") or os.getenv("OPENAI_API_BASE"),
                'api_key': os.getenv("API_KEY") or os.getenv("OPENAI_API_KEY")
            }
    
    def extract(self, text_contents: List[str], schema: Dict[str, Any], 
                table_name: str, nl_prompt: str = "", context=None, 
                warm_start: bool = False, previous_snapshot=None, 
                violations: List = None, full_schema: Dict[str, Any] = None) -> List[TableRow]:
        """从文本内容中提取表格数据
        
        Args:
            text_contents: 文本内容列表（已由Orchestrator分块处理）
            schema: 数据库架构定义（通常是表特定的schema）
            table_name: 目标表名
            nl_prompt: 自然语言提示
            context: 处理上下文
            warm_start: 是否为暖启动模式
            previous_snapshot: 之前的数据快照
            violations: 需要重新提取的违规列表
            full_schema: 完整的数据库架构定义（包含所有表和关系定义）
            
        Returns:
            提取的表格行列表
        """
        self.logger.info(f"开始提取表格 {table_name} 的数据")
        
        if isinstance(schema, str):
            if not schema.strip():
                actual_schema = self.utils.generate_default_schema_from_prompt(table_name, nl_prompt)
            else:
                try:
                    actual_schema = json.loads(schema)
                except Exception as e:
                    raise ValueError(f"Schema格式错误: {e}")
        else:
            actual_schema = schema
        
        table_def = self._get_table_definition(actual_schema, table_name)
        if not table_def:
            raise ValueError(f"未找到表格定义: {table_name}")
        
        if self.enable_locate and self.locator and not warm_start:
            self.logger.info(f"启用文档定位，处理 {len(text_contents)} 个文档")
            
            document_names = []
            if context and hasattr(context, 'batch_document_names') and context.batch_document_names:
                document_names = context.batch_document_names
            elif context and hasattr(context, 'documents') and context.documents:
                document_names = [os.path.basename(doc_path) for doc_path in context.documents]
            else:
                document_names = [f"doc_{i+1}" for i in range(len(text_contents))]
            
            located_segments = self.locator.locate_from_multi_documents(
                text_contents=text_contents,
                document_names=document_names,
                schema=actual_schema,
                table_name=table_name,
                nl_prompt=nl_prompt,
                context=context
            )
            
            located_text = self.locator.merge_segments(located_segments)
            
            if context:
                context.located_segments = located_segments
                self.logger.info(f'已保存 {len(located_segments)} 个segments到context')
            
            text_contents = [located_text]
            
            summary = self.locator.get_segments_summary(located_segments)
            self.logger.info(
                f'Locate完成：{len(located_segments)} 个片段，'
                f'总长度 {len(located_text)}'
            )
        
        document_names = []
        if context and hasattr(context, 'documents'):
            document_names = [os.path.basename(doc_path) for doc_path in context.documents]
        
        if False and not warm_start:  # 暂时禁用document_filter机制
            text_contents, document_names = self._apply_document_filter(
                text_contents, document_names, table_def
            )
            
            if not text_contents:
                self.logger.warning(f"⚠️ 文档过滤后没有匹配的文档，返回空结果")
                return []
        
        all_rows = []
        
        if warm_start and violations and previous_snapshot and len(text_contents) > 1:
            self.logger.info(f"🔄 [Warm Start批量] 一次性处理 {len(text_contents)} 个文档的修复")
            
            try:
                all_rows = self._batch_warm_start_fix(
                    text_contents, table_def, nl_prompt, document_names,
                    context, previous_snapshot, violations
                )
                self.logger.info(f"✅ [Warm Start批量] 完成，共修复 {len(all_rows)} 行数据")
                return all_rows
            except Exception as e:
                self.logger.error(f"❌ [Warm Start批量] 失败: {e}，降级到逐个处理")
        
        current_snapshot = previous_snapshot
        
        for i, text_content in enumerate(text_contents):
            doc_name = document_names[i] if i < len(document_names) else f"文档{i+1}"
            
            if not text_content or not text_content.strip():
                self.logger.warning(f"文本内容 {i} ({doc_name}) 为空，跳过")
                continue
            
            try:
                if warm_start and violations and current_snapshot:
                    doc_rows = self._extract_with_warm_start(
                        text_content, table_def, nl_prompt, doc_name, context, i,
                        current_snapshot, violations
                    )
                else:
                    doc_rows = self._extract_with_cold_start(
                        text_content, table_def, nl_prompt, doc_name, context, i, full_schema
                    )
                
                all_rows.extend(doc_rows)
                
                if warm_start and doc_rows:
                    from ...memory.snapshot import TableSnapshot
                    from ...core.io import IOManager
                    current_snapshot = TableSnapshot(
                        run_id=f"temp_fix_{i}",
                        table=table_name,
                        rows=all_rows,
                        created_at=IOManager.get_timestamp(),
                        table_id=previous_snapshot.table_id if hasattr(previous_snapshot, 'table_id') else table_name
                    )
                
            except Exception as e:
                self.logger.error(f"❌ 处理文本内容 {i} ({doc_name}) 失败: {e}")
                continue
        
        all_rows = self.utils.deduplicate_rows(all_rows)
        
        self.logger.info(f"✅ 共提取 {len(all_rows)} 行数据")
        return all_rows
    
    def _extract_with_cold_start(self, text_content: str, table_def: Dict[str, Any],
                               nl_prompt: str, doc_source: str, context=None, 
                               doc_index: int = 0, full_schema: Dict[str, Any] = None) -> List[TableRow]:
        """
        Cold Start提取入口 - 根据表类型分派到实体提取或关系提取
        
        Args:
            text_content: 文本内容
            table_def: 表定义
            nl_prompt: 自然语言提示
            doc_source: 文档来源
            context: 处理上下文
            doc_index: 文档索引
            full_schema: 完整schema
            
        Returns:
            提取的表格行列表
        """
        table_type = table_def.get('type', 'entity').lower()
        table_name = table_def.get('name', 'data_table')
        
        relation_extraction_config = table_def.get('relation_extraction', {})
        is_relation_table = (table_type in ['relation', 'relationship'] or 
                            relation_extraction_config.get('enabled', False))
        
        if is_relation_table:
            self.logger.info(f"🔗 [Cold Start] 关系表提取: {table_name}")
            return self.relation_extractor.extract_cold_start(
                text_content, table_def, nl_prompt, doc_source, 
                context, doc_index, full_schema,
                get_model_api_config_func=self._get_model_api_config,
                create_list_model_func=self._create_list_model_from_schema,
                build_schema_context_func=self._build_schema_context,
                parse_structured_response_func=self._parse_structured_response,
                record_extraction_step_func=self._record_extraction_step,
                fallback_func=self._extract_with_fallback
            )
        else:
            self.logger.info(f"📦 [Cold Start] 实体表提取: {table_name}")
            return self.entity_extractor.extract_cold_start(
                text_content, table_def, nl_prompt, doc_source, 
                context, doc_index, full_schema,
                get_model_api_config_func=self._get_model_api_config,
                create_list_model_func=self._create_list_model_from_schema,
                build_schema_context_func=self._build_schema_context,
                parse_structured_response_func=self._parse_structured_response,
                record_extraction_step_func=self._record_extraction_step,
                fallback_func=self._extract_with_fallback
            )
    
    def _extract_with_warm_start(self, text_content: str, table_def: Dict[str, Any],
                                nl_prompt: str, doc_source: str, context=None, 
                                doc_index: int = 0, previous_snapshot=None, 
                                violations: List = None) -> List[TableRow]:
        """
        Warm Start提取入口 - 根据表类型分派到实体或关系的warm start提取
        
        Args:
            text_content: 文本内容
            table_def: 表定义
            nl_prompt: 自然语言提示
            doc_source: 文档来源
            context: 处理上下文
            doc_index: 文档索引
            previous_snapshot: 之前的数据快照
            violations: 需要修复的违规列表
            
        Returns:
            修复后的表格行列表
        """
        table_type = table_def.get('type', 'entity').lower()
        table_name = table_def.get('name') or table_def.get('table_name', 'unknown')
        
        relation_extraction_config = table_def.get('relation_extraction', {})
        is_relation_table = (table_type in ['relation', 'relationship'] or 
                            relation_extraction_config.get('enabled', False))
        
        if is_relation_table:
            self.logger.info(f"🔗 [Warm Start] 关系表修复: {table_name}")
            return self.relation_extractor.extract_warm_start(
                text_content, table_def, nl_prompt, doc_source, 
                context, doc_index, previous_snapshot, violations,
                check_warm_start_limit_func=self._check_warm_start_limit,
                identify_cells_from_violations_func=self._identify_cells_from_violations,
                build_cell_fix_prompt_func=self._build_cell_fix_prompt,
                parse_cell_fix_json_func=self.utils.parse_cell_fix_json,
                apply_cell_fixes_func=self._apply_cell_fixes
            )
        else:
            self.logger.info(f"📦 [Warm Start] 实体表修复: {table_name}")
            return self.entity_extractor.extract_warm_start(
                text_content, table_def, nl_prompt, doc_source, 
                context, doc_index, previous_snapshot, violations,
                check_warm_start_limit_func=self._check_warm_start_limit,
                identify_cells_from_violations_func=self._identify_cells_from_violations,
                build_cell_fix_prompt_func=self._build_cell_fix_prompt,
                parse_cell_fix_json_func=self.utils.parse_cell_fix_json,
                apply_cell_fixes_func=self._apply_cell_fixes
            )
    
    def _extract_with_fallback(self, text_content: str, table_def: Dict[str, Any],
                              nl_prompt: str, doc_source: str, context=None,
                              doc_index: int = 0) -> List[TableRow]:
        """降级提取策略 - 使用简单的Markdown表格格式"""
        self.logger.info(f"[降级提取] 使用Markdown表格格式")
        
        rows = []
        attributes = table_def.get('attributes', [])
        
        schema_info = self.utils.build_simple_schema_prompt(table_def)
        
        system_prompt = """You are a professional data extraction expert. 
Your role is to read documents carefully and extract structured data with high accuracy and recall."""
        
        prompt = f"""Extract structured data from the following document according to the schema.

{schema_info}

Return the data in a Markdown table format:

<TABLE BEGIN>
| {attributes[0]['name'] if attributes else 'field1'} | {attributes[1]['name'] if len(attributes) > 1 else 'field2'} | ... |
|---|---|---|
| value1 | value2 | ... |
<TABLE END>

Document Content:
{text_content}

Additional Instructions: {nl_prompt if nl_prompt else 'No special requirements'}
"""
        
        try:
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            llm_response = get_answer(prompt, system_prompt=system_prompt, model=model)
            
            extracted_data = self.utils.parse_markdown_table_response(llm_response, table_def, doc_source)
            rows.extend(extracted_data)
            
            self.logger.info(f"✅ [降级提取] 获得 {len(extracted_data)} 行数据")
            
        except Exception as e:
            self.logger.error(f"❌ [降级提取] 失败: {e}")
        
        return rows
    
    def _parse_structured_response(self, llm_response: str, fixing_parser, 
                                   table_name: str, doc_source: str) -> List[TableRow]:
        """解析LLM的结构化响应并转换为TableRow格式"""
        try:
            result = fixing_parser.parse(llm_response)
            
            rows_data = []
            if hasattr(result, 'rows'):
                for row in result.rows:
                    if hasattr(row, 'model_dump'):
                        row_dict = row.model_dump()
                    elif hasattr(row, 'dict'):
                        row_dict = row.dict()
                    else:
                        row_dict = dict(row)
                    rows_data.append(row_dict)
            
            self.logger.info(f"✅ 成功解析 {len(rows_data)} 行数据")
            
            rows = []
            for i, row_dict in enumerate(rows_data):
                tuple_id = IdGenerator.generate_tuple_id(table_name, i, row_dict)
                
                cells = {}
                for field_name, value in row_dict.items():
                    if value is not None:
                        cells[field_name] = CellData(
                            value=value,
                            evidences=[doc_source]
                        )
                
                if cells:
                    table_row = TableRow(tuple_id=tuple_id, cells=cells)
                    rows.append(table_row)
            
            return rows
            
        except Exception as parse_error:
            self.logger.error(f"❌ 解析失败: {parse_error}")
            return []
    
    def _record_extraction_step(self, context, table_name: str, doc_index: int, 
                                doc_source: str, extraction_mode: str, model: str,
                                prompt: str, llm_response: str, row_count: int):
        """记录提取步骤到context"""
        if context and hasattr(context, 'step_outputs'):
            from ...core.io import IOManager
            context.step_outputs.append({
                'step': f'extractor_{table_name}_{doc_index}',
                'status': 'completed',
                'description': f"🚀 文档 {doc_index+1} ({os.path.basename(doc_source)}) [表: {table_name}]",
                'details': {
                    'document': os.path.basename(doc_source),
                    'table_name': table_name,
                    'extraction_mode': extraction_mode,
                    'extracted_rows': row_count,
                    'model_used': model,
                    'llm_input': prompt[:1000] + '...' if len(prompt) > 1000 else prompt,
                    'llm_output': llm_response[:1000] + '...' if len(llm_response) > 1000 else llm_response,
                },
                'timestamp': IOManager.get_timestamp()
            })
    
    def _check_warm_start_limit(self, context, table_name: str) -> bool:
        """检查warm start限制，返回是否可以继续"""
        if context and hasattr(context, 'coordinator'):
            coordinator = context.coordinator
            current_batch_index = getattr(context, 'current_batch_index', 1)
            
            if not coordinator.get_table_tracker(table_name):
                coordinator.init_table_tracker(table_name, total_batches=1)
            
            table_tracker = coordinator.get_table_tracker(table_name)
            if current_batch_index not in table_tracker.batch_trackers:
                coordinator.init_batch_tracker(table_name, current_batch_index, 1)
            
            if not coordinator.can_batch_warm_start(table_name, current_batch_index):
                self.logger.warning(f'表 {table_name} batch {current_batch_index} 已达到最大warm start尝试次数')
                return False
            
            coordinator.increment_batch_warm_start_count(table_name, current_batch_index)
        
        return True
    
    def _batch_warm_start_fix(self, text_contents: List[str], table_def: Dict[str, Any],
                             nl_prompt: str, document_names: List[str],
                             context=None, previous_snapshot=None, 
                             violations: List = None) -> List[TableRow]:
        """批量Warm Start修复 - 一次性处理多个文档"""
        table_name = table_def.get('name') or table_def.get('table_name', 'unknown')
        
        if context and hasattr(context, 'coordinator'):
            coordinator = context.coordinator
            current_batch_index = getattr(context, 'current_batch_index', 1)
            
            if not coordinator.get_table_tracker(table_name):
                coordinator.init_table_tracker(table_name, total_batches=1)
            
            table_tracker = coordinator.get_table_tracker(table_name)
            if current_batch_index not in table_tracker.batch_trackers:
                coordinator.init_batch_tracker(table_name, current_batch_index, 1)
            
            if not coordinator.can_batch_warm_start(table_name, current_batch_index):
                self.logger.warning(f'表 {table_name} batch {current_batch_index} 已达到最大warm start尝试次数')
                return previous_snapshot.rows if previous_snapshot else []
            
            coordinator.increment_batch_warm_start_count(table_name, current_batch_index)
        
        cells_to_fix = self._identify_cells_from_violations(violations, previous_snapshot)
        
        if not cells_to_fix:
            return previous_snapshot.rows if previous_snapshot else []
        
        extraction_prompt = self._build_batch_cell_fix_prompt(
            cells_to_fix, table_def, previous_snapshot, nl_prompt, len(text_contents)
        )
        
        system_prompt = """You are a data extraction expert specializing in fixing data quality issues across multiple documents.
Your task is to analyze ALL documents and extract ONLY the specified field values that need correction.
Output in strict JSON format with the structure: {"fixes": [{"tuple_id": "...", "field": "...", "new_value": "..."}]}
"""
        
        try:
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            
            documents_section = "\n\nDocuments Content:\n"
            for i, (text_content, doc_name) in enumerate(zip(text_contents, document_names)):
                if text_content and text_content.strip():
                    documents_section += f"\n=== Document {i+1}: {doc_name} ===\n{text_content}\n"
            
            full_prompt = f"""{extraction_prompt}
{documents_section}

Remember: 
- Analyze ALL {len(text_contents)} documents
- Output ONLY in JSON format with the structure shown above
- Extract accurate values for each field that needs correction
"""
            
            llm_response = get_answer(full_prompt, system_prompt=system_prompt, model=model)
            
            fixes_data = self.utils.parse_cell_fix_json(llm_response)
            
            if not fixes_data:
                return previous_snapshot.rows if previous_snapshot else []
            
            doc_sources = ", ".join(document_names)
            updated_rows, fix_records = self._apply_cell_fixes(
                previous_snapshot.rows if previous_snapshot else [],
                fixes_data,
                f"[Batch: {doc_sources}]",
                table_name
            )
            
            if context and hasattr(context, 'step_outputs'):
                from ...core.io import IOManager
                context.step_outputs.append({
                    'step': f'extractor_batch_warm_start_{table_name}',
                    'status': 'completed',
                    'description': f"🚀 [批量Warm Start] 一次性处理 {len(text_contents)} 个文档 [表: {table_name}]",
                    'details': {
                        'documents_count': len(text_contents),
                        'documents': document_names,
                        'table_name': table_name,
                        'mode': 'batch_warm_start',
                        'cells_identified': len(cells_to_fix),
                        'cells_fixed': len(fixes_data),
                        'total_rows': len(updated_rows),
                        'model_used': model,
                    },
                    'timestamp': IOManager.get_timestamp()
                })
            
            return updated_rows
            
        except Exception as e:
            self.logger.error(f"❌ [批量Warm Start] 失败: {e}")
            raise
    
    
    def _get_table_definition(self, schema: Dict[str, Any], table_name: str) -> Optional[Dict[str, Any]]:
        """获取表格定义"""
        tables = schema.get('tables', [])
        
        for table in tables:
            if table.get('name') == table_name:
                result = table.copy()
                if 'fields' in result and 'attributes' not in result:
                    result['attributes'] = result.pop('fields')
                return result
        
        if '_' in table_name and table_name.rsplit('_', 1)[-1].isdigit():
            base_name = table_name.rsplit('_', 1)[0]
            suffix_num = int(table_name.rsplit('_', 1)[1])
            
            matching_tables = [t for t in tables if isinstance(t, dict) and t.get('name') == base_name]
            
            if matching_tables and 1 <= suffix_num <= len(matching_tables):
                target_table = matching_tables[suffix_num - 1]
                result = target_table.copy()
                if 'fields' in result and 'attributes' not in result:
                    result['attributes'] = result.pop('fields')
                return result
        
        return None
    
    def _create_pydantic_model_from_schema(self, schema: Dict[str, Any], 
                                          table_name: str) -> Optional[Type]:
        """从schema动态创建Pydantic模型"""
        if not STRUCTURED_OUTPUT_AVAILABLE:
            return None
        
        try:
            table_def = self._get_table_definition(schema, table_name)
            if not table_def:
                return None
            
            attributes = table_def.get('attributes', [])
            field_definitions = {}
            
            for attr in attributes:
                field_name = attr['name']
                field_type = attr.get('type', 'VARCHAR')
                field_desc = attr.get('description', '')
                
                if 'VARCHAR' in field_type or 'TEXT' in field_type or 'ENUM' in field_type or 'string' in field_type.lower():
                    python_type = str
                elif 'INT' in field_type or 'integer' in field_type.lower():
                    python_type = int
                elif 'DECIMAL' in field_type or 'FLOAT' in field_type or 'number' in field_type.lower():
                    python_type = float
                elif 'BOOLEAN' in field_type or 'boolean' in field_type.lower():
                    python_type = bool
                elif 'DATE' in field_type or 'date' in field_type.lower():
                    python_type = str
                else:
                    python_type = str
                
                if not attr.get('required', False):
                    python_type = Optional[python_type]
                
                field_definitions[field_name] = (python_type, Field(description=field_desc, default=None))
            
            model_class = create_model(table_name, **field_definitions)
            return model_class
            
        except Exception as e:
            self.logger.error(f"❌ 创建Pydantic模型失败: {e}")
            return None
    
    def _create_list_model_from_schema(self, schema: Dict[str, Any], 
                                      table_name: str) -> Optional[Type]:
        """创建包含多行数据的列表模型"""
        if not STRUCTURED_OUTPUT_AVAILABLE:
            return None
        
        try:
            row_model = self._create_pydantic_model_from_schema(schema, table_name)
            if not row_model:
                return None
            
            list_model = create_model(
                f"{table_name}List",
                rows=(List[row_model], Field(description=f"List of {table_name} records"))
            )
            
            return list_model
            
        except Exception as e:
            self.logger.error(f"❌ 创建列表模型失败: {e}")
            return None
    
    def _build_schema_context(self, table_def: Dict[str, Any], nl_prompt: str = "", full_schema: Dict[str, Any] = None) -> str:
        """构建schema上下文"""
        table_name = table_def.get('name', 'data_table')
        attributes = table_def.get('attributes', [])
        
        context_parts = []
        
        context_parts.append(f"Target Table: {table_name}")
        
        table_type = table_def.get('type', '')
        if table_type:
            context_parts.append(f"Type: {table_type}")
        
        table_desc = table_def.get('description', '')
        if table_desc:
            context_parts.append(f"Description: {table_desc}")
        
        if attributes:
            context_parts.append(f"Fields ({len(attributes)}):")
            for attr in attributes:
                field_name = attr.get('name', 'unknown')
                field_type = attr.get('type', 'VARCHAR')
                field_desc = attr.get('description', '')
                constraints = attr.get('constraints', {})
                
                constraint_info = []
                
                if constraints.get('primary_key'):
                    constraint_info.append('PK')
                if constraints.get('foreign_key'):
                    constraint_info.append('FK')
                if constraints.get('unique'):
                    constraint_info.append('UNIQUE')
                if not constraints.get('nullable', True):
                    constraint_info.append('NOT NULL')
                
                if attr.get('required', False) and 'NOT NULL' not in constraint_info:
                    constraint_info.append('REQUIRED')
                if attr.get('unique', False) and 'UNIQUE' not in constraint_info:
                    constraint_info.append('UNIQUE')
                
                if 'domain' in attr:
                    domain_values = attr['domain']
                    if isinstance(domain_values, list):
                        constraint_info.append(f"domain={domain_values}")
                    else:
                        constraint_info.append(f"domain={domain_values}")
                
                if 'format' in attr:
                    constraint_info.append(f"format='{attr['format']}'")
                
                if 'min' in attr:
                    constraint_info.append(f"min={attr['min']}")
                if 'max' in attr:
                    constraint_info.append(f"max={attr['max']}")
                
                constraint_str = f" [{', '.join(constraint_info)}]" if constraint_info else ""
                context_parts.append(f"  - {field_name} ({field_type}){constraint_str}: {field_desc}")
        
        if table_type and table_type.lower() == 'relationship' and full_schema:
            relations_info = self._build_relations_context(table_name, full_schema)
            if relations_info:
                context_parts.append("\nTable Relationships:")
                context_parts.append(relations_info)
        
        return "\n".join(context_parts)
    
    def _build_relations_context(self, table_name: str, full_schema: Dict[str, Any]) -> str:
        """构建表关系上下文信息（参考relation_extractor.py实现）
        
        从schema的relations部分提取与当前表相关的关系信息，
        帮助LLM理解该关系表连接了哪些实体表。
        """
        if not full_schema:
            return ""
        
        relations = full_schema.get('relations', [])
        if not relations:
            return ""
        
        relevant_relations = []
        for relation in relations:
            if not isinstance(relation, dict):
                continue
            
            from_info = relation.get('from', {})
            to_info = relation.get('to', {})
            
            if (from_info.get('table') == table_name or 
                to_info.get('table') == table_name):
                relevant_relations.append(relation)
        
        if not relevant_relations:
            return ""
        
        relation_lines = []
        for relation in relevant_relations:
            from_info = relation.get('from', {})
            to_info = relation.get('to', {})
            relation_type = relation.get('type', 'unknown')
            relation_id = relation.get('id', '')
            
            from_table = from_info.get('table', '')
            from_field = from_info.get('field', '')
            to_table = to_info.get('table', '')
            to_field = to_info.get('field', '')
            
            if from_table and from_field and to_table and to_field:
                relation_desc = f"  - {from_table}.{from_field} → {to_table}.{to_field} ({relation_type})"
                if relation_id:
                    relation_desc += f" [ID: {relation_id}]"
                relation_lines.append(relation_desc)
        
        return "\n".join(relation_lines)
    
    
    
    def _identify_cells_from_violations(self, violations: List, 
                                       previous_snapshot) -> List[Dict[str, Any]]:
        """从violations中识别需要修复的cells"""
        cells_to_fix = []
        
        if not violations or not previous_snapshot:
            return cells_to_fix
        
        rows_map = {row.tuple_id: row for row in previous_snapshot.rows}
        
        for violation in violations:
            tuple_id = getattr(violation, 'tuple_id', '')
            field = getattr(violation, 'attr', '')
            constraint_type = getattr(violation, 'constraint_type', '').upper()
            description = getattr(violation, 'description', '')
            
            current_value = None
            if tuple_id in rows_map and field in rows_map[tuple_id].cells:
                current_value = rows_map[tuple_id].cells[field].value
            
            cells_to_fix.append({
                'tuple_id': tuple_id,
                'field': field,
                'current_value': current_value,
                'violation_id': getattr(violation, 'id', ''),
                'violation_desc': description,
                'constraint_type': constraint_type
            })
        
        return cells_to_fix
    
    def _build_cell_fix_prompt(self, cells_to_fix: List[Dict[str, Any]], 
                              table_def: Dict[str, Any],
                              previous_snapshot, nl_prompt: str) -> str:
        """构建cell级修复的prompt"""
        table_name = table_def.get('name', 'unknown_table')
        
        rows_map = {}
        if previous_snapshot and previous_snapshot.rows:
            rows_map = {row.tuple_id: row for row in previous_snapshot.rows}
        
        cell_list = []
        for i, cell in enumerate(cells_to_fix[:20]):
            tuple_id = cell['tuple_id']
            field = cell['field']
            current_value = cell['current_value']
            issue = cell['violation_desc'][:100]
            
            row_context = ""
            if tuple_id in rows_map:
                row = rows_map[tuple_id]
                context_values = []
                for field_name, cell_data in row.cells.items():
                    if field_name != field:
                        context_values.append(f"{field_name}='{cell_data.value}'")
                if context_values:
                    row_context = f" | Row Context: {', '.join(context_values[:5])}"
            
            cell_list.append(
                f"  {i+1}. tuple_id: {tuple_id}, field: {field}, "
                f"current_value: {current_value}{row_context}\n"
                f"      Issue: {issue}"
            )
        
        if len(cells_to_fix) > 20:
            cell_list.append(f"  ... {len(cells_to_fix) - 20} more cells need to be fixed")
        
        attributes = table_def.get('attributes', [])
        field_definitions = []
        for attr in attributes:
            field_definitions.append(
                f"  - {attr['name']} ({attr.get('type', 'TEXT')}): {attr.get('description', 'N/A')}"
            )
        
        prompt = f"""🎯 Task: Fix problematic cell values in table [{table_name}]

【Field Definitions】
{chr(10).join(field_definitions)}

【Cells to Fix】Total: {len(cells_to_fix)} cells
{chr(10).join(cell_list)}

【Task Requirements】
1. Carefully read the document content to find clues related to the cells above
2. Locate cells to fix based on tuple_id + field
3. Use "row context" information to accurately match the corresponding data
4. Extract accurate corrected values from the document
5. If the information is not found in the document, output null

Output strictly in the following JSON format:
{{
  "fixes": [
    {{
      "tuple_id": "Original tuple_id from the data",
      "field": "Field name",
      "new_value": "Corrected value extracted from document (or null)"
    }}
  ]
}}

【Important Notes】
- Output ONLY JSON, do not add any other text
- tuple_id must exactly match those listed above
- field must exist in the field definitions
- Prevent mismatches: Use "row context" to accurately locate the data
"""
        
        if nl_prompt and nl_prompt.strip():
            prompt += f"\n\n【Additional User Requirements】\n{nl_prompt}\n"
        
        return prompt
    
    def _build_batch_cell_fix_prompt(self, cells_to_fix: List[Dict[str, Any]], 
                                    table_def: Dict[str, Any],
                                    previous_snapshot, nl_prompt: str,
                                    document_count: int) -> str:
        """构建批量cell修复的prompt"""
        table_name = table_def.get('name', 'unknown_table')
        
        rows_map = {}
        if previous_snapshot and previous_snapshot.rows:
            rows_map = {row.tuple_id: row for row in previous_snapshot.rows}
        
        cell_list = []
        for i, cell in enumerate(cells_to_fix[:50]):
            tuple_id = cell['tuple_id']
            field = cell['field']
            current_value = cell['current_value']
            issue = cell['violation_desc'][:100]
            
            row_context = ""
            if tuple_id in rows_map:
                row = rows_map[tuple_id]
                context_values = []
                for field_name, cell_data in row.cells.items():
                    if field_name != field:
                        context_values.append(f"{field_name}='{cell_data.value}'")
                if context_values:
                    row_context = f" | Row Context: {', '.join(context_values[:5])}"
            
            cell_list.append(
                f"  {i+1}. tuple_id: {tuple_id}, field: {field}, "
                f"current_value: {current_value}{row_context}\n"
                f"      Issue: {issue}"
            )
        
        if len(cells_to_fix) > 50:
            cell_list.append(f"  ... {len(cells_to_fix) - 50} more cells need to be fixed")
        
        attributes = table_def.get('attributes', [])
        field_definitions = []
        for attr in attributes:
            field_definitions.append(
                f"  - {attr['name']} ({attr.get('type', 'TEXT')}): {attr.get('description', 'N/A')}"
            )
        
        prompt = f"""🎯 Task: Batch fix problematic cell values in table [{table_name}]

📊 Document Count: {document_count} documents will be analyzed together

【Field Definitions】
{chr(10).join(field_definitions)}

【Cells to Fix】Total: {len(cells_to_fix)} cells
{chr(10).join(cell_list)}

【Task Requirements】
1. Carefully read the content of **ALL {document_count} documents**
2. Accurately locate cells to fix based on tuple_id + field
3. Use "row context" information to extract corresponding data from the correct document
4. If the information is not found in any document, output null

Output strictly in the following JSON format:
{{
  "fixes": [
    {{
      "tuple_id": "Original tuple_id from the data",
      "field": "Field name",
      "new_value": "Corrected value extracted from document (or null)"
    }}
  ]
}}

【Important Notes】
- Output ONLY JSON
- tuple_id must exactly match
- field must exist in the field definitions
- Prevent mismatches: Must use "row context" to accurately locate the data
"""
        
        if nl_prompt and nl_prompt.strip():
            prompt += f"\n\n【Additional User Requirements】\n{nl_prompt}\n"
        
        return prompt
    
    
    def _apply_cell_fixes(self, original_rows: List[TableRow], 
                         fixes_data: List[Dict[str, Any]],
                         doc_source: str,
                         table_name: str = None) -> tuple[List[TableRow], List]:
        """在cell级别应用修复"""
        rows_map = {row.tuple_id: row for row in original_rows}
        
        applied_count = 0
        skipped_count = 0
        fix_records = []
        
        for fix_data in fixes_data:
            tuple_id = fix_data.get('tuple_id', '')
            field = fix_data.get('field', '')
            new_value = fix_data.get('new_value')
            
            if new_value is None or new_value == 'null':
                skipped_count += 1
                continue
            
            if tuple_id not in rows_map:
                skipped_count += 1
                continue
            
            row = rows_map[tuple_id]
            old_value = None
            
            if field in row.cells:
                old_value = row.cells[field].value
                row.cells[field].value = new_value
                if hasattr(row.cells[field], 'evidences'):
                    row.cells[field].evidences.append(f"[Warm Start Fix] {doc_source}")
            else:
                old_value = None
                row.cells[field] = CellData(
                    value=new_value,
                    evidences=[f"[Warm Start Fix] {doc_source}"]
                )
            
            fix_record = Fix(
                id=IdGenerator.generate_fix_id(
                    table=table_name or 'unknown',
                    tuple_id=tuple_id,
                    attr=field,
                    fix_type='warm_start_extraction',
                    old_value=old_value
                ),
                table=table_name or 'unknown',
                tuple_id=tuple_id,
                attr=field,
                old=old_value,
                new=new_value,
                fix_type='warm_start_extraction',
                applied_by='BaseExtractor',
                timestamp=datetime.now().isoformat(),
                fix_success=True,
                failure_reason=''
            )
            fix_records.append(fix_record)
            
            applied_count += 1
        
        self.logger.info(f"✅ 应用修复：{applied_count} 个成功，{skipped_count} 个跳过")
        
        return list(rows_map.values()), fix_records
    
    
    def _apply_document_filter(self, text_contents: List[str], document_names: List[str], 
                               table_def: Dict[str, Any]) -> tuple:
        """根据表定义中的文档过滤配置，过滤文档列表
        
        Args:
            text_contents: 文档内容列表
            document_names: 文档名称列表
            table_def: 表定义
            
        Returns:
            (过滤后的text_contents, 过滤后的document_names)
        """
        doc_filter = None
        
        if 'relation_extraction' in table_def:
            relation_config = table_def['relation_extraction']
            if isinstance(relation_config, dict) and 'document_filter' in relation_config:
                doc_filter = relation_config['document_filter']
        
        if not doc_filter and 'document_filter' in table_def:
            doc_filter = table_def['document_filter']
        
        if not doc_filter:
            return text_contents, document_names
        
        mode = doc_filter.get('mode', 'include')  # include 或 exclude
        patterns = doc_filter.get('patterns', [])
        
        if not patterns:
            return text_contents, document_names
        
        self.logger.info(f"📋 应用文档过滤：mode={mode}, patterns={patterns}")
        self.logger.info(f"   输入文档数量: text_contents={len(text_contents)}, document_names={len(document_names)}")
        
        filtered_contents = []
        filtered_names = []
        
        for i, doc_name in enumerate(document_names):
            matches = any(pattern in doc_name for pattern in patterns)
            
            self.logger.debug(f"  文档: {doc_name}, 匹配: {matches}")
            for pattern in patterns:
                self.logger.debug(f"    检查pattern '{pattern}' in '{doc_name}': {pattern in doc_name}")
            
            should_keep = (mode == 'include' and matches) or (mode == 'exclude' and not matches)
            
            if should_keep:
                if i < len(text_contents):
                    filtered_contents.append(text_contents[i])
                    filtered_names.append(doc_name)
                    self.logger.info(f"  ✅ 保留文档: {doc_name}")
                else:
                    self.logger.warning(f"  ⚠️ 文档索引越界: i={i}, len(text_contents)={len(text_contents)}")
            else:
                self.logger.info(f"  ⊗ 过滤掉文档: {doc_name}")
        
        self.logger.info(f"📋 文档过滤完成：{len(document_names)} -> {len(filtered_names)} 个文档")
        
        return filtered_contents, filtered_names
    
    def supports_format(self, file_path: str) -> bool:
        """检查是否支持指定格式的文件"""
        supported_extensions = {'.txt', '.md', '.csv', '.json'}
        file_ext = Path(file_path).suffix.lower()
        return file_ext in supported_extensions
