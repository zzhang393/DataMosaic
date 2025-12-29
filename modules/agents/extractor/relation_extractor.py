"""
关系提取器模块
负责从文档中提取关系表数据（Cold Start和Warm Start）
"""
import os
import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from ...memory import TableRow, CellData
from ...core.ids import IdGenerator

from llm.main import get_answer

try:
    from pydantic import BaseModel, Field
    from langchain_core.output_parsers import PydanticOutputParser
    from langchain_openai import ChatOpenAI
    from langchain.output_parsers import OutputFixingParser
    STRUCTURED_OUTPUT_AVAILABLE = True
except ImportError:
    STRUCTURED_OUTPUT_AVAILABLE = False


class RelationExtractor:
    """关系提取器 - 处理关系表的数据提取"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('doc2db.extractor.relation')
        self.entity_mention_log_file = None
        self._mention_handler = None
    
    def _setup_entity_mention_logging(self, output_dir: str):
        """设置实体提及日志记录到文件"""
        if not output_dir or self._mention_handler:
            return  # 已经设置过或没有output_dir
        
        from pathlib import Path
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        self.entity_mention_log_file = output_path / 'entity_mentions.log'
        
        self._mention_handler = logging.FileHandler(self.entity_mention_log_file, mode='a', encoding='utf-8')
        self._mention_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self._mention_handler.setFormatter(formatter)
        
        if not any(isinstance(h, logging.FileHandler) and 
                  hasattr(h, 'baseFilename') and 
                  'entity_mentions.log' in h.baseFilename 
                  for h in self.logger.handlers):
            self.logger.addHandler(self._mention_handler)
            self.logger.setLevel(logging.DEBUG)
            self.logger.info(f"📝 实体提及日志文件已创建: {self.entity_mention_log_file}")
    
    def extract_cold_start(self, text_content: str, table_def: Dict[str, Any],
                          nl_prompt: str, doc_source: str, context=None,
                          doc_index: int = 0, full_schema: Dict[str, Any] = None,
                          get_model_api_config_func=None,
                          create_list_model_func=None,
                          build_schema_context_func=None,
                          parse_structured_response_func=None,
                          record_extraction_step_func=None,
                          fallback_func=None,
                          extraction_strategy: str = 'entity_anchored',
                          use_batch_processing: bool = True,
                          batch_size: int = 20) -> List[TableRow]:
        """
        关系表的Cold Start提取 - 支持多种提取策略
        
        Args:
            extraction_strategy: 提取策略选择
                - "entity_anchored": Entity-anchored blocked extraction
                - "global": 传统全局提取
                优先级：方法参数 > table_def['relation_extraction']['strategy'] > 默认值'global'
            use_batch_processing: 是否使用批处理模式（仅对entity_anchored策略有效）
                - True: 批量处理多个实体，减少LLM调用次数（推荐）
                - False: 逐个处理实体（兼容模式）
            batch_size: 批处理时每批的实体数量（默认20）
        
        Entity-anchored策略：
        1. 以第一个参考表为anchor（如Student表）
        2. 对每个anchor实体，调用LLM找到文档中提及它的句子
        3. 在这些句子中提取与该实体相关的关系
        4. 复杂度从O(m×n)降低到O(m)次LLM调用
        5. 批处理模式进一步降低到O(m/batch_size)次LLM调用
        
        Global策略：
        1. 一次性从整个文档提取所有关系
        2. 适合文档较短或关系较少的场景
        """
        if not STRUCTURED_OUTPUT_AVAILABLE:
            return fallback_func(text_content, table_def, nl_prompt, doc_source, context, doc_index)
        
        try:
            table_name = table_def.get('name', 'data_table')
            
            relation_extraction_config = table_def.get('relation_extraction', {})
            
            self.logger.info(f"🔍 [DEBUG] 开始处理关系表 {table_name}")
            self.logger.info(f"   原始text_content长度: {len(text_content)} 字符")
            self.logger.info(f"   relation_extraction配置: {relation_extraction_config.keys() if relation_extraction_config else 'None'}")
            
            text_content = self._load_document_sources(relation_extraction_config, text_content, context)
            
            self.logger.info(f"   加载后text_content长度: {len(text_content)} 字符")
            if '学生入学登记表' in text_content:
                self.logger.warning(f"   ⚠️ text_content中包含'学生入学登记表'，可能加载了错误的文档")
            if '教务系统运行日志' in text_content or '选课' in text_content:
                self.logger.info(f"   ✅ text_content中包含'教务系统运行日志'或'选课'相关内容，文档正确")
            
            reference_tables_data = None
            
            if relation_extraction_config.get('enabled', False):
                self.logger.info(f"🔗 [Entity-Anchored] 加载参考表数据...")
                reference_tables_data = self._load_reference_tables(relation_extraction_config, context)
                if reference_tables_data:
                    self.logger.info(f"✅ [Entity-Anchored] 成功加载 {len(reference_tables_data)} 个参考表")
            
            if not reference_tables_data and context:
                reference_tables_data = self._get_entity_snapshots_as_reference(context, table_def)
                if reference_tables_data:
                    self.logger.info(f"✅ [Entity-Anchored] 从context获取 {len(reference_tables_data)} 个实体表数据")
            
            strategy = extraction_strategy

            
            if strategy == 'entity_anchored':
                if not reference_tables_data:
                    self.logger.warning(f"⚠️ 配置要求使用entity_anchored策略，但没有参考表数据，降级为全局提取")
                    strategy = 'global'
                else:
                    self.logger.info(f"📌 [策略选择] 使用Entity-Anchored策略（策略配置: {strategy}）")
            
            if strategy == 'global':
                self.logger.info(f"📌 [策略选择] 使用全局提取策略（策略配置: {strategy}）")
                return self._extract_global(
                    text_content, table_def, nl_prompt, doc_source, context, doc_index,
                    full_schema, reference_tables_data, get_model_api_config_func, 
                    create_list_model_func, build_schema_context_func, 
                    parse_structured_response_func, record_extraction_step_func, fallback_func
                )
            
            anchor_table_name = list(reference_tables_data.keys())[0]
            anchor_entities = reference_tables_data[anchor_table_name]
            
            self.logger.info(
                f"🎯 [Entity-Anchored] 使用锚点表: {anchor_table_name}, "
                f"共 {len(anchor_entities)} 个锚点实体"
            )
            
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            api_config = get_model_api_config_func(model)
            
            all_relation_rows = []
            
            if use_batch_processing:

                anchor_entities_with_mentions = self._find_entity_mentions_via_llm_batch(
                    text_content, anchor_entities, anchor_table_name, 
                    model, api_config, context, batch_size=batch_size
                )
                
                all_relation_rows = self._extract_relations_for_anchors_batch(
                    anchor_entities_with_mentions, table_def,
                    reference_tables_data, relation_extraction_config,
                    model, api_config, doc_source,
                    create_list_model_func, build_schema_context_func,
                    parse_structured_response_func, full_schema,
                    batch_size=batch_size
                )
                
            else:
                self.logger.info(f"🐌 [逐个处理模式] 使用传统串行处理")
                
                for i, anchor_entity in enumerate(anchor_entities):
                    self.logger.info(
                        f"  [{i+1}/{len(anchor_entities)}] 处理锚点: {anchor_entity}"
                    )
                    
                    relevant_sentences = self._find_entity_mentions_via_llm(
                        text_content, anchor_entity, anchor_table_name, model, api_config, context
                    )
                    
                    if not relevant_sentences:
                        self.logger.debug(f"    未找到提及该实体的句子，跳过")
                        continue
                    
                    self.logger.info(f"    找到 {len(relevant_sentences)} 个相关句子")
                    
                    rows = self._extract_relations_for_anchor(
                        anchor_entity, relevant_sentences, table_def, 
                        reference_tables_data, relation_extraction_config,
                        model, api_config, doc_source,
                        create_list_model_func, build_schema_context_func,
                        parse_structured_response_func, full_schema
                    )
                    
                    all_relation_rows.extend(rows)
                    self.logger.info(f"    提取到 {len(rows)} 条关系记录")
            
            unique_rows = self._deduplicate_relation_rows(all_relation_rows)
            
            self.logger.info(
                f"✅ [Entity-Anchored] 完成：{len(all_relation_rows)} 条原始记录 "
                f"-> {len(unique_rows)} 条去重后记录"
            )
            
            record_extraction_step_func(
                context, table_name, doc_index, doc_source, 
                'entity_anchored_relation_extraction', model, 
                f"Entity-Anchored extraction with {len(anchor_entities)} anchors",
                f"Extracted {len(unique_rows)} unique relations", 
                len(unique_rows)
            )
            
            return unique_rows
            
        except Exception as e:
            self.logger.error(f"❌ [Entity-Anchored] 异常: {e}")
            import traceback
            traceback.print_exc()
            return fallback_func(text_content, table_def, nl_prompt, doc_source, context, doc_index)
    
    def extract_warm_start(self, text_content: str, table_def: Dict[str, Any],
                          nl_prompt: str, doc_source: str, context=None,
                          doc_index: int = 0, previous_snapshot=None,
                          violations: List = None,
                          check_warm_start_limit_func=None,
                          get_model_api_config_func=None,
                          create_list_model_func=None,
                          build_schema_context_func=None,
                          parse_structured_response_func=None,
                          record_extraction_step_func=None,
                          full_schema: Dict[str, Any] = None) -> List[TableRow]:
        """
        关系表的Warm Start提取 - Entity-Anchored增量重提取
        
        论文逻辑：
        1. 从violation识别涉及的anchor entities
        2. 只针对这些entities重新运行entity-anchored提取
        3. 移除受影响的旧记录，添加新记录，保留无关记录
        
        Args:
            text_content: 文档内容
            table_def: 表定义
            nl_prompt: 自然语言提示
            doc_source: 文档来源
            context: 处理上下文
            doc_index: 文档索引
            previous_snapshot: 上一次快照
            violations: 违规列表
            check_warm_start_limit_func: 检查warm start限制的函数
            get_model_api_config_func: 获取模型API配置函数
            create_list_model_func: 创建列表模型函数
            build_schema_context_func: 构建schema上下文函数
            parse_structured_response_func: 解析结构化响应函数
            record_extraction_step_func: 记录提取步骤函数
            full_schema: 完整schema定义
            
        Returns:
            更新后的TableRow列表
        """
        table_name = table_def.get('name') or table_def.get('table_name', 'unknown')
        
        if not check_warm_start_limit_func(context, table_name):
            return previous_snapshot.rows if previous_snapshot else []
        
        self.logger.info(f"🔄 [关系Warm Start] 开始Entity-Anchored增量重提取 - 表: {table_name}")
        
        affected_anchor_entities = self._identify_affected_anchors(
            violations, previous_snapshot, table_def, context
        )
        
        if not affected_anchor_entities:
            self.logger.info(f"⚠️ [关系Warm Start] 未识别到受影响的anchor entities，保持原快照")
            return previous_snapshot.rows if previous_snapshot else []
        
        self.logger.info(
            f"🎯 [关系Warm Start] 识别到 {len(affected_anchor_entities)} 个受影响的anchor entities"
        )
        
        relation_extraction_config = table_def.get('relation_extraction', {})
        text_content = self._load_document_sources(relation_extraction_config, text_content, context)
        
        reference_tables_data = None
        if relation_extraction_config.get('enabled', False):
            reference_tables_data = self._load_reference_tables(relation_extraction_config, context)
        
        if not reference_tables_data and context:
            reference_tables_data = self._get_entity_snapshots_as_reference(context, table_def)
        
        if not reference_tables_data:
            self.logger.warning(f"⚠️ [关系Warm Start] 无参考表数据，无法执行entity-anchored重提取，保持原快照")
            return previous_snapshot.rows if previous_snapshot else []
        
        model = context.model if context and hasattr(context, 'model') else "gpt-4o"
        api_config = get_model_api_config_func(model)
        
        re_extracted_rows = []
        anchor_table_name = list(reference_tables_data.keys())[0]
        
        for i, anchor_entity in enumerate(affected_anchor_entities):
            anchor_desc = ", ".join([f"{k}={v}" for k, v in anchor_entity.items()])
            self.logger.info(f"  [{i+1}/{len(affected_anchor_entities)}] 🔄 重新提取anchor: {anchor_desc}")
            
            relevant_sentences = self._find_entity_mentions_via_llm(
                text_content, anchor_entity, anchor_table_name,
                model, api_config, context
            )
            
            if relevant_sentences:
                rows = self._extract_relations_for_anchor(
                    anchor_entity, relevant_sentences, table_def,
                    reference_tables_data, relation_extraction_config,
                    model, api_config, doc_source,
                    create_list_model_func, build_schema_context_func,
                    parse_structured_response_func, full_schema
                )
                re_extracted_rows.extend(rows)
                self.logger.info(f"    提取到 {len(rows)} 条关系")
            else:
                self.logger.info(f"    未找到相关文本，该anchor无关系记录")
        
        updated_rows = self._merge_warm_start_results(
            previous_snapshot.rows if previous_snapshot else [],
            re_extracted_rows,
            affected_anchor_entities,
            table_def
        )
        
        self.logger.info(
            f"✅ [关系Warm Start] 完成：原 {len(previous_snapshot.rows if previous_snapshot else [])} 条 "
            f"-> 重提取 {len(re_extracted_rows)} 条 -> 最终 {len(updated_rows)} 条记录"
        )
        
        if record_extraction_step_func:
            record_extraction_step_func(
                context, table_name, doc_index, doc_source,
                'entity_anchored_warm_start', model,
                f"Re-extracted {len(affected_anchor_entities)} affected anchors",
                f"Updated to {len(updated_rows)} rows (re-extracted {len(re_extracted_rows)})",
                len(updated_rows)
            )
        
        return updated_rows
    
    def _get_entity_snapshots_as_reference(self, context, table_def: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """从context中获取已抽取的实体表数据作为参考
        
        用于关系提取时，将已抽取的实体表数据转换为参考格式
        """
        reference_data = {}
        
        if not hasattr(context, 'all_snapshots') or not context.all_snapshots:
            return reference_data
        
        attributes = table_def.get('attributes', table_def.get('fields', []))
        referenced_tables = set()
        
        for attr in attributes:
            constraints = attr.get('constraints', {})
            if constraints.get('foreign_key'):
                field_name = attr.get('name', '')
                if field_name.endswith('ID') or field_name.endswith('_id'):
                    ref_table = field_name[:-2] if field_name.endswith('ID') else field_name[:-3]
                    if ref_table:
                        referenced_tables.add(ref_table)
        
        for table_name, snapshot in context.all_snapshots.items():
            if any(table_name.lower() == ref.lower() for ref in referenced_tables):
                rows_data = []
                if hasattr(snapshot, 'rows') and snapshot.rows:
                    for row in snapshot.rows:
                        row_dict = {}
                        for field_name, cell_data in row.cells.items():
                            if hasattr(cell_data, 'value'):
                                row_dict[field_name] = cell_data.value
                        if row_dict:
                            rows_data.append(row_dict)
                
                if rows_data:
                    reference_data[table_name] = rows_data
                    self.logger.info(f"📋 从context获取参考表 {table_name}: {len(rows_data)} 行")
        
        return reference_data
    
    def _find_project_root(self) -> Optional[Path]:
        """查找项目根目录（包含dataset目录的目录）
        
        从当前目录向上查找，直到找到包含dataset目录的目录
        """
        current = Path.cwd()
        max_depth = 5  # 最多向上查找5层
        
        for _ in range(max_depth):
            if (current / 'dataset').exists():
                return current
            
            parent = current.parent
            if parent == current:  # 已到根目录
                break
            current = parent
        
        return None
    
    def _load_document_sources(self, relation_config: Dict[str, Any], 
                               default_text_content: str, context=None) -> str:
        """从配置的document_sources加载文档内容
        
        如果配置了document_sources，直接从指定路径读取文档，忽略传入的text_content。
        这样可以绕过orchestrator的batch合并逻辑，精确控制关系表的数据源。
        
        Args:
            relation_config: relation_extraction配置
            default_text_content: 默认的文本内容（如果没有配置document_sources则使用）
            context: 处理上下文
            
        Returns:
            文档内容（可能是从document_sources加载的，也可能是默认的）
        """
        self.logger.info(f"🔍 [_load_document_sources] 开始检查document_sources配置")
        self.logger.info(f"   relation_config类型: {type(relation_config)}, 内容: {relation_config}")
        
        if not relation_config:
            self.logger.info(f"   ⚠️ relation_config为空，使用默认text_content")
            return default_text_content
        
        document_sources = relation_config.get('document_sources', [])
        self.logger.info(f"   检测到document_sources: {document_sources}")
        
        if not document_sources:
            self.logger.info(f"   ⚠️ document_sources为空列表，使用默认text_content")
            return default_text_content
        
        self.logger.info(f"🎯 检测到document_sources配置，将直接读取指定文档: {document_sources}")
        
        merged_content = []
        
        for doc_source in document_sources:
            try:
                doc_path = Path(doc_source)
                
                if not doc_path.is_absolute():
                    if context and hasattr(context, 'workspace_path'):
                        doc_path = Path(context.workspace_path) / doc_source
                    else:
                        candidate1 = Path.cwd() / doc_source
                        candidate2 = Path.cwd().parent / doc_source if Path.cwd().name == 'backend' else None
                        project_root = self._find_project_root()
                        candidate3 = project_root / doc_source if project_root else None
                        
                        if candidate1.exists():
                            doc_path = candidate1
                        elif candidate2 and candidate2.exists():
                            doc_path = candidate2
                            self.logger.info(f"✅ 使用项目根目录路径: {doc_path}")
                        elif candidate3 and candidate3.exists():
                            doc_path = candidate3
                            self.logger.info(f"✅ 找到项目根目录: {doc_path}")
                        else:
                            doc_path = candidate1  # 默认使用第一个候选
                
                if not doc_path.exists():
                    self.logger.error(f"❌ 文档源不存在: {doc_path}")
                    continue
                
                with open(doc_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if content:
                    merged_content.append(f"=== Document: {doc_path.name} ===\n")
                    merged_content.append(content)
                    merged_content.append(f"\n=== End of {doc_path.name} ===\n\n")
                    self.logger.info(f"✅ 成功加载文档: {doc_path.name} ({len(content)} 字符)")
                
            except Exception as e:
                self.logger.error(f"❌ 加载文档失败 {doc_source}: {e}")
                continue
        
        if merged_content:
            result = "\n".join(merged_content)
            self.logger.info(f"✅ 从document_sources加载了 {len(document_sources)} 个文档，总计 {len(result)} 字符")
            return result
        else:
            self.logger.warning(f"⚠️ document_sources配置的文档都加载失败，降级使用传入的text_content")
            return default_text_content
    
    def _load_reference_tables(self, relation_config: Dict[str, Any], 
                              context=None) -> Dict[str, List[Dict[str, Any]]]:
        """加载关系抽取所需的参考表数据
        
        Args:
            relation_config: relation_extraction配置
            context: 提取上下文（可能包含workspace路径等信息）
            
        Returns:
            字典，key为表名，value为该表的数据列表
        """
        reference_tables = {}
        
        reference_configs = relation_config.get('reference_tables', [])
        if not reference_configs:
            return reference_tables
        
        for ref_config in reference_configs:
            table_name = ref_config.get('table')
            data_source = ref_config.get('data_source')
            key_fields = ref_config.get('key_fields', [])
            
            if not table_name or not data_source:
                self.logger.warning(f"⚠️ 参考表配置不完整，跳过: {ref_config}")
                continue
            
            try:
                data_path = Path(data_source)
                if not data_path.is_absolute():
                    if context and hasattr(context, 'workspace_path'):
                        data_path = Path(context.workspace_path) / data_source
                    else:
                        candidate1 = Path.cwd() / data_source
                        candidate2 = Path.cwd().parent / data_source if Path.cwd().name == 'backend' else None
                        candidate3 = self._find_project_root() / data_source if self._find_project_root() else None
                        
                        if candidate1.exists():
                            data_path = candidate1
                        elif candidate2 and candidate2.exists():
                            data_path = candidate2
                            self.logger.info(f"✅ 使用项目根目录路径: {data_path}")
                        elif candidate3 and candidate3.exists():
                            data_path = candidate3
                            self.logger.info(f"✅ 找到项目根目录: {data_path}")
                        else:
                            data_path = candidate1  # 默认使用第一个候选
                
                if not data_path.exists():
                    self.logger.error(f"❌ 参考表数据文件不存在: {data_path}")
                    if not data_path.is_absolute():
                        self.logger.error(f"   提示: 当前工作目录为 {Path.cwd()}")
                    continue
                
                with open(data_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if table_name in data:
                    table_data = data[table_name]
                    
                    reference_tables[table_name] = table_data
                    
                    fields_info = f" (key_fields: {key_fields})" if key_fields else ""
                    self.logger.info(f"✅ 加载参考表 {table_name}: {len(table_data)} 行{fields_info}")
                else:
                    self.logger.warning(f"⚠️ 数据文件中未找到表 {table_name}")
                
            except Exception as e:
                self.logger.error(f"❌ 加载参考表 {table_name} 失败: {e}")
                continue
        
        return reference_tables
    
    def _build_reference_tables_prompt(self, reference_tables_data: Dict[str, List[Dict[str, Any]]], 
                                       relation_config: Dict[str, Any]) -> str:
        """构建包含参考表数据的prompt部分
        
        Args:
            reference_tables_data: 参考表数据字典
            relation_config: relation_extraction配置
            
        Returns:
            格式化的prompt字符串
        """
        if not reference_tables_data:
            return ""
        
        prompt_parts = ["\n【Reference Tables for Relationship Extraction】"]
        
        extraction_hint = relation_config.get('extraction_hint', '')
        if extraction_hint:
            prompt_parts.append(f"\nTask: {extraction_hint}\n")
        
        for table_name, table_data in reference_tables_data.items():
            if not table_data:
                continue
            
            table_config = None
            for ref_config in relation_config.get('reference_tables', []):
                if ref_config.get('table') == table_name:
                    table_config = ref_config
                    break
            
            prompt_parts.append(f"\n{table_name} Table:")
            
            if table_config and table_config.get('description'):
                prompt_parts.append(f"  Description: {table_config['description']}")
            
            if table_data:
                fields = list(table_data[0].keys())
                
                header = "  | " + " | ".join(fields) + " |"
                separator = "  |" + "|".join(["-" * (len(f) + 2) for f in fields]) + "|"
                prompt_parts.append(header)
                prompt_parts.append(separator)
                
                max_rows = min(50, len(table_data))
                for row in table_data[:max_rows]:
                    values = [str(row.get(f, '')) for f in fields]
                    data_row = "  | " + " | ".join(values) + " |"
                    prompt_parts.append(data_row)
                
                if len(table_data) > max_rows:
                    prompt_parts.append(f"  ... (total {len(table_data)} rows)")
            
            prompt_parts.append("")  # 空行分隔
        
        prompt_parts.append("IMPORTANT Instructions for Relationship Extraction:")
        prompt_parts.append("- Use the reference tables above to match entity names/descriptions to their IDs")
        prompt_parts.append("- For foreign key fields, MUST use the exact ID values from the reference tables")
        
        matching_instructions = []
        for table_name, table_data in reference_tables_data.items():
            if table_data:
                sample_row = table_data[0]
                id_field = None
                name_field = None
                
                for field in sample_row.keys():
                    if 'id' in field.lower():
                        id_field = field
                    elif 'name' in field.lower():
                        name_field = field
                
                if id_field and name_field:
                    matching_instructions.append(
                        f"- Match {table_name} entities by their {name_field} to {table_name}.{id_field}"
                    )
        
        if matching_instructions:
            prompt_parts.extend(matching_instructions)
        
        prompt_parts.append("- Extract ONLY the relationships (ID pairs) that are explicitly mentioned in the document")
        prompt_parts.append("")
        
        return "\n".join(prompt_parts)
    
    def _extract_global(self, text_content: str, table_def: Dict[str, Any],
                       nl_prompt: str, doc_source: str, context=None,
                       doc_index: int = 0, full_schema: Dict[str, Any] = None,
                       reference_tables_data: Dict[str, List[Dict]] = None,
                       get_model_api_config_func=None,
                       create_list_model_func=None,
                       build_schema_context_func=None,
                       parse_structured_response_func=None,
                       record_extraction_step_func=None,
                       fallback_func=None) -> List[TableRow]:
        """
        传统的全局关系提取
        """
        relation_extraction_config = table_def.get('relation_extraction', {})
        
        self.logger.info(f"🔍 [_extract_global] 开始全局关系提取")
        self.logger.info(f"   原始text_content长度: {len(text_content)} 字符")
        
        text_content = self._load_document_sources(relation_extraction_config, text_content, context)
        
        self.logger.info(f"   加载后text_content长度: {len(text_content)} 字符")
        
        temp_schema = {'tables': [table_def]}
        table_name = table_def.get('name', 'data_table')
        
        list_model = create_list_model_func(temp_schema, table_name)
        if not list_model:
            return fallback_func(text_content, table_def, nl_prompt, doc_source, context, doc_index)
        
        model = context.model if context and hasattr(context, 'model') else "gpt-4o"
        api_config = get_model_api_config_func(model)
        api_base = api_config['api_base']
        api_key = api_config['api_key']
        
        llm_kwargs = {"model": model, "temperature": 0}
        if api_base:
            llm_kwargs["base_url"] = api_base
        if api_key:
            llm_kwargs["api_key"] = api_key
        
        llm = ChatOpenAI(**llm_kwargs)
        
        parser = PydanticOutputParser(pydantic_object=list_model)
        fixing_parser = OutputFixingParser.from_llm(parser=parser, llm=llm)
        
        schema_context = build_schema_context_func(table_def, full_schema=full_schema)
        
        reference_tables_prompt = self._build_reference_tables_prompt(
            reference_tables_data or {}, relation_extraction_config
        )
        
        prompt = f"""Please extract structured data from the following text according to the schema for table "{table_name}".

{schema_context}

{reference_tables_prompt}

Extract ALL relevant records for the "{table_name}" table from the document. Each record should be a separate item in the list.

Text content:
{text_content}

{parser.get_format_instructions()}

IMPORTANT: 
- Return a JSON object with a "rows" field containing a list of all extracted records for "{table_name}".
- Extract EVERY occurrence of relevant data for this table, even if mentioned multiple times.
- For foreign key fields, use exact values from the reference tables provided above.
"""

        if nl_prompt and nl_prompt.strip():
            prompt += f"\nAdditional Instructions: {nl_prompt}\n"
        
        response = llm.invoke(prompt)
        llm_response = response.content
        
        rows = parse_structured_response_func(
            llm_response, fixing_parser, table_name, doc_source
        )
        
        record_extraction_step_func(
            context, table_name, doc_index, doc_source, 
            'global_relation_extraction', model, prompt, llm_response, len(rows)
        )
        
        return rows
    
    def _find_entity_mentions_via_llm_batch(self, text_content: str, 
                                           anchor_entities: List[Dict[str, Any]],
                                           anchor_table_name: str, model: str,
                                           api_config: Dict[str, str], 
                                           context=None,
                                           batch_size: int = 20) -> Dict[str, List[str]]:
        """
        批量使用LLM找到文档中提及多个锚点实体的文本块(chunk)
        
        相比单个处理，批处理的优势：
        1. 减少网络往返次数（N次 -> N/batch_size次）
        2. 更高吞吐量
        3. 降低总延迟
        
        Args:
            text_content: 完整文档内容
            anchor_entities: 锚点实体列表
            anchor_table_name: 锚点表名
            model: 使用的模型
            api_config: API配置
            context: 处理上下文
            batch_size: 每批处理的实体数量
            
        Returns:
            字典，key为实体描述，value为包含该实体的文本块列表
            例如: {"StudentID=s1, StudentName=Alice": ["chunk1", "chunk2"], ...}
        """
        if context:
            output_dir = None
            if hasattr(context, 'io_manager') and hasattr(context.io_manager, 'output_dir'):
                output_dir = context.io_manager.output_dir
            elif hasattr(context, 'output_dir'):
                output_dir = context.output_dir
            if output_dir:
                self._setup_entity_mention_logging(output_dir)
        
        self.logger.info(f"🚀 [批处理] 开始批量查找 {len(anchor_entities)} 个实体的提及")
        
        all_results = {}
        
        for batch_idx in range(0, len(anchor_entities), batch_size):
            batch = anchor_entities[batch_idx:batch_idx + batch_size]
            self.logger.info(f"  批次 {batch_idx//batch_size + 1}: 处理 {len(batch)} 个实体")
            
            entities_list = []
            entity_keys = []  # 用于映射返回结果
            for entity in batch:
                entity_desc = ", ".join([f"{k}={v}" for k, v in entity.items()])
                entities_list.append(entity_desc)
                entity_keys.append(entity_desc)
            
            system_prompt = """You are a document analysis expert. Your task is to find all relevant text chunks 
that mention specific entities and provide sufficient context for understanding relationships."""
            
            prompt = f"""Find all text chunks in the document that mention ANY of the following entities from the {anchor_table_name} table.

TARGET ENTITIES:
{chr(10).join([f"{i+1}. {desc}" for i, desc in enumerate(entities_list)])}

DOCUMENT TO SEARCH:
{text_content}

TASK:
- For EACH entity (by its ID or name), find ALL text passages that mention it
- Extract complete sentences with context (2-5 sentences per chunk)
- Include surrounding text to help understand relationships

OUTPUT FORMAT (STRICTLY FOLLOW):
<entity id="1">
<chunk>Text passage mentioning entity 1...</chunk>
<chunk>Another passage mentioning entity 1...</chunk>
</entity>
<entity id="2">
<chunk>Text passage mentioning entity 2...</chunk>
</entity>

IMPORTANT:
- You MUST process ALL {len(entities_list)} entities listed above
- If an entity is not mentioned in the document, write: <entity id="X">NO_MENTION</entity>
- Do NOT skip any entity
- Use the exact entity ID numbers (1, 2, 3, etc.)
"""
            
            try:
                llm_response = get_answer(prompt, system_prompt=system_prompt, model=model)
                
                self.logger.debug(f"  [DEBUG] LLM原始响应长度: {len(llm_response)} 字符")
                self.logger.debug(f"  [DEBUG] LLM响应前500字符:\n{llm_response[:500]}")
                
                self.logger.info(f"📋 [批次 {batch_idx//batch_size + 1}] 处理 {len(batch)} 个实体的提及查找")
                for idx, entity_desc in enumerate(entities_list, 1):
                    self.logger.debug(f"   实体 {idx}: {entity_desc}")
                
                import re
                
                for i, entity_key in enumerate(entity_keys):
                    entity_num = i + 1
                    
                    entity_pattern = rf'<entity\s+id\s*=\s*["\']?{entity_num}["\']?\s*>(.*?)</entity>'
                    entity_match = re.search(entity_pattern, llm_response, re.DOTALL | re.IGNORECASE)
                    
                    chunks = []
                    if entity_match:
                        entity_content = entity_match.group(1)
                        self.logger.debug(f"  [DEBUG] 实体 {entity_num} 匹配成功，内容长度: {len(entity_content)}")
                        
                        if "NO_MENTION" not in entity_content:
                            chunk_matches = re.findall(r'<chunk>(.*?)</chunk>', entity_content, re.DOTALL)
                            
                            for chunk in chunk_matches:
                                chunk = chunk.strip()
                                if chunk and len(chunk) > 20:
                                    if len(chunk) > 800:
                                        chunk = chunk[:800] + "..."
                                    chunks.append(chunk)
                            
                            if not chunks:
                                self.logger.debug(f"  [DEBUG] 实体 {entity_num} 没有<chunk>标签，尝试降级解析")
                                potential_chunks = entity_content.strip().split('\n\n')
                                for chunk in potential_chunks:
                                    chunk = chunk.strip()
                                    if chunk and len(chunk) > 30:
                                        if len(chunk) > 800:
                                            chunk = chunk[:800] + "..."
                                        chunks.append(chunk)
                    else:
                        self.logger.debug(f"  [DEBUG] 实体 {entity_num} 未匹配到<entity>标签")
                    
                    chunks = chunks[:10]
                    all_results[entity_key] = chunks
                    
                    if chunks:
                        self.logger.info(f"    ✅ 实体 {entity_num} ({entity_key[:50]}...): {len(chunks)} 个chunks")
                        for i, chunk in enumerate(chunks, 1):
                            chunk_preview = chunk[:200] + "..." if len(chunk) > 200 else chunk
                            self.logger.debug(f"      Chunk {i}: {chunk_preview}")
                    else:
                        self.logger.info(f"    ⚠️ 实体 {entity_num} ({entity_key[:50]}...): 未找到")
                
            except Exception as e:
                self.logger.error(f"❌ 批量查找实体提及失败（批次 {batch_idx//batch_size + 1}）: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                for entity_key in entity_keys:
                    all_results[entity_key] = []
        
        successful_count = sum(1 for chunks in all_results.values() if chunks)
        self.logger.info(f"✅ [批处理] 完成：{len(anchor_entities)} 个实体中有 {successful_count} 个找到提及")
        
        return all_results
    
    def _find_entity_mentions_via_llm(self, text_content: str, anchor_entity: Dict[str, Any],
                                     anchor_table_name: str, model: str,
                                     api_config: Dict[str, str], context=None) -> List[str]:
        """
        使用LLM找到文档中提及该锚点实体的文本块(chunk)
        
        注意：此方法用于单个实体处理。对于批量处理，请使用 _find_entity_mentions_via_llm_batch
        
        Args:
            text_content: 完整文档内容
            anchor_entity: 锚点实体，例如 {'StudentID': 's1', 'StudentName': 'Alice'}
            anchor_table_name: 锚点表名
            model: 使用的模型
            api_config: API配置
            context: 处理上下文，用于获取output_dir
            
        Returns:
            包含该实体的文本块列表
        """
        if context:
            output_dir = None
            if hasattr(context, 'io_manager') and hasattr(context.io_manager, 'output_dir'):
                output_dir = context.io_manager.output_dir
            elif hasattr(context, 'output_dir'):
                output_dir = context.output_dir
            
            if output_dir:
                self._setup_entity_mention_logging(output_dir)
        
        entity_desc = ", ".join([f"{k}={v}" for k, v in anchor_entity.items()])
        
        self.logger.info(f"🔍 开始查找实体提及 - 表: {anchor_table_name}, 实体: {entity_desc}")
        
        system_prompt = """You are a document analysis expert. Your task is to find all relevant text chunks 
that mention a specific entity and provide sufficient context for understanding relationships."""
        
        prompt = f"""Find all text chunks in the document that mention the following entity from the {anchor_table_name} table:

Entity: {entity_desc}

Document:
{text_content}

Instructions:
1. Identify the entity by any of its attributes (ID, name, etc.)
2. Extract complete text chunks (not just single sentences) that mention this entity
3. A chunk should be 2-5 sentences long to provide sufficient context
4. Include surrounding context that helps understand relationships and attributes
5. If a mention is very brief, include the surrounding sentences for context
6. Return ONLY the text chunks, no explanations
7. If no mention found, return "NO_MENTION"

Output format:
<chunk>First text chunk mentioning the entity with context.</chunk>
<chunk>Second text chunk mentioning the entity with context.</chunk>
"""
        
        try:
            llm_response = get_answer(prompt, system_prompt=system_prompt, model=model)
            
            chunks = []
            if "NO_MENTION" in llm_response:
                return chunks
            
            import re
            chunk_matches = re.findall(r'<chunk>(.*?)</chunk>', llm_response, re.DOTALL)
            
            for chunk in chunk_matches:
                chunk = chunk.strip()
                if chunk and len(chunk) > 20:  # 确保chunk有实质内容
                    if len(chunk) > 800:
                        chunk = chunk[:800] + "..."
                    chunks.append(chunk)
            
            if not chunks:
                potential_chunks = llm_response.strip().split('\n\n')
                for chunk in potential_chunks:
                    chunk = chunk.strip()
                    if chunk and not chunk.startswith('#') and len(chunk) > 30:
                        if len(chunk) > 800:
                            chunk = chunk[:800] + "..."
                        chunks.append(chunk)
            
            final_chunks = chunks[:15]  # 限制最多15个chunks
            if final_chunks:
                self.logger.info(f"✅ 找到 {len(final_chunks)} 个文本块提及实体 {entity_desc}")
                for i, chunk in enumerate(final_chunks, 1):
                    chunk_preview = chunk[:200] + "..." if len(chunk) > 200 else chunk
                    self.logger.debug(f"   Chunk {i}: {chunk_preview}")
            else:
                self.logger.info(f"⚠️ 未找到提及实体 {entity_desc} 的文本块")
            
            return final_chunks
            
        except Exception as e:
            self.logger.error(f"❌ LLM查找实体提及失败 - 表: {anchor_table_name}, 实体: {entity_desc}, 错误: {e}")
            return []
    
    def _extract_relations_for_anchors_batch(self, 
                                            anchor_entities_with_context: Dict[str, List[str]],
                                            table_def: Dict[str, Any],
                                            reference_tables_data: Dict[str, List[Dict]],
                                            relation_config: Dict[str, Any],
                                            model: str,
                                            api_config: Dict[str, str],
                                            doc_source: str,
                                            create_list_model_func=None,
                                            build_schema_context_func=None,
                                            parse_structured_response_func=None,
                                            full_schema: Dict[str, Any] = None,
                                            batch_size: int = 10) -> List[TableRow]:
        """
        批量为多个锚点实体提取关系
        
        相比单个处理的优势：
        1. 减少LLM调用次数（N次 -> N/batch_size次）
        2. 共享schema和reference tables的上下文
        3. 显著降低总延迟
        
        Args:
            anchor_entities_with_context: 字典，key为实体描述，value为相关文本块列表
                例如: {"StudentID=s1, StudentName=Alice": ["chunk1", "chunk2"], ...}
            table_def: 关系表定义
            reference_tables_data: 所有参考表数据
            relation_config: 关系提取配置
            model: 模型名称
            api_config: API配置
            doc_source: 文档来源
            batch_size: 每批处理的实体数量
            
        Returns:
            所有实体的关系行列表（已合并）
        """
        self.logger.info(f"🚀 [批处理] 开始批量提取 {len(anchor_entities_with_context)} 个实体的关系")
        
        temp_schema = {'tables': [table_def]}
        table_name = table_def.get('name', 'data_table')
        
        list_model = create_list_model_func(temp_schema, table_name)
        if not list_model:
            return []
        
        api_base = api_config['api_base']
        api_key = api_config['api_key']
        
        llm_kwargs = {"model": model, "temperature": 0}
        if api_base:
            llm_kwargs["base_url"] = api_base
        if api_key:
            llm_kwargs["api_key"] = api_key
        
        from langchain_openai import ChatOpenAI
        from langchain_core.output_parsers import PydanticOutputParser
        from langchain.output_parsers import OutputFixingParser
        
        llm = ChatOpenAI(**llm_kwargs)
        parser = PydanticOutputParser(pydantic_object=list_model)
        fixing_parser = OutputFixingParser.from_llm(parser=parser, llm=llm)
        
        schema_context = build_schema_context_func(table_def, full_schema=full_schema)
        reference_tables_prompt = self._build_reference_tables_prompt(
            reference_tables_data, relation_config
        )
        
        all_rows = []
        
        entities_with_text = {k: v for k, v in anchor_entities_with_context.items() if v}
        
        entity_items = list(entities_with_text.items())
        for batch_idx in range(0, len(entity_items), batch_size):
            batch = entity_items[batch_idx:batch_idx + batch_size]
            self.logger.info(f"  批次 {batch_idx//batch_size + 1}: 提取 {len(batch)} 个实体的关系")
            
            entities_context_parts = []
            for i, (entity_desc, relevant_texts) in enumerate(batch, 1):
                context_text = "\n".join(relevant_texts)
                entities_context_parts.append(
                    f"=== Entity {i}: {entity_desc} ===\n{context_text}\n"
                )
            
            combined_context = "\n".join(entities_context_parts)
            
            prompt = f"""Extract relationship records for ALL the following anchor entities:

{schema_context}

{reference_tables_prompt}

Relevant Text for Multiple Entities:
{combined_context}

{parser.get_format_instructions()}

IMPORTANT:
- Extract relationships for ALL {len(batch)} entities listed above
- For each entity, extract ONLY relationships involving that specific entity
- For foreign key fields, use exact values from the reference tables
- Return a JSON object with a "rows" field containing ALL extracted records
- Each record should be properly linked to its anchor entity

TIPS: Process each entity section separately and combine all results.
"""
            
            try:
                response = llm.invoke(prompt)
                llm_response = response.content
                
                self.logger.debug(f"  [DEBUG] 关系提取LLM响应长度: {len(llm_response)} 字符")
                self.logger.debug(f"  [DEBUG] 关系提取LLM响应前300字符:\n{llm_response[:300]}")
                
                batch_rows = parse_structured_response_func(
                    llm_response, fixing_parser, table_name, doc_source
                )
                
                all_rows.extend(batch_rows)
                self.logger.info(f"    提取到 {len(batch_rows)} 条关系记录")
                
                if batch_rows:
                    self.logger.debug(f"    [批次 {batch_idx//batch_size + 1}] 提取的关系详情:")
                    for idx, row in enumerate(batch_rows[:5], 1):  # 限制显示前5条
                        row_desc = ", ".join([f"{k}={v.value if hasattr(v, 'value') else v}" 
                                             for k, v in (row.cells.items() if hasattr(row, 'cells') else [])])
                        self.logger.debug(f"      关系 {idx}: {row_desc}")
                    if len(batch_rows) > 5:
                        self.logger.debug(f"      ... (还有 {len(batch_rows)-5} 条关系)")
                
            except Exception as e:
                self.logger.error(f"❌ 批量提取关系失败（批次 {batch_idx//batch_size + 1}）: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
        
        self.logger.info(f"✅ [批处理] 完成：总计提取 {len(all_rows)} 条关系记录")
        
        return all_rows
    
    def _extract_relations_for_anchor(self, anchor_entity: Dict[str, Any],
                                     relevant_sentences: List[str],
                                     table_def: Dict[str, Any],
                                     reference_tables_data: Dict[str, List[Dict]],
                                     relation_config: Dict[str, Any],
                                     model: str,
                                     api_config: Dict[str, str],
                                     doc_source: str,
                                     create_list_model_func=None,
                                     build_schema_context_func=None,
                                     parse_structured_response_func=None,
                                     full_schema: Dict[str, Any] = None) -> List[TableRow]:
        """
        为单个锚点实体在相关句子中提取关系
        
        注意：此方法用于单个实体处理。对于批量处理，请使用 _extract_relations_for_anchors_batch
        
        Args:
            anchor_entity: 锚点实体
            relevant_sentences: 包含该实体的句子列表
            table_def: 关系表定义
            reference_tables_data: 所有参考表数据
            relation_config: 关系提取配置
            model: 模型名称
            api_config: API配置
            doc_source: 文档来源
            
        Returns:
            提取的关系行列表
        """
        temp_schema = {'tables': [table_def]}
        table_name = table_def.get('name', 'data_table')
        
        list_model = create_list_model_func(temp_schema, table_name)
        if not list_model:
            return []
        
        api_base = api_config['api_base']
        api_key = api_config['api_key']
        
        llm_kwargs = {"model": model, "temperature": 0}
        if api_base:
            llm_kwargs["base_url"] = api_base
        if api_key:
            llm_kwargs["api_key"] = api_key
        
        llm = ChatOpenAI(**llm_kwargs)
        
        parser = PydanticOutputParser(pydantic_object=list_model)
        fixing_parser = OutputFixingParser.from_llm(parser=parser, llm=llm)
        
        schema_context = build_schema_context_func(table_def, full_schema=full_schema)
        
        reference_tables_prompt = self._build_reference_tables_prompt(
            reference_tables_data, relation_config
        )
        
        anchor_desc = ", ".join([f"{k}={v}" for k, v in anchor_entity.items()])
        
        context_text = "\n".join(relevant_sentences)
        
        prompt = f"""Extract relationship records involving the following anchor entity:

Anchor Entity: {anchor_desc}

{schema_context}

{reference_tables_prompt}

Relevant Text (sentences mentioning this entity):
{context_text}

{parser.get_format_instructions()}

IMPORTANT:
- Extract ONLY relationships involving the anchor entity specified above
- For foreign key fields, use exact values from the reference tables
- Return a JSON object with a "rows" field
- If no relationships found, return {{"rows": []}}

TIPS: Focus on relationships explicitly stated in the relevant text.
"""
        
        try:
            response = llm.invoke(prompt)
            llm_response = response.content
            
            rows = parse_structured_response_func(
                llm_response, fixing_parser, table_name, doc_source
            )
            
            return rows
            
        except Exception as e:
            self.logger.error(f"❌ 为锚点提取关系失败: {e}")
            return []
    
    def _deduplicate_relation_rows(self, rows: List[TableRow]) -> List[TableRow]:
        """
        去重关系行
        
        基于tuple_id或cell内容进行去重
        """
        if not rows:
            return []
        
        seen_tuples = set()
        unique_rows = []
        
        for row in rows:
            cell_values = tuple(sorted([
                (field_name, str(cell_data.value)) 
                for field_name, cell_data in row.cells.items()
            ]))
            
            if cell_values not in seen_tuples:
                seen_tuples.add(cell_values)
                unique_rows.append(row)
        
        return unique_rows
    
    def _identify_affected_anchors(self, violations: List, previous_snapshot,
                                   table_def: Dict[str, Any], context) -> List[Dict[str, Any]]:
        """
        从violations中识别受影响的anchor entities
        
        论文逻辑："identifies the involved entities"
        
        Args:
            violations: 违规列表
            previous_snapshot: 上一次快照
            table_def: 表定义
            context: 处理上下文
            
        Returns:
            受影响的anchor entity列表，格式如 [{'StudentID': 's1', 'StudentName': 'Alice'}, ...]
        """
        if not violations or not previous_snapshot:
            return []
        
        attributes = table_def.get('attributes', table_def.get('fields', []))
        anchor_fk_field = None
        for attr in attributes:
            constraints = attr.get('constraints', {})
            if constraints.get('foreign_key'):
                anchor_fk_field = attr.get('name')
                break  # 取第一个外键作为anchor
        
        if not anchor_fk_field:
            self.logger.warning("⚠️ 未找到anchor外键字段")
            return []
        
        self.logger.info(f"📌 使用anchor字段: {anchor_fk_field}")
        
        affected_anchor_ids = set()
        for violation in violations:
            
            tuple_ids = []
            
            if hasattr(violation, 'affected_tuple_ids') and violation.affected_tuple_ids:
                tuple_ids = violation.affected_tuple_ids
                self.logger.debug(f"  检测到聚合类型违规，受影响的tuple数: {len(tuple_ids)}")
            elif hasattr(violation, 'tuple_id') and violation.tuple_id:
                tuple_ids = [violation.tuple_id]
            elif isinstance(violation, dict):
                tuple_ids = violation.get('tuple_ids', [])
            
            for tuple_id in tuple_ids:
                for row in previous_snapshot.rows:
                    if row.tuple_id == tuple_id:
                        if anchor_fk_field in row.cells:
                            anchor_value = row.cells[anchor_fk_field].value
                            affected_anchor_ids.add(anchor_value)
                            self.logger.debug(f"  从violation识别anchor ID: {anchor_value}")
        
        self.logger.info(f"📋 识别到 {len(affected_anchor_ids)} 个受影响的anchor IDs: {affected_anchor_ids}")
        
        relation_config = table_def.get('relation_extraction', {})
        reference_tables_data = self._load_reference_tables(relation_config, context)
        
        if not reference_tables_data:
            reference_tables_data = self._get_entity_snapshots_as_reference(context, table_def)
        
        if not reference_tables_data:
            self.logger.warning("⚠️ 无法获取参考表数据")
            return []
        
        anchor_table_name = list(reference_tables_data.keys())[0]
        anchor_entities = reference_tables_data[anchor_table_name]
        
        self.logger.info(f"📋 从参考表 {anchor_table_name} 获取 {len(anchor_entities)} 个entities")
        
        affected_entities = []
        for entity in anchor_entities:
            entity_id = (entity.get(anchor_fk_field) or 
                        entity.get('ID') or 
                        entity.get('id') or
                        entity.get(f'{anchor_table_name}ID'))
            
            if entity_id in affected_anchor_ids:
                affected_entities.append(entity)
                self.logger.debug(f"  匹配到受影响entity: {entity}")
        
        self.logger.info(f"✅ 最终识别 {len(affected_entities)} 个受影响的anchor entities")
        
        return affected_entities
    
    def _merge_warm_start_results(self, old_rows: List[TableRow], 
                                   new_rows: List[TableRow],
                                   affected_anchors: List[Dict[str, Any]],
                                   table_def: Dict[str, Any]) -> List[TableRow]:
        """
        合并warm start结果：移除涉及affected_anchors的旧记录，添加新记录，保留无关记录
        
        论文逻辑："Relationship tuples unrelated to the violation are left unchanged"
        
        Args:
            old_rows: 旧的关系行
            new_rows: 新提取的关系行
            affected_anchors: 受影响的anchor entities
            table_def: 表定义
            
        Returns:
            合并后的行列表
        """
        attributes = table_def.get('attributes', table_def.get('fields', []))
        anchor_fk_field = None
        for attr in attributes:
            constraints = attr.get('constraints', {})
            if constraints.get('foreign_key'):
                anchor_fk_field = attr.get('name')
                break
        
        if not anchor_fk_field:
            self.logger.warning("⚠️ 未找到anchor字段，无法精确合并，返回去重后的新记录")
            return self._deduplicate_relation_rows(new_rows)
        
        affected_ids = set()
        for anchor in affected_anchors:
            anchor_id = (anchor.get(anchor_fk_field) or 
                        anchor.get('ID') or 
                        anchor.get('id'))
            if anchor_id:
                affected_ids.add(anchor_id)
        
        self.logger.info(f"🔄 开始合并结果 - 受影响的anchor IDs: {affected_ids}")
        
        unaffected_rows = []
        removed_count = 0
        for row in old_rows:
            if anchor_fk_field in row.cells:
                row_anchor_id = row.cells[anchor_fk_field].value
                if row_anchor_id not in affected_ids:
                    unaffected_rows.append(row)
                else:
                    removed_count += 1
        
        self.logger.info(f"  保留 {len(unaffected_rows)} 条无关记录，移除 {removed_count} 条受影响记录")
        
        merged_rows = unaffected_rows + new_rows
        
        self.logger.info(f"  添加 {len(new_rows)} 条新提取记录")
        
        unique_rows = self._deduplicate_relation_rows(merged_rows)
        
        self.logger.info(f"✅ 合并完成: {len(old_rows)} 条原始 -> {len(unique_rows)} 条最终（去重后）")
        
        return unique_rows

