"""协调器工具模块"""
import os
import json
import logging
from typing import List, Dict, Any, Optional

from .context import Doc2DBContext


class SchemaUtils:
    """Schema处理工具类"""
    
    @staticmethod
    def extract_table_names_from_schema(schema: Dict[str, Any]) -> List[str]:
        """从Schema中提取所有表名，并自动处理同名表（添加编号后缀）"""
        table_names = []
        
        if 'tables' in schema and isinstance(schema['tables'], list):
            for table in schema['tables']:
                if isinstance(table, dict) and 'name' in table:
                    table_names.append(table['name'])
        
        elif 'table_name' in schema and 'columns' in schema:
            table_names.append(schema['table_name'])
        
        if table_names:
            name_counts = {}
            unique_names = []
            
            for name in table_names:
                if name not in name_counts:
                    name_counts[name] = 0
                name_counts[name] += 1
            
            name_indices = {}
            for name in table_names:
                if name_counts[name] > 1:
                    if name not in name_indices:
                        name_indices[name] = 1
                    unique_name = f"{name}_{name_indices[name]}"
                    name_indices[name] += 1
                    unique_names.append(unique_name)
                else:
                    unique_names.append(name)
            
            return unique_names
        
        return table_names
    
    @staticmethod
    def classify_tables_by_type(schema: Dict[str, Any]) -> Dict[str, List[str]]:
        """将表分类为实体表和关系表
        
        新逻辑：直接读取 schema 中表的 "type" 字段来分类
        - type: "entity" → 实体表
        - type: "relationship" → 关系表
        - 如果没有 type 字段，默认为实体表
        
        Returns:
            {
                'entity_tables': ['court', 'case', 'person', ...],
                'relation_tables': ['caseparty', ...]
            }
        """
        if not isinstance(schema, dict):
            return {'entity_tables': [], 'relation_tables': []}
        
        entity_tables = []
        relation_tables = []
        
        if 'tables' in schema and isinstance(schema['tables'], list):
            for table in schema['tables']:
                if not isinstance(table, dict) or 'name' not in table:
                    continue
                
                table_name = table['name']
                table_type = table.get('type', 'entity')  # 默认为实体表
                
                if table_type == 'relation':
                    relation_tables.append(table_name)
                else:
                    entity_tables.append(table_name)
        
        elif 'table_name' in schema:
            table_name = schema['table_name']
            table_type = schema.get('type', 'entity')
            
            if table_type == 'relation':
                relation_tables.append(table_name)
            else:
                entity_tables.append(table_name)
        
        return {
            'entity_tables': entity_tables,
            'relation_tables': relation_tables
        }
    
    @staticmethod
    def extract_table_specific_schema(schema: Dict[str, Any], table_name: str) -> Optional[Dict[str, Any]]:
        """从完整schema中提取特定表的schema定义
        
        支持编号后的表名（如 tax_1, tax_2），会自动解析并找到对应的原始表定义
        """
        if not isinstance(schema, dict):
            return None
        
        if 'tables' in schema and isinstance(schema['tables'], list):
            for table in schema['tables']:
                if isinstance(table, dict) and table.get('name') == table_name:
                    return {
                        'tables': [table],
                        'table_name': table_name
                    }
            
            if '_' in table_name and table_name.rsplit('_', 1)[-1].isdigit():
                base_name = table_name.rsplit('_', 1)[0]
                suffix_num = int(table_name.rsplit('_', 1)[1])
                
                matching_tables = [
                    table for table in schema['tables']
                    if isinstance(table, dict) and table.get('name') == base_name
                ]
                
                if matching_tables and 1 <= suffix_num <= len(matching_tables):
                    target_table = matching_tables[suffix_num - 1]
                    return {
                        'tables': [target_table],
                        'table_name': table_name  # 使用编号后的表名
                    }
        
        elif schema.get('table_name') == table_name and 'columns' in schema:
            return schema  # 已经是单表格式
        
        return None


class DocumentUtils:
    """文档处理工具类"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('document_utils')
    
    def convert_documents_to_text(self, documents: List[str]) -> List[str]:
        """将文档路径转换为文本内容"""
        text_contents = []
        
        for doc_path in documents:
            try:
                file_ext = os.path.splitext(doc_path.lower())[1]
                
                if file_ext in ['.txt', '.md']:
                    try:
                        with open(doc_path, 'r', encoding='utf-8') as f:
                            text_content = f.read()
                    except UnicodeDecodeError:
                        with open(doc_path, 'r', encoding='gbk') as f:
                            text_content = f.read()
                    text_contents.append(text_content)
                    
                elif file_ext in ['.pdf', '.docx', '.doc']:
                    from ...utils.textin_parser import TextinParser
                    parser = TextinParser()
                    
                    parse_result = parser.parse_pdf_with_textin(doc_path, main_content_only=True)
                    markdown_content = parse_result.get('markdown_content', '')
                    
                    if markdown_content:
                        text_contents.append(markdown_content)
                    else:
                        self.logger.warning(f"未能从文档 {doc_path} 中提取到内容")
                        text_contents.append("")
                        
                else:
                    self.logger.warning(f"不支持的文件格式: {doc_path}")
                    text_contents.append("")
                    
            except Exception as e:
                self.logger.error(f"处理文档 {doc_path} 失败: {e}")
                text_contents.append("")
        
        return text_contents


class SignalUtils:
    """信号处理工具类"""
    
    @staticmethod
    def create_extraction_signal_data(context: Doc2DBContext, table_name: str, table_specific_schema: Dict[str, Any]) -> Dict[str, Any]:
        """创建提取信号数据"""
        document_utils = DocumentUtils()
        
        return {
            'context': {
                'run_id': context.run_id,
                'table_name': table_name,
                'schema': table_specific_schema,
                'full_schema': context.schema,
                'text_contents': document_utils.convert_documents_to_text(context.documents),
                'nl_prompt': context.nl_prompt or context.user_query,
                'processing_context': context
            }
        }
    
    @staticmethod
    def create_verification_signal_data(context: Doc2DBContext, table_name: str, 
                                      table_specific_schema: Dict[str, Any], snapshots: List) -> Dict[str, Any]:
        """创建验证信号数据"""
        return {
            'context': {
                'run_id': context.run_id,
                'table_name': table_name,
                'schema': table_specific_schema,
                'full_schema': context.schema,
                'processing_context': context
            },
            'snapshots': snapshots
        }
    
    @staticmethod
    def create_fixing_signal_data(context: Doc2DBContext, table_name: str, 
                                table_specific_schema: Dict[str, Any], violations: List, snapshot) -> Dict[str, Any]:
        """创建修复信号数据"""
        return {
            'context': {
                'run_id': context.run_id,
                'table_name': table_name,
                'schema': table_specific_schema,
                'full_schema': context.schema,
                'processing_context': context
            },
            'violations': violations,
            'snapshot': snapshot
        }
    
    @staticmethod
    def get_correlation_id(run_id: str, table_name: str, operation: str, timestamp: Optional[int] = None) -> str:
        """生成关联ID"""
        if timestamp is None:
            import time
            timestamp = int(time.time() * 1000)
        return f"{run_id}_{table_name}_{operation}_{timestamp}"


class StepUtils:
    """步骤处理工具类"""
    
    @staticmethod
    def create_start_step(context: Doc2DBContext) -> Dict[str, Any]:
        """创建开始处理步骤"""
        return {
            'step': 'orchestrator_start',
            'description': 'Orchestrator开始处理流程',
            'status': 'completed',
            'timestamp': context.io_manager.get_timestamp(),
            'details': {
                'run_id': context.run_id,
                'target_tables': getattr(context, 'target_tables', []),
                'signal_mode': True
            }
        }
    
    @staticmethod
    def create_completion_step(context: Doc2DBContext) -> Dict[str, Any]:
        """创建完成步骤"""
        return {
            'step': 'orchestrator_completion',
            'step_name': 'orchestrator_completion',
            'description': '数据处理流程完成 - 所有表格已完成提取、验证和修复',
            'status': 'completed',
            'timestamp': context.io_manager.get_timestamp(),
            'details': {
                'total_tables_processed': len(context.all_snapshots),
                'total_snapshots': len(context.all_snapshots),
                'total_violations': sum(len(v) for v in context.all_violations.values()),
                'total_fixes': sum(len(f) for f in context.all_fixes.values()),
                'processing_state': context.current_state.value,
                'processing_summary': {
                    'extraction_completed': True,
                    'verification_completed': True,
                    'fixing_completed': True,
                    'quality_issues_resolved': sum(len(f) for f in context.all_fixes.values())
                }
            },
            'final_stats': {
                'tables': list(context.all_snapshots.keys()),
                'success': True,
                'completion_time': context.io_manager.get_timestamp()
            }
        }
    
    @staticmethod
    def create_error_step(context: Doc2DBContext, error: Exception, step_name: str = 'orchestrator_error') -> Dict[str, Any]:
        """创建错误步骤"""
        return {
            'step': step_name,
            'description': f'Orchestrator处理异常: {str(error)}',
            'status': 'failed',
            'timestamp': context.io_manager.get_timestamp(),
            'error': str(error),
            'details': {
                'exception_type': type(error).__name__,
                'processing_state': context.current_state.value
            }
        }


class DataTransferUtils:
    """数据转移工具类"""
    
    @staticmethod
    def transfer_processing_results(context: Doc2DBContext):
        """将多表处理结果转移到context主要字段中"""
        if context.all_snapshots:
            context.snapshots = list(context.all_snapshots.values())
        
        if context.all_violations:
            context.violations = []
            for table_name, table_violations in context.all_violations.items():
                context.violations.extend(table_violations)
        
        if context.all_fixes:
            context.fixes = []
            for table_name, table_fixes in context.all_fixes.items():
                context.fixes.extend(table_fixes)
    
    @staticmethod
    def save_final_snapshots(context: Doc2DBContext):
        """保存最终完成状态快照"""
        for table_name, snapshot in context.all_snapshots.items():
            if snapshot and hasattr(snapshot, 'rows') and snapshot.rows:
                original_row_count = len(snapshot.rows)
                snapshot.rows = [
                    row for row in snapshot.rows
                    if not any(
                        (hasattr(cell, 'value') and (cell.value == "__DELETED__" or str(cell.value) == "__DELETED__"))
                        for cell in row.cells.values()
                    )
                ]
                deleted_row_count = original_row_count - len(snapshot.rows)
                if deleted_row_count > 0:
                    context.io_manager.logger.info(f'🧹 [最终清理] 表 {table_name}: 清理了 {deleted_row_count} 行__DELETED__标记的数据')
            
            final_snapshot = snapshot
            final_snapshot.processing_stage = 'final'
            final_snapshot.stage_description = f'最终完成状态 - 表 {table_name}'
            context.io_manager.append_snapshot(final_snapshot)
        
        DataTransferUtils.save_result_json(context)
    
    @staticmethod
    def save_result_json(context: Doc2DBContext):
        """生成result.json文件
        
        🔧 只输出用户请求的目标表，不包含参考表（用于关系抽取验证的辅助表）
        """
        try:
            import json
            from pathlib import Path
            
            best_snapshots = {}
            for table_name in context.target_tables:
                if table_name in context.all_snapshots:
                    snapshot = context.all_snapshots[table_name]
                    
                    if hasattr(snapshot, 'processing_stage') and snapshot.processing_stage == 'reference_data':
                        context.io_manager.logger.debug(f'跳过参考表 {table_name}（不包含在result.json中）')
                        continue
                    
                    best_snapshots[table_name] = snapshot
            
            if not best_snapshots:
                context.io_manager.logger.warning("target_tables的快照为空，尝试从文件读取final快照")
                best_snapshots = DataTransferUtils._find_final_snapshots_from_file(context)
            
            tables_data = {}
            
            schema_field_orders = {}
            if hasattr(context, 'schema') and context.schema:
                schema = context.schema
                if isinstance(schema, str):
                    import json as json_module
                    schema = json_module.loads(schema)
                
                if 'tables' in schema and isinstance(schema['tables'], list):
                    for table in schema['tables']:
                        table_name_key = table.get('name')
                        fields_list = table.get('attributes') or table.get('fields', [])
                        if table_name_key and fields_list:
                            field_order = [f.get('name') or f.get('field_name') for f in fields_list if f.get('name') or f.get('field_name')]
                            schema_field_orders[table_name_key] = field_order
            
            for table_name, snapshot in best_snapshots.items():
                if not snapshot or not hasattr(snapshot, 'rows') or not snapshot.rows:
                    continue
                
                first_row = snapshot.rows[0]
                if not hasattr(first_row, 'cells') or not first_row.cells:
                    continue
                
                if table_name in schema_field_orders:
                    headers = [field for field in schema_field_orders[table_name] if field in first_row.cells]
                    for field in first_row.cells.keys():
                        if field not in headers:
                            headers.append(field)
                else:
                    headers = list(first_row.cells.keys())
                    context.io_manager.logger.warning(f"表 {table_name} 未在 schema 中找到，使用 cells 推断的顺序")
                
                table_rows = []
                skipped_deleted_rows = 0  # 统计跳过的删除标记行
                
                for row in snapshot.rows:
                    has_deleted_marker = False
                    for header in headers:
                        cell = row.cells.get(header)
                        if cell:
                            cell_value = None
                            if hasattr(cell, 'value'):
                                cell_value = cell.value
                            elif hasattr(cell, 'best') and hasattr(cell.best, 'value'):
                                cell_value = cell.best.value
                            
                            if cell_value == "__DELETED__" or str(cell_value) == "__DELETED__":
                                has_deleted_marker = True
                                break
                    
                    if has_deleted_marker:
                        skipped_deleted_rows += 1
                        context.io_manager.logger.debug(f"跳过删除标记的行: {row.tuple_id}")
                        continue
                    
                    row_data = {}
                    for header in headers:
                        cell = row.cells.get(header)
                        if cell:
                            if hasattr(cell, 'value'):
                                row_data[header] = cell.value
                            elif hasattr(cell, 'best') and hasattr(cell.best, 'value'):
                                row_data[header] = cell.best.value
                            else:
                                row_data[header] = ''
                        else:
                            row_data[header] = ''
                    table_rows.append(row_data)
                
                if skipped_deleted_rows > 0:
                    context.io_manager.logger.info(f"表 {table_name}: 过滤了 {skipped_deleted_rows} 行标记为删除的数据")
                
                tables_data[table_name] = table_rows
            
            result_data = {
                'run_id': context.run_id,
                'tables': tables_data
            }
            
            result_file = context.io_manager.output_dir / 'result.json'
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2, sort_keys=False)
            
            import os
            if not (os.path.exists(result_file) and os.path.getsize(result_file) > 0):
                context.io_manager.logger.warning(f"result.json 写入可能未完成: {result_file}")
            
        except Exception as e:
            context.io_manager.logger.error(f"生成result.json失败: {e}")
    
    @staticmethod
    def _find_final_snapshots_from_file(context: Doc2DBContext):
        """从snapshots.jsonl文件中读取final阶段的快照
        
        final阶段的快照是在save_final_snapshots中保存的，
        代表的是context.all_snapshots的内容（已经包含了所有batch合并后的完整数据）
        """
        try:
            import json
            from ...memory.snapshot import TableSnapshot
            
            snapshots_file = context.io_manager.output_dir / 'snapshots.jsonl'
            if not snapshots_file.exists():
                context.io_manager.logger.warning(f"snapshots.jsonl文件不存在: {snapshots_file}")
                return None
            
            final_snapshots = {}
            with open(snapshots_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        snapshot_dict = json.loads(line)
                        table_name = snapshot_dict.get('table')
                        stage = snapshot_dict.get('processing_stage', '')
                        
                        if stage == 'final' and table_name:
                            try:
                                snapshot_obj = TableSnapshot.from_dict(snapshot_dict)
                                final_snapshots[table_name] = snapshot_obj
                                context.io_manager.logger.info(
                                    f"从文件读取表 {table_name} 的final快照: {len(snapshot_obj.rows)}行"
                                )
                            except Exception as e:
                                context.io_manager.logger.error(f"转换快照失败: {e}")
                    except Exception as e:
                        continue
            
            return final_snapshots if final_snapshots else None
                
        except Exception as e:
            context.io_manager.logger.error(f"从文件读取final快照失败: {e}")
            return None


class DebugUtils:
    """调试工具类"""
    
    @staticmethod
    def write_debug_log(context: Doc2DBContext, message: str):
        """写入调试日志"""
        try:
            with open(f"{context.io_manager.base_path}/debug_orchestrator.log", "a", encoding="utf-8") as f:
                f.write(f"[{context.io_manager.get_timestamp()}] {message}\n")
        except:
            pass  # 忽略日志写入错误
    
    @staticmethod
    def ensure_completion_step(context: Doc2DBContext) -> bool:
        """确保有完成步骤记录（兜底处理）"""
        has_completion_step = any(
            'Orchestrator完成所有处理流程' in step.get('description', '')
            for step in context.step_outputs
        )
        
        if not has_completion_step:
            completion_step = {
                'step': 'orchestrator_completion_fallback',
                'description': 'Orchestrator完成所有处理流程',
                'status': 'completed',
                'timestamp': context.io_manager.get_timestamp(),
                'details': {'fallback': True, 'sync_call': True}
            }
            context.step_outputs.append(completion_step)
            return True
        
        return False
