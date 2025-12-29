"""
提取器工具函数模块
包含数据解析、去重、格式转换等工具函数
"""

import re
import json
import logging
from typing import List, Dict, Any

from ...memory import TableRow, CellData
from ...core.ids import IdGenerator


class ExtractorUtils:
    """提取器工具类"""
    
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger('doc2db.extractor.utils')
    
    
    def parse_markdown_table_response(self, llm_response: str, 
                                     table_def: Dict[str, Any], 
                                     doc_source: str) -> List[TableRow]:
        """解析Markdown表格格式的响应
        
        Args:
            llm_response: LLM返回的包含Markdown表格的响应
            table_def: 表定义
            doc_source: 数据来源
            
        Returns:
            解析后的TableRow列表
        """
        rows = []
        
        try:
            table_match = re.search(r'<TABLE BEGIN>(.*?)<TABLE END>', llm_response, re.DOTALL)
            if not table_match:
                return rows
            
            table_markdown = table_match.group(1).strip()
            lines = [line.strip() for line in table_markdown.strip().split('\n') if line.strip()]
            
            if len(lines) < 3:
                return rows
            
            header_line = lines[0]
            column_names = [col.strip() for col in header_line.split('|')[1:-1]]
            
            data_lines = lines[2:]
            attributes = table_def.get('attributes', [])
            schema_fields = {attr['name']: attr for attr in attributes}
            
            for row_index, data_line in enumerate(data_lines):
                if not data_line.startswith('|') or not data_line.endswith('|'):
                    continue
                
                cells_data = [cell.strip() for cell in data_line.split('|')[1:-1]]
                if not cells_data:
                    continue
                
                tuple_id = IdGenerator.generate_tuple_id(
                    table_def.get('name', 'table'), row_index, cells_data
                )
                
                cells = {}
                for col_index, cell_value in enumerate(cells_data):
                    if col_index < len(column_names):
                        column_name = column_names[col_index].strip()
                        
                        if column_name in schema_fields:
                            clean_value = cell_value.strip()
                            cells[column_name] = CellData(
                                value=clean_value,
                                evidences=[doc_source]
                            )
                
                if cells:
                    row = TableRow(tuple_id=tuple_id, cells=cells)
                    rows.append(row)
            
        except Exception as e:
            self.logger.error(f"❌ 解析Markdown表格失败: {e}")
        
        return rows
    
    
    def parse_cell_fix_json(self, llm_response: str) -> List[Dict[str, Any]]:
        """解析LLM返回的cell修复JSON
        
        Args:
            llm_response: LLM返回的JSON响应
            
        Returns:
            修复数据列表，每个元素包含 tuple_id, field, new_value
        """
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', llm_response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = llm_response.strip()
        
        try:
            data = json.loads(json_str)
            
            if isinstance(data, dict) and 'fixes' in data:
                fixes = data['fixes']
            elif isinstance(data, list):
                fixes = data
            else:
                return []
            
            valid_fixes = []
            for fix in fixes:
                if isinstance(fix, dict) and all(k in fix for k in ['tuple_id', 'field', 'new_value']):
                    valid_fixes.append(fix)
            
            return valid_fixes
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ JSON解析失败: {e}")
            return []
    
    
    def deduplicate_rows(self, rows: List[TableRow]) -> List[TableRow]:
        """去重：基于字段值签名
        
        Args:
            rows: 原始行列表
            
        Returns:
            去重后的行列表
        """
        if not rows:
            return rows
        
        seen_signatures = set()
        deduplicated_rows = []
        
        for row in rows:
            signature = self.create_row_signature(row)
            
            if signature not in seen_signatures:
                seen_signatures.add(signature)
                deduplicated_rows.append(row)
        
        if len(deduplicated_rows) < len(rows):
            self.logger.info(f"🔄 去重：{len(rows)} 行 → {len(deduplicated_rows)} 行")
        
        return deduplicated_rows
    
    def create_row_signature(self, row: TableRow) -> str:
        """创建行签名用于去重
        
        Args:
            row: 表格行
            
        Returns:
            行的唯一签名字符串
        """
        values = []
        
        sorted_cells = sorted(row.cells.items(), key=lambda x: x[0])
        
        for field_name, cell_data in sorted_cells:
            if hasattr(cell_data, 'value'):
                value = cell_data.value
            else:
                value = str(cell_data)
            
            normalized = self.normalize_value_for_dedup(value)
            if normalized:
                values.append(f"{field_name}:{normalized}")
        
        return "||".join(values)
    
    def normalize_value_for_dedup(self, value) -> str:
        """标准化值用于去重
        
        Args:
            value: 原始值
            
        Returns:
            标准化后的字符串
        """
        if value is None:
            return ""
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, (int, float)):
            return str(value)
        return str(value).strip().lower()
    
    
    def generate_default_schema_from_prompt(self, table_name: str, 
                                           nl_prompt: str) -> Dict[str, Any]:
        """基于自然语言提示生成默认schema
        
        Args:
            table_name: 表名
            nl_prompt: 自然语言提示
            
        Returns:
            生成的schema字典
        """
        default_attributes = [
            {'name': 'id', 'type': 'TEXT', 'description': '记录唯一标识'}
        ]
        
        if nl_prompt and nl_prompt.strip():
            prompt_lower = nl_prompt.lower()
            
            if any(word in prompt_lower for word in ['订单编号', 'order_id', '编号', 'id']):
                default_attributes.append({'name': 'order_id', 'type': 'TEXT', 'description': '订单编号'})
            if any(word in prompt_lower for word in ['客户', '姓名', 'customer', 'name']):
                default_attributes.append({'name': 'customer_name', 'type': 'TEXT', 'description': '客户姓名'})
            if any(word in prompt_lower for word in ['金额', 'amount', '价格', 'price', '费用']):
                default_attributes.append({'name': 'amount', 'type': 'DECIMAL', 'description': '金额'})
            if any(word in prompt_lower for word in ['日期', 'date', '时间', 'time']):
                default_attributes.append({'name': 'date', 'type': 'DATE', 'description': '日期'})
            if any(word in prompt_lower for word in ['商品', 'product', '产品', 'item']):
                default_attributes.append({'name': 'product', 'type': 'TEXT', 'description': '商品信息'})
        
        if len(default_attributes) == 1:
            default_attributes.extend([
                {'name': 'content', 'type': 'TEXT', 'description': '提取的内容'},
                {'name': 'category', 'type': 'TEXT', 'description': '分类信息'}
            ])
        
        return {
            'tables': [{
                'name': table_name,
                'attributes': default_attributes
            }]
        }
    
    
    def build_simple_schema_prompt(self, table_def: Dict[str, Any]) -> str:
        """构建简单的schema prompt
        
        Args:
            table_def: 表定义
            
        Returns:
            schema提示字符串
        """
        table_name = table_def.get('name', 'data_table')
        attributes = table_def.get('attributes', [])
        
        prompt = f"Target Table: {table_name}\n\nFields:\n"
        for attr in attributes:
            prompt += f"- {attr['name']} ({attr.get('type', 'TEXT')}): {attr.get('description', 'N/A')}\n"
        
        return prompt

