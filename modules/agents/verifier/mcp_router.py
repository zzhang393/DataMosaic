"""智能MCP路由器 - 使用LLM Agent选择合适的验证组件"""
import json
import logging
from typing import List, Dict, Any, Tuple
from ...memory import TableSnapshot

from baml_src.client_selector import load_env_from_llm_folder, get_baml_options
load_env_from_llm_folder()

from baml_client.sync_client import b as baml_client


class MCPRouter:
    """智能MCP路由器 - 基于LLM的验证组件选择"""
    
    def __init__(self):
        self.logger = logging.getLogger('doc2db.mcp_router')
        
        self.mandatory_mcps = [
            "TypeMCP",       # 类型验证
            "StructureMCP"   # 结构验证（非空、唯一性等）
        ]
        
        self.optional_mcps = {
            "ValueMCP": {
                "description": "值域约束验证器",
                "scenarios": "验证数值范围、枚举值、字符串长度等值域约束。适用于有明确取值范围的字段。"
            },
            "FormatMCP": {
                "description": "格式约束验证器",
                "scenarios": "验证电话号码、邮箱、身份证、编号等格式。适用于有固定格式要求的字段。"
            },
            "TemporalMCP": {
                "description": "时间约束验证器",
                "scenarios": "验证日期时间的合理性和逻辑关系。适用于包含日期/时间字段的表格。"
            },
            "ReferenceMCP": {
                "description": "引用约束验证器",
                "scenarios": "验证外键引用完整性。适用于有外键关系的表格。"
            },
            "AggregationMCP": {
                "description": "聚合计算验证器",
                "scenarios": "验证计算字段的正确性（如总额=单价×数量）。适用于有计算关系的表格。"
            },
            "LogicMCP": {
                "description": "业务逻辑验证器（使用LLM，成本较高）",
                "scenarios": "验证复杂的业务规则和逻辑一致性。适用于金融、合同、交易等复杂业务场景。"
            }
        }
        
        self.logger.info(f"初始化MCP路由器: {len(self.mandatory_mcps)} 个必要MCP, {len(self.optional_mcps)} 个可选MCP")
    
    def select_mcps(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
                   table_name: str) -> Tuple[List[str], Dict[str, Any]]:
        """
        选择需要执行的MCP组件
        
        Args:
            snapshot: 表格快照
            schema: 表结构定义
            table_name: 表名
            
        Returns:
            (selected_mcps, routing_info): 选中的MCP列表和路由决策信息
        """
        table_schema_str = self._format_table_schema(schema, table_name)
        data_sample_str = self._format_data_sample(snapshot, max_rows=5)
        available_mcps_str = self._format_available_mcps()
        
        try:
            baml_options = get_baml_options()
            
            
            decision = baml_client.SelectMCPsForVerification(
                table_name=table_name,
                table_schema=table_schema_str,
                data_sample=data_sample_str,
                available_mcps=available_mcps_str,
                baml_options=baml_options
            )
            
            selected_optional = decision.selected_mcps if decision.selected_mcps else []
            
            all_selected = self.mandatory_mcps + selected_optional
            
            all_optional = set(self.optional_mcps.keys())
            selected_optional_set = set(selected_optional)
            skipped = list(all_optional - selected_optional_set)
            
            routing_info = {
                "mandatory_mcps": self.mandatory_mcps,
                "optional_mcps_selected": selected_optional,
                "optional_mcps_skipped": skipped
            }
            
            
            return all_selected, routing_info
            
        except Exception as e:
            self.logger.error(f"LLM路由决策失败: {e}")
            
            all_mcps = self.mandatory_mcps + list(self.optional_mcps.keys())
            routing_info = {
                "mandatory_mcps": self.mandatory_mcps,
                "optional_mcps_selected": list(self.optional_mcps.keys()),
                "optional_mcps_skipped": []
            }
            
            return all_mcps, routing_info
    
    def _format_table_schema(self, schema: Dict[str, Any], table_name: str) -> str:
        """格式化表结构定义"""
        try:
            tables = schema.get('tables', [])
            table_def = None
            
            for table in tables:
                if table.get('name') == table_name:
                    table_def = table
                    break
            
            if not table_def:
                return f"表 {table_name} 的结构定义未找到"
            
            attributes = table_def.get('attributes', [])
            schema_lines = [f"表名: {table_name}"]
            schema_lines.append(f"字段数: {len(attributes)}")
            schema_lines.append("\n字段列表:")
            
            for attr in attributes:
                attr_name = attr.get('name', 'unknown')
                attr_type = attr.get('type', 'unknown')
                constraints = attr.get('constraints', {})
                
                constraint_strs = []
                if constraints.get('not_null'):
                    constraint_strs.append("NOT NULL")
                if constraints.get('unique'):
                    constraint_strs.append("UNIQUE")
                if 'min' in constraints or 'max' in constraints:
                    constraint_strs.append(f"范围约束")
                if 'enum' in constraints:
                    constraint_strs.append(f"枚举值")
                if 'format' in constraints:
                    constraint_strs.append(f"格式: {constraints['format']}")
                
                constraint_str = f" [{', '.join(constraint_strs)}]" if constraint_strs else ""
                schema_lines.append(f"  - {attr_name}: {attr_type}{constraint_str}")
            
            return "\n".join(schema_lines)
            
        except Exception as e:
            self.logger.error(f"格式化表结构失败: {e}")
            return f"表结构格式化失败: {str(e)}"
    
    def _format_data_sample(self, snapshot: TableSnapshot, max_rows: int = 5) -> str:
        """格式化数据样本"""
        try:
            if not snapshot.rows:
                return "数据为空"
            
            sample_rows = snapshot.rows[:max_rows]
            
            if sample_rows:
                field_names = list(sample_rows[0].cells.keys())
            else:
                return "数据为空"
            
            lines = [f"数据行数: {len(snapshot.rows)} (显示前{len(sample_rows)}行)"]
            lines.append("")
            
            header = "| " + " | ".join(field_names) + " |"
            separator = "|" + "|".join(["---" for _ in field_names]) + "|"
            lines.append(header)
            lines.append(separator)
            
            for row in sample_rows:
                values = []
                for field_name in field_names:
                    cell = row.cells.get(field_name)
                    value = str(cell.value) if cell and cell.value is not None else ""
                    if len(value) > 30:
                        value = value[:27] + "..."
                    values.append(value)
                
                line = "| " + " | ".join(values) + " |"
                lines.append(line)
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.error(f"格式化数据样本失败: {e}")
            return f"数据样本格式化失败: {str(e)}"
    
    def _format_available_mcps(self) -> str:
        """格式化可用的MCP列表"""
        lines = []
        for mcp_name, mcp_info in self.optional_mcps.items():
            lines.append(f"- {mcp_name}: {mcp_info['scenarios']}")
        return "\n".join(lines)

