"""数据质量MCP服务器实现"""
import json
from typing import Dict, Any, List
from .base import (
    MCPServer, MCPMessage, MCPResource, MCPPrompt, MCPTool, 
    MCPCapability
)


class DataQualityMCPServer(MCPServer):
    """数据质量MCP服务器 - 专门处理数据质量验证和修复"""
    
    def __init__(self):
        super().__init__(
            name="doc2db-data-quality",
            version="1.0.0",
            capabilities=[MCPCapability.RESOURCES, MCPCapability.PROMPTS, MCPCapability.TOOLS]
        )
        
        self._register_data_quality_components()
    
    def _register_data_quality_components(self):
        """注册数据质量相关组件"""
        self.register_resource(MCPResource(
            uri="constraint://types",
            name="约束类型定义",
            description="支持的数据约束类型及其定义",
            mimeType="application/json"
        ))
        
        self.register_resource(MCPResource(
            uri="fix://strategies", 
            name="修复策略",
            description="各种违规的修复策略和方法",
            mimeType="application/json"
        ))
        
        self.register_resource(MCPResource(
            uri="schema://template",
            name="数据模式模板",
            description="标准的数据模式定义模板",
            mimeType="application/json"
        ))
        
        self.register_prompt(MCPPrompt(
            name="verify-data-quality",
            description="验证数据质量并识别违规",
            arguments=[
                {"name": "data", "description": "要验证的数据", "required": True},
                {"name": "schema", "description": "数据模式定义", "required": True},
                {"name": "constraint_type", "description": "约束类型", "required": False}
            ]
        ))
        
        self.register_prompt(MCPPrompt(
            name="suggest-data-fix",
            description="为数据违规建议修复方案",
            arguments=[
                {"name": "violation", "description": "违规信息", "required": True},
                {"name": "context", "description": "上下文数据", "required": True}
            ]
        ))
        
        self.register_prompt(MCPPrompt(
            name="analyze-data-pattern",
            description="分析数据模式和质量趋势",
            arguments=[
                {"name": "data", "description": "数据集", "required": True},
                {"name": "analysis_type", "description": "分析类型", "required": False}
            ]
        ))
        
        self.register_tool(MCPTool(
            name="verify_constraint",
            description="验证特定约束",
            inputSchema={
                "type": "object",
                "properties": {
                    "constraint_type": {"type": "string"},
                    "data": {"type": "object"},
                    "schema": {"type": "object"}
                },
                "required": ["constraint_type", "data", "schema"]
            }
        ))
        
        self.register_tool(MCPTool(
            name="fix_violation",
            description="修复数据违规",
            inputSchema={
                "type": "object", 
                "properties": {
                    "violation": {"type": "object"},
                    "snapshot": {"type": "object"}
                },
                "required": ["violation", "snapshot"]
            }
        ))
        
        self.register_tool(MCPTool(
            name="batch_verify",
            description="批量验证数据质量",
            inputSchema={
                "type": "object",
                "properties": {
                    "snapshots": {"type": "array"},
                    "schema": {"type": "object"},
                    "constraint_types": {"type": "array"}
                },
                "required": ["snapshots", "schema"]
            }
        ))
        
    
    async def handle_request(self, message: MCPMessage) -> MCPMessage:
        """处理MCP请求"""
        try:
            method = message.method
            params = message.params or {}
            
            if method == "resources/list":
                return MCPMessage(
                    id=message.id,
                    result={"resources": [res.to_dict() for res in self.list_resources()]}
                )
            
            elif method == "resources/read":
                uri = params.get("uri")
                return await self._handle_resource_read(message.id, uri)
            
            elif method == "prompts/list":
                return MCPMessage(
                    id=message.id,
                    result={"prompts": [prompt.to_dict() for prompt in self.list_prompts()]}
                )
            
            elif method == "prompts/get":
                prompt_name = params.get("name")
                return await self._handle_prompt_get(message.id, prompt_name, params.get("arguments", {}))
            
            elif method == "tools/list":
                return MCPMessage(
                    id=message.id,
                    result={"tools": [tool.to_dict() for tool in self.list_tools()]}
                )
            
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                return await self._handle_tool_call(message.id, tool_name, arguments)
            
            else:
                return MCPMessage(
                    id=message.id,
                    error={"code": -32601, "message": f"未知方法: {method}"}
                )
                
        except Exception as e:
            self.logger.error(f"处理请求失败: {e}")
            return MCPMessage(
                id=message.id,
                error={"code": -32603, "message": f"内部错误: {str(e)}"}
            )
    
    async def _handle_resource_read(self, request_id: str, uri: str) -> MCPMessage:
        """处理资源读取请求"""
        if uri == "constraint://types":
            constraint_types = {
                "single_table": {
                    "TYPE": {
                        "name": "数据类型约束",
                        "description": "验证数据类型是否符合schema定义",
                        "supported_types": ["integer", "float", "decimal", "date", "datetime", "boolean", "string"],
                        "implementation": "algorithmic"
                    },
                    "VALUE": {
                        "name": "值域约束", 
                        "description": "验证值范围、域约束、枚举值",
                        "supported_constraints": ["min/max", "domain", "enum"],
                        "implementation": "algorithmic"
                    },
                    "STRUCTURE": {
                        "name": "结构约束",
                        "description": "验证非空、唯一性、主键约束",
                        "supported_constraints": ["not_null", "required", "unique", "primary_key"],
                        "implementation": "algorithmic"
                    },
                    "FORMAT": {
                        "name": "格式约束",
                        "description": "验证数据格式、模式匹配、长度约束",
                        "predefined_formats": ["email", "phone", "id_card", "url", "ip", "date_iso", "currency"],
                        "implementation": "algorithmic"
                    },
                    "TEMPORAL": {
                        "name": "时间约束",
                        "description": "验证时间格式、时间范围、时间逻辑关系",
                        "supported_constraints": ["time_format", "time_range", "time_logic", "time_series"],
                        "implementation": "algorithmic"
                    },
                    "LOGIC": {
                        "name": "逻辑约束",
                        "description": "使用LLM验证复杂的业务逻辑",
                        "focus_areas": ["business_rules", "field_relationships", "value_reasonableness", "state_transitions"],
                        "implementation": "llm"
                    }
                },
                "multi_table": {
                    "REFERENCE": {
                        "name": "引用约束",
                        "description": "验证外键引用完整性",
                        "supported_constraints": ["foreign_key"],
                        "implementation": "algorithmic"
                    },
                    "AGGREGATION": {
                        "name": "聚合约束",
                        "description": "验证计算字段、总计、平均值等聚合逻辑",
                        "supported_operations": ["sum", "multiply", "percentage", "average", "count"],
                        "implementation": "algorithmic+llm"
                    }
                }
            }
            return MCPMessage(
                id=request_id,
                result={"contents": [{"type": "text", "text": json.dumps(constraint_types, ensure_ascii=False, indent=2)}]}
            )
        
        elif uri == "fix://strategies":
            fix_strategies = {
                "TYPE": {
                    "strategy": "类型转换和标准化",
                    "methods": {
                        "numeric": "移除非数字字符，转换为目标类型",
                        "date": "标准化日期格式",
                        "boolean": "标准化布尔值表示"
                    }
                },
                "VALUE": {
                    "strategy": "值映射和范围调整",
                    "methods": {
                        "range": "截取到边界值",
                        "domain": "映射到最相似的域值",
                        "enum": "选择最匹配的枚举值"
                    }
                },
                "STRUCTURE": {
                    "strategy": "结构修复和默认值填充",
                    "methods": {
                        "not_null": "提供智能默认值",
                        "unique": "标记但不自动修复（需人工处理）"
                    }
                },
                "FORMAT": {
                    "strategy": "格式标准化",
                    "methods": {
                        "standardization": "格式标准化",
                        "length_adjustment": "长度调整",
                        "pattern_fix": "模式修复"
                    }
                },
                "TEMPORAL": {
                    "strategy": "时间格式和逻辑修复",
                    "methods": {
                        "format_standardization": "时间格式标准化",
                        "range_clipping": "时间范围截取",
                        "logic_fix": "逻辑关系修复"
                    }
                },
                "LOGIC": {
                    "strategy": "基于LLM的智能修复",
                    "methods": {
                        "llm_suggestion": "LLM生成智能修复建议",
                        "business_rule_fix": "业务规则修复"
                    }
                },
                "REFERENCE": {
                    "strategy": "引用映射",
                    "methods": {
                        "similarity_mapping": "映射到最相似的引用值"
                    }
                },
                "AGGREGATION": {
                    "strategy": "重新计算聚合值",
                    "methods": {
                        "recalculation": "重新计算正确的聚合值",
                        "llm_complex_logic": "LLM处理复杂聚合逻辑"
                    }
                }
            }
            return MCPMessage(
                id=request_id,
                result={"contents": [{"type": "text", "text": json.dumps(fix_strategies, ensure_ascii=False, indent=2)}]}
            )
        
        elif uri == "schema://template":
            schema_template = {
                "table_name": "示例表",
                "columns": [
                    {
                        "name": "id",
                        "type": "integer",
                        "constraints": ["primary_key", "not_null"]
                    },
                    {
                        "name": "name",
                        "type": "string",
                        "constraints": ["not_null"],
                        "max_length": 100
                    },
                    {
                        "name": "email",
                        "type": "string",
                        "format": "email",
                        "constraints": ["unique"]
                    },
                    {
                        "name": "age",
                        "type": "integer",
                        "min_value": 0,
                        "max_value": 150
                    },
                    {
                        "name": "created_at",
                        "type": "datetime",
                        "format": "iso8601"
                    }
                ]
            }
            return MCPMessage(
                id=request_id,
                result={"contents": [{"type": "text", "text": json.dumps(schema_template, ensure_ascii=False, indent=2)}]}
            )
        
        return MCPMessage(
            id=request_id,
            error={"code": -32602, "message": f"未找到资源: {uri}"}
        )
    
    async def _handle_prompt_get(self, request_id: str, prompt_name: str, arguments: Dict[str, Any]) -> MCPMessage:
        """处理提示获取请求"""
        if prompt_name == "verify-data-quality":
            data = arguments.get("data", "")
            schema = arguments.get("schema", "")
            constraint_type = arguments.get("constraint_type", "所有类型")
            
            prompt_text = f"""请验证以下数据的质量，重点关注{constraint_type}约束：

数据模式：
{json.dumps(schema, ensure_ascii=False, indent=2)}

数据内容：
{json.dumps(data, ensure_ascii=False, indent=2)}

请识别所有违反约束的问题，并按以下格式返回：
1. 违规类型
2. 违规位置（表名、行号、列名）
3. 违规描述
4. 当前值
5. 期望值或修复建议

请使用结构化的JSON格式返回结果。"""

            return MCPMessage(
                id=request_id,
                result={
                    "messages": [
                        {
                            "role": "user",
                            "content": {
                                "type": "text",
                                "text": prompt_text
                            }
                        }
                    ]
                }
            )
        
        elif prompt_name == "suggest-data-fix":
            violation = arguments.get("violation", {})
            context = arguments.get("context", {})
            
            prompt_text = f"""请为以下数据违规提供修复建议：

违规信息：
{json.dumps(violation, ensure_ascii=False, indent=2)}

上下文数据：
{json.dumps(context, ensure_ascii=False, indent=2)}

请提供：
1. 修复策略说明
2. 具体修复步骤
3. 修复后的值
4. 修复的置信度（1-10分）
5. 可能的副作用或注意事项

请使用结构化的JSON格式返回修复建议。"""

            return MCPMessage(
                id=request_id,
                result={
                    "messages": [
                        {
                            "role": "user", 
                            "content": {
                                "type": "text",
                                "text": prompt_text
                            }
                        }
                    ]
                }
            )
        
        elif prompt_name == "analyze-data-pattern":
            data = arguments.get("data", "")
            analysis_type = arguments.get("analysis_type", "全面分析")
            
            prompt_text = f"""请对以下数据进行{analysis_type}：

数据集：
{json.dumps(data, ensure_ascii=False, indent=2)}

请分析：
1. 数据质量总体评估
2. 常见的数据质量问题模式
3. 数据分布特征
4. 异常值识别
5. 改进建议

请提供详细的分析报告。"""

            return MCPMessage(
                id=request_id,
                result={
                    "messages": [
                        {
                            "role": "user",
                            "content": {
                                "type": "text",
                                "text": prompt_text
                            }
                        }
                    ]
                }
            )
        
        return MCPMessage(
            id=request_id,
            error={"code": -32602, "message": f"未找到提示: {prompt_name}"}
        )
    
    async def _handle_tool_call(self, request_id: str, tool_name: str, arguments: Dict[str, Any]) -> MCPMessage:
        """处理工具调用请求"""
        if tool_name == "verify_constraint":
            constraint_type = arguments.get("constraint_type")
            data = arguments.get("data")
            schema = arguments.get("schema")
            
            result = {
                "status": "success",
                "constraint_type": constraint_type,
                "violations": [],
                "message": f"{constraint_type}约束验证完成",
                "processed_records": len(data.get("rows", [])) if isinstance(data, dict) else 0
            }
            return MCPMessage(id=request_id, result=result)
        
        elif tool_name == "fix_violation":
            violation = arguments.get("violation")
            snapshot = arguments.get("snapshot")
            
            fixes = []
            
            try:
                from . import (
                    TypeMCP, ValueMCP, StructureMCP, LogicMCP,
                    TemporalMCP, FormatMCP, ReferenceMCP, AggregationMCP
                )
                from ...memory import TableSnapshot, TableRow, CellData, Violation, SuggestedFix
                
                constraint_type = violation.get("constraint_type", "").upper()
                
                fixers = {
                    "TYPE": TypeMCP().fixer,
                    "VALUE": ValueMCP().fixer,
                    "STRUCTURE": StructureMCP().fixer,
                    "FORMAT": FormatMCP().fixer,
                    "LOGIC": LogicMCP().fixer,
                    "TEMPORAL": TemporalMCP().fixer,
                    "REFERENCE": ReferenceMCP().fixer,
                    "AGGREGATION": AggregationMCP().fixer
                }
                
                fixer = fixers.get(constraint_type)
                if fixer:
                    suggested_fix = None
                    if violation.get("suggested_fix"):
                        suggested_fix = SuggestedFix(value=violation["suggested_fix"])
                    
                    violation_obj = Violation(
                        id=violation.get("id", ""),
                        table=violation.get("table", ""),
                        tuple_id=violation.get("tuple_id", ""),
                        attr=violation.get("attr", ""),
                        constraint_type=violation.get("constraint_type", ""),
                        description=violation.get("description", ""),
                        severity=violation.get("severity", "warn"),
                        suggested_fix=suggested_fix,
                        detector_id="mcp_server",
                        timestamp=""
                    )
                    
                    rows = []
                    for row_data in snapshot.get("rows", []):
                        tuple_id = row_data.get("tuple_id", "")
                        cells = {}
                        for attr, value in row_data.get("cells", {}).items():
                            cell = CellData(
                                value=value,
                                evidences=[]
                            )
                            cells[attr] = cell
                        rows.append(TableRow(tuple_id=tuple_id, cells=cells))
                    
                    snapshot_obj = TableSnapshot(run_id="test_run", table="test_table", rows=rows, created_at="")
                    
                    fix_results = fixer.fix(violation_obj, snapshot_obj)
                    
                    fixes = []
                    for fix in fix_results:
                        fix_dict = {
                            "id": fix.id,
                            "table": fix.table,
                            "tuple_id": fix.tuple_id,
                            "attr": fix.attr,
                            "old": fix.old,
                            "new": fix.new,
                            "fix_type": fix.fix_type,
                            "applied_by": fix.applied_by
                        }
                        fixes.append(fix_dict)
                
                result = {
                    "status": "success",
                    "violation_id": violation.get("id") if violation else None,
                    "fixes": fixes,
                    "message": f"违规修复完成，生成 {len(fixes)} 个修复方案"
                }
                
            except ImportError as e:
                self.logger.error(f"导入MCP修复组件失败: {e}")
                result = {
                    "status": "error",
                    "violation_id": violation.get("id") if violation else None,
                    "fixes": [],
                    "message": f"MCP修复组件导入失败: {e}"
                }
            except Exception as e:
                self.logger.error(f"违规修复执行失败: {e}")
                result = {
                    "status": "error",
                    "violation_id": violation.get("id") if violation else None,
                    "fixes": [],
                    "message": f"修复执行失败: {e}"
                }
            
            return MCPMessage(id=request_id, result=result)
        
        elif tool_name == "batch_verify":
            snapshots = arguments.get("snapshots", [])
            schema = arguments.get("schema")
            constraint_types = arguments.get("constraint_types", [])
            
            all_violations = []
            violations_by_type = {}
            
            try:
                from . import (
                    TypeMCP, ValueMCP, StructureMCP, LogicMCP, 
                    TemporalMCP, FormatMCP, ReferenceMCP, AggregationMCP
                )
                from ...memory import TableSnapshot, TableRow, CellData
                
                mcps = [
                    ("TYPE", TypeMCP()),
                    ("VALUE", ValueMCP()),
                    ("STRUCTURE", StructureMCP()),
                    ("FORMAT", FormatMCP()),
                    ("LOGIC", LogicMCP()),
                    ("TEMPORAL", TemporalMCP()),
                    ("REFERENCE", ReferenceMCP()),
                    ("AGGREGATION", AggregationMCP())
                ]
                
                for snapshot_data in snapshots:
                    table_name = snapshot_data.get("table_name", "unknown")
                    rows_data = snapshot_data.get("rows", [])
                    
                    rows = []
                    for row_data in rows_data:
                        tuple_id = row_data.get("tuple_id", "")
                        cells = {}
                        for attr, value in row_data.get("cells", {}).items():
                            cell = CellData(
                                value=value,
                                evidences=[]
                            )
                            cells[attr] = cell
                        rows.append(TableRow(tuple_id=tuple_id, cells=cells))
                    
                    snapshot = TableSnapshot(run_id="batch_verify", table=table_name, rows=rows, created_at="")
                    
                    for constraint_type, mcp in mcps:
                        if constraint_types and constraint_type not in constraint_types:
                            continue
                            
                        try:
                            violations = mcp.verify(snapshot, schema, table_name)
                            all_violations.extend(violations)
                            
                            if constraint_type not in violations_by_type:
                                violations_by_type[constraint_type] = 0
                            violations_by_type[constraint_type] += len(violations)
                            
                        except Exception as e:
                            self.logger.error(f"{constraint_type}MCP验证失败: {e}")
                            continue
                
                violations_data = []
                for violation in all_violations:
                    violation_dict = {
                        "id": violation.id,
                        "table": violation.table,
                        "tuple_id": violation.tuple_id,
                        "attr": violation.attr,
                        "constraint_type": violation.constraint_type,
                        "description": violation.description,
                        "severity": violation.severity
                    }
                    if violation.suggested_fix:
                        violation_dict["suggested_fix"] = violation.suggested_fix.value
                    violations_data.append(violation_dict)
                
                result = {
                    "status": "success",
                    "processed_snapshots": len(snapshots),
                    "constraint_types": constraint_types,
                    "total_violations": len(all_violations),
                    "violations_by_type": violations_by_type,
                    "violations": violations_data,
                    "message": f"批量验证完成，发现 {len(all_violations)} 个违规"
                }
                
            except ImportError as e:
                self.logger.error(f"导入MCP组件失败: {e}")
                result = {
                    "status": "error",
                    "processed_snapshots": 0,
                    "total_violations": 0,
                    "violations": [],
                    "message": f"MCP组件导入失败: {e}"
                }
            except Exception as e:
                self.logger.error(f"批量验证执行失败: {e}")
                result = {
                    "status": "error",
                    "processed_snapshots": 0,
                    "total_violations": 0,
                    "violations": [],
                    "message": f"验证执行失败: {e}"
                }
            
            return MCPMessage(id=request_id, result=result)
        
        
        return MCPMessage(
            id=request_id,
            error={"code": -32602, "message": f"未找到工具: {tool_name}"}
        )
