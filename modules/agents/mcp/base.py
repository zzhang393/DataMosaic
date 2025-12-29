"""MCP (Model Context Protocol) 基础接口 - 基于模型上下文协议的数据质量处理框架"""
import logging
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum
from ...memory import TableSnapshot, Violation, Fix, ConstraintType


class MCPMessageType(Enum):
    """MCP消息类型"""
    REQUEST = "request"
    RESPONSE = "response"
    NOTIFICATION = "notification"


class MCPCapability(Enum):
    """MCP服务器能力"""
    RESOURCES = "resources"
    PROMPTS = "prompts"
    TOOLS = "tools"


@dataclass
class MCPMessage:
    """MCP消息基类"""
    jsonrpc: str = "2.0"
    id: Optional[str] = None
    method: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class MCPResource:
    """MCP资源定义"""
    uri: str
    name: str
    description: str
    mimeType: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MCPPrompt:
    """MCP提示定义"""
    name: str
    description: str
    arguments: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MCPTool:
    """MCP工具定义"""
    name: str
    description: str
    inputSchema: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MCPServer(ABC):
    """MCP服务器基类 - 提供数据质量处理的上下文和工具"""
    
    def __init__(self, name: str, version: str, capabilities: List[MCPCapability]):
        self.name = name
        self.version = version
        self.capabilities = {cap.value: {} for cap in capabilities}
        self.logger = logging.getLogger(f'doc2db.mcp.{name}')
        
        self.resources: Dict[str, MCPResource] = {}
        self.prompts: Dict[str, MCPPrompt] = {}
        self.tools: Dict[str, MCPTool] = {}
    
    def get_server_info(self) -> Dict[str, Any]:
        """获取服务器信息"""
        return {
            "name": self.name,
            "version": self.version,
            "capabilities": self.capabilities
        }
    
    def register_resource(self, resource: MCPResource):
        """注册资源"""
        self.resources[resource.uri] = resource
    
    def register_prompt(self, prompt: MCPPrompt):
        """注册提示"""
        self.prompts[prompt.name] = prompt
    
    def register_tool(self, tool: MCPTool):
        """注册工具"""
        self.tools[tool.name] = tool
    
    @abstractmethod
    async def handle_request(self, message: MCPMessage) -> MCPMessage:
        """处理MCP请求"""
        pass
    
    def list_resources(self) -> List[MCPResource]:
        """列出所有资源"""
        return list(self.resources.values())
    
    def list_prompts(self) -> List[MCPPrompt]:
        """列出所有提示"""
        return list(self.prompts.values())
    
    def list_tools(self) -> List[MCPTool]:
        """列出所有工具"""
        return list(self.tools.values())


class MCPClient:
    """MCP客户端 - 与MCP服务器通信"""
    
    def __init__(self, client_name: str):
        self.client_name = client_name
        self.logger = logging.getLogger(f'doc2db.mcp.client.{client_name}')
        self.connected_servers: Dict[str, MCPServer] = {}
    
    def connect_server(self, server: MCPServer):
        """连接到MCP服务器"""
        self.connected_servers[server.name] = server
        self.logger.info(f"连接到MCP服务器: {server.name}")
    
    async def send_request(self, server_name: str, method: str, 
                          params: Optional[Dict[str, Any]] = None) -> MCPMessage:
        """发送请求到指定服务器"""
        if server_name not in self.connected_servers:
            raise ValueError(f"未连接到服务器: {server_name}")
        
        server = self.connected_servers[server_name]
        request = MCPMessage(
            id=f"{self.client_name}_{method}_{id(params)}",
            method=method,
            params=params
        )
        
        return await server.handle_request(request)
    
    async def list_server_resources(self, server_name: str) -> List[MCPResource]:
        """列出服务器资源"""
        response = await self.send_request(server_name, "resources/list")
        if response.result:
            return [MCPResource(**res) for res in response.result.get("resources", [])]
        return []
    
    async def get_resource(self, server_name: str, uri: str) -> Optional[Dict[str, Any]]:
        """获取指定资源"""
        response = await self.send_request(server_name, "resources/read", {"uri": uri})
        return response.result
    
    async def call_tool(self, server_name: str, tool_name: str, 
                       arguments: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """调用工具"""
        response = await self.send_request(server_name, "tools/call", {
            "name": tool_name,
            "arguments": arguments
        })
        return response.result


def get_table_definition(schema: Dict[str, Any], table_name: str) -> Optional[Dict[str, Any]]:
    """获取表格定义 - 通用辅助函数"""
    if not isinstance(schema, dict):
        return None
        
    tables = schema.get('tables', [])
    
    if isinstance(tables, list):
        for table in tables:
            if table.get('name') == table_name:
                result = table.copy()
                if 'fields' in result and 'attributes' not in result:
                    result['attributes'] = result.pop('fields')
                
                if 'attributes' in result:
                    flattened_attributes = []
                    for attr in result['attributes']:
                        attr_copy = attr.copy()
                        if 'constraints' in attr_copy:
                            constraints = attr_copy.pop('constraints')
                            for key, value in constraints.items():
                                if key not in attr_copy:  # 不覆盖已有的字段
                                    attr_copy[key] = value
                        flattened_attributes.append(attr_copy)
                    result['attributes'] = flattened_attributes
                
                return result
        
        if '_' in table_name and table_name.rsplit('_', 1)[-1].isdigit():
            base_name = table_name.rsplit('_', 1)[0]
            suffix_num = int(table_name.rsplit('_', 1)[1])
            
            matching_tables = [
                table for table in tables
                if isinstance(table, dict) and table.get('name') == base_name
            ]
            
            if matching_tables and 1 <= suffix_num <= len(matching_tables):
                target_table = matching_tables[suffix_num - 1]
                result = target_table.copy()
                if 'fields' in result and 'attributes' not in result:
                    result['attributes'] = result.pop('fields')
                
                if 'attributes' in result:
                    flattened_attributes = []
                    for attr in result['attributes']:
                        attr_copy = attr.copy()
                        if 'constraints' in attr_copy:
                            constraints = attr_copy.pop('constraints')
                            for key, value in constraints.items():
                                if key not in attr_copy:  # 不覆盖已有的字段
                                    attr_copy[key] = value
                        flattened_attributes.append(attr_copy)
                    result['attributes'] = flattened_attributes
                
                return result
    elif isinstance(tables, dict):
        if table_name in tables:
            table_def = tables[table_name].copy()
            if 'name' not in table_def:
                table_def['name'] = table_name
            if 'fields' in table_def and 'attributes' not in table_def:
                table_def['attributes'] = table_def.pop('fields')
            
            if 'attributes' in table_def:
                flattened_attributes = []
                for attr in table_def['attributes']:
                    attr_copy = attr.copy()
                    if 'constraints' in attr_copy:
                        constraints = attr_copy.pop('constraints')
                        for key, value in constraints.items():
                            if key not in attr_copy:  # 不覆盖已有的字段
                                attr_copy[key] = value
                    flattened_attributes.append(attr_copy)
                table_def['attributes'] = flattened_attributes
            
            return table_def
    
    if schema.get('table_name') == table_name and 'columns' in schema:
        columns = schema.get('columns', {})
        attributes = []
        
        if isinstance(columns, dict):
            for col_name, col_def in columns.items():
                attr = {'name': col_name}
                attr.update(col_def)  # 添加类型、约束等信息
                attributes.append(attr)
        elif isinstance(columns, list):
            attributes = columns
            
        return {
            'name': schema['table_name'],
            'attributes': attributes
        }
    
    return None


def get_cell_value(violation: Violation, snapshot: TableSnapshot) -> Optional[Any]:
    """获取单元格值 - 通用辅助函数（支持tuple_id格式容错）"""
    import logging
    logger = logging.getLogger('doc2db.mcp')
    
    target_tuple_id = violation.tuple_id.strip('[]').strip()
    
    for row in snapshot.rows:
        row_tuple_id = row.tuple_id.strip('[]').strip()
        if row_tuple_id == target_tuple_id:
            cell_data = row.cells.get(violation.attr)
            if cell_data:
                return cell_data.value
    
    logger.debug(f"在当前快照中未找到tuple_id={violation.tuple_id}（快照有{len(snapshot.rows)}行）")
    logger.debug(f"这可能发生在处理的中间阶段，通常可以忽略")
    
    if logger.isEnabledFor(logging.DEBUG):
        sample_ids = [row.tuple_id for row in snapshot.rows[:5]]
        logger.debug(f"快照中的前5个tuple_id示例: {sample_ids}")
        logger.debug(f"尝试匹配的tuple_id: '{target_tuple_id}'")
    
    return None



class MCPVerifier(ABC):
    """MCP验证器基类 - 传统架构"""
    
    def __init__(self, verifier_id: str):
        self.verifier_id = verifier_id
        self.mcp_id = verifier_id  # 添加mcp_id属性以兼容
        self.logger = logging.getLogger(f'doc2db.mcp.verifier.{verifier_id}')
    
    @abstractmethod
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], table_name: str) -> List[Violation]:
        """验证数据快照"""
        pass


class MCPFixer(ABC):
    """MCP修复器基类 - 传统架构"""
    
    def __init__(self, fixer_id: str):
        self.fixer_id = fixer_id
        self.mcp_id = fixer_id  # 添加mcp_id属性以兼容
        self.logger = logging.getLogger(f'doc2db.mcp.fixer.{fixer_id}')
    
    @abstractmethod
    def fix(self, violation: Violation, snapshot: TableSnapshot, context=None) -> List[Fix]:
        """修复违规"""
        pass
    
    def fix_batch(self, violations: List[Violation], snapshot: TableSnapshot, 
                  context=None) -> List[Fix]:
        """
        批量修复违规 - 默认实现（子类可以重写以提供更高效的批量修复）
        
        默认行为：逐个调用 fix() 方法
        子类应该重写此方法以实现真正的批量修复（一次LLM调用处理所有违规）
        
        Args:
            violations: 需要修复的违规列表
            snapshot: 表格快照
            context: 上下文信息
            
        Returns:
            List[Fix]: 修复对象列表
        """
        all_fixes = []
        for violation in violations:
            try:
                fixes = self.fix(violation, snapshot, context)
                all_fixes.extend(fixes)
            except Exception as e:
                self.logger.error(f"修复违规 {getattr(violation, 'id', 'unknown')} 失败: {e}")
        return all_fixes
    
    def get_unfixable_violations(self, fixes: List[Fix]) -> List[Fix]:
        """
        获取无法修复的违规列表 - 基类默认实现
        
        Args:
            fixes: 修复结果列表
            
        Returns:
            List[Fix]: 修复失败的Fix对象列表
        """
        unfixable_fixes = []
        for fix in fixes:
            if not getattr(fix, 'fix_success', True):  # 默认为True以兼容旧版本
                unfixable_fixes.append(fix)
                self.logger.warning(f"发现无法修复的违规: {getattr(fix, 'id', 'unknown')}, 原因: {getattr(fix, 'failure_reason', 'unknown')}")
        
        self.logger.info(f"筛选出 {len(unfixable_fixes)} 个无法修复的违规")
        return unfixable_fixes


class BaseMCP:
    """MCP组件基类 - 传统架构"""
    
    def __init__(self, mcp_id: str, verifier: MCPVerifier, fixer: MCPFixer):
        self.mcp_id = mcp_id
        self.verifier = verifier
        self.fixer = fixer
        self.logger = logging.getLogger(f'doc2db.mcp.{mcp_id}')
        
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], table_name: str) -> List[Violation]:
        """验证数据"""
        self.logger.info(f"开始验证表格 {table_name}")
        try:
            violations = self.verifier.verify(snapshot, schema, table_name)
            self.logger.info(f"验证完成，发现 {len(violations)} 个违规")
            return violations
        except Exception as e:
            self.logger.error(f"验证失败: {e}")
            return []
    
    def fix(self, violation: Violation, snapshot: TableSnapshot) -> List[Fix]:
        """修复违规"""
        try:
            fixes = self.fixer.fix(violation, snapshot)
            return fixes
        except Exception as e:
            self.logger.error(f"修复失败: {e}")
            return []


def is_valid_fix_value(value: str) -> bool:
    """验证建议修复值是否为有效的数据值（而非提示性文本）
    
    这是一个共享的工具函数，供所有Fixer使用。
    
    Args:
        value: 建议修复值
        
    Returns:
        True表示是有效数据值，False表示可能是提示文本
    """
    if not value or not isinstance(value, str):
        return False
    
    prompt_keywords = [
        "确认", "检查", "建议", "请", "是否", "或者", "可能", "需要",
        "应该", "必须", "注意", "提示", "警告", "说明", "参考",
        "数据源", "采集逻辑", "业务逻辑", "特定需求", "合理的",
        "问题", "错误", "修改", "更正", "调整"
    ]
    
    if len(value) > 100:
        return False
    
    keyword_count = sum(1 for keyword in prompt_keywords if keyword in value)
    if keyword_count >= 2:
        return False
    
    if any(char in value for char in ['。', '？', '！', '?', '!']):
        return False
    
    return True