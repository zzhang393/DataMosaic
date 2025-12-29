"""MCP (Model Context Protocol) 接口模块 - 基于模型上下文协议的数据质量处理框架"""

from .base import (
    MCPServer, MCPClient, MCPMessage, MCPResource, MCPPrompt, MCPTool,
    MCPMessageType, MCPCapability, get_table_definition, get_cell_value
)
from .data_quality_server import DataQualityMCPServer


try:
    from .type_mcp import TypeMCP
    from .value_mcp import ValueMCP
    from .structure_mcp import StructureMCP
    from .logic_mcp import LogicMCP
    from .temporal_mcp import TemporalMCP
    from .format_mcp import FormatMCP
    from .reference_mcp import ReferenceMCP
    from .aggregation_mcp import AggregationMCP
    

    LEGACY_MCPS = [
        'TypeMCP', 'ValueMCP', 'StructureMCP', 'LogicMCP',
        'TemporalMCP', 'FormatMCP', 'ReferenceMCP', 'AggregationMCP'
    ]
except ImportError:
    LEGACY_MCPS = []

__all__ = [
    'MCPServer', 'MCPClient', 'MCPMessage', 'MCPResource', 'MCPPrompt', 'MCPTool',
    'MCPMessageType', 'MCPCapability', 'DataQualityMCPServer',
    'get_table_definition', 'get_cell_value'
] + LEGACY_MCPS
