"""违规检测契约定义"""
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json


class ViolationSeverity(Enum):
    """违规严重程度"""
    ERROR = "error"
    WARN = "warn"
    INFO = "info"


class ConstraintType(Enum):
    """约束类型 - 基于图中的分类体系"""
    TYPE = "type"
    VALUE = "value"
    STRUCTURE = "structure"
    LOGIC = "logic"
    TEMPORAL = "temporal"
    FORMAT = "format"
    
    REFERENCE = "reference"
    CROSS_TABLE_LOGIC = "cross_table_logic"
    CROSS_TABLE_TEMPORAL = "cross_table_temporal"
    AGGREGATION = "aggregation"



@dataclass
class SuggestedFix:
    """建议修复"""
    value: Any
    
    def to_dict(self) -> Dict[str, Any]:
        return {"value": self.value}


@dataclass
class Violation:
    """违规记录"""
    id: str
    table: str
    tuple_id: str
    attr: str
    constraint_type: str
    description: str
    severity: str
    suggested_fix: Optional[SuggestedFix]
    detector_id: str
    timestamp: str
    processing_category: Optional[str] = None  # 处理分类：requires_reextraction 或 tool_fixable
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "id": self.id,
            "table": self.table,
            "tuple_id": self.tuple_id,
            "attr": self.attr,
            "constraint_type": self.constraint_type,
            "description": self.description,
            "severity": self.severity,
            "detector_id": self.detector_id,
            "timestamp": self.timestamp
        }
        
        if self.suggested_fix:
            result["suggested_fix"] = self.suggested_fix.to_dict()
        
        if self.processing_category:
            from enum import Enum
            if isinstance(self.processing_category, Enum):
                result["processing_category"] = self.processing_category.value
            else:
                result["processing_category"] = self.processing_category
        
        return result
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Violation':
        """从字典创建违规对象"""
        suggested_fix = None
        if 'suggested_fix' in data:
            fix_data = data['suggested_fix']
            suggested_fix = SuggestedFix(
                value=fix_data.get('value')
            )
        
        return cls(
            id=data.get('id', ''),
            table=data.get('table', ''),
            tuple_id=data.get('tuple_id', ''),
            attr=data.get('attr', ''),
            constraint_type=data.get('constraint_type', ''),
            description=data.get('description', ''),
            severity=data.get('severity', 'warn'),
            suggested_fix=suggested_fix,
            detector_id=data.get('detector_id', ''),
            timestamp=data.get('timestamp', ''),
            processing_category=data.get('processing_category', None)
        )

