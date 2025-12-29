"""修复记录契约定义"""
from typing import Dict, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json


class FixType(Enum):
    """修复类型"""
    FORMAT_NORMALIZE = "format_normalize"
    TYPE_CONVERSION = "type_conversion"
    DEFAULT_VALUE = "default_value"
    UNIT_MAPPING = "unit_mapping"
    PATTERN_FIX = "pattern_fix"
    RANGE_CLAMP = "range_clamp"
    REFERENCE_FIX = "reference_fix"
    
    DOMAIN_MAPPING = "domain_mapping"
    VALUE_CORRECTION = "value_correction"
    STRUCTURE_FIX = "structure_fix"
    LENGTH_ADJUST = "length_adjust"
    TEMPORAL_FIX = "temporal_fix"
    LOGIC_FIX = "logic_fix"
    FORMAT_FIX = "format_fix"
    BUSINESS_RULE_FIX = "business_rule_fix"
    CALCULATION_FIX = "calculation_fix"
    AGGREGATION_FIX = "aggregation_fix"
    FOREIGN_KEY_FIX = "foreign_key_fix"


@dataclass
class Fix:
    """修复记录"""
    id: str
    table: str
    tuple_id: str
    attr: str
    old: Any
    new: Any
    fix_type: str
    applied_by: str
    timestamp: str
    fix_success: bool = True
    failure_reason: str = ""
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "table": self.table,
            "tuple_id": self.tuple_id,
            "attr": self.attr,
            "old": self.old,
            "new": self.new,
            "fix_type": self.fix_type,
            "applied_by": self.applied_by,
            "timestamp": self.timestamp,
            "fix_success": self.fix_success,
            "failure_reason": self.failure_reason
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Fix':
        """从字典创建修复对象"""
        return cls(
            id=data.get('id', ''),
            table=data.get('table', ''),
            tuple_id=data.get('tuple_id', ''),
            attr=data.get('attr', ''),
            old=data.get('old'),
            new=data.get('new'),
            fix_type=data.get('fix_type', ''),
            applied_by=data.get('applied_by', ''),
            timestamp=data.get('timestamp', ''),
            fix_success=data.get('fix_success', True),
            failure_reason=data.get('failure_reason', '')
        )

