"""幂等ID生成规则"""
import hashlib
import uuid
from typing import Any, Dict, List
import json


class IdGenerator:
    """ID生成器，确保相同输入产生相同ID"""
    
    @staticmethod
    def generate_run_id(schema_path: str, docs_path: str, nl_prompt: str, table_name: str) -> str:
        """生成运行ID，基于输入参数的哈希"""
        content = f"{schema_path}:{docs_path}:{nl_prompt}:{table_name}"
        return f"run-{hashlib.sha256(content.encode()).hexdigest()[:16]}"
    
    @staticmethod
    def generate_tuple_id(table_name: str, row_index: int, row_content: Dict[str, Any]) -> str:
        """生成元组ID"""
        content_str = json.dumps(row_content, sort_keys=True, ensure_ascii=False)
        content_hash = hashlib.md5(content_str.encode()).hexdigest()[:8]
        return f"t-{table_name}-{row_index:03d}-{content_hash}"
    
    @staticmethod
    def generate_violation_id(table: str, tuple_id: str, attr: str, constraint_type: str) -> str:
        """生成违规ID"""
        content = f"{table}:{tuple_id}:{attr}:{constraint_type}"
        hash_val = hashlib.md5(content.encode()).hexdigest()[:8]
        return f"v-{hash_val}"
    
    @staticmethod
    def generate_fix_id(table: str, tuple_id: str, attr: str, fix_type: str, old_value: Any) -> str:
        """生成修复ID"""
        content = f"{table}:{tuple_id}:{attr}:{fix_type}:{str(old_value)}"
        hash_val = hashlib.md5(content.encode()).hexdigest()[:8]
        return f"f-{hash_val}"
    
    @staticmethod
    def generate_detector_id(detector_class: str, version: str = "v1") -> str:
        """生成检测器ID"""
        return f"{detector_class}.{version}"
    
    @staticmethod
    def generate_fixer_id(fixer_class: str, version: str = "v1") -> str:
        """生成修复器ID"""
        return f"{fixer_class}.{version}"
    
    @staticmethod
    def generate_table_id(table_name: str, schema_hash: str = None) -> str:
        """生成表格ID"""
        if schema_hash:
            return f"table-{table_name}-{schema_hash[:8]}"
        else:
            unique_suffix = str(uuid.uuid4())[:8]
            return f"table-{table_name}-{unique_suffix}"

