"""类型约束MCP - 验证和修复数据类型相关问题"""
import re
from typing import List, Dict, Any, Optional
from .base import MCPVerifier, MCPFixer, BaseMCP, get_table_definition, get_cell_value, is_valid_fix_value
from ...memory import TableSnapshot, Violation, Fix, SuggestedFix, ConstraintType, ViolationSeverity, FixType
from ...core.ids import IdGenerator


class TypeVerifier(MCPVerifier):
    """类型约束验证器"""
    
    def __init__(self):
        super().__init__("TypeVerifier.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.TYPE.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.TYPE.value
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """验证表格中的类型约束"""
        violations = []
        table_def = get_table_definition(schema, table_name)
        
        if not table_def:
            return violations
        
        attributes = table_def.get('attributes', [])
        attr_dict = {attr['name']: attr for attr in attributes}
        
        for row in snapshot.rows:
            for attr_name, cell_data in row.cells.items():
                if attr_name not in attr_dict:
                    continue
                
                attr_def = attr_dict[attr_name]
                value = cell_data.value
                
                cell_violations = self.verify_cell(
                    snapshot.table, row.tuple_id, attr_name, 
                    value, attr_def, snapshot, context
                )
                violations.extend(cell_violations)
        
        return violations
    
    def verify_cell(self, table: str, tuple_id: str, attr: str, 
                   value: Any, attr_def: Dict[str, Any], 
                   snapshot: TableSnapshot, context=None) -> List[Violation]:
        """验证单个单元格的类型约束"""
        violations = []
        expected_type = attr_def.get('type', 'string')
        
        if value is None or str(value).strip() == '':
            return violations
        
        if not self._is_valid_type(str(value), expected_type):
            violation_id = IdGenerator.generate_violation_id(
                table, tuple_id, attr, ConstraintType.TYPE.value
            )
            
            suggested_value = self._suggest_type_fix(str(value), expected_type)
            
            violation = Violation(
                id=violation_id,
                table=table,
                tuple_id=tuple_id,
                attr=attr,
                constraint_type=ConstraintType.TYPE.value,
                description=f"字段 {attr} 期望类型为 {expected_type}，但值 '{value}' 不符合类型要求",
                severity=ViolationSeverity.WARN.value,
                
                suggested_fix=SuggestedFix(value=suggested_value, ),
                detector_id=self.mcp_id,
                timestamp=""
            )
            violations.append(violation)
        
        decimal_violation = self._check_decimal_precision(str(value), expected_type, table, tuple_id, attr)
        if decimal_violation:
            violations.append(decimal_violation)
        
        return violations
    
    def _is_valid_type(self, value: str, expected_type: str) -> bool:
        """检查值是否符合期望类型"""
        type_checks = {
            'integer': self._is_integer,
            'int': self._is_integer,
            'float': self._is_float,
            'decimal': self._is_decimal,
            'number': self._is_number,
            'date': self._is_date,
            'datetime': self._is_datetime,
            'timestamp': self._is_datetime,
            'boolean': self._is_boolean,
            'bool': self._is_boolean,
            'string': lambda x: True,  # 字符串总是有效
            'text': lambda x: True,
        }
        
        check_func = type_checks.get(expected_type.lower())
        return check_func(value) if check_func else True
    
    def _is_integer(self, value: str) -> bool:
        """检查是否为整数"""
        try:
            cleaned = re.sub(r'[,¥$€£]', '', value.strip())
            int(cleaned)
            return True
        except ValueError:
            return False
    
    def _is_float(self, value: str) -> bool:
        """检查是否为浮点数"""
        try:
            cleaned = re.sub(r'[,¥$€£]', '', value.strip())
            float(cleaned)
            return True
        except ValueError:
            return False
    
    def _is_decimal(self, value: str) -> bool:
        """检查是否为小数"""
        cleaned = re.sub(r'[,¥$€£]', '', value.strip())
        return re.match(r'^\d+\.\d+$', cleaned) is not None
    
    def _is_number(self, value: str) -> bool:
        """检查是否为数字（整数或浮点数）"""
        return self._is_integer(value) or self._is_float(value)
    
    def _is_date(self, value: str) -> bool:
        """检查是否为日期格式"""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # 2024-01-15
            r'^\d{4}/\d{2}/\d{2}$',  # 2024/01/15
            r'^\d{2}/\d{2}/\d{4}$',  # 01/15/2024
            r'^\d{2}-\d{2}-\d{4}$',  # 01-15-2024
            r'^\d{4}\d{2}\d{2}$',    # 20240115
        ]
        return any(re.match(pattern, value.strip()) for pattern in date_patterns)
    
    def _is_datetime(self, value: str) -> bool:
        """检查是否为日期时间格式"""
        value = value.strip()
        if ' ' in value:
            date_part = value.split(' ')[0]
            return self._is_date(date_part)
        return self._is_date(value)
    
    def _is_boolean(self, value: str) -> bool:
        """检查是否为布尔值"""
        value_lower = value.strip().lower()
        return value_lower in [
            'true', 'false', '1', '0', 'yes', 'no', 
            '是', '否', 'y', 'n', 'on', 'off'
        ]
    
    def _check_decimal_precision(self, value: str, expected_type: str, 
                                 table: str, tuple_id: str, attr: str) -> Optional[Violation]:
        """检查DECIMAL类型的精度和小数位数"""
        if not expected_type.upper().startswith('DECIMAL'):
            return None
        
        match = re.match(r'DECIMAL\((\d+),\s*(\d+)\)', expected_type, re.IGNORECASE)
        if not match:
            return None
        
        precision = int(match.group(1))  # 总位数
        scale = int(match.group(2))       # 小数位数
        
        cleaned_value = re.sub(r'[,¥$€£]', '', value.strip())
        
        try:
            float_value = float(cleaned_value)
        except ValueError:
            return None
        
        if '.' in cleaned_value:
            decimal_part = cleaned_value.split('.')[1]
            actual_scale = len(decimal_part)
        else:
            actual_scale = 0
        
        if actual_scale != scale:
            violation_id = IdGenerator.generate_violation_id(
                table, tuple_id, attr, ConstraintType.TYPE.value
            )
            
            format_string = f"{{:.{scale}f}}"
            suggested_value = format_string.format(float_value)
            
            violation = Violation(
                id=violation_id,
                table=table,
                tuple_id=tuple_id,
                attr=attr,
                constraint_type=ConstraintType.TYPE.value,
                description=f"字段 {attr} 类型为 {expected_type}，要求{scale}位小数，但当前值 '{value}' 有{actual_scale}位小数",
                severity=ViolationSeverity.WARN.value,
                suggested_fix=SuggestedFix(value=suggested_value),
                detector_id=self.mcp_id,
                timestamp=""
            )
            return violation
        
        return None
    
    def _suggest_type_fix(self, value: str, expected_type: str) -> str:
        """建议类型修复值"""
        if expected_type.lower() in ['date', 'datetime', 'timestamp']:
            if re.match(r'\d{4}/\d{2}/\d{2}', value):
                return value.replace('/', '-')
            elif re.match(r'\d{2}/\d{2}/\d{4}', value):
                parts = value.split('/')
                return f"20{parts[2]}-{parts[0]}-{parts[1]}"
            elif re.match(r'\d{4}\d{2}\d{2}', value):
                return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
        
        elif expected_type.upper().startswith('DECIMAL'):
            match = re.match(r'DECIMAL\((\d+),\s*(\d+)\)', expected_type, re.IGNORECASE)
            if match:
                scale = int(match.group(2))
                cleaned = re.sub(r'[^\d.-]', '', value)
                try:
                    float_value = float(cleaned)
                    format_string = f"{{:.{scale}f}}"
                    return format_string.format(float_value)
                except ValueError:
                    return cleaned if cleaned else "0.0000"
            cleaned = re.sub(r'[^\d.-]', '', value)
            return cleaned if cleaned else "0"
        
        elif expected_type.lower() in ['integer', 'int', 'float', 'decimal', 'number']:
            cleaned = re.sub(r'[^\d.-]', '', value)
            return cleaned if cleaned else "0"
        
        elif expected_type.lower() in ['boolean', 'bool']:
            value_lower = value.strip().lower()
            if value_lower in ['true', '1', 'yes', '是', 'y', 'on']:
                return 'true'
            elif value_lower in ['false', '0', 'no', '否', 'n', 'off']:
                return 'false'
        
        return value


class TypeFixer(MCPFixer):
    """类型约束修复器"""
    
    def __init__(self):
        super().__init__("TypeFixer.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.TYPE.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.TYPE.value
    
    def can_fix(self, violation: Violation) -> bool:
        return violation.constraint_type == ConstraintType.TYPE.value
    
    def get_supported_fix_types(self) -> List[str]:
        return [FixType.TYPE_CONVERSION.value, FixType.FORMAT_NORMALIZE.value]
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, 
            context=None) -> List[Fix]:
        """修复类型约束违规"""
        if not self.can_fix(violation):
            return []
        
        fixes = []
        old_value = get_cell_value(violation, snapshot)
        
        if not old_value:
            return fixes
        
        if violation.suggested_fix and violation.suggested_fix.value:
            suggested_value = violation.suggested_fix.value
            if is_valid_fix_value(suggested_value):
                new_value = suggested_value
                
                if str(new_value) != str(old_value):
                    fix_id = IdGenerator.generate_fix_id(
                        violation.table, violation.tuple_id, violation.attr,
                        FixType.TYPE_CONVERSION.value, old_value
                    )
                    
                    fix = Fix(
                        id=fix_id,
                        table=violation.table,
                        tuple_id=violation.tuple_id,
                        attr=violation.attr,
                        old=old_value,
                        new=new_value,
                        fix_type=FixType.TYPE_CONVERSION.value,
                        applied_by=self.mcp_id,
                        timestamp="",
                        fix_success=True
                    )
                    fixes.append(fix)
                    self.logger.info(f"类型修复: {violation.attr} '{old_value}' -> '{new_value}'")
            else:
                self.logger.warning(f"建议修复值'{suggested_value}'看起来是提示文本而非数据值，跳过修复")
        
        return fixes
    
    def fix_batch(self, violations: List[Violation], snapshot: TableSnapshot, 
                  context=None) -> List[Fix]:
        """
        批量修复类型约束违规
        
        策略：对于有 suggested_fix 的直接应用，对于没有的使用 LLM 批量转换
        """
        all_fixes = []
        
        with_suggestion = []
        need_llm = []
        
        for violation in violations:
            if not self.can_fix(violation):
                continue
            
            if violation.suggested_fix and violation.suggested_fix.value:
                with_suggestion.append(violation)
            else:
                need_llm.append(violation)
        
        for violation in with_suggestion:
            fixes = self.fix(violation, snapshot, context)
            all_fixes.extend(fixes)
        
        if need_llm:
            llm_fixes = self._batch_fix_with_llm(need_llm, snapshot, context)
            all_fixes.extend(llm_fixes)
        
        if all_fixes:
            self.logger.info(f"TypeFixer: 批量修复 {len(violations)} 个违规，生成 {len(all_fixes)} 个修复")
        
        return all_fixes
    
    def _batch_fix_with_llm(self, violations: List[Violation], 
                           snapshot: TableSnapshot, context=None) -> List[Fix]:
        """使用LLM批量进行类型转换"""
        fixes = []
        
        try:
            from llm.main import get_answer
            import json
            
            violations_info = []
            for i, v in enumerate(violations):
                old_value = get_cell_value(v, snapshot)
                violations_info.append({
                    "index": i,
                    "tuple_id": v.tuple_id,
                    "attr": v.attr,
                    "current_value": old_value,
                    "description": v.description,
                    "expected_type": v.description.split("应为")[-1].split("类型")[0].strip() if "应为" in v.description else "unknown"
                })
            
            system_prompt = """You are a data type conversion expert. Convert values to the correct data type.
Output in strict JSON format: {"fixes": [{"index": 0, "new_value": "converted value"}, ...]}
- For INTEGER: convert to integer (e.g., "123.45" -> "123", "1,234" -> "1234")
- For DECIMAL: convert to decimal (e.g., "1,234.56" -> "1234.56")
- For DATE: convert to ISO format (e.g., "2024/01/01" -> "2024-01-01")
- For BOOLEAN: convert to true/false
Use null if conversion is impossible."""
            
            user_prompt = f"""Convert the following values to their expected types:

Table: {snapshot.table}
Type violations to fix:
{json.dumps(violations_info, ensure_ascii=False, indent=2)}

Output ONLY JSON in format: {{"fixes": [{{"index": 0, "new_value": "..."}}]}}
"""
            
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            llm_response = get_answer(user_prompt, system_prompt=system_prompt, model=model)
            
            fixes_data = self._parse_llm_fixes(llm_response)
            
            for fix_data in fixes_data:
                idx = fix_data.get('index')
                new_value = fix_data.get('new_value')
                
                if idx is None or idx >= len(violations):
                    continue
                
                violation = violations[idx]
                old_value = get_cell_value(violation, snapshot)
                
                if new_value is None or str(new_value).lower() == 'null':
                    continue
                
                if not is_valid_fix_value(str(new_value)):
                    continue
                
                fix_id = IdGenerator.generate_fix_id(
                    violation.table, violation.tuple_id, violation.attr,
                    FixType.TYPE_CONVERSION.value, old_value
                )
                
                fix = Fix(
                    id=fix_id,
                    table=violation.table,
                    tuple_id=violation.tuple_id,
                    attr=violation.attr,
                    old=old_value,
                    new=str(new_value),
                    fix_type=FixType.TYPE_CONVERSION.value,
                    applied_by=self.mcp_id,
                    timestamp="",
                    fix_success=True
                )
                fixes.append(fix)
            
        except Exception as e:
            self.logger.error(f"LLM批量类型转换失败: {e}")
        
        return fixes
    
    def _parse_llm_fixes(self, llm_response: str) -> List[Dict[str, Any]]:
        """解析LLM返回的修复数据"""
        import json
        import re
        
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', llm_response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = llm_response.strip()
        
        try:
            data = json.loads(json_str)
            
            if isinstance(data, dict) and 'fixes' in data:
                return data['fixes']
            elif isinstance(data, list):
                return data
            else:
                return []
                
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析失败: {e}")
            return []


class TypeMCP(BaseMCP):
    """类型约束MCP"""
    
    def __init__(self):
        verifier = TypeVerifier()
        fixer = TypeFixer()
        super().__init__("TypeMCP.v1", verifier, fixer)
