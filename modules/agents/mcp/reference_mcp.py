"""引用约束MCP - 验证和修复外键引用相关问题"""
from typing import List, Dict, Any, Optional
from .base import MCPVerifier, MCPFixer, BaseMCP, get_table_definition, get_cell_value, is_valid_fix_value
from ...memory import TableSnapshot, Violation, Fix, SuggestedFix, ConstraintType, ViolationSeverity, FixType
from ...core.ids import IdGenerator


class ReferenceVerifier(MCPVerifier):
    """引用约束验证器"""
    
    def __init__(self):
        super().__init__("ReferenceVerifier.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.REFERENCE.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.REFERENCE.value
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """验证表格中的引用约束"""
        violations = []
        table_def = get_table_definition(schema, table_name)
        
        if not table_def:
            return violations
        
        all_snapshots = context.all_snapshots if context and hasattr(context, 'all_snapshots') else {}
        
        attributes = table_def.get('attributes', [])
        
        for attr_def in attributes:
            if self._has_foreign_key(attr_def):
                fk_violations = self._verify_foreign_key_constraint(
                    snapshot, attr_def, table_name, all_snapshots, schema
                )
                violations.extend(fk_violations)
        
        return violations
    
    def verify_cell(self, table: str, tuple_id: str, attr: str, 
                   value: Any, attr_def: Dict[str, Any], 
                   snapshot: TableSnapshot, context=None) -> List[Violation]:
        """验证单个单元格的引用约束"""
        violations = []
        
        if value is None or str(value).strip() == '':
            return violations
        
        has_fk = self._has_foreign_key(attr_def)
        if has_fk:
            all_snapshots = context.all_snapshots if context and hasattr(context, 'all_snapshots') else {}
            
            schema = None
            if context and hasattr(context, 'schema'):
                schema = context.schema
            
            fk_violations = self._check_foreign_key_reference(
                table, tuple_id, attr, value, attr_def, all_snapshots, schema
            )
            violations.extend(fk_violations)
        
        return violations
    
    def _has_foreign_key(self, attr_def: Dict[str, Any]) -> bool:
        """检查属性是否有外键约束"""
        if 'foreign_key' in attr_def:
            return True
        
        constraints = attr_def.get('constraints', {})
        if isinstance(constraints, dict) and constraints.get('foreign_key'):
            return True
        
        return False
    
    def _get_foreign_key_info(self, attr_def: Dict[str, Any], table_name: str, 
                             schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """获取外键信息"""
        if 'foreign_key' in attr_def:
            fk = attr_def['foreign_key']
            if isinstance(fk, dict):
                return fk
            elif fk is True:
                return self._find_relation_info(table_name, attr_def['name'], schema)
        
        constraints = attr_def.get('constraints', {})
        if isinstance(constraints, dict):
            fk = constraints.get('foreign_key')
            if fk:
                if isinstance(fk, dict):
                    return fk
                elif fk is True:
                    return self._find_relation_info(table_name, attr_def['name'], schema)
        
        return None
    
    def _find_relation_info(self, table_name: str, field_name: str, 
                           schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """从schema的relations部分查找外键关系信息"""
        relations = schema.get('relations', [])
        
        for relation in relations:
            from_info = relation.get('from', {})
            if from_info.get('table') == table_name and from_info.get('field') == field_name:
                to_info = relation.get('to', {})
                return {
                    'table': to_info.get('table'),
                    'field': to_info.get('field', 'id')
                }
        
        return None
    
    def _verify_foreign_key_constraint(self, snapshot: TableSnapshot, 
                                      attr_def: Dict[str, Any], 
                                      table_name: str,
                                      all_snapshots: Dict[str, TableSnapshot],
                                      schema: Dict[str, Any]) -> List[Violation]:
        """验证外键约束"""
        violations = []
        
        fk_info = self._get_foreign_key_info(attr_def, table_name, schema)
        if not fk_info or not isinstance(fk_info, dict):
            return violations
        
        attr_name = attr_def['name']
        ref_table = fk_info.get('table')
        ref_field = fk_info.get('field', 'id')
        
        if not ref_table:
            return violations
        
        ref_snapshot = all_snapshots.get(ref_table)
        if not ref_snapshot:
            self.logger.warning(f"引用表 {ref_table} 的数据不可用，跳过外键验证")
            return violations
        
        valid_ref_values = set()
        for ref_row in ref_snapshot.rows:
            if ref_field in ref_row.cells:
                ref_value = ref_row.cells[ref_field].value
                if ref_value is not None:
                    valid_ref_values.add(str(ref_value))
        
        for row in snapshot.rows:
            if attr_name in row.cells:
                fk_value = row.cells[attr_name].value
                if fk_value is not None and str(fk_value).strip():
                    str_fk_value = str(fk_value).strip()
                    
                    if str_fk_value not in valid_ref_values:
                        violation_id = IdGenerator.generate_violation_id(
                            table_name, row.tuple_id, attr_name, ConstraintType.REFERENCE.value
                        )
                        
                        suggested_value = self._find_closest_reference_value(
                            str_fk_value, valid_ref_values
                        )
                        
                        violation = Violation(
                            id=violation_id,
                            table=table_name,
                            tuple_id=row.tuple_id,
                            attr=attr_name,
                            constraint_type=ConstraintType.REFERENCE.value,
                            description=f"外键 {attr_name} 值 '{fk_value}' 在引用表 {ref_table}.{ref_field} 中不存在",
                            severity=ViolationSeverity.ERROR.value,
                            
                            suggested_fix=SuggestedFix(value=suggested_value, ),
                            detector_id=self.mcp_id,
                            timestamp=""
                        )
                        violations.append(violation)
        
        return violations
    
    def _check_foreign_key_reference(self, table: str, tuple_id: str, attr: str, 
                                    value: Any, attr_def: Dict[str, Any],
                                    all_snapshots: Dict[str, TableSnapshot],
                                    schema: Optional[Dict[str, Any]] = None) -> List[Violation]:
        """检查单个外键引用"""
        violations = []
        
        fk_info = None
        if schema:
            fk_info = self._get_foreign_key_info(attr_def, table, schema)
        else:
            fk_info = attr_def.get('foreign_key', {})
        
        if not fk_info or not isinstance(fk_info, dict):
            return violations
        
        ref_table = fk_info.get('table')
        ref_field = fk_info.get('field', 'id')
        
        if not ref_table:
            return violations
        
        ref_snapshot = all_snapshots.get(ref_table)
        if not ref_snapshot:
            return violations
        
        str_value = str(value).strip()
        ref_exists = False
        
        for ref_row in ref_snapshot.rows:
            if ref_field in ref_row.cells:
                ref_value = ref_row.cells[ref_field].value
                if ref_value is not None and str(ref_value).strip() == str_value:
                    ref_exists = True
                    break
        
        if not ref_exists:
            violation_id = IdGenerator.generate_violation_id(
                table, tuple_id, attr, ConstraintType.REFERENCE.value
            )
            
            valid_ref_values = set()
            for ref_row in ref_snapshot.rows:
                if ref_field in ref_row.cells:
                    ref_value = ref_row.cells[ref_field].value
                    if ref_value is not None:
                        valid_ref_values.add(str(ref_value))
            
            suggested_value = self._find_closest_reference_value(str_value, valid_ref_values)
            
            violation = Violation(
                id=violation_id,
                table=table,
                tuple_id=tuple_id,
                attr=attr,
                constraint_type=ConstraintType.REFERENCE.value,
                description=f"外键 {attr} 值 '{value}' 在引用表 {ref_table}.{ref_field} 中不存在",
                severity=ViolationSeverity.ERROR.value,
                
                suggested_fix=SuggestedFix(value=suggested_value, ),
                detector_id=self.mcp_id,
                timestamp=""
            )
            violations.append(violation)
        
        return violations
    
    def _find_closest_reference_value(self, value: str, valid_values: set) -> str:
        """找到最接近的引用值"""
        if not valid_values:
            return value
        
        for valid_val in valid_values:
            if value.lower() == valid_val.lower():
                return valid_val
        
        best_match = list(valid_values)[0]
        best_score = 0
        
        for valid_val in valid_values:
            value_chars = set(value.lower())
            valid_chars = set(valid_val.lower())
            intersection = len(value_chars & valid_chars)
            union = len(value_chars | valid_chars)
            
            if union > 0:
                score = intersection / union
                if score > best_score:
                    best_score = score
                    best_match = valid_val
        
        return best_match


class ReferenceFixer(MCPFixer):
    """引用约束修复器"""
    
    def __init__(self):
        super().__init__("ReferenceFixer.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.REFERENCE.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.REFERENCE.value
    
    def can_fix(self, violation: Violation) -> bool:
        return violation.constraint_type == ConstraintType.REFERENCE.value
    
    def get_supported_fix_types(self) -> List[str]:
        return [
            FixType.REFERENCE_FIX.value,
            FixType.FOREIGN_KEY_FIX.value
        ]
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, 
            context=None) -> List[Fix]:
        """修复引用约束违规"""
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
            else:
                self.logger.warning(f"建议修复值'{suggested_value}'看起来是提示文本而非数据值，跳过修复")
                return fixes
            
            fix_id = IdGenerator.generate_fix_id(
                violation.table, violation.tuple_id, violation.attr,
                FixType.REFERENCE_FIX.value, old_value
            )
            
            fix = Fix(
                id=fix_id,
                table=violation.table,
                tuple_id=violation.tuple_id,
                attr=violation.attr,
                old=old_value,
                new=new_value,
                fix_type=FixType.REFERENCE_FIX.value,
                applied_by=self.mcp_id,
                timestamp="",

            )
            fixes.append(fix)
        
        return fixes


class ReferenceMCP(BaseMCP):
    """引用约束MCP"""
    
    def __init__(self):
        verifier = ReferenceVerifier()
        fixer = ReferenceFixer()
        super().__init__("ReferenceMCP.v1", verifier, fixer)
