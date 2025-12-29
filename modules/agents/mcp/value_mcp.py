"""值约束MCP - 验证和修复值范围、域约束等问题"""
import re
from typing import List, Dict, Any, Optional
from .base import MCPVerifier, MCPFixer, BaseMCP, get_table_definition, get_cell_value, is_valid_fix_value
from ...memory import TableSnapshot, Violation, Fix, SuggestedFix, ConstraintType, ViolationSeverity, FixType
from ...core.ids import IdGenerator


class ValueVerifier(MCPVerifier):
    """值约束验证器"""
    
    def __init__(self):
        super().__init__("ValueVerifier.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [
            ConstraintType.VALUE.value,
        ]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type in self.get_supported_constraints()
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """验证表格中的值约束"""
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
        
        if context and hasattr(context, 'all_snapshots'):
            cross_table_violations = self._check_cross_table_entity_consistency(
                table_name, snapshot, context.all_snapshots, schema
            )
            violations.extend(cross_table_violations)
        
        return violations
    
    def verify_cell(self, table: str, tuple_id: str, attr: str, 
                   value: Any, attr_def: Dict[str, Any], 
                   snapshot: TableSnapshot, context=None) -> List[Violation]:
        """验证单个单元格的值约束"""
        violations = []
        
        if value is None or str(value).strip() == '' or str(value).lower() == 'null':
            constraints = attr_def.get('constraints', [])
            has_not_null_constraint = (
                'NOT NULL' in constraints or
                (isinstance(constraints, dict) and constraints.get('nullable') == False)
            )
            
            attr_type = attr_def.get('type', '').upper()
            is_numeric_type = any(t in attr_type for t in ['DECIMAL', 'INTEGER', 'FLOAT', 'DOUBLE', 'NUMBER', 'INT'])
            
            has_non_null_in_other_rows = self._check_field_has_values_in_snapshot(
                snapshot, attr, table, tuple_id
            )
            
            if has_not_null_constraint or (is_numeric_type and has_non_null_in_other_rows):
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.VALUE.value
                )
                
                suggested_value = self._suggest_null_fix(attr, attr_def)
                
                if has_not_null_constraint:
                    description = f"字段 {attr} 缺失值（空值/null），违反了NOT NULL约束"
                    severity = ViolationSeverity.ERROR.value
                else:
                    description = f"字段 {attr} 缺失值（空值/null），建议从文档中重新提取补充该值"
                    severity = ViolationSeverity.ERROR.value  # 改为ERROR以触发warm start
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.VALUE.value,
                    description=description,
                    severity=severity,
                    suggested_fix=SuggestedFix(value=suggested_value),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
            return violations
        
        range_violations = self._check_range_constraint(
            table, tuple_id, attr, value, attr_def
        )
        violations.extend(range_violations)
        
        domain_violations = self._check_domain_constraint(
            table, tuple_id, attr, value, attr_def
        )
        violations.extend(domain_violations)
        
        enum_violations = self._check_enum_constraint(
            table, tuple_id, attr, value, attr_def
        )
        violations.extend(enum_violations)
        
        return violations
    
    def _check_range_constraint(self, table: str, tuple_id: str, attr: str, 
                               value: Any, attr_def: Dict[str, Any]) -> List[Violation]:
        """检查范围约束"""
        violations = []
        min_val = attr_def.get('min')
        max_val = attr_def.get('max')
        
        constraints = attr_def.get('constraints', [])
        for constraint in constraints:
            if isinstance(constraint, str):
                if 'CHECK >=' in constraint:
                    try:
                        min_val = float(constraint.split('>=')[1].strip())
                    except (ValueError, IndexError):
                        pass
                elif 'CHECK <=' in constraint:
                    try:
                        max_val = float(constraint.split('<=')[1].strip())
                    except (ValueError, IndexError):
                        pass
                elif 'CHECK >' in constraint:
                    try:
                        min_val = float(constraint.split('>')[1].strip()) + 0.001
                    except (ValueError, IndexError):
                        pass
                elif 'CHECK <' in constraint:
                    try:
                        max_val = float(constraint.split('<')[1].strip()) - 0.001
                    except (ValueError, IndexError):
                        pass
        
        if (min_val is not None or max_val is not None) and value is not None:
            try:
                numeric_value = self._extract_numeric_value(str(value))
                
                if min_val is not None and numeric_value < min_val:
                    violation_id = IdGenerator.generate_violation_id(
                        table, tuple_id, attr, ConstraintType.VALUE.value
                    )
                    
                    violation = Violation(
                        id=violation_id,
                        table=table,
                        tuple_id=tuple_id,
                        attr=attr,
                        constraint_type=ConstraintType.VALUE.value,
                        description=f"字段 {attr} 值 {value} 小于最小值 {min_val}",
                        severity=ViolationSeverity.WARN.value,
                        suggested_fix=SuggestedFix(value=str(min_val)),
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violations.append(violation)
                
                elif max_val is not None and numeric_value > max_val:
                    violation_id = IdGenerator.generate_violation_id(
                        table, tuple_id, attr, ConstraintType.VALUE.value
                    )
                    
                    violation = Violation(
                        id=violation_id,
                        table=table,
                        tuple_id=tuple_id,
                        attr=attr,
                        constraint_type=ConstraintType.VALUE.value,
                        description=f"字段 {attr} 值 {value} 大于最大值 {max_val}",
                        severity=ViolationSeverity.WARN.value,
                        suggested_fix=SuggestedFix(value=str(max_val)),
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violations.append(violation)
                    
            except ValueError:
                pass
        
        return violations
    
    def _check_domain_constraint(self, table: str, tuple_id: str, attr: str, 
                                value: Any, attr_def: Dict[str, Any]) -> List[Violation]:
        """检查域约束"""
        violations = []
        domain = attr_def.get('domain')
        
        constraints = attr_def.get('constraints', [])
        for constraint in constraints:
            if isinstance(constraint, str) and 'CHECK IN' in constraint:
                try:
                    in_part = constraint.split('CHECK IN')[1].strip()
                    if in_part.startswith('(') and in_part.endswith(')'):
                        values_str = in_part[1:-1]  # 移除括号
                        domain = [val.strip().strip("'\"") for val in values_str.split(',')]
                except (IndexError, ValueError):
                    pass
        
        if domain and value is not None and str(value).strip():
            str_value = str(value).strip()
            if str_value not in domain:
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.VALUE.value
                )
                
                suggested_value = self._find_closest_domain_value(str_value, domain)
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.VALUE.value,
                    description=f"字段 {attr} 值 '{value}' 不在允许的域 {domain} 中",
                    severity=ViolationSeverity.WARN.value,
                    suggested_fix=SuggestedFix(value=suggested_value),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations
    
    def _check_enum_constraint(self, table: str, tuple_id: str, attr: str, 
                              value: Any, attr_def: Dict[str, Any]) -> List[Violation]:
        """检查枚举值约束"""
        violations = []
        enum_values = attr_def.get('enum')
        
        if enum_values and value is not None and str(value).strip():
            str_value = str(value).strip()
            if str_value not in enum_values:
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.VALUE.value
                )
                
                suggested_value = self._find_closest_domain_value(str_value, enum_values)
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.VALUE.value,
                    description=f"字段 {attr} 值 '{value}' 不在允许的枚举值 {enum_values} 中",
                    severity=ViolationSeverity.WARN.value,
                    
                    suggested_fix=SuggestedFix(value=suggested_value, ),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations
    
    def _extract_numeric_value(self, value: str) -> float:
        """从字符串中提取数值"""
        cleaned = re.sub(r'[,¥$€£\s]', '', value.strip())
        return float(cleaned)
    
    def _find_closest_domain_value(self, value: str, domain: List[str]) -> str:
        """找到最接近的域值"""
        if not domain:
            return value
        
        for domain_val in domain:
            if value.lower() == domain_val.lower():
                return domain_val
        
        best_match = domain[0]
        best_score = 0
        
        for domain_val in domain:
            value_chars = set(value.lower())
            domain_chars = set(domain_val.lower())
            intersection = len(value_chars & domain_chars)
            union = len(value_chars | domain_chars)
            
            if union > 0:
                score = intersection / union
                if score > best_score:
                    best_score = score
                    best_match = domain_val
        
        return best_match
    
    def _check_field_has_values_in_snapshot(self, snapshot: TableSnapshot, 
                                            attr: str, table: str, 
                                            current_tuple_id: str) -> bool:
        """
        检查快照中其他记录是否有该字段的非null值
        如果有，说明这个字段通常应该有值，当前记录的null可能是遗漏
        """
        if not snapshot or not snapshot.rows:
            return False
        
        for row in snapshot.rows:
            if row.tuple_id == current_tuple_id:
                continue
            
            if attr in row.cells:
                cell_value = row.cells[attr].value
                if cell_value is not None and str(cell_value).strip() != '' and str(cell_value).lower() != 'null':
                    return True
        
        return False
    
    def _check_cross_table_entity_consistency(self, current_table: str, 
                                             current_snapshot: TableSnapshot,
                                             all_snapshots: Dict[str, TableSnapshot],
                                             schema: Dict[str, Any]) -> List[Violation]:
        """检测跨表相似实体表示不一致
        
        例如：一个表中是"格力电器"，另一个表中是"格力电器股份有限公司"
        
        Args:
            current_table: 当前表名
            current_snapshot: 当前表的快照
            all_snapshots: 所有表的快照字典
            schema: 数据库schema
            
        Returns:
            跨表实体不一致的违规列表
        """
        violations = []
        
        if len(all_snapshots) < 2:
            return violations
        
        if not current_snapshot.rows:
            return violations
        
        current_fields = set(current_snapshot.rows[0].cells.keys())
        
        for other_table_name, other_snapshot in all_snapshots.items():
            if other_table_name == current_table:
                continue
            
            if not other_snapshot.rows:
                continue
            
            other_fields = set(other_snapshot.rows[0].cells.keys())
            common_fields = current_fields & other_fields
            
            for field_name in common_fields:
                field_violations = self._check_field_entity_consistency(
                    current_table, current_snapshot,
                    other_table_name, other_snapshot,
                    field_name
                )
                violations.extend(field_violations)
        
        return violations
    
    def _check_field_entity_consistency(self, table1: str, snapshot1: TableSnapshot,
                                       table2: str, snapshot2: TableSnapshot,
                                       field_name: str) -> List[Violation]:
        """检查特定字段在两个表之间的实体一致性"""
        violations = []
        
        values1 = set()
        for row in snapshot1.rows:
            if field_name in row.cells:
                val = row.cells[field_name].value
                if val is not None and str(val).strip():
                    values1.add(str(val).strip())
        
        values2 = set()
        for row in snapshot2.rows:
            if field_name in row.cells:
                val = row.cells[field_name].value
                if val is not None and str(val).strip():
                    values2.add(str(val).strip())
        
        if not values1 or not values2:
            return violations
        
        similar_groups = self._detect_similar_entity_groups(values1, values2)
        
        if not similar_groups:
            return violations
        
        self.logger.info(f"  🔍 字段 '{field_name}' 在表 {table1} 和 {table2} 间发现 {len(similar_groups)} 组相似实体")
        
        for similar_group in similar_groups:
            canonical_form = max(similar_group, key=len)
            
            for form in similar_group:
                if form != canonical_form and form in values1:
                    violations.extend(self._generate_entity_violations(
                        table1, snapshot1, field_name, form, canonical_form, similar_group
                    ))
            
            self.logger.info(f"    相似实体组: {similar_group}")
            self.logger.info(f"    标准形式: {canonical_form}")
        
        return violations
    
    def _generate_entity_violations(self, table: str, snapshot: TableSnapshot,
                                   field_name: str, current_form: str,
                                   canonical_form: str, similar_group: set) -> List[Violation]:
        """为使用非标准形式的记录生成违规"""
        violations = []
        
        for row in snapshot.rows:
            if field_name not in row.cells:
                continue
            
            cell_value = row.cells[field_name].value
            if cell_value is not None and str(cell_value).strip() == current_form:
                violation_id = IdGenerator.generate_violation_id(
                    table, row.tuple_id, field_name, ConstraintType.VALUE.value
                )
                
                description = (
                    f"跨表实体表示不一致: 字段 '{field_name}' 的值 '{current_form}' "
                    f"与其他表中的相似实体 {similar_group} 表示不统一，建议统一为 '{canonical_form}'"
                )
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=row.tuple_id,
                    attr=field_name,
                    constraint_type=ConstraintType.VALUE.value,
                    description=description,
                    severity=ViolationSeverity.WARN.value,
                    suggested_fix=SuggestedFix(value=canonical_form),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations
    
    def _detect_similar_entity_groups(self, values1: set, values2: set) -> List[set]:
        """检测两个值集合中的相似实体组
        
        Returns:
            相似实体组列表，每组包含跨表的相似值
        """
        similar_groups = []
        all_values = values1 | values2
        grouped_values = set()
        
        for val1 in all_values:
            if val1 in grouped_values:
                continue
            
            similar_group = {val1}
            grouped_values.add(val1)
            
            for val2 in all_values:
                if val2 == val1 or val2 in grouped_values:
                    continue
                
                if self._are_values_similar(val1, val2):
                    similar_group.add(val2)
                    grouped_values.add(val2)
            
            if len(similar_group) > 1:
                has_from_table1 = any(v in values1 for v in similar_group)
                has_from_table2 = any(v in values2 for v in similar_group)
                
                if has_from_table1 and has_from_table2:
                    similar_groups.append(similar_group)
        
        return similar_groups
    
    def _are_values_similar(self, val1: str, val2: str) -> bool:
        """判断两个值是否相似（可能指代同一实体）
        
        使用多种启发式规则判断：
        1. 子串关系：一个是另一个的子串（如"格力电器" vs "格力电器股份有限公司"）
        2. 公司/机构名称特征：去除后缀词后相同
        3. 编辑距离：Levenshtein距离较小
        """
        if not val1 or not val2:
            return False
        
        val1_str = str(val1).strip()
        val2_str = str(val2).strip()
        
        if val1_str == val2_str:
            return False  # 不算相似实体，算完全一致
        
        if val1_str in val2_str or val2_str in val1_str:
            min_len = min(len(val1_str), len(val2_str))
            if min_len >= 3:  # 至少3个字符
                return True
        
        entity_suffixes = [
            "股份有限公司", "有限公司", "集团", "股份公司", 
            "公司", "集团股份有限公司", "（集团）股份有限公司",
            "Co.,Ltd", "Inc.", "Corp.", "Ltd.",
            "大学", "学院", "研究所", "研究院", "中心"
        ]
        
        val1_core = val1_str
        val2_core = val2_str
        
        for suffix in entity_suffixes:
            val1_core = val1_core.replace(suffix, "")
            val2_core = val2_core.replace(suffix, "")
        
        val1_core = val1_core.strip()
        val2_core = val2_core.strip()
        
        if val1_core and val2_core:
            if val1_core == val2_core:
                return True
            
            if val1_core in val2_core or val2_core in val1_core:
                min_len = min(len(val1_core), len(val2_core))
                if min_len >= 2:
                    return True
        
        max_len = max(len(val1_str), len(val2_str))
        min_len = min(len(val1_str), len(val2_str))
        
        if max_len > 0:
            edit_distance = self._levenshtein_distance(val1_str, val2_str)
            similarity_ratio = 1 - (edit_distance / max_len)
            
            if similarity_ratio > 0.9 and min_len >= 5:
                return True
        
        return False
    
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """计算两个字符串的Levenshtein编辑距离"""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def _suggest_null_fix(self, attr: str, attr_def: Dict[str, Any]) -> str:
        """为NULL值生成建议修复值"""
        attr_type = attr_def.get('type', '').lower()
        
        if 'id' in attr.lower():
            return "1"
        elif 'name' in attr.lower() or 'varchar' in attr_type or 'text' in attr_type:
            return "未知"
        elif 'date' in attr_type:
            return "2024-01-01"
        elif 'decimal' in attr_type or 'float' in attr_type or 'number' in attr_type:
            return "0"
        elif 'status' in attr.lower():
            constraints = attr_def.get('constraints', [])
            for constraint in constraints:
                if isinstance(constraint, str) and 'CHECK IN' in constraint:
                    try:
                        in_part = constraint.split('CHECK IN')[1].strip()
                        if in_part.startswith('(') and in_part.endswith(')'):
                            values_str = in_part[1:-1]
                            values = [val.strip().strip("'\"") for val in values_str.split(',')]
                            if values:
                                return values[0]  # 返回第一个合法值
                    except (IndexError, ValueError):
                        pass
            return "待确认"
        else:
            return "待补充"


class ValueFixer(MCPFixer):
    """值约束修复器"""
    
    def __init__(self):
        super().__init__("ValueFixer.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [
            ConstraintType.VALUE.value,
        ]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type in self.get_supported_constraints()
    
    def can_fix(self, violation: Violation) -> bool:
        if violation.constraint_type not in self.get_supported_constraints():
            return False
        
        description = violation.description.lower() if violation.description else ""
        if any(keyword in description for keyword in ['缺失值', 'missing', '空值', 'null']):
            return False
        
        return True
    
    def get_supported_fix_types(self) -> List[str]:
        return [
            FixType.RANGE_CLAMP.value,
            FixType.DOMAIN_MAPPING.value,
            FixType.VALUE_CORRECTION.value
        ]
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, 
            context=None) -> List[Fix]:
        """修复值约束违规"""
        if not self.can_fix(violation):
            return []
        
        fixes = []
        old_value = get_cell_value(violation, snapshot)
        
        if not violation.suggested_fix or not violation.suggested_fix.value:
            return fixes
        
        if violation.suggested_fix and violation.suggested_fix.value:
            suggested_value = violation.suggested_fix.value
            if is_valid_fix_value(suggested_value):
                new_value = suggested_value
            else:
                self.logger.warning(f"建议修复值'{suggested_value}'看起来是提示文本而非数据值，跳过修复")
                return fixes
            
            fix_type = self._determine_fix_type(violation.constraint_type)
            
            fix_id = IdGenerator.generate_fix_id(
                violation.table, violation.tuple_id, violation.attr,
                fix_type, old_value
            )
            
            old_value_str = old_value if old_value is not None else ""
            
            fix = Fix(
                id=fix_id,
                table=violation.table,
                tuple_id=violation.tuple_id,
                attr=violation.attr,
                old=old_value_str,
                new=new_value,
                fix_type=fix_type,
                applied_by=self.mcp_id,
                timestamp=""
            )
            fixes.append(fix)
        
        return fixes
    
    def _determine_fix_type(self, constraint_type: str) -> str:
        """确定修复类型"""
        if constraint_type == ConstraintType.VALUE.value:
            return FixType.DOMAIN_MAPPING.value
        else:
            return FixType.VALUE_CORRECTION.value
    
    def fix_batch(self, violations: List[Violation], snapshot: TableSnapshot, 
                  context=None) -> List[Fix]:
        """
        批量修复值约束违规
        
        优化策略：
        1. 对于有 suggested_fix 的违规，直接应用修复（不需要LLM）
        2. 对于没有 suggested_fix 的违规，使用 LLM 批量推断修复值
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
            self.logger.info(f"ValueFixer: 批量修复 {len(violations)} 个违规，生成 {len(all_fixes)} 个修复")
        
        return all_fixes
    
    def _batch_fix_with_llm(self, violations: List[Violation], 
                           snapshot: TableSnapshot, context=None) -> List[Fix]:
        """使用LLM批量推断修复值"""
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
                    "constraint_type": v.constraint_type
                })
            
            table_context = self._build_table_context(snapshot, violations)
            
            system_prompt = """You are a data quality expert. Your task is to suggest corrected values for data quality violations.
Output in strict JSON format: {"fixes": [{"index": 0, "new_value": "corrected value"}, ...]}
Use null if the value cannot be determined."""
            
            user_prompt = f"""Please suggest corrections for the following value violations:

Table: {snapshot.table}
Violations to fix:
{json.dumps(violations_info, ensure_ascii=False, indent=2)}

Table Context (sample rows):
{table_context}

Requirements:
- For domain violations: map to the correct domain value
- For range violations: adjust to within the valid range
- For missing values: suggest a reasonable default or null
- Output ONLY JSON in format: {{"fixes": [{{"index": 0, "new_value": "..."}}]}}
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
                    self.logger.warning(f"建议修复值'{new_value}'看起来是提示文本，跳过")
                    continue
                
                fix_type = self._determine_fix_type(violation.constraint_type)
                fix_id = IdGenerator.generate_fix_id(
                    violation.table, violation.tuple_id, violation.attr,
                    fix_type, old_value
                )
                
                fix = Fix(
                    id=fix_id,
                    table=violation.table,
                    tuple_id=violation.tuple_id,
                    attr=violation.attr,
                    old=old_value if old_value is not None else "",
                    new=str(new_value),
                    fix_type=fix_type,
                    applied_by=self.mcp_id,
                    timestamp=""
                )
                fixes.append(fix)
            
        except Exception as e:
            self.logger.error(f"LLM批量修复失败: {e}")
            for violation in violations:
                try:
                    individual_fixes = self.fix(violation, snapshot, context)
                    fixes.extend(individual_fixes)
                except Exception as fix_error:
                    self.logger.error(f"修复违规 {violation.id} 失败: {fix_error}")
        
        return fixes
    
    def _build_table_context(self, snapshot: TableSnapshot, 
                            violations: List[Violation]) -> str:
        """构建表格上下文信息"""
        lines = []
        
        for i, row in enumerate(snapshot.rows[:5]):
            row_data = {attr: cell.value for attr, cell in row.cells.items()}
            lines.append(f"Row {i+1} (tuple_id={row.tuple_id}): {row_data}")
        
        if len(snapshot.rows) > 5:
            lines.append(f"... ({len(snapshot.rows)} rows total)")
        
        return "\n".join(lines)
    
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


class ValueMCP(BaseMCP):
    """值约束MCP"""
    
    def __init__(self):
        verifier = ValueVerifier()
        fixer = ValueFixer()
        super().__init__("ValueMCP.v1", verifier, fixer)
