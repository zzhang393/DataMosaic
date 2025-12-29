"""聚合约束MCP - 验证和修复聚合计算相关问题"""
import json
import re
import os
import logging
from typing import List, Dict, Any, Optional
from .base import MCPVerifier, MCPFixer, BaseMCP, get_table_definition, get_cell_value, is_valid_fix_value
from ...memory import TableSnapshot, Violation, Fix, SuggestedFix, ConstraintType, ViolationSeverity, FixType
from ...core.ids import IdGenerator

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../..'))
from llm.main import get_answer


class AggregationVerifier(MCPVerifier):
    """聚合约束验证器"""
    
    def __init__(self, config_path: str = None):
        super().__init__("AggregationVerifier.v1")
        self.enable_llm_validation = True
        
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), 
                'config', 
                'aggregation_rules.json'
            )
        
        self.config = self._load_config(config_path)
        self.aggregation_keywords = self.config.get('aggregation_keywords', {})
        self.calculation_types = self.config.get('calculation_types', {})
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """从JSON文件加载聚合规则配置"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"配置文件未找到: {config_path}，使用默认配置")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"配置文件解析失败: {e}，使用默认配置")
            return {}
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.AGGREGATION.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.AGGREGATION.value
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """验证表格中的聚合约束"""
        violations = []
        
        algo_violations = self._verify_algorithmic_aggregation(
            snapshot, schema, table_name, context
        )
        violations.extend(algo_violations)
        
        schema_rule_violations = self._verify_schema_aggregation_rules(
            snapshot, schema, table_name, context
        )
        violations.extend(schema_rule_violations)
        
        if self.enable_llm_validation:
            llm_violations = self._llm_validate_aggregation(
                snapshot, schema, table_name, context
            )
            violations.extend(llm_violations)
        
        return violations
    
    def verify_cell(self, table: str, tuple_id: str, attr: str, 
                   value: Any, attr_def: Dict[str, Any], 
                   snapshot: TableSnapshot, context=None) -> List[Violation]:
        """验证单个单元格的聚合约束"""
        violations = []
        
        if attr_def.get('calculated', False):
            calc_violations = self._verify_calculated_field(
                table, tuple_id, attr, value, attr_def, snapshot
            )
            violations.extend(calc_violations)
        
        return violations
    
    def _verify_algorithmic_aggregation(self, snapshot: TableSnapshot, 
                                       schema: Dict[str, Any], 
                                       table_name: str, context=None) -> List[Violation]:
        """验证算法可检测的聚合约束"""
        violations = []
        table_def = get_table_definition(schema, table_name)
        
        if not table_def:
            return violations
        
        attributes = table_def.get('attributes', [])
        
        for row in snapshot.rows:
            for attr_def in attributes:
                attr_name = attr_def['name']
                if attr_name in row.cells and attr_def.get('calculated', False):
                    value = row.cells[attr_name].value
                    
                    calc_violations = self._verify_calculated_field(
                        snapshot.table, row.tuple_id, attr_name, 
                        value, attr_def, snapshot
                    )
                    violations.extend(calc_violations)
        
        table_violations = self._verify_table_aggregation(
            snapshot, table_def, table_name
        )
        violations.extend(table_violations)
        
        return violations
    
    def _verify_calculated_field(self, table: str, tuple_id: str, attr: str, 
                                value: Any, attr_def: Dict[str, Any], 
                                snapshot: TableSnapshot) -> List[Violation]:
        """验证计算字段"""
        violations = []
        
        calculation = attr_def.get('calculation')
        if not calculation:
            return violations
        
        target_row = None
        for row in snapshot.rows:
            if row.tuple_id == tuple_id:
                target_row = row
                break
        
        if not target_row:
            return violations
        
        expected_value = self._calculate_expected_value(calculation, target_row)
        
        if expected_value is not None and value is not None:
            try:
                actual_value = float(str(value).replace(',', ''))
                expected_float = float(expected_value)
                
                tolerance = 0.01
                if abs(actual_value - expected_float) > tolerance:
                    violation_id = IdGenerator.generate_violation_id(
                        table, tuple_id, attr, ConstraintType.AGGREGATION.value
                    )
                    
                    violation = Violation(
                        id=violation_id,
                        table=table,
                        tuple_id=tuple_id,
                        attr=attr,
                        constraint_type=ConstraintType.AGGREGATION.value,
                        description=f"计算字段 {attr} 值 {value} 不正确，期望值为 {expected_value}",
                        severity=ViolationSeverity.ERROR.value,
                        
                        suggested_fix=SuggestedFix(value=str(expected_value), ),
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violations.append(violation)
                    
            except (ValueError, TypeError):
                pass
        
        return violations
    
    def _calculate_expected_value(self, calculation: Dict[str, Any], row) -> Optional[float]:
        """计算期望值"""
        calc_type = calculation.get('type')
        
        if calc_type == 'sum':
            fields = calculation.get('fields', [])
            total = 0
            for field in fields:
                if field in row.cells:
                    try:
                        value = row.cells[field].value
                        if value is not None:
                            total += float(str(value).replace(',', ''))
                    except (ValueError, TypeError):
                        return None
            return total
        
        elif calc_type == 'multiply':
            fields = calculation.get('fields', [])
            if len(fields) < 2:
                return None
            
            result = 1
            for field in fields:
                if field in row.cells:
                    try:
                        value = row.cells[field].value
                        if value is not None:
                            result *= float(str(value).replace(',', ''))
                        else:
                            return None
                    except (ValueError, TypeError):
                        return None
                else:
                    return None
            return result
        
        elif calc_type == 'percentage':
            numerator_field = calculation.get('numerator')
            denominator_field = calculation.get('denominator')
            
            if not numerator_field or not denominator_field:
                return None
            
            try:
                numerator = row.cells.get(numerator_field)
                denominator = row.cells.get(denominator_field)
                
                if numerator and denominator:
                    num_val = float(str(numerator.value).replace(',', ''))
                    den_val = float(str(denominator.value).replace(',', ''))
                    
                    if den_val != 0:
                        return (num_val / den_val) * 100
                    
            except (ValueError, TypeError, AttributeError):
                return None
        
        return None
    
    def _verify_table_aggregation(self, snapshot: TableSnapshot, 
                                 table_def: Dict[str, Any], 
                                 table_name: str) -> List[Violation]:
        """验证表级聚合约束"""
        violations = []
        
        total_violations = self._check_total_fields(snapshot, table_def, table_name)
        violations.extend(total_violations)
        
        avg_violations = self._check_average_fields(snapshot, table_def, table_name)
        violations.extend(avg_violations)
        
        return violations
    
    def _check_total_fields(self, snapshot: TableSnapshot, 
                           table_def: Dict[str, Any], 
                           table_name: str) -> List[Violation]:
        """检查总计字段"""
        violations = []
        
        total_keywords = self.aggregation_keywords.get('total', ['总计', 'total', '合计', 'sum'])
        
        total_rows = []
        for row in snapshot.rows:
            for attr, cell in row.cells.items():
                value = str(cell.value).lower()
                if any(keyword in value for keyword in total_keywords):
                    total_rows.append(row)
                    break
        
        if not total_rows:
            return violations
        
        for total_row in total_rows:
            for attr, cell in total_row.cells.items():
                try:
                    total_value = float(str(cell.value).replace(',', ''))
                    
                    calculated_total = 0
                    count = 0
                    
                    for row in snapshot.rows:
                        if row.tuple_id != total_row.tuple_id and attr in row.cells:
                            try:
                                value = row.cells[attr].value
                                if value is not None:
                                    num_value = float(str(value).replace(',', ''))
                                    calculated_total += num_value
                                    count += 1
                            except (ValueError, TypeError):
                                continue
                    
                    if count > 0 and abs(total_value - calculated_total) > 0.01:
                        violation_id = IdGenerator.generate_violation_id(
                            table_name, total_row.tuple_id, attr, ConstraintType.AGGREGATION.value
                        )
                        
                        violation = Violation(
                            id=violation_id,
                            table=table_name,
                            tuple_id=total_row.tuple_id,
                            attr=attr,
                            constraint_type=ConstraintType.AGGREGATION.value,
                            description=f"总计字段 {attr} 值 {total_value} 不正确，计算值为 {calculated_total}",
                            severity=ViolationSeverity.ERROR.value,
                            
                            suggested_fix=SuggestedFix(value=str(calculated_total), ),
                            detector_id=self.mcp_id,
                            timestamp=""
                        )
                        violations.append(violation)
                        
                except (ValueError, TypeError):
                    continue
        
        return violations
    
    def _check_average_fields(self, snapshot: TableSnapshot, 
                             table_def: Dict[str, Any], 
                             table_name: str) -> List[Violation]:
        """检查平均值字段"""
        violations = []
        
        avg_keywords = self.aggregation_keywords.get('average', ['平均', 'average', 'avg', '均值'])
        
        avg_rows = []
        for row in snapshot.rows:
            for attr, cell in row.cells.items():
                value = str(cell.value).lower()
                if any(keyword in value for keyword in avg_keywords):
                    avg_rows.append(row)
                    break
        
        if not avg_rows:
            return violations
        
        for avg_row in avg_rows:
            for attr, cell in avg_row.cells.items():
                try:
                    avg_value = float(str(cell.value).replace(',', ''))
                    
                    total = 0
                    count = 0
                    
                    for row in snapshot.rows:
                        if row.tuple_id != avg_row.tuple_id and attr in row.cells:
                            try:
                                value = row.cells[attr].value
                                if value is not None:
                                    num_value = float(str(value).replace(',', ''))
                                    total += num_value
                                    count += 1
                            except (ValueError, TypeError):
                                continue
                    
                    if count > 0:
                        calculated_avg = total / count
                        if abs(avg_value - calculated_avg) > 0.01:
                            violation_id = IdGenerator.generate_violation_id(
                                table_name, avg_row.tuple_id, attr, ConstraintType.AGGREGATION.value
                            )
                            
                            violation = Violation(
                                id=violation_id,
                                table=table_name,
                                tuple_id=avg_row.tuple_id,
                                attr=attr,
                                constraint_type=ConstraintType.AGGREGATION.value,
                                description=f"平均值字段 {attr} 值 {avg_value} 不正确，计算值为 {calculated_avg:.2f}",
                                severity=ViolationSeverity.ERROR.value,
                                
                                suggested_fix=SuggestedFix(value=f"{calculated_avg:.2f}", ),
                                detector_id=self.mcp_id,
                                timestamp=""
                            )
                            violations.append(violation)
                            
                except (ValueError, TypeError):
                    continue
        
        return violations
    
    def _verify_schema_aggregation_rules(self, snapshot: TableSnapshot,
                                         schema: Dict[str, Any],
                                         table_name: str, context=None) -> List[Violation]:
        """验证schema中定义的聚合规则"""
        violations = []
        
        rules = schema.get('rules', [])
        if not rules:
            return violations
        
        self.logger.info(f"📋 [AggregationVerifier] 检查schema中的聚合规则...")
        
        for rule in rules:
            if not rule.get('enabled', True):
                continue
            
            rule_type = rule.get('type', '')
            if rule_type != 'AGGREGATION':
                continue
            
            rule_id = rule.get('id', '')
            rule_name = rule.get('name', '')
            rule_tables = rule.get('tables', [])
            
            if table_name not in rule_tables:
                continue
            
            self.logger.debug(f"  🔍 验证聚合规则 {rule_id} ({rule_name})")
            
            rule_scope = rule.get('scope', 'single_table')
            
            if rule_scope == 'single_table':
                rule_violations = self._verify_single_table_aggregation_rule(
                    rule, snapshot, schema, table_name, context
                )
                violations.extend(rule_violations)
            elif rule_scope == 'multi_table':
                self.logger.debug(f"    ⏭️  规则 {rule_id} 需要多表验证，跳过单表验证阶段")
        
        if violations:
            self.logger.info(f"  ⚠️  发现 {len(violations)} 个聚合规则违规")
        
        return violations
    
    def _verify_single_table_aggregation_rule(self, rule: Dict[str, Any],
                                             snapshot: TableSnapshot,
                                             schema: Dict[str, Any],
                                             table_name: str, context=None) -> List[Violation]:
        """验证单表聚合规则（如agg_2: 课程容量限制）"""
        violations = []
        
        rule_id = rule.get('id', '')
        rule_description = rule.get('description', '')
        validation_logic = rule.get('validation_logic', {})
        severity = rule.get('severity', 'ERROR')
        
        aggregation_type = validation_logic.get('aggregation_type', '')
        group_by = validation_logic.get('group_by', '')
        max_value = validation_logic.get('max_value')
        
        if not group_by or max_value is None:
            self.logger.warning(f"规则 {rule_id} 缺少必要的验证参数")
            return violations
        
        group_counts = {}
        for row in snapshot.rows:
            if group_by in row.cells:
                group_key = row.cells[group_by].value
                if group_key:
                    if group_key not in group_counts:
                        group_counts[group_key] = []
                    group_counts[group_key].append(row.tuple_id)
        
        for group_key, tuple_ids in group_counts.items():
            if aggregation_type == 'count':
                actual_count = len(tuple_ids)
                if actual_count > max_value:
                    violation_id = IdGenerator.generate_violation_id(
                        table_name, 'AGGREGATION_' + str(group_key), group_by, ConstraintType.AGGREGATION.value
                    )
                    
                    violation = Violation(
                        id=violation_id,
                        table=table_name,
                        tuple_id='AGGREGATION_' + str(group_key),
                        attr=group_by,
                        constraint_type=ConstraintType.AGGREGATION.value,
                        description=f"[{rule_id}] {rule_description} - {group_by}={group_key} 超过限制: 实际{actual_count}，最大{max_value}",
                        severity=severity,
                        suggested_fix=None,
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violation.current_value = f"count={actual_count}"
                    violation.business_rule_id = rule_id
                    violation.affected_tuple_ids = tuple_ids  # 记录所有受影响的tuple_ids
                    violations.append(violation)
        
        return violations
    
    def verify_multi_table_aggregation_rules(self, all_snapshots: Dict[str, TableSnapshot],
                                            schema: Dict[str, Any],
                                            context=None) -> List[Violation]:
        """验证多表聚合规则（如agg_1: 学生选课学分限制）"""
        violations = []
        
        rules = schema.get('rules', [])
        if not rules:
            return violations
        
        self.logger.info(f"📋 [AggregationVerifier] 检查多表聚合规则...")
        
        for rule in rules:
            if not rule.get('enabled', True):
                continue
            
            rule_type = rule.get('type', '')
            if rule_type != 'AGGREGATION':
                continue
            
            rule_scope = rule.get('scope', 'single_table')
            if rule_scope != 'multi_table':
                continue
            
            rule_id = rule.get('id', '')
            rule_name = rule.get('name', '')
            
            self.logger.debug(f"  🔍 验证多表聚合规则 {rule_id} ({rule_name})")
            
            if rule_id == 'agg_1':
                rule_violations = self._verify_student_credit_limit(
                    rule, all_snapshots, schema, context
                )
                violations.extend(rule_violations)
        
        if violations:
            self.logger.info(f"  ⚠️  多表聚合规则发现 {len(violations)} 个违规")
        
        return violations
    
    def _verify_student_credit_limit(self, rule: Dict[str, Any],
                                    all_snapshots: Dict[str, TableSnapshot],
                                    schema: Dict[str, Any],
                                    context=None) -> List[Violation]:
        """验证agg_1: 学生选课学分限制"""
        violations = []
        
        rule_id = rule.get('id', '')
        rule_description = rule.get('description', '')
        severity = rule.get('severity', 'ERROR')
        validation_logic = rule.get('validation_logic', {})
        
        max_credits = validation_logic.get('max_value', 12)
        
        course_offering_snapshot = all_snapshots.get('CourseOffering')
        course_snapshot = all_snapshots.get('Course')
        
        if not course_offering_snapshot or not course_snapshot:
            self.logger.debug(f"  ℹ️  未找到CourseOffering或Course表，跳过agg_1验证")
            return violations
        
        course_credits = {}
        for row in course_snapshot.rows:
            course_id = row.cells.get('ID')
            credit = row.cells.get('Credit')
            if course_id and credit:
                course_credits[str(course_id.value)] = credit.value
        
        student_credits = {}
        student_courses = {}  # 记录每个学生选的课程，用于生成详细信息
        
        for row in course_offering_snapshot.rows:
            student_id = row.cells.get('StudentID')
            course_id = row.cells.get('CourseID')
            
            if student_id and course_id:
                student_id_str = str(student_id.value)
                course_id_str = str(course_id.value)
                
                if course_id_str in course_credits:
                    credit = course_credits[course_id_str]
                    try:
                        credit_value = float(str(credit).replace(',', ''))
                        
                        if student_id_str not in student_credits:
                            student_credits[student_id_str] = 0
                            student_courses[student_id_str] = []
                        
                        student_credits[student_id_str] += credit_value
                        student_courses[student_id_str].append((course_id_str, credit_value, row.tuple_id))
                    except (ValueError, TypeError):
                        continue
        
        for student_id, total_credits in student_credits.items():
            if total_credits > max_credits:
                violation_id = IdGenerator.generate_violation_id(
                    'CourseOffering', 'STUDENT_' + student_id, 'StudentID', ConstraintType.AGGREGATION.value
                )
                
                affected_tuple_ids = [tid for _, _, tid in student_courses[student_id]]
                
                violation = Violation(
                    id=violation_id,
                    table='CourseOffering',
                    tuple_id='STUDENT_' + student_id,
                    attr='StudentID',
                    constraint_type=ConstraintType.AGGREGATION.value,
                    description=f"[{rule_id}] {rule_description} - 学生 {student_id} 总学分超限: 实际{total_credits}分，最大{max_credits}分",
                    severity=severity,
                    suggested_fix=None,
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violation.current_value = f"total_credits={total_credits}"
                violation.business_rule_id = rule_id
                violation.affected_tuple_ids = affected_tuple_ids
                violations.append(violation)
        
        return violations
    
    def _llm_validate_aggregation(self, snapshot: TableSnapshot, 
                                 schema: Dict[str, Any], 
                                 table_name: str, context=None) -> List[Violation]:
        """使用LLM验证复杂的聚合逻辑"""
        violations = []
        
        try:
            aggregation_context = self._build_aggregation_context(
                snapshot, schema, table_name
            )
            
            system_prompt = """你是一个专业的数据聚合验证专家，擅长发现聚合计算中的错误。"""
            
            prompt = f"""
请分析以下表格数据中的聚合计算问题。

{aggregation_context}

验证重点：
1. 总计行的计算是否正确
2. 平均值的计算是否准确
3. 百分比计算是否合理
4. 汇总数据的逻辑一致性
5. 分组聚合的正确性
6. 计算字段的公式验证

请返回发现的问题，格式如下：
{{
  "violations": [
    {{
      "tuple_id": "行ID",
      "attr": "字段名",
      "problem_type": "aggregation",
      "description": "问题描述",
      "severity": "error/warn/info",
      "suggested_fix": "具体的修复值（如果有）"
    }}
  ]
}}

⚠️ 重要提示：
- suggested_fix字段必须是一个具体的数据值（如数字、日期等），而不是描述性文本
- 如果无法确定具体的修复值，请将suggested_fix设置为null或空字符串
- 不要在suggested_fix中填写"请检查"、"建议修改"等提示性文字
- suggested_fix应该是可以直接替换原值的数据

示例：
✅ 正确："suggested_fix": "12345.67"
✅ 正确："suggested_fix": "2024-03-15"
✅ 正确："suggested_fix": null
❌ 错误："suggested_fix": "确认数据是否应去重，或是否有其他特定需求使得多次记录是合理的。"
❌ 错误："suggested_fix": "建议修改为正确格式"

如果没有发现问题，返回空数组。
"""
            
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            llm_response = get_answer(prompt, system_prompt=system_prompt, model=model)
            
            llm_violations = self._parse_llm_aggregation_response(llm_response, table_name)
            violations.extend(llm_violations)
            
        except Exception as e:
            self.logger.error(f"LLM聚合验证失败: {e}")
        
        return violations
    
    def _build_aggregation_context(self, snapshot: TableSnapshot, 
                                  schema: Dict[str, Any], 
                                  table_name: str) -> str:
        """构建聚合验证上下文"""
        context_parts = []
        
        context_parts.append(f"=== 表格信息 ===")
        context_parts.append(f"表名: {table_name}")
        context_parts.append(f"总行数: {len(snapshot.rows)}")
        
        table_def = get_table_definition(schema, table_name)
        if table_def:
            attributes = table_def.get('attributes', [])
            context_parts.append(f"\n字段定义:")
            for attr in attributes:
                field_info = f"- {attr['name']}"
                if 'type' in attr:
                    field_info += f" ({attr['type']})"
                if attr.get('calculated'):
                    field_info += " [计算字段]"
                if 'description' in attr:
                    field_info += f": {attr['description']}"
                context_parts.append(field_info)
        
        context_parts.append(f"\n=== 数据内容 ===")
        for i, row in enumerate(snapshot.rows):
            row_data = []
            for attr, cell in row.cells.items():
                value = cell.value
                row_data.append(f"{attr}: {value}")
            
            context_parts.append(f"第{i+1}行 [{row.tuple_id}]: {', '.join(row_data)}")
        
        return "\n".join(context_parts)
    
    def _parse_llm_aggregation_response(self, llm_response: str, table_name: str) -> List[Violation]:
        """解析LLM聚合验证响应"""
        violations = []
        
        try:
            if not llm_response or not llm_response.strip():
                return violations
            
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', llm_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_str = llm_response.strip()
            
            if not json_str:
                return violations
            
            data = json.loads(json_str)
            llm_violations = data.get('violations', [])
            
            for viol_data in llm_violations:
                violation_id = IdGenerator.generate_violation_id(
                    table_name, 
                    viol_data.get('tuple_id', ''),
                    viol_data.get('attr', ''),
                    ConstraintType.AGGREGATION.value
                )
                
                suggested_fix = None
                if viol_data.get('suggested_fix'):
                    suggested_fix = SuggestedFix(
                        value=viol_data['suggested_fix'],

                    )
                
                violation = Violation(
                    id=violation_id,
                    table=table_name,
                    tuple_id=viol_data.get('tuple_id', ''),
                    attr=viol_data.get('attr', ''),
                    constraint_type=ConstraintType.AGGREGATION.value,
                    description=f"[聚合验证] {viol_data.get('description', '聚合计算问题')}",
                    severity=viol_data.get('severity', 'warn'),

                    suggested_fix=suggested_fix,
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
                
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            self.logger.error(f"解析LLM聚合验证响应失败: {e}")
        
        return violations


class AggregationFixer(MCPFixer):
    """聚合约束修复器"""
    
    def __init__(self, config_path: str = None):
        super().__init__("AggregationFixer.v1")
        
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), 
                'config', 
                'aggregation_rules.json'
            )
        
        self.config = self._load_config(config_path)
        self.field_fix_rules = self.config.get('field_fix_rules', {})
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """从JSON文件加载聚合规则配置"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"配置文件未找到: {config_path}，使用默认配置")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"配置文件解析失败: {e}，使用默认配置")
            return {}
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.AGGREGATION.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.AGGREGATION.value
    
    def can_fix(self, violation: Violation) -> bool:
        return violation.constraint_type == ConstraintType.AGGREGATION.value
    
    def get_supported_fix_types(self) -> List[str]:
        return [
            FixType.CALCULATION_FIX.value,
            FixType.AGGREGATION_FIX.value
        ]
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, 
            context=None) -> List[Fix]:
        """修复聚合约束违规"""
        if not self.can_fix(violation):
            return []
        
        fixes = []
        old_value = get_cell_value(violation, snapshot)
        
        if not old_value:
            return fixes
        
        new_value = None
        
        if violation.suggested_fix and violation.suggested_fix.value:
            suggested_value = violation.suggested_fix.value
            if is_valid_fix_value(suggested_value):
                new_value = suggested_value
            else:
                self.logger.warning(f"建议修复值'{suggested_value}'看起来是提示文本而非数据值，忽略并尝试自动生成修复")
                new_value = self._generate_aggregation_fix(violation, old_value, snapshot)
        else:
            new_value = self._generate_aggregation_fix(violation, old_value, snapshot)
        
        if new_value and new_value != old_value:
            fix_id = IdGenerator.generate_fix_id(
                violation.table, violation.tuple_id, violation.attr,
                FixType.AGGREGATION_FIX.value, old_value
            )
            
            fix = Fix(
                id=fix_id,
                table=violation.table,
                tuple_id=violation.tuple_id,
                attr=violation.attr,
                old=old_value,
                new=new_value,
                fix_type=FixType.AGGREGATION_FIX.value,
                applied_by=self.mcp_id,
                timestamp=""
            )
            fixes.append(fix)
            self.logger.info(f"聚合修复: {old_value} -> {new_value}")
        
        return fixes
    
    def _generate_aggregation_fix(self, violation: Violation, old_value: str, 
                                 snapshot: TableSnapshot) -> Optional[str]:
        """自动生成聚合问题的修复"""
        try:
            attr = violation.attr
            
            if attr not in self.field_fix_rules:
                return None
            
            field_rule = self.field_fix_rules[attr]
            fix_type = field_rule.get('type')
            
            if fix_type == 'amount_unit':
                return self._fix_amount_unit_consistency(old_value, snapshot, attr, field_rule)
            elif fix_type == 'quarter_format':
                return self._fix_quarter_format_consistency(old_value, snapshot, attr, field_rule)
            
            return None
            
        except Exception as e:
            self.logger.error(f"生成聚合修复失败: {e}")
            return None
    
    def _fix_amount_unit_consistency(self, old_value: str, snapshot: TableSnapshot, 
                                   attr: str, field_rule: Dict[str, Any]) -> Optional[str]:
        """修复税额单位一致性问题"""
        target_format = field_rule.get('target_format', '亿元')
        conversion_rules = field_rule.get('conversion_rules', [])
        
        for rule in conversion_rules:
            pattern = rule['pattern']
            action = rule['action']
            
            match = re.match(pattern, old_value)
            if match:
                number = match.group(1)
                
                if action == 'remove_space_add_yuan':
                    return f"{number}亿元"
                elif action == 'replace_renminbi_with_yuan':
                    return f"{number}亿元"
                elif action == 'add_yuan':
                    return f"{number}亿元"
                elif action == 'add_yi_yuan':
                    return f"{number}亿元"
        
        return None
    
    def _fix_quarter_format_consistency(self, old_value: str, snapshot: TableSnapshot, 
                                      attr: str, field_rule: Dict[str, Any]) -> Optional[str]:
        """修复季度格式一致性问题"""
        mapping = field_rule.get('mapping', {})
        standard_values = field_rule.get('standard_values', [])
        
        if old_value.lower() in mapping:
            return mapping[old_value.lower()]
        
        for quarter in standard_values:
            if quarter in old_value:
                return quarter
        
        return None


class AggregationMCP(BaseMCP):
    """聚合约束MCP"""
    
    def __init__(self):
        verifier = AggregationVerifier()
        fixer = AggregationFixer()
        super().__init__("AggregationMCP.v1", verifier, fixer)
