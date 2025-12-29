"""逻辑约束MCP - 使用LLM验证和修复业务逻辑相关问题"""
import json
import re
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from .base import MCPVerifier, MCPFixer, BaseMCP, get_table_definition, get_cell_value
from ...memory import TableSnapshot, Violation, Fix, SuggestedFix, ConstraintType, ViolationSeverity, FixType
from ...core.ids import IdGenerator

from baml_src.client_selector import load_env_from_llm_folder, get_client_name_for_model, get_baml_options
load_env_from_llm_folder()

from baml_client.sync_client import b as baml_client
from baml_client.types import LogicViolation, LogicFix


class BusinessLogicRulesLoader:
    """业务逻辑规则加载器 - 从JSON配置文件加载规则"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化规则加载器
        
        Args:
            config_path: 配置文件路径，默认为 config/business_logic_rules.json
        """
        self.logger = __import__('logging').getLogger('doc2db.logic_rules')
        
        if config_path is None:
            project_root = Path(__file__).parent.parent.parent.parent
            config_path = project_root / "config" / "business_logic_rules.json"
        
        self.config_path = Path(config_path)
        self.rules = []
        self.default_prompt = "通用业务表格，需要验证数据的逻辑一致性"
        
        self._load_rules()
    
    def _load_rules(self):
        """加载业务逻辑规则"""
        try:
            if not self.config_path.exists():
                self.logger.warning(f"业务逻辑规则配置文件不存在: {self.config_path}")
                return
            
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self.rules = config.get('rules', [])
            self.default_prompt = config.get('default_prompt', self.default_prompt)
            
        except Exception as e:
            self.logger.error(f"加载业务逻辑规则失败: {e}")
    
    def get_applicable_prompts(self, table_name: str, field_names: List[str]) -> List[str]:
        """
        获取适用的验证提示
        
        Args:
            table_name: 表名
            field_names: 字段名列表
            
        Returns:
            适用的验证提示列表
        """
        applicable_prompts = []
        
        table_name_lower = table_name.lower()
        field_names_lower = [f.lower() for f in field_names]
        
        for rule in self.rules:
            if not rule.get('enabled', True):
                continue
            
            triggers = rule.get('triggers', {})
            table_keywords = triggers.get('table_name_keywords', [])
            field_keywords = triggers.get('field_name_keywords', [])
            
            table_matched = any(
                keyword.lower() in table_name_lower 
                for keyword in table_keywords
            )
            
            field_matched = any(
                any(keyword.lower() in field_name for keyword in field_keywords)
                for field_name in field_names_lower
            )
            
            if table_matched or field_matched:
                prompts = rule.get('validation_prompts', [])
                applicable_prompts.extend(prompts)
                self.logger.debug(f"规则 '{rule['name']}' 被触发 (表名匹配: {table_matched}, 字段匹配: {field_matched})")
        
        return applicable_prompts


class LogicVerifier(MCPVerifier):
    """逻辑约束验证器 - 使用LLM进行业务逻辑验证"""
    
    def __init__(self):
        super().__init__("LogicVerifier.v1")
        self.enable_llm_validation = True
        
        self.rules_loader = BusinessLogicRulesLoader()
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.LOGIC.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.LOGIC.value
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """验证表格中的逻辑约束（表级+单元格级别）"""
        violations = []
        
        if not self.enable_llm_validation:
            return violations
        
        table_def = get_table_definition(schema, table_name)
        if table_def:
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
        
        schema_rule_violations = self._verify_schema_business_rules(
            snapshot, schema, table_name, context
        )
        violations.extend(schema_rule_violations)
        
        llm_violations = self._llm_validate_business_logic(
            snapshot, schema, table_name, context
        )
        violations.extend(llm_violations)
        
        return violations
    
    def verify_cell(self, table: str, tuple_id: str, attr: str, 
                   value: Any, attr_def: Dict[str, Any], 
                   snapshot: TableSnapshot, context=None) -> List[Violation]:
        """验证单个单元格的逻辑约束（基于规则的快速检查）"""
        violations = []
        
        business_rules = attr_def.get('business_rules', [])
        for rule in business_rules:
            if not self._check_business_rule(value, rule):
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.LOGIC.value
                )
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.LOGIC.value,
                    description=f"[业务规则] 字段 {attr} 违反业务规则: {rule.get('description', '未知规则')}",
                    severity=ViolationSeverity.WARN.value,
                    suggested_fix=None,
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations
    
    def _check_business_rule(self, value: Any, rule: Dict[str, Any]) -> bool:
        """检查业务规则"""
        rule_type = rule.get('type', '')
        
        if rule_type == 'pattern':
            pattern = rule.get('pattern', '')
            return re.match(pattern, str(value)) is not None
        
        elif rule_type == 'range':
            try:
                num_value = float(str(value).replace(',', ''))
                min_val = rule.get('min')
                max_val = rule.get('max')
                if min_val is not None and num_value < min_val:
                    return False
                if max_val is not None and num_value > max_val:
                    return False
            except ValueError:
                return False
        
        return True
    
    def _verify_schema_business_rules(self, snapshot: TableSnapshot,
                                      schema: Dict[str, Any],
                                      table_name: str, context=None) -> List[Violation]:
        """验证schema中定义的业务规则"""
        violations = []
        
        business_rules = schema.get('rules', [])
        if not business_rules:
            return violations
        
        self.logger.info(f"📋 [LogicVerifier] 检查schema中的 {len(business_rules)} 个业务规则...")
        
        for rule in business_rules:
            if not rule.get('enabled', True):
                continue
            
            rule_id = rule.get('id', '')
            rule_name = rule.get('name', '')
            rule_scope = rule.get('scope', 'single_table')
            rule_tables = rule.get('tables', [])
            
            if table_name not in rule_tables:
                continue
            
            self.logger.debug(f"  🔍 验证规则 {rule_id} ({rule_name}) - 范围: {rule_scope}")
            
            if rule_scope == 'single_table':
                rule_violations = self._verify_single_table_rule(
                    rule, snapshot, schema, table_name, context
                )
                violations.extend(rule_violations)
            elif rule_scope == 'multi_table':
                self.logger.debug(f"    ⏭️  规则 {rule_id} 需要多表验证，跳过单表验证阶段")
        
        if violations:
            self.logger.info(f"  ⚠️  发现 {len(violations)} 个业务规则违规")
        
        return violations
    
    def _verify_single_table_rule(self, rule: Dict[str, Any],
                                  snapshot: TableSnapshot,
                                  schema: Dict[str, Any],
                                  table_name: str, context=None) -> List[Violation]:
        """验证单表业务规则"""
        violations = []
        
        rule_id = rule.get('id', '')
        rule_name = rule.get('name', '')
        rule_description = rule.get('description', '')
        validation_logic = rule.get('validation_logic', {})
        
        check_method = validation_logic.get('check_method', '')
        
        if check_method == 'formula':
            violations = self._verify_formula_rule(
                rule, snapshot, schema, table_name, context
            )
        elif check_method == 'llm_validation':
            violations = self._verify_llm_rule(
                rule, snapshot, schema, table_name, context
            )
        else:
            self.logger.warning(f"未知的验证方法: {check_method}")
        
        return violations
    
    def _verify_formula_rule(self, rule: Dict[str, Any],
                            snapshot: TableSnapshot,
                            schema: Dict[str, Any],
                            table_name: str, context=None) -> List[Violation]:
        """验证基于公式的业务规则（如phi_2: cash_sanity_bound）"""
        violations = []
        
        rule_id = rule.get('id', '')
        rule_name = rule.get('name', '')
        rule_description = rule.get('description', '')
        validation_logic = rule.get('validation_logic', {})
        severity = rule.get('severity', 'WARN')
        
        formula = validation_logic.get('formula', '')
        required_fields = validation_logic.get('fields', [])
        
        if not formula or not required_fields:
            self.logger.warning(f"规则 {rule_id} 缺少公式或字段定义")
            return violations
        
        for row in snapshot.rows:
            field_values = {}
            missing_fields = []
            
            for field in required_fields:
                if field in row.cells:
                    value = row.cells[field].value
                    if value is not None:
                        try:
                            field_values[field] = float(str(value).replace(',', ''))
                        except (ValueError, TypeError):
                            field_values[field] = value
                    else:
                        missing_fields.append(field)
                else:
                    missing_fields.append(field)
            
            if missing_fields:
                continue
            
            try:
                if rule_id == 'phi_2':
                    ending_cash = None
                    beginning_cash = None
                    
                    for field_name, value in row.cells.items():
                        field_lower = field_name.lower()
                        if 'ending' in field_lower or '期末' in field_name or 'end' in field_lower:
                            if 'cash' in field_lower or '现金' in field_name:
                                ending_cash = value.value
                        elif 'beginning' in field_lower or '期初' in field_name or 'begin' in field_lower:
                            if 'cash' in field_lower or '现金' in field_name:
                                beginning_cash = value.value
                    
                    if ending_cash is not None and beginning_cash is not None:
                        try:
                            ending = float(str(ending_cash).replace(',', ''))
                            beginning = float(str(beginning_cash).replace(',', ''))
                            
                            if beginning != 0 and abs(ending / beginning) >= 10:
                                violation_id = IdGenerator.generate_violation_id(
                                    table_name, row.tuple_id, 'cash_flow', ConstraintType.LOGIC.value
                                )
                                
                                violation = Violation(
                                    id=violation_id,
                                    table=table_name,
                                    tuple_id=row.tuple_id,
                                    attr='cash_flow',
                                    constraint_type=ConstraintType.LOGIC.value,
                                    description=f"[{rule_id}] {rule_description} - 期末现金/期初现金比率过大: {ending/beginning:.2f}",
                                    severity=severity,
                                    suggested_fix=None,
                                    detector_id=self.mcp_id,
                                    timestamp=""
                                )
                                violation.current_value = f"ratio={ending/beginning:.2f}"
                                violation.business_rule_id = rule_id
                                violations.append(violation)
                        except (ValueError, TypeError, ZeroDivisionError):
                            pass
            
            except Exception as e:
                self.logger.error(f"评估公式规则 {rule_id} 失败: {e}")
        
        return violations
    
    def _verify_llm_rule(self, rule: Dict[str, Any],
                        snapshot: TableSnapshot,
                        schema: Dict[str, Any],
                        table_name: str, context=None) -> List[Violation]:
        """使用LLM验证复杂业务规则"""
        violations = []
        
        rule_id = rule.get('id', '')
        rule_description = rule.get('description', '')
        validation_logic = rule.get('validation_logic', {})
        severity = rule.get('severity', 'WARN')
        
        try:
            field_definitions = self._build_field_definitions(schema, table_name)
            data_sample = self._build_data_sample(snapshot)
            
            rule_context = f"""
业务规则验证:
- 规则ID: {rule_id}
- 规则描述: {rule_description}
- 约束条件: {validation_logic.get('constraint', '')}

请验证数据是否满足此业务规则。
"""
            
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            baml_options = get_baml_options(model)
            
            self.logger.info(f"🤖 [LogicVerifier] 使用LLM验证规则 {rule_id}...")
            baml_violations = baml_client.ValidateBusinessLogic(
                table_name=table_name,
                field_definitions=field_definitions,
                data_sample=data_sample,
                business_context=rule_context,
                baml_options=baml_options
            )
            
            llm_violations = self._convert_baml_violations(baml_violations, table_name)
            for violation in llm_violations:
                violation.business_rule_id = rule_id
            
            violations.extend(llm_violations)
            
        except Exception as e:
            self.logger.error(f"LLM验证规则 {rule_id} 失败: {e}")
        
        return violations
    
    def verify_multi_table_business_rules(self, all_snapshots: Dict[str, TableSnapshot],
                                         schema: Dict[str, Any],
                                         context=None) -> List[Violation]:
        """验证多表业务规则（如phi_1互投禁止、phi_3递归投资）"""
        violations = []
        
        business_rules = schema.get('rules', [])
        if not business_rules:
            return violations
        
        self.logger.info(f"📋 [LogicVerifier] 检查多表业务规则...")
        
        for rule in business_rules:
            if not rule.get('enabled', True):
                continue
            
            rule_id = rule.get('id', '')
            rule_name = rule.get('name', '')
            rule_scope = rule.get('scope', 'single_table')
            
            if rule_scope != 'multi_table':
                continue
            
            self.logger.debug(f"  🔍 验证多表规则 {rule_id} ({rule_name})")
            
            if rule_id == 'phi_1':
                rule_violations = self._verify_no_mutual_investment(
                    rule, all_snapshots, schema, context
                )
                violations.extend(rule_violations)
            
            elif rule_id == 'phi_3':
                rule_violations = self._verify_recursive_investment(
                    rule, all_snapshots, schema, context
                )
                violations.extend(rule_violations)
        
        if violations:
            self.logger.info(f"  ⚠️  多表规则发现 {len(violations)} 个违规")
        
        return violations
    
    def _verify_no_mutual_investment(self, rule: Dict[str, Any],
                                     all_snapshots: Dict[str, TableSnapshot],
                                     schema: Dict[str, Any],
                                     context=None) -> List[Violation]:
        """验证phi_1: 互投禁止规则"""
        violations = []
        
        rule_id = rule.get('id', '')
        rule_description = rule.get('description', '')
        severity = rule.get('severity', 'ERROR')
        
        investment_table_names = ['company_report', '公司_报告']
        investment_snapshot = None
        investment_table_name = None
        
        for tname in investment_table_names:
            if tname in all_snapshots:
                investment_snapshot = all_snapshots[tname]
                investment_table_name = tname
                break
        
        if not investment_snapshot:
            self.logger.debug(f"  ℹ️  未找到投资关系表，跳过phi_1验证")
            return violations
        
        investments = {}
        for row in investment_snapshot.rows:
            cells = row.cells
            
            investor = None
            investee = None
            
            for field_name in cells.keys():
                if 'company' in field_name.lower() or '公司' in field_name:
                    if investor is None:
                        investor = cells[field_name].value
                    elif investee is None:
                        investee = cells[field_name].value
            
            if investor and investee:
                if investor not in investments:
                    investments[investor] = set()
                investments[investor].add(investee)
        
        checked_pairs = set()
        for investor, investees in investments.items():
            for investee in investees:
                pair = tuple(sorted([investor, investee]))
                if pair in checked_pairs:
                    continue
                checked_pairs.add(pair)
                
                if investee in investments and investor in investments[investee]:
                    violation_id = IdGenerator.generate_violation_id(
                        investment_table_name, 'MULTI_TABLE', 'mutual_investment', ConstraintType.LOGIC.value
                    )
                    
                    violation = Violation(
                        id=violation_id,
                        table=investment_table_name,
                        tuple_id='MULTI_TABLE',
                        attr='mutual_investment',
                        constraint_type=ConstraintType.LOGIC.value,
                        description=f"[{rule_id}] {rule_description} - 发现互投关系: {investor} ⇄ {investee}",
                        severity=severity,
                        suggested_fix=None,
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violation.current_value = f"{investor} ⇄ {investee}"
                    violation.business_rule_id = rule_id
                    violations.append(violation)
        
        return violations
    
    def _verify_recursive_investment(self, rule: Dict[str, Any],
                                    all_snapshots: Dict[str, TableSnapshot],
                                    schema: Dict[str, Any],
                                    context=None) -> List[Violation]:
        """验证phi_3: 递归投资规则（传递闭包）"""
        violations = []
        
        rule_id = rule.get('id', '')
        rule_description = rule.get('description', '')
        severity = rule.get('severity', 'WARN')
        
        investment_table_names = ['company_report', '公司_报告']
        investment_snapshot = None
        investment_table_name = None
        
        for tname in investment_table_names:
            if tname in all_snapshots:
                investment_snapshot = all_snapshots[tname]
                investment_table_name = tname
                break
        
        if not investment_snapshot:
            return violations
        
        investments = {}
        for row in investment_snapshot.rows:
            cells = row.cells
            
            investor = None
            investee = None
            
            for field_name in cells.keys():
                if 'company' in field_name.lower() or '公司' in field_name:
                    if investor is None:
                        investor = cells[field_name].value
                    elif investee is None:
                        investee = cells[field_name].value
            
            if investor and investee:
                if investor not in investments:
                    investments[investor] = set()
                investments[investor].add(investee)
        
        all_companies = set(investments.keys())
        for investee_set in investments.values():
            all_companies.update(investee_set)
        
        for a in all_companies:
            if a not in investments:
                continue
            
            for b in investments[a]:
                if b not in investments:
                    continue
                
                for c in investments[b]:
                    if c != a and (a not in investments or c not in investments[a]):
                        violation_id = IdGenerator.generate_violation_id(
                            investment_table_name, 'MULTI_TABLE', 'transitive_investment', ConstraintType.LOGIC.value
                        )
                        
                        violation = Violation(
                            id=violation_id,
                            table=investment_table_name,
                            tuple_id='MULTI_TABLE',
                            attr='transitive_investment',
                            constraint_type=ConstraintType.LOGIC.value,
                            description=f"[{rule_id}] {rule_description} - 缺少传递投资关系: {a}→{b}→{c}，但未记录{a}→{c}",
                            severity=severity,
                            suggested_fix=SuggestedFix(value=f"添加投资关系: {a} → {c}"),
                            detector_id=self.mcp_id,
                            timestamp=""
                        )
                        violation.current_value = f"{a}→{b}→{c}"
                        violation.business_rule_id = rule_id
                        violations.append(violation)
        
        return violations
    
    def _llm_validate_business_logic(self, snapshot: TableSnapshot, 
                                    schema: Dict[str, Any], 
                                    table_name: str, context=None) -> List[Violation]:
        """使用LLM进行业务逻辑验证"""
        violations = []
        
        try:
            field_definitions = self._build_field_definitions(schema, table_name)
            data_sample = self._build_data_sample(snapshot)
            business_context = self._infer_business_context(table_name, snapshot)
            
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            baml_options = get_baml_options(model)
            
            self.logger.info(f"🤖 [LogicVerifier] 正在调用LLM验证业务逻辑（model={model}）...")
            baml_violations = baml_client.ValidateBusinessLogic(
                table_name=table_name,
                field_definitions=field_definitions,
                data_sample=data_sample,
                business_context=business_context,
                baml_options=baml_options  # ✅ 传递客户端配置
            )
            
            llm_violations = self._convert_baml_violations(baml_violations, table_name)
            violations.extend(llm_violations)
            
        except Exception as e:
            self.logger.error(f"LLM业务逻辑验证失败: {e}")
        
        return violations
    
    def _build_field_definitions(self, schema: Dict[str, Any], table_name: str) -> str:
        """构建字段定义字符串"""
        table_def = get_table_definition(schema, table_name)
        if not table_def:
            return ""
        
        attributes = table_def.get('attributes', [])
        field_lines = []
        
        for attr in attributes:
            field_info = f"- {attr['name']}"
            if 'type' in attr:
                field_info += f" ({attr['type']})"
            if 'description' in attr:
                field_info += f": {attr['description']}"
            
            constraints = []
            if attr.get('required'):
                constraints.append("必填")
            if 'domain' in attr:
                constraints.append(f"允许值: {attr['domain']}")
            if 'min' in attr or 'max' in attr:
                range_info = []
                if 'min' in attr:
                    range_info.append(f"最小: {attr['min']}")
                if 'max' in attr:
                    range_info.append(f"最大: {attr['max']}")
                constraints.append(f"范围: {', '.join(range_info)}")
            
            if constraints:
                field_info += f" [约束: {' | '.join(constraints)}]"
            
            field_lines.append(field_info)
        
        return "\n".join(field_lines)
    
    def _build_data_sample(self, snapshot: TableSnapshot) -> str:
        """构建数据样本字符串"""
        sample_lines = [f"总行数: {len(snapshot.rows)}"]
        
        max_rows = min(10, len(snapshot.rows))
        for i, row in enumerate(snapshot.rows[:max_rows]):
            row_data = []
            for attr, cell in row.cells.items():
                value = cell.value
                row_data.append(f"{attr}: {value}")
            
            sample_lines.append(f"第{i+1}行 [{row.tuple_id}]: {', '.join(row_data)}")
        
        if len(snapshot.rows) > max_rows:
            sample_lines.append(f"... 还有 {len(snapshot.rows) - max_rows} 行数据")
        
        return "\n".join(sample_lines)
    
    def _convert_baml_violations(self, baml_violations: List[LogicViolation], 
                                 table_name: str) -> List[Violation]:
        """将 BAML 违规转换为系统 Violation 对象"""
        violations = []
        
        for baml_viol in baml_violations:
            try:
                tuple_id = baml_viol.tuple_id.strip('[]')
                
                violation_id = IdGenerator.generate_violation_id(
                    table_name, 
                    tuple_id,
                    baml_viol.attr,
                    ConstraintType.LOGIC.value
                )
                
                suggested_fix = None
                if baml_viol.suggested_fix:
                    suggested_fix = SuggestedFix(
                        value=baml_viol.suggested_fix
                    )
                
                violation = Violation(
                    id=violation_id,
                    table=table_name,
                    tuple_id=tuple_id,
                    attr=baml_viol.attr,
                    constraint_type=ConstraintType.LOGIC.value,
                    description=f"[业务逻辑] {baml_viol.description}",
                    severity=baml_viol.severity,
                    suggested_fix=suggested_fix,
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
                
            except Exception as ve:
                self.logger.error(f"转换违规项时出错: {ve}")
                continue
        
        return violations
    
    def _build_logic_validation_context(self, snapshot: TableSnapshot, 
                                       schema: Dict[str, Any], 
                                       table_name: str) -> str:
        """构建业务逻辑验证上下文"""
        context_parts = []
        
        context_parts.append(f"=== 表格信息 ===")
        context_parts.append(f"表名: {table_name}")
        
        table_def = get_table_definition(schema, table_name)
        if table_def:
            attributes = table_def.get('attributes', [])
            context_parts.append(f"\n字段定义:")
            for attr in attributes:
                field_info = f"- {attr['name']}"
                if 'type' in attr:
                    field_info += f" ({attr['type']})"
                if 'description' in attr:
                    field_info += f": {attr['description']}"
                
                constraints = []
                if attr.get('required'):
                    constraints.append("必填")
                if 'domain' in attr:
                    constraints.append(f"允许值: {attr['domain']}")
                if 'min' in attr or 'max' in attr:
                    range_info = []
                    if 'min' in attr:
                        range_info.append(f"最小: {attr['min']}")
                    if 'max' in attr:
                        range_info.append(f"最大: {attr['max']}")
                    constraints.append(f"范围: {', '.join(range_info)}")
                
                if constraints:
                    field_info += f" [约束: {' | '.join(constraints)}]"
                
                context_parts.append(field_info)
        
        context_parts.append(f"\n=== 数据内容 ===")
        context_parts.append(f"总行数: {len(snapshot.rows)}")
        
        max_rows = min(10, len(snapshot.rows))
        for i, row in enumerate(snapshot.rows[:max_rows]):
            row_data = []
            for attr, cell in row.cells.items():
                value = cell.value
                row_data.append(f"{attr}: {value}")
            
            context_parts.append(f"第{i+1}行 [{row.tuple_id}]: {', '.join(row_data)}")
        
        if len(snapshot.rows) > max_rows:
            context_parts.append(f"... 还有 {len(snapshot.rows) - max_rows} 行数据")
        
        context_parts.append(f"\n=== 业务场景分析 ===")
        business_context = self._infer_business_context(table_name, snapshot)
        context_parts.append(business_context)
        
        return "\n".join(context_parts)
    
    def _infer_business_context(self, table_name: str, snapshot: TableSnapshot) -> str:
        """推断业务场景上下文 - 使用配置文件中的规则"""
        field_names = []
        if len(snapshot.rows) > 0:
            field_names = list(snapshot.rows[0].cells.keys())
        
        applicable_prompts = self.rules_loader.get_applicable_prompts(table_name, field_names)
        
        if not applicable_prompts:
            applicable_prompts = [self.rules_loader.default_prompt]
        
        return "\n".join([f"- {prompt}" for prompt in applicable_prompts])
    
    def _parse_llm_logic_response(self, llm_response: str, table_name: str) -> List[Violation]:
        """解析LLM业务逻辑验证响应"""
        violations = []
        
        try:
            if not llm_response or not llm_response.strip():
                self.logger.warning("LLM返回空响应")
                return violations
            
            json_str = None
            
            json_match = re.search(r'```json\s*(\{.*?\}|\[.*?\])\s*```', llm_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r'```\s*(\{.*?\}|\[.*?\])\s*```', llm_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    json_match = re.search(r'\{.*?"violations".*?\}', llm_response, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                    else:
                        json_match = re.search(r'\[\s*\]', llm_response)
                        if json_match:
                            json_str = json_match.group(0)
                        else:
                            json_str = llm_response.strip()
            
            if not json_str or not json_str.strip():
                self.logger.warning("无法提取有效的JSON内容")
                return violations
            
            data = json.loads(json_str)
            
            if isinstance(data, dict):
                llm_violations = data.get('violations', [])
            elif isinstance(data, list):
                llm_violations = data
            else:
                self.logger.warning(f"JSON解析结果格式不支持: {type(data)}")
                return violations
            
            if not isinstance(llm_violations, list):
                self.logger.warning(f"violations数据不是列表类型: {type(llm_violations)}")
                return violations
            
            for i, viol_data in enumerate(llm_violations):
                try:
                    if not isinstance(viol_data, dict):
                        self.logger.warning(f"违规项 {i} 不是字典类型: {type(viol_data)}")
                        continue
                    
                    tuple_id_raw = viol_data.get('tuple_id', '')
                    tuple_id = tuple_id_raw.strip('[]')
                    
                    violation_id = IdGenerator.generate_violation_id(
                        table_name, 
                        tuple_id,
                        viol_data.get('attr', ''),
                        ConstraintType.LOGIC.value
                    )
                    
                    suggested_fix = None
                    if viol_data.get('suggested_fix'):
                        suggested_fix = SuggestedFix(
                            value=viol_data['suggested_fix']
                        )
                    
                    violation = Violation(
                        id=violation_id,
                        table=table_name,
                        tuple_id=tuple_id,  # 使用清理后的tuple_id
                        attr=viol_data.get('attr', ''),
                        constraint_type=ConstraintType.LOGIC.value,
                        description=f"[业务逻辑] {viol_data.get('description', '业务逻辑问题')}",
                        severity=viol_data.get('severity', 'warn'),
                        suggested_fix=suggested_fix,
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violations.append(violation)
                    
                except Exception as ve:
                    self.logger.error(f"处理违规项 {i} 时出错: {ve}")
                    continue
                
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析失败: {e}")
            self.logger.error(f"尝试解析的内容: {json_str if 'json_str' in locals() else llm_response}")
            
            if 'json_str' in locals() and json_str:
                try:
                    fixed_violations = self._attempt_json_repair(json_str, llm_response, table_name)
                    violations.extend(fixed_violations)
                except Exception as repair_error:
                    self.logger.error(f"JSON修复失败: {repair_error}")
            
        except (KeyError, ValueError, TypeError) as e:
            self.logger.error(f"解析LLM业务逻辑验证响应失败: {e}")
        except Exception as e:
            self.logger.error(f"未知错误: {e}")
        
        return violations
    
    def _attempt_json_repair(self, json_str: str, original_response: str, table_name: str = "tax") -> List[Violation]:
        """尝试修复损坏的JSON并解析违规"""
        violations = []
        
        try:
            if json_str.startswith('{') and not json_str.endswith('}'):
                violations_pattern = r'"violations"\s*:\s*\[(.*?)\]'
                match = re.search(violations_pattern, original_response, re.DOTALL)
                if match:
                    violations_content = match.group(1)
                    repaired_json = f'{{"violations": [{violations_content}]}}'
                    self.logger.info("尝试修复不完整的JSON对象")
                    data = json.loads(repaired_json)
                    return self._process_violations_data(data.get('violations', []), table_name)
            
            violations_items = re.findall(r'\{[^}]*"tuple_id"[^}]*\}', original_response, re.DOTALL)
            if violations_items:
                for item_str in violations_items:
                    try:
                        item_data = json.loads(item_str)
                        violation = self._create_violation_from_data(item_data, table_name)
                        if violation:
                            violations.append(violation)
                    except:
                        continue
            
            return violations
            
        except Exception as e:
            self.logger.error(f"JSON修复尝试失败: {e}")
            return []
    
    def _process_violations_data(self, violations_data: List[Dict], table_name: str) -> List[Violation]:
        """处理violations数据并转换为Violation对象"""
        violations = []
        for i, viol_data in enumerate(violations_data):
            try:
                violation = self._create_violation_from_data(viol_data, table_name)
                if violation:
                    violations.append(violation)
            except Exception as ve:
                self.logger.error(f"处理违规项 {i} 时出错: {ve}")
                continue
        return violations
    
    def _create_violation_from_data(self, viol_data: Dict, table_name: str) -> Optional[Violation]:
        """从数据创建Violation对象"""
        try:
            tuple_id_raw = viol_data.get('tuple_id', '')
            tuple_id = tuple_id_raw.strip('[]')  # 移除方括号
            attr = viol_data.get('attr', '')
            description = viol_data.get('description', '')
            
            if not tuple_id or not attr:
                return None
            
            suggested_fix_text = viol_data.get('suggested_fix', '')
            suggested_fix = SuggestedFix(
                value=suggested_fix_text
            ) if suggested_fix_text else None
            
            violation = Violation(
                id=IdGenerator.generate_violation_id(
                    table_name, tuple_id, attr, ConstraintType.LOGIC.value
                ),
                table=table_name,
                tuple_id=tuple_id,
                attr=attr,
                constraint_type=ConstraintType.LOGIC.value,
                description=description,
                severity=ViolationSeverity.WARN.value,  # 默认为警告
                suggested_fix=suggested_fix,
                detector_id=self.verifier_id,
                timestamp=""
            )
            
            return violation
            
        except Exception as e:
            self.logger.error(f"创建Violation对象失败: {e}")
            return None


class LogicFixer(MCPFixer):
    """逻辑约束修复器 - 使用LLM进行业务逻辑修复"""
    
    def __init__(self):
        super().__init__("LogicFixer.v1")
        self.enable_llm_fixing = True
        
        self.format_standards = {
            '季度': '第一季度',  # 统一使用中文全称格式
            '税额': '亿元'       # 统一使用亿元单位
        }
        self.applied_fixes_memory = {}  # 记录已应用的修复，避免反复修复
        
        self.rules_loader = BusinessLogicRulesLoader()
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.LOGIC.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.LOGIC.value
    
    def can_fix(self, violation: Violation) -> bool:
        return (violation.constraint_type == ConstraintType.LOGIC.value and 
                self.enable_llm_fixing)
    
    def get_supported_fix_types(self) -> List[str]:
        return [
            FixType.LOGIC_FIX.value,
            FixType.BUSINESS_RULE_FIX.value,
            FixType.CALCULATION_FIX.value
        ]
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, 
            context=None) -> List[Fix]:
        """修复逻辑约束违规"""
        if not self.can_fix(violation):
            return []
        
        fixes = []
        
        violation_key = f"{violation.attr}_{violation.tuple_id}"
        if violation_key in self.applied_fixes_memory:
            return []
        
        
        standard_fix = self._apply_standard_format(violation, snapshot)
        if standard_fix:
            fixes.append(standard_fix)
            self.applied_fixes_memory[violation_key] = standard_fix.new
        else:
            llm_fixes = self._llm_fix_logic_violation(violation, snapshot, context)
            fixes.extend(llm_fixes)
            for fix in llm_fixes:
                fix_key = f"{fix.attr}_{fix.tuple_id}"
                self.applied_fixes_memory[fix_key] = fix.new
        
        return fixes
    
    def _apply_standard_format(self, violation: Violation, snapshot: TableSnapshot) -> Optional[Fix]:
        """应用预定义的标准格式修复"""
        try:
            old_value = get_cell_value(violation, snapshot)
            if not old_value:
                return None
            
            attr_name = violation.attr
            new_value = None
            
            if attr_name == '季度':
                new_value = self._standardize_quarter_format(old_value)
            
            elif attr_name == '税额':
                new_value = self._standardize_amount_format(old_value)
            
            if new_value and new_value != old_value:
                fix_id = IdGenerator.generate_fix_id(
                    violation.table, violation.tuple_id, violation.attr,
                    FixType.LOGIC_FIX.value, old_value
                )
                
                fix = Fix(
                    id=fix_id,
                    table=violation.table,
                    tuple_id=violation.tuple_id,
                    attr=violation.attr,
                    old=old_value,
                    new=new_value,
                    fix_type=FixType.LOGIC_FIX.value,
                    applied_by=f"{self.mcp_id}_standard",
                    timestamp=""
                )
                
                return fix
                
        except Exception as e:
            self.logger.error(f"标准格式修复失败: {e}")
        
        return None
    
    def _standardize_quarter_format(self, value: str) -> Optional[str]:
        """标准化季度格式为中文全称"""
        if not value:
            return None
        
        value = str(value).strip()
        value_lower = value.lower()
        
        if value_lower in ['q1', 'quarter 1', '1q', '一季度']:
            return '第一季度'
        elif value_lower in ['q2', 'quarter 2', '2q', '二季度']:
            return '第二季度'
        elif value_lower in ['q3', 'quarter 3', '3q', '三季度']:
            return '第三季度'
        elif value_lower in ['q4', 'quarter 4', '4q', '四季度']:
            return '第四季度'
        elif '第一季度' in value:
            return '第一季度'
        elif '第二季度' in value:
            return '第二季度'
        elif '第三季度' in value:
            return '第三季度'
        elif '第四季度' in value:
            return '第四季度'
        
        return None
    
    def _standardize_amount_format(self, value: str) -> Optional[str]:
        """标准化金额格式，确保有单位"""
        if not value:
            return None
        
        value = str(value).strip()
        
        import re
        
        if re.match(r'^\d+(\.\d+)?\s+亿$', value):
            return value.replace(' ', '') + '元'
        
        if value.endswith('亿人民币'):
            return value.replace('亿人民币', '亿元')
        
        if value.endswith('亿') and not value.endswith('亿元'):
            return value + '元'
        
        if re.match(r'^\d+(\.\d+)?$', value):
            return value + '亿元'
        
        if re.match(r'^\d+(\.\d+)?亿.+', value) and not value.endswith('亿元'):
            number_match = re.match(r'^(\d+(?:\.\d+)?)亿', value)
            if number_match:
                number_part = number_match.group(1)
                return f"{number_part}亿元"
        
        return None
    
    def _build_fix_consistency_rules(self, violation: Violation, snapshot: TableSnapshot) -> str:
        """
        根据当前表和字段动态构建修复规则
        只包含相关的规则，避免添加不必要的prompt
        """
        field_names = []
        if len(snapshot.rows) > 0:
            field_names = list(snapshot.rows[0].cells.keys())
        
        applicable_prompts = self.rules_loader.get_applicable_prompts(
            violation.table, 
            field_names
        )
        
        if applicable_prompts:
            rules_text = "数据标准化规则（仅适用于当前表）：\n"
            
            for prompt in applicable_prompts:
                rules_text += f"- {prompt}\n"
            
            return rules_text
        else:
            return "数据标准化规则：\n- 保持数据的一致性和准确性\n"
    
    def _llm_fix_logic_violation(self, violation: Violation, snapshot: TableSnapshot, 
                                context=None) -> List[Fix]:
        """使用LLM修复业务逻辑违规"""
        fixes = []
        
        try:
            old_value = get_cell_value(violation, snapshot)
            
            target_row_data = self._get_target_row_data(violation, snapshot)
            reference_samples = self._get_reference_samples(violation, snapshot)
            
            consistency_rules = self._build_fix_consistency_rules(violation, snapshot)
            
            universal_constraints = """
⚠️ **重要约束**：
1. 修复时**只能使用文档中明确出现的信息**
2. **严禁推导、计算或编造任何值**（例如：不能根据出生日期计算年龄，不能根据其他字段推导值）
3. **不要**基于常识或外部知识添加信息（如具体地址、邮编、电话等）
4. 如果文档中没有该字段的明确值，**必须保持为null**，不要尝试修复
5. 对于地理位置等信息，只能使用文档中明确提到的内容
6. 不要进行推测性修复，如果不确定就不要修复
7. **当前值为null且文档中没有明确提供该值时，new_value必须返回null**
"""
            
            consistency_rules += "\n" + universal_constraints
            
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            baml_options = get_baml_options(model)
            
            self.logger.info(f"🤖 [LogicFixer] 正在调用LLM修复 {violation.attr} 字段（model={model}）...")
            baml_fix = baml_client.FixBusinessLogic(
                table_name=violation.table,
                field_name=violation.attr,
                problem_description=violation.description,
                current_value=str(old_value),
                target_row_data=target_row_data,
                reference_samples=reference_samples,
                consistency_rules=consistency_rules,
                baml_options=baml_options  # ✅ 传递客户端配置
            )
            
            llm_fix = self._convert_baml_fix(baml_fix, violation, old_value, context)
            if llm_fix:
                fixes.append(llm_fix)
                
        except Exception as e:
            self.logger.error(f"LLM业务逻辑修复失败: {e}")
        
        return fixes
    
    def _get_target_row_data(self, violation: Violation, snapshot: TableSnapshot) -> str:
        """获取目标行的完整数据"""
        target_row = None
        for row in snapshot.rows:
            if row.tuple_id == violation.tuple_id:
                target_row = row
                break
        
        if not target_row:
            return ""
        
        row_parts = []
        for attr, cell in target_row.cells.items():
            value = cell.value
            row_parts.append(f"{attr}: {value}")
        
        return "\n".join(row_parts)
    
    def _get_reference_samples(self, violation: Violation, snapshot: TableSnapshot) -> str:
        """获取参考数据样本"""
        other_rows = [row for row in snapshot.rows[:5] if row.tuple_id != violation.tuple_id]
        if not other_rows:
            return ""
        
        sample_parts = []
        for i, row in enumerate(other_rows):
            row_data = []
            for attr, cell in row.cells.items():
                value = cell.value
                row_data.append(f"{attr}: {value}")
            sample_parts.append(f"样本{i+1}: {', '.join(row_data)}")
        
        return "\n".join(sample_parts)
    
    def _mark_unfixable(self, violation: Violation, context):
        """标记违规为无法修复"""
        try:
            memory_manager = None
            run_id = None
            
            if context:
                if isinstance(context, dict):
                    memory_manager = context.get('memory_manager')
                    run_id = context.get('run_id')
                else:
                    memory_manager = getattr(context, 'memory_manager', None)
                    run_id = getattr(context, 'run_id', None)
            
            if memory_manager and run_id:
                import asyncio
                
                try:
                    loop = asyncio.get_running_loop()
                    asyncio.create_task(
                        memory_manager.mark_violation_unfixable(violation.id, run_id)
                    )
                    self.logger.debug(f"✅ 已标记违规 {violation.id} 为无法修复（异步）")
                except RuntimeError:
                    asyncio.run(memory_manager.mark_violation_unfixable(violation.id, run_id))
                    self.logger.debug(f"✅ 已标记违规 {violation.id} 为无法修复")
            else:
                self.logger.debug(f"⚠️ 无法标记违规 {violation.id} 为unfixable: context缺少memory_manager或run_id")
        except Exception as e:
            self.logger.error(f"❌ 标记违规 {violation.id} 为unfixable失败: {e}")
            import traceback
            traceback.print_exc()
    
    def _convert_baml_fix(self, baml_fix: LogicFix, violation: Violation, 
                         old_value: Any, context=None) -> Optional[Fix]:
        """将修复转换为系统 Fix 对象"""
        try:
            new_value = baml_fix.new_value
            if not new_value or new_value.strip() == "":
                self.logger.info(f"表示无法修复此问题: {violation.id} (原因: {baml_fix.reasoning})")
                
                self._mark_unfixable(violation, context)
                
                return None
            
            fix_type_map = {
                'logic_fix': FixType.LOGIC_FIX.value,
                'business_rule_fix': FixType.BUSINESS_RULE_FIX.value,
                'calculation_fix': FixType.CALCULATION_FIX.value,
            }
            
            fix_type = fix_type_map.get(
                baml_fix.fix_type.lower() if baml_fix.fix_type else '', 
                FixType.LOGIC_FIX.value
            )
            
            fix_id = IdGenerator.generate_fix_id(
                violation.table, violation.tuple_id, violation.attr,
                fix_type, old_value
            )
            
            fix = Fix(
                id=fix_id,
                table=violation.table,
                tuple_id=violation.tuple_id,
                attr=violation.attr,
                old=old_value,
                new=new_value,
                fix_type=fix_type,
                applied_by=f"{self.mcp_id}_baml",
                timestamp=""
            )
            
            return fix
            
        except Exception as e:
            self.logger.error(f"转换BAML修复响应失败: {e}")
            return None
    
    def _build_logic_fix_context(self, violation: Violation, snapshot: TableSnapshot) -> str:
        """构建业务逻辑修复上下文"""
        context_parts = []
        
        target_row = None
        for row in snapshot.rows:
            if row.tuple_id == violation.tuple_id:
                target_row = row
                break
        
        if target_row:
            context_parts.append("目标行数据:")
            for attr, cell in target_row.cells.items():
                value = cell.value
                context_parts.append(f"  {attr}: {value}")
        
        other_rows = [row for row in snapshot.rows[:5] if row.tuple_id != violation.tuple_id]
        if other_rows:
            context_parts.append("\n参考数据样本:")
            for i, row in enumerate(other_rows):
                row_data = []
                for attr, cell in row.cells.items():
                    value = cell.value
                    row_data.append(f"{attr}: {value}")
                context_parts.append(f"  样本{i+1}: {', '.join(row_data)}")
        
        return "\n".join(context_parts)
    
    def _parse_llm_logic_fix_response(self, llm_response: str, violation: Violation, 
                                     old_value: Any) -> Optional[Fix]:
        """解析LLM业务逻辑修复响应"""
        try:
            if not llm_response or not llm_response.strip():
                self.logger.warning("LLM返回空修复响应")
                return None
            
            json_str = None
            
            json_match = re.search(r'```json\s*(\{.*?\}|\[.*?\])\s*```', llm_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r'```\s*(\{.*?\}|\[.*?\])\s*```', llm_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    json_match = re.search(r'\{.*?"fix".*?\}', llm_response, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                    else:
                        json_str = llm_response.strip()
            
            if not json_str or not json_str.strip():
                self.logger.warning("无法提取有效的修复JSON内容")
                return None
            
            data = json.loads(json_str)
            
            if not isinstance(data, dict):
                self.logger.warning(f"修复JSON解析结果不是字典类型: {type(data)}")
                return None
                
            fix_data = data.get('fix')
            
            if fix_data is None:
                self.logger.info("LLM表示无法修复此问题")
                return None
            
            if not isinstance(fix_data, dict):
                self.logger.warning(f"fix字段不是字典类型: {type(fix_data)}")
                return None
            
            new_value = fix_data.get('new_value')
            if new_value is None:
                self.logger.warning("修复数据中缺少new_value字段")
                return None
            
            fix_type_map = {
                'logic_fix': FixType.LOGIC_FIX.value,
                'business_rule_fix': FixType.BUSINESS_RULE_FIX.value,
                'calculation_fix': FixType.CALCULATION_FIX.value,
            }
            
            fix_type = fix_type_map.get(
                fix_data.get('fix_type', '').lower(), 
                FixType.LOGIC_FIX.value
            )
            
            fix_id = IdGenerator.generate_fix_id(
                violation.table, violation.tuple_id, violation.attr,
                fix_type, old_value
            )
            
            fix = Fix(
                id=fix_id,
                table=violation.table,
                tuple_id=violation.tuple_id,
                attr=violation.attr,
                old=old_value,
                new=new_value,
                fix_type=fix_type,
                applied_by=f"{self.mcp_id}_llm",
                timestamp=""
            )
            
            return fix
            
        except json.JSONDecodeError as e:
            self.logger.error(f"修复JSON解析失败: {e}")
            self.logger.error(f"尝试解析的内容: {json_str if 'json_str' in locals() else llm_response}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            self.logger.error(f"解析LLM业务逻辑修复响应失败: {e}")
            return None
        except Exception as e:
            self.logger.error(f"解析修复响应时发生未知错误: {e}")
            return None
    
    def fix_batch(self, violations: List[Violation], snapshot: TableSnapshot, 
                  context=None) -> List[Fix]:
        """
        批量修复逻辑约束违规
        
        策略：
        1. 先应用标准格式修复（不需要LLM）
        2. 对需要LLM的违规进行批量处理
        """
        all_fixes = []
        
        standard_violations = []
        need_llm_violations = []
        
        for violation in violations:
            if not self.can_fix(violation):
                continue
            
            violation_key = f"{violation.attr}_{violation.tuple_id}"
            if violation_key in self.applied_fixes_memory:
                continue
            
            attr_name = violation.attr
            if attr_name in self.format_standards:
                standard_violations.append(violation)
            else:
                need_llm_violations.append(violation)
        
        for violation in standard_violations:
            standard_fix = self._apply_standard_format(violation, snapshot)
            if standard_fix:
                all_fixes.append(standard_fix)
                violation_key = f"{violation.attr}_{violation.tuple_id}"
                self.applied_fixes_memory[violation_key] = standard_fix.new
        
        if need_llm_violations:
            llm_fixes = self._batch_fix_with_llm(need_llm_violations, snapshot, context)
            all_fixes.extend(llm_fixes)
            for fix in llm_fixes:
                fix_key = f"{fix.attr}_{fix.tuple_id}"
                self.applied_fixes_memory[fix_key] = fix.new
        
        if all_fixes:
            self.logger.info(f"LogicFixer: 批量修复 {len(violations)} 个违规，生成 {len(all_fixes)} 个修复")
        
        return all_fixes
    
    def _batch_fix_with_llm(self, violations: List[Violation], 
                           snapshot: TableSnapshot, context=None) -> List[Fix]:
        """
        批量使用 LLM 修复业务逻辑违规
        
        核心优化：将所有违规合并到一个 prompt，LLM 一次性返回所有修复
        """
        if not violations:
            return []
        
        fixes = []
        table_name = violations[0].table
        
        violations_info = []
        for i, violation in enumerate(violations):
            old_value = get_cell_value(violation, snapshot)
            target_row_data = self._get_target_row_data(violation, snapshot)
            
            violations_info.append(f"""
【违规 {i+1}】
- violation_id: {violation.id}
- tuple_id: {violation.tuple_id}
- field: {violation.attr}
- problem: {violation.description}
- current_value: {old_value}
- row_data:
{target_row_data}
""")
        
        reference_samples = self._get_reference_samples(violations[0], snapshot)
        consistency_rules = self._build_fix_consistency_rules(violations[0], snapshot)
        
        universal_constraints = """
⚠️ **重要约束**：
1. 修复时**只能使用文档中明确出现的信息**
2. **严禁推导、计算或编造任何值**
3. **不要**基于常识或外部知识添加信息
4. 如果文档中没有该字段的明确值，**必须保持为null**
5. **当前值为null且文档中没有明确提供该值时，new_value必须返回null**
"""
        
        user_prompt = f"""🎯 任务：批量修复表 [{table_name}] 的业务逻辑违规

【需要修复的违规】共 {len(violations)} 个
{''.join(violations_info)}

【参考样本】
{reference_samples}

【一致性规则】
{consistency_rules}
{universal_constraints}

【输出格式】请严格按照以下 JSON 格式输出：
```json
{{
  "fixes": [
    {{
      "violation_id": "v-xxx",
      "tuple_id": "...",
      "field": "...",
      "new_value": "...",  // 如果无法修复则为 null
      "reasoning": "修复原因或无法修复的原因",
      "can_fix": true  // 或 false
    }}
  ]
}}
```

请分析所有违规并一次性输出所有修复建议。
"""
        
        system_prompt = """You are a data quality expert specializing in fixing business logic violations.
Your task is to analyze ALL violations and provide fixes for each one based ONLY on explicitly provided information in the documents.
Output in strict JSON format."""
        
        try:
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            
            self.logger.info(f"🚀 [LogicFixer] 批量修复 {len(violations)} 个违规（model={model}）...")
            
            from llm.main import get_answer
            llm_response = get_answer(user_prompt, system_prompt=system_prompt, model=model)
            
            fixes = self._parse_batch_llm_response(llm_response, violations, snapshot, context)
            
            self.logger.info(f"✅ [LogicFixer] 批量修复完成，生成 {len(fixes)} 个修复")
            
        except Exception as e:
            self.logger.error(f"❌ [LogicFixer] 批量LLM修复失败: {e}")
            self.logger.info("⚠️ 降级到逐个修复模式...")
            for violation in violations:
                try:
                    individual_fixes = self._llm_fix_logic_violation(violation, snapshot, context)
                    fixes.extend(individual_fixes)
                except Exception as ve:
                    self.logger.error(f"修复违规 {violation.id} 失败: {ve}")
        
        return fixes
    
    def _parse_batch_llm_response(self, llm_response: str, violations: List[Violation],
                                  snapshot: TableSnapshot, context) -> List[Fix]:
        """解析批量 LLM 响应"""
        fixes = []
        
        try:
            json_str = None
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', llm_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r'```\s*(\{.*?\})\s*```', llm_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    json_match = re.search(r'\{.*?"fixes".*?\}', llm_response, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
            
            if not json_str:
                self.logger.warning("无法从LLM响应中提取JSON")
                return fixes
            
            data = json.loads(json_str)
            fixes_data = data.get('fixes', [])
            
            violation_map = {v.id: v for v in violations}
            
            for fix_data in fixes_data:
                try:
                    violation_id = fix_data.get('violation_id')
                    if not violation_id or violation_id not in violation_map:
                        self.logger.warning(f"无效的 violation_id: {violation_id}")
                        continue
                    
                    violation = violation_map[violation_id]
                    can_fix = fix_data.get('can_fix', True)
                    new_value = fix_data.get('new_value')
                    reasoning = fix_data.get('reasoning', '')
                    
                    if not can_fix or new_value is None or str(new_value).strip() == "":
                        self.logger.info(f"无法修复此问题: {violation_id} (原因: {reasoning})")
                        self._mark_unfixable(violation, context)
                        continue
                    
                    old_value = get_cell_value(violation, snapshot)
                    
                    fix = Fix(
                        id=IdGenerator.generate_fix_id(
                            violation.table, 
                            violation.tuple_id, 
                            violation.attr,
                            FixType.LOGIC_FIX.value, 
                            old_value
                        ),
                        table=violation.table,
                        tuple_id=violation.tuple_id,
                        attr=violation.attr,
                        old=old_value,
                        new=new_value,
                        fix_type=FixType.LOGIC_FIX.value,
                        applied_by=f"{self.mcp_id}_batch_llm",
                        timestamp=""
                    )
                    fixes.append(fix)
                    
                except Exception as e:
                    self.logger.error(f"处理修复数据失败: {e}")
                    continue
            
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析失败: {e}")
        except Exception as e:
            self.logger.error(f"解析批量LLM响应失败: {e}")
        
        return fixes


class LogicMCP(BaseMCP):
    """逻辑约束MCP"""
    
    def __init__(self):
        verifier = LogicVerifier()
        fixer = LogicFixer()
        super().__init__("LogicMCP.v1", verifier, fixer)
