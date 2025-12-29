"""格式约束MCP - 单元格级别的格式验证和修复"""
import re
import json
import os
import logging
from typing import List, Dict, Any, Optional
from .base import MCPVerifier, MCPFixer, BaseMCP, get_cell_value, get_table_definition
from ...memory import TableSnapshot, Violation, Fix, SuggestedFix, ConstraintType, ViolationSeverity, FixType
from ...core.ids import IdGenerator


class FormatVerifier(MCPVerifier):
    """格式约束验证器 - 单元格级别"""
    
    def __init__(self, config_path: str = None):
        super().__init__("FormatVerifier.v1")
        self.logger = logging.getLogger('doc2db.mcp.format_verifier')
        
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), 
                'config', 
                'format_rules.json'
            )
        
        self.field_format_rules = self._load_config(config_path)
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """从JSON文件加载格式规则配置"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config.get('field_format_rules', {})
        except FileNotFoundError:
            self.logger.warning(f"配置文件未找到: {config_path}，使用默认配置")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"配置文件解析失败: {e}，使用默认配置")
            return {}
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.FORMAT.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.FORMAT.value
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """逐单元格验证格式"""
        violations = []
        
        self.logger.info(f"开始单元格级别格式验证: {table_name}")
        total_cells_checked = 0
        
        table_def = get_table_definition(schema, table_name)
        attr_dict = {}
        
        if table_def:
            attributes = table_def.get('attributes', [])
            attr_dict = {attr['name']: attr for attr in attributes}
            self.logger.info(f"从schema中获取到 {len(attr_dict)} 个字段定义")
        else:
            self.logger.warning(f"未能从schema中获取表 {table_name} 的定义，仅使用配置文件规则")
        
        for row in snapshot.rows:
            for attr, cell in row.cells.items():
                value = str(cell.value) if cell.value is not None else ""
                
                if not value:
                    continue
                
                schema_pattern = None
                schema_description = None
                
                if attr in attr_dict:
                    attr_def = attr_dict[attr]
                    schema_pattern = attr_def.get('pattern')
                    schema_description = attr_def.get('pattern_description') or attr_def.get('format_description')
                
                if schema_pattern:
                    total_cells_checked += 1
                    
                    if not re.match(schema_pattern, value):
                        violation_id = IdGenerator.generate_violation_id(
                            table_name, row.tuple_id, attr, 
                            ConstraintType.FORMAT.value
                        )
                        
                        description = f"[格式不符] {attr}字段格式不符合schema定义"
                        if schema_description:
                            description += f": {schema_description}"
                        description += f"，当前值: '{value}'"
                        
                        violation = Violation(
                            id=violation_id,
                            table=table_name,
                            tuple_id=row.tuple_id,
                            attr=attr,
                            constraint_type=ConstraintType.FORMAT.value,
                            description=description,
                            severity=ViolationSeverity.WARN.value,
                            suggested_fix=SuggestedFix(
                                value=f"请修改为符合模式 '{schema_pattern}' 的格式"
                            ),
                            detector_id=self.mcp_id,
                            timestamp=""
                        )
                        violations.append(violation)
                        
                        self.logger.debug(f"检测到格式违规(schema): {row.tuple_id}.{attr} = '{value}'")
                
                elif attr in self.field_format_rules:
                    total_cells_checked += 1
                    rule = self.field_format_rules[attr]
                    
                    if not re.match(rule['pattern'], value):
                        violation_id = IdGenerator.generate_violation_id(
                            table_name, row.tuple_id, attr, 
                            ConstraintType.FORMAT.value
                        )
                        
                        violation = Violation(
                            id=violation_id,
                            table=table_name,
                            tuple_id=row.tuple_id,
                            attr=attr,
                            constraint_type=ConstraintType.FORMAT.value,
                            description=f"[格式不符] {attr}字段格式不符合标准'{rule['description']}'，当前值: '{value}'",
                            severity=ViolationSeverity.WARN.value,
                            suggested_fix=SuggestedFix(
                                value=f"建议修改为标准格式，如: {', '.join(rule['standard_examples'][:2])}"
                            ),
                            detector_id=self.mcp_id,
                            timestamp=""
                        )
                        violations.append(violation)
                        
                        self.logger.debug(f"检测到格式违规(配置): {row.tuple_id}.{attr} = '{value}'")
        
        self.logger.info(f"格式验证完成: 检查了{total_cells_checked}个单元格，发现{len(violations)}个违规")
        return violations
    
    def verify_cell(self, table: str, tuple_id: str, attr: str, 
                   value: Any, attr_def: Dict[str, Any], 
                   snapshot: TableSnapshot, context=None) -> List[Violation]:
        """验证单个单元格的格式"""
        violations = []
        value_str = str(value) if value is not None else ""
        
        if not value_str:
            return violations
        
        schema_pattern = attr_def.get('pattern') if attr_def else None
        schema_description = None
        
        if attr_def:
            schema_description = attr_def.get('pattern_description') or attr_def.get('format_description')
        
        if schema_pattern:
            if not re.match(schema_pattern, value_str):
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.FORMAT.value
                )
                
                description = f"[格式不符] {attr}字段格式不符合schema定义"
                if schema_description:
                    description += f": {schema_description}"
                description += f"，当前值: '{value_str}'"
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.FORMAT.value,
                    description=description,
                    severity=ViolationSeverity.WARN.value,
                    suggested_fix=SuggestedFix(
                        value=f"请修改为符合模式 '{schema_pattern}' 的格式"
                    ),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        elif attr in self.field_format_rules:
            rule = self.field_format_rules[attr]
            
            if not re.match(rule['pattern'], value_str):
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.FORMAT.value
                )
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.FORMAT.value,
                    description=f"[格式不符] {attr}字段格式不符合标准'{rule['description']}'，当前值: '{value_str}'",
                    severity=ViolationSeverity.WARN.value,
                    suggested_fix=SuggestedFix(
                        value=f"建议修改为标准格式，如: {', '.join(rule['standard_examples'][:2])}"
                    ),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations


class FormatFixer(MCPFixer):
    """格式约束修复器 - 单元格级别"""
    
    def __init__(self, config_path: str = None):
        super().__init__("FormatFixer.v1")
        self.logger = logging.getLogger('doc2db.mcp.format_fixer')
        
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), 
                'config', 
                'format_rules.json'
            )
        
        self.format_config = self._load_config(config_path)
        
        self.format_normalizers = {}
        for field_name in self.format_config.keys():
            if field_name == '季度':
                self.format_normalizers[field_name] = self._normalize_quarter
            elif field_name == '税额':
                self.format_normalizers[field_name] = self._normalize_amount
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """从JSON文件加载格式规则配置"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config.get('field_format_rules', {})
        except FileNotFoundError:
            self.logger.warning(f"配置文件未找到: {config_path}，使用默认配置")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"配置文件解析失败: {e}，使用默认配置")
            return {}
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.FORMAT.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.FORMAT.value
    
    def can_fix(self, violation: Violation) -> bool:
        return (violation.constraint_type == ConstraintType.FORMAT.value and
                violation.attr in self.format_normalizers)
    
    def get_supported_fix_types(self) -> List[str]:
        return [FixType.FORMAT_FIX.value]
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, 
            context=None) -> List[Fix]:
        """修复格式违规"""
        if not self.can_fix(violation):
            return []
        
        fixes = []
        
        try:
            old_value = get_cell_value(violation, snapshot)
            
            self.logger.info(f"FormatFixer: 尝试修复 {violation.tuple_id}.{violation.attr}")
            self.logger.info(f"FormatFixer: 获取到的old_value = {old_value}")
            
            if old_value is None:
                self.logger.warning(f"无法获取单元格值: {violation.tuple_id}.{violation.attr}")
                return []
            
            normalizer = self.format_normalizers[violation.attr]
            new_value = normalizer(str(old_value))
            
            self.logger.info(f"FormatFixer: 标准化结果 '{old_value}' -> '{new_value}'")
            
            if new_value and new_value != old_value:
                fix_id = IdGenerator.generate_fix_id(
                    violation.table, violation.tuple_id, violation.attr,
                    FixType.FORMAT_FIX.value, old_value
                )
                
                fix = Fix(
                    id=fix_id,
                    table=violation.table,
                    tuple_id=violation.tuple_id,
                    attr=violation.attr,
                    old=old_value,
                    new=new_value,
                    fix_type=FixType.FORMAT_FIX.value,
                    applied_by=self.mcp_id,
                    timestamp="",
                    fix_success=True
                )
                
                fixes.append(fix)
                self.logger.info(f"格式修复: {violation.attr} '{old_value}' -> '{new_value}'")
                
        except Exception as e:
            self.logger.error(f"格式修复失败: {e}")
        
        return fixes
    
    def fix_batch(self, violations: List[Violation], snapshot: TableSnapshot, 
                  context=None) -> List[Fix]:
        """批量修复格式违规，支持统一修复"""
        all_fixes = []
        
        unified_fixes = self._apply_unified_fixes(violations, snapshot)
        all_fixes.extend(unified_fixes)
        
        fixed_violations = {f"{fix.attr}_{fix.tuple_id}" for fix in unified_fixes}
        
        remaining_violations = [
            v for v in violations 
            if f"{v.attr}_{v.tuple_id}" not in fixed_violations
        ]
        
        for violation in remaining_violations:
            fixes = self.fix(violation, snapshot, context)
            all_fixes.extend(fixes)
        
        self.logger.info(f"批量格式修复完成，共生成 {len(all_fixes)} 个修复")
        return all_fixes
    
    def _apply_unified_fixes(self, violations: List[Violation], 
                            snapshot: TableSnapshot) -> List[Fix]:
        """应用统一修复规则"""
        unified_fixes = []
        
        try:
            violations_by_attr = {}
            for violation in violations:
                attr = getattr(violation, 'attr', '')
                if attr:
                    if attr not in violations_by_attr:
                        violations_by_attr[attr] = []
                    violations_by_attr[attr].append(violation)
            
            for attr, attr_violations in violations_by_attr.items():
                if attr == '季度':
                    attr_fixes = self._unify_quarter_format(attr_violations, snapshot)
                    unified_fixes.extend(attr_fixes)
                elif attr == '税额':
                    attr_fixes = self._unify_amount_format(attr_violations, snapshot)
                    unified_fixes.extend(attr_fixes)
                
        except Exception as e:
            self.logger.error(f"统一修复处理失败: {e}")
        
        return unified_fixes
    
    def _unify_quarter_format(self, violations: List[Violation], 
                             snapshot: TableSnapshot) -> List[Fix]:
        """统一季度格式"""
        fixes = []
        
        try:
            if '季度' not in self.format_config:
                return fixes
            
            config = self.format_config['季度']
            quarter_map = config.get('mapping', {})
            standard_values = config.get('standard_values', [])
            
            processed_tuples = set()
            
            for row in snapshot.rows:
                if '季度' in row.cells:
                    current_value = str(row.cells['季度'].value).strip()
                    new_value = None
                    
                    if current_value.lower() in quarter_map:
                        new_value = quarter_map[current_value.lower()]
                    else:
                        for quarter in standard_values:
                            if quarter in current_value:
                                new_value = quarter
                                break
                    
                    if new_value and new_value != current_value and row.tuple_id not in processed_tuples:
                        fix_id = IdGenerator.generate_fix_id(
                            snapshot.table, row.tuple_id, '季度',
                            FixType.FORMAT_FIX.value, current_value
                        )
                        
                        fix = Fix(
                            id=fix_id,
                            table=snapshot.table,
                            tuple_id=row.tuple_id,
                            attr='季度',
                            old=current_value,
                            new=new_value,
                            fix_type=FixType.FORMAT_FIX.value,
                            applied_by=f"{self.mcp_id}_unified",
                            timestamp="",
                            fix_success=True
                        )
                        fixes.append(fix)
                        processed_tuples.add(row.tuple_id)
                        
        except Exception as e:
            self.logger.error(f"统一季度格式失败: {e}")
        
        return fixes
    
    def _unify_amount_format(self, violations: List[Violation], 
                            snapshot: TableSnapshot) -> List[Fix]:
        """统一税额格式"""
        fixes = []
        
        try:
            if '税额' not in self.format_config:
                return fixes
            
            config = self.format_config['税额']
            conversion_rules = config.get('conversion_rules', [])
            
            processed_tuples = set()
            
            for row in snapshot.rows:
                if '税额' in row.cells:
                    current_value = str(row.cells['税额'].value).strip()
                    new_value = None
                    
                    for rule in conversion_rules:
                        pattern = rule['pattern']
                        template = rule['template']
                        
                        match = re.match(pattern, current_value)
                        if match:
                            new_value = template.format(*match.groups())
                            break
                    
                    if new_value and new_value != current_value and row.tuple_id not in processed_tuples:
                        fix_id = IdGenerator.generate_fix_id(
                            snapshot.table, row.tuple_id, '税额',
                            FixType.FORMAT_FIX.value, current_value
                        )
                        
                        fix = Fix(
                            id=fix_id,
                            table=snapshot.table,
                            tuple_id=row.tuple_id,
                            attr='税额',
                            old=current_value,
                            new=new_value,
                            fix_type=FixType.FORMAT_FIX.value,
                            applied_by=f"{self.mcp_id}_unified",
                            timestamp="",
                            fix_success=True
                        )
                        fixes.append(fix)
                        processed_tuples.add(row.tuple_id)
                        
        except Exception as e:
            self.logger.error(f"统一税额格式失败: {e}")
        
        return fixes
    
    def _normalize_quarter(self, value: str) -> Optional[str]:
        """标准化季度格式为中文全称"""
        if not value or '季度' not in self.format_config:
            return None
        
        value = str(value).strip()
        value_lower = value.lower()
        
        config = self.format_config['季度']
        quarter_map = config.get('mapping', {})
        standard_values = config.get('standard_values', [])
        
        if value_lower in quarter_map:
            return quarter_map[value_lower]
        
        for quarter in standard_values:
            if quarter in value:
                return quarter
        
        return None
    
    def _normalize_amount(self, value: str) -> Optional[str]:
        """标准化金额格式，确保有单位"""
        if not value or '税额' not in self.format_config:
            return None
        
        value = str(value).strip()
        
        config = self.format_config['税额']
        standard_pattern = config.get('standard_pattern', '')
        conversion_rules = config.get('conversion_rules', [])
        
        if re.match(standard_pattern, value):
            return value
        
        for rule in conversion_rules:
            pattern = rule['pattern']
            template = rule['template']
            
            match = re.match(pattern, value)
            if match:
                return template.format(*match.groups())
        
        return None


class FormatMCP(BaseMCP):
    """格式约束MCP"""
    
    def __init__(self):
        verifier = FormatVerifier()
        fixer = FormatFixer()
        super().__init__("FormatMCP.v1", verifier, fixer)
