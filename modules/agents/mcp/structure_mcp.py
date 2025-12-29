"""结构约束MCP - 验证和修复数据结构相关问题（非空、唯一性等）"""
from typing import List, Dict, Any, Optional, Set
from .base import MCPVerifier, MCPFixer, BaseMCP, get_table_definition, get_cell_value, is_valid_fix_value
from ...memory import TableSnapshot, Violation, Fix, SuggestedFix, ConstraintType, ViolationSeverity, FixType, TableRow
from ...core.ids import IdGenerator
from datetime import datetime
import json

from baml_src.client_selector import load_env_from_llm_folder, get_baml_options
load_env_from_llm_folder()
from baml_client.sync_client import b as baml_client
from baml_client.types import StructureFix


class StructureVerifier(MCPVerifier):
    """结构约束验证器"""
    
    def __init__(self):
        super().__init__("StructureVerifier.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [
            ConstraintType.STRUCTURE.value,


        ]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type in self.get_supported_constraints()
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """验证表格中的结构约束"""
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
        
        table_violations = self._verify_table_constraints(
            snapshot, attr_dict, table_name
        )
        violations.extend(table_violations)
        
        return violations
    
    def verify_cell(self, table: str, tuple_id: str, attr: str, 
                   value: Any, attr_def: Dict[str, Any], 
                   snapshot: TableSnapshot, context=None) -> List[Violation]:
        """验证单个单元格的结构约束"""
        violations = []
        
        not_null_violations = self._check_not_null_constraint(
            table, tuple_id, attr, value, attr_def
        )
        violations.extend(not_null_violations)
        
        required_violations = self._check_required_constraint(
            table, tuple_id, attr, value, attr_def
        )
        violations.extend(required_violations)
        
        return violations
    
    def _check_not_null_constraint(self, table: str, tuple_id: str, attr: str, 
                                  value: Any, attr_def: Dict[str, Any]) -> List[Violation]:
        """检查非空约束"""
        violations = []
        
        if attr_def.get('not_null', False) or attr_def.get('required', False):
            if self._is_empty_value(value):
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.STRUCTURE.value
                )
                
                default_value = self._suggest_default_value(attr, attr_def)
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.STRUCTURE.value,
                    description=f"字段 {attr} 缺失值（空值/null），违反了结构约束",
                    severity=ViolationSeverity.ERROR.value,
                    
                    suggested_fix=SuggestedFix(value=default_value, ),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations
    
    def _check_required_constraint(self, table: str, tuple_id: str, attr: str, 
                                  value: Any, attr_def: Dict[str, Any]) -> List[Violation]:
        """检查必填约束"""
        violations = []
        
        if attr_def.get('required', False) and not attr_def.get('not_null', False):
            if self._is_empty_value(value):
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.STRUCTURE.value
                )
                
                default_value = self._suggest_default_value(attr, attr_def)
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.STRUCTURE.value,
                    description=f"字段 {attr} 缺失值（空值/null），是必填字段",
                    severity=ViolationSeverity.WARN.value,
                    
                    suggested_fix=SuggestedFix(value=default_value, ),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations
    
    def _verify_table_constraints(self, snapshot: TableSnapshot, 
                                 attr_dict: Dict[str, Dict[str, Any]], 
                                 table_name: str) -> List[Violation]:
        """验证表级结构约束"""
        violations = []
        
        unique_violations = self._check_unique_constraints(
            snapshot, attr_dict, table_name
        )
        violations.extend(unique_violations)
        
        pk_violations = self._check_primary_key_constraints(
            snapshot, attr_dict, table_name
        )
        violations.extend(pk_violations)
        
        return violations
    
    def _check_unique_constraints(self, snapshot: TableSnapshot, 
                                 attr_dict: Dict[str, Dict[str, Any]], 
                                 table_name: str) -> List[Violation]:
        """检查唯一性约束"""
        violations = []
        
        unique_attrs = [
            attr_name for attr_name, attr_def in attr_dict.items()
            if attr_def.get('unique', False)
        ]
        
        for attr_name in unique_attrs:
            value_to_tuples = {}
            
            for row in snapshot.rows:
                if attr_name in row.cells:
                    value = row.cells[attr_name].value
                    if not self._is_empty_value(value):
                        str_value = str(value)
                        if str_value not in value_to_tuples:
                            value_to_tuples[str_value] = []
                        value_to_tuples[str_value].append(row.tuple_id)
            
            for value, tuple_ids in value_to_tuples.items():
                if len(tuple_ids) > 1:
                    for tuple_id in tuple_ids:
                        violation_id = IdGenerator.generate_violation_id(
                            table_name, tuple_id, attr_name, ConstraintType.STRUCTURE.value
                        )
                        
                        other_tuples = [tid for tid in tuple_ids if tid != tuple_id]
                        
                        violation = Violation(
                            id=violation_id,
                            table=table_name,
                            tuple_id=tuple_id,
                            attr=attr_name,
                            constraint_type=ConstraintType.STRUCTURE.value,
                            description=f"字段 {attr_name} 值 '{value}' 重复，重复的行: {', '.join(other_tuples)}",
                            severity=ViolationSeverity.ERROR.value,
                            
                            suggested_fix=None,  # 唯一性违规通常需要人工处理
                            detector_id=self.mcp_id,
                            timestamp=""
                        )
                        violations.append(violation)
        
        return violations
    
    def _check_primary_key_constraints(self, snapshot: TableSnapshot, 
                                      attr_dict: Dict[str, Dict[str, Any]], 
                                      table_name: str) -> List[Violation]:
        """检查主键约束"""
        violations = []
        
        pk_attrs = [
            attr_name for attr_name, attr_def in attr_dict.items()
            if attr_def.get('primary_key', False)
        ]
        
        if not pk_attrs:
            return violations
        
        pk_values = {}
        
        for row in snapshot.rows:
            pk_value_parts = []
            has_null = False
            
            for pk_attr in pk_attrs:
                if pk_attr in row.cells:
                    value = row.cells[pk_attr].value
                    if self._is_empty_value(value):
                        has_null = True
                        break
                    pk_value_parts.append(str(value))
                else:
                    has_null = True
                    break
            
            if has_null:
                for pk_attr in pk_attrs:
                    if pk_attr in row.cells:
                        value = row.cells[pk_attr].value
                        if self._is_empty_value(value):
                            violation_id = IdGenerator.generate_violation_id(
                                table_name, row.tuple_id, pk_attr, ConstraintType.STRUCTURE.value
                            )
                            
                            violation = Violation(
                                id=violation_id,
                                table=table_name,
                                tuple_id=row.tuple_id,
                                attr=pk_attr,
                                constraint_type=ConstraintType.STRUCTURE.value,
                                description=f"主键字段 {pk_attr} 不能为空",
                                severity=ViolationSeverity.ERROR.value,
                                
                                suggested_fix=None,
                                detector_id=self.mcp_id,
                                timestamp=""
                            )
                            violations.append(violation)
            else:
                pk_value = '|'.join(pk_value_parts)
                if pk_value in pk_values:
                    for pk_attr in pk_attrs:
                        violation_id = IdGenerator.generate_violation_id(
                            table_name, row.tuple_id, pk_attr, ConstraintType.STRUCTURE.value
                        )
                        
                        violation = Violation(
                            id=violation_id,
                            table=table_name,
                            tuple_id=row.tuple_id,
                            attr=pk_attr,
                            constraint_type=ConstraintType.STRUCTURE.value,
                            description=f"主键值重复，与行 {pk_values[pk_value]} 冲突",
                            severity=ViolationSeverity.ERROR.value,
                            
                            suggested_fix=None,
                            detector_id=self.mcp_id,
                            timestamp=""
                        )
                        violations.append(violation)
                else:
                    pk_values[pk_value] = row.tuple_id
        
        return violations
    
    def _is_empty_value(self, value: Any) -> bool:
        """检查值是否为空"""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == '':
            return True
        return False
    
    def _suggest_default_value(self, attr: str, attr_def: Dict[str, Any]) -> str:
        """建议默认值"""
        attr_type = attr_def.get('type', 'string').lower()
        attr_name = attr.lower()
        
        if 'status' in attr_name:
            return '待处理'
        elif 'date' in attr_name or 'time' in attr_name:
            return datetime.now().strftime('%Y-%m-%d')
        elif 'amount' in attr_name or 'price' in attr_name or 'cost' in attr_name:
            return '0.00'
        elif 'quantity' in attr_name or 'count' in attr_name:
            return '1'
        elif 'name' in attr_name:
            return '未命名'
        elif 'description' in attr_name or 'remark' in attr_name:
            return '无'
        
        if attr_type in ['integer', 'int', 'number']:
            return '0'
        elif attr_type in ['float', 'decimal']:
            return '0.0'
        elif attr_type in ['boolean', 'bool']:
            return 'false'
        elif attr_type in ['date']:
            return datetime.now().strftime('%Y-%m-%d')
        elif attr_type in ['datetime', 'timestamp']:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            return ''


class StructureFixer(MCPFixer):
    """结构约束修复器 - 使用LLM智能修复主键冲突和结构问题"""
    
    def __init__(self):
        super().__init__("StructureFixer.v2")  # 版本升级到v2，表示支持LLM
        self.enable_llm_fixing = True  # 启用LLM修复
    
    def _is_empty_value(self, value: Any) -> bool:
        """检查值是否为空"""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == '':
            return True
        return False
    
    def get_supported_constraints(self) -> List[str]:
        return [
            ConstraintType.STRUCTURE.value,
        ]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type in self.get_supported_constraints()
    
    def can_fix(self, violation: Violation) -> bool:
        """
        现在可以修复所有结构性违规，包括主键冲突和唯一性违规
        通过LLM智能分析和合并重复记录
        """
        if violation.constraint_type != ConstraintType.STRUCTURE.value:
            return False
        
        if self.enable_llm_fixing:
            return True
        
        return 'unique' not in violation.description.lower() and '重复' not in violation.description.lower()
    
    def get_supported_fix_types(self) -> List[str]:
        return [
            FixType.DEFAULT_VALUE.value,
            FixType.STRUCTURE_FIX.value
        ]
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, 
            context=None) -> List[Fix]:
        """
        单个违规修复（保留向后兼容）
        对于主键冲突等复杂问题，推荐使用 fix_batch 批量修复
        """
        if not self.can_fix(violation):
            return []
        
        fixes = []
        
        if '缺失' in violation.description or '空值' in violation.description:
            if violation.suggested_fix and violation.suggested_fix.value:
                suggested_value = violation.suggested_fix.value
                if is_valid_fix_value(suggested_value):
                    new_value = suggested_value
                else:
                    self.logger.warning(f"建议修复值'{suggested_value}'看起来是提示文本而非数据值，跳过修复")
                    return fixes
                
                fix_id = IdGenerator.generate_fix_id(
                    violation.table, violation.tuple_id, violation.attr,
                    FixType.DEFAULT_VALUE.value, None
                )
                
                fix = Fix(
                    id=fix_id,
                    table=violation.table,
                    tuple_id=violation.tuple_id,
                    attr=violation.attr,
                    old=None,
                    new=new_value,
                    fix_type=FixType.DEFAULT_VALUE.value,
                    applied_by=self.mcp_id,
                    timestamp="",
                )
                fixes.append(fix)
        
        return fixes
    
    def fix_batch(self, violations: List[Violation], snapshot: TableSnapshot, 
                  context=None) -> List[Fix]:
        """
        批量修复结构性违规 - 使用LLM智能处理主键冲突和重复记录
        
        这个方法会：
        1. 分析所有结构性违规
        2. 识别重复记录组
        3. 使用LLM智能决定如何合并/删除/更新记录
        4. 生成修复操作
        """
        if not violations or not self.enable_llm_fixing:
            all_fixes = []
            for violation in violations:
                fixes = self.fix(violation, snapshot, context)
                all_fixes.extend(fixes)
            return all_fixes
        
        
        try:
            primary_key_violations = [
                v for v in violations 
                if '主键' in v.description and ('重复' in v.description or '冲突' in v.description)
            ]
            other_violations = [v for v in violations if v not in primary_key_violations]
            
            
            all_fixes = []
            
            if primary_key_violations:
                llm_fixes = self._llm_fix_structure_violations(
                    primary_key_violations, snapshot, context
                )
                all_fixes.extend(llm_fixes)
            
            for violation in other_violations:
                simple_fixes = self.fix(violation, snapshot, context)
                all_fixes.extend(simple_fixes)
            
            
            return all_fixes
            
        except Exception as e:
            self.logger.error(f"LLM批量修复失败: {e}")
            import traceback
            traceback.print_exc()
            
            all_fixes = []
            for violation in violations:
                fixes = self.fix(violation, snapshot, context)
                all_fixes.extend(fixes)
            return all_fixes
    
    def _llm_fix_structure_violations(self, violations: List[Violation], 
                                     snapshot: TableSnapshot, 
                                     context=None) -> List[Fix]:
        """使用LLM修复结构性违规（主要是主键冲突和重复记录）"""
        fixes = []
        
        try:
            self.logger.info(f"[StructureFixer] 修复 {len(violations)} 个结构违规，表={snapshot.table}, 行数={len(snapshot.rows) if snapshot.rows else 0}")
            
            schema_def = self._build_schema_definition(snapshot, context)
            
            violations_info = self._build_violations_info(violations)
            
            table_snapshot_str = self._build_table_snapshot(snapshot)
            
            duplicate_groups = self._identify_duplicate_groups(violations, snapshot)
            
            model = context.model if context and hasattr(context, 'model') else "gpt-4o"
            baml_options = get_baml_options(model)
            
            structure_fix: StructureFix = baml_client.FixStructureViolations(
                table_name=snapshot.table,
                schema_definition=schema_def,
                violations_info=violations_info,
                table_snapshot=table_snapshot_str,
                duplicate_groups=duplicate_groups,
                baml_options=baml_options
            )
            
            fixes = self._convert_structure_fix_to_fixes(
                structure_fix, violations, snapshot, context
            )
            
            if len(fixes) > 0:
                self.logger.info(f"[StructureFixer] 生成 {len(fixes)} 个修复")
            else:
                self.logger.warning(f"[StructureFixer] 未生成修复（操作数={len(structure_fix.operations) if structure_fix.operations else 0}）")
            
            return fixes
            
        except Exception as e:
            self.logger.error(f"LLM结构修复失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _build_schema_definition(self, snapshot: TableSnapshot, context=None) -> str:
        """构建schema定义字符串"""
        if not context or not hasattr(context, 'schema'):
            return f"表名: {snapshot.table}"
        
        schema = context.schema
        table_def = get_table_definition(schema, snapshot.table)
        
        if not table_def:
            return f"表名: {snapshot.table}"
        
        lines = [f"表名: {snapshot.table}"]
        lines.append("\n字段定义:")
        
        attributes = table_def.get('attributes', [])
        for attr in attributes:
            attr_name = attr.get('name')
            attr_type = attr.get('type', 'string')
            is_pk = attr.get('primary_key', False)
            is_unique = attr.get('unique', False)
            is_required = attr.get('required', False) or attr.get('not_null', False)
            
            flags = []
            if is_pk:
                flags.append('主键')
            if is_unique:
                flags.append('唯一')
            if is_required:
                flags.append('必填')
            
            flag_str = f" ({', '.join(flags)})" if flags else ""
            lines.append(f"  - {attr_name}: {attr_type}{flag_str}")
        
        return "\n".join(lines)
    
    def _build_violations_info(self, violations: List[Violation]) -> str:
        """构建违规信息字符串"""
        lines = []
        for i, v in enumerate(violations, 1):
            lines.append(f"{i}. 行ID: {v.tuple_id}")
            lines.append(f"   字段: {v.attr}")
            lines.append(f"   问题: {v.description}")
            lines.append(f"   严重程度: {v.severity}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _build_table_snapshot(self, snapshot: TableSnapshot) -> str:
        """构建表格快照字符串（用于LLM分析）"""
        lines = []
        
        if snapshot.rows:
            headers = list(snapshot.rows[0].cells.keys())
            lines.append("| " + " | ".join(headers) + " | 行ID |")
            lines.append("| " + " | ".join(["---"] * (len(headers) + 1)) + " |")
            
            for row in snapshot.rows:
                values = []
                for h in headers:
                    cell = row.cells.get(h)
                    value = str(cell.value) if cell else "NULL"
                    values.append(value)
                values.append(row.tuple_id)
                lines.append("| " + " | ".join(values) + " |")
        
        return "\n".join(lines)
    
    def _identify_duplicate_groups(self, violations: List[Violation], 
                                   snapshot: TableSnapshot) -> str:
        """识别重复记录组"""
        duplicate_map = {}  # key: 主键值, value: [tuple_ids]
        
        for v in violations:
            if '主键' in v.description and '冲突' in v.description:
                import re
                match = re.search(r'与行\s+(t-[^\s]+)\s+冲突', v.description)
                if match:
                    conflicting_tuple = match.group(1)
                    
                    pk_value = self._get_primary_key_value(v.tuple_id, snapshot)
                    
                    if pk_value not in duplicate_map:
                        duplicate_map[pk_value] = []
                    
                    if v.tuple_id not in duplicate_map[pk_value]:
                        duplicate_map[pk_value].append(v.tuple_id)
                    if conflicting_tuple not in duplicate_map[pk_value]:
                        duplicate_map[pk_value].append(conflicting_tuple)
        
        lines = []
        for pk_value, tuple_ids in duplicate_map.items():
            lines.append(f"主键值: {pk_value}")
            lines.append(f"重复的行ID: {', '.join(tuple_ids)}")
            lines.append(f"重复次数: {len(tuple_ids)}")
            
            for tuple_id in tuple_ids:
                row = self._get_row_by_tuple_id(tuple_id, snapshot)
                if row:
                    row_data = {k: str(v.value) for k, v in row.cells.items()}
                    lines.append(f"  - {tuple_id}: {json.dumps(row_data, ensure_ascii=False)}")
            
            lines.append("")
        
        return "\n".join(lines) if lines else "无重复记录组"
    
    def _get_primary_key_field(self, snapshot: TableSnapshot, context=None) -> Optional[str]:
        """获取主键字段名"""
        schema = None
        
        if context:
            if isinstance(context, dict):
                schema = context.get('schema')
                if not schema and 'processing_context' in context:
                    processing_context = context['processing_context']
                    if hasattr(processing_context, 'schema'):
                        schema = processing_context.schema
            else:
                if hasattr(context, 'schema'):
                    schema = context.schema
        
        if not schema and hasattr(snapshot, 'schema'):
            schema = snapshot.schema
        
        if not schema:
            return None
        
        if isinstance(schema, dict):
            if 'tables' in schema:
                table_def = get_table_definition(schema, snapshot.table)
                if table_def:
                    attributes = table_def.get('attributes', [])
                    for attr in attributes:
                        if isinstance(attr, dict):
                            constraints = attr.get('constraints', {})
                            if constraints.get('primary_key', False):
                                return attr.get('name')
            else:
                attributes = schema.get('attributes', [])
                for attr in attributes:
                    if isinstance(attr, dict):
                        constraints = attr.get('constraints', {})
                        if constraints.get('primary_key', False):
                            return attr.get('name')
        
        return None
    
    def _get_primary_key_value(self, tuple_id: str, snapshot: TableSnapshot) -> str:
        """获取主键值"""
        row = self._get_row_by_tuple_id(tuple_id, snapshot)
        if not row:
            return "unknown"
        
        first_cell = list(row.cells.values())[0] if row.cells else None
        return str(first_cell.value) if first_cell else "unknown"
    
    def _get_row_by_tuple_id(self, tuple_id: str, snapshot: TableSnapshot) -> Optional[TableRow]:
        """根据tuple_id获取行"""
        for row in snapshot.rows:
            if row.tuple_id == tuple_id:
                return row
        return None
    
    def _mark_violations_unfixable(self, violations: List[Violation], context):
        """标记违规列表为unfixable"""
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
            
            if memory_manager and run_id and violations:
                import asyncio
                for violation in violations:
                    try:
                        try:
                            loop = asyncio.get_running_loop()
                            asyncio.create_task(
                                memory_manager.mark_violation_unfixable(violation.id, run_id)
                            )
                        except RuntimeError:
                            asyncio.run(memory_manager.mark_violation_unfixable(violation.id, run_id))
                        self.logger.info(f"✅ 标记违规 {violation.id} 为unfixable（安全检查拒绝）")
                    except Exception as mark_error:
                        self.logger.error(f"标记违规unfixable失败: {mark_error}")
            else:
                self.logger.debug(f"⚠️ 无法标记违规为unfixable: context缺少memory_manager或run_id")
        except Exception as e:
            self.logger.error(f"❌ 标记违规unfixable失败: {e}")
    
    def _are_rows_identical(self, row1: TableRow, row2: TableRow) -> bool:
        """检查两行是否完全相同"""
        if not row1 or not row2:
            return False
        
        all_fields = set(row1.cells.keys()) | set(row2.cells.keys())
        
        for field in all_fields:
            val1 = row1.cells.get(field)
            val2 = row2.cells.get(field)
            
            v1 = val1.value if val1 and hasattr(val1, 'value') else (val1 if val1 else None)
            v2 = val2.value if val2 and hasattr(val2, 'value') else (val2 if val2 else None)
            
            v1_str = str(v1).strip() if v1 is not None else ""
            v2_str = str(v2).strip() if v2 is not None else ""
            
            if v1_str != v2_str:
                return False
        
        return True
    
    def _is_row_subset_of(self, subset_row: TableRow, superset_row: TableRow) -> bool:
        """检查subset_row是否是superset_row的子集
        
        子集定义：subset_row的所有非空字段值都在superset_row中存在且相同
        """
        if not subset_row or not superset_row:
            return False
        
        for field, cell in subset_row.cells.items():
            val = cell.value if hasattr(cell, 'value') else cell
            if val is None or (isinstance(val, str) and val.strip() == ""):
                continue
            
            superset_cell = superset_row.cells.get(field)
            if not superset_cell:
                return False
            
            superset_val = superset_cell.value if hasattr(superset_cell, 'value') else superset_cell
            
            val_str = str(val).strip() if val is not None else ""
            superset_val_str = str(superset_val).strip() if superset_val is not None else ""
            
            if val_str != superset_val_str:
                return False
        
        return True
    
    def _can_safely_delete_row(self, row_to_delete: TableRow, snapshot: TableSnapshot, 
                               violations: List[Violation], context=None) -> tuple[bool, str]:
        """检查是否可以安全删除一行
        
        只有在以下情况下才允许删除：
        1. 存在另一行与被删除行完全相同
        2. 被删除行是另一行的子集（即另一行包含更多信息）
        
        Returns:
            (can_delete: bool, reason: str)
        """
        if not row_to_delete:
            return False, "行不存在"
        
        pk_field = self._get_primary_key_field(snapshot, context)
        if not pk_field:
            return False, "无法确定主键字段，保守策略：不允许删除"
        
        delete_pk_cell = row_to_delete.cells.get(pk_field)
        if not delete_pk_cell:
            return False, "被删除行没有主键值"
        
        delete_pk_value = delete_pk_cell.value if hasattr(delete_pk_cell, 'value') else delete_pk_cell
        
        conflicting_rows = []
        for row in snapshot.rows:
            if row.tuple_id == row_to_delete.tuple_id:
                continue  # 跳过自己
            
            row_pk_cell = row.cells.get(pk_field)
            if not row_pk_cell:
                continue
            
            row_pk_value = row_pk_cell.value if hasattr(row_pk_cell, 'value') else row_pk_cell
            
            if str(row_pk_value).strip() == str(delete_pk_value).strip():
                conflicting_rows.append(row)
        
        if not conflicting_rows:
            return False, f"没有找到主键值相同的其他行（主键值: {delete_pk_value}），不允许删除"
        
        for conflicting_row in conflicting_rows:
            if self._are_rows_identical(row_to_delete, conflicting_row):
                self.logger.info(f"✅ [安全删除检查] 行 {row_to_delete.tuple_id} 与行 {conflicting_row.tuple_id} 完全相同，允许删除")
                return True, f"与行 {conflicting_row.tuple_id} 完全相同"
            
            if self._is_row_subset_of(row_to_delete, conflicting_row):
                self.logger.info(f"✅ [安全删除检查] 行 {row_to_delete.tuple_id} 是行 {conflicting_row.tuple_id} 的子集，允许删除")
                return True, f"是行 {conflicting_row.tuple_id} 的子集"
        
        for conflicting_row in conflicting_rows:
            if self._is_row_subset_of(conflicting_row, row_to_delete):
                self.logger.warning(f"⚠️  [安全删除检查] 行 {conflicting_row.tuple_id} 是行 {row_to_delete.tuple_id} 的子集，应该删除冲突行而不是被删除行")
                return False, f"冲突行 {conflicting_row.tuple_id} 是被删除行的子集，应该删除冲突行"
        
        conflicting_tuple_ids = [r.tuple_id for r in conflicting_rows]
        self.logger.warning(f"❌ [安全删除检查] 行 {row_to_delete.tuple_id} 与冲突行 {conflicting_tuple_ids} 既不完全相同，也不是子集关系，不允许删除")
        return False, f"与冲突行 {conflicting_tuple_ids} 既不完全相同，也不是子集关系"
    
    def _convert_structure_fix_to_fixes(self, structure_fix: StructureFix, 
                                       violations: List[Violation],
                                       snapshot: TableSnapshot, 
                                       context=None) -> List[Fix]:
        """将LLM的StructureFix转换为Fix对象列表"""
        fixes = []
        
        deletion_count = sum(1 for op in structure_fix.operations or [] if op.operation_type == "delete")
        total_rows = len(snapshot.rows) if snapshot.rows else 0
        
        if deletion_count >= total_rows and total_rows > 0:
            self.logger.error(f"⚠️  [StructureFixer SAFETY CHECK] 企图删除所有 {total_rows} 行数据！")
            self.logger.error(f"⚠️  [StructureFixer SAFETY CHECK] 这可能是LLM错误决策，拒绝执行")
            self.logger.error(f"⚠️  [StructureFixer SAFETY CHECK] 删除操作数: {deletion_count}/{total_rows}")
            
            self.logger.error(f"⚠️  [StructureFixer SAFETY CHECK] 违规列表:")
            for i, v in enumerate(violations, 1):
                self.logger.error(f"      {i}. {v.tuple_id} - {v.attr}: {v.description}")
            
            self.logger.error(f"⚠️  [StructureFixer SAFETY CHECK] LLM返回的操作:")
            for i, op in enumerate(structure_fix.operations or [], 1):
                self.logger.error(f"      {i}. {op.operation_type} - {op.tuple_id}: {op.reason}")
            
            self.logger.error(f"⚠️  [StructureFixer SAFETY CHECK] 为防止数据丢失，拒绝执行所有删除操作")
            
            self._mark_violations_unfixable(violations, context)
            
            return []  # 不执行任何删除
        
        for operation in structure_fix.operations:
            try:
                if operation.operation_type == "delete":
                    row_to_delete = self._get_row_by_tuple_id(operation.tuple_id, snapshot)
                    if not row_to_delete:
                        self.logger.warning(f"⚠️  [删除操作] 行 {operation.tuple_id} 不存在，跳过删除操作")
                        continue
                    
                    can_delete, reason = self._can_safely_delete_row(
                        row_to_delete, snapshot, violations, context
                    )
                    
                    if not can_delete:
                        self.logger.warning(
                            f"❌ [安全删除检查] 拒绝删除行 {operation.tuple_id}: {reason}"
                        )
                        self.logger.warning(
                            f"   LLM删除原因: {operation.reason if hasattr(operation, 'reason') else 'N/A'}"
                        )
                        continue
                    else:
                        self.logger.info(
                            f"✅ [安全删除检查] 允许删除行 {operation.tuple_id}: {reason}"
                        )
                    
                    row = row_to_delete  # 使用已经获取的行
                    if row:
                        pk_field = self._get_primary_key_field(snapshot, context)
                        if pk_field and pk_field in row.cells:
                            old_cell = row.cells.get(pk_field)
                            old_value = old_cell.value if old_cell else None
                            
                            fix_id = IdGenerator.generate_fix_id(
                                snapshot.table, operation.tuple_id, pk_field,
                                FixType.STRUCTURE_FIX.value, old_value
                            )
                            
                            fix = Fix(
                                id=fix_id,
                                table=snapshot.table,
                                tuple_id=operation.tuple_id,
                                attr=pk_field,
                                old=old_value,
                                new="__DELETED__",  # 特殊值标记删除
                                fix_type=FixType.STRUCTURE_FIX.value,
                                applied_by=self.mcp_id,
                                timestamp=datetime.now().isoformat(),
                                fix_success=True,
                                failure_reason=operation.reason
                            )
                            fixes.append(fix)
                        else:
                            if row.cells:
                                first_field = list(row.cells.keys())[0]
                                old_cell = row.cells.get(first_field)
                                old_value = old_cell.value if old_cell else None
                                
                                fix_id = IdGenerator.generate_fix_id(
                                    snapshot.table, operation.tuple_id, first_field,
                                    FixType.STRUCTURE_FIX.value, old_value
                                )
                                
                                fix = Fix(
                                    id=fix_id,
                                    table=snapshot.table,
                                    tuple_id=operation.tuple_id,
                                    attr=first_field,
                                    old=old_value,
                                    new="__DELETED__",
                                    fix_type=FixType.STRUCTURE_FIX.value,
                                    applied_by=self.mcp_id,
                                    timestamp=datetime.now().isoformat(),
                                    fix_success=True,
                                    failure_reason=operation.reason
                                )
                                fixes.append(fix)
                    
                elif operation.operation_type == "update":
                    if operation.changes:
                        update_fixes = self._parse_and_create_updates(
                            operation.tuple_id, operation.changes, 
                            snapshot, operation.reason
                        )
                        fixes.extend(update_fixes)
                
                elif operation.operation_type == "merge":
                    merged_record = next(
                        (mr for mr in structure_fix.merged_records 
                         if operation.tuple_id in mr.merged_from or mr.tuple_id == operation.tuple_id),
                        None
                    )
                    if merged_record:
                        merge_fixes = self._create_merge_fixes(
                            merged_record, snapshot, operation.reason
                    )
                    fixes.extend(merge_fixes)
                
                elif operation.operation_type == "keep":
                    continue
                
            except Exception as e:
                self.logger.error(f"转换修复操作失败: {e}")
        
        return fixes
    
    def _parse_and_create_updates(self, tuple_id: str, changes: str, 
                                  snapshot: TableSnapshot, reason: str) -> List[Fix]:
        """解析changes字符串并创建Fix对象"""
        fixes = []
        
        import re
        pattern = r'(\w+):\s*([^-]+?)\s*->\s*([^,]+)'
        matches = re.findall(pattern, changes)
        
        for field_name, old_value, new_value in matches:
            field_name = field_name.strip()
            old_value = old_value.strip()
            new_value = new_value.strip()
            
            fix_id = IdGenerator.generate_fix_id(
                snapshot.table, tuple_id, field_name,
                FixType.STRUCTURE_FIX.value, old_value
            )
            
            fix = Fix(
                id=fix_id,
                table=snapshot.table,
                tuple_id=tuple_id,
                attr=field_name,
                old=old_value if old_value != "NULL" else None,
                new=new_value if new_value != "NULL" else None,
                fix_type=FixType.STRUCTURE_FIX.value,
                applied_by=self.mcp_id,
                timestamp=datetime.now().isoformat(),
                )
            fixes.append(fix)
        
        return fixes
    
    def _create_merge_fixes(self, merged_record, snapshot: TableSnapshot, 
                           reason: str) -> List[Fix]:
        """为合并操作创建Fix对象"""
        fixes = []
        
        try:
            final_data = json.loads(merged_record.final_data)
            
            target_tuple_id = merged_record.tuple_id
            target_row = self._get_row_by_tuple_id(target_tuple_id, snapshot)
            
            if target_row:
                for field_name, new_value in final_data.items():
                    old_cell = target_row.cells.get(field_name)
                    old_value = old_cell.value if old_cell else None
                    
                    if self._is_empty_value(old_value):
                        continue
                    
                    if str(old_value) != str(new_value):
                        fix_id = IdGenerator.generate_fix_id(
                            snapshot.table, target_tuple_id, field_name,
                            FixType.STRUCTURE_FIX.value, old_value
                        )
                        
                        fix = Fix(
                            id=fix_id,
                            table=snapshot.table,
                            tuple_id=target_tuple_id,
                            attr=field_name,
                            old=old_value,
                            new=new_value,
                            fix_type=FixType.STRUCTURE_FIX.value,
                            applied_by=self.mcp_id,
                            timestamp=datetime.now().isoformat(),
                        )
                        fixes.append(fix)
            
        except Exception as e:
            self.logger.error(f"创建合并Fix失败: {e}")
        
        return fixes


class StructureMCP(BaseMCP):
    """结构约束MCP"""
    
    def __init__(self):
        verifier = StructureVerifier()
        fixer = StructureFixer()
        super().__init__("StructureMCP.v1", verifier, fixer)
