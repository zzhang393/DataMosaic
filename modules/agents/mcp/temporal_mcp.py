"""时间约束MCP - 验证和修复时间相关约束问题"""
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from .base import MCPVerifier, MCPFixer, BaseMCP, get_table_definition, get_cell_value, is_valid_fix_value
from ...memory import TableSnapshot, Violation, Fix, SuggestedFix, ConstraintType, ViolationSeverity, FixType
from ...core.ids import IdGenerator


class TemporalVerifier(MCPVerifier):
    """时间约束验证器"""
    
    def __init__(self):
        super().__init__("TemporalVerifier.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.TEMPORAL.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.TEMPORAL.value
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """验证表格中的时间约束（单元格级别+表级）"""
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
        
        row_violations = self._verify_row_temporal_constraints(
            snapshot, table_def, table_name
        )
        violations.extend(row_violations)
        
        table_violations = self._verify_table_temporal_constraints(
            snapshot, table_def, table_name
        )
        violations.extend(table_violations)
        
        return violations
    
    def verify_cell(self, table: str, tuple_id: str, attr: str, 
                   value: Any, attr_def: Dict[str, Any], 
                   snapshot: TableSnapshot, context=None) -> List[Violation]:
        """验证单个单元格的时间约束"""
        violations = []
        
        if value is None or str(value).strip() == '':
            return violations
        
        format_violations = self._check_temporal_format(
            table, tuple_id, attr, value, attr_def
        )
        violations.extend(format_violations)
        
        range_violations = self._check_temporal_range(
            table, tuple_id, attr, value, attr_def
        )
        violations.extend(range_violations)
        
        return violations
    
    def _verify_row_temporal_constraints(self, snapshot: TableSnapshot, 
                                        table_def: Dict[str, Any], 
                                        table_name: str) -> List[Violation]:
        """验证行内时间约束"""
        violations = []
        attributes = table_def.get('attributes', [])
        
        for row in snapshot.rows:
            temporal_violations = self._check_temporal_logic_within_row(
                row, attributes, table_name
            )
            violations.extend(temporal_violations)
        
        return violations
    
    def _verify_table_temporal_constraints(self, snapshot: TableSnapshot, 
                                          table_def: Dict[str, Any], 
                                          table_name: str) -> List[Violation]:
        """验证表级时间约束"""
        violations = []
        
        sequence_violations = self._check_temporal_sequence(
            snapshot, table_def, table_name
        )
        violations.extend(sequence_violations)
        
        return violations
    
    def _check_temporal_format(self, table: str, tuple_id: str, attr: str, 
                              value: Any, attr_def: Dict[str, Any]) -> List[Violation]:
        """检查时间格式有效性"""
        violations = []
        
        if not self._is_temporal_field(attr, attr_def):
            return violations
        
        str_value = str(value).strip()
        
        parsed_date = self._parse_date(str_value)
        if parsed_date is None:
            violation_id = IdGenerator.generate_violation_id(
                table, tuple_id, attr, ConstraintType.TEMPORAL.value
            )
            
            suggested_value = self._suggest_date_fix(str_value)
            
            violation = Violation(
                id=violation_id,
                table=table,
                tuple_id=tuple_id,
                attr=attr,
                constraint_type=ConstraintType.TEMPORAL.value,
                description=f"字段 {attr} 值 '{value}' 不是有效的时间格式",
                severity=ViolationSeverity.WARN.value,
                
                suggested_fix=SuggestedFix(value=suggested_value, ),
                detector_id=self.mcp_id,
                timestamp=""
            )
            violations.append(violation)
        
        return violations
    
    def _check_temporal_range(self, table: str, tuple_id: str, attr: str, 
                             value: Any, attr_def: Dict[str, Any]) -> List[Violation]:
        """检查时间范围约束"""
        violations = []
        
        if not self._is_temporal_field(attr, attr_def):
            return violations
        
        str_value = str(value).strip()
        parsed_date = self._parse_date(str_value)
        
        if parsed_date is None:
            return violations
        
        min_date = attr_def.get('min_date')
        if min_date:
            min_parsed = self._parse_date(str(min_date))
            if min_parsed and parsed_date < min_parsed:
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.TEMPORAL.value
                )
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.TEMPORAL.value,
                    description=f"字段 {attr} 时间 {value} 早于最小时间 {min_date}",
                    severity=ViolationSeverity.WARN.value,
                    
                    suggested_fix=SuggestedFix(value=str(min_date), ),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        max_date = attr_def.get('max_date')
        if max_date:
            max_parsed = self._parse_date(str(max_date))
            if max_parsed and parsed_date > max_parsed:
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.TEMPORAL.value
                )
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.TEMPORAL.value,
                    description=f"字段 {attr} 时间 {value} 晚于最大时间 {max_date}",
                    severity=ViolationSeverity.WARN.value,
                    
                    suggested_fix=SuggestedFix(value=str(max_date), ),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        if attr_def.get('no_future', False):
            now = datetime.now()
            if parsed_date > now:
                violation_id = IdGenerator.generate_violation_id(
                    table, tuple_id, attr, ConstraintType.TEMPORAL.value
                )
                
                violation = Violation(
                    id=violation_id,
                    table=table,
                    tuple_id=tuple_id,
                    attr=attr,
                    constraint_type=ConstraintType.TEMPORAL.value,
                    description=f"字段 {attr} 不能是未来时间 {value}",
                    severity=ViolationSeverity.WARN.value,
                    
                    suggested_fix=SuggestedFix(value=now.strftime('%Y-%m-%d'), ),
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations
    
    def _check_temporal_logic_within_row(self, row, attributes: List[Dict[str, Any]], 
                                        table_name: str) -> List[Violation]:
        """检查行内时间字段之间的逻辑关系"""
        violations = []
        
        temporal_fields = {}
        for attr_def in attributes:
            attr_name = attr_def['name']
            if attr_name in row.cells and self._is_temporal_field(attr_name, attr_def):
                value = row.cells[attr_name].value
                if value:
                    parsed_date = self._parse_date(str(value))
                    if parsed_date:
                        temporal_fields[attr_name] = {
                            'value': value,
                            'parsed': parsed_date,
                            'attr_def': attr_def
                        }
        
        violations.extend(self._check_start_end_logic(row.tuple_id, temporal_fields, table_name))
        violations.extend(self._check_create_update_logic(row.tuple_id, temporal_fields, table_name))
        violations.extend(self._check_order_delivery_logic(row.tuple_id, temporal_fields, table_name))
        
        return violations
    
    def _check_start_end_logic(self, tuple_id: str, temporal_fields: Dict[str, Any], 
                              table_name: str) -> List[Violation]:
        """检查开始时间和结束时间的逻辑关系"""
        violations = []
        
        start_fields = [name for name in temporal_fields.keys() 
                       if 'start' in name.lower() or 'begin' in name.lower()]
        end_fields = [name for name in temporal_fields.keys() 
                     if 'end' in name.lower() or 'finish' in name.lower() or 'complete' in name.lower()]
        
        for start_field in start_fields:
            for end_field in end_fields:
                start_date = temporal_fields[start_field]['parsed']
                end_date = temporal_fields[end_field]['parsed']
                
                if start_date > end_date:
                    violation_id = IdGenerator.generate_violation_id(
                        table_name, tuple_id, end_field, ConstraintType.TEMPORAL.value
                    )
                    
                    violation = Violation(
                        id=violation_id,
                        table=table_name,
                        tuple_id=tuple_id,
                        attr=end_field,
                        constraint_type=ConstraintType.TEMPORAL.value,
                        description=f"结束时间 {end_field} ({temporal_fields[end_field]['value']}) 不能早于开始时间 {start_field} ({temporal_fields[start_field]['value']})",
                        severity=ViolationSeverity.ERROR.value,
                        
                        suggested_fix=SuggestedFix(
                            value=(start_date + timedelta(days=1)).strftime('%Y-%m-%d'), 
                            
                        ),
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violations.append(violation)
        
        return violations
    
    def _check_create_update_logic(self, tuple_id: str, temporal_fields: Dict[str, Any], 
                                  table_name: str) -> List[Violation]:
        """检查创建时间和更新时间的逻辑关系"""
        violations = []
        
        create_fields = [name for name in temporal_fields.keys() 
                        if 'create' in name.lower() or 'add' in name.lower()]
        update_fields = [name for name in temporal_fields.keys() 
                        if 'update' in name.lower() or 'modify' in name.lower()]
        
        for create_field in create_fields:
            for update_field in update_fields:
                create_date = temporal_fields[create_field]['parsed']
                update_date = temporal_fields[update_field]['parsed']
                
                if create_date > update_date:
                    violation_id = IdGenerator.generate_violation_id(
                        table_name, tuple_id, update_field, ConstraintType.TEMPORAL.value
                    )
                    
                    violation = Violation(
                        id=violation_id,
                        table=table_name,
                        tuple_id=tuple_id,
                        attr=update_field,
                        constraint_type=ConstraintType.TEMPORAL.value,
                        description=f"更新时间 {update_field} ({temporal_fields[update_field]['value']}) 不能早于创建时间 {create_field} ({temporal_fields[create_field]['value']})",
                        severity=ViolationSeverity.ERROR.value,
                        
                        suggested_fix=SuggestedFix(
                            value=create_date.strftime('%Y-%m-%d'), 
                            
                        ),
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violations.append(violation)
        
        return violations
    
    def _check_order_delivery_logic(self, tuple_id: str, temporal_fields: Dict[str, Any], 
                                   table_name: str) -> List[Violation]:
        """检查订单时间和交付时间的逻辑关系"""
        violations = []
        
        order_fields = [name for name in temporal_fields.keys() 
                       if 'order' in name.lower()]
        delivery_fields = [name for name in temporal_fields.keys() 
                          if 'delivery' in name.lower() or 'ship' in name.lower()]
        
        for order_field in order_fields:
            for delivery_field in delivery_fields:
                order_date = temporal_fields[order_field]['parsed']
                delivery_date = temporal_fields[delivery_field]['parsed']
                
                if order_date > delivery_date:
                    violation_id = IdGenerator.generate_violation_id(
                        table_name, tuple_id, delivery_field, ConstraintType.TEMPORAL.value
                    )
                    
                    violation = Violation(
                        id=violation_id,
                        table=table_name,
                        tuple_id=tuple_id,
                        attr=delivery_field,
                        constraint_type=ConstraintType.TEMPORAL.value,
                        description=f"交付时间 {delivery_field} ({temporal_fields[delivery_field]['value']}) 不能早于订单时间 {order_field} ({temporal_fields[order_field]['value']})",
                        severity=ViolationSeverity.ERROR.value,
                        
                        suggested_fix=SuggestedFix(
                            value=(order_date + timedelta(days=3)).strftime('%Y-%m-%d'), 
                            
                        ),
                        detector_id=self.mcp_id,
                        timestamp=""
                    )
                    violations.append(violation)
        
        return violations
    
    def _check_temporal_sequence(self, snapshot: TableSnapshot, 
                                table_def: Dict[str, Any], 
                                table_name: str) -> List[Violation]:
        """检查时间序列约束"""
        violations = []
        
        attributes = table_def.get('attributes', [])
        sort_field = None
        
        for attr_def in attributes:
            if (attr_def.get('temporal_sort', False) or 
                'date' in attr_def['name'].lower() or 
                'time' in attr_def['name'].lower()):
                sort_field = attr_def['name']
                break
        
        if not sort_field:
            return violations
        
        dates = []
        for row in snapshot.rows:
            if sort_field in row.cells:
                value = row.cells[sort_field].value
                if value:
                    parsed_date = self._parse_date(str(value))
                    if parsed_date:
                        dates.append({
                            'tuple_id': row.tuple_id,
                            'date': parsed_date,
                            'value': value
                        })
        
        dates.sort(key=lambda x: x['date'])
        for i in range(1, len(dates)):
            prev_date = dates[i-1]['date']
            curr_date = dates[i]['date']
            
            if (curr_date - prev_date).days > 365:
                violation_id = IdGenerator.generate_violation_id(
                    table_name, dates[i]['tuple_id'], sort_field, ConstraintType.TEMPORAL.value
                )
                
                violation = Violation(
                    id=violation_id,
                    table=table_name,
                    tuple_id=dates[i]['tuple_id'],
                    attr=sort_field,
                    constraint_type=ConstraintType.TEMPORAL.value,
                    description=f"时间序列异常：{sort_field} 值 {dates[i]['value']} 与前一条记录时间间隔过大",
                    severity=ViolationSeverity.INFO.value,
                    
                    suggested_fix=None,
                    detector_id=self.mcp_id,
                    timestamp=""
                )
                violations.append(violation)
        
        return violations
    
    def _is_temporal_field(self, attr_name: str, attr_def: Dict[str, Any]) -> bool:
        """判断是否为时间字段
        
        优先级：
        1. 完全依赖schema中的type定义（推荐方式）
        2. 只有当type未定义时，才使用字段名启发式判断（fallback）
        """
        field_type = attr_def.get('type', '').strip()
        
        if field_type:
            field_type_lower = field_type.lower()
            
            if any(t in field_type_lower for t in ['date', 'time', 'timestamp']):
                return True
            
            return False
        
        self.logger.warning(
            f"字段 {attr_name} 没有类型定义，使用字段名启发式判断（不推荐）"
        )
        
        import re
        
        separated = re.sub(r'(?<!^)(?=[A-Z])', '_', attr_name)
        words = [word.lower() for word in separated.split('_') if word]
        
        temporal_keywords = ['date', 'time', 'created', 'updated', 'start', 'end', 'begin', 'finish', 'at', 'on']
        
        return any(word in temporal_keywords for word in words)
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """解析日期字符串"""
        date_formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%Y%m%d',
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        return None
    
    def _suggest_date_fix(self, date_str: str) -> str:
        """建议日期修复值"""
        if re.match(r'\d{4}/\d{2}/\d{2}', date_str):
            return date_str.replace('/', '-')
        elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
            parts = date_str.split('/')
            return f"20{parts[2]}-{parts[0]}-{parts[1]}"
        elif re.match(r'\d{4}\d{2}\d{2}', date_str):
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        return datetime.now().strftime('%Y-%m-%d')


class TemporalFixer(MCPFixer):
    """时间约束修复器"""
    
    def __init__(self):
        super().__init__("TemporalFixer.v1")
    
    def get_supported_constraints(self) -> List[str]:
        return [ConstraintType.TEMPORAL.value]
    
    def can_handle(self, constraint_type: str) -> bool:
        return constraint_type == ConstraintType.TEMPORAL.value
    
    def can_fix(self, violation: Violation) -> bool:
        if '时间序列异常' in violation.description:
            return False
        return violation.constraint_type == ConstraintType.TEMPORAL.value
    
    def get_supported_fix_types(self) -> List[str]:
        return [
            FixType.TEMPORAL_FIX.value,
            FixType.FORMAT_NORMALIZE.value,
            FixType.RANGE_CLAMP.value
        ]
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, 
            context=None) -> List[Fix]:
        """修复时间约束违规"""
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
            
            fix_type = self._determine_fix_type(violation.description)
            
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
                applied_by=self.mcp_id,
                timestamp="",

            )
            fixes.append(fix)
        
        return fixes
    
    def _determine_fix_type(self, description: str) -> str:
        """确定修复类型"""
        if '格式' in description:
            return FixType.FORMAT_NORMALIZE.value
        elif '早于' in description or '晚于' in description:
            return FixType.RANGE_CLAMP.value
        else:
            return FixType.TEMPORAL_FIX.value


class TemporalMCP(BaseMCP):
    """时间约束MCP"""
    
    def __init__(self):
        verifier = TemporalVerifier()
        fixer = TemporalFixer()
        super().__init__("TemporalMCP.v1", verifier, fixer)
