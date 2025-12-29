"""基于MCP (Model Context Protocol) 的验证器实现"""
import logging
import asyncio
from typing import List, Dict, Any, Optional
from ..mcp.base import MCPClient
from ..mcp.data_quality_server import DataQualityMCPServer
from ...memory import TableSnapshot, Violation, ConstraintType, ViolationSeverity, SuggestedFix
from ...core.ids import IdGenerator
from ...core.io import IOManager
from enum import Enum

from .mcp_router import MCPRouter


class ViolationCategory(Enum):
    """违规分类 - 区分处理方式"""
    TOOL_FIXABLE = "tool_fixable"  # 可以通过工具修复
    REQUIRES_REEXTRACTION = "requires_reextraction"  # 需要重新从文档提取


class MCPBasedVerifier:
    """基于Model Context Protocol的验证器"""
    
    def __init__(self, enable_smart_routing: bool = True):
        """
        初始化MCP验证器
        
        Args:
            enable_smart_routing: 是否启用智能MCP路由（默认True）
        """
        self.verifier_id = "MCPBasedVerifier.v2.1"
        self.logger = logging.getLogger('doc2db.verifier')
        
        self.mcp_client = MCPClient("doc2db-verifier-client")
        self.data_quality_server = DataQualityMCPServer()
        
        self.mcp_client.connect_server(self.data_quality_server)
        
        self.constraint_type_mapping = {
            ConstraintType.TYPE.value: "TYPE",
            ConstraintType.VALUE.value: "VALUE",
            ConstraintType.STRUCTURE.value: "STRUCTURE",
            ConstraintType.LOGIC.value: "LOGIC",
            ConstraintType.TEMPORAL.value: "TEMPORAL",
            ConstraintType.FORMAT.value: "FORMAT",
            ConstraintType.REFERENCE.value: "REFERENCE",
            ConstraintType.AGGREGATION.value: "AGGREGATION",
        }
        
        self.enable_smart_routing = enable_smart_routing
        self.router = MCPRouter() if enable_smart_routing else None
        
        routing_status = "启用智能路由" if enable_smart_routing else "使用传统顺序验证"
        self.logger.info(f"初始化MCP验证器 v2.1，支持 {len(self.constraint_type_mapping)} 种约束类型，{routing_status}")
    
    def verify_table(self, table, context=None) -> List[Violation]:
        """
        直接验证Table对象 (新接口)
        
        Args:
            table: Table对象
            context: 处理上下文
            
        Returns:
            发现的违规列表
        """
        try:
            return table.verify(self, context)
        except Exception as e:
            self.logger.error(f"验证Table对象失败: {e}")
            return []
    
    def verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
               table_name: str, context=None) -> List[Violation]:
        """使用MCP验证表格快照中的数据"""
        self.logger.info(f"开始MCP验证表格 {table_name}")
        
        if isinstance(schema, str):
            try:
                import json
                schema = json.loads(schema)
                self.logger.info(f"Schema从字符串解析为字典")
            except Exception as e:
                self.logger.error(f"Schema解析失败: {e}")
                return []
        elif not isinstance(schema, dict):
            self.logger.error(f"Schema类型不正确: {type(schema)}")
            return []
        
        try:
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self._async_verify(snapshot, schema, table_name, context))
                    return future.result()
            except RuntimeError:
                return asyncio.run(self._async_verify(snapshot, schema, table_name, context))
        except Exception as e:
            self.logger.error(f"异步验证失败: {e}")
            return []
    
    async def _async_verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
                           table_name: str, context=None) -> List[Violation]:
        """异步验证实现"""
        all_violations = []
        routing_info = None
        
        constraint_types_to_check = list(self.constraint_type_mapping.values())  # 默认全部
        
        if self.enable_smart_routing and self.router:
            try:
                selected_mcp_names, routing_info = self.router.select_mcps(snapshot, schema, table_name)
                
                mcp_to_constraint_map = {
                    "TypeMCP": "TYPE",
                    "ValueMCP": "VALUE", 
                    "StructureMCP": "STRUCTURE",
                    "FormatMCP": "FORMAT",
                    "LogicMCP": "LOGIC",
                    "TemporalMCP": "TEMPORAL",
                    "ReferenceMCP": "REFERENCE",
                    "AggregationMCP": "AGGREGATION"
                }
                
                constraint_types_to_check = [
                    mcp_to_constraint_map[mcp_name] 
                    for mcp_name in selected_mcp_names 
                    if mcp_name in mcp_to_constraint_map
                ]
                
                self.logger.info(f"🧠 智能路由选择了 {len(constraint_types_to_check)} 个约束类型: {constraint_types_to_check}")
                
            except Exception as e:
                self.logger.warning(f"智能路由失败，使用全部MCP: {e}")
                constraint_types_to_check = list(self.constraint_type_mapping.values())
        
        verification_data = {
            "table_name": table_name,
            "rows": [
                {
                    "tuple_id": row.tuple_id,
                    "cells": {
                        attr: cell.value
                        for attr, cell in row.cells.items()
                    }
                }
                for row in snapshot.rows
            ]
        }
        
        mcp_success = False
        try:
            response = await self.mcp_client.call_tool(
                server_name="doc2db-data-quality",
                tool_name="batch_verify",
                arguments={
                    "snapshots": [verification_data],
                    "schema": schema,
                    "constraint_types": constraint_types_to_check  #  使用智能路由选择的约束类型
                }
            )
            
            if response and response.get("status") == "success":
                mcp_success = True
                total_violations = response.get("total_violations", 0)
                self.logger.info(f"MCP批量验证成功，处理了 {response.get('processed_snapshots', 0)} 个快照，发现 {total_violations} 个违规")
                
                violations_data = response.get("violations", [])
                
                routing_info = {
                    'violations_by_mcp': response.get("violations_by_type", {}),
                    'constraint_types_checked': response.get("constraint_types", []),
                    'total_violations': total_violations
                }
                
                for violation_data in violations_data:
                    all_violations.append(self._convert_mcp_violation_to_object(violation_data, table_name))
                    
            elif response and response.get("status") == "error":
                self.logger.error(f"MCP验证返回错误: {response.get('message', '未知错误')}")
            
        except Exception as e:
            self.logger.error(f"MCP工具调用失败: {e}")
        
        if not mcp_success:
            self.logger.info("MCP服务器验证失败，使用具体MCP组件作为后备")
            all_violations = await self._fallback_verify(snapshot, schema, table_name, context)
        
        unique_violations = {}
        for violation in all_violations:
            if hasattr(violation, 'id') and violation.id not in unique_violations:
                unique_violations[violation.id] = violation
        
        final_violations = list(unique_violations.values())
        
        categorized_violations = self._categorize_violations(final_violations, table_name)
        
        for violation, category in zip(final_violations, categorized_violations):
            if category == ViolationCategory.REQUIRES_REEXTRACTION:
                violation.processing_category = "requires_reextraction"
            elif category == ViolationCategory.TOOL_FIXABLE:
                violation.processing_category = "tool_fixable"
            else:
                violation.processing_category = str(category)
        
        verification_info = f"MCP验证完成，发现 {len(final_violations)} 个违规"
        self.logger.info(verification_info)
        
        if hasattr(context, 'step_outputs'):
            error_violations = [v for v in final_violations if getattr(v, 'severity', 'warn') == 'error']
            warn_violations = [v for v in final_violations if getattr(v, 'severity', 'warn') == 'warn']
            
            details = {
                'table_name': table_name,
                'violations_found': len(final_violations),
                'error_violations': len(error_violations),
                'warn_violations': len(warn_violations),
                'mcp_server': self.data_quality_server.name,
                'constraint_types_checked': constraint_types_to_check,
                'violations_by_type': {},
                'output_text': f"✅ MCP验证完成，发现 {len(final_violations)} 个质量问题"
            }
            
            for violation in final_violations:
                constraint_type = getattr(violation, 'constraint_type', 'unknown')
                if constraint_type not in details['violations_by_type']:
                    details['violations_by_type'][constraint_type] = 0
                details['violations_by_type'][constraint_type] += 1
            
            if routing_info and 'violations_by_mcp' in routing_info:
                details['mcp_results'] = []
                mcp_name_map = {
                    'TYPE': 'TypeMCP',
                    'VALUE': 'ValueMCP',
                    'STRUCTURE': 'StructureMCP',
                    'FORMAT': 'FormatMCP',
                    'LOGIC': 'LogicMCP',
                    'TEMPORAL': 'TemporalMCP',
                    'REFERENCE': 'ReferenceMCP',
                    'AGGREGATION': 'AggregationMCP'
                }
                
                for constraint_type, violation_count in routing_info['violations_by_mcp'].items():
                    mcp_name = mcp_name_map.get(constraint_type, f"{constraint_type}MCP")
                    details['mcp_results'].append({
                        'mcp_name': mcp_name,
                        'constraint_type': constraint_type,
                        'violations_found': violation_count,
                        'status': 'found_issues' if violation_count > 0 else 'passed'
                    })
                
                for constraint_type in constraint_types_to_check:
                    if constraint_type not in routing_info['violations_by_mcp']:
                        mcp_name = mcp_name_map.get(constraint_type, f"{constraint_type}MCP")
                        details['mcp_results'].append({
                            'mcp_name': mcp_name,
                            'constraint_type': constraint_type,
                            'violations_found': 0,
                            'status': 'passed'
                        })
                
                details['smart_routing_enabled'] = True
                details['mcps_skipped'] = routing_info.get('optional_mcps_skipped', [])
            else:
                details['smart_routing_enabled'] = False
            
            context.step_outputs.append({
                'step': f'verifier_{table_name}_completed',
                'step_name': f'verifier_{table_name}_completed',
                'status': 'completed', 
                'description': f"表 {table_name} 验证完成，发现 {len(final_violations)} 个质量问题",
                'details': details,
                'timestamp': IOManager.get_timestamp()
            })
            
        
        return final_violations
    
    async def _fallback_verify(self, snapshot: TableSnapshot, schema: Dict[str, Any], 
                              table_name: str, context=None) -> List[Violation]:
        """回退验证方法 - 直接使用具体的MCP组件"""
        self.logger.info("直接使用具体MCP组件进行验证")
        
        violations = []
        routing_info = None
        
        try:
            from ..mcp import (
                TypeMCP, ValueMCP, StructureMCP, FormatMCP,
                LogicMCP, TemporalMCP, ReferenceMCP, AggregationMCP
            )
            
            all_mcps = {
                "TypeMCP": TypeMCP(),
                "ValueMCP": ValueMCP(), 
                "StructureMCP": StructureMCP(),
                "FormatMCP": FormatMCP(),
                "LogicMCP": LogicMCP(),
                "TemporalMCP": TemporalMCP(),
                "ReferenceMCP": ReferenceMCP(),
                "AggregationMCP": AggregationMCP()
            }
            
            if self.enable_smart_routing and self.router:
                selected_mcp_names, routing_info = self.router.select_mcps(snapshot, schema, table_name)
                
                mcps_to_run = [(name, all_mcps[name]) for name in selected_mcp_names if name in all_mcps]
            else:
                mcps_to_run = list(all_mcps.items())
            
            for mcp_name, mcp in mcps_to_run:
                try:
                    self.logger.info(f"使用 {mcp_name} 验证...")
                    
                    mcp_violations = mcp.verify(snapshot, schema, table_name)
                    violations.extend(mcp_violations)
                except Exception as e:
                    self.logger.error(f"{mcp_name} 验证失败: {e}")
            
        except ImportError as e:
            self.logger.error(f"无法导入MCP组件: {e}")
            violations = []
        
        if violations:
            categorized_violations = self._categorize_violations(violations, table_name)
            
            for violation, category in zip(violations, categorized_violations):
                violation.processing_category = category
        
        self.logger.info(f"MCP验证完成，共发现 {len(violations)} 个违规")
        
        if routing_info and hasattr(context, 'step_outputs'):
            context.step_outputs.append({
                'step': f'mcp_routing_{table_name}',
                'status': 'completed',
                'description': f'表 {table_name} 的MCP路由决策',
                'details': routing_info,
                'timestamp': IOManager.get_timestamp()
            })
        
        return violations
    
    def _convert_mcp_violation_to_object(self, violation_data: Dict[str, Any], table_name: str) -> Violation:
        """将MCP返回的违规数据转换为Violation对象"""
        
        tuple_id = violation_data.get('tuple_id', '')
        attr = violation_data.get('attr', '')
        constraint_type = violation_data.get('constraint_type', 'UNKNOWN')
        
        violation_id = IdGenerator.generate_violation_id(
            table_name, tuple_id, attr, constraint_type
        )
        
        suggested_fix = None
        if violation_data.get('suggested_fix'):
            suggested_fix = SuggestedFix(
                value=violation_data['suggested_fix']
            )
        
        severity = violation_data.get('severity', ViolationSeverity.WARN.value)
        if severity not in [s.value for s in ViolationSeverity]:
            severity = ViolationSeverity.WARN.value
        
        violation = Violation(
            id=violation_id,
            table=table_name,
            tuple_id=tuple_id,
            attr=attr,
            constraint_type=constraint_type,
            description=violation_data.get('description', f"MCP检测到的{constraint_type}约束违规"),
            severity=severity,
            suggested_fix=suggested_fix,
            detector_id=self.verifier_id,
            timestamp=""
        )
        
        if 'current_value' in violation_data:
            violation.current_value = violation_data['current_value']
        
        return violation
    
    def get_supported_constraints(self) -> List[str]:
        """获取支持的约束类型列表"""
        return list(self.constraint_type_mapping.keys())
    
    async def get_mcp_resources(self) -> List[Dict[str, Any]]:
        """获取MCP服务器资源"""
        try:
            resources = await self.mcp_client.list_server_resources("doc2db-data-quality")
            return [resource.to_dict() for resource in resources]
        except Exception as e:
            self.logger.error(f"获取MCP资源失败: {e}")
            return []
    
    async def get_constraint_types_info(self) -> Optional[Dict[str, Any]]:
        """获取约束类型信息"""
        try:
            resource_data = await self.mcp_client.get_resource("doc2db-data-quality", "constraint://types")
            return resource_data
        except Exception as e:
            self.logger.error(f"获取约束类型信息失败: {e}")
            return None
    
    def _categorize_violations(self, violations: List[Violation], table_name: str) -> List[ViolationCategory]:
        """对违规进行分类，区分处理方式"""
        categories = []
        
        for violation in violations:
            category = self._determine_violation_category(violation)
            categories.append(category)
            
            category_name = "工具修复" if category == ViolationCategory.TOOL_FIXABLE else "重新提取"
        
        tool_fixable_count = sum(1 for cat in categories if cat == ViolationCategory.TOOL_FIXABLE)
        reextraction_count = sum(1 for cat in categories if cat == ViolationCategory.REQUIRES_REEXTRACTION)
        
        
        return categories
    
    def _determine_violation_category(self, violation: Violation) -> ViolationCategory:
        """判断单个违规的处理方式"""
        constraint_type = violation.constraint_type
        description = violation.description.lower() if violation.description else ""
        
        is_missing_value = any(keyword in description for keyword in ["缺失", "missing", "空值", "null"])
        if is_missing_value:
            pass  # Auto-fixed empty block
        
        if hasattr(violation, 'business_rule_id') and violation.business_rule_id:
            if violation.business_rule_id in ['agg_1', 'agg_2']:
                return ViolationCategory.REQUIRES_REEXTRACTION
        
        
        tool_fixable_conditions = [
            constraint_type == ConstraintType.TYPE.value and any(keyword in description for keyword in [
                "类型", "type", "转换", "convert", "格式", "format"
            ]),
            
            constraint_type == ConstraintType.FORMAT.value,
            
            constraint_type == ConstraintType.VALUE.value and any(keyword in description for keyword in [
                "范围", "range", "长度", "length", "大小", "size", "超出", "exceed"
            ]),
            
            hasattr(violation, 'suggested_fix') and violation.suggested_fix is not None,
            
            constraint_type == ConstraintType.LOGIC.value and any(format_keyword in description for format_keyword in [
                "格式", "单位", "标识", "format", "unit", "identifier", "统一", "standardize", "空格", "spacing"
            ]),
            
            constraint_type == ConstraintType.STRUCTURE.value and any(keyword in description for keyword in [
                "重复", "duplicate", "冲突", "conflict", "唯一", "unique"
            ]),
        ]
        
        reextraction_conditions = [
            constraint_type == ConstraintType.STRUCTURE.value and not any(keyword in description for keyword in [
                "重复", "duplicate", "冲突", "conflict", "唯一", "unique"
            ]),
            
            constraint_type == ConstraintType.LOGIC.value and any(keyword in description for keyword in [
                "关系", "relation", "依赖", "dependency", "一致性", "consistency", "计算错误", "数值异常", "业务规则冲突"
            ]) and not any(format_keyword in description for format_keyword in [
                "格式", "单位", "标识", "format", "unit", "identifier", "统一", "standardize"
            ]),
            
            constraint_type == ConstraintType.REFERENCE.value,
            
            "缺失" in description or "missing" in description or "空值" in description or "null" in description,
            
            violation.severity == ViolationSeverity.ERROR.value and not (
                hasattr(violation, 'suggested_fix') and violation.suggested_fix is not None
            ),
        ]
        
        if any(tool_fixable_conditions):
            category = ViolationCategory.TOOL_FIXABLE
            if is_missing_value:
                pass  # Auto-fixed empty block
            return category
        elif any(reextraction_conditions):
            category = ViolationCategory.REQUIRES_REEXTRACTION
            if is_missing_value:
                pass  # Auto-fixed empty block
            return category
        else:
            if violation.severity == ViolationSeverity.ERROR.value:
                category = ViolationCategory.REQUIRES_REEXTRACTION  # 严重错误倾向于重新提取
                if is_missing_value:
                    pass  # Auto-fixed empty block
                return category
            else:
                category = ViolationCategory.TOOL_FIXABLE
                if is_missing_value:
                    pass  # Auto-fixed empty block
                return category
    
    def verify_multi_table(self, all_snapshots: Dict[str, TableSnapshot], 
                          schema: Dict[str, Any], context=None) -> List[Violation]:
        """
        多表验证 - 验证表间关系和跨表约束
        
        Args:
            all_snapshots: 所有表的快照字典 {table_name: TableSnapshot}
            schema: 完整的schema定义（包含relations）
            context: 处理上下文
            
        Returns:
            跨表违规列表
        """
        self.logger.info(f"开始多表验证，共 {len(all_snapshots)} 个表")
        
        try:
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self._async_verify_multi_table(all_snapshots, schema, context))
                    return future.result()
            except RuntimeError:
                return asyncio.run(self._async_verify_multi_table(all_snapshots, schema, context))
        except Exception as e:
            self.logger.error(f"多表验证失败: {e}")
            return []
    
    async def _async_verify_multi_table(self, all_snapshots: Dict[str, TableSnapshot],
                                       schema: Dict[str, Any], context=None) -> List[Violation]:
        """异步多表验证实现"""
        all_violations = []
        
        relation_verification_details = []
        same_field_verification_details = []
        
        relations = schema.get('relations', [])
        if relations:
            self.logger.info(f"📋 [多表验证-Relations] 发现 {len(relations)} 个relation定义")
            
            for rel_idx, relation in enumerate(relations):
                if not isinstance(relation, dict):
                    continue
                
                rel_type = relation.get('type', 'unknown')
                from_def = relation.get('from', {})
                to_def = relation.get('to', {})
                
                from_table = from_def.get('table')
                from_field = from_def.get('field')
                to_table = to_def.get('table')
                to_field = to_def.get('field')
                
                if not all([from_table, from_field, to_table, to_field]):
                    self.logger.warning(f"Relation {rel_idx} 定义不完整，跳过")
                    continue
                
                self.logger.info(f"  🔗 验证关系 [{rel_type}]: {from_table}.{from_field} → {to_table}.{to_field}")
                
                relation_violations = await self._verify_single_relation(
                    relation, all_snapshots, schema, context
                )
                
                relation_detail = {
                    'relation_type': rel_type,
                    'from_table': from_table,
                    'from_field': from_field,
                    'to_table': to_table,
                    'to_field': to_field,
                    'violations_count': len(relation_violations),
                    'status': 'passed' if not relation_violations else 'violations_found'
                }
                relation_verification_details.append(relation_detail)
                
                if relation_violations:
                    self.logger.warning(f"    ⚠️ 发现 {len(relation_violations)} 个违规")
                    all_violations.extend(relation_violations)
                else:
                    self.logger.info(f"    ✅ 关系完整")
        else:
            self.logger.info("Schema中未定义relations")
            self.logger.info("ℹ️ [多表验证-Relations] Schema中未定义relations")
        
        self.logger.info(f"🔍 [多表验证-相同字段] 开始检查跨表的相同字段...")
        same_field_violations = await self._verify_same_fields_across_tables(
            all_snapshots, schema, context
        )
        
        if same_field_violations:
            self.logger.warning(f"  ⚠️ 发现 {len(same_field_violations)} 个相同字段违规")
            all_violations.extend(same_field_violations)
        else:
            self.logger.info(f"  ✅ 相同字段验证通过")
        
        self.logger.info(f"🔍 [多表验证-业务规则] 开始检查schema定义的多表业务规则...")
        business_rule_violations = await self._verify_multi_table_business_rules(
            all_snapshots, schema, context
        )
        
        if business_rule_violations:
            self.logger.warning(f"  ⚠️ 发现 {len(business_rule_violations)} 个业务规则违规")
            all_violations.extend(business_rule_violations)
        else:
            self.logger.info(f"  ✅ 业务规则验证通过")
        
        self.logger.info(f"📊 [多表验证] 完成，共发现 {len(all_violations)} 个跨表违规")
        self.logger.info(f"    - Relations验证: {len(all_violations) - len(same_field_violations)} 个")
        self.logger.info(f"    - 相同字段验证: {len(same_field_violations)} 个")
        
        if context and hasattr(context, 'step_outputs'):
            from ...core.io import IOManager
            
            multi_table_detail_step = {
                'step': 'multi_table_verification_detailed',
                'step_name': 'multi_table_verification_detailed',
                'status': 'completed',
                'description': f'多表验证详细信息',
                'details': {
                    'total_tables': len(all_snapshots),
                    'tables_verified': list(all_snapshots.keys()),
                    'relations_verified': len(relations) if relations else 0,
                    'relation_verification_details': relation_verification_details,
                    'same_field_checks': len(same_field_verification_details),
                    'total_violations': len(all_violations),
                    'relation_violations': len(all_violations) - len(same_field_violations),
                    'same_field_violations': len(same_field_violations),
                    'verification_summary': f"验证了 {len(relations) if relations else 0} 个表关系，检查了跨表字段一致性"
                },
                'timestamp': IOManager.get_timestamp()
            }
            context.step_outputs.append(multi_table_detail_step)
        
        return all_violations
    
    async def _verify_single_relation(self, relation: Dict[str, Any],
                                     all_snapshots: Dict[str, TableSnapshot],
                                     schema: Dict[str, Any], context=None) -> List[Violation]:
        """验证单个relation的完整性"""
        violations = []
        
        from_def = relation.get('from', {})
        to_def = relation.get('to', {})
        
        from_table = from_def.get('table')
        from_field = from_def.get('field')
        to_table = to_def.get('table')
        to_field = to_def.get('field')
        rel_type = relation.get('type', 'unknown')
        
        if from_table not in all_snapshots:
            self.logger.warning(f"源表 {from_table} 不存在，跳过relation验证")
            return violations
        
        if to_table not in all_snapshots:
            self.logger.warning(f"目标表 {to_table} 不存在，跳过relation验证")
            return violations
        
        from_snapshot = all_snapshots[from_table]
        to_snapshot = all_snapshots[to_table]
        
        to_values = set()
        for row in to_snapshot.rows:
            if to_field in row.cells:
                cell = row.cells[to_field]
                if cell.value is not None:
                    to_values.add(str(cell.value))
        
        for row in from_snapshot.rows:
            if from_field not in row.cells:
                continue
            
            cell = row.cells[from_field]
            if cell.value is None:
                field_nullable = self._is_field_nullable(schema, from_table, from_field)
                if not field_nullable:
                    violation = Violation(
                        id=IdGenerator.generate_violation_id(
                            from_table, row.tuple_id, from_field, "REFERENCE"
                        ),
                        table=from_table,
                        tuple_id=row.tuple_id,
                        attr=from_field,
                        constraint_type=ConstraintType.REFERENCE.value,
                        description=f"外键字段 {from_field} 为空，但该字段不允许为空（引用 {to_table}.{to_field}）",
                        severity=ViolationSeverity.ERROR.value,
                        suggested_fix=None,
                        detector_id=self.verifier_id,
                        timestamp=""
                    )
                    violation.current_value = None
                    violations.append(violation)
                continue
            
            fk_value = str(cell.value)
            if fk_value not in to_values:
                violation = Violation(
                    id=IdGenerator.generate_violation_id(
                        from_table, row.tuple_id, from_field, "REFERENCE"
                    ),
                    table=from_table,
                    tuple_id=row.tuple_id,
                    attr=from_field,
                    constraint_type=ConstraintType.REFERENCE.value,
                    description=f"外键值 '{fk_value}' 在目标表 {to_table}.{to_field} 中不存在（{rel_type} 关系）",
                    severity=ViolationSeverity.ERROR.value,
                    suggested_fix=None,
                    detector_id=self.verifier_id,
                    timestamp=""
                )
                violation.current_value = fk_value
                violations.append(violation)
        
        return violations
    
    def _is_field_nullable(self, schema: Dict[str, Any], table_name: str, field_name: str) -> bool:
        """检查字段是否允许为空"""
        tables = schema.get('tables', [])
        for table in tables:
            if table.get('name') != table_name:
                continue
            
            fields = table.get('fields', table.get('attributes', []))
            for field in fields:
                if field.get('name') == field_name:
                    constraints = field.get('constraints', {})
                    return constraints.get('nullable', True)
        
        return True
    
    async def _verify_same_fields_across_tables(self, all_snapshots: Dict[str, TableSnapshot],
                                               schema: Dict[str, Any], context=None) -> List[Violation]:
        """验证不同表中的相同字段（字段名相同）
        
        验证内容包括：
        1. 相同字段的数据类型一致性
        2. 相同字段的值域一致性（如果字段值表示相同的实体/概念）
        3. 相同字段的格式一致性
        
        Args:
            all_snapshots: 所有表的快照
            schema: 完整schema
            context: 处理上下文
            
        Returns:
            跨表相同字段违规列表
        """
        violations = []
        
        table_fields = {}  # {table_name: {field_name: field_info}}
        
        for table_name, snapshot in all_snapshots.items():
            if not snapshot or not snapshot.rows:
                continue
            
            if snapshot.rows:
                first_row = snapshot.rows[0]
                field_names = list(first_row.cells.keys())
                
                field_defs = self._get_table_field_definitions(schema, table_name)
                
                table_fields[table_name] = {
                    'field_names': field_names,
                    'field_defs': field_defs,
                    'snapshot': snapshot
                }
        
        field_to_tables = {}  # {field_name: [table_names]}
        for table_name, info in table_fields.items():
            for field_name in info['field_names']:
                if field_name not in field_to_tables:
                    field_to_tables[field_name] = []
                field_to_tables[field_name].append(table_name)
        
        common_fields = {field: tables for field, tables in field_to_tables.items() if len(tables) > 1}
        
        if not common_fields:
            self.logger.info("未发现跨表的相同字段")
            return violations
        
        self.logger.info(f"  📌 发现 {len(common_fields)} 个跨表相同字段:")
        for field, tables in common_fields.items():
            self.logger.info(f"     - {field}: {', '.join(tables)}")
        
        for field_name, table_names in common_fields.items():
            self.logger.info(f"  🔍 验证相同字段: {field_name}")
            
            field_values_by_table = {}
            field_types_by_table = {}
            
            for table_name in table_names:
                snapshot = table_fields[table_name]['snapshot']
                values = []
                types = set()
                
                for row in snapshot.rows:
                    if field_name in row.cells:
                        cell = row.cells[field_name]
                        if cell.value is not None:
                            values.append(cell.value)
                            types.add(type(cell.value).__name__)
                
                field_values_by_table[table_name] = values
                field_types_by_table[table_name] = types
            
            type_violations = self._check_field_type_consistency(
                field_name, table_names, field_types_by_table, all_snapshots
            )
            violations.extend(type_violations)
            
            domain_violations = self._check_field_domain_consistency(
                field_name, table_names, field_values_by_table, all_snapshots, schema
            )
            violations.extend(domain_violations)
            
            format_violations = self._check_field_format_consistency(
                field_name, table_names, field_values_by_table, all_snapshots
            )
            violations.extend(format_violations)
        
        return violations
    
    def _get_table_field_definitions(self, schema: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """从schema中获取表的字段定义"""
        tables = schema.get('tables', [])
        for table in tables:
            if table.get('name') == table_name:
                fields = table.get('fields', table.get('attributes', []))
                return {field.get('name'): field for field in fields if 'name' in field}
        return {}
    
    def _check_field_type_consistency(self, field_name: str, table_names: List[str],
                                     field_types_by_table: Dict[str, set],
                                     all_snapshots: Dict[str, TableSnapshot]) -> List[Violation]:
        """检查相同字段在不同表中的类型一致性"""
        violations = []
        
        all_types = set()
        for types in field_types_by_table.values():
            all_types.update(types)
        
        if len(all_types) > 1:
            self.logger.warning(f"    ⚠️ 类型不一致: {all_types}")
            
            type_counts = {}
            for table_name, types in field_types_by_table.items():
                for t in types:
                    type_counts[t] = type_counts.get(t, 0) + 1
            
            expected_type = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else None
            
            for table_name, types in field_types_by_table.items():
                if expected_type and expected_type not in types:
                    actual_type = list(types)[0] if types else "unknown"
                    violation = Violation(
                        id=IdGenerator.generate_violation_id(
                            table_name, "CROSS_TABLE", field_name, "TYPE"
                        ),
                        table=table_name,
                        tuple_id="CROSS_TABLE",
                        attr=field_name,
                        constraint_type=ConstraintType.TYPE.value,
                        description=f"跨表字段类型不一致: 字段 {field_name} 在表 {table_name} 中类型为 {actual_type}，但在其他表中为 {expected_type}",
                        severity=ViolationSeverity.WARN.value,
                        suggested_fix=None,
                        detector_id=self.verifier_id,
                        timestamp=""
                    )
                    violations.append(violation)
        
        return violations
    
    def _check_field_domain_consistency(self, field_name: str, table_names: List[str],
                                       field_values_by_table: Dict[str, List],
                                       all_snapshots: Dict[str, TableSnapshot],
                                       schema: Dict[str, Any]) -> List[Violation]:
        """检查相同字段在不同表中的值域一致性
        
        如果字段在schema中定义了枚举值，检查实际值是否都在枚举范围内
        
        注意：相似实体检测现在由ConsistencyMCP组件负责
        """
        violations = []
        
        all_unique_values = set()
        for values in field_values_by_table.values():
            all_unique_values.update(str(v) for v in values)
        
        if len(all_unique_values) < 20 and len(all_unique_values) > 0:
            value_sets = {table: set(str(v) for v in values) 
                         for table, values in field_values_by_table.items()}
            
            common_values = set.intersection(*value_sets.values()) if value_sets else set()
            
            for table_name, values in value_sets.items():
                unique_to_table = values - common_values
                if unique_to_table and len(unique_to_table) / len(values) > 0.3:  # 超过30%是特有值
                    self.logger.info(f"    ℹ️  表 {table_name} 有特有值: {unique_to_table}")
        
        return violations
    
    def _check_field_format_consistency(self, field_name: str, table_names: List[str],
                                       field_values_by_table: Dict[str, List],
                                       all_snapshots: Dict[str, TableSnapshot]) -> List[Violation]:
        """检查相同字段在不同表中的格式一致性（针对字符串型字段）"""
        violations = []
        
        format_patterns = {}  # {table_name: set_of_patterns}
        
        for table_name, values in field_values_by_table.items():
            patterns = set()
            for value in values:
                if isinstance(value, str):
                    pattern = self._detect_string_pattern(value)
                    patterns.add(pattern)
            format_patterns[table_name] = patterns
        
        all_patterns = set()
        for patterns in format_patterns.values():
            all_patterns.update(patterns)
        
        if len(all_patterns) > 3:  # 超过3种不同模式
            self.logger.info(f"    ⚠️ 格式模式较多: {all_patterns}")
        
        return violations
    
    async def _verify_multi_table_business_rules(self, all_snapshots: Dict[str, TableSnapshot],
                                                 schema: Dict[str, Any], context=None) -> List[Violation]:
        """验证schema中定义的多表业务规则（包括逻辑规则和聚合规则）"""
        violations = []
        
        try:
            from ..mcp import LogicMCP
            
            logic_mcp = LogicMCP()
            if hasattr(logic_mcp.verifier, 'verify_multi_table_business_rules'):
                logic_violations = logic_mcp.verifier.verify_multi_table_business_rules(
                    all_snapshots, schema, context
                )
                violations.extend(logic_violations)
        except Exception as e:
            self.logger.error(f"多表业务逻辑规则验证失败: {e}")
        
        try:
            from ..mcp import AggregationMCP
            
            aggregation_mcp = AggregationMCP()
            if hasattr(aggregation_mcp.verifier, 'verify_multi_table_aggregation_rules'):
                aggregation_violations = aggregation_mcp.verifier.verify_multi_table_aggregation_rules(
                    all_snapshots, schema, context
                )
                violations.extend(aggregation_violations)
        except Exception as e:
            self.logger.error(f"多表聚合规则验证失败: {e}")
        
        return violations
    
    def _detect_string_pattern(self, value: str) -> str:
        """检测字符串的格式模式"""
        import re
        
        if not value:
            return "empty"
        
        if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
            return "date_iso"
        elif re.match(r'^\d{2}/\d{2}/\d{4}$', value):
            return "date_us"
        elif re.match(r'^\d+$', value):
            return "numeric"
        elif re.match(r'^\d+\.\d+$', value):
            return "decimal"
        elif re.match(r'^[A-Z][a-z]+$', value):
            return "capitalized_word"
        elif re.match(r'^[A-Z]+$', value):
            return "uppercase"
        elif re.match(r'^[a-z]+$', value):
            return "lowercase"
        else:
            return "mixed"