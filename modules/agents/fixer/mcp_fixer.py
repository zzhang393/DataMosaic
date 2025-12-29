"""基于MCP (Model Context Protocol) 的修复器实现"""
import logging
import asyncio
from typing import List, Dict, Any, Optional
from ..mcp.base import MCPClient
from ..mcp.data_quality_server import DataQualityMCPServer
from ...memory import TableSnapshot, Violation, Fix, ConstraintType


class MCPBasedFixer:
    """基于Model Context Protocol的修复器"""
    
    def __init__(self):
        self.fixer_id = "MCPBasedFixer.v2.0"
        self.logger = logging.getLogger('doc2db.fixer')
        
        self.mcp_client = MCPClient("doc2db-fixer-client")
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
        
        self.logger.info(f"初始化MCP修复器，支持 {len(self.constraint_type_mapping)} 种约束类型")
    
    def fix_table(self, table, violations: List[Violation], context=None) -> List[Fix]:
        """
        直接修复Table对象 (新接口)
        
        Args:
            table: Table对象
            violations: 需要修复的违规列表
            context: 处理上下文
            
        Returns:
            应用的修复列表
        """
        try:
            return table.fix(self, violations, context)
        except Exception as e:
            self.logger.error(f"修复Table对象失败: {e}")
            return []
    
    def fix(self, violation: Violation, snapshot: TableSnapshot, context=None) -> List[Fix]:
        """使用MCP修复违规"""
        if not self.can_fix(violation):
            return []
        
        try:
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self._async_fix(violation, snapshot, context))
                    return future.result()
            except RuntimeError:
                return asyncio.run(self._async_fix(violation, snapshot, context))
        except Exception as e:
            self.logger.error(f"异步修复失败: {e}")
            return []
    
    async def _async_fix(self, violation: Violation, snapshot: TableSnapshot, context=None) -> List[Fix]:
        """异步修复实现"""
        fixes = []
        
        if hasattr(violation, 'business_rule_id') and violation.business_rule_id:
            self.logger.info(f"🎯 [MCPFixer] 检测到业务规则违规: {violation.business_rule_id}")
            business_rule_fixes = await self._fix_business_rule_violation(
                violation, snapshot, context
            )
            if business_rule_fixes:
                return business_rule_fixes
        
        try:
            violation_data = {
                "id": getattr(violation, 'id', ''),
                "constraint_type": getattr(violation, 'constraint_type', ''),
                "table_name": getattr(violation, 'table', ''),
                "tuple_id": getattr(violation, 'tuple_id', ''),
                "attr": getattr(violation, 'attr', ''),
                "description": getattr(violation, 'description', ''),
                "current_value": getattr(violation, 'current_value', None)
            }
            
            snapshot_data = {
                "table_id": snapshot.table_id,
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
            
            response = await self.mcp_client.call_tool(
                server_name="doc2db-data-quality",
                tool_name="fix_violation",
                arguments={
                    "violation": violation_data,
                    "snapshot": snapshot_data
                }
            )
            
            if response and response.get("status") == "success":
                self.logger.info(f"MCP修复成功，违规ID: {violation_data.get('id')}")
                
                fixes_data = response.get("fixes", [])
                for fix_data in fixes_data:
                    pass
            
        except Exception as e:
            self.logger.error(f"MCP工具调用失败: {e}")
        
        if not fixes:
            fixes = await self._fallback_fix(violation, snapshot, context)
        
        if fixes:
            self.logger.info(f"为违规 {getattr(violation, 'id', 'unknown')} 生成 {len(fixes)} 个修复")
        else:
            self.logger.warning(f"⚠️ 违规 {getattr(violation, 'id', 'unknown')} 无法修复，尝试标记为unfixable")
            await self._mark_violation_unfixable(violation, context)
        
        return fixes
    
    async def _mark_violation_unfixable(self, violation: Violation, context):
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
                await memory_manager.mark_violation_unfixable(violation.id, run_id)
                self.logger.debug(f"✅ 已标记违规 {violation.id} 为无法修复")
            else:
                self.logger.debug(f"⚠️ 无法标记违规 {violation.id} 为unfixable: context缺少memory_manager或run_id")
        except Exception as e:
            self.logger.error(f"❌ 标记违规 {violation.id} 为unfixable失败: {e}")
    
    async def _fallback_fix(self, violation: Violation, snapshot: TableSnapshot, context=None) -> List[Fix]:
        """回退修复方法 - 直接使用具体的MCP组件"""
        self.logger.info("直接使用具体MCP组件进行修复")
        
        fixes = []
        
        try:
            from ..mcp import (
                TypeMCP, ValueMCP, StructureMCP, FormatMCP,
                LogicMCP, TemporalMCP, ReferenceMCP, AggregationMCP
            )
            
            constraint_type = getattr(violation, 'constraint_type', '').lower()  # 改为小写比较
            violation_attr = getattr(violation, 'attr', 'unknown')
            
            
            mcp_instance = None
            mcp_name = ""
            
            if constraint_type == 'type':
                mcp_instance = TypeMCP()
                mcp_name = "TypeMCP"
            elif constraint_type == 'value' or constraint_type == 'domain':
                mcp_instance = ValueMCP()
                mcp_name = "ValueMCP"
            elif constraint_type == 'structure' or constraint_type == 'null':
                mcp_instance = StructureMCP()
                mcp_name = "StructureMCP"
            elif constraint_type == 'format':  # 精确匹配format类型
                mcp_instance = FormatMCP()
                mcp_name = "FormatMCP"
            elif constraint_type == 'logic' or constraint_type == 'business':
                mcp_instance = LogicMCP()
                mcp_name = "LogicMCP"
            elif constraint_type == 'temporal' or constraint_type == 'time' or constraint_type == 'date':
                mcp_instance = TemporalMCP()
                mcp_name = "TemporalMCP"
            elif constraint_type == 'reference' or constraint_type == 'foreign':
                mcp_instance = ReferenceMCP()
                mcp_name = "ReferenceMCP"
            elif constraint_type == 'aggregation' or constraint_type == 'sum' or constraint_type == 'count':
                mcp_instance = AggregationMCP()
                mcp_name = "AggregationMCP"
            else:
                mcps = [
                    ("TypeMCP", TypeMCP()),
                    ("ValueMCP", ValueMCP()),
                    ("StructureMCP", StructureMCP()),
                    ("FormatMCP", FormatMCP()),
                    ("LogicMCP", LogicMCP()),
                    ("TemporalMCP", TemporalMCP()),
                    ("ReferenceMCP", ReferenceMCP()),
                    ("AggregationMCP", AggregationMCP())
                ]
                
                for name, mcp in mcps:
                    try:
                        mcp_fixes = mcp.fix(violation, snapshot)
                        if mcp_fixes:
                            fixes.extend(mcp_fixes)
                            self.logger.info(f"{name} 生成 {len(mcp_fixes)} 个修复")
                    except Exception as e:
                        self.logger.error(f"{name} 修复失败: {e}")
            
            if mcp_instance:
                try:
                    mcp_fixes = mcp_instance.fix(violation, snapshot)
                    fixes.extend(mcp_fixes)
                    
                    
                except Exception as e:
                    error_msg = f"{mcp_name} 修复失败: {e}"
                    self.logger.error(error_msg)
            
        except ImportError as e:
            self.logger.error(f"无法导入MCP组件: {e}")
        
        
        
        return fixes
    
    def _generate_simple_fix(self, violation: Violation, snapshot: TableSnapshot) -> List[Fix]:
        """生成简单的默认值修复"""
        fixes = []
        
        try:
            from ...memory import Fix, FixType
            
            attr = getattr(violation, 'attr', '')
            tuple_id = getattr(violation, 'tuple_id', '')
            old_value = getattr(violation, 'current_value', None)
            
            if attr and tuple_id:
                new_value = self._get_default_value_for_attr(attr, snapshot)
                
                if new_value is not None:
                    fix = Fix(
                        id=f"simple_fix_{tuple_id}_{attr}",
                        table=violation.table if hasattr(violation, 'table') else snapshot.table,
                        tuple_id=tuple_id,
                        attr=attr,
                        old=old_value,
                        new=new_value,

                        fix_type=FixType.VALUE_CORRECTION.value,
                        applied_by=self.fixer_id,
                        timestamp=""
                    )
                    fixes.append(fix)
        
        except ImportError:
            self.logger.error("无法导入Fix类，跳过简单修复")
        except Exception as e:
            self.logger.error(f"生成简单修复失败: {e}")
        
        return fixes
    
    def _get_default_value_for_attr(self, attr: str, snapshot: TableSnapshot) -> Any:
        """为属性生成默认值"""
        if 'id' in attr.lower():
            return 0
        elif 'name' in attr.lower():
            return "未知"
        elif 'date' in attr.lower():
            return "1900-01-01"
        elif 'time' in attr.lower():
            return "00:00:00"
        else:
            return ""
    
    def can_fix(self, violation: Violation) -> bool:
        """检查是否可以修复指定的违规"""
        constraint_type = getattr(violation, 'constraint_type', '')
        return constraint_type in self.constraint_type_mapping
    
    def get_supported_fix_types(self) -> List[str]:
        """获取支持的修复类型列表"""
        return [
            "DOMAIN_MAPPING",      # 域映射修复
            "VALUE_CORRECTION",    # 值纠正修复
            "STRUCTURE_FIX",       # 结构修复
            "LENGTH_ADJUST",       # 长度调整修复
            "TEMPORAL_FIX",        # 时间修复
            "LOGIC_FIX",           # 逻辑修复
            "BUSINESS_RULE_FIX",   # 业务规则修复
            "CALCULATION_FIX",     # 计算修复
            "AGGREGATION_FIX",     # 聚合修复
            "FOREIGN_KEY_FIX",     # 外键修复
            "FORMAT_FIX",          # 格式修复
            "TYPE_CONVERSION"      # 类型转换修复
        ]
    
    async def get_mcp_fix_strategies(self) -> Optional[Dict[str, Any]]:
        """获取MCP修复策略信息"""
        try:
            resource_data = await self.mcp_client.get_resource("doc2db-data-quality", "fix://strategies")
            return resource_data
        except Exception as e:
            self.logger.error(f"获取修复策略信息失败: {e}")
            return None
    
    def get_unfixable_violations(self, fixes: List[Fix]) -> List[Fix]:
        """
        获取无法修复的违规列表
        
        Args:
            fixes: 修复结果列表
            
        Returns:
            List[Fix]: 修复失败的Fix对象列表
        """
        unfixable_fixes = []
        for fix in fixes:
            if not fix.fix_success:
                unfixable_fixes.append(fix)
                self.logger.warning(f"发现无法修复的违规: {fix.id}, 原因: {fix.failure_reason}")
        
        self.logger.info(f"筛选出 {len(unfixable_fixes)} 个无法修复的违规")
        return unfixable_fixes
    
    def fix_batch(self, violations: List[Violation], snapshot: TableSnapshot, 
                  context=None) -> List[Fix]:
        """批量修复违规"""
        try:
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self._async_fix_batch(violations, snapshot, context))
                    return future.result()
            except RuntimeError:
                return asyncio.run(self._async_fix_batch(violations, snapshot, context))
        except Exception as e:
            self.logger.error(f"异步批量修复失败: {e}")
            return []
    
    async def _async_fix_batch(self, violations: List[Violation], snapshot: TableSnapshot, 
                              context=None) -> List[Fix]:
        """异步批量修复实现"""
        all_fixes = []
        
        violations_by_type = self._group_violations(violations)
        
        for (constraint_type, attr), grouped_violations in violations_by_type.items():
            try:
                mcp_fixes = await self._batch_fix_by_mcp(
                    grouped_violations, snapshot, constraint_type, context
                )
                all_fixes.extend(mcp_fixes)
            except Exception as e:
                self.logger.error(f"批量修复 {constraint_type}/{attr} 失败: {e}")
        
        return all_fixes
    
    def _group_violations(self, violations: List[Violation]) -> Dict[tuple, List[Violation]]:
        """按约束类型和属性分组违规"""
        groups = {}
        for violation in violations:
            constraint_type = getattr(violation, 'constraint_type', '')
            attr = getattr(violation, 'attr', '')
            key = (constraint_type, attr)
            
            if key not in groups:
                groups[key] = []
            groups[key].append(violation)
        
        return groups
    
    async def _batch_fix_by_mcp(self, violations: List[Violation], 
                               snapshot: TableSnapshot, 
                               constraint_type: str,
                               context=None) -> List[Fix]:
        """使用对应的MCP进行批量修复"""
        all_fixes = []
        
        try:
            from ..mcp import (
                TypeMCP, ValueMCP, StructureMCP, FormatMCP,
                LogicMCP, TemporalMCP, ReferenceMCP, AggregationMCP
            )
            
            constraint_type_lower = constraint_type.lower()
            
            mcp_instance = None
            if constraint_type_lower == 'type':
                mcp_instance = TypeMCP()
            elif constraint_type_lower in ['value', 'domain']:
                mcp_instance = ValueMCP()
            elif constraint_type_lower in ['structure', 'null']:
                mcp_instance = StructureMCP()
            elif constraint_type_lower == 'format':
                mcp_instance = FormatMCP()
            elif constraint_type_lower in ['logic', 'business']:
                mcp_instance = LogicMCP()
            elif constraint_type_lower in ['temporal', 'time', 'date']:
                mcp_instance = TemporalMCP()
            elif constraint_type_lower in ['reference', 'foreign']:
                mcp_instance = ReferenceMCP()
            elif constraint_type_lower in ['aggregation', 'sum', 'count']:
                mcp_instance = AggregationMCP()
            
            if mcp_instance and hasattr(mcp_instance.fixer, 'fix_batch'):
                all_fixes = mcp_instance.fixer.fix_batch(violations, snapshot, context)
                if all_fixes:
                    mcp_name = mcp_instance.mcp_id if hasattr(mcp_instance, 'mcp_id') else constraint_type
                    self.logger.info(f"{mcp_name}: 批量修复 {len(violations)} 个违规，生成 {len(all_fixes)} 个修复")
            elif mcp_instance:
                for violation in violations:
                    fixes = mcp_instance.fix(violation, snapshot)
                    all_fixes.extend(fixes)
                if all_fixes:
                    mcp_name = mcp_instance.mcp_id if hasattr(mcp_instance, 'mcp_id') else constraint_type
                    self.logger.info(f"{mcp_name}: 修复 {len(violations)} 个违规，生成 {len(all_fixes)} 个修复")
            else:
                for violation in violations:
                    fixes = await self._async_fix(violation, snapshot, context)
                    all_fixes.extend(fixes)
                    
        except ImportError as e:
            self.logger.error(f"无法导入MCP组件: {e}")
        
        return all_fixes
    
    async def _fix_business_rule_violation(self, violation: Violation,
                                          snapshot: TableSnapshot,
                                          context=None) -> List[Fix]:
        """修复schema定义的业务规则违规
        
        只有当schema中定义了rules字段时才会触发此方法
        """
        from ...memory import Fix, FixType, SuggestedFix
        from ...core.ids import IdGenerator
        
        fixes = []
        rule_id = getattr(violation, 'business_rule_id', '')
        
        self.logger.info(f"🔧 [MCPFixer] 修复业务规则违规: {rule_id}")
        
        if rule_id == 'phi_1':
            fixes = await self._fix_mutual_investment_violation(violation, snapshot, context)
        
        elif rule_id == 'phi_2':
            fixes = await self._fix_cash_sanity_violation(violation, snapshot, context)
        
        elif rule_id == 'phi_3':
            fixes = await self._fix_recursive_investment_violation(violation, snapshot, context)
        
        else:
            self.logger.debug(f"  使用通用MCP修复未知业务规则: {rule_id}")
            fixes = await self._fallback_fix(violation, snapshot, context)
        
        return fixes
    
    async def _fix_mutual_investment_violation(self, violation: Violation,
                                              snapshot: TableSnapshot,
                                              context=None) -> List[Fix]:
        """修复phi_1: 互投禁止违规
        
        策略：标记需要人工审核，因为需要判断哪个投资关系更可信
        """
        from ...memory import Fix, FixType
        from ...core.ids import IdGenerator
        
        fixes = []
        
        self.logger.info("  ⚠️ [phi_1] 互投禁止违规需要人工审核")
        
        try:
            fix_id = IdGenerator.generate_fix_id(
                violation.table, violation.tuple_id, violation.attr,
                FixType.BUSINESS_RULE_FIX.value, "manual_review"
            )
            
            fix = Fix(
                id=fix_id,
                table=violation.table,
                tuple_id=violation.tuple_id,
                attr=violation.attr,
                old=getattr(violation, 'current_value', None),
                new="[需要人工审核：判断哪个投资关系更可信]",
                fix_type=FixType.BUSINESS_RULE_FIX.value,
                applied_by=f"{self.fixer_id}_phi1",
                timestamp=""
            )
            fix.needs_manual_review = True
            fix.fix_strategy = "manual_decision_on_investment_credibility"
            fix.business_rule_id = "phi_1"
            
            fixes.append(fix)
            
        except Exception as e:
            self.logger.error(f"生成phi_1修复标记失败: {e}")
        
        return fixes
    
    async def _fix_cash_sanity_violation(self, violation: Violation,
                                        snapshot: TableSnapshot,
                                        context=None) -> List[Fix]:
        """修复phi_2: 现金合理性边界违规
        
        策略：标记需要重新从源文档验证
        """
        from ...memory import Fix, FixType
        from ...core.ids import IdGenerator
        
        fixes = []
        
        self.logger.info("  ℹ️ [phi_2] 现金合理性违规：建议重新验证源文档")
        
        try:
            fix_id = IdGenerator.generate_fix_id(
                violation.table, violation.tuple_id, violation.attr,
                FixType.BUSINESS_RULE_FIX.value, "verify_source"
            )
            
            fix = Fix(
                id=fix_id,
                table=violation.table,
                tuple_id=violation.tuple_id,
                attr=violation.attr,
                old=getattr(violation, 'current_value', None),
                new="[需要重新验证源文档]",
                fix_type=FixType.BUSINESS_RULE_FIX.value,
                applied_by=f"{self.fixer_id}_phi2",
                timestamp=""
            )
            fix.needs_manual_review = True
            fix.fix_strategy = "verify_source_document"
            fix.business_rule_id = "phi_2"
            
            fixes.append(fix)
            
        except Exception as e:
            self.logger.error(f"生成phi_2修复标记失败: {e}")
        
        return fixes
    
    async def _fix_recursive_investment_violation(self, violation: Violation,
                                                 snapshot: TableSnapshot,
                                                 context=None) -> List[Fix]:
        """修复phi_3: 递归投资违规（传递闭包）
        
        策略：根据suggested_fix添加缺失的间接投资关系
        """
        from ...memory import Fix, FixType
        from ...core.ids import IdGenerator
        
        fixes = []
        
        self.logger.info("  ➕ [phi_3] 添加缺失的传递投资关系")
        
        if hasattr(violation, 'suggested_fix') and violation.suggested_fix:
            suggested_value = violation.suggested_fix.value
            
            try:
                fix_id = IdGenerator.generate_fix_id(
                    violation.table, "NEW_ROW", "transitive_investment",
                    FixType.BUSINESS_RULE_FIX.value, "add_relation"
                )
                
                fix = Fix(
                    id=fix_id,
                    table=violation.table,
                    tuple_id="NEW_ROW",
                    attr="transitive_investment",
                    old=None,
                    new=suggested_value,
                    fix_type=FixType.BUSINESS_RULE_FIX.value,
                    applied_by=f"{self.fixer_id}_phi3",
                    timestamp=""
                )
                fix.fix_action = "insert_row"
                fix.suggested_relation = suggested_value
                fix.business_rule_id = "phi_3"
                
                fixes.append(fix)
                
            except Exception as e:
                self.logger.error(f"生成phi_3修复失败: {e}")
        else:
            self.logger.warning("  ⚠️ [phi_3] 缺少修复建议，跳过")
        
        return fixes
