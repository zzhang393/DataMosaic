"""Doc2DB Orchestrator 详细日志记录器
记录每个处理步骤的快照、违规和修复状态
"""
import json
import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import asdict

from ..memory import TableSnapshot, Violation, Fix
from ..core.io import IOManager


class OrchestratorLogger:
    """Orchestrator专用的详细日志记录器"""
    
    def __init__(self, run_id: str, base_path: str = "logs"):
        """初始化日志记录器
        
        Args:
            run_id: 运行ID
            base_path: 日志基础路径
        """
        self.run_id = run_id
        self.base_path = base_path
        self.log_file = os.path.join(base_path, f"orchestrator_{run_id}.json")
        
        os.makedirs(base_path, exist_ok=True)
        
        self.log_data = {
            "run_id": run_id,
            "start_time": IOManager.get_timestamp(),
            "processing_steps": [],
            "snapshots_history": {},
            "violations_history": {},
            "fixes_history": {},
            "state_transitions": [],
            "error_events": [],
            "performance_metrics": {}
        }
        
        self._write_log()
        
        self.logger = logging.getLogger(f'orchestrator.logger.{run_id}')
        
    def log_state_transition(self, from_state: str, to_state: str, 
                           context: Any = None, extra_info: Dict = None):
        """记录状态转换"""
        transition = {
            "timestamp": IOManager.get_timestamp(),
            "from_state": from_state,
            "to_state": to_state,
            "context_info": {
                "current_table": getattr(context, 'table_name', 'unknown') if context else 'unknown',
                "current_table_index": getattr(context, 'current_table_index', -1) if context else -1,
                "iteration": getattr(context, 'iteration', 0) if context else 0,
                "target_tables": getattr(context, 'target_tables', []) if context else []
            },
            "extra_info": extra_info or {}
        }
        
        self.log_data["state_transitions"].append(transition)
        self._write_log()
        
        self.logger.info(f"状态转换: {from_state} → {to_state}")
    
    def log_step_start(self, step_name: str, description: str, context: Any = None):
        """记录步骤开始"""
        step_info = {
            "step_name": step_name,
            "status": "started",
            "start_time": IOManager.get_timestamp(),
            "description": description,
            "context_snapshot": self._extract_context_info(context) if context else {}
        }
        
        self.log_data["processing_steps"].append(step_info)
        self._write_log()
        
        self.logger.info(f"步骤开始: {step_name} - {description}")
    
    def log_step_complete(self, step_name: str, result_summary: Dict = None, 
                         context: Any = None):
        """记录步骤完成"""
        for step in reversed(self.log_data["processing_steps"]):
            if step["step_name"] == step_name and step["status"] == "started":
                step["status"] = "completed"
                step["end_time"] = IOManager.get_timestamp()
                step["result_summary"] = result_summary or {}
                step["final_context"] = self._extract_context_info(context) if context else {}
                break
        
        self._write_log()
        self.logger.info(f"步骤完成: {step_name}")
    
    def log_step_error(self, step_name: str, error: Exception, context: Any = None):
        """记录步骤错误"""
        for step in reversed(self.log_data["processing_steps"]):
            if step["step_name"] == step_name and step["status"] == "started":
                step["status"] = "failed"
                step["end_time"] = IOManager.get_timestamp()
                step["error"] = str(error)
                break
        
        error_event = {
            "timestamp": IOManager.get_timestamp(),
            "step_name": step_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context_info": self._extract_context_info(context) if context else {}
        }
        
        self.log_data["error_events"].append(error_event)
        self._write_log()
        
        self.logger.error(f"步骤错误: {step_name} - {error}")
    
    def log_snapshot_created(self, table_name: str, snapshot: TableSnapshot, 
                           step_name: str = "unknown"):
        """记录快照创建"""
        snapshot_info = {
            "timestamp": IOManager.get_timestamp(),
            "step_name": step_name,
            "action": "created",
            "table_name": table_name,
            "snapshot_summary": {
                "table": snapshot.table,
                "run_id": snapshot.run_id,
                "total_rows": len(snapshot.rows),
                "columns": list(snapshot.rows[0].cells.keys()) if snapshot.rows else [],
                "created_at": snapshot.created_at
            },
            "data_preview": self._create_data_preview(snapshot, max_rows=3)
        }
        
        if table_name not in self.log_data["snapshots_history"]:
            self.log_data["snapshots_history"][table_name] = []
        
        self.log_data["snapshots_history"][table_name].append(snapshot_info)
        self._write_log()
        
        self.logger.info(f"快照创建: {table_name} - {len(snapshot.rows)} 行数据")
    
    def log_snapshot_updated(self, table_name: str, snapshot: TableSnapshot,
                           step_name: str = "unknown", changes_summary: Dict = None):
        """记录快照更新"""
        snapshot_info = {
            "timestamp": IOManager.get_timestamp(),
            "step_name": step_name,
            "action": "updated",
            "table_name": table_name,
            "snapshot_summary": {
                "table": snapshot.table,
                "run_id": snapshot.run_id,
                "total_rows": len(snapshot.rows),
                "columns": list(snapshot.rows[0].cells.keys()) if snapshot.rows else [],
                "created_at": snapshot.created_at
            },
            "changes_summary": changes_summary or {},
            "data_preview": self._create_data_preview(snapshot, max_rows=3)
        }
        
        if table_name not in self.log_data["snapshots_history"]:
            self.log_data["snapshots_history"][table_name] = []
        
        self.log_data["snapshots_history"][table_name].append(snapshot_info)
        self._write_log()
        
        self.logger.info(f"快照更新: {table_name} - 变更: {changes_summary}")
    
    def log_violations_detected(self, table_name: str, violations: List[Violation],
                              step_name: str = "unknown", round_num: int = 1):
        """记录违规检测"""
        violations_info = {
            "timestamp": IOManager.get_timestamp(),
            "step_name": step_name,
            "table_name": table_name,
            "round_number": round_num,
            "total_violations": len(violations),
            "violations_by_severity": self._group_violations_by_severity(violations),
            "violations_by_type": self._group_violations_by_type(violations),
            "detailed_violations": [self._violation_to_log_format(v) for v in violations[:10]]  # 只记录前10个
        }
        
        if table_name not in self.log_data["violations_history"]:
            self.log_data["violations_history"][table_name] = []
        
        self.log_data["violations_history"][table_name].append(violations_info)
        self._write_log()
        
        self.logger.info(f"违规检测: {table_name} 第{round_num}轮 - 发现 {len(violations)} 个问题")
    
    def log_fixes_applied(self, table_name: str, fixes: List[Fix],
                         step_name: str = "unknown", round_num: int = 1):
        """记录修复应用"""
        fixes_info = {
            "timestamp": IOManager.get_timestamp(),
            "step_name": step_name,
            "table_name": table_name,
            "round_number": round_num,
            "total_fixes": len(fixes),
            "fixes_by_type": self._group_fixes_by_type(fixes),
            "successful_fixes": len([f for f in fixes if f.fix_success]),
            "failed_fixes": len([f for f in fixes if not f.fix_success]),
            "detailed_fixes": [self._fix_to_log_format(f) for f in fixes[:10]]  # 只记录前10个
        }
        
        if table_name not in self.log_data["fixes_history"]:
            self.log_data["fixes_history"][table_name] = []
        
        self.log_data["fixes_history"][table_name].append(fixes_info)
        self._write_log()
        
        successful = len([f for f in fixes if f.fix_success])
        self.logger.info(f"修复应用: {table_name} 第{round_num}轮 - {successful}/{len(fixes)} 成功")
    
    def log_performance_metric(self, metric_name: str, value: float, unit: str = "ms"):
        """记录性能指标"""
        self.log_data["performance_metrics"][metric_name] = {
            "value": value,
            "unit": unit,
            "timestamp": IOManager.get_timestamp()
        }
        self._write_log()
    
    def log_warm_start_triggered(self, table_name: str, unfixable_violations: List,
                               step_name: str = "warm_start"):
        """记录暖启动触发"""
        warm_start_info = {
            "timestamp": IOManager.get_timestamp(),
            "step_name": step_name,
            "table_name": table_name,
            "action": "warm_start_triggered",
            "unfixable_violations_count": len(unfixable_violations),
            "unfixable_details": [
                {
                    "violation_id": getattr(v, 'id', 'unknown'),
                    "constraint_type": getattr(v, 'constraint_type', 'unknown'),
                    "attr": getattr(v, 'attr', 'unknown'),
                    "description": getattr(v, 'description', 'unknown')
                }
                for v in unfixable_violations[:5]  # 只记录前5个
            ]
        }
        
        self.log_data["processing_steps"].append(warm_start_info)
        self._write_log()
        
        self.logger.warning(f"暖启动触发: {table_name} - {len(unfixable_violations)} 个无法修复的违规")
    
    def log_multi_table_verification(self, all_snapshots: Dict, 
                                   multi_table_violations: List[Violation]):
        """记录多表验证"""
        multi_table_info = {
            "timestamp": IOManager.get_timestamp(),
            "step_name": "multi_table_verification",
            "action": "completed",
            "tables_verified": list(all_snapshots.keys()),
            "total_tables": len(all_snapshots),
            "cross_table_violations": len(multi_table_violations),
            "violations_by_type": self._group_violations_by_type(multi_table_violations),
            "table_summaries": {
                table_name: {
                    "rows": len(snapshot.rows),
                    "columns": list(snapshot.rows[0].cells.keys()) if snapshot.rows else []
                }
                for table_name, snapshot in all_snapshots.items()
            }
        }
        
        self.log_data["processing_steps"].append(multi_table_info)
        self._write_log()
        
        self.logger.info(f"多表验证完成: {len(all_snapshots)} 个表 - {len(multi_table_violations)} 个跨表问题")
    
    def log_final_summary(self, context: Any):
        """记录最终摘要"""
        final_summary = {
            "timestamp": IOManager.get_timestamp(),
            "step_name": "final_summary",
            "total_tables_processed": len(getattr(context, 'all_snapshots', {})),
            "total_rows_extracted": sum(
                len(snapshot.rows) 
                for snapshot in getattr(context, 'all_snapshots', {}).values()
            ),
            "total_violations_found": len(getattr(context, 'violations', [])),
            "total_fixes_applied": len(getattr(context, 'fixes', [])),
            "processing_duration": self._calculate_duration(),
            "tables_summary": {}
        }
        
        if hasattr(context, 'all_snapshots'):
            for table_name, snapshot in context.all_snapshots.items():
                table_violations = len(context.all_violations.get(table_name, []))
                table_fixes = len(context.all_fixes.get(table_name, []))
                
                final_summary["tables_summary"][table_name] = {
                    "rows": len(snapshot.rows),
                    "violations": table_violations,
                    "fixes": table_fixes,
                    "final_state": "processed"
                }
        
        self.log_data["final_summary"] = final_summary
        self.log_data["end_time"] = IOManager.get_timestamp()
        self._write_log()
        
        self.logger.info(f"处理完成摘要: {final_summary['total_tables_processed']} 表, "
                        f"{final_summary['total_rows_extracted']} 行, "
                        f"{final_summary['total_violations_found']} 问题, "
                        f"{final_summary['total_fixes_applied']} 修复")
    
    def _extract_context_info(self, context: Any) -> Dict[str, Any]:
        """提取上下文信息"""
        if not context:
            return {}
        
        return {
            "run_id": getattr(context, 'run_id', 'unknown'),
            "table_name": getattr(context, 'table_name', 'unknown'),
            "current_state": str(getattr(context, 'current_state', 'unknown')),
            "iteration": getattr(context, 'iteration', 0),
            "current_table_index": getattr(context, 'current_table_index', -1),
            "target_tables": getattr(context, 'target_tables', []),
            "language": getattr(context, 'language', 'unknown'),
            "model": getattr(context, 'model', 'unknown')
        }
    
    def _create_data_preview(self, snapshot: TableSnapshot, max_rows: int = 3) -> Dict[str, Any]:
        """创建数据预览"""
        preview_rows = []
        
        for i, row in enumerate(snapshot.rows[:max_rows]):
            row_preview = {
                "tuple_id": row.tuple_id,
                "cells": {}
            }
            
            for attr, cell in row.cells.items():
                row_preview["cells"][attr] = {
                    "value": cell.value,
                    "evidences_count": len(cell.evidences)
                }
            
            preview_rows.append(row_preview)
        
        return {
            "total_rows": len(snapshot.rows),
            "preview_rows": preview_rows,
            "columns": list(snapshot.rows[0].cells.keys()) if snapshot.rows else []
        }
    
    def _group_violations_by_severity(self, violations: List[Violation]) -> Dict[str, int]:
        """按严重程度分组违规"""
        groups = {}
        for violation in violations:
            severity = getattr(violation, 'severity', 'unknown')
            groups[severity] = groups.get(severity, 0) + 1
        return groups
    
    def _group_violations_by_type(self, violations: List[Violation]) -> Dict[str, int]:
        """按类型分组违规"""
        groups = {}
        for violation in violations:
            constraint_type = getattr(violation, 'constraint_type', 'unknown')
            groups[constraint_type] = groups.get(constraint_type, 0) + 1
        return groups
    
    def _group_fixes_by_type(self, fixes: List[Fix]) -> Dict[str, int]:
        """按类型分组修复"""
        groups = {}
        for fix in fixes:
            fix_type = getattr(fix, 'fix_type', 'unknown')
            groups[fix_type] = groups.get(fix_type, 0) + 1
        return groups
    
    def _violation_to_log_format(self, violation: Violation) -> Dict[str, Any]:
        """将违规转换为日志格式"""
        return {
            "id": getattr(violation, 'id', 'unknown'),
            "table": getattr(violation, 'table', 'unknown'),
            "tuple_id": getattr(violation, 'tuple_id', 'unknown'),
            "attr": getattr(violation, 'attr', 'unknown'),
            "constraint_type": getattr(violation, 'constraint_type', 'unknown'),
            "severity": getattr(violation, 'severity', 'unknown'),
            "description": getattr(violation, 'description', 'unknown')[:200],  # 限制长度
            "has_suggested_fix": bool(getattr(violation, 'suggested_fix', None))
        }
    
    def _fix_to_log_format(self, fix: Fix) -> Dict[str, Any]:
        """将修复转换为日志格式"""
        return {
            "id": getattr(fix, 'id', 'unknown'),
            "table": getattr(fix, 'table', 'unknown'),
            "tuple_id": getattr(fix, 'tuple_id', 'unknown'),
            "attr": getattr(fix, 'attr', 'unknown'),
            "old_value": str(getattr(fix, 'old', 'unknown'))[:100],  # 限制长度
            "new_value": str(getattr(fix, 'new', 'unknown'))[:100],
            "fix_type": getattr(fix, 'fix_type', 'unknown'),
            "success": getattr(fix, 'fix_success', True),
            "failure_reason": getattr(fix, 'failure_reason', '') if not getattr(fix, 'fix_success', True) else None
        }
    
    def _calculate_duration(self) -> str:
        """计算处理持续时间"""
        try:
            start_time = datetime.fromisoformat(self.log_data["start_time"])
            current_time = datetime.now()
            duration = current_time - start_time
            return str(duration)
        except:
            return "unknown"
    
    def _write_log(self):
        """写入日志文件"""
        try:
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(self.log_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"写入日志文件失败: {e}")
    
    def get_log_file_path(self) -> str:
        """获取日志文件路径"""
        return self.log_file
    
    def get_summary_report(self) -> Dict[str, Any]:
        """获取摘要报告"""
        return {
            "run_id": self.run_id,
            "start_time": self.log_data.get("start_time"),
            "end_time": self.log_data.get("end_time"),
            "total_steps": len(self.log_data["processing_steps"]),
            "total_state_transitions": len(self.log_data["state_transitions"]),
            "total_errors": len(self.log_data["error_events"]),
            "tables_processed": list(self.log_data["snapshots_history"].keys()),
            "final_summary": self.log_data.get("final_summary", {}),
            "log_file": self.log_file
        }


class OrchestratorLoggerMixin:
    """Orchestrator日志记录器混入类
    
    可以被添加到Doc2DBOrchestrator中以启用详细日志记录
    """
    
    def _init_detailed_logger(self):
        """初始化详细日志记录器"""
        if hasattr(self, 'current_context') and self.current_context:
            run_id = self.current_context.run_id
        else:
            run_id = f"orchestrator_{IOManager.get_timestamp().replace(':', '-')}"
        
        self.detailed_logger = OrchestratorLogger(run_id)
        self.logger.info(f"详细日志记录器已启用: {self.detailed_logger.get_log_file_path()}")
    
    def _log_with_detailed_logger(self, method_name: str, *args, **kwargs):
        """使用详细日志记录器记录"""
        if hasattr(self, 'detailed_logger'):
            method = getattr(self.detailed_logger, method_name, None)
            if method:
                try:
                    method(*args, **kwargs)
                except Exception as e:
                    self.logger.warning(f"详细日志记录失败 ({method_name}): {e}")
