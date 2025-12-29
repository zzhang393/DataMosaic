"""Doc2DB Orchestrator 日志集成模块
将详细日志记录功能集成到现有的Doc2DBOrchestrator中

⚠️ DEPRECATED WARNING ⚠️
此模块可能已被新的日志系统替代。
在使用前请确认是否还需要此功能。
相关模块: orchestrator_logger.py
建议: 检查新架构中的日志实现方式
"""
import functools
import warnings
from typing import Any

from .orchestrator_logger import OrchestratorLogger

warnings.warn(
    "orchestrator_logging_integration.py 可能已弃用，请检查新架构中的日志实现",
    PendingDeprecationWarning,
    stacklevel=2
)


def enable_detailed_logging(orchestrator_class):
    """装饰器：为Orchestrator类启用详细日志记录
    
    用法:
    @enable_detailed_logging
    class Doc2DBOrchestrator:
        ...
    """
    
    original_init = orchestrator_class.__init__
    original_process = orchestrator_class.process
    original_init_processing = orchestrator_class._init_processing
    original_analyze_tables = orchestrator_class._analyze_tables
    original_extract_data = orchestrator_class._extract_data
    original_verify_data = orchestrator_class._verify_data
    original_fix_data = orchestrator_class._fix_data
    original_multi_table_verify = orchestrator_class._multi_table_verify
    original_finalize_processing = orchestrator_class._finalize_processing
    original_apply_fixes = orchestrator_class._apply_fixes
    original_trigger_warm_start = orchestrator_class._trigger_warm_start_extraction
    
    def new_init(self, *args, enable_detailed_logging=True, **kwargs):
        """增强的初始化方法"""
        original_init(self, *args, **kwargs)
        self.detailed_logging_enabled = enable_detailed_logging
        self.detailed_logger = None
    
    def new_process(self, context):
        """增强的主处理方法"""
        if self.detailed_logging_enabled:
            self.detailed_logger = OrchestratorLogger(context.run_id)
            self.detailed_logger.log_step_start(
                "orchestrator_process", 
                f"开始Doc2DB处理流程 - 运行ID: {context.run_id}",
                context
            )
        
        try:
            result = original_process(self, context)
            
            if self.detailed_logger:
                self.detailed_logger.log_step_complete(
                    "orchestrator_process",
                    {"final_state": str(context.current_state)},
                    context
                )
                self.detailed_logger.log_final_summary(context)
            
            return result
            
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error("orchestrator_process", e, context)
            raise
    
    def new_init_processing(self, context):
        """增强的初始化处理方法"""
        if self.detailed_logger:
            self.detailed_logger.log_step_start(
                "init_processing",
                f"初始化处理 - 目标表: {context.table_name}",
                context
            )
            self.detailed_logger.log_state_transition(
                "NONE", "INIT", context
            )
        
        try:
            original_init_processing(self, context)
            
            if self.detailed_logger:
                self.detailed_logger.log_step_complete(
                    "init_processing",
                    {"language": context.language, "documents_count": len(context.documents)},
                    context
                )
                self.detailed_logger.log_state_transition(
                    "INIT", "ANALYZING", context
                )
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error("init_processing", e, context)
            raise
    
    def new_analyze_tables(self, context):
        """增强的表分析方法"""
        if self.detailed_logger:
            self.detailed_logger.log_step_start(
                "analyze_tables",
                "分析Schema中的表结构",
                context
            )
        
        try:
            original_analyze_tables(self, context)
            
            if self.detailed_logger:
                self.detailed_logger.log_step_complete(
                    "analyze_tables",
                    {
                        "target_tables": context.target_tables,
                        "total_tables": len(context.target_tables)
                    },
                    context
                )
                self.detailed_logger.log_state_transition(
                    "ANALYZING", "EXTRACTING", context
                )
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error("analyze_tables", e, context)
            raise
    
    def new_extract_data(self, context):
        """增强的数据提取方法"""
        current_table = self._get_current_table_name(context)
        
        if self.detailed_logger:
            self.detailed_logger.log_step_start(
                f"extract_data_{current_table}",
                f"提取表 {current_table} 的数据",
                context
            )
        
        try:
            original_extract_data(self, context)
            
            if self.detailed_logger and context.snapshots:
                snapshot = context.snapshots[0]
                self.detailed_logger.log_snapshot_created(
                    current_table, snapshot, f"extract_data_{current_table}"
                )
                self.detailed_logger.log_step_complete(
                    f"extract_data_{current_table}",
                    {
                        "rows_extracted": len(snapshot.rows),
                        "columns": list(snapshot.rows[0].cells.keys()) if snapshot.rows else []
                    },
                    context
                )
                self.detailed_logger.log_state_transition(
                    "EXTRACTING", "VERIFYING", context
                )
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error(f"extract_data_{current_table}", e, context)
            raise
    
    def new_verify_data(self, context):
        """增强的数据验证方法"""
        current_table = self._get_current_table_name(context)
        round_num = context.iteration + 1
        
        if self.detailed_logger:
            self.detailed_logger.log_step_start(
                f"verify_data_{current_table}_round_{round_num}",
                f"验证表 {current_table} 第 {round_num} 轮",
                context
            )
        
        try:
            original_verify_data(self, context)
            
            if self.detailed_logger:
                current_violations = context.all_violations.get(current_table, [])
                self.detailed_logger.log_violations_detected(
                    current_table, current_violations,
                    f"verify_data_{current_table}_round_{round_num}",
                    round_num
                )
                
                error_violations = self._get_error_violations(current_violations)
                next_state = "FIXING" if error_violations and context.iteration < context.max_iterations else "NEXT_TABLE_OR_MULTI_VERIFY"
                
                self.detailed_logger.log_step_complete(
                    f"verify_data_{current_table}_round_{round_num}",
                    {
                        "total_violations": len(current_violations),
                        "error_violations": len(error_violations),
                        "next_action": next_state
                    },
                    context
                )
                
                if error_violations and context.iteration < context.max_iterations:
                    self.detailed_logger.log_state_transition(
                        "VERIFYING", "FIXING", context
                    )
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error(f"verify_data_{current_table}_round_{round_num}", e, context)
            raise
    
    def new_fix_data(self, context):
        """增强的数据修复方法"""
        current_table = self._get_current_table_name(context)
        round_num = context.iteration + 1
        
        if self.detailed_logger:
            self.detailed_logger.log_step_start(
                f"fix_data_{current_table}_round_{round_num}",
                f"修复表 {current_table} 第 {round_num} 轮",
                context
            )
        
        try:
            old_snapshot = context.all_snapshots.get(current_table)
            
            original_fix_data(self, context)
            
            if self.detailed_logger:
                current_fixes = context.all_fixes.get(current_table, [])
                round_fixes = [f for f in current_fixes if hasattr(f, 'round_num') or True]  # 简化筛选
                
                self.detailed_logger.log_fixes_applied(
                    current_table, round_fixes,
                    f"fix_data_{current_table}_round_{round_num}",
                    round_num
                )
                
                new_snapshot = context.all_snapshots.get(current_table)
                if new_snapshot and new_snapshot != old_snapshot:
                    self.detailed_logger.log_snapshot_updated(
                        current_table, new_snapshot,
                        f"fix_data_{current_table}_round_{round_num}",
                        {"fixes_applied": len(round_fixes)}
                    )
                
                self.detailed_logger.log_step_complete(
                    f"fix_data_{current_table}_round_{round_num}",
                    {
                        "fixes_applied": len(round_fixes),
                        "successful_fixes": len([f for f in round_fixes if f.fix_success]),
                        "next_iteration": context.iteration + 1
                    },
                    context
                )
                self.detailed_logger.log_state_transition(
                    "FIXING", "VERIFYING", context
                )
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error(f"fix_data_{current_table}_round_{round_num}", e, context)
            raise
    
    def new_multi_table_verify(self, context):
        """增强的多表验证方法"""
        if self.detailed_logger:
            self.detailed_logger.log_step_start(
                "multi_table_verify",
                f"多表验证 - {len(context.all_snapshots)} 个表",
                context
            )
        
        try:
            original_multi_table_verify(self, context)
            
            if self.detailed_logger:
                self.detailed_logger.log_multi_table_verification(
                    context.all_snapshots,
                    context.multi_table_violations
                )
                self.detailed_logger.log_step_complete(
                    "multi_table_verify",
                    {
                        "tables_verified": list(context.all_snapshots.keys()),
                        "cross_table_violations": len(context.multi_table_violations)
                    },
                    context
                )
                self.detailed_logger.log_state_transition(
                    "MULTI_TABLE_VERIFYING", "FINALIZING", context
                )
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error("multi_table_verify", e, context)
            raise
    
    def new_finalize_processing(self, context):
        """增强的最终化处理方法"""
        if self.detailed_logger:
            self.detailed_logger.log_step_start(
                "finalize_processing",
                "最终化处理流程",
                context
            )
        
        try:
            original_finalize_processing(self, context)
            
            if self.detailed_logger:
                self.detailed_logger.log_step_complete(
                    "finalize_processing",
                    {
                        "total_tables": len(context.all_snapshots),
                        "total_rows": sum(len(s.rows) for s in context.all_snapshots.values()),
                        "total_violations": len(context.violations),
                        "total_fixes": len(context.fixes)
                    },
                    context
                )
                self.detailed_logger.log_state_transition(
                    "FINALIZING", "COMPLETED", context
                )
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error("finalize_processing", e, context)
            raise
    
    def new_trigger_warm_start(self, context, unfixable_fixes):
        """增强的暖启动触发方法"""
        current_table = self._get_current_table_name(context)
        
        if self.detailed_logger:
            self.detailed_logger.log_warm_start_triggered(
                current_table, unfixable_fixes,
                f"warm_start_{current_table}"
            )
        
        try:
            original_trigger_warm_start(self, context, unfixable_fixes)
            
            if self.detailed_logger:
                new_snapshot = context.all_snapshots.get(current_table)
                if new_snapshot:
                    self.detailed_logger.log_snapshot_created(
                        current_table, new_snapshot,
                        f"warm_start_reextraction_{current_table}"
                    )
        except Exception as e:
            if self.detailed_logger:
                self.detailed_logger.log_step_error(f"warm_start_{current_table}", e, context)
            raise
    
    orchestrator_class.__init__ = new_init
    orchestrator_class.process = new_process
    orchestrator_class._init_processing = new_init_processing
    orchestrator_class._analyze_tables = new_analyze_tables
    orchestrator_class._extract_data = new_extract_data
    orchestrator_class._verify_data = new_verify_data
    orchestrator_class._fix_data = new_fix_data
    orchestrator_class._multi_table_verify = new_multi_table_verify
    orchestrator_class._finalize_processing = new_finalize_processing
    orchestrator_class._trigger_warm_start_extraction = new_trigger_warm_start
    
    def get_detailed_log_path(self):
        """获取详细日志文件路径"""
        return self.detailed_logger.get_log_file_path() if self.detailed_logger else None
    
    def get_log_summary(self):
        """获取日志摘要"""
        return self.detailed_logger.get_summary_report() if self.detailed_logger else None
    
    orchestrator_class.get_detailed_log_path = get_detailed_log_path
    orchestrator_class.get_log_summary = get_log_summary
    
    return orchestrator_class


def log_method_execution(method_name: str = None):
    """方法装饰器：记录方法执行
    
    用法:
    @log_method_execution("custom_method_name")
    def some_method(self, ...):
        ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            name = method_name or func.__name__
            
            if hasattr(self, 'detailed_logger') and self.detailed_logger:
                context = args[0] if args and hasattr(args[0], 'run_id') else None
                self.detailed_logger.log_step_start(
                    name,
                    f"执行方法: {func.__name__}",
                    context
                )
            
            try:
                result = func(self, *args, **kwargs)
                
                if hasattr(self, 'detailed_logger') and self.detailed_logger:
                    context = args[0] if args and hasattr(args[0], 'run_id') else None
                    self.detailed_logger.log_step_complete(
                        name,
                        {"result_type": type(result).__name__},
                        context
                    )
                
                return result
                
            except Exception as e:
                if hasattr(self, 'detailed_logger') and self.detailed_logger:
                    context = args[0] if args and hasattr(args[0], 'run_id') else None
                    self.detailed_logger.log_step_error(name, e, context)
                raise
        
        return wrapper
    return decorator


def apply_logging_to_orchestrator(orchestrator_instance, run_id: str = None):
    """动态为现有的orchestrator实例应用日志记录
    
    Args:
        orchestrator_instance: Doc2DBOrchestrator实例
        run_id: 可选的运行ID，如果不提供则自动生成
    """
    if not run_id:
        from ..core.io import IOManager
        run_id = f"dynamic_{IOManager.get_timestamp().replace(':', '-')}"
    
    orchestrator_instance.detailed_logger = OrchestratorLogger(run_id)
    orchestrator_instance.detailed_logging_enabled = True
    
    original_process = orchestrator_instance.process
    
    def logged_process(context):
        orchestrator_instance.detailed_logger.log_step_start(
            "orchestrator_process", 
            f"开始Doc2DB处理流程 - 运行ID: {context.run_id}",
            context
        )
        
        try:
            result = original_process(context)
            orchestrator_instance.detailed_logger.log_final_summary(context)
            return result
        except Exception as e:
            orchestrator_instance.detailed_logger.log_step_error("orchestrator_process", e, context)
            raise
    
    orchestrator_instance.process = logged_process
    
    return orchestrator_instance
