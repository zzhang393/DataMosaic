"""Doc2DB处理上下文和状态定义"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

from ...core.io import IOManager
from ...memory import TableSnapshot, Violation, Fix


class Doc2DBProcessingState(Enum):
    """Doc2DB处理状态枚举"""
    INIT = "init"
    ANALYZING = "analyzing"                   # 分析阶段 - 识别所有需要处理的表
    EXTRACTING = "extracting"                 # 单表提取阶段
    VERIFYING = "verifying"                   # 单表验证阶段
    FIXING = "fixing"                         # 单表修复阶段
    MULTI_TABLE_VERIFYING = "multi_table_verifying"  # 多表验证阶段
    FINALIZING = "finalizing"                 # 最终化阶段
    COMPLETED = "completed"                   # 完成
    FAILED = "failed"                         # 失败


@dataclass 
class Doc2DBContext:
    """Doc2DB处理上下文"""
    run_id: str
    table_name: str
    schema: Dict[str, Any]
    documents: List[str]
    nl_prompt: str
    io_manager: IOManager
    
    user_query: str = ""              # 用户查询/需求
    analysis_steps: List[Dict] = None # 分析步骤
    current_step: int = 0             # 当前执行步骤
    step_results: Dict = None         # 步骤执行结果
    language: str = "chinese"         # 检测到的语言
    model: str = "gpt-4o"            # 用于处理的LLM模型
    
    document_mode: str = "single"     # 文档处理模式: "single" (单文档) 或 "multi" (多文档拼接)
    
    target_tables: List[str] = None                       # 待处理的表名列表
    entity_tables: List[str] = None                       # 实体表列表（新增）
    relation_tables: List[str] = None                 # 关系表列表（新增）
    current_table_index: int = 0                          # 当前处理的表索引
    all_snapshots: Dict[str, TableSnapshot] = None       # 所有表的快照
    all_violations: Dict[str, List[Violation]] = None    # 所有表的单表违规
    all_fixes: Dict[str, List[Fix]] = None               # 所有表的修复记录
    multi_table_violations: List[Violation] = None       # 多表验证违规
    
    current_state: Doc2DBProcessingState = Doc2DBProcessingState.INIT
    iteration: int = 0
    max_iterations: int = 3
    
    progress: Dict[str, Any] = None   # 进度信息
    step_outputs: List[Dict] = None   # 每个步骤的输出
    
    signal_history: List[Dict] = None # 信号历史记录
    correlation_ids: Dict[str, str] = None  # 关联ID映射
    
    snapshots: List[TableSnapshot] = None
    violations: List[Violation] = None
    fixes: List[Fix] = None
    
    def __post_init__(self):
        if self.analysis_steps is None:
            self.analysis_steps = []
        if self.step_results is None:
            self.step_results = {}
        if self.progress is None:
            self.progress = {}
        if self.step_outputs is None:
            self.step_outputs = []
        if self.target_tables is None:
            self.target_tables = []
        if self.entity_tables is None:
            self.entity_tables = []
        if self.relation_tables is None:
            self.relation_tables = []
        if self.all_snapshots is None:
            self.all_snapshots = {}
        if self.all_violations is None:
            self.all_violations = {}
        if self.all_fixes is None:
            self.all_fixes = {}
        if self.multi_table_violations is None:
            self.multi_table_violations = []
        if self.signal_history is None:
            self.signal_history = []
        if self.correlation_ids is None:
            self.correlation_ids = {}
        if self.snapshots is None:
            self.snapshots = []
        if self.violations is None:
            self.violations = []
        if self.fixes is None:
            self.fixes = []
    
    def to_dict(self) -> Dict[str, Any]:
        """将上下文转换为字典格式"""
        return {
            'run_id': self.run_id,
            'table_name': self.table_name,
            'schema': self.schema,
            'documents': self.documents,
            'nl_prompt': self.nl_prompt,
            'user_query': self.user_query,
            'analysis_steps': self.analysis_steps,
            'current_step': self.current_step,
            'step_results': self.step_results,
            'language': self.language,
            'model': self.model,
            'document_mode': self.document_mode,
            'target_tables': self.target_tables,
            'current_table_index': self.current_table_index,
            'current_state': self.current_state.value,
            'iteration': self.iteration,
            'max_iterations': self.max_iterations,
            'progress': self.progress,
            'step_outputs': self.step_outputs,
            'signal_history': self.signal_history,
            'correlation_ids': self.correlation_ids,
            'snapshots_count': len(self.snapshots) if self.snapshots else 0,
            'violations_count': len(self.violations) if self.violations else 0,
            'fixes_count': len(self.fixes) if self.fixes else 0,
            'all_snapshots_count': len(self.all_snapshots) if self.all_snapshots else 0,
            'all_violations_count': sum(len(v) for v in self.all_violations.values()) if self.all_violations else 0,
            'all_fixes_count': sum(len(f) for f in self.all_fixes.values()) if self.all_fixes else 0,
            'multi_table_violations_count': len(self.multi_table_violations) if self.multi_table_violations else 0
        }


def create_doc2db_context(run_id: str, table_name: str, schema: Dict[str, Any], 
                         documents: List[str], nl_prompt: str, io_manager: IOManager,
                         user_query: str = "", model: str = "gpt-4o", 
                         document_mode: str = "single") -> Doc2DBContext:
    """创建Doc2DB处理上下文
    
    Args:
        document_mode: 文档处理模式，"single" (单文档逐一处理) 或 "multi" (多文档拼接处理)
    """
    return Doc2DBContext(
        run_id=run_id,
        table_name=table_name,
        schema=schema,
        documents=documents,
        nl_prompt=nl_prompt,
        io_manager=io_manager,
        user_query=user_query,
        model=model,
        document_mode=document_mode,
        max_iterations=3
    )
