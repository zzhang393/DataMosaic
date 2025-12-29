"""数据契约模块"""

from .snapshot import TableSnapshot, TableRow, CellData
from .violation import Violation, SuggestedFix, ViolationSeverity, ConstraintType
from .fix import Fix, FixType
from .manager import MemoryManager, MemoryReference
from .table import Table, TableProcessingState

__all__ = [
    'TableSnapshot', 'TableRow', 'CellData',
    'Violation', 'SuggestedFix', 'ViolationSeverity', 'ConstraintType',
    'Fix', 'FixType',
    'MemoryManager', 'MemoryReference',
    'Table', 'TableProcessingState'
]

