"""数据快照契约定义"""
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class CellData:
    """单元格数据 - 简化版本，直接存储值和证据"""
    value: Any
    evidences: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "evidences": self.evidences
        }


@dataclass
class TableRow:
    """表格行"""
    tuple_id: str
    cells: Dict[str, CellData]
    
    def to_dict(self) -> Dict[str, Any]:
        cells_dict = {}
        for attr, cell in self.cells.items():
            cells_dict[attr] = cell.to_dict()
        
        return {
            "tuple_id": self.tuple_id,
            "cells": cells_dict
        }


@dataclass
class TableSnapshot:
    """表格快照"""
    run_id: str
    table: str
    rows: List[TableRow]
    created_at: str
    table_id: Optional[str] = None  # 添加table_id字段
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if self.table_id is None:
            self.table_id = self.table
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "run_id": self.run_id,
            "table": self.table,
            "table_id": self.table_id,
            "rows": [row.to_dict() for row in self.rows],
            "created_at": self.created_at
        }
        
        if hasattr(self, 'processing_stage'):
            result['processing_stage'] = self.processing_stage
        if hasattr(self, 'stage_description'):
            result['stage_description'] = self.stage_description
            
        return result
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableSnapshot':
        """从字典创建快照对象"""
        rows = []
        for row_data in data.get('rows', []):
            cells = {}
            for attr, cell_data in row_data.get('cells', {}).items():
                if 'best' in cell_data:
                    value = cell_data.get('best', {}).get('value')
                else:
                    value = cell_data.get('value')
                
                cells[attr] = CellData(
                    value=value,
                    evidences=cell_data.get('evidences', [])
                )
            
            rows.append(TableRow(
                tuple_id=row_data.get('tuple_id', ''),
                cells=cells
            ))
        
        snapshot = cls(
            run_id=data.get('run_id', ''),
            table=data.get('table', ''),
            table_id=data.get('table_id'),  # 从数据中读取table_id
            rows=rows,
            created_at=data.get('created_at', '')
        )
        
        if 'processing_stage' in data:
            snapshot.processing_stage = data['processing_stage']
        if 'stage_description' in data:
            snapshot.stage_description = data['stage_description']
            
        return snapshot

