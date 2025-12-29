"""统一的Table对象 - 管理表格的完整生命周期"""

import os
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from . import TableSnapshot, TableRow, Violation, Fix
from ..core.ids import IdGenerator
from ..core.io import IOManager


class TableProcessingState:
    """表格处理状态"""
    CREATED = "created"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    FIXING = "fixing"
    FIXED = "fixed"
    COMPLETED = "completed"
    ERROR = "error"


class Table:
    """统一的Table对象 - 管理表格数据的完整生命周期"""
    
    def __init__(self, table_name: str, schema: Dict[str, Any], 
                 table_id: str = None, output_dir: str = None):
        """
        初始化Table对象
        
        Args:
            table_name: 表名
            schema: 表结构定义
            table_id: 表ID（可选，如果不提供会自动生成）
            output_dir: 输出目录（可选）
        """
        self.table_name = table_name
        self.schema = schema
        import json
        import hashlib
        schema_str = json.dumps(schema, sort_keys=True, ensure_ascii=False) if schema else ""
        schema_hash = hashlib.md5(schema_str.encode()).hexdigest() if schema_str else None
        
        self.table_id = table_id or IdGenerator.generate_table_id(table_name, schema_hash)
        self.output_dir = output_dir
        
        self.current_rows: List[TableRow] = []
        self.current_state = TableProcessingState.CREATED
        
        self.snapshots: List[TableSnapshot] = []
        self.violations_history: List[List[Violation]] = []
        self.fixes_history: List[List[Fix]] = []
        
        self.created_at = datetime.now().isoformat()
        self.updated_at = self.created_at
        
        self.stats = {
            'total_extractions': 0,
            'total_verifications': 0, 
            'total_fixes': 0,
            'current_violations': 0,
            'current_rows': 0
        }
        
        self.logger = logging.getLogger('doc2db.table')
        self.logger.info(f"创建Table对象: {self.table_name} (ID: {self.table_id})")
    
    def extract(self, extractor, documents: List[str], nl_prompt: str = "", 
                context=None, warm_start: bool = False) -> bool:
        """
        执行数据抽取
        
        Args:
            extractor: 抽取器实例
            documents: 文档内容列表
            nl_prompt: 自然语言提示
            context: 处理上下文
            warm_start: 是否为暖启动
            
        Returns:
            是否抽取成功
        """
        self._update_state(TableProcessingState.EXTRACTING)
        
        try:
            previous_snapshot = self.get_latest_snapshot() if warm_start else None
            violations = self.get_latest_violations() if warm_start else None
            
            extracted_rows = extractor.extract(
                documents, self.schema, self.table_name, nl_prompt,
                context=context, warm_start=warm_start,
                previous_snapshot=previous_snapshot, violations=violations
            )
            
            self.current_rows = extracted_rows
            self._update_state(TableProcessingState.EXTRACTED)
            
            snapshot = self._create_snapshot(f"extraction_{self.stats['total_extractions'] + 1}")
            self.snapshots.append(snapshot)
            
            self.stats['total_extractions'] += 1
            self.stats['current_rows'] = len(self.current_rows)
            self._update_timestamp()
            
            if self.output_dir:
                self._save_snapshot_to_file(snapshot)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Table {self.table_name} 抽取失败: {e}")
            self._update_state(TableProcessingState.ERROR)
            return False
    
    def verify(self, verifier, context=None) -> List[Violation]:
        """
        执行数据验证
        
        Args:
            verifier: 验证器实例
            context: 处理上下文
            
        Returns:
            发现的违规列表
        """
        self._update_state(TableProcessingState.VERIFYING)
        
        try:
            current_snapshot = self.get_current_snapshot()
            
            violations = verifier.verify(current_snapshot, self.schema, self.table_name, context)
            
            self.violations_history.append(violations)
            self._update_state(TableProcessingState.VERIFIED)
            
            self.stats['total_verifications'] += 1
            self.stats['current_violations'] = len(violations)
            self._update_timestamp()
            
            if self.output_dir and violations:
                self._save_violations_to_file(violations)
            
            return violations
            
        except Exception as e:
            self.logger.error(f"Table {self.table_name} 验证失败: {e}")
            self._update_state(TableProcessingState.ERROR)
            return []
    
    def fix(self, fixer, violations: List[Violation], context=None) -> List[Fix]:
        """
        执行数据修复
        
        Args:
            fixer: 修复器实例
            violations: 需要修复的违规列表
            context: 处理上下文
            
        Returns:
            应用的修复列表
        """
        self._update_state(TableProcessingState.FIXING)
        
        try:
            current_snapshot = self.get_current_snapshot()
            
            all_fixes = []
            if hasattr(fixer, 'fix_batch'):
                all_fixes = fixer.fix_batch(violations, current_snapshot, context)
            else:
                for violation in violations:
                    fixes = fixer.fix(violation, current_snapshot, context)
                    all_fixes.extend(fixes)
            
            applied_fixes = self._apply_fixes(all_fixes)
            
            self.fixes_history.append(applied_fixes)
            self._update_state(TableProcessingState.FIXED)
            
            if applied_fixes:
                snapshot = self._create_snapshot(f"fix_{self.stats['total_fixes'] + 1}")
                self.snapshots.append(snapshot)
                if self.output_dir:
                    self._save_snapshot_to_file(snapshot)
            
            self.stats['total_fixes'] += 1
            self._update_timestamp()
            
            return applied_fixes
            
        except Exception as e:
            self.logger.error(f"Table {self.table_name} 修复失败: {e}")
            self._update_state(TableProcessingState.ERROR)
            return []
    
    def get_current_snapshot(self) -> TableSnapshot:
        """获取当前数据的快照"""
        return self._create_snapshot(f"current_{len(self.snapshots)}")
    
    def get_latest_snapshot(self) -> Optional[TableSnapshot]:
        """获取最新的历史快照"""
        return self.snapshots[-1] if self.snapshots else None
    
    def get_latest_violations(self) -> List[Violation]:
        """获取最新的违规列表"""
        return self.violations_history[-1] if self.violations_history else []
    
    def get_unfixable_violations(self) -> List[Violation]:
        """获取无法修复的违规"""
        latest_violations = self.get_latest_violations()
        latest_fixes = self.fixes_history[-1] if self.fixes_history else []
        
        fixed_violation_ids = {fix.violation_id for fix in latest_fixes if hasattr(fix, 'violation_id') and getattr(fix, 'fix_success', True)}
        
        unfixable = [v for v in latest_violations if v.id not in fixed_violation_ids]
        return unfixable
    
    def load_from_snapshots(self, snapshots_file: str, violations_file: str = None):
        """从快照文件加载Table状态"""
        try:
            if os.path.exists(snapshots_file):
                with open(snapshots_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        snapshot_data = json.loads(line.strip())
                        snapshot = self._deserialize_snapshot(snapshot_data)
                        self.snapshots.append(snapshot)
                
                if self.snapshots:
                    latest_snapshot = self.snapshots[-1]
                    self.current_rows = latest_snapshot.rows
                    self.stats['current_rows'] = len(self.current_rows)
            
            if violations_file and os.path.exists(violations_file):
                violations = []
                with open(violations_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        violation_data = json.loads(line.strip())
                        violation = self._deserialize_violation(violation_data)
                        violations.append(violation)
                
                if violations:
                    self.violations_history.append(violations)
                    self.stats['current_violations'] = len(violations)
            
            self._update_state(TableProcessingState.EXTRACTED if self.current_rows else TableProcessingState.CREATED)
            self._update_timestamp()
            
        except Exception as e:
            self.logger.error(f"从快照加载Table失败: {e}")
    
    def _create_snapshot(self, snapshot_type: str) -> TableSnapshot:
        """创建数据快照"""
        run_id = f"{self.table_id}_{snapshot_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        snapshot = TableSnapshot(
            run_id=run_id,
            table=self.table_name,
            rows=self.current_rows.copy(),
            created_at=IOManager.get_timestamp(),
            table_id=self.table_id
        )
        
        return snapshot
    
    def _apply_fixes(self, fixes: List[Fix]) -> List[Fix]:
        """应用修复到当前数据"""
        applied_fixes = []
        rows_to_delete = set()  # 记录需要删除的行
        
        for fix in fixes:
            try:
                if fix.new == "__DELETED__":
                    rows_to_delete.add(fix.tuple_id)
                    fix.fix_success = True
                    applied_fixes.append(fix)
                    continue
                
                for row in self.current_rows:
                    if row.tuple_id == fix.tuple_id and fix.attr in row.cells:
                        old_value = row.cells[fix.attr].value
                        row.cells[fix.attr].value = fix.new
                        
                        fix.fix_success = True
                        applied_fixes.append(fix)
                        
                        self.logger.info(f"应用修复: {fix.attr} '{old_value}' -> '{fix.new}'")
                        break
                
            except Exception as e:
                self.logger.error(f"应用修复失败: {fix.id} - {e}")
                fix.fix_success = False
                fix.failure_reason = str(e)
        
        if rows_to_delete:
            original_count = len(self.current_rows)
            self.current_rows = [row for row in self.current_rows if row.tuple_id not in rows_to_delete]
            deleted_count = original_count - len(self.current_rows)
            self.logger.info(f"删除了 {deleted_count} 行数据")
        
        rows_with_deleted_values = []
        for row in self.current_rows:
            has_deleted_marker = False
            for attr, cell in row.cells.items():
                if cell.value == "__DELETED__" or str(cell.value) == "__DELETED__":
                    has_deleted_marker = True
                    break
            if has_deleted_marker:
                rows_with_deleted_values.append(row.tuple_id)
        
        if rows_with_deleted_values:
            original_count = len(self.current_rows)
            self.current_rows = [row for row in self.current_rows if row.tuple_id not in rows_with_deleted_values]
            deleted_count = original_count - len(self.current_rows)
            self.logger.info(f"🧹 清理了 {deleted_count} 行包含__DELETED__标记的数据")
        
        return applied_fixes
    
    def _save_snapshot_to_file(self, snapshot: TableSnapshot):
        """保存快照到文件"""
        if not self.output_dir:
            return
        
        snapshots_file = os.path.join(self.output_dir, 'snapshots.jsonl')
        
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            
            with open(snapshots_file, 'a', encoding='utf-8') as f:
                snapshot_data = self._serialize_snapshot(snapshot)
                f.write(json.dumps(snapshot_data, ensure_ascii=False, sort_keys=False) + '\n')
                
        except Exception as e:
            self.logger.error(f"保存快照失败: {e}")
    
    def _save_violations_to_file(self, violations: List[Violation]):
        """保存违规到文件"""
        if not self.output_dir:
            return
        
        violations_file = os.path.join(self.output_dir, 'violations.jsonl')
        
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            
            with open(violations_file, 'w', encoding='utf-8') as f:
                for violation in violations:
                    violation_data = self._serialize_violation(violation)
                    f.write(json.dumps(violation_data, ensure_ascii=False, sort_keys=False) + '\n')
                    
        except Exception as e:
            self.logger.error(f"保存违规失败: {e}")
    
    def _serialize_snapshot(self, snapshot: TableSnapshot) -> Dict[str, Any]:
        """序列化快照为字典"""
        return {
            'run_id': snapshot.run_id,
            'table_id': snapshot.table_id,
            'table': snapshot.table,
            'created_at': snapshot.created_at,
            'rows': [
                {
                    'tuple_id': row.tuple_id,
                    'cells': {
                        attr: {
                            'value': cell.value,
                            'evidences': cell.evidences
                        }
                        for attr, cell in row.cells.items()
                    }
                }
                for row in snapshot.rows
            ]
        }
    
    def _serialize_violation(self, violation: Violation) -> Dict[str, Any]:
        """序列化违规为字典"""
        return {
            'id': violation.id,
            'table': violation.table,
            'tuple_id': violation.tuple_id,
            'attr': violation.attr,
            'constraint_type': violation.constraint_type,
            'description': violation.description,
            'severity': violation.severity,
            'detector_id': violation.detector_id,
            'timestamp': violation.timestamp,
            'processing_category': getattr(violation, 'processing_category', None).value if hasattr(violation, 'processing_category') and hasattr(getattr(violation, 'processing_category'), 'value') else None
        }
    
    def _deserialize_snapshot(self, data: Dict[str, Any]) -> TableSnapshot:
        """从字典反序列化快照"""
        from . import TableRow, CellData  # 避免循环导入
        
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
        
        return TableSnapshot(
            run_id=data.get('run_id', ''),
            table=data.get('table', ''),
            rows=rows,
            created_at=data.get('created_at', ''),
            table_id=data.get('table_id', '')
        )
    
    def _deserialize_violation(self, data: Dict[str, Any]) -> Violation:
        """从字典反序列化违规"""
        violation = Violation(
            id=data.get('id', ''),
            table=data.get('table', ''),
            tuple_id=data.get('tuple_id', ''),
            attr=data.get('attr', ''),
            constraint_type=data.get('constraint_type', ''),
            description=data.get('description', ''),
            severity=data.get('severity', 'warn'),
            detector_id=data.get('detector_id', ''),
            timestamp=data.get('timestamp', '')
        )
        
        if 'processing_category' in data and data['processing_category']:
            from ..agents.verifier.mcp_verifier import ViolationCategory
            try:
                violation.processing_category = ViolationCategory(data['processing_category'])
            except:
                pass
        
        return violation
    
    def _update_state(self, new_state: str):
        """更新处理状态"""
        self.current_state = new_state
        self.logger.info(f"Table {self.table_name} 状态更新: {new_state}")
    
    def _update_timestamp(self):
        """更新时间戳"""
        self.updated_at = datetime.now().isoformat()
    
    def get_summary(self) -> Dict[str, Any]:
        """获取Table摘要信息"""
        return {
            'table_name': self.table_name,
            'table_id': self.table_id,
            'current_state': self.current_state,
            'stats': self.stats,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'snapshots_count': len(self.snapshots),
            'violations_count': len(self.violations_history),
            'fixes_count': len(self.fixes_history)
        }
    
    def __str__(self) -> str:
        return f"Table({self.table_name}, {self.current_state}, {len(self.current_rows)} rows)"
    
    def __repr__(self) -> str:
        return self.__str__()
