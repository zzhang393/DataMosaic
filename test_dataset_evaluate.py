#!/usr/bin/env python3
"""
Doc2DB 数据集评测脚本
用于测评系统在不同数据集上的性能表现
"""

import os
import sys
import json
import time
import glob
import shutil
import requests
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

from dotenv import load_dotenv
env_file = Path(__file__).parent.parent/ "llm" / ".env"
if env_file.exists():
    load_dotenv(env_file)
    for url_var in ['API_URL', 'API_URL1', 'API_URL2', 'DEEPSEEK_URL', 'QWEN_URL']:
        url_value = os.getenv(url_var)
        if url_value and url_value.endswith('/chat/completions'):
            base_url = url_value.replace('/chat/completions', '')
            os.environ[url_var] = base_url
else:
    print(f"Warning: Environment file not found: {env_file}")



logger = logging.getLogger(__name__)

BACKEND_URL = "http://localhost:5000"
DATASET_DIR = Path(__file__).parent.parent / "dataset"
OUTPUT_DIR = Path(__file__).parent.parent / "dataset_output"
POLL_INTERVAL = 2
MAX_WAIT_TIME = 7200

DEFAULT_MODEL = "gpt-4o"
DEFAULT_RUN_NAME = None 


class Doc2DBEvaluator:
    """Doc2DB评估器"""
    
    def __init__(self, backend_url: str, dataset_dir: Path, output_dir: Path,
                 model: str = DEFAULT_MODEL, run_name: Optional[str] = None, 
                 document_mode: str = 'single', enable_llm_eval: bool = True):
        self.backend_url = backend_url
        self.dataset_dir = dataset_dir
        self.output_dir = output_dir
        self.model = model
        self.run_name = run_name
        self.document_mode = document_mode
        self.enable_llm_eval = enable_llm_eval
        self.results = []
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Model: {model}")
        logger.info(f"Document mode: {document_mode}")
        logger.info(f"LLM evaluation: {'enabled' if enable_llm_eval else 'disabled (P/R/F1 only)'}")
        if run_name:
            logger.info(f"Run name: {run_name}")
    
    def _build_filename_prefix(self, db_name: str, case_name: str, model: str) -> str:
        """Build filename prefix: DB_case_model_mode"""
        safe_model = model.replace('/', '_').replace('\\', '_').replace('-', '_')
        mode = self.document_mode
        return f"{db_name}_{case_name}_{safe_model}_{mode}"
    
    def check_case_already_processed(self, case_info: Dict[str, Any]) -> bool:
        """Check if case has been processed"""
        case_dir = self.output_dir / case_info['db_name'] / case_info['case_name']
        
        if not case_dir.exists():
            return False
        
        filename_prefix = self._build_filename_prefix(
            case_info['db_name'], 
            case_info['case_name'], 
            self.model
        )
        
        prefix_dir = case_dir / filename_prefix
        if not prefix_dir.exists() or not prefix_dir.is_dir():
            return False
        
        key_files = [
            prefix_dir / "output_tables.json",
            prefix_dir / "metadata.json"
        ]
        
        for file_path in key_files:
            if file_path.exists():
                logger.info(f"   Found existing output: {filename_prefix}/{file_path.name}")
                return True
        
        return False
        
    def check_backend_health(self) -> bool:
        """检查后端服务是否可用"""
        try:
            response = requests.get(f"{self.backend_url}/api/health", timeout=5)
            if response.status_code == 200:
                logger.info(f" 后端服务正常: {self.backend_url}")
                return True
            else:
                logger.error(f" 后端服务异常，状态码: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f" 无法连接到后端服务: {e}")
            return False
    
    def scan_datasets(self) -> List[Dict[str, Any]]:
        """扫描所有数据集和case"""
        cases = []
        
        if not self.dataset_dir.exists():
            logger.error(f" 数据集目录不存在: {self.dataset_dir}")
            return cases
        
        for db_dir in self.dataset_dir.iterdir():
            if not db_dir.is_dir():
                continue
            
            db_name = db_dir.name
            logger.info(f" 扫描数据库: {db_name}")
            
            for case_dir in db_dir.iterdir():
                if not case_dir.is_dir() or not case_dir.name.startswith("case"):
                    continue
                
                case_name = case_dir.name
                
                docs_dir = case_dir / "docs"
                schema_file = case_dir / "schema.json"
                answer_file = case_dir / "answer.json"
                
                if not docs_dir.exists():
                    logger.warning(f"️  {db_name}/{case_name}: docs目录不存在，跳过")
                    continue
                
                if not schema_file.exists():
                    logger.warning(f"️  {db_name}/{case_name}: schema.json不存在，跳过")
                    continue
                
                if not answer_file.exists():
                    logger.warning(f"️  {db_name}/{case_name}: answer.json不存在，跳过")
                    continue
                
                doc_files = []
                for ext in ['*.txt', '*.pdf', '*.md', '*.docx', '*.doc']:
                    doc_files.extend(list(docs_dir.glob(ext)))
                
                if not doc_files:
                    logger.warning(f"️  {db_name}/{case_name}: docs目录为空，跳过")
                    continue
                
                cases.append({
                    'db_name': db_name,
                    'case_name': case_name,
                    'case_dir': case_dir,
                    'docs_dir': docs_dir,
                    'schema_file': schema_file,
                    'answer_file': answer_file,
                    'doc_files': [str(f) for f in doc_files]
                })
                
                logger.info(f"   {db_name}/{case_name}: 发现 {len(doc_files)} 个文档文件")
        
        logger.info(f"\n 总共发现 {len(cases)} 个有效测试案例\n")
        return cases
    
    def upload_documents(self, doc_files: List[str]) -> List[str]:
        """上传文档到后端"""
        uploaded_paths = []
        
        for doc_file in doc_files:
            try:
                with open(doc_file, 'rb') as f:
                    files = {'file': (os.path.basename(doc_file), f)}
                    response = requests.post(
                        f"{self.backend_url}/api/upload",
                        files=files,
                        timeout=60
                    )
                
                if response.status_code == 200:
                    result = response.json()
                    uploaded_path = result.get('filepath', '')
                    uploaded_paths.append(uploaded_path)
                    logger.info(f"   上传成功: {os.path.basename(doc_file)}")
                elif response.status_code == 409:
                    result = response.json()
                    uploaded_path = result.get('filepath', '')
                    uploaded_paths.append(uploaded_path)
                    logger.info(f"  ⊙ 文件已存在，跳过上传: {os.path.basename(doc_file)}")
                else:
                    logger.error(f"   上传失败: {os.path.basename(doc_file)} - {response.text}")
                    
            except Exception as e:
                logger.error(f"   上传异常: {os.path.basename(doc_file)} - {e}")
        
        return uploaded_paths
    
    def start_processing(self, files: List[str], schema: Dict[str, Any]) -> Optional[str]:
        """启动文档处理任务"""
        try:
            converted_schema = self._convert_schema_format(schema)
            
            table_name = ''
            tables_count = len(converted_schema.get('tables', []))
            if tables_count == 1:
                table_name = converted_schema['tables'][0]['name']
            elif tables_count > 1:
                table_name = ''
                logger.info(f"Schema contains {tables_count} tables, will process all tables")
            
            request_data = {
                'files': files,
                'schema': json.dumps(converted_schema),
                'table_name': table_name,
                'input_mode': 'schema',
                'schema_mode': 'json',
                'model': self.model,
                'nl_prompt': '',
                'processing_mode': 'serial',
                'max_concurrent_tables': 3,
                'document_mode': self.document_mode
            }
            
            response = requests.post(
                f"{self.backend_url}/api/doc2db/process",
                json=request_data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                task_id = result.get('task_id')
                logger.info(f"Processing task started: {task_id}")
                return task_id
            else:
                logger.error(f"Failed to start processing: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error starting processing: {e}")
            return None
    
    def _convert_schema_format(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Convert schema format: 'fields' to 'attributes'"""
        if not isinstance(schema, dict):
            return schema
        
        converted = schema.copy()
        
        if 'tables' in converted:
            new_tables = []
            for table in converted['tables']:
                new_table = table.copy()
                if 'fields' in new_table:
                    new_table['attributes'] = new_table.pop('fields')
                new_tables.append(new_table)
            converted['tables'] = new_tables
        
        logger.info(f"  ️  Schema format converted (fields -> attributes)")
        return converted
    
    def wait_for_completion(self, task_id: str) -> Optional[Dict[str, Any]]:
        """等待任务完成并返回结果"""
        start_time = time.time()
        
        while True:
            try:
                elapsed = time.time() - start_time
                if elapsed > MAX_WAIT_TIME:
                    logger.error(f"   任务超时 ({MAX_WAIT_TIME}秒)")
                    return None
                
                response = requests.get(
                    f"{self.backend_url}/api/doc2db/status/{task_id}",
                    timeout=10
                )
                
                if response.status_code != 200:
                    logger.error(f"   获取状态失败: {response.text}")
                    time.sleep(POLL_INTERVAL)
                    continue
                
                status_data = response.json()
                status = status_data.get('status', 'unknown')
                current_step = status_data.get('current_step', '')
                
                if current_step:
                    logger.info(f"   处理中 [{int(elapsed)}s]: {current_step}")
                
                if status == 'completed':
                    logger.info(f"   任务完成 (耗时 {int(elapsed)}秒)")
                    return status_data
                elif status == 'failed':
                    error_msg = status_data.get('error', '未知错误')
                    logger.error(f"   任务失败: {error_msg}")
                    return None
                
                time.sleep(POLL_INTERVAL)
                
            except Exception as e:
                logger.error(f"   轮询状态异常: {e}")
                time.sleep(POLL_INTERVAL)
    
    def extract_result_data(self, status_data: Dict[str, Any], task_id: str) -> Optional[Dict[str, Any]]:
        """从状态数据中提取结果"""
        try:
            result = status_data.get('result', {})
            result_context = status_data.get('result_context', {})
            
            tables = result.get('tables', {})
            if not tables and result_context:
                tables = result_context.get('tables', {})
            
            steps = status_data.get('steps', [])
            
            return {
                'tables': tables,
                'summary': result.get('summary', result_context.get('summary', {})),
                'full_result': result if result else result_context,
                'steps': steps,  # 保存所有处理步骤
                'task_id': task_id
            }
            
        except Exception as e:
            logger.error(f"   提取结果数据失败: {e}")
            return None
    
    def load_result_from_output(self, prefix_output_dir: Path, filename_prefix: str) -> Optional[Dict[str, Any]]:
        """从输出目录加载最终结果（支持多种输出格式）"""
        try:
            output_tables_file = prefix_output_dir / "output_tables.json"
            
            if output_tables_file.exists():
                logger.info(f"Reading from output_tables.json...")
                with open(output_tables_file, 'r', encoding='utf-8') as f:
                    tables = json.load(f)
                
                if not tables:
                    logger.warning(f"output_tables.json is empty")
                    return None
                
                logger.info(f"Loaded {len(tables)} tables from output_tables.json")
                return {'tables': tables}
            
            extracted_data_file = prefix_output_dir / "extracted_data.json"
            if extracted_data_file.exists():
                logger.info(f"Reading from extracted_data.json...")
                with open(extracted_data_file, 'r', encoding='utf-8') as f:
                    tables = json.load(f)
                
                if not tables:
                    logger.warning(f"extracted_data.json is empty")
                    return None
                
                logger.info(f"Loaded {len(tables)} tables from extracted_data.json")
                return {'tables': tables}
            
            extracted_table_file = prefix_output_dir / "extracted_table.json"
            if extracted_table_file.exists():
                logger.info(f"Reading from extracted_table.json...")
                with open(extracted_table_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if 'documents' in data and isinstance(data['documents'], list):
                    merged_tables = self._merge_documents_tables(data['documents'])
                    
                    if not merged_tables:
                        logger.warning(f"No valid table data in extracted_table.json")
                        return None
                    
                    logger.info(f"Loaded and merged {len(merged_tables)} tables from extracted_table.json (documents format)")
                    return {'tables': merged_tables}
                
                elif isinstance(data, dict):
                    tables = {}
                    metadata_fields = {'metadata', 'version', 'generator', 'merged_files', 'tables', 'relations'}
                    
                    for key, value in data.items():
                        if key in metadata_fields:
                            continue
                        if isinstance(value, list):
                            tables[key] = value
                    
                    if not tables:
                        logger.warning(f"No valid table data in extracted_table.json")
                        return None
                    
                    logger.info(f"Loaded {len(tables)} tables from extracted_table.json (direct dict format)")
                    return {'tables': tables}
                
                else:
                    logger.warning(f"Unsupported format in extracted_table.json")
                    return None
            
            logger.warning(f"Not found: output_tables.json, extracted_data.json or extracted_table.json in {prefix_output_dir}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to load output result: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _merge_documents_tables(self, documents: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """合并 documents 数组中的表数据
        
        extracted_table.json 的格式：
        {
          "documents": [
            {
              "court": [...],
              "case": [...],
              "person": [...]
            },
            {
              "court": [...],
              "case": [...]
            }
          ]
        }
        
        需要转换为：
        {
          "court": [...],  # 合并所有documents中的court数组
          "case": [...],
          "person": [...]
        }
        
        Args:
            documents: documents 数组
            
        Returns:
            合并后的表数据字典 {table_name: [row_dict, ...]}
        """
        merged_tables = {}
        
        try:
            for doc_idx, document in enumerate(documents):
                if not isinstance(document, dict):
                    logger.warning(f"  ️  document[{doc_idx}] 不是字典类型，跳过")
                    continue
                
                for table_name, table_data in document.items():
                    if table_name in ['metadata', 'version', 'generator', 'merged_files']:
                        continue
                    
                    if not isinstance(table_data, list):
                        logger.warning(f"  ️  表 {table_name} 在 document[{doc_idx}] 中不是数组类型，跳过")
                        continue
                    
                    if table_name not in merged_tables:
                        merged_tables[table_name] = []
                    
                    merged_tables[table_name].extend(table_data)
            
            for table_name, rows in merged_tables.items():
                if rows:
                    seen = set()
                    unique_rows = []
                    for row in rows:
                        row_str = json.dumps(row, sort_keys=True, ensure_ascii=False)
                        if row_str not in seen:
                            seen.add(row_str)
                            unique_rows.append(row)
                    
                    merged_tables[table_name] = unique_rows
                    logger.info(f"    - 表 {table_name}: 合并后共 {len(unique_rows)} 行（去重前 {len(rows)} 行）")
            
            return merged_tables
            
        except Exception as e:
            logger.error(f"   合并 documents 表数据失败: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def _load_best_snapshots_from_file(self, snapshots_file: Path) -> Dict[str, List[Dict[str, Any]]]:
        """从snapshots.jsonl文件中找到数据最完整的快照（参考utils.py的实现）
        
        优先级（按Table的完成状态判断）：
        1. final阶段的快照（处理完成时保存的最终状态，数据最完整且经过所有验证和修复）
        2. verification阶段的快照（经过提取和验证，数据较完整）
        3. fixing阶段的快照（经过修复）
        4. extraction阶段的快照（初始提取的数据）
        
        返回: {table_name: [row_dict, ...]}
        """
        try:
            snapshots_by_table = {}
            with open(snapshots_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        snapshot_dict = json.loads(line)
                        table_name = snapshot_dict.get('table')
                        stage = snapshot_dict.get('processing_stage', '')
                        rows_count = snapshot_dict.get('rows_count', 0)
                        
                        if table_name:
                            if table_name not in snapshots_by_table:
                                snapshots_by_table[table_name] = []
                            snapshots_by_table[table_name].append({
                                'dict': snapshot_dict,
                                'stage': stage,
                                'rows_count': rows_count
                            })
                    except Exception as e:
                        continue
            
            tables_data = {}
            for table_name, snapshots_list in snapshots_by_table.items():
                snapshots_list = [
                    s for s in snapshots_list 
                    if 'Batch验证' not in s.get('dict', {}).get('stage_description', '')
                ]
                
                final_snapshots = [s for s in snapshots_list if s['stage'] == 'final']
                verification_snapshots = [s for s in snapshots_list if s['stage'] == 'verification']
                fixing_snapshots = [s for s in snapshots_list if s['stage'] == 'fixing']
                extraction_snapshots = [s for s in snapshots_list if s['stage'] == 'extraction']
                
                chosen_snapshot = None
                chosen_reason = ''
                
                if final_snapshots:
                    chosen_snapshot = final_snapshots[-1]
                    chosen_reason = f'final阶段（完成状态），{chosen_snapshot["rows_count"]}行'
                else:
                    all_candidates = []
                    
                    if verification_snapshots:
                        all_candidates.extend([(s, 'verification') for s in verification_snapshots])
                    if fixing_snapshots:
                        all_candidates.extend([(s, 'fixing') for s in fixing_snapshots])
                    if extraction_snapshots:
                        all_candidates.extend([(s, 'extraction') for s in extraction_snapshots])
                    
                    if all_candidates:
                        chosen_snapshot, stage = max(all_candidates, key=lambda x: x[0]['rows_count'])
                        chosen_reason = f'{stage}阶段，{chosen_snapshot["rows_count"]}行'
                
                if chosen_snapshot:
                    logger.info(f"    - 表 {table_name}: 选择{chosen_reason}")
                    
                    snapshot_dict = chosen_snapshot['dict']
                    rows = snapshot_dict.get('rows', [])
                    
                    table_rows = []
                    for row in rows:
                        row_dict = {}
                        cells = row.get('cells', {})
                        for attr_name, cell_data in cells.items():
                            if isinstance(cell_data, dict):
                                if 'value' in cell_data:
                                    row_dict[attr_name] = cell_data['value']
                                elif 'best' in cell_data and isinstance(cell_data['best'], dict):
                                    row_dict[attr_name] = cell_data['best'].get('value', '')
                                else:
                                    row_dict[attr_name] = ''
                            else:
                                row_dict[attr_name] = cell_data
                        if row_dict:
                            table_rows.append(row_dict)
                    
                    if table_rows:
                        tables_data[table_name] = table_rows
            
            return tables_data
            
        except Exception as e:
            logger.error(f"   从snapshots.jsonl读取快照失败: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def _extract_tables_from_full_result(self, full_result: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """从 full_result 中提取表数据（支持多表）
        
        Args:
            full_result: 完整的结果数据（包含snapshot等）
            
        Returns:
            {table_name: [row_dict, ...]} 格式的表数据
        """
        tables = {}
        
        try:
            if 'tables' in full_result and isinstance(full_result['tables'], dict):
                tables = full_result['tables']
                if tables:
                    logger.info(f"   从 full_result.tables 获取 {len(tables)} 个表")
                    return tables
            
            if 'snapshot' in full_result and 'rows' in full_result['snapshot']:
                table_name = full_result['snapshot'].get('table', 'unknown')
                if table_name == 'unknown' and full_result['snapshot']['rows']:
                    first_tuple_id = full_result['snapshot']['rows'][0].get('tuple_id', '')
                    if first_tuple_id.startswith('t-'):
                        parts = first_tuple_id.split('-')
                        if len(parts) >= 2:
                            table_name = parts[1]
                
                rows = []
                for row in full_result['snapshot']['rows']:
                    row_dict = {}
                    if 'cells' in row:
                        for attr_name, cell_data in row['cells'].items():
                            if isinstance(cell_data, dict):
                                if 'best' in cell_data and isinstance(cell_data['best'], dict):
                                    row_dict[attr_name] = cell_data['best'].get('value', '')
                                elif 'value' in cell_data:
                                    row_dict[attr_name] = cell_data['value']
                                else:
                                    row_dict[attr_name] = ''
                            else:
                                row_dict[attr_name] = cell_data
                    if row_dict:
                        rows.append(row_dict)
                
                if rows:
                    tables[table_name] = rows
                    logger.info(f"  ️  从 full_result.snapshot 提取表 {table_name}: {len(rows)} 行（仅单表）")
            
            elif 'table_data' in full_result and 'rows' in full_result['table_data']:
                table_name = full_result.get('summary', {}).get('table_name', 'unknown')
                rows = []
                for row in full_result['table_data']['rows']:
                    row_dict = {}
                    for attr_name, cell_data in row.items():
                        if isinstance(cell_data, dict) and 'value' in cell_data:
                            row_dict[attr_name] = cell_data['value']
                        else:
                            row_dict[attr_name] = cell_data
                    if row_dict:
                        rows.append(row_dict)
                if rows:
                    tables[table_name] = rows
                    logger.info(f"  ️  从 full_result.table_data 提取表 {table_name}: {len(rows)} 行（仅单表）")
                    
        except Exception as e:
            logger.error(f"   从 full_result 提取表数据失败: {e}")
            import traceback
            traceback.print_exc()
        
        return tables
    
    def save_output(self, case_info: Dict[str, Any], result_data: Dict[str, Any]) -> tuple[Path, str]:
        """
        保存处理结果到输出目录
        新结构：output_dir / db_name / case_name / prefix / files
        返回：(prefix_output_dir, filename_prefix)
        """
        case_dir = self.output_dir / case_info['db_name'] / case_info['case_name']
        case_dir.mkdir(parents=True, exist_ok=True)
        
        filename_prefix = self._build_filename_prefix(
            case_info['db_name'], 
            case_info['case_name'], 
            self.model
        )
        
        prefix_output_dir = case_dir / filename_prefix
        prefix_output_dir.mkdir(parents=True, exist_ok=True)
        
        metadata = {
            'model': self.model,
            'run_name': self.run_name,
            'document_mode': self.document_mode,  # 记录文档模式
            'db_name': case_info['db_name'],
            'case_name': case_info['case_name'],
            'filename_prefix': filename_prefix,
            'task_id': result_data.get('task_id', '')
        }
        
        metadata_file = prefix_output_dir / "metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        task_id = result_data.get('task_id')
        tables_data = {}
        
        if task_id:
            backend_output_dir = Path(__file__).parent.parent / "backend" / "output" / task_id
            backend_result_file = backend_output_dir / "result.json"
            
            if backend_result_file.exists():
                logger.info(f"  ️  从后端输出目录读取完整的 result.json")
                with open(backend_result_file, 'r', encoding='utf-8') as f:
                    backend_result = json.load(f)
                tables_data = backend_result.get('tables', {})
                logger.info(f"   从后端 result.json 读取 {len(tables_data)} 个表")
            else:
                logger.warning(f"  ️  后端输出目录不存在: {backend_output_dir}")
        
        if not tables_data:
            logger.info(f"  ️  从 full_result 提取表数据")
            tables_data = self._extract_tables_from_full_result(result_data['full_result'])
        
        tables_file = prefix_output_dir / "output_tables.json"
        with open(tables_file, 'w', encoding='utf-8') as f:
            json.dump(tables_data, f, ensure_ascii=False, indent=2)
        logger.info(f"   保存output_tables.json: {len(tables_data)}个表")
        
        steps_file = prefix_output_dir / "processing_steps.json"
        with open(steps_file, 'w', encoding='utf-8') as f:
            json.dump(result_data['steps'], f, ensure_ascii=False, indent=2)
        
        logger.info(f"   结果已保存到: {prefix_output_dir}")
        logger.info(f"   文件夹: {filename_prefix}")
        return prefix_output_dir, filename_prefix
    
    def get_value_type(self, value: Any) -> str:
        """判断值的类型"""
        if value is None or value == "":
            return "null"
        
        if isinstance(value, (int, float)):
            return "numeric"
        
        if isinstance(value, str):
            try:
                float(value.replace(',', ''))  # 处理千分位分隔符
                return "numeric"
            except ValueError:
                pass
            
            import re
            if re.match(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}', value.strip()):
                return "date"
            
            if value.strip().lower() in ['true', 'false', 'yes', 'no', '是', '否']:
                return "boolean"
        
        if isinstance(value, bool):
            return "boolean"
        
        return "string"
    
    def normalize_numeric_value(self, value: Any) -> tuple[float, int]:
        """标准化数值，返回(数值, 小数位数)"""
        if value is None or value == "":
            return None, 0
        
        try:
            if isinstance(value, str):
                clean_value = value.replace(',', '').replace(' ', '').strip()
                num_value = float(clean_value)
            else:
                num_value = float(value)
            
            if isinstance(value, str) and '.' in value:
                decimal_places = len(value.split('.')[-1].rstrip('0'))
            elif isinstance(value, float) and not value.is_integer():
                decimal_places = len(str(value).split('.')[-1].rstrip('0'))
            else:
                decimal_places = 0
                
            return num_value, decimal_places
        except (ValueError, TypeError):
            return None, 0
    
    def normalize_string_value(self, value: Any) -> str:
        """标准化字符串值"""
        if value is None or value == "":
            return ""
        
        text = str(value).strip()
        import re
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def _is_chinese_entity_match(self, str1: str, str2: str) -> bool:
        """Check if two strings match as Chinese entity abbreviations"""
        if not str1 or not str2:
            return False
        
        import re
        
        s1_no_space = re.sub(r'\s+', '', str1)
        s2_no_space = re.sub(r'\s+', '', str2)
        
        if len(s1_no_space) >= 4 and s1_no_space in s2_no_space:
            company_suffixes = ['股份有限公司', '有限公司', '公司', '集团', '股份', '(集团)', '（集团）']
            s2_core = s2_no_space
            for suffix in company_suffixes:
                s2_core = s2_core.replace(suffix, '')
            if s1_no_space in s2_core or s2_core in s1_no_space:
                return True
                
        if len(s2_no_space) >= 4 and s2_no_space in s1_no_space:
            company_suffixes = ['股份有限公司', '有限公司', '公司', '集团', '股份', '(集团)', '（集团）']
            s1_core = s1_no_space
            for suffix in company_suffixes:
                s1_core = s1_core.replace(suffix, '')
            if s2_no_space in s1_core or s1_core in s2_no_space:
                return True
        
        year_pattern = r'20\d{2}'
        quarter_patterns = {
            'Q1': ['q1', '一季度', '第一季度', '1季度'],
            'Q2': ['q2', '二季度', '第二季度', '2季度'],
            'Q3': ['q3', '三季度', '第三季度', '3季度'],
            'Q4': ['q4', '四季度', '第四季度', '4季度'],
        }
        
        year1_match = re.search(year_pattern, str1)
        year2_match = re.search(year_pattern, str2)
        
        if year1_match and year2_match:
            year1 = year1_match.group()
            year2 = year2_match.group()
            
            if year1 == year2:
                str1_lower = str1.lower()
                str2_lower = str2.lower()
                
                for quarter, patterns in quarter_patterns.items():
                    found_in_s1 = quarter.lower() in str1_lower or any(p in str1_lower for p in patterns)
                    found_in_s2 = quarter.lower() in str2_lower or any(p in str2_lower for p in patterns)
                    
                    if found_in_s1 and found_in_s2:
                        return True
        
        chinese_words_1 = set(re.findall(r'[\u4e00-\u9fa5]{3,}', s1_no_space))
        chinese_words_2 = set(re.findall(r'[\u4e00-\u9fa5]{3,}', s2_no_space))
        
        if chinese_words_1 and chinese_words_2:
            common_words = chinese_words_1 & chinese_words_2
            if common_words:
                min_words_count = min(len(chinese_words_1), len(chinese_words_2))
                if len(common_words) / min_words_count >= 0.5:
                    return True
        
        return False
    
    def calculate_string_similarity(self, str1: str, str2: str) -> float:
        """计算字符串相似度（使用编辑距离）"""
        if str1 == str2:
            return 1.0
        
        s1 = self.normalize_string_value(str1).lower()
        s2 = self.normalize_string_value(str2).lower()
        
        if s1 == s2:
            return 1.0
        
        if self._is_chinese_entity_match(s1, s2):
            return 0.95  # 给予高相似度但不是完全匹配
        
        def levenshtein_distance(a, b):
            if len(a) < len(b):
                return levenshtein_distance(b, a)
            
            if len(b) == 0:
                return len(a)
            
            previous_row = list(range(len(b) + 1))
            for i, c1 in enumerate(a):
                current_row = [i + 1]
                for j, c2 in enumerate(b):
                    insertions = previous_row[j + 1] + 1
                    deletions = current_row[j] + 1
                    substitutions = previous_row[j] + (c1 != c2)
                    current_row.append(min(insertions, deletions, substitutions))
                previous_row = current_row
            
            return previous_row[-1]
        
        max_len = max(len(s1), len(s2))
        if max_len == 0:
            return 1.0
        
        distance = levenshtein_distance(s1, s2)
        similarity = 1 - (distance / max_len)
        return max(0, similarity)
    
    def _is_descriptive_field_match(self, expected_str: str, generated_str: str) -> bool:
        """Check if descriptive fields match using keyword extraction"""
        if not expected_str or not generated_str:
            return False
        
        exp = expected_str.lower().strip()
        gen = generated_str.lower().strip()
        
        if len(gen) > len(exp) * 2:
            import re
            exp_clean = re.sub(r'[^\w]', '', exp)
            gen_clean = re.sub(r'[^\w]', '', gen)
            if exp_clean in gen_clean:
                return True
        
        if exp in gen or gen in exp:
            return True
        
        import re
        
        stop_words = {
            '的', '了', '和', '是', '在', '有', '与', '对', '及', '等', 
            '等等', '为', '以', '由', '被', '将', '可', '也', '都', '而',
            '本', '该', '此', '这', '那', '其', '之', '于', '从', '到',
            '课程', '本课程', '该课程', '学生', '设计', '提供',
            '介绍', '涵盖', '探讨', '包括', '教授'
        }
        
        def extract_keywords(text):
            keywords = set()
            
            chinese_phrases = re.findall(r'[\u4e00-\u9fa5]{2,6}', text)
            for phrase in chinese_phrases:
                if phrase not in stop_words:
                    keywords.add(phrase)
                    if len(phrase) > 2:
                        for i in range(len(phrase) - 1):
                            sub_phrase = phrase[i:i+2]
                            if sub_phrase not in stop_words:
                                keywords.add(sub_phrase)
            
            english_words = re.findall(r'[a-z0-9]+', text.lower())
            for word in english_words:
                if len(word) > 1 and word not in stop_words:
                    keywords.add(word)
            
            return keywords
        
        expected_keywords = extract_keywords(exp)
        generated_keywords = extract_keywords(gen)
        
        if not expected_keywords:
            return True
        
        matched_keywords = expected_keywords & generated_keywords
        keyword_match_ratio = len(matched_keywords) / len(expected_keywords)
        
        if keyword_match_ratio >= 0.3:
            return True
        
        core_concepts_matched = 0
        for exp_keyword in expected_keywords:
            for gen_keyword in generated_keywords:
                if exp_keyword in gen_keyword or gen_keyword in exp_keyword:
                    core_concepts_matched += 1
                    break
        
        partial_match_ratio = core_concepts_matched / len(expected_keywords) if expected_keywords else 0
        
        return partial_match_ratio >= 0.5
    
    def compare_cell_values(self, expected: Any, generated: Any) -> dict:
        """精细化比较两个cell值"""
        result = {
            'match': False,
            'match_type': 'none',
            'similarity': 0.0,
            'expected_type': self.get_value_type(expected),
            'generated_type': self.get_value_type(generated),
            'details': {}
        }
        
        if result['expected_type'] == 'null' and result['generated_type'] == 'null':
            result['match'] = True
            result['match_type'] = 'exact'
            result['similarity'] = 1.0
            return result
        
        if result['expected_type'] == 'null' or result['generated_type'] == 'null':
            return result
        
        if result['expected_type'] == 'numeric' or result['generated_type'] == 'numeric':
            exp_val, exp_decimals = self.normalize_numeric_value(expected)
            gen_val, gen_decimals = self.normalize_numeric_value(generated)
            
            if exp_val is not None and gen_val is not None:
                if exp_val == gen_val:
                    result['match'] = True
                    result['match_type'] = 'exact_numeric'
                    result['similarity'] = 1.0
                    
                    if result['expected_type'] != result['generated_type']:
                        result['details']['storage_type_mismatch'] = True
                        result['details']['note'] = 'INTEGER vs VARCHAR - value matches but storage type differs'
                    
                    if exp_decimals == gen_decimals:
                        result['details']['decimal_precision_match'] = True
                    else:
                        result['details']['decimal_precision_match'] = False
                        result['details']['expected_decimals'] = exp_decimals
                        result['details']['generated_decimals'] = gen_decimals
                else:
                    if exp_val != 0:
                        relative_error = abs(exp_val - gen_val) / abs(exp_val)
                        result['similarity'] = max(0, 1 - relative_error)
                    
                result['details']['expected_value'] = exp_val
                result['details']['generated_value'] = gen_val
                return result
        
        
        elif result['expected_type'] == 'string' or result['generated_type'] == 'string':
            exp_str = self.normalize_string_value(expected)
            gen_str = self.normalize_string_value(generated)
            
            similarity = self.calculate_string_similarity(exp_str, gen_str)
            result['similarity'] = similarity
            
            import re
            has_chinese = bool(re.search(r'[\u4e00-\u9fa5]', exp_str)) or bool(re.search(r'[\u4e00-\u9fa5]', gen_str))
            
            similarity_threshold = 0.85 if has_chinese else 0.9
            
            if similarity >= similarity_threshold:
                result['match'] = True
                result['match_type'] = 'string_similar'
                if has_chinese:
                    result['details']['chinese_match'] = True
            elif exp_str.lower() == gen_str.lower():
                result['match'] = True
                result['match_type'] = 'case_insensitive'
                result['similarity'] = 1.0
            elif self._is_descriptive_field_match(exp_str, gen_str):
                result['match'] = True
                result['match_type'] = 'keyword_match'
                result['similarity'] = 0.85
                result['details']['match_method'] = 'keyword_based'
            
            result['details']['expected_string'] = exp_str
            result['details']['generated_string'] = gen_str
        
        
        elif result['expected_type'] == 'date' or result['generated_type'] == 'date':
            exp_str = self.normalize_string_value(expected)
            gen_str = self.normalize_string_value(generated)
            
            import re
            exp_normalized = re.sub(r'[-/\s]+', '-', exp_str)
            gen_normalized = re.sub(r'[-/\s]+', '-', gen_str)
            
            if exp_normalized == gen_normalized:
                result['match'] = True
                result['match_type'] = 'date_normalized'
                result['similarity'] = 1.0
        
        
        elif result['expected_type'] == 'boolean' and result['generated_type'] == 'boolean':
            exp_bool = str(expected).lower() in ['true', 'yes', '是', '1']
            gen_bool = str(generated).lower() in ['true', 'yes', '是', '1']
            
            if exp_bool == gen_bool:
                result['match'] = True
                result['match_type'] = 'boolean'
                result['similarity'] = 1.0
        
        return result
    
    def _find_best_matching_row(self, expected_row: dict, generated_rows: list, used_indices: set) -> tuple:
        """Find best matching row based on content similarity"""
        if not isinstance(expected_row, dict) or not expected_row:
            return None, 0.0
        
        best_idx = None
        best_score = 0.0
        
        for idx, gen_row in enumerate(generated_rows):
            if idx in used_indices or not isinstance(gen_row, dict):
                continue
            
            row_score = self._calculate_row_similarity(expected_row, gen_row)
            
            if row_score > best_score:
                best_score = row_score
                best_idx = idx
        
        if best_score >= 0.3:
            return best_idx, best_score
        else:
            return None, 0.0
    
    def _calculate_row_similarity(self, row1: dict, row2: dict) -> float:
        """Calculate similarity between two rows"""
        if not row1 or not row2:
            return 0.0
        
        fields1 = {k.lower(): k for k in row1.keys()}
        fields2 = {k.lower(): k for k in row2.keys()}
        common_fields = set(fields1.keys()) & set(fields2.keys())
        
        if not common_fields:
            return 0.0
        
        field_scores = []
        for field_lower in common_fields:
            field1 = fields1[field_lower]
            field2 = fields2[field_lower]
            
            val1 = row1[field1]
            val2 = row2[field2]
            
            comparison = self.compare_cell_values(val1, val2)
            
            if comparison['match']:
                field_scores.append(1.0)
            else:
                field_scores.append(comparison['similarity'])
        
        return sum(field_scores) / len(field_scores) if field_scores else 0.0
    
    def _is_auto_generated_id_field(self, field_name: str) -> bool:
        """Check if field is an auto-generated ID field"""
        field_lower = field_name.lower()
        return field_lower.endswith('companyid')
    
    def calculate_cell_metrics(self, generated: Dict[str, Any], expected: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cell-level recall and precision metrics"""
        try:
            comparison_results = []
            table_stats = {}
            skipped_id_fields = set()
            
            for table_name, expected_rows in expected.items():
                if not isinstance(expected_rows, list):
                    continue
                
                table_name_lower = table_name.lower()
                table_stats[table_name_lower] = {
                    'expected_cells': 0,
                    'generated_cells': 0,
                    'correct_cells': 0,
                    'exact_matches': 0,
                    'numeric_precision_errors': 0,
                    'string_similarity_matches': 0,
                    'type_mismatches': 0,
                    'expected_rows': len(expected_rows),
                    'generated_rows': 0,
                    'detailed_comparisons': []
                }
                
                generated_rows = None
                matched_gen_table_name = None
                for gen_table_name, gen_rows in generated.items():
                    gen_name_lower = gen_table_name.lower()
                    if gen_name_lower == table_name_lower:
                        generated_rows = gen_rows
                        matched_gen_table_name = gen_table_name
                        break
                    if gen_name_lower.rstrip('s') == table_name_lower.rstrip('s'):
                        generated_rows = gen_rows
                        matched_gen_table_name = gen_table_name
                        logger.info(f"  ️  Table name plural mismatch: expected '{table_name}' vs generated '{gen_table_name}'")
                        break
                
                if generated_rows is None:
                    logger.warning(f"   No matching generated table found: '{table_name}' (available: {list(generated.keys())})")
                    for row_idx, row in enumerate(expected_rows):
                        if isinstance(row, dict):
                            for attr, value in row.items():
                                if value is not None and value != "":
                                    table_stats[table_name_lower]['expected_cells'] += 1
                    continue
                
                table_stats[table_name_lower]['generated_rows'] = len(generated_rows)
                
                used_generated_indices = set()
                
                
                for exp_row_idx, expected_row in enumerate(expected_rows):
                    if not isinstance(expected_row, dict):
                        continue
                    
                    best_gen_idx, match_score = self._find_best_matching_row(
                        expected_row, generated_rows, used_generated_indices
                    )
                    
                    if best_gen_idx is not None:
                        generated_row = generated_rows[best_gen_idx]
                        used_generated_indices.add(best_gen_idx)
                    else:
                        generated_row = {}
                    
                    all_fields = set()
                    if isinstance(expected_row, dict):
                        all_fields.update(expected_row.keys())
                    if isinstance(generated_row, dict):
                        all_fields.update(generated_row.keys())
                    
                    for field in all_fields:
                        field_lower = field.lower()
                        
                        if self._is_auto_generated_id_field(field):
                            skipped_id_fields.add(field)
                            continue
                        
                        expected_val = expected_row.get(field) if isinstance(expected_row, dict) else None
                        
                        generated_val = None
                        if isinstance(generated_row, dict):
                            for gen_field, gen_val in generated_row.items():
                                if gen_field.lower() == field_lower:
                                    generated_val = gen_val
                                    break
                        
                        if field in expected_row:
                            table_stats[table_name_lower]['expected_cells'] += 1
                        
                        if generated_row and field_lower in [f.lower() for f in generated_row.keys()]:
                            table_stats[table_name_lower]['generated_cells'] += 1
                        
                        if (expected_val is not None and expected_val != "") or (generated_val is not None and generated_val != ""):
                            comparison = self.compare_cell_values(expected_val, generated_val)
                            comparison['table'] = table_name_lower
                            comparison['row_index'] = exp_row_idx
                            comparison['field'] = field_lower
                            comparison['expected_value'] = expected_val
                            comparison['generated_value'] = generated_val
                            
                            comparison_results.append(comparison)
                            table_stats[table_name_lower]['detailed_comparisons'].append(comparison)
                            
                            if comparison['match']:
                                table_stats[table_name_lower]['correct_cells'] += 1
                                
                                if comparison['match_type'] == 'exact_numeric':
                                    table_stats[table_name_lower]['exact_matches'] += 1
                                    if not comparison['details'].get('decimal_precision_match', True):
                                        table_stats[table_name_lower]['numeric_precision_errors'] += 1
                                elif comparison['match_type'] in ['string_similar', 'case_insensitive', 'keyword_match']:
                                    table_stats[table_name_lower]['string_similarity_matches'] += 1
                                else:
                                    table_stats[table_name_lower]['exact_matches'] += 1
                            else:
                                if comparison['expected_type'] != comparison['generated_type']:
                                    is_storage_mismatch = comparison['details'].get('storage_type_mismatch', False)
                                    if not is_storage_mismatch:
                                        table_stats[table_name_lower]['type_mismatches'] += 1
                        elif (expected_val is None or expected_val == "") and (generated_val is None or generated_val == ""):
                            field_in_expected = field in expected_row
                            field_in_generated = generated_row and field_lower in [f.lower() for f in generated_row.keys()]
                            if field_in_expected and field_in_generated:
                                table_stats[table_name_lower]['correct_cells'] += 1
                
                for gen_idx, gen_row in enumerate(generated_rows):
                    if gen_idx not in used_generated_indices and isinstance(gen_row, dict):
                        for field in gen_row.keys():
                            table_stats[table_name_lower]['generated_cells'] += 1
            
            total_expected = sum(stats['expected_cells'] for stats in table_stats.values())
            total_generated = sum(stats['generated_cells'] for stats in table_stats.values())
            total_correct = sum(stats['correct_cells'] for stats in table_stats.values())
            total_exact = sum(stats['exact_matches'] for stats in table_stats.values())
            total_precision_errors = sum(stats['numeric_precision_errors'] for stats in table_stats.values())
            total_similarity_matches = sum(stats['string_similarity_matches'] for stats in table_stats.values())
            total_type_mismatches = sum(stats['type_mismatches'] for stats in table_stats.values())
            
            recall = (total_correct / total_expected * 100) if total_expected > 0 else 0
            precision = (total_correct / total_generated * 100) if total_generated > 0 else 0
            f1_score = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0
            
            if skipped_id_fields:
                logger.info(f"  ️  Skipped {len(skipped_id_fields)} auto-generated ID fields: {', '.join(sorted(skipped_id_fields))}")
            
            table_metrics = {}
            for table_name, stats in table_stats.items():
                table_recall = (stats['correct_cells'] / stats['expected_cells'] * 100) if stats['expected_cells'] > 0 else 0
                table_precision = (stats['correct_cells'] / stats['generated_cells'] * 100) if stats['generated_cells'] > 0 else 0
                table_f1 = (2 * table_precision * table_recall / (table_precision + table_recall)) if (table_precision + table_recall) > 0 else 0
                
                table_metrics[table_name] = {
                    'recall': table_recall,
                    'precision': table_precision,
                    'f1': table_f1,
                    'expected_cells': stats['expected_cells'],
                    'generated_cells': stats['generated_cells'],
                    'correct_cells': stats['correct_cells'],
                    'exact_matches': stats['exact_matches'],
                    'numeric_precision_errors': stats['numeric_precision_errors'],
                    'string_similarity_matches': stats['string_similarity_matches'],
                    'type_mismatches': stats['type_mismatches'],
                    'expected_rows': stats['expected_rows'],
                    'generated_rows': stats['generated_rows']
                }
            
            return {
                "cell_recall": recall,
                "cell_precision": precision,
                "cell_f1": f1_score,
                "total_expected_cells": total_expected,
                "total_generated_cells": total_generated,
                "total_correct_cells": total_correct,
                "total_exact_matches": total_exact,
                "total_numeric_precision_errors": total_precision_errors,
                "total_string_similarity_matches": total_similarity_matches,
                "total_type_mismatches": total_type_mismatches,
                "table_metrics": table_metrics,
                "detailed_comparisons": comparison_results,
                "total_expected_tables": len([t for t in table_stats.values() if t['expected_cells'] > 0]),
                "total_generated_tables": len([t for t in table_stats.values() if t['generated_cells'] > 0])
            }
            
        except Exception as e:
            logger.error(f"   计算cell指标失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                "cell_recall": 0.0,
                "cell_precision": 0.0,
                "cell_f1": 0.0,
                "total_expected_cells": 0,
                "total_generated_cells": 0,
                "total_correct_cells": 0,
                "total_exact_matches": 0,
                "total_numeric_precision_errors": 0,
                "total_string_similarity_matches": 0,
                "total_type_mismatches": 0,
                "table_metrics": {},
                "detailed_comparisons": [],
                "total_expected_tables": 0,
                "total_generated_tables": 0
            }
    
    def get_table_types_from_schema(self, schema: Dict[str, Any]) -> Dict[str, str]:
        """从schema中提取每个表的类型（entity或relation）
        
        Args:
            schema: schema字典
            
        Returns:
            {table_name: table_type} 的映射，table_type为'entity'或'relation'
        """
        table_types = {}
        
        try:
            if 'tables' in schema and isinstance(schema['tables'], list):
                for table in schema['tables']:
                    if isinstance(table, dict):
                        table_name = table.get('name', '')
                        table_type = table.get('type', 'entity')  # 默认为entity
                        if table_name:
                            table_types[table_name.lower()] = table_type
                
                if table_types:
                    entity_count = sum(1 for t in table_types.values() if t == 'entity')
                    relation_count = sum(1 for t in table_types.values() if t == 'relation')
                    logger.info(f"   Schema中的表类型: Entity={entity_count}, Relation={relation_count}")
                    for tname, ttype in table_types.items():
                        logger.info(f"     - {tname}: {ttype}")
        except Exception as e:
            logger.warning(f"  ️  解析schema表类型失败: {e}")
        
        return table_types
    
    def calculate_structural_metrics(self, generated: Dict[str, Any], expected: Dict[str, Any], schema: Dict[str, Any] = None) -> Dict[str, Any]:
        """Calculate structural-level metrics (table structure, field coverage, etc.)"""
        try:
            expected_tables = set(expected.keys())
            generated_tables = set(generated.keys())
            
            table_recall = len(expected_tables & generated_tables) / len(expected_tables) * 100 if expected_tables else 0
            table_precision = len(expected_tables & generated_tables) / len(generated_tables) * 100 if generated_tables else 0
            
            field_stats = {
                'total_expected_fields': 0,
                'total_generated_fields': 0,
                'correct_fields': 0
            }
            
            for table_name in expected_tables:
                if table_name in expected and isinstance(expected[table_name], list) and expected[table_name]:
                    expected_fields = set(expected[table_name][0].keys()) if expected[table_name] else set()
                    field_stats['total_expected_fields'] += len(expected_fields)
                    
                    if table_name in generated and isinstance(generated[table_name], list) and generated[table_name]:
                        generated_fields = set(generated[table_name][0].keys()) if generated[table_name] else set()
                        field_stats['total_generated_fields'] += len(generated_fields)
                        field_stats['correct_fields'] += len(expected_fields & generated_fields)
            
            field_recall = field_stats['correct_fields'] / field_stats['total_expected_fields'] * 100 if field_stats['total_expected_fields'] > 0 else 0
            field_precision = field_stats['correct_fields'] / field_stats['total_generated_fields'] * 100 if field_stats['total_generated_fields'] > 0 else 0
            
            entity_relation_stats = self.calculate_entity_relation_metrics(generated, expected, schema)
            
            return {
                'table_recall': table_recall,
                'table_precision': table_precision,
                'field_recall': field_recall,
                'field_precision': field_precision,
                'expected_tables_count': len(expected_tables),
                'generated_tables_count': len(generated_tables),
                'correct_tables_count': len(expected_tables & generated_tables),
                **field_stats,
                **entity_relation_stats
            }
            
        except Exception as e:
            logger.error(f"   计算结构指标失败: {e}")
            return {
                'table_recall': 0.0,
                'table_precision': 0.0,
                'field_recall': 0.0,
                'field_precision': 0.0,
                'expected_tables_count': 0,
                'generated_tables_count': 0,
                'correct_tables_count': 0,
                'total_expected_fields': 0,
                'total_generated_fields': 0,
                'correct_fields': 0,
                'entity_precision': 0.0,
                'entity_recall': 0.0,
                'entity_f1': 0.0,
                'relation_precision': 0.0,
                'relation_recall': 0.0,
                'relation_f1': 0.0
            }
    
    def calculate_entity_relation_metrics(self, generated: Dict[str, Any], expected: Dict[str, Any], schema: Dict[str, Any] = None) -> Dict[str, Any]:
        """Calculate cell-level P/R/F1 by table type (Entity/Relation)"""
        try:
            table_types = {}
            if schema:
                table_types = self.get_table_types_from_schema(schema)
            
            entity_stats = {
                'expected_cells': 0,
                'generated_cells': 0,
                'correct_cells': 0
            }
            relation_stats = {
                'expected_cells': 0,
                'generated_cells': 0,
                'correct_cells': 0
            }
            
            skipped_id_fields = set()
            
            
            for table_name, expected_rows in expected.items():
                if not isinstance(expected_rows, list):
                    continue
                
                table_name_lower = table_name.lower()
                
                table_type = table_types.get(table_name_lower, 'entity')
                
                stats = entity_stats if table_type == 'entity' else relation_stats
                
                generated_rows = None
                matched_gen_table_name = None
                for gen_table_name, gen_rows in generated.items():
                    gen_name_lower = gen_table_name.lower()
                    if gen_name_lower == table_name_lower or gen_name_lower.rstrip('s') == table_name_lower.rstrip('s'):
                        generated_rows = gen_rows
                        matched_gen_table_name = gen_table_name
                        break
                
                if generated_rows is None:
                    for row in expected_rows:
                        if isinstance(row, dict):
                            for attr, value in row.items():
                                if value is not None and value != "":
                                    stats['expected_cells'] += 1
                    continue
                
                used_generated_indices = set()
                
                for exp_row in expected_rows:
                    if not isinstance(exp_row, dict):
                        continue
                    
                    best_gen_idx, match_score = self._find_best_matching_row(
                        exp_row, generated_rows, used_generated_indices
                    )
                    
                    if best_gen_idx is not None:
                        generated_row = generated_rows[best_gen_idx]
                        used_generated_indices.add(best_gen_idx)
                    else:
                        generated_row = {}
                    
                    all_fields = set()
                    if isinstance(exp_row, dict):
                        all_fields.update(exp_row.keys())
                    if isinstance(generated_row, dict):
                        all_fields.update(generated_row.keys())
                    
                    for field in all_fields:
                        field_lower = field.lower()
                        
                        if self._is_auto_generated_id_field(field):
                            skipped_id_fields.add(field)
                            continue
                        
                        expected_val = exp_row.get(field) if isinstance(exp_row, dict) else None
                        
                        generated_val = None
                        if isinstance(generated_row, dict):
                            for gen_field, gen_val in generated_row.items():
                                if gen_field.lower() == field_lower:
                                    generated_val = gen_val
                                    break
                        
                        if field in exp_row:
                            stats['expected_cells'] += 1
                        
                        if generated_row and field_lower in [f.lower() for f in generated_row.keys()]:
                            stats['generated_cells'] += 1
                        
                        if (expected_val is not None and expected_val != "") or (generated_val is not None and generated_val != ""):
                            comparison = self.compare_cell_values(expected_val, generated_val)
                            if comparison['match']:
                                stats['correct_cells'] += 1
                        elif (expected_val is None or expected_val == "") and (generated_val is None or generated_val == ""):
                            field_in_expected = field in exp_row
                            field_in_generated = generated_row and field_lower in [f.lower() for f in generated_row.keys()]
                            if field_in_expected and field_in_generated:
                                stats['correct_cells'] += 1
                
                for gen_idx, gen_row in enumerate(generated_rows):
                    if gen_idx not in used_generated_indices and isinstance(gen_row, dict):
                        for field in gen_row.keys():
                            stats['generated_cells'] += 1
            
            entity_recall = (entity_stats['correct_cells'] / entity_stats['expected_cells'] * 100) if entity_stats['expected_cells'] > 0 else 0
            entity_precision = (entity_stats['correct_cells'] / entity_stats['generated_cells'] * 100) if entity_stats['generated_cells'] > 0 else 0
            entity_f1 = (2 * entity_precision * entity_recall / (entity_precision + entity_recall)) if (entity_precision + entity_recall) > 0 else 0
            
            relation_recall = (relation_stats['correct_cells'] / relation_stats['expected_cells'] * 100) if relation_stats['expected_cells'] > 0 else 0
            relation_precision = (relation_stats['correct_cells'] / relation_stats['generated_cells'] * 100) if relation_stats['generated_cells'] > 0 else 0
            relation_f1 = (2 * relation_precision * relation_recall / (relation_precision + relation_recall)) if (relation_precision + relation_recall) > 0 else 0
            
            logger.info(f"   Entity统计: expected={entity_stats['expected_cells']}, generated={entity_stats['generated_cells']}, correct={entity_stats['correct_cells']}")
            logger.info(f"   Relation统计: expected={relation_stats['expected_cells']}, generated={relation_stats['generated_cells']}, correct={relation_stats['correct_cells']}")
            
            return {
                'entity_precision': entity_precision,
                'entity_recall': entity_recall,
                'entity_f1': entity_f1,
                'entity_expected_cells': entity_stats['expected_cells'],
                'entity_generated_cells': entity_stats['generated_cells'],
                'entity_correct_cells': entity_stats['correct_cells'],
                'relation_precision': relation_precision,
                'relation_recall': relation_recall,
                'relation_f1': relation_f1,
                'relation_expected_cells': relation_stats['expected_cells'],
                'relation_generated_cells': relation_stats['generated_cells'],
                'relation_correct_cells': relation_stats['correct_cells']
            }
            
        except Exception as e:
            logger.error(f"   计算Entity/Relation指标失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'entity_precision': 0.0,
                'entity_recall': 0.0,
                'entity_f1': 0.0,
                'entity_expected_cells': 0,
                'entity_generated_cells': 0,
                'entity_correct_cells': 0,
                'relation_precision': 0.0,
                'relation_recall': 0.0,
                'relation_f1': 0.0,
                'relation_expected_cells': 0,
                'relation_generated_cells': 0,
                'relation_correct_cells': 0
            }
    
    def evaluate_with_llm(self, generated: Dict[str, Any], expected: Dict[str, Any], 
                          case_info: Dict[str, Any]) -> Dict[str, Any]:
        """使用LLM进行精细化评估（只返回总分）"""
        try:
            schema = {}
            try:
                with open(case_info['schema_file'], 'r', encoding='utf-8') as f:
                    schema = json.load(f)
            except Exception as e:
                logger.warning(f"  ️  无法读取schema文件: {e}")
            
            evaluation_prompt = f"""
You are a database evaluation expert. Please evaluate the quality of database tables extracted from documents.

Database Name: {case_info['db_name']}
Test Case: {case_info['case_name']}

[Database Schema]:
{json.dumps(schema, ensure_ascii=False, indent=2)}

[Generated Results]:
{json.dumps(generated, ensure_ascii=False, indent=2)}

[Standard Answer]:
{json.dumps(expected, ensure_ascii=False, indent=2)}


Please evaluate the quality of the generated results from the following dimensions:

- Whether the generated results are semantically consistent with the standard answer
- Whether numeric data is accurate (values must be exactly equal to be considered correct)
- Whether the decimal places of numeric values match the standard answer
- Whether string data is accurate (similarity needs to reach 90% or above)
- Whether there are hallucinations or erroneous information

- Whether the generated results contain all key information from the standard answer
- Whether there are important data omissions
- Whether the table structure and fields are complete
- Further explanations of these key points can be omitted, but the core content must be complete

- Whether the data format complies with the Schema definition
- Whether the data types are correct (numeric, string, date, etc.)
- Whether the formats are consistent (e.g., date format, number format, etc.)

1. Numeric values must be exactly equal to be considered correct, including decimal places
2. String similarity below 90% is not considered a match
3. Type mismatches are serious errors
4. If the generated results fully comply with all key points of the standard answer, the total score should be 100 points
5. Please note: The standard answer may be considered the correct answer to the question

The assistant will receive a total score from 0 to 100 points, with higher scores indicating better overall performance. Please note that if the assistant's answer fully complies with the above standards and matches the standard answer, the total score should be 100 points.

Please first provide a comprehensive evaluation explanation (no more than 100 words), avoiding any potential bias. Then output the score in strict format.

Please output in the following format:
<Start Output>
Evaluation evidence: Your evaluation explanation (no more than 100 words)
Rating: [[score]]
<End Output>

Where score is an integer between 0-100.

Now begin the evaluation:
"""
            
            logger.info(f"   使用 {self.model} 模型进行精细化评估")
            
            from llm.main import get_answer
            
            response = get_answer(evaluation_prompt, model=self.model)
            
            if not response:
                raise Exception("LLM返回空响应")
            
            import re
            overall_score = 0
            evaluation_details = ""
            
            score_match = re.search(r'Rating:\s*\[\[(\d+(?:\.\d+)?)\]\]', response)
            if score_match:
                overall_score = float(score_match.group(1))
            else:
                score_match = re.search(r'"overall_score":\s*(\d+(?:\.\d+)?)', response)
                if score_match:
                    overall_score = float(score_match.group(1))
                else:
                    score_match = re.search(r'(\d+(?:\.\d+)?)\s*分', response)
                    if score_match:
                        overall_score = float(score_match.group(1))
            
            evidence_match = re.search(r'Evaluation evidence:\s*([^\n]+)', response)
            if evidence_match:
                evaluation_details = evidence_match.group(1).strip()
            else:
                evaluation_details = response[:500]  # 限制长度
            
            overall_score = max(0, min(100, overall_score))
            
            llm_evaluation = {
                "overall": overall_score,
                "evaluation_details": evaluation_details,
                "raw_response": response
            }
            
            logger.info(f"   LLM 精细化评估完成，总分: {overall_score:.1f}")
            return llm_evaluation
                
        except Exception as e:
            logger.warning(f"  ️  LLM评估失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                "overall": 0.0,
                "evaluation_details": f"LLM评估失败: {str(e)}",
                "raw_response": ""
            }
    
    
    def process_case(self, case_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理单个测试案例"""
        case_id = f"{case_info['db_name']}/{case_info['case_name']}"
        logger.info(f"\n{'='*80}")
        logger.info(f" 开始处理: {case_id}")
        logger.info(f"{'='*80}")
        
        result = {
            'case_id': case_id,
            'db_name': case_info['db_name'],
            'case_name': case_info['case_name'],
            'model': self.model,
            'run_name': self.run_name,
            'document_mode': self.document_mode,  # 记录文档模式
            'status': 'failed',
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'duration': 0,
            'evaluation': None,
            'error': None
        }
        
        start_time = time.time()
        
        try:
            logger.info("读取schema...")
            with open(case_info['schema_file'], 'r', encoding='utf-8') as f:
                schema = json.load(f)
            
            logger.info("读取标准答案...")
            with open(case_info['answer_file'], 'r', encoding='utf-8') as f:
                expected_answer = json.load(f)
            
            logger.info(f" 上传 {len(case_info['doc_files'])} 个文档...")
            uploaded_files = self.upload_documents(case_info['doc_files'])
            
            if not uploaded_files:
                raise Exception("文档上传失败")
            
            logger.info("启动处理任务...")
            task_id = self.start_processing(uploaded_files, schema)
            
            if not task_id:
                raise Exception("启动处理任务失败")
            
            result['task_id'] = task_id
            
            logger.info("等待处理完成...")
            status_data = self.wait_for_completion(task_id)
            
            if not status_data:
                raise Exception("处理任务失败或超时")
            
            logger.info("提取处理结果...")
            result_data = self.extract_result_data(status_data, task_id)
            
            if not result_data:
                raise Exception("提取结果失败")
            
            if result_data.get('steps'):
                logger.info(f"  ️  处理步骤数: {len(result_data['steps'])}")
                for step in result_data['steps']:
                    step_name = step.get('step', 'unknown')
                    step_status = step.get('status', 'unknown')
                    logger.info(f"    - {step_name}: {step_status}")
            
            logger.info("保存处理结果...")
            prefix_output_dir, filename_prefix = self.save_output(case_info, result_data)
            result['output_dir'] = str(prefix_output_dir)
            result['filename_prefix'] = filename_prefix
            
            logger.info("加载表数据用于评估...")
            output_result = self.load_result_from_output(prefix_output_dir, filename_prefix)
            
            if output_result and output_result.get('tables'):
                generated_tables = output_result['tables']
                logger.info(f"   成功加载 {len(generated_tables)} 个表用于评估")
            else:
                logger.warning(f"  ️  未能从输出目录加载表数据，使用原始结果")
                generated_tables = result_data.get('tables', {})
            
            logger.info("计算Cell级别的召回率和准确率...")
            cell_metrics = self.calculate_cell_metrics(generated_tables, expected_answer)
            logger.info(f"   Cell召回率: {cell_metrics['cell_recall']:.2f}%")
            logger.info(f"   Cell准确率: {cell_metrics['cell_precision']:.2f}%")
            logger.info(f"   Cell F1分数: {cell_metrics['cell_f1']:.2f}")
            logger.info(f"  ️  正确Cells: {cell_metrics['total_correct_cells']}/{cell_metrics['total_expected_cells']}")
            
            logger.info("计算结构级别的指标...")
            structural_metrics = self.calculate_structural_metrics(generated_tables, expected_answer, schema)
            logger.info(f"   表召回率: {structural_metrics['table_recall']:.2f}%")
            logger.info(f"   表准确率: {structural_metrics['table_precision']:.2f}%")
            logger.info(f"   字段召回率: {structural_metrics['field_recall']:.2f}%")
            logger.info(f"   字段准确率: {structural_metrics['field_precision']:.2f}%")
            logger.info(f"   Entity F1: {structural_metrics.get('entity_f1', 0):.2f}")
            logger.info(f"   Relation F1: {structural_metrics.get('relation_f1', 0):.2f}")
            
            if self.enable_llm_eval:
                logger.info("使用LLM评估结果...")
                llm_evaluation = self.evaluate_with_llm(
                    generated_tables, 
                    expected_answer, 
                    case_info
                )
            else:
                logger.info("跳过LLM评估（仅计算P/R/F1）")
                llm_evaluation = {'overall': 0, 'evaluation_details': '', 'raw_response': ''}
            
            evaluation = {
                "cell_recall": cell_metrics['cell_recall'],
                "cell_precision": cell_metrics['cell_precision'],
                "cell_f1": cell_metrics['cell_f1'],
                "total_expected_cells": cell_metrics['total_expected_cells'],
                "total_generated_cells": cell_metrics['total_generated_cells'],
                "total_correct_cells": cell_metrics['total_correct_cells'],
                "table_metrics": cell_metrics.get('table_metrics', {}),
                
                "table_recall": structural_metrics['table_recall'],
                "table_precision": structural_metrics['table_precision'],
                "field_recall": structural_metrics['field_recall'],
                "field_precision": structural_metrics['field_precision'],
                "expected_tables_count": structural_metrics['expected_tables_count'],
                "generated_tables_count": structural_metrics['generated_tables_count'],
                "correct_tables_count": structural_metrics['correct_tables_count'],
                "total_expected_fields": structural_metrics['total_expected_fields'],
                "total_generated_fields": structural_metrics['total_generated_fields'],
                "correct_fields": structural_metrics['correct_fields'],
                
                "entity_precision": structural_metrics.get('entity_precision', 0),
                "entity_recall": structural_metrics.get('entity_recall', 0),
                "entity_f1": structural_metrics.get('entity_f1', 0),
                "entity_expected_cells": structural_metrics.get('entity_expected_cells', 0),
                "entity_generated_cells": structural_metrics.get('entity_generated_cells', 0),
                "entity_correct_cells": structural_metrics.get('entity_correct_cells', 0),
                "relation_precision": structural_metrics.get('relation_precision', 0),
                "relation_recall": structural_metrics.get('relation_recall', 0),
                "relation_f1": structural_metrics.get('relation_f1', 0),
                "relation_expected_cells": structural_metrics.get('relation_expected_cells', 0),
                "relation_generated_cells": structural_metrics.get('relation_generated_cells', 0),
                "relation_correct_cells": structural_metrics.get('relation_correct_cells', 0),
                
                "llm_score": llm_evaluation.get('overall', 0),
                "llm_evaluation_details": llm_evaluation.get('evaluation_details', 'N/A'),
                "llm_raw_response": llm_evaluation.get('raw_response', '')
            }
            result['evaluation'] = evaluation
            
            logger.info("保存评估结果...")
            evaluation_file = prefix_output_dir / f"{filename_prefix}_evaluate.json"
            with open(evaluation_file, 'w', encoding='utf-8') as f:
                json.dump(evaluation, f, ensure_ascii=False, indent=2)
            logger.info(f"   评估结果已保存: {evaluation_file}")
            
            result['status'] = 'success'
            result['duration'] = time.time() - start_time
            result['end_time'] = datetime.now().isoformat()
            
            logger.info(f"\n {case_id} 处理成功！")
            logger.info(f"️  耗时: {int(result['duration'])}秒")
            logger.info(f" 详细评估结果:")
            logger.info(f"    Cell级别指标:")
            logger.info(f"      - 召回率: {evaluation['cell_recall']:.2f}%")
            logger.info(f"      - 准确率: {evaluation['cell_precision']:.2f}%")
            logger.info(f"      - F1分数: {evaluation['cell_f1']:.2f}")
            logger.info(f"      - 精确匹配: {evaluation.get('total_exact_matches', 0)}/{evaluation['total_expected_cells']}")
            logger.info(f"      - 数值精度错误: {evaluation.get('total_numeric_precision_errors', 0)}")
            logger.info(f"      - 字符串相似匹配: {evaluation.get('total_string_similarity_matches', 0)}")
            logger.info(f"      - 类型不匹配: {evaluation.get('total_type_mismatches', 0)}")
            logger.info(f"   ️  结构级别指标:")
            logger.info(f"      - 表召回率: {evaluation['table_recall']:.2f}%")
            logger.info(f"      - 表准确率: {evaluation['table_precision']:.2f}%")
            logger.info(f"      - 字段召回率: {evaluation['field_recall']:.2f}%")
            logger.info(f"      - 字段准确率: {evaluation['field_precision']:.2f}%")
            logger.info(f"    LLM精细化评分:")
            logger.info(f"      - 总分: {evaluation['llm_score']:.1f}/100")
            
            eval_details = evaluation.get('llm_evaluation_details', 'N/A')
            if len(eval_details) > 500:
                eval_details = eval_details[:500] + "..."
            logger.info(f" LLM评估详情: {eval_details}")
            
            if evaluation.get('table_metrics'):
                logger.info(f" 各表详细指标:")
                for table_name, metrics in evaluation['table_metrics'].items():
                    logger.info(f"   - {table_name}: 召回率={metrics['recall']:.1f}%, "
                              f"准确率={metrics['precision']:.1f}%, F1={metrics['f1']:.2f}")
                    logger.info(f"     行数: {metrics['generated_rows']}/{metrics['expected_rows']}, "
                              f"正确cells: {metrics['correct_cells']}/{metrics['expected_cells']}")
                    logger.info(f"     精确匹配: {metrics.get('exact_matches', 0)}, "
                              f"数值精度错误: {metrics.get('numeric_precision_errors', 0)}, "
                              f"类型不匹配: {metrics.get('type_mismatches', 0)}")
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            result['duration'] = time.time() - start_time
            result['end_time'] = datetime.now().isoformat()
            logger.error(f"\n {case_id} 处理失败: {e}")
        
        return result
    
    def scan_all_method_outputs(self) -> List[Dict[str, Any]]:
        """扫描所有方法的输出（包括系统方法和外部方法）
        
        Returns:
            List[Dict]: 每个字典包含：
                - db_name: 数据库名
                - case_name: 案例名
                - method_name: 方法名（如 gpt_4o_multi, langchain, langextract等）
                - output_dir: 输出目录路径
                - has_evaluation: 是否已有评估文件
        """
        all_outputs = []
        
        if not self.output_dir.exists():
            logger.error(f" 输出目录不存在: {self.output_dir}")
            return all_outputs
        
        for db_dir in self.output_dir.iterdir():
            if not db_dir.is_dir() or db_dir.name.startswith('evaluation_'):
                continue
            
            db_name = db_dir.name
            
            for case_dir in db_dir.iterdir():
                if not case_dir.is_dir():
                    continue
                
                case_name = case_dir.name
                
                for method_dir in case_dir.iterdir():
                    if not method_dir.is_dir():
                        continue
                    
                    method_name = method_dir.name
                    
                    has_data = (method_dir / "output_tables.json").exists() or \
                               (method_dir / "extracted_data.json").exists() or \
                               (method_dir / "extracted_table.json").exists()
                    
                    if not has_data:
                        continue
                    
                    evaluate_file = method_dir / f"{method_name}_evaluate.json"
                    has_evaluation = evaluate_file.exists()
                    
                    all_outputs.append({
                        'db_name': db_name,
                        'case_name': case_name,
                        'method_name': method_name,
                        'output_dir': method_dir,
                        'has_evaluation': has_evaluation
                    })
        
        logger.info(f" 找到 {len(all_outputs)} 个方法输出")
        
        all_outputs.sort(key=lambda x: (x['db_name'], x['case_name'], x['method_name']))
        
        return all_outputs
    
    def evaluate_method_output(self, method_output: Dict[str, Any], case_info: Dict[str, Any]) -> Dict[str, Any]:
        """评估单个方法的输出
        
        Args:
            method_output: 方法输出信息（包含method_name, output_dir等）
            case_info: 案例信息（包含answer_file, schema_file等）
            
        Returns:
            评估结果字典
        """
        method_name = method_output['method_name']
        output_dir = method_output['output_dir']
        case_id = f"{method_output['db_name']}/{method_output['case_name']}/{method_name}"
        
        logger.info(f"\n 评估: {case_id}")
        
        result = {
            'case_id': case_id,
            'db_name': method_output['db_name'],
            'case_name': method_output['case_name'],
            'method_name': method_name,
            'status': 'failed',
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'duration': 0,
            'evaluation': None,
            'error': None
        }
        
        start_time = time.time()
        
        try:
            logger.info("读取标准答案和schema...")
            with open(case_info['answer_file'], 'r', encoding='utf-8') as f:
                expected_answer = json.load(f)
            
            schema = {}
            try:
                with open(case_info['schema_file'], 'r', encoding='utf-8') as f:
                    schema = json.load(f)
            except Exception as e:
                logger.warning(f"  ️  无法读取schema文件: {e}")
            
            logger.info("加载输出结果...")
            output_result = self.load_result_from_output(output_dir, method_name)
            
            if not output_result or not output_result.get('tables'):
                raise Exception("未能加载有效的输出结果")
            
            generated_tables = output_result['tables']
            logger.info(f"   成功加载 {len(generated_tables)} 个表")
            
            logger.info("计算Cell级别的召回率和准确率...")
            cell_metrics = self.calculate_cell_metrics(generated_tables, expected_answer)
            logger.info(f"   Cell召回率: {cell_metrics['cell_recall']:.2f}%")
            logger.info(f"   Cell准确率: {cell_metrics['cell_precision']:.2f}%")
            logger.info(f"   Cell F1分数: {cell_metrics['cell_f1']:.2f}")
            
            logger.info("计算结构级别的指标...")
            structural_metrics = self.calculate_structural_metrics(generated_tables, expected_answer, schema)
            logger.info(f"   表召回率: {structural_metrics['table_recall']:.2f}%")
            logger.info(f"   表准确率: {structural_metrics['table_precision']:.2f}%")
            
            if self.enable_llm_eval:
                logger.info("使用LLM评估结果...")
                llm_evaluation = self.evaluate_with_llm(
                    generated_tables, 
                    expected_answer, 
                    case_info
                )
            else:
                logger.info("跳过LLM评估（仅计算P/R/F1）")
                llm_evaluation = {'overall': 0, 'evaluation_details': '', 'raw_response': ''}
            
            evaluation = {
                "cell_recall": cell_metrics['cell_recall'],
                "cell_precision": cell_metrics['cell_precision'],
                "cell_f1": cell_metrics['cell_f1'],
                "total_expected_cells": cell_metrics['total_expected_cells'],
                "total_generated_cells": cell_metrics['total_generated_cells'],
                "total_correct_cells": cell_metrics['total_correct_cells'],
                "table_metrics": cell_metrics.get('table_metrics', {}),
                
                "table_recall": structural_metrics['table_recall'],
                "table_precision": structural_metrics['table_precision'],
                "field_recall": structural_metrics['field_recall'],
                "field_precision": structural_metrics['field_precision'],
                "expected_tables_count": structural_metrics['expected_tables_count'],
                "generated_tables_count": structural_metrics['generated_tables_count'],
                "correct_tables_count": structural_metrics['correct_tables_count'],
                "total_expected_fields": structural_metrics['total_expected_fields'],
                "total_generated_fields": structural_metrics['total_generated_fields'],
                "correct_fields": structural_metrics['correct_fields'],
                
                "entity_precision": structural_metrics.get('entity_precision', 0),
                "entity_recall": structural_metrics.get('entity_recall', 0),
                "entity_f1": structural_metrics.get('entity_f1', 0),
                "entity_expected_cells": structural_metrics.get('entity_expected_cells', 0),
                "entity_generated_cells": structural_metrics.get('entity_generated_cells', 0),
                "entity_correct_cells": structural_metrics.get('entity_correct_cells', 0),
                "relation_precision": structural_metrics.get('relation_precision', 0),
                "relation_recall": structural_metrics.get('relation_recall', 0),
                "relation_f1": structural_metrics.get('relation_f1', 0),
                "relation_expected_cells": structural_metrics.get('relation_expected_cells', 0),
                "relation_generated_cells": structural_metrics.get('relation_generated_cells', 0),
                "relation_correct_cells": structural_metrics.get('relation_correct_cells', 0),
                
                "llm_score": llm_evaluation.get('overall', 0),
                "llm_evaluation_details": llm_evaluation.get('evaluation_details', 'N/A'),
                "llm_raw_response": llm_evaluation.get('raw_response', '')
            }
            result['evaluation'] = evaluation
            
            logger.info("保存评估结果...")
            evaluation_file = output_dir / f"{method_name}_evaluate.json"
            with open(evaluation_file, 'w', encoding='utf-8') as f:
                json.dump(evaluation, f, ensure_ascii=False, indent=2)
            logger.info(f"   评估结果已保存: {evaluation_file}")
            
            result['status'] = 'success'
            result['duration'] = time.time() - start_time
            result['end_time'] = datetime.now().isoformat()
            
            logger.info(f" {case_id} 评估完成！")
            logger.info(f"   Cell F1: {evaluation['cell_f1']:.2f} | LLM分数: {evaluation['llm_score']:.1f}/100")
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            result['duration'] = time.time() - start_time
            result['end_time'] = datetime.now().isoformat()
            logger.error(f" {case_id} 评估失败: {e}")
            import traceback
            traceback.print_exc()
        
        return result
    
    
    def evaluate_all_methods(self, skip_evaluated: bool = True):
        """评估所有方法的输出（包括外部方法）
        
        Args:
            skip_evaluated: 是否跳过已有评估文件的输出
        """
        logger.info("\n" + "="*80)
        logger.info("评估所有方法的输出结果")
        logger.info("="*80 + "\n")
        
        logger.info("扫描输出目录...")
        all_method_outputs = self.scan_all_method_outputs()
        
        if not all_method_outputs:
            logger.error("未找到任何方法输出")
            return
        
        if skip_evaluated:
            to_evaluate = [m for m in all_method_outputs if not m['has_evaluation']]
            skipped_count = len(all_method_outputs) - len(to_evaluate)
            logger.info(f"️  跳过 {skipped_count} 个已评估的输出")
        else:
            to_evaluate = all_method_outputs
            logger.info(f"️  重新评估所有输出")
        
        if not to_evaluate:
            logger.info("所有输出都已评估！")
            return
        
        logger.info(f" 待评估: {len(to_evaluate)} 个方法输出\n")
        
        cases = self.scan_datasets()
        case_info_map = {f"{c['db_name']}/{c['case_name']}": c for c in cases}
        
        for i, method_output in enumerate(to_evaluate, 1):
            logger.info(f"\n{'#'*80}")
            logger.info(f"# 进度: {i}/{len(to_evaluate)}")
            logger.info(f"{'#'*80}")
            
            case_key = f"{method_output['db_name']}/{method_output['case_name']}"
            case_info = case_info_map.get(case_key)
            
            if not case_info:
                logger.error(f" 未找到案例信息: {case_key}")
                continue
            
            result = self.evaluate_method_output(method_output, case_info)
            self.results.append(result)
            
            time.sleep(0.5)
        
        logger.info("\n" + "="*80)
        logger.info("生成统一评估报告...")
        logger.info("="*80 + "\n")
        
        
        self.print_unified_evaluation_summary()
        
        logger.info("\n" + "="*80)
        logger.info("评估完成！")
        logger.info("="*80 + "\n")
    
    def evaluate_existing_outputs(self):
        """评估已有的输出结果（不重新运行处理）"""
        logger.info("\n" + "="*80)
        logger.info("评估已有输出结果")
        logger.info("="*80 + "\n")
        
        logger.info("扫描数据集...")
        cases = self.scan_datasets()
        
        if not cases:
            logger.error("未找到有效的测试案例")
            return
        
        for i, case_info in enumerate(cases, 1):
            logger.info(f"\n{'#'*80}")
            logger.info(f"# 进度: {i}/{len(cases)}")
            logger.info(f"{'#'*80}")
            
            case_id = f"{case_info['db_name']}/{case_info['case_name']}"
            logger.info(f" 评估案例: {case_id}")
            
            if not self.check_case_already_processed(case_info):
                logger.warning(f"️  案例未处理，跳过: {case_id}")
                skipped_result = {
                    'case_id': case_id,
                    'db_name': case_info['db_name'],
                    'case_name': case_info['case_name'],
                    'model': self.model,
                    'run_name': self.run_name,
                    'document_mode': self.document_mode,
                    'status': 'no_output',
                    'message': '未找到输出文件'
                }
                self.results.append(skipped_result)
                continue
            
            result = self.evaluate_case_output(case_info)
            self.results.append(result)
        
        logger.info("\n" + "="*80)
        logger.info("生成评估报告...")
        logger.info("="*80 + "\n")
        
        
        self.print_evaluation_summary()
        
        logger.info("\n" + "="*80)
        logger.info("Evaluation completed!")
        logger.info("="*80 + "\n")
    
    def evaluate_case_output(self, case_info: Dict[str, Any]) -> Dict[str, Any]:
        """评估单个案例的已有输出"""
        case_id = f"{case_info['db_name']}/{case_info['case_name']}"
        logger.info(f"\n 评估案例: {case_id}")
        
        result = {
            'case_id': case_id,
            'db_name': case_info['db_name'],
            'case_name': case_info['case_name'],
            'model': self.model,
            'run_name': self.run_name,
            'document_mode': self.document_mode,
            'status': 'failed',
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'duration': 0,
            'evaluation': None,
            'error': None
        }
        
        start_time = time.time()
        
        try:
            logger.info("读取标准答案和schema...")
            with open(case_info['answer_file'], 'r', encoding='utf-8') as f:
                expected_answer = json.load(f)
            
            schema = {}
            try:
                with open(case_info['schema_file'], 'r', encoding='utf-8') as f:
                    schema = json.load(f)
            except Exception as e:
                logger.warning(f"  ️  无法读取schema文件: {e}")
            
            case_dir = self.output_dir / case_info['db_name'] / case_info['case_name']
            filename_prefix = self._build_filename_prefix(
                case_info['db_name'], 
                case_info['case_name'], 
                self.model
            )
            prefix_output_dir = case_dir / filename_prefix
            
            if not prefix_output_dir.exists():
                raise Exception(f"输出目录不存在: {prefix_output_dir}")
            
            logger.info("加载输出结果...")
            output_result = self.load_result_from_output(prefix_output_dir, filename_prefix)
            
            if not output_result or not output_result.get('tables'):
                raise Exception("未能加载有效的输出结果")
            
            generated_tables = output_result['tables']
            logger.info(f"   成功加载 {len(generated_tables)} 个表")
            
            logger.info("计算Cell级别的召回率和准确率...")
            cell_metrics = self.calculate_cell_metrics(generated_tables, expected_answer)
            logger.info(f"   Cell召回率: {cell_metrics['cell_recall']:.2f}%")
            logger.info(f"   Cell准确率: {cell_metrics['cell_precision']:.2f}%")
            logger.info(f"   Cell F1分数: {cell_metrics['cell_f1']:.2f}")
            
            logger.info("计算结构级别的指标...")
            structural_metrics = self.calculate_structural_metrics(generated_tables, expected_answer, schema)
            logger.info(f"   表召回率: {structural_metrics['table_recall']:.2f}%")
            logger.info(f"   表准确率: {structural_metrics['table_precision']:.2f}%")
            logger.info(f"   字段召回率: {structural_metrics['field_recall']:.2f}%")
            logger.info(f"   字段准确率: {structural_metrics['field_precision']:.2f}%")
            
            if self.enable_llm_eval:
                logger.info("使用LLM评估结果...")
                llm_evaluation = self.evaluate_with_llm(
                    generated_tables, 
                    expected_answer, 
                    case_info
                )
            else:
                logger.info("跳过LLM评估（仅计算P/R/F1）")
                llm_evaluation = {'overall': 0, 'evaluation_details': '', 'raw_response': ''}
            
            evaluation = {
                "cell_recall": cell_metrics['cell_recall'],
                "cell_precision": cell_metrics['cell_precision'],
                "cell_f1": cell_metrics['cell_f1'],
                "total_expected_cells": cell_metrics['total_expected_cells'],
                "total_generated_cells": cell_metrics['total_generated_cells'],
                "total_correct_cells": cell_metrics['total_correct_cells'],
                "table_metrics": cell_metrics.get('table_metrics', {}),
                
                "table_recall": structural_metrics['table_recall'],
                "table_precision": structural_metrics['table_precision'],
                "field_recall": structural_metrics['field_recall'],
                "field_precision": structural_metrics['field_precision'],
                "expected_tables_count": structural_metrics['expected_tables_count'],
                "generated_tables_count": structural_metrics['generated_tables_count'],
                "correct_tables_count": structural_metrics['correct_tables_count'],
                "total_expected_fields": structural_metrics['total_expected_fields'],
                "total_generated_fields": structural_metrics['total_generated_fields'],
                "correct_fields": structural_metrics['correct_fields'],
                
                "entity_precision": structural_metrics.get('entity_precision', 0),
                "entity_recall": structural_metrics.get('entity_recall', 0),
                "entity_f1": structural_metrics.get('entity_f1', 0),
                "entity_expected_cells": structural_metrics.get('entity_expected_cells', 0),
                "entity_generated_cells": structural_metrics.get('entity_generated_cells', 0),
                "entity_correct_cells": structural_metrics.get('entity_correct_cells', 0),
                "relation_precision": structural_metrics.get('relation_precision', 0),
                "relation_recall": structural_metrics.get('relation_recall', 0),
                "relation_f1": structural_metrics.get('relation_f1', 0),
                "relation_expected_cells": structural_metrics.get('relation_expected_cells', 0),
                "relation_generated_cells": structural_metrics.get('relation_generated_cells', 0),
                "relation_correct_cells": structural_metrics.get('relation_correct_cells', 0),
                
                "llm_score": llm_evaluation.get('overall', 0),
                "llm_evaluation_details": llm_evaluation.get('evaluation_details', 'N/A'),
                "llm_raw_response": llm_evaluation.get('raw_response', '')
            }
            result['evaluation'] = evaluation
            
            logger.info("保存评估结果...")
            evaluation_file = prefix_output_dir / f"{filename_prefix}_evaluate.json"
            with open(evaluation_file, 'w', encoding='utf-8') as f:
                json.dump(evaluation, f, ensure_ascii=False, indent=2)
            logger.info(f"   评估结果已保存: {evaluation_file}")
            
            result['status'] = 'success'
            result['duration'] = time.time() - start_time
            result['end_time'] = datetime.now().isoformat()
            
            logger.info(f"\n {case_id} 评估完成！")
            logger.info(f"️  耗时: {int(result['duration'])}秒")
            logger.info(f" 评估结果:")
            logger.info(f"   - Cell F1分数: {evaluation['cell_f1']:.2f}")
            logger.info(f"   - 表召回率: {evaluation['table_recall']:.2f}%")
            logger.info(f"   - LLM分数: {evaluation['llm_score']:.1f}/100")
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            result['duration'] = time.time() - start_time
            result['end_time'] = datetime.now().isoformat()
            logger.error(f"\n {case_id} 评估失败: {e}")
        
        return result
    
    def generate_evaluation_standards(self) -> Dict[str, Any]:
        """生成详细的评估标准说明"""
        return {
            "evaluation_framework": {
                "name": "Doc2DB 多维度评估框架",
                "version": "2.0",
                "description": "基于Cell级别、结构级别和LLM评分的综合评估体系"
            },
            "metrics_definition": {
                "cell_level": {
                    "description": "Cell级别指标评估每个数据单元格的提取准确性",
                    "metrics": {
                        "cell_recall": {
                            "definition": "正确提取的cells数量 / 标准答案中的总cells数量",
                            "range": "0-100%",
                            "interpretation": {
                                "90-100%": "优秀 - 几乎完整提取所有信息",
                                "70-89%": "良好 - 提取了大部分重要信息",
                                "50-69%": "中等 - 提取了部分信息，存在遗漏",
                                "30-49%": "较差 - 大量信息遗漏",
                                "0-29%": "很差 - 严重信息缺失"
                            }
                        },
                        "cell_precision": {
                            "definition": "正确提取的cells数量 / 生成结果中的总cells数量",
                            "range": "0-100%",
                            "interpretation": {
                                "90-100%": "优秀 - 几乎没有错误或幻觉",
                                "70-89%": "良好 - 少量错误信息",
                                "50-69%": "中等 - 存在一定错误",
                                "30-49%": "较差 - 错误较多",
                                "0-29%": "很差 - 大量错误或幻觉"
                            }
                        },
                        "cell_f1": {
                            "definition": "召回率和准确率的调和平均数",
                            "formula": "2 * (precision * recall) / (precision + recall)",
                            "range": "0-100",
                            "interpretation": "综合评估提取质量，平衡完整性和准确性"
                        }
                    }
                },
                "structural_level": {
                    "description": "结构级别指标评估表结构和字段的识别准确性",
                    "metrics": {
                        "table_recall": "识别出的正确表数量 / 标准答案中的表数量",
                        "table_precision": "识别出的正确表数量 / 生成结果中的表数量",
                        "field_recall": "识别出的正确字段数量 / 标准答案中的字段数量",
                        "field_precision": "识别出的正确字段数量 / 生成结果中的字段数量"
                    }
                },
                "llm_evaluation": {
                    "description": "基于大语言模型的综合评估（0-100分）",
                    "evaluation_dimensions": {
                        "accuracy_and_hallucinations": {
                            "description": "准确性和幻觉",
                            "criteria": [
                                "生成结果与标准答案在语义上是否一致",
                                "数值型数据是否准确（数值必须完全相等才算正确）",
                                "数值的小数位数是否与标准答案一致",
                                "字符串型数据是否准确（相似度需要达到90%以上）",
                                "是否存在幻觉或错误信息"
                            ]
                        },
                        "completeness": {
                            "description": "完整性",
                            "criteria": [
                                "生成结果是否包含标准答案中的所有关键信息",
                                "是否有重要数据遗漏",
                                "表结构和字段是否完整",
                                "进一步说明这些关键点可以省略，但核心内容必须完整"
                            ]
                        },
                        "format_consistency": {
                            "description": "格式一致性",
                            "criteria": [
                                "数据格式是否符合Schema定义",
                                "数据类型是否正确（数值型、字符串型、日期型等）",
                                "格式是否统一（如日期格式、数字格式等）"
                            ]
                        }
                    },
                    "overall_score": {
                        "definition": "综合评分（总分）",
                        "range": "0-100分",
                        "calculation": "基于多维度的综合评估"
                    },
                    "key_requirements": [
                        "数值必须完全相等才算正确，包括小数位数",
                        "字符串相似度低于90%不算匹配",
                        "类型不匹配是严重错误",
                        "如果生成结果完全符合标准答案的所有关键点，总分应为满分100分"
                    ],
                    "output_format": "Rating: [[score]]，其中score是0-100之间的整数"
                }
            },
            "quality_thresholds": {
                "excellent": {
                    "cell_f1": ">= 90",
                    "table_recall": ">= 90%",
                    "field_recall": ">= 85%",
                    "llm_score": ">= 85",
                    "description": "优秀级别 - 可直接用于生产环境"
                },
                "good": {
                    "cell_f1": "70-89",
                    "table_recall": "70-89%",
                    "field_recall": "65-84%",
                    "llm_score": "70-84",
                    "description": "良好级别 - 经过少量调整可用于生产"
                },
                "acceptable": {
                    "cell_f1": "50-69",
                    "table_recall": "50-69%",
                    "field_recall": "45-64%",
                    "llm_score": "50-69",
                    "description": "可接受级别 - 需要进一步优化"
                },
                "poor": {
                    "cell_f1": "< 50",
                    "table_recall": "< 50%",
                    "field_recall": "< 45%",
                    "llm_score": "< 50",
                    "description": "较差级别 - 需要重大改进"
                }
            },
            "evaluation_process": {
                "steps": [
                    "1. 数据标准化：统一格式、大小写、日期等",
                    "2. Cell级别匹配：逐个比较数据单元格",
                    "3. 结构级别分析：评估表和字段的识别",
                    "4. LLM语义评估：深度理解和综合评分",
                    "5. 多维度综合：生成最终评估报告"
                ],
                "considerations": [
                    "忽略空值和null值的比较",
                    "数字格式标准化（如1.0 -> 1）",
                    "日期格式统一处理",
                    "大小写不敏感匹配",
                    "表名和字段名的灵活匹配"
                ]
            }
        }
    
    
    def print_unified_evaluation_summary(self):
        """打印统一的评估摘要（支持多个方法的对比）"""
        try:
            method_results = {}
            for result in self.results:
                if result['status'] != 'success' or not result.get('evaluation'):
                    continue
                
                method_name = result.get('method_name', 'unknown')
                if method_name not in method_results:
                    method_results[method_name] = []
                method_results[method_name].append(result)
            
            if not method_results:
                logger.warning("没有成功的评估结果")
                return
            
            logger.info("\n各方法平均指标对比:")
            logger.info("-" * 160)
            logger.info(f"{'方法名':30s} | {'测试案例':8s} | {'Cell召回':9s} | {'Cell准确':9s} | {'Cell F1':9s} | {'表召回':8s} | {'字段召回':9s} | {'LLM分数':9s}")
            logger.info("-" * 160)
            
            method_avg_data = []
            for method_name, method_res_list in sorted(method_results.items()):
                cell_recalls = [r['evaluation']['cell_recall'] for r in method_res_list]
                cell_precisions = [r['evaluation']['cell_precision'] for r in method_res_list]
                cell_f1s = [r['evaluation']['cell_f1'] for r in method_res_list]
                table_recalls = [r['evaluation']['table_recall'] for r in method_res_list]
                field_recalls = [r['evaluation']['field_recall'] for r in method_res_list]
                llm_scores = [r['evaluation']['llm_score'] for r in method_res_list]
                
                avg_cell_recall = sum(cell_recalls) / len(cell_recalls)
                avg_cell_precision = sum(cell_precisions) / len(cell_precisions)
                avg_cell_f1 = sum(cell_f1s) / len(cell_f1s)
                avg_table_recall = sum(table_recalls) / len(table_recalls)
                avg_field_recall = sum(field_recalls) / len(field_recalls)
                avg_llm_score = sum(llm_scores) / len(llm_scores)
                
                method_avg_data.append({
                    'method': method_name,
                    'cases': len(method_res_list),
                    'cell_recall': avg_cell_recall,
                    'cell_precision': avg_cell_precision,
                    'cell_f1': avg_cell_f1,
                    'table_recall': avg_table_recall,
                    'field_recall': avg_field_recall,
                    'llm_score': avg_llm_score
                })
                
                logger.info(
                    f"{method_name:30s} | {len(method_res_list):8d} | "
                    f"{avg_cell_recall:7.2f}% | {avg_cell_precision:7.2f}% | "
                    f"{avg_cell_f1:9.2f} | {avg_table_recall:6.1f}% | "
                    f"{avg_field_recall:7.1f}% | {avg_llm_score:7.1f}/100"
                )
            
            logger.info("-" * 160)
            
            if method_avg_data:
                best_by_f1 = max(method_avg_data, key=lambda x: x['cell_f1'])
                best_by_llm = max(method_avg_data, key=lambda x: x['llm_score'])
                
                logger.info(f"\n🏆 最佳方法:")
                logger.info(f"  - Cell F1最高: {best_by_f1['method']} (F1={best_by_f1['cell_f1']:.2f})")
                logger.info(f"  - LLM分数最高: {best_by_llm['method']} (分数={best_by_llm['llm_score']:.1f}/100)")
            
        except Exception as e:
            logger.error(f"打印统一评估摘要失败: {e}")
            import traceback
            traceback.print_exc()
    
    def save_evaluation_report(self, results: List[Dict[str, Any]]) -> Path:
        """生成并保存详细的评估报告"""
        try:
            standards = self.generate_evaluation_standards()
            
            total = len(results)
            success = len([r for r in results if r['status'] == 'success'])
            skipped = len([r for r in results if r['status'] == 'skipped'])
            failed = len([r for r in results if r['status'] == 'failed'])
            
            successful_results = [r for r in results if r['status'] == 'success' and r.get('evaluation')]
            
            if successful_results:
                avg_metrics = {}
                metric_keys = [
                    'cell_recall', 'cell_precision', 'cell_f1',
                    'table_recall', 'table_precision', 
                    'field_recall', 'field_precision',
                    'llm_score',
                    'total_exact_matches', 'total_numeric_precision_errors',
                    'total_string_similarity_matches', 'total_type_mismatches'
                ]
                
                for key in metric_keys:
                    values = [r['evaluation'][key] for r in successful_results if key in r['evaluation']]
                    avg_metrics[key] = sum(values) / len(values) if values else 0
            else:
                avg_metrics = {key: 0 for key in metric_keys}
            
            def get_quality_level(metrics):
                cell_f1 = metrics.get('cell_f1', 0)
                table_recall = metrics.get('table_recall', 0)
                field_recall = metrics.get('field_recall', 0)
                llm_score = metrics.get('llm_score', 0)
                
                if (cell_f1 >= 90 and table_recall >= 90 and 
                    field_recall >= 85 and llm_score >= 85):
                    return "excellent"
                elif (cell_f1 >= 70 and table_recall >= 70 and 
                      field_recall >= 65 and llm_score >= 70):
                    return "good"
                elif (cell_f1 >= 50 and table_recall >= 50 and 
                      field_recall >= 45 and llm_score >= 50):
                    return "acceptable"
                else:
                    return "poor"
            
            overall_quality = get_quality_level(avg_metrics)
            
            report = {
            "metadata": {
                "model": self.model,
                "run_name": self.run_name,
                "document_mode": self.document_mode,
                "timestamp": datetime.now().isoformat(),
                "total_cases": total,
                "evaluation_framework_version": "2.0"
            },
                "summary": {
                    "success_cases": success,
                    "skipped_cases": skipped,
                    "failed_cases": failed,
                    "success_rate": f"{success/total*100:.1f}%" if total > 0 else "0%",
                    "overall_quality_level": overall_quality
                },
                "average_metrics": avg_metrics,
                "evaluation_standards": standards,
                "detailed_results": results
            }
            
            safe_model = self.model.replace('/', '_').replace('\\', '_').replace('-', '_')
            mode = self.document_mode
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"evaluation_report_{safe_model}_{mode}_{timestamp}.json"
            if self.run_name:
                safe_run_name = self.run_name.replace('/', '_').replace('\\', '_')
                report_filename = f"evaluation_report_{safe_model}_{mode}_{safe_run_name}_{timestamp}.json"
            
            report_file = self.output_dir / report_filename
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"\n 详细评估报告已保存到: {report_file}")
            return report_file
            
        except Exception as e:
            logger.error(f"生成评估报告失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def print_evaluation_summary(self):
        """打印评估摘要"""
        try:
            total = len(self.results)
            success = len([r for r in self.results if r['status'] == 'success'])
            skipped = len([r for r in self.results if r['status'] == 'skipped'])
            failed = len([r for r in self.results if r['status'] == 'failed'])
            
            successful_results = [r for r in self.results if r['status'] == 'success' and r.get('evaluation')]
            
            if successful_results:
                cell_recalls = [r['evaluation']['cell_recall'] for r in successful_results]
                cell_precisions = [r['evaluation']['cell_precision'] for r in successful_results]
                cell_f1s = [r['evaluation']['cell_f1'] for r in successful_results]
                
                table_recalls = [r['evaluation']['table_recall'] for r in successful_results]
                table_precisions = [r['evaluation']['table_precision'] for r in successful_results]
                field_recalls = [r['evaluation']['field_recall'] for r in successful_results]
                field_precisions = [r['evaluation']['field_precision'] for r in successful_results]
                
                llm_scores = [r['evaluation']['llm_score'] for r in successful_results]
                
                avg_cell_recall = sum(cell_recalls) / len(cell_recalls)
                avg_cell_precision = sum(cell_precisions) / len(cell_precisions)
                avg_cell_f1 = sum(cell_f1s) / len(cell_f1s)
                avg_table_recall = sum(table_recalls) / len(table_recalls)
                avg_table_precision = sum(table_precisions) / len(table_precisions)
                avg_field_recall = sum(field_recalls) / len(field_recalls)
                avg_field_precision = sum(field_precisions) / len(field_precisions)
                avg_llm_score = sum(llm_scores) / len(llm_scores)
            else:
                avg_cell_recall = avg_cell_precision = avg_cell_f1 = 0
                avg_table_recall = avg_table_precision = 0
                avg_field_recall = avg_field_precision = 0
                avg_llm_score = 0
            
            logger.info("评估摘要:")
            logger.info(f"   配置信息:")
            logger.info(f"     模型: {self.model}")
            logger.info(f"     文档模式: {self.document_mode}")
            if self.run_name:
                logger.info(f"     运行名称: {self.run_name}")
            
            logger.info(f"\n  📈 执行统计:")
            logger.info(f"     总测试案例: {total}")
            logger.info(f"     成功: {success}")
            logger.info(f"     跳过: {skipped}")
            logger.info(f"     失败: {failed}")
            logger.info(f"     成功率: {success/total*100:.1f}%" if total > 0 else "0%")
            
            if successful_results:
                logger.info(f"\n   Cell级别平均指标:")
                logger.info(f"     召回率: {avg_cell_recall:.2f}%")
                logger.info(f"     准确率: {avg_cell_precision:.2f}%")
                logger.info(f"     F1分数: {avg_cell_f1:.2f}")
                
                logger.info(f"\n  ️  结构级别平均指标:")
                logger.info(f"     表召回率: {avg_table_recall:.2f}%")
                logger.info(f"     表准确率: {avg_table_precision:.2f}%")
                logger.info(f"     字段召回率: {avg_field_recall:.2f}%")
                logger.info(f"     字段准确率: {avg_field_precision:.2f}%")
                
                logger.info(f"\n   LLM精细化平均评分:")
                logger.info(f"     总分: {avg_llm_score:.1f}/100")
                
                if (avg_cell_f1 >= 90 and avg_table_recall >= 90 and 
                    avg_field_recall >= 85 and avg_llm_score >= 85):
                    quality_level = "🌟 优秀"
                    quality_desc = "可直接用于生产环境"
                elif (avg_cell_f1 >= 70 and avg_table_recall >= 70 and 
                      avg_field_recall >= 65 and avg_llm_score >= 70):
                    quality_level = "良好"
                    quality_desc = "经过少量调整可用于生产"
                elif (avg_cell_f1 >= 50 and avg_table_recall >= 50 and 
                      avg_field_recall >= 45 and avg_llm_score >= 50):
                    quality_level = "可接受"
                    quality_desc = "需要进一步优化"
                else:
                    quality_level = "较差"
                    quality_desc = "需要重大改进"
                
                logger.info(f"\n   综合质量评级: {quality_level}")
                logger.info(f"     {quality_desc}")
            
            logger.info(f"\n 各案例详细评分:")
            logger.info("-" * 140)
            logger.info(f"{'案例ID':25s} | {'Cell召回':8s} | {'Cell准确':8s} | {'Cell F1':8s} | {'表召回':7s} | {'字段召回':8s} | {'LLM分数':8s} | 状态")
            logger.info("-" * 140)
            
            for result in self.results:
                case_id = result['case_id']
                status = result['status']
                
                if status == 'success' and result.get('evaluation'):
                    eval_data = result['evaluation']
                    cell_recall = eval_data.get('cell_recall', 0)
                    cell_precision = eval_data.get('cell_precision', 0)
                    cell_f1 = eval_data.get('cell_f1', 0)
                    table_recall = eval_data.get('table_recall', 0)
                    field_recall = eval_data.get('field_recall', 0)
                    llm_score = eval_data.get('llm_score', 0)
                    
                    logger.info(f"{case_id:25s} | {cell_recall:6.2f}% | {cell_precision:6.2f}% | {cell_f1:8.2f} | {table_recall:5.1f}% | {field_recall:6.1f}% | {llm_score:6.1f}/100 |  成功")
                elif status == 'skipped':
                    logger.info(f"{case_id:25s} | {'SKIP':8s} | {'SKIP':8s} | {'SKIP':8s} | {'SKIP':7s} | {'SKIP':8s} | {'SKIP':8s} | ️  跳过")
                else:
                    error = result.get('error', 'Unknown error')[:30]
                    logger.info(f"{case_id:25s} | {'FAIL':8s} | {'FAIL':8s} | {'FAIL':8s} | {'FAIL':7s} | {'FAIL':8s} | {'FAIL':8s} |  失败: {error}")
            
            logger.info("-" * 140)
            
        except Exception as e:
            logger.error(f"打印评估摘要失败: {e}")
            import traceback
            traceback.print_exc()
    
    def run_evaluation(self):
        """运行完整的评估流程"""
        logger.info("\n" + "="*80)
        logger.info("Doc2DB 数据集评估开始")
        logger.info("="*80 + "\n")
        
        logger.info("检查后端服务...")
        if not self.check_backend_health():
            logger.error("后端服务不可用，请先启动后端服务")
            logger.error("提示: 在终端运行 'source activate dm && cd backend && python app.py'")
            return
        
        logger.info("扫描数据集...")
        cases = self.scan_datasets()
        
        if not cases:
            logger.error("未找到有效的测试案例")
            return
        
        for i, case_info in enumerate(cases, 1):
            logger.info(f"\n{'#'*80}")
            logger.info(f"# 进度: {i}/{len(cases)}")
            logger.info(f"{'#'*80}")
            
            case_id = f"{case_info['db_name']}/{case_info['case_name']}"
            logger.info(f" 检查案例: {case_id}")
            
            if self.check_case_already_processed(case_info):
                logger.info(f"️  案例已处理，跳过: {case_id}")
                skipped_result = {
                    'case_id': case_id,
                    'db_name': case_info['db_name'],
                    'case_name': case_info['case_name'],
                    'model': self.model,
                    'run_name': self.run_name,
                    'document_mode': self.document_mode,
                    'status': 'skipped',
                    'message': '已有输出文件，跳过处理'
                }
                self.results.append(skipped_result)
                continue
            
            result = self.process_case(case_info)
            self.results.append(result)
            
            time.sleep(1)
        
        logger.info("\n" + "="*80)
        logger.info("生成评估报告...")
        logger.info("="*80 + "\n")
        
        
        self.print_evaluation_summary()
        
        logger.info("\n" + "="*80)
        logger.info("评估完成！")
        logger.info("="*80 + "\n")
    
  
    
    def generate_evaluation_summary_excel(self, output_file: Optional[str] = None) -> Optional[Path]:
        """
        遍历所有的 {method_name}_evaluate.json 文件，汇总评估结果并生成Excel报告
        
        Args:
            output_file: 输出Excel文件路径（可选）。如果不指定，则自动生成在output_dir下
            
        Returns:
            生成的Excel文件路径，失败时返回None
        """
        try:
            import pandas as pd
        except ImportError:
            logger.error("需要安装 pandas 库: pip install pandas openpyxl")
            return None
        
        try:
            logger.info("\n" + "="*80)
            logger.info("生成评估汇总Excel报告")
            logger.info("="*80 + "\n")
            
            evaluate_files = list(self.output_dir.glob("**/*_evaluate.json"))
            
            if not evaluate_files:
                logger.warning("未找到任何评估结果文件")
                return None
            
            logger.info(f" 找到 {len(evaluate_files)} 个评估结果文件")
            
            all_evaluations = []
            
            for eval_file in evaluate_files:
                try:
                    with open(eval_file, 'r', encoding='utf-8') as f:
                        evaluation = json.load(f)
                    
                    parts = eval_file.parts
                    
                    try:
                        output_dir_index = parts.index(self.output_dir.name)
                        db_name = parts[output_dir_index + 1]
                        case_name = parts[output_dir_index + 2]
                        method_name = parts[output_dir_index + 3]
                    except (ValueError, IndexError):
                        logger.warning(f"️  无法解析文件路径: {eval_file}")
                        db_name = eval_file.parent.parent.parent.name if len(eval_file.parts) > 3 else "Unknown"
                        case_name = eval_file.parent.parent.name if len(eval_file.parts) > 2 else "Unknown"
                        method_name = eval_file.parent.name if len(eval_file.parts) > 1 else "Unknown"
                    
                    record = {
                        'Database': db_name,
                        'Case': case_name,
                        'Method': method_name,
                        'Cell Recall (%)': evaluation.get('cell_recall', 0),
                        'Cell Precision (%)': evaluation.get('cell_precision', 0),
                        'Cell F1': evaluation.get('cell_f1', 0),
                        'Table Recall (%)': evaluation.get('table_recall', 0),
                        'Table Precision (%)': evaluation.get('table_precision', 0),
                        'Field Recall (%)': evaluation.get('field_recall', 0),
                        'Field Precision (%)': evaluation.get('field_precision', 0),
                        'LLM Score': evaluation.get('llm_score', 0),
                        'Expected Tables': evaluation.get('expected_tables_count', 0),
                        'Generated Tables': evaluation.get('generated_tables_count', 0),
                        'Correct Tables': evaluation.get('correct_tables_count', 0),
                        'Expected Cells': evaluation.get('total_expected_cells', 0),
                        'Generated Cells': evaluation.get('total_generated_cells', 0),
                        'Correct Cells': evaluation.get('total_correct_cells', 0),
                        'Exact Matches': evaluation.get('total_exact_matches', 0),
                        'Numeric Precision Errors': evaluation.get('total_numeric_precision_errors', 0),
                        'String Similarity Matches': evaluation.get('total_string_similarity_matches', 0),
                        'Type Mismatches': evaluation.get('total_type_mismatches', 0),
                    }
                    
                    all_evaluations.append(record)
                    
                except Exception as e:
                    logger.warning(f"️  读取评估文件失败 {eval_file}: {e}")
                    continue
            
            if not all_evaluations:
                logger.warning("未能成功读取任何评估数据")
                return None
            
            logger.info(f" 成功加载 {len(all_evaluations)} 条评估记录")
            
            df = pd.DataFrame(all_evaluations)
            
            df = df.sort_values(['Database', 'Case', 'Method'])
            
            if output_file is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = self.output_dir / f"evaluation_summary_{timestamp}.xlsx"
            else:
                output_file = Path(output_file)
            
            method_summary = df.groupby('Method').agg({
                'Cell Precision (%)': 'mean',
                'Cell Recall (%)': 'mean', 
                'Cell F1': 'mean',
                'LLM Score': 'mean',
                'Case': 'count'  # 统计每个Method的案例数
            }).round(2)
            
            method_summary.rename(columns={
                'Case': 'Test Cases',
                'Cell Precision (%)': 'Precision (%)',
                'Cell Recall (%)': 'Recall (%)',
                'Cell F1': 'F1',
                'LLM Score': 'LLM Score'
            }, inplace=True)
            method_summary = method_summary.reset_index()
            
            column_order = [
                'Method',
                'Test Cases', 
                'Precision (%)',
                'Recall (%)',
                'F1',
                'LLM Score'
            ]
            method_summary = method_summary[column_order]
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                method_summary.to_excel(writer, sheet_name='Method Summary', index=False)
                
                worksheet = writer.sheets['Method Summary']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 20)  # 限制最大宽度为20
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
            logger.info(f"\n Excel报告生成成功!")
            logger.info(f" 报告路径: {output_file}")
            logger.info(f"\n 报告内容:")
            logger.info(f"  - 方法汇总表: {len(method_summary)} 个方法的评估指标")
            logger.info(f"  - 基于 {len(all_evaluations)} 条评估记录生成")
            
            return output_file
            
        except Exception as e:
            logger.error(f" 生成Excel报告失败: {e}")
            import traceback
            traceback.print_exc()
            return None


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Doc2DB 数据集评测系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例用法:
  python test_dataset_evaluate.py
  
  python test_dataset_evaluate.py --model gpt-4o
  
  python test_dataset_evaluate.py --model gpt-4o --document-mode single
  
  python test_dataset_evaluate.py --model gpt-4o --run-name exp_001
  
  python test_dataset_evaluate.py --model gpt-4o --evaluate-only
  
  python test_dataset_evaluate.py --evaluate-all-methods
  
  python test_dataset_evaluate.py --evaluate-all-methods --force-reevaluate
  
  python test_dataset_evaluate.py --model qwen-long --document-mode multi --run-name test_001
  
  python test_dataset_evaluate.py --model gpt-4o --document-mode multi --evaluate-only --run-name evaluation_001
  
  python test_dataset_evaluate.py --generate-excel
  
  python test_dataset_evaluate.py --generate-excel --excel-output ./my_report.xlsx
  
        '''
    )
    
    parser.add_argument('--model', type=str, default=DEFAULT_MODEL,
                        help=f'使用的模型名称（默认: {DEFAULT_MODEL}）')
    parser.add_argument('--document-mode', type=str, default='multi', choices=['single', 'multi'],
                        help='文档处理模式：single（单文档模式）或 multi（多文档合并模式）（默认: multi）')
    parser.add_argument('--run-name', type=str, default=None,
                        help='运行名称（可选）')
    parser.add_argument('--backend-url', type=str, default=BACKEND_URL,
                        help=f'后端服务URL（默认: {BACKEND_URL}）')
    parser.add_argument('--evaluate-only', action='store_true',
                        help='仅评估模式：只对已有的输出结果进行评估，不重新运行处理')
    parser.add_argument('--evaluate-all-methods', action='store_true',
                        help='评估所有方法：评估所有方法的输出结果（包括外部方法如langchain、langextract等）')
    parser.add_argument('--force-reevaluate', action='store_true',
                        help='Force re-evaluation even if evaluation files exist')
    parser.add_argument('--generate-excel', action='store_true',
                        help='生成Excel汇总报告：遍历所有评估结果文件并生成Excel汇总报告')
    parser.add_argument('--excel-output', type=str,  default=None,
                        help='Excel输出文件路径（可选，默认自动生成在output目录）')
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("🌟 Doc2DB 数据集评测系统")
    print("="*80)
    
    if args.generate_excel:
        print("模式: 生成Excel汇总报告")
        print("="*80 + "\n")
        
        evaluator = Doc2DBEvaluator(
            backend_url=args.backend_url,
            dataset_dir=DATASET_DIR,
            output_dir=OUTPUT_DIR,
            model=args.model,
            run_name=args.run_name,
            document_mode=args.document_mode
        )
        
        try:
            excel_file = evaluator.generate_evaluation_summary_excel(args.excel_output)
            if excel_file:
                print(f"\n Excel报告生成完成: {excel_file}")
            else:
                print("\nExcel报告生成失败")
        except KeyboardInterrupt:
            print("\n\n操作被用户中断")
        except Exception as e:
            print(f"\n\n 生成Excel报告出错: {e}")
            import traceback
            traceback.print_exc()
        
        return
    
    print(f" 模型: {args.model}")
    print(f" 文档模式: {args.document_mode}")
    if args.run_name:
        print(f"️  运行名称: {args.run_name}")
    if args.evaluate_only:
        print("模式: 仅评估已有输出")
    else:
        print("模式: 完整处理+评估")
    print("="*80 + "\n")
    
    if 'CONDA_DEFAULT_ENV' not in os.environ or os.environ['CONDA_DEFAULT_ENV'] != 'dm':
        print("警告: 未检测到dm环境")
        print("💡 提示: 请先运行 'source activate dm'")
        print()
    
    evaluator = Doc2DBEvaluator(
        backend_url=args.backend_url,
        dataset_dir=DATASET_DIR,
        output_dir=OUTPUT_DIR,
        model=args.model,
        run_name=args.run_name,
        document_mode=args.document_mode  # 传递文档模式参数
    )
    
    try:
        if args.evaluate_all_methods:
            evaluator.evaluate_all_methods(skip_evaluated=not args.force_reevaluate)
        elif args.evaluate_only:
            evaluator.evaluate_existing_outputs()
        else:
            evaluator.run_evaluation()
    except KeyboardInterrupt:
        print("\n\n评估被用户中断")
    except Exception as e:
        print(f"\n\n 评估过程出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

