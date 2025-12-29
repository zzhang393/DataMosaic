"""文档定位器 - 从长文档中定位与schema相关的片段

🔧 当前状态：已停用
- entity_extractor.py 中 enable_locate 默认值已改为 False
- relation_extractor.py 中已移除 locator 调用逻辑
- 所有文档现在直接进行抽取，不进行定位处理
"""

import os
import json
import logging
import re
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path

import llm.main as llm_main


@dataclass
class LocatedSegment:
    """定位到的文档片段（精简版）"""
    content: str           # 原始文本内容（一字不改）
    start_position: int    # 在原文档中的起始位置
    end_position: int      # 在原文档中的结束位置
    relevance_score: float # 相关性得分（0-1）
    source_document: str = "" # 来源文档名称（用于多文档模式）


class DocumentLocator:
    """
    文档定位器 - 处理长文档的智能片段定位
    
    核心功能：
    1. 判断文档是否过长需要定位
    2. 使用LLM智能定位与schema相关的原始片段
    3. 保持原文不变（一字不改）
    4. 返回定位后的片段供后续抽取使用
    """
    
    DEFAULT_WORD_THRESHOLD = 30000    # 默认词数阈值
    DEFAULT_MAX_SEGMENT_LENGTH = 2000 # 单个片段最大长度
    DEFAULT_CACHE_DIR = ".cache/locator"  # 默认缓存目录
    
    def __init__(self, 
                 word_threshold: int = DEFAULT_WORD_THRESHOLD,
                 max_segment_length: int = DEFAULT_MAX_SEGMENT_LENGTH,
                 enable_cache: bool = True,
                 cache_dir: str = DEFAULT_CACHE_DIR):
        """
        初始化文档定位器
        
        Args:
            word_threshold: 词数阈值，超过此阈值才进行定位
            max_segment_length: 单个片段最大长度
            enable_cache: 是否启用缓存功能（默认True）
            cache_dir: 缓存文件存储目录（默认 .cache/locator）
        
        注意：
        - unified_orchestrator 负责多文档的分批策略
        - extractor 使用 should_locate 判断是否需要定位
        - 此类提供文档定位的核心功能
        """
        self.word_threshold = word_threshold
        self.max_segment_length = max_segment_length
        self.enable_cache = enable_cache
        self.cache_dir = cache_dir
        self.logger = logging.getLogger('doc2db.locator')
        
        if self.enable_cache:
            Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
    
    def _estimate_word_count(self, text_content: str) -> int:
        """
        估算文本的词数
        
        Args:
            text_content: 文本内容
            
        Returns:
            int: 估算的词数
        """
        if not text_content:
            return 0
        
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text_content))
        english_words = len(re.findall(r'[a-zA-Z]+', text_content))
        return chinese_chars + english_words
    
    def should_locate(self, text_content: str) -> bool:
        """
        判断文档是否需要进行定位（即是否过长）
        
        Args:
            text_content: 文档内容
            
        Returns:
            bool: 是否需要定位
        """
        if not text_content or not text_content.strip():
            return False
        
        estimated_word_count = self._estimate_word_count(text_content)
        
        needs_locate = estimated_word_count > self.word_threshold
        
        return needs_locate
    
    def _generate_cache_key(self, 
                           text_content: str, 
                           schema: Dict[str, Any], 
                           table_name: str,
                           nl_prompt: str = "") -> str:
        """
        生成缓存键（基于文档内容和schema的哈希）
        
        Args:
            text_content: 文档内容
            schema: schema定义
            table_name: 表名
            nl_prompt: 自然语言提示
            
        Returns:
            str: 缓存键（MD5哈希值）
        """
        cache_input = json.dumps({
            'text_content_hash': hashlib.md5(text_content.encode('utf-8')).hexdigest(),
            'schema': schema,
            'table_name': table_name,
            'nl_prompt': nl_prompt,
            'max_segment_length': self.max_segment_length
        }, sort_keys=True, ensure_ascii=False)
        
        cache_key = hashlib.md5(cache_input.encode('utf-8')).hexdigest()
        return cache_key
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """
        获取缓存文件路径
        
        Args:
            cache_key: 缓存键
            
        Returns:
            Path: 缓存文件路径
        """
        return Path(self.cache_dir) / f"{cache_key}.json"
    
    def _load_from_cache(self, cache_key: str) -> Optional[List[LocatedSegment]]:
        """
        从缓存加载定位结果
        
        Args:
            cache_key: 缓存键
            
        Returns:
            Optional[List[LocatedSegment]]: 缓存的片段列表，如果不存在则返回None
        """
        if not self.enable_cache:
            return None
        
        cache_path = self._get_cache_path(cache_key)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            if not isinstance(cache_data, dict) or 'segments' not in cache_data:
                self.logger.warning(f"缓存文件格式无效: {cache_path}")
                return None
            
            segments = []
            for seg_dict in cache_data['segments']:
                segments.append(LocatedSegment(
                    content=seg_dict['content'],
                    start_position=seg_dict['start_position'],
                    end_position=seg_dict['end_position'],
                    relevance_score=seg_dict['relevance_score'],
                    source_document=seg_dict.get('source_document', '')
                ))
            
            return segments
            
        except Exception as e:
            self.logger.warning(f"加载缓存失败: {e}")
            return None
    
    def _save_to_cache(self, cache_key: str, segments: List[LocatedSegment], 
                      metadata: Dict[str, Any] = None):
        """
        保存定位结果到缓存
        
        Args:
            cache_key: 缓存键
            segments: 定位到的片段列表
            metadata: 额外的元数据（可选）
        """
        if not self.enable_cache:
            return
        
        cache_path = self._get_cache_path(cache_key)
        
        try:
            segments_data = [
                {
                    'content': seg.content,
                    'start_position': seg.start_position,
                    'end_position': seg.end_position,
                    'relevance_score': seg.relevance_score,
                    'source_document': seg.source_document
                }
                for seg in segments
            ]
            
            cache_data = {
                'version': '1.0',
                'segments': segments_data,
                'metadata': metadata or {},
                'timestamp': self._get_timestamp()
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"缓存已保存: {cache_path}")
            
        except Exception as e:
            self.logger.warning(f"保存缓存失败: {e}")
    
    def _get_timestamp(self) -> str:
        """获取当前时间戳"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def locate_relevant_segments(self,
                                 text_content: str,
                                 schema: Dict[str, Any],
                                 table_name: str,
                                 nl_prompt: str = "",
                                 context = None,
                                 source_document: str = "") -> List[LocatedSegment]:
        """
        从文档中定位与schema相关的片段
        
        注意：
        - 调用此方法前应先使用 should_locate() 判断是否需要定位
        - 此方法直接进行定位处理，不做文本长度判断
        - unified_orchestrator 负责多文档的分批策略
        
        Args:
            text_content: 完整文档内容
            schema: 数据库schema定义
            table_name: 目标表名
            nl_prompt: 自然语言提示
            context: 处理上下文
            source_document: 来源文档名称
            
        Returns:
            List[LocatedSegment]: 定位到的相关片段列表
        """
        self.logger.info(f"开始定位文档片段: {table_name}")
        
        if self.enable_cache:
            cache_key = self._generate_cache_key(text_content, schema, table_name, nl_prompt)
            cached_segments = self._load_from_cache(cache_key)
            
            if cached_segments is not None:
                self.logger.info(f"从缓存加载定位结果: {len(cached_segments)} 个片段")
                if context and hasattr(context, 'step_outputs'):
                    from ...core.io import IOManager
                    context.step_outputs.append({
                        'step': f'locator_document_{table_name}',
                        'status': 'completed',
                        'description': f'文档片段定位 [表: {table_name}] (从缓存)',
                        'details': {
                            'model': 'cached',
                            'segments_found': len(cached_segments),
                            'segments_info': [
                                {
                                    'preview': s.content[:100] + '...' if len(s.content) > 100 else s.content
                                }
                                for s in cached_segments
                            ]
                        },
                        'timestamp': IOManager.get_timestamp()
                    })
                
                return cached_segments
        
        word_count = self._estimate_word_count(text_content)
        self.logger.info(f"文档词数: {word_count}")
        
        segments = self._locate_single_document(
            text_content=text_content,
            schema=schema,
            table_name=table_name,
            nl_prompt=nl_prompt,
            context=context,
            source_document=source_document
        )
        
        if self.enable_cache:
            cache_key = self._generate_cache_key(text_content, schema, table_name, nl_prompt)
            metadata = {
                'table_name': table_name,
                'document_length': len(text_content),
                'word_count': word_count,
                'source_document': source_document
            }
            self._save_to_cache(cache_key, segments, metadata)
        
        return segments
    
    def _locate_single_document(self,
                               text_content: str,
                               schema: Dict[str, Any],
                               table_name: str,
                               nl_prompt: str = "",
                               context = None,
                               source_document: str = "") -> List[LocatedSegment]:
        """
        对单个文档进行定位（内部方法）
        
        Args:
            text_content: 完整文档内容
            schema: 数据库schema定义
            table_name: 目标表名
            nl_prompt: 自然语言提示
            context: 处理上下文
            source_document: 来源文档名称
            
        Returns:
            List[LocatedSegment]: 定位到的相关片段列表
        """
        
        model = 'gemini-2.5-pro'
        
        table_def = self._get_table_definition(schema, table_name)
        if not table_def:
            self.logger.error(f"未找到表定义: {table_name}")
            return [LocatedSegment(
                content=text_content,
                start_position=0,
                end_position=len(text_content),
                relevance_score=1.0,
                source_document=source_document
            )]
        
        field_descriptions = self._build_field_descriptions(table_def)
        
        try:
            self.logger.info(f'🔍 开始定位相关片段 - 表: {table_name}, 文档长度: {len(text_content)}')
            
            prompt = self._build_locate_prompt(
                text_content=text_content,
                table_name=table_name,
                field_descriptions=field_descriptions,
                nl_prompt=nl_prompt,
                max_segment_length=self.max_segment_length
            )
            
            self.logger.info(f'🔍 LLM输入 - 表: {table_name}')
            
            response = llm_main.get_answer(
                question=prompt,
                model='gemini-2.5-pro' #qwen-long
            )
            
            self.logger.info(f'🔍 LLM输出 - 表: {table_name}')
            if response:
                self.logger.debug(f'  完整Response:\n{response[:2000]}...' if len(response) > 2000 else f'  完整Response:\n{response}')
            else:
                self.logger.warning(f'  ⚠️ LLM返回空响应！')
            
            if not response or len(response.strip()) <= 1:
                self.logger.warning(f'  ⚠️ 第一次LLM调用失败，尝试GPT-4o fallback')
                response = llm_main.get_answer(
                    question=prompt,
                    model='gpt-4o'
                )
                self.logger.info(f'🔍 GPT-4o Fallback输出')
                if response:
                    self.logger.debug(f'  完整Response:\n{response[:2000]}...' if len(response) > 2000 else f'  完整Response:\n{response}')
            
            segments = self._parse_llm_response(
                response, text_content, source_document
            )
            
            for i, seg in enumerate(segments):
                preview = seg.content[:100].replace('\n', ' ') + '...' if len(seg.content) > 100 else seg.content.replace('\n', ' ')
                if seg.source_document:
                    self.logger.debug(f'  片段 {i+1}: 长度={len(seg.content)}, 相关性={seg.relevance_score:.2f}, 来源={seg.source_document}, 预览={preview}')
                else:
                    self.logger.debug(f'  片段 {i+1}: 长度={len(seg.content)}, 相关性={seg.relevance_score:.2f}, 预览={preview}')
            
            if context and hasattr(context, 'step_outputs'):
                from ...core.io import IOManager
                context.step_outputs.append({
                    'step': f'locator_document_{table_name}',
                    'status': 'completed',
                    'description': f'文档片段定位 [表: {table_name}]',
                    'details': {
                        'model': model,
                        'original_length': len(text_content),
                        'segments_found': len(segments),
                        'total_segment_length': sum(len(s.content) for s in segments),
                        'compression_ratio': f"{sum(len(s.content) for s in segments) / len(text_content) * 100:.1f}%",
                        'segments_info': [
                            {
                                'length': len(s.content),
                                'relevance': s.relevance_score,
                                'preview': s.content[:100] + '...' if len(s.content) > 100 else s.content
                            }
                            for s in segments
                        ]
                    },
                    'timestamp': IOManager.get_timestamp()
                })
            
            return segments
            
        except Exception as e:
            self.logger.error(f"定位失败: {e}")
            
            return [LocatedSegment(
                content=text_content,
                start_position=0,
                end_position=len(text_content),
                relevance_score=1.0,
                source_document=source_document
            )]
    
    
    def _get_table_definition(self, schema: Dict[str, Any], table_name: str) -> Optional[Dict[str, Any]]:
        """获取表定义（与BaseExtractor逻辑一致）"""
        
        if 'table_name' in schema and 'tables' in schema and len(schema['tables']) == 1:
            result = schema['tables'][0].copy()
            if 'fields' in result and 'attributes' not in result:
                result['attributes'] = result.pop('fields')
            return result
        
        tables = schema.get('tables', [])
        
        for table in tables:
            if table.get('name') == table_name:
                result = table.copy()
                if 'fields' in result and 'attributes' not in result:
                    result['attributes'] = result.pop('fields')
                return result
        
        if '_' in table_name and table_name.rsplit('_', 1)[-1].isdigit():
            base_name = table_name.rsplit('_', 1)[0]
            suffix_num = int(table_name.rsplit('_', 1)[1])
            
            matching_tables = [
                table for table in tables
                if isinstance(table, dict) and table.get('name') == base_name
            ]
            
            if matching_tables and 1 <= suffix_num <= len(matching_tables):
                result = matching_tables[suffix_num - 1].copy()
                if 'fields' in result and 'attributes' not in result:
                    result['attributes'] = result.pop('fields')
                return result
        
        if schema.get('table_name') == table_name and 'columns' in schema:
            columns = schema.get('columns', {})
            if isinstance(columns, dict):
                attributes = [
                    {
                        'name': col_name,
                        'type': col_def.get('type', ''),
                        'description': col_def.get('description', '')
                    }
                    for col_name, col_def in columns.items()
                ]
            else:
                attributes = columns
            
            return {
                'name': table_name,
                'attributes': attributes
            }
        
        return None
    
    def _build_field_descriptions(self, table_def: Dict[str, Any]) -> str:
        """构建字段描述信息"""
        attributes = table_def.get('attributes', [])
        
        field_descriptions = []
        for attr in attributes:
            field_info = f"- {attr['name']} ({attr.get('type', 'TEXT')})"
            if 'description' in attr:
                field_info += f": {attr['description']}"
            field_descriptions.append(field_info)
        
        return '\n'.join(field_descriptions)
    
    def _build_locate_prompt(self,
                            text_content: str,
                            table_name: str,
                            field_descriptions: str,
                            nl_prompt: str,
                            max_segment_length: int) -> str:
        """构建文档定位的 prompt"""
        prompt = f"""我有一个长文档，需要定位与数据抽取schema相关的原始片段。

=== 目标表格信息 ===
表名: {table_name}

字段定义:
{field_descriptions}

=== 任务要求 ===
请分析以下文档内容，采用**宽泛匹配策略**，定位所有可能与上述字段相关的片段。

 核心原则：宁可多提取，不要漏掉！
1. 提取片段时保留主要内容，可以适当清理多余的空格、换行等格式噪音
2. 包含足够的上下文（至少一个完整句子/段落）
3. 采用**宽泛匹配**：只要内容可能与字段有关联，就应该提取
4. 提供相关性得分（0-1）：即使相关性较低（如0.3-0.5）也应该提取

📏 什么样的内容应该被提取？（宽泛匹配原则）
✅ 直接包含字段信息的内容（相关性 0.8-1.0）
   例如：包含姓名、年龄、工资等具体数据的段落
   
✅ 间接相关的内容，如背景信息、说明文字（相关性 0.5-0.7）
   例如：部门介绍、职位说明、薪资体系说明等
   
✅ 可能包含隐含信息的内容（相关性 0.3-0.5）
   例如：公司组织架构、人员统计、相关政策文件等
   
✅ 描述性内容、上下文信息、相关章节标题（相关性 0.2-0.4）
   例如：章节标题、目录、表格标题、注释说明等
   
✅ 任何表格、列表、结构化数据都应该完整提取
   即使不确定是否相关，也应该提取（相关性可以标低一些）

❌ 只有完全无关的内容才不提取
   例如：完全不涉及目标字段的其他话题章节

📐 片段选择策略：
- 优先选择完整段落/章节而非句子片段
- 单个片段最大长度: {max_segment_length} 字符
- 如果相邻片段都可能相关，合并它们成为一个大片段
- 如果整个文档都相关或可能相关，可以返回完整文档作为单个片段
- **重要**：对于表格、列表、结构化数据，一定要完整提取整个表格/列表
- 可以适当清理内容中的多余空格、连续换行等格式噪音，保持内容可读即可

📝 额外提示: {nl_prompt if nl_prompt else '无'}

⚠️ 记住：我们的目标是**不要漏掉任何可能有用的信息**，提取宽泛一些总比遗漏关键信息要好！

=== 文档内容开始 ===
（注意：下面的分隔线"=== 文档内容开始 ==="仅用于标记位置，不是文档的一部分！
你返回的content字段必须只包含文档内的原始文本，不要包含这些分隔标记！）

{text_content}

=== 文档内容结束 ===

=== 输出要求 ===
⚠️ 重要提醒：
1. content字段必须是从上面"文档内容开始"和"文档内容结束"之间提取的文本
2. 不要包含任何分隔标记（如"===目标表格信息===" "===文档内容开始===" "===文档内容结束==="等）
3. 可以适当清理多余的空格（多个连续空格可以合并为一个）、多余的换行等格式问题
4. 保留文本的核心内容和结构，确保信息完整

返回所有定位到的相关或可能相关的片段。宁可多返回也不要遗漏。
如果整个文档都相关或可能相关，可以返回一个包含完整文档的片段。
只有在文档内容完全无关时才返回空数组。

请以 JSON 数组格式返回结果：
[
  {{
    "content": "文档中提取的文本内容（可适当清理格式）",
    "relevance_score": 0.85
  }},
  {{
    "content": "另一段相关内容（可适当清理格式）",
    "relevance_score": 0.45
  }},
  ...
]

请确保返回的是有效的 JSON 格式，不要包含其他说明文字。
"""
        return prompt
    
    def _clean_prompt_markers(self, content: str) -> str:
        """
        清理LLM返回内容中可能混入的prompt标记
        
        Args:
            content: LLM返回的内容
            
        Returns:
            清理后的内容
        """
        markers_to_remove = [
            '=== 目标表格信息 ===',
            '=== 任务要求 ===',
            '=== 文档内容 ===',
            '=== 文档内容开始 ===',
            '=== 文档内容结束 ===',
            '=== 输出要求 ===',
        ]
        
        cleaned = content
        for marker in markers_to_remove:
            if marker in cleaned:
                self.logger.warning(f'⚠️ 检测到LLM返回内容中包含prompt标记: "{marker}"，正在清理...')
                parts = cleaned.split(marker)
                if len(parts) > 1:
                    cleaned = parts[1].strip()
        
        return cleaned
    
    def _validate_content_in_original(self, content: str, original_text: str) -> bool:
        """
        验证LLM返回的content是否真的存在于原文中（允许空白符差异）
        
        Args:
            content: LLM返回的片段内容
            original_text: 原始文档
            
        Returns:
            True表示内容有效，False表示可能混入了prompt标记
        """
        import re
        
        prompt_indicators = [
            '=== 目标表格信息',
            '=== 任务要求',
            '=== 文档内容',
            '=== 输出要求',
            '字段定义:',
            'relevance_score'
        ]
        
        for indicator in prompt_indicators:
            if indicator in content[:200]:  # 只检查前200个字符
                self.logger.warning(f'⚠️ 检测到content中包含prompt标记: "{indicator}"')
                return False
        
        if content in original_text:
            return True
        
        content_stripped = content.strip()
        if content_stripped in original_text:
            return True
        
        content_normalized = re.sub(r'\s+', ' ', content_stripped.lower())
        
        if len(content_normalized) < 30:
            original_normalized = re.sub(r'\s+', ' ', original_text.lower())
            return content_normalized in original_normalized
        
        anchor_length = min(100, len(content_normalized))
        anchor = content_normalized[:anchor_length]
        
        original_lower = original_text.lower()
        window_size = anchor_length + 500  # 给予一定容错空间
        
        for start_idx in range(0, len(original_lower) - anchor_length + 1, 100):
            end_idx = min(start_idx + window_size, len(original_lower))
            window = original_lower[start_idx:end_idx]
            window_normalized = re.sub(r'\s+', ' ', window)
            
            if anchor in window_normalized:
                return True
        
        return False
    
    def _parse_llm_response(self,
                           response: str,
                           original_text: str,
                           source_document: str = "") -> List[LocatedSegment]:
        """
        解析 LLM 返回的 JSON 格式结果
        
        Args:
            response: LLM 返回的文本
            original_text: 原始文档文本
            source_document: 来源文档名称
            
        Returns:
            转换后的LocatedSegment列表
        """
        segments = []
        
        if not response or not response.strip():
            self.logger.warning(f'⚠️ LLM响应为空，返回完整文档作为fallback')
            return [LocatedSegment(
                content=original_text,
                start_position=0,
                end_position=len(original_text),
                relevance_score=1.0,
                source_document=source_document
            )]
        
        try:
            response = response.strip()
            
            if response.startswith('```'):
                lines = response.split('\n')
                if len(lines) > 1:
                    response = '\n'.join(lines[1:])
                if response.endswith('```'):
                    response = response[:-3].strip()
            
            parsed_segments = json.loads(response)
            
            if not isinstance(parsed_segments, list):
                raise ValueError("返回的不是数组格式")
            
            if not parsed_segments:
                self.logger.warning(f'⚠️ LLM返回空数组，使用完整文档作为fallback')
                return [LocatedSegment(
                    content=original_text,
                    start_position=0,
                    end_position=len(original_text),
                    relevance_score=1.0,
                    source_document=source_document
                )]
            
            
            if len(parsed_segments) == 0:
                self.logger.warning(f'⚠️ 未能解析出任何片段，将使用完整文档内容')
            
            for i, seg_dict in enumerate(parsed_segments):
                try:
                    content = seg_dict.get('content', '')
                    
                    if not content or not content.strip():
                        continue
                    
                    if not self._validate_content_in_original(content, original_text):
                        self.logger.warning(f'⚠️ 片段 {i+1} 内容未在原文中找到，可能混入了prompt标记，尝试清理...')
                        cleaned_content = self._clean_prompt_markers(content)
                        if cleaned_content != content and self._validate_content_in_original(cleaned_content, original_text):
                            self.logger.info(f'✅ 片段 {i+1} 清理成功，使用清理后的内容')
                            content = cleaned_content
                        else:
                            self.logger.warning(f'⚠️ 片段 {i+1} 清理失败，跳过此片段')
                            continue
                    
                    import re
                    content_stripped = content.strip()
                    
                    start_pos = original_text.find(content_stripped)
                    if start_pos >= 0:
                        end_pos = start_pos + len(content_stripped)
                    else:
                        anchor_length = min(100, len(content_stripped))
                        if anchor_length > 20:  # 确保锚点足够长
                            anchor = content_stripped[:anchor_length]
                            anchor_normalized = re.sub(r'\s+', ' ', anchor.lower().strip())
                            
                            original_lower = original_text.lower()
                            window_size = len(anchor) + 300  # 给予一定容错空间
                            best_match_pos = -1
                            
                            for start_idx in range(0, len(original_text) - len(anchor_normalized) + 1, 100):
                                end_idx = min(start_idx + window_size, len(original_text))
                                window = original_lower[start_idx:end_idx]
                                window_normalized = re.sub(r'\s+', ' ', window.strip())
                                
                                if anchor_normalized in window_normalized:
                                    best_match_pos = start_idx
                                    break
                            
                            if best_match_pos >= 0:
                                start_pos = best_match_pos
                                estimated_end = best_match_pos + int(len(content_stripped) * 1.2)  # 允许20%的长度差异
                                end_pos = min(estimated_end, len(original_text))
                            else:
                                simple_anchor = anchor[:50] if len(anchor) >= 50 else anchor
                                anchor_pos = original_lower.find(simple_anchor.lower())
                                if anchor_pos >= 0:
                                    start_pos = anchor_pos
                                    end_pos = anchor_pos + len(content_stripped)
                                else:
                                    self.logger.warning(f'⚠️ 片段 {i+1} 无法定位（锚点: "{anchor[:30]}..."），fallback到完整文档')
                                    start_pos = 0
                                    end_pos = len(original_text)
                        else:
                            self.logger.warning(f'⚠️ 片段 {i+1} 内容太短({len(content_stripped)}字符)，fallback到完整文档')
                            start_pos = 0
                            end_pos = len(original_text)
                    
                    relevance_score = float(seg_dict.get('relevance_score', 0.8))
                    
                    segment = LocatedSegment(
                        content=content,
                        start_position=start_pos,
                        end_position=end_pos,
                        relevance_score=relevance_score,
                        source_document=source_document
                    )
                    
                    segments.append(segment)
                    preview = content[:80].replace('\n', ' ') + '...' if len(content) > 80 else content.replace('\n', ' ')
                    
                except Exception as e:
                    self.logger.warning(f"解析片段 {i+1} 失败: {e}")
                    continue
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ JSON解析失败: {e}")
            self.logger.warning(f"⚠️ JSON解析失败，使用完整文档作为fallback")
            segments = [LocatedSegment(
                content=original_text,
                start_position=0,
                end_position=len(original_text),
                relevance_score=1.0,
                source_document=source_document
            )]
        except Exception as e:
            self.logger.error(f"解析LLM响应失败: {e}")
            segments = [LocatedSegment(
                content=original_text,
                start_position=0,
                end_position=len(original_text),
                relevance_score=1.0,
                source_document=source_document
            )]
        
        if not segments:
            segments = [LocatedSegment(
                content=original_text,
                start_position=0,
                end_position=len(original_text),
                relevance_score=1.0,
                source_document=source_document
            )]
        
        self._print_location_stats(segments, original_text)
        
        return segments
    
    def _print_location_stats(self, segments: List[LocatedSegment], original_text: str):
        """打印定位统计信息"""
        if not segments:
            return
        
        total_located_chars = sum(len(seg.content) for seg in segments)
        coverage_ratio = total_located_chars / len(original_text) if len(original_text) > 0 else 0
        avg_relevance = sum(seg.relevance_score for seg in segments) / len(segments)
        
        
        high_relevance = sum(1 for seg in segments if seg.relevance_score >= 0.7)
        medium_relevance = sum(1 for seg in segments if 0.4 <= seg.relevance_score < 0.7)
        low_relevance = sum(1 for seg in segments if seg.relevance_score < 0.4)
        
    
    def merge_segments(self, segments: List[LocatedSegment]) -> str:
        """
        合并多个片段为单一文本（用于后续提取）
        
        Args:
            segments: 定位到的片段列表
            
        Returns:
            str: 合并后的文本内容（包含来源文档标注和相关性信息）
        """
        if not segments:
            return ""
        
        if len(segments) == 1:
            seg = segments[0]
            header = self._build_segment_header(seg, 1, 1)
            return f"{header}\n\n{seg.content}"
        
        sorted_segments = sorted(
            segments,
            key=lambda s: (-s.relevance_score, s.start_position if s.start_position >= 0 else float('inf'))
        )
        
        merged_parts = []
        for i, seg in enumerate(sorted_segments, 1):
            header = self._build_segment_header(seg, i, len(sorted_segments))
            merged_parts.append(f"{header}\n\n{seg.content}")
        
        separator = "\n\n" + "="*80 + "\n\n"
        merged = separator.join(merged_parts)
        
        return merged
    
    def _build_segment_header(self, segment: LocatedSegment, index: int, total: int) -> str:
        """
        构建片段头部标注信息
        
        Args:
            segment: 片段对象
            index: 片段编号
            total: 总片段数
            
        Returns:
            str: 格式化的头部标注
        """
        header_lines = [
            "=" * 80,
            f"【片段 {index}/{total}】",
        ]
        
        if segment.source_document:
            header_lines.append(f"来源文档: {segment.source_document}")
        
        header_lines.append(f"相关性得分: {segment.relevance_score:.2f}")
        header_lines.append("=" * 20)
        
        return "\n".join(header_lines)
    
    
    def concatenate_documents(self, 
                             text_contents: List[str], 
                             document_names: List[str] = None) -> Tuple[str, List[Dict[str, Any]]]:
        """
        将多个文档拼接成一个大文档，并标注每个文档的边界
        
        Args:
            text_contents: 文档内容列表
            document_names: 文档名称列表（可选，默认使用 doc_1, doc_2...）
            
        Returns:
            Tuple[str, List[Dict]]: 
                - 拼接后的完整文档
                - 文档边界信息列表 [{'name': 'doc1', 'start': 0, 'end': 100}, ...]
        """
        if not text_contents:
            return "", []
        
        if document_names is None or len(document_names) != len(text_contents):
            document_names = [f"doc_{i+1}" for i in range(len(text_contents))]
        
        
        concatenated_parts = []
        document_boundaries = []
        current_position = 0
        
        for i, (text_content, doc_name) in enumerate(zip(text_contents, document_names)):
            separator = f"\n{'='*80}\n【文档 {i+1}: {doc_name}】\n{'='*80}\n\n"
            
            if i > 0:  # 第一个文档前不加分隔符
                concatenated_parts.append(separator)
                current_position += len(separator)
            
            doc_start = current_position
            concatenated_parts.append(text_content)
            current_position += len(text_content)
            doc_end = current_position
            
            document_boundaries.append({
                'name': doc_name,
                'start': doc_start,
                'end': doc_end,
                'length': len(text_content)
            })
            
        
        concatenated_text = "".join(concatenated_parts)
        
        
        return concatenated_text, document_boundaries
    
    def locate_from_multi_documents(self,
                                    text_contents: List[str],
                                    document_names: List[str],
                                    schema: Dict[str, Any],
                                    table_name: str,
                                    nl_prompt: str = "",
                                    context = None) -> List[LocatedSegment]:
        """
        从多个文档中定位相关片段（用于entity_extractor的batch模式）
        
        注意：此方法处理已经分好的batch，不再进行额外的分批。
        unified_orchestrator 已经负责了文档的分批策略。
        
        Args:
            text_contents: 多个文档内容列表（已经是一个batch）
            document_names: 文档名称列表
            schema: 数据库schema定义
            table_name: 目标表名
            nl_prompt: 自然语言提示
            context: 处理上下文
            
        Returns:
            List[LocatedSegment]: 定位到的相关片段列表（带来源标注）
        """
        if document_names is None or len(document_names) != len(text_contents):
            document_names = [f"doc_{i+1}" for i in range(len(text_contents))]
        
        self.logger.info(f'多文档定位: {len(text_contents)} 个文档')
        
        if self.enable_cache:
            combined_text = '\n\n'.join(text_contents)
            cache_key = self._generate_cache_key(combined_text, schema, table_name, nl_prompt)
            cached_segments = self._load_from_cache(cache_key)
            
            if cached_segments is not None:
                self.logger.info(f"从缓存加载多文档定位结果: {len(cached_segments)} 个片段")
                return cached_segments
        
        concatenated_text, document_boundaries = self.concatenate_documents(
            text_contents, document_names
        )
        
        if not self.should_locate(concatenated_text):
            self.logger.info('文档长度未超过阈值，返回完整文档')
            segments = []
            for text_content, doc_name in zip(text_contents, document_names):
                segments.append(LocatedSegment(
                    content=text_content,
                    start_position=0,
                    end_position=len(text_content),
                    relevance_score=1.0,
                    source_document=doc_name
                ))
            return segments
        
        self.logger.info(f'文档需要定位，总长度: {len(concatenated_text)} 字符')
        
        source_doc_name = document_names[0] if len(document_names) == 1 else "concatenated"
        
        located_segments = self.locate_relevant_segments(
            text_content=concatenated_text,
            schema=schema,
            table_name=table_name,
            nl_prompt=nl_prompt,
            context=context,
            source_document=source_doc_name
        )
        
        total_segments = len(located_segments)
        fallback_segments = 0
        concat_length = len(concatenated_text)
        
        for segment in located_segments:
            if (segment.start_position == 0 and 
                segment.end_position >= concat_length * 0.95):  # 允许5%的误差
                fallback_segments += 1
        
        fallback_ratio = fallback_segments / total_segments if total_segments > 0 else 0
        has_unreliable_positions = fallback_ratio > 0.5
        
        if has_unreliable_positions:
            self.logger.warning(
                f'⚠️ 检测到locate效果不佳：{fallback_segments}/{total_segments} '
                f'个片段为完整文档 ({fallback_ratio*100:.1f}%)，放弃locate'
            )
        
        if has_unreliable_positions:
            segments_with_source = []
            for doc_idx, (text_content, doc_name) in enumerate(zip(text_contents, document_names)):
                segments_with_source.append(LocatedSegment(
                    content=text_content,
                    start_position=0,
                    end_position=len(text_content),
                    relevance_score=1.0,  # 完整文档，相关性设为1.0
                    source_document=doc_name
                ))
                self.logger.info(f'  返回完整文档 {doc_idx+1}: {doc_name} ({len(text_content)} 字符)')
        else:
            self.logger.info(
                f'✅ Locate成功：{total_segments - fallback_segments}/{total_segments} '
                f'个片段成功定位 ({(1-fallback_ratio)*100:.1f}%)，使用定位结果'
            )
            segments_with_source = []
            for seg_idx, segment in enumerate(located_segments):
                segment_start = segment.start_position
                source_doc = "unknown"
                
                for boundary in document_boundaries:
                    if boundary['start'] <= segment_start < boundary['end']:
                        source_doc = boundary['name']
                        break
                
                new_segment = LocatedSegment(
                    content=segment.content,
                    start_position=segment.start_position,
                    end_position=segment.end_position,
                    relevance_score=segment.relevance_score,
                    source_document=source_doc
                )
                segments_with_source.append(new_segment)
        
        if self.enable_cache:
            combined_text = '\n\n'.join(text_contents)
            cache_key = self._generate_cache_key(combined_text, schema, table_name, nl_prompt)
            metadata = {
                'table_name': table_name,
                'document_count': len(text_contents),
                'document_names': document_names
            }
            self._save_to_cache(cache_key, segments_with_source, metadata)
        
        self.logger.info(f'多文档定位完成: {len(segments_with_source)} 个片段')
        return segments_with_source
    
    
    def get_segments_summary(self, segments: List[LocatedSegment]) -> Dict[str, Any]:
        """
        获取片段摘要信息
        
        Args:
            segments: 片段列表
            
        Returns:
            Dict: 摘要信息
        """
        if not segments:
            return {
                'total_segments': 0,
                'total_length': 0,
                'average_relevance': 0.0
            }
        
        return {
            'total_segments': len(segments),
            'total_length': sum(len(seg.content) for seg in segments),
            'average_relevance': sum(seg.relevance_score for seg in segments) / len(segments),
            'segments_info': [
                {
                    'length': len(seg.content),
                    'relevance': seg.relevance_score
                }
                for seg in segments
            ]
        }
    
    def clear_cache(self, older_than_days: int = None) -> int:
        """
        清理缓存文件
        
        Args:
            older_than_days: 只清理指定天数之前的缓存（None表示清理所有缓存）
            
        Returns:
            int: 清理的文件数量
        """
        if not self.enable_cache:
            return 0
        
        cache_path = Path(self.cache_dir)
        if not cache_path.exists():
            return 0
        
        cache_files = list(cache_path.glob("*.json"))
        
        if not cache_files:
            return 0
        
        deleted_count = 0
        
        if older_than_days is None:
            for cache_file in cache_files:
                try:
                    cache_file.unlink()
                    deleted_count += 1
                except Exception as e:
                    self.logger.warning(f"删除缓存文件失败: {cache_file}, {e}")
            
        else:
            import time
            cutoff_time = time.time() - (older_than_days * 24 * 60 * 60)
            
            for cache_file in cache_files:
                try:
                    if cache_file.stat().st_mtime < cutoff_time:
                        cache_file.unlink()
                        deleted_count += 1
                except Exception as e:
                    self.logger.warning(f"删除缓存文件失败: {cache_file}, {e}")
            
        
        return deleted_count
    
    def clear_multi_document_cache(self) -> int:
        """
        清理多文档模式的缓存文件（保留单文档模式的缓存）
        
        通过检查缓存文件的metadata字段来区分：
        - 多文档模式: metadata中包含 'document_names' 或 'document_count' > 1
        - 单文档模式: metadata中只有 'source_document'
        
        Returns:
            int: 清理的文件数量
        """
        if not self.enable_cache:
            return 0
        
        cache_path = Path(self.cache_dir)
        if not cache_path.exists():
            return 0
        
        cache_files = list(cache_path.glob("*.json"))
        
        if not cache_files:
            return 0
        
        
        deleted_count = 0
        single_doc_count = 0
        error_count = 0
        
        for cache_file in cache_files:
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                metadata = cache_data.get('metadata', {})
                
                is_multi_doc = (
                    'document_names' in metadata or 
                    metadata.get('document_count', 0) > 1
                )
                
                if is_multi_doc:
                    cache_file.unlink()
                    deleted_count += 1
                    doc_names = metadata.get('document_names', [])
                    doc_count = metadata.get('document_count', len(doc_names))
                else:
                    single_doc_count += 1
                    source_doc = metadata.get('source_document', 'unknown')
                    
            except Exception as e:
                error_count += 1
                self.logger.warning(f"处理缓存文件失败: {cache_file}, {e}")
        
        if error_count > 0:
            self.logger.warning(f"⚠️ 清理缓存时遇到 {error_count} 个错误")
        
        return deleted_count
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息
        
        Returns:
            Dict: 缓存统计信息
        """
        if not self.enable_cache:
            return {
                'enabled': False,
                'cache_dir': self.cache_dir,
                'total_files': 0,
                'total_size': 0
            }
        
        cache_path = Path(self.cache_dir)
        if not cache_path.exists():
            return {
                'enabled': True,
                'cache_dir': self.cache_dir,
                'total_files': 0,
                'total_size': 0
            }
        
        cache_files = list(cache_path.glob("*.json"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            'enabled': True,
            'cache_dir': str(cache_path.absolute()),
            'total_files': len(cache_files),
            'total_size': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }


def example_locate_segments():
    """文档定位器使用示例
    
    演示：
    1. 使用 should_locate 判断是否需要定位
    2. 如果需要，调用 locate_relevant_segments
    3. 如果不需要，直接使用原文档
    """
    
    locator = DocumentLocator(
        word_threshold=500  # 演示用的小阈值
    )
    
    long_document = """
公司2023年度员工信息汇总报告

一、研发部门
1. 张三，28岁，高级软件工程师，月薪15000元，2020年3月1日入职。
   擅长Python和Java开发，参与过多个核心项目。
   
2. 李四，32岁，技术总监，月薪25000元，2018年8月15日入职。
   负责技术团队管理，有10年以上开发经验。

二、产品部门  
1. 王五，29岁，产品经理，月薪18000元，2021年6月1日入职。
   负责产品规划和需求分析。
   
2. 赵六，35岁，产品总监，月薪28000元，2017年4月20日入职。
   负责产品战略规划。

三、市场部门
1. 孙七，26岁，市场专员，月薪12000元，2022年9月1日入职。
   负责市场推广活动。
   
四、其他信息
公司总部位于北京，成立于2015年，目前有员工200余人...
    """ * 50  # 重复50次模拟长文档
    
    schema = {
        'tables': [{
            'name': 'employees',
            'attributes': [
                {'name': 'name', 'type': 'TEXT', 'description': '员工姓名'},
                {'name': 'age', 'type': 'INTEGER', 'description': '年龄'},
                {'name': 'position', 'type': 'TEXT', 'description': '职位'},
                {'name': 'salary', 'type': 'DECIMAL', 'description': '月薪'},
                {'name': 'hire_date', 'type': 'DATE', 'description': '入职日期'}
            ]
        }]
    }
    
    if locator.should_locate(long_document):
        print(f"文档长度: {len(long_document)} 字符，需要定位")
        
        segments = locator.locate_relevant_segments(
            text_content=long_document,
            schema=schema,
            table_name='employees',
            nl_prompt="提取所有员工的基本信息"
        )
        
        summary = locator.get_segments_summary(segments)
        print(f"定位到 {summary['total_segments']} 个片段")
        
        merged_text = locator.merge_segments(segments)
        
        return segments, merged_text
    else:
        print(f"文档长度: {len(long_document)} 字符，不需要定位，直接使用")
        return [], long_document


if __name__ == "__main__":
    example_locate_segments()

