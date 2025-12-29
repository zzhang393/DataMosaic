"""
Task utilities for Doc2DB system - extracted from reference_code
"""

import re
import json
import logging
from typing import Dict, List, Optional

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from llm.main import get_answer

logger = logging.getLogger(__name__)

def detect_language(text: str) -> str:
    """Detect if the text is primarily in Chinese or English.
    
    Args:
        text: Input text to analyze
        
    Returns:
        'chinese' if Chinese characters are detected, 'english' otherwise
    """
    chinese_chars = sum(1 for char in text if '\u4e00' <= char <= '\u9fff')
    if len(text) > 0 and chinese_chars / len(text) > 0.1:
        return 'chinese'
    return 'english'

def get_language_instruction(query: str) -> str:
    """Get language-specific instruction for prompts based on query language.
    
    Args:
        query: The user query
        
    Returns:
        Language instruction string to be added to prompts
    """
    lang = detect_language(query)
    if lang == 'chinese':
        return "\n\n**重要：由于用户查询是中文，请确保你的回答也使用中文。**"
    else:
        return "\n\n**Important: Please ensure your response is in English.**"

def get_llm_response(prompt: str, model: str = None, history: List = None, system_prompt: str = None) -> str:
    """Get response from LLM using the Doc2DB LLM system.
    
    Args:
        prompt: The user prompt
        model: Model to use (optional)
        history: Chat history (optional)
        system_prompt: System prompt (optional, not used in current implementation)
        
    Returns:
        LLM response string
    """
    if model is None:
        try:
            from llm import global_config
            model = global_config.get_model()
        except ImportError:
            model = "gpt-4o"  # fallback
    
    if system_prompt:
        combined_prompt = f"{system_prompt}\n\n{prompt}"
    else:
        combined_prompt = prompt
        
    response = get_answer(combined_prompt, history=history, model=model)
    return response

def extract_json_from_text(text: str) -> Dict:
    """Extract JSON object from text string.
    
    Handles cases where the JSON might be embedded within markdown or other text.
    """
    json_pattern = r'({[\s\S]*})'
    match = re.search(json_pattern, text)
    
    if match:
        json_str = match.group(1)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            json_str = re.sub(r'```json|```', '', json_str).strip()
            try:
                return json.loads(json_str)
            except:
                pass
    
    try:
        text = text.replace("'", '"')
        dict_pattern = r'({[^{}]*})'
        matches = re.findall(dict_pattern, text)
        for potential_json in matches:
            try:
                return json.loads(potential_json)
            except:
                continue
    except:
        pass
    
    return {}
