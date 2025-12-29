"""
GPT 系列模型的调用接口
支持 GPT-4o、Claude、Gemini 等通过 OpenAI 兼容接口的模型
"""

import requests
import time
import json
import logging
import os
from datetime import datetime
from typing import Optional, List, Dict, Any
import llm.global_config as global_config
from llm.utils import load_env_config, build_messages

logger = logging.getLogger('llm.gpt')

# 加载配置
config = load_env_config()

# 日志目录配置
current_dir = os.path.dirname(os.path.abspath(__file__))
LLM_LOG_DIR = os.path.join(current_dir, "..", "llm_logs")
os.makedirs(LLM_LOG_DIR, exist_ok=True)


def save_llm_call_log(
    model: str,
    input_data: Dict[str, Any],
    output_data: Any,
    error: Optional[str] = None
) -> Optional[str]:
    """
    保存 LLM 调用的输入和输出到文件
    
    Args:
        model: 使用的模型名称
        input_data: LLM 输入数据（字典）
        output_data: LLM 输出数据（字符串或字典）
        error: 如果有错误，错误信息
    
    Returns:
        str: 日志文件路径，如果保存失败则返回 None
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        log_filename = f"llm_call_{timestamp}.json"
        log_filepath = os.path.join(LLM_LOG_DIR, log_filename)
        
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "model": model,
            "input": input_data,
            "output": output_data,
            "error": error,
            "success": error is None
        }
        
        with open(log_filepath, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"LLM调用日志已保存: {log_filepath}")
        return log_filepath
    except Exception as e:
        logger.error(f"保存LLM调用日志失败: {str(e)}")
        return None


def get_answer(
    text: str,
    image: Optional[str] = None,
    system_prompt: Optional[str] = None,
    history: Optional[List[Dict[str, str]]] = None,
    model: Optional[str] = None
) -> str:
    """
    调用 GPT 系列模型获取回答
    
    Args:
        text: 用户输入的文本
        image: 图片路径（可选）
        system_prompt: 系统提示词（可选）
        history: 历史对话记录（可选）
        model: 模型名称（可选），如果为 None 则使用全局配置
    
    Returns:
        str: 模型返回的回答文本
    """
    output = ' '
    
    # 获取模型名称
    if model is None:
        model = global_config.get_model()
    
    # 支持的模型列表
    model_list = [
        'gpt-4o', 'gpt-4o-mini', 'o1',
        'claude-3-5-sonnet-20240620', 'claude-sonnet-4-20250514',
        'deepseek-r1', 'deepseek-v3',
        'gpt-5', 'gemini-2.5-pro'
    ]
    
    if model not in model_list:
        logger.warning(f"模型 {model} 不在支持列表中，使用默认模型 gpt-4o")
        model = 'gpt-4o'
    
    # 构建消息列表
    messages = build_messages(text, image, system_prompt, history)
    
    # 获取 API 配置
    api_url = config.get("API_URL")
    api_key = config.get("API_KEY")
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    data = {
        'model': model,
        'messages': messages,
    }
    
    # 重试逻辑
    last_error = None
    for i in range(3):
        try:
            logger.debug("=" * 50)
            logger.debug("GPT 输入:")
            logger.debug(json.dumps(data, ensure_ascii=False, indent=2))
            logger.debug("=" * 50)
            
            response = requests.post(api_url, headers=headers, json=data, timeout=120)
            
            logger.debug("=" * 50)
            logger.debug("GPT 输出:")
            logger.debug(response.text)
            logger.debug("=" * 50)
            
            response_json = response.json()
            output = response_json['choices'][0]['message']['content']
            
            logger.info(f"GPT 调用成功 (模型={model}, 输出长度={len(output)})")
            return output
            
        except Exception as e:
            last_error = str(e)
            logger.warning(f"GPT 调用失败 (尝试 {i+1}/3): {last_error}")
            if i < 2:  # 不在最后一次尝试时等待
                time.sleep(5)
    
    logger.error(f"GPT 调用失败，已重试3次: {last_error}")
    return output


if __name__ == '__main__':
    # 测试代码
    test_input = """
What is the FY2022 unadjusted EBITDA less capex for PepsiCo? 
Define unadjusted EBITDA as unadjusted operating income + depreciation and amortization.
Answer in USD millions.
"""
    
    answer = get_answer(test_input, model="gpt-4o-mini")
    print(answer)
