"""
Qwen 系列模型的调用接口
支持 Qwen-Long、Qwen2.5 等系列模型
"""

import requests
import time
import json
import logging
from typing import Optional, List, Dict
import llm.global_config as global_config
from llm.utils import load_env_config, build_messages

logger = logging.getLogger('llm.qwen')

# 加载配置
config = load_env_config()


def get_answer(
    text: str,
    image: Optional[str] = None,
    system_prompt: Optional[str] = None,
    history: Optional[List[Dict[str, str]]] = None,
    model: Optional[str] = None
) -> str:
    """
    调用 Qwen 系列模型获取回答
    
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
        'qwen2.5-7b-instruct',
        'qwen2.5-14b-instruct',
        'qwen2.5-32b-instruct',
        'qwen2.5-72b-instruct',
        'qwen2.5-14b-instruct-1m',
        'qwen2-72b-instruct',
        'qwen-long',
        'qwen-turbo'
    ]
    
    if model not in model_list:
        logger.warning(f"模型 {model} 不在支持列表中，使用默认模型 qwen-long")
        model = 'qwen-long'
    
    logger.debug(f"使用 Qwen 模型: {model}")
    
    # 构建消息列表
    messages = build_messages(text, image, system_prompt, history)
    
    # 获取 API 配置
    api_url = config.get("QWEN_URL")
    api_key = config.get("QWEN_KEY")
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    data = {
        'model': model,
        'messages': messages,
    }
    
    # 重试逻辑
    for i in range(3):
        try:
            logger.debug("=" * 50)
            logger.debug("Qwen 输入:")
            logger.debug(json.dumps(data, ensure_ascii=False, indent=2))
            logger.debug("=" * 50)
            
            response = requests.post(api_url, headers=headers, json=data, timeout=120)
            response_json = response.json()
            output = response_json['choices'][0]['message']['content']
            
            logger.debug("=" * 50)
            logger.debug(f"Qwen 输出: {output[:500]}{'...' if len(output) > 500 else ''}")
            logger.debug("=" * 50)
            
            logger.info(f"Qwen 调用成功 (模型={model}, 输出长度={len(output)})")
            return output
            
        except Exception as e:
            logger.warning(f"Qwen 调用失败 (尝试 {i+1}/3): {str(e)}")
            if i < 2:  # 不在最后一次尝试时等待
                time.sleep(5)
    
    logger.error("Qwen 调用失败，已重试3次")
    return output


if __name__ == '__main__':
    # 测试代码
    test_input = """
What is the FY2022 unadjusted EBITDA less capex for PepsiCo? 
Define unadjusted EBITDA as unadjusted operating income + depreciation and amortization.
Answer in USD millions.
"""
    
    answer = get_answer(test_input, model="qwen-long")
    print(answer)
