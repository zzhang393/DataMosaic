"""
Deepseek 系列模型的调用接口
支持 Deepseek-Chat (V3) 和 Deepseek-Reasoner (R1) 模型
"""

import time
import logging
from typing import Optional, List, Dict
from openai import OpenAI
import llm.global_config as global_config
from llm.utils import load_env_config

logger = logging.getLogger('llm.deepseek')

# 加载配置
config = load_env_config()


def get_answer(
    content: str,
    system_prompt: Optional[str] = None,
    history: Optional[List[Dict[str, str]]] = None,
    model: Optional[str] = None
) -> str:
    """
    调用 Deepseek 系列模型获取回答
    
    Args:
        content: 用户输入的内容
        system_prompt: 系统提示词（可选）
        history: 历史对话记录（可选）
        model: 模型名称（可选），如果为 None 则使用全局配置
    
    Returns:
        str: 模型返回的回答文本
    """
    output = ''
    
    # 获取模型名称
    if model is None:
        model = global_config.get_model()
    
    # 模型名称映射
    if model.lower() == 'deepseek-v3':
        model = 'deepseek-chat'
    elif 'deepseek' in model.lower():
        if 'r1' in model.lower():
            model = 'deepseek-reasoner'
        else:
            model = 'deepseek-chat'
    else:
        model = 'deepseek-chat'
    
    logger.debug(f"使用 Deepseek 模型: {model}")
    
    # 构建消息列表
    if history is None:
        if system_prompt is None:
            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": content}
            ]
        else:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content},
            ]
    else:
        if system_prompt is None:
            messages = history + [{"role": "user", "content": content}]
        else:
            messages = [
                {"role": "system", "content": system_prompt}
            ] + history + [
                {"role": "user", "content": content}
            ]
    
    # 获取 API 配置
    api_key = config.get("DEEPSEEK_KEY")
    base_url = config.get("DEEPSEEK_URL")
    
    # 创建客户端
    client = OpenAI(api_key=api_key, base_url=base_url)
    
    # 重试逻辑
    for i in range(3):
        try:
            logger.debug("=" * 50)
            logger.debug("Deepseek 输入:")
            logger.debug(f"模型: {model}")
            logger.debug(f"消息: {messages}")
            logger.debug("=" * 50)
            
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                stream=False
            )
            
            output = response.choices[0].message.content
            
            logger.debug("=" * 50)
            logger.debug(f"Deepseek 输出: {output[:500]}{'...' if len(output) > 500 else ''}")
            logger.debug("=" * 50)
            
            logger.info(f"Deepseek 调用成功 (模型={model}, 输出长度={len(output)})")
            return output
            
        except Exception as e:
            logger.warning(f"Deepseek 调用失败 (尝试 {i+1}/3): {str(e)}")
            if i < 2:  # 不在最后一次尝试时等待
                time.sleep(5)
    
    logger.error("Deepseek 调用失败，已重试3次")
    return output


if __name__ == '__main__':
    # 测试代码
    test_input = """
What is the FY2022 unadjusted EBITDA less capex for PepsiCo? 
Define unadjusted EBITDA as unadjusted operating income + depreciation and amortization.
Answer in USD millions.
"""
    
    ans = get_answer(test_input)
    print(ans)
