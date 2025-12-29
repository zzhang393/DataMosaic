"""
LLM 模块的统一入口
根据配置自动选择合适的模型进行调用
"""

import logging
from typing import Optional, List, Dict
from pathlib import Path
import llm.gpt as gpt
import llm.qwen as qwen
import llm.deepseek as deepseek
import llm.global_config as config

logger = logging.getLogger('llm.main')


def get_answer(
    question: str,
    image: Optional[str] = None,
    history: Optional[List[Dict[str, str]]] = None,
    system_prompt: Optional[str] = None,
    model: Optional[str] = None
) -> Optional[str]:
    """
    统一的 LLM 调用接口，根据模型名称自动路由到相应的实现
    
    Args:
        question: 用户输入的问题或内容
        image: 图片路径或 URL（可选）
        history: 历史对话记录（可选）
        system_prompt: 系统提示词（可选）
        model: 模型名称（可选），如果为 None 则使用全局配置
    
    Returns:
        str: 模型返回的回答文本，如果失败则返回 None
    """
    # 获取模型名称
    if model is None:
        model = config.get_model().lower()
    else:
        model = model.lower()
    
    logger.debug(f"调用 LLM: 模型={model}, 输入长度={len(question)}")
    
    # 根据模型名称路由到相应的实现
    try:
        if 'qwen' in model:
            output = qwen.get_answer(
                text=question,
                image=image,
                system_prompt=system_prompt,
                history=history,
                model=model
            )
        elif 'deepseek' in model:
            output = deepseek.get_answer(
                content=question,
                system_prompt=system_prompt,
                history=history,
                model=model
            )
        else:
            # 默认使用 GPT 接口（兼容多种模型）
            output = gpt.get_answer(
                text=question,
                image=image,
                system_prompt=system_prompt,
                history=history,
                model=model
            )
        
        if output:
            logger.info(f"LLM 调用成功 (模型={model}, 输出长度={len(str(output))})")
        else:
            logger.warning(f"LLM 调用失败 (模型={model})")
        
        return output
        
    except Exception as e:
        logger.error(f"LLM 调用异常 (模型={model}): {str(e)}")
        return None


if __name__ == '__main__':
    # 测试代码
    test_question = "What is the capital of France?"
    answer = get_answer(test_question)
    print(f"回答: {answer}")
