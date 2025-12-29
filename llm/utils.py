"""
LLM模块的公共工具函数
"""

import re
import base64
import os
from pathlib import Path
from dotenv import dotenv_values
from typing import Optional


def load_env_config(env_filename: str = ".env") -> dict:
    """
    加载环境配置文件
    
    Args:
        env_filename: 环境配置文件名，默认为 ".env"
    
    Returns:
        dict: 配置字典
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(current_dir, env_filename)
    return dotenv_values(env_path)


def count_len(text: str) -> int:
    """
    计算文本长度（中文字符数 + 英文单词数）
    
    Args:
        text: 要计算长度的文本
    
    Returns:
        int: 文本长度
    """
    chinese_count = len(re.findall(r'[\u4e00-\u9fa5]', text))
    word_count = len(re.findall(r'[a-zA-Z]+', text))
    total_count = chinese_count + word_count
    return total_count


def encode_image(image_path: str) -> str:
    """
    将图片文件编码为 base64 字符串
    
    Args:
        image_path: 图片文件路径
    
    Returns:
        str: base64 编码的图片字符串
    """
    with open(image_path, "rb") as image_file:
        image_bytes = image_file.read()
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    return base64_image


def build_messages(
    content: str,
    image: Optional[str] = None,
    system_prompt: Optional[str] = None,
    history: Optional[list] = None
) -> list:
    """
    构建消息列表
    
    Args:
        content: 用户输入的内容
        image: 图片路径（可选）
        system_prompt: 系统提示词（可选）
        history: 历史对话记录（可选）
    
    Returns:
        list: 消息列表
    """
    # 默认系统提示词
    default_system = {"role": "system", "content": "You are a helpful assistant."}
    
    # 构建用户消息
    if image is None:
        user_message = {"role": "user", "content": content}
    else:
        base64_image = encode_image(image)
        user_message = {
            "role": "user",
            "content": [
                {"type": "text", "text": content},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
            ]
        }
    
    # 组装完整消息列表
    if history is None:
        if system_prompt is None:
            messages = [default_system, user_message]
        else:
            messages = [{"role": "system", "content": system_prompt}, user_message]
    else:
        # 如果有历史记录，添加系统提示（如果提供）
        if system_prompt is not None:
            messages = [{"role": "system", "content": system_prompt}] + history + [user_message]
        else:
            messages = history + [user_message]
    
    return messages

