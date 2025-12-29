"""
LLM 模块的全局配置管理
用于管理默认使用的模型名称
"""

import json
import os
from pathlib import Path
from typing import Optional

# 全局模型名称
_model = 'gpt'


def _load_config_from_file() -> None:
    """
    从已知位置的 config.json 文件加载配置
    按优先级顺序查找配置文件，使用第一个找到的有效配置
    """
    global _model
    
    config_locations = [
        Path(__file__).parent.parent / "document_chat_app" / "backend" / "config.json",
        Path(__file__).parent.parent / "refer_code" / "backend" / "config.json",
        Path(__file__).parent / "config.json",
        Path(__file__).parent.parent / "config.json"
    ]
    
    for config_path in config_locations:
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    if 'model' in config:
                        _model = config['model']
                        print(f"已从 {config_path} 加载模型配置: {_model}")
                        return
            except Exception as e:
                print(f"从 {config_path} 加载配置出错: {e}")
                continue


def set_model(new_model: str) -> None:
    """
    设置全局模型名称
    
    Args:
        new_model: 新的模型名称
    """
    global _model
    _model = new_model


def get_model() -> str:
    """
    获取当前全局模型名称
    
    Returns:
        str: 当前的全局模型名称
    """
    return _model


def reload_config() -> None:
    """
    重新从配置文件加载模型配置
    """
    _load_config_from_file()


# 模块导入时自动加载配置
_load_config_from_file()
