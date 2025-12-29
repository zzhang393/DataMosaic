"""提取器模块"""
from .base import BaseExtractor
from .document_locator import DocumentLocator, LocatedSegment

__all__ = [
    'BaseExtractor',
    'DocumentLocator',
    'LocatedSegment'
]

