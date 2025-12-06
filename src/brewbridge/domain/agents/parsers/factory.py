from typing import Dict, Type
from .base_parser import BaseParser

class ParserFactory:
    _registry: Dict[str, Type[BaseParser]] = {}

    @classmethod
    def register(cls, name: str, parser_cls: Type[BaseParser]):
        cls._registry[name] = parser_cls

    @classmethod
    def create(cls, name: str) -> BaseParser:
        if name not in cls._registry:
            raise KeyError(f"Parser '{name}' not registered")
        return cls._registry[name]()