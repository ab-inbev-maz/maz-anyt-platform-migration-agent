import yaml
from typing import Dict, Any
from pathlib import Path
from brewbridge.utils.exceptions import ParserError

class YamlHandler:
    """
    Handler for safe I/O operations on YAML files.
    """

    @staticmethod
    def load_yaml(path: str) -> Dict[str, Any]:
        """Loadind YAML file into a dictionary."""
        try:
            file_path = Path(path)
            if not file_path.exists():
                raise ParserError(f"Archivo YAML no encontrado: {path}")
                
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ParserError(f"Error de sintaxis YAML en {path}: {e}")

    @staticmethod
    def write_yaml(path: str, content: Dict[str, Any]) -> None:
        """Write a dictionary to a YAML file preserving basic formatting."""
        try:
            file_path = Path(path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # sort_keys para mantener el orden lógico de Hopsflow
                yaml.dump(content, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
        except Exception as e:
            raise ParserError(f"Error escribiendo YAML en {path}: {e}")