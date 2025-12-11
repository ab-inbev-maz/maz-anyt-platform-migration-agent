import re
import json
import ast
from typing import List, Any, Optional
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)

class CodeAnalyzer:
    """
    Static analyzer for Python code (Databricks Notebooks).
    Extracts configurations defined in widgets using prioritized regex strategies.
    """

    def __init__(self, code_content: str):
        self.code = code_content or ""

    def get_widget_value(self, param_name: str) -> Optional[Any]:
        """
        Searches for the default value of a Databricks widget by its name.
        Utilizes multiple regex patterns to handle triple quotes and lists with internal commas.
        """
        
        patterns = [
            # Comillas Triples Dobles (Usado en watermark_column)
            # Captura lo que está dentro de """ ... """
            rf'dbutils\.widgets\.(?:text|dropdown)\(\s*["\']{param_name}["\']\s*,\s*"""(.*?)"""',
            
            # Comillas Triples Simples
            # Captura lo que está dentro de ''' ... '''
            rf"dbutils\.widgets\.(?:text|dropdown)\(\s*['\"]{param_name}['\"]\s*,\s*'''(.*?)'''",
            
            # Comillas Simples (Maneja listas como '["A", "B"]')
            rf"dbutils\.widgets\.(?:text|dropdown)\(\s*['\"]{param_name}['\"]\s*,\s*'(.*?)'\s*,",
            
            # Comillas Dobles
            rf'dbutils\.widgets\.(?:text|dropdown)\(\s*["\']{param_name}["\']\s*,\s*"(.*?)"\s*,'
        ]

        for pattern in patterns:
            match = re.search(pattern, self.code, re.DOTALL)
            if match:
                raw_value = match.group(1)
                return self._robust_parse(raw_value)
        
        return None

    def _robust_parse(self, value_str: str) -> Any:
        """
        Trasnform the string into Python objects.
        """
        if not value_str:
            return None

        clean_str = value_str.strip()

        # Intento JSON
        try:
            return json.loads(clean_str)
        except json.JSONDecodeError:
            pass

        # Intento AST -> "['A', 'B']" fallaría en JSON pero pasa en AST.
        try:
            return ast.literal_eval(clean_str)
        except (ValueError, SyntaxError):
            pass

        return clean_str

    # Métodos Específicos para Hopsflow 
    def extract_dedup_keys(self) -> List[str]:
        """Extract columns used for deduplication (key_columns)."""
        val = self.get_widget_value("key_columns")
        
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                # Reemplazar comillas simples por dobles -> JSON
                if val.startswith("[") and val.endswith("]"):
                     return json.loads(val.replace("'", '"'))
            except:
                pass
        return []

    def extract_watermark(self) -> Optional[List[str]]:
        """Extract columns used for ordering (watermark_column)."""
        val = self.get_widget_value("watermark_column")
        
        if isinstance(val, list):
            return val
        if isinstance(val, str) and val:
            # Intento parsear como lista JSON
            if val.startswith("["):
                 return []
            return [val]
        return None
    
    def detect_upsert(self) -> bool:
        """Detects if the load strategy is UPSERT."""
        val = self.get_widget_value("load_type")
        return val and "UPSERT" in str(val).upper()

    def detect_input_filename_usage(self) -> bool:
        """Detects if input_file_name() is used in the code."""
        return "input_file_name" in self.code

    def detect_checkpoint_reset(self) -> bool:
        """ Detects if checkpoint reset is requested."""
        val = self.get_widget_value("reset_stream_checkpoint")
        return str(val).lower() == "true"
