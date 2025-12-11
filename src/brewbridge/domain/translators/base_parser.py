from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from brewbridge.domain.translators.strategies.transformations_parser.yaml_handler import YamlHandler
from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import ParserError

logger = get_logger(__name__)

class BaseParser(ABC):
    """
    Abstract base class for all Parsers (Translators).
    Implements the 'Template Method' pattern in the 'parse' method.
    
    Responsibility:
    1. Load the context (raw artifacts).
    2. Load the empty template.
    3. Delegate the translation logic to the subclass (_generate_updates).
    4. Apply the changes and save the result.
    """

    def __init__(self, raw_artifacts: Dict[str, Any]):
        self.raw_artifacts = raw_artifacts
        # Unpack the artifacts for quick access in subclasses
        self.items = raw_artifacts.get("items", [])
        self.global_params = raw_artifacts.get("global_parameters", {})
        self.notebooks = raw_artifacts.get("notebooks_source", {})
        self.pipeline_json = raw_artifacts.get("pipeline_json", {})

    def parse(self, target_table: str, template_path: str, output_path: str) -> None:
        """
        TEMPLATE METHOD.
        Defines the skeleton of the translation algorithm. Subclasses should not override this.
        """
        logger.info(f" Iniciando parsing para tabla: {target_table}")

        # 1. Encontrar la entidad específica en la 'Maleta'
        item = self._find_item(target_table)
        if not item:
            logger.error(f"Tabla {target_table} no encontrada en los artefactos extraídos.")
            raise ParserError(f"No se encontraron datos para la tabla {target_table}")

        # 2. Cargar el Template Vacío (Generado previamente por EngineeringStore)
        template_content = YamlHandler.load_yaml(template_path)
        logger.debug(f"Template cargado desde: {template_path}")

        # 3. Generar Actualizaciones (Lógica Polimórfica - Aquí ocurre la magia)
        # La subclase decide QUÉ cambiar.
        updates = self._generate_updates(item, template_content)

        # 4. Aplicar Cambios al Template
        final_content = self._apply_updates(template_content, updates)

        # 5. Escribir el Archivo Final
        YamlHandler.write_yaml(output_path, final_content)
        logger.info(f" Archivo generado exitosamente: {output_path}")

    @abstractmethod
    def _generate_updates(self, item: Dict[str, Any], template: Dict[str, Any]) -> Dict[str, Any]:
        """
        Abstract Method. Must be implemented by each concrete Parser (Transformations, ACL, etc.).
        
        Input:
            - item: The MigrationItem object (with configs and paths).
            - template: The current content of the empty YAML.
            
        Output:
            - A dictionary of updates using Dot Notation for nesting.
              Ex: {"transformations.0.name": "data_cleaning", "checkpoint": True}
        """
        pass

    def _find_item(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Busca el diccionario del item en la lista de items procesados."""
        for i in self.items:
            # Comparamos ignorando case por seguridad
            if i.get("table_name", "").lower() == table_name.lower():
                return i
        return None

    def _apply_updates(self, content: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Applies updates to the content dictionary.
        Supports nested keys with dots (e.g., 'parent.child.grandchild').
        Supports list indices (e.g., 'list.0.field').
        """
        for key_path, value in updates.items():
            self._set_nested_value(content, key_path, value)
        return content

    def _set_nested_value(self, data: Any, path: str, value: Any) -> None:
        """Helper to navigate and set deep values."""
        keys = path.split('.')
        current = data
        
        for i, key in enumerate(keys[:-1]):
            # Manejo de listas (si la key es un número)
            if isinstance(current, list) and key.isdigit():
                idx = int(key)
                current = current[idx]
            # Manejo de diccionarios
            elif isinstance(current, dict):
                current = current.setdefault(key, {})
            else:
                raise ParserError(f"No se puede navegar la ruta '{path}' en el segmento '{key}'")

        # Asignar el valor final
        last_key = keys[-1]
        if isinstance(current, list) and last_key.isdigit():
            current[int(last_key)] = value
        elif isinstance(current, dict):
            current[last_key] = value
        else:
             raise ParserError(f"No se puede asignar valor en '{path}'")