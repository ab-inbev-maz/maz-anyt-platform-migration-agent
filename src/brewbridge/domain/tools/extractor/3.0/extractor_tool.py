import os
from typing import Dict, Any
from brewbridge.core.base_nodes import ToolNode
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure import GitHubClient
from brewbridge.domain.extractor_strategies.brewdat.brewdat_3_0_strategy import Brewdat3Strategy
from brewbridge.infrastructure import get_logger
from brewbridge.utils.exceptions import InvalidInputError, ExtractionError
from brewbridge.infrastructure.observability.mlflow_tracer import track_node 

logger = get_logger(__name__)

class ExtractorTool(ToolNode):
    """
    Nodo-Herramienta encargado de orquestar la extracción de artefactos.
    Actúa como un 'Factory' que selecciona la estrategia adecuada (3.0, COBOS, etc.)
    basándose en la configuración del pipeline.
    """

    def execute(self, state: MigrationGraphState) -> Dict[str, Any]:
        """
        Ejecuta la lógica de extracción seleccionando la estrategia correcta.
        """
        pipeline_data = state.current_pipeline_data
        if not pipeline_data:
            raise InvalidInputError("No hay 'current_pipeline_data' en el estado para iniciar la extracción.")

        # 1. Configurar Cliente de Infraestructura
        # Recuperamos el token del entorno (o de state.credentials si así lo decidieron)
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            raise ExtractionError("GITHUB_TOKEN no encontrado en variables de entorno.")
        
        client = GitHubClient(token=token)

        # 2. Selección de Estrategia (Factory Pattern)
        # Aquí decidimos qué estrategia usar según el origen definido en el manifiesto
        # Por ahora, forzamos o detectamos Platform 3.0
        source_type = pipeline_data.get("source_type", "platform_3_0") # Default a 3.0
        
        logger.info(f"🔧 ExtractorTool activado. Estrategia seleccionada: {source_type}")

        if source_type == "platform_3_0":
            strategy = Brewdat3Strategy(github_client=client)
        
        # elif source_type == "cobos":
        #     strategy = CobosStrategy(...)
        
        else:
            raise ExtractionError(f"Tipo de fuente no soportado: {source_type}")

        # 3. Preparar Input para la Estrategia
        # El manifiesto debe proveer 'repo_name' y 'trigger_name'
        pipeline_info = {
            "repo_name": pipeline_data.get("repo_name"),
            "trigger_name": pipeline_data.get("trigger_name")
        }

        # 4. Ejecutar Estrategia (Delegación)
        # Esto devuelve el diccionario con 'raw_artifacts'
        result = strategy.extract(pipeline_info)

        # 5. Retornar actualización del Estado
        # Solo devolvemos lo que queremos actualizar en el State global
        return {
            "raw_artifacts": result["raw_artifacts"]
        }

@track_node("tool") # Descomentar si usas observabilidad
def extractor_node(state: MigrationGraphState) -> MigrationGraphState:
    """
    Función envoltorio para integrar la clase ExtractorTool en el grafo.
    """
    tool = ExtractorTool(node_name="Extractor_3_0")
    
    # El método .run() viene de la clase padre BaseNode/ToolNode
    # y maneja el try/catch y logging estándar.
    updated_state_dict = tool.run(state)
    
    # En LangGraph, si devuelves un diccionario, se hace un merge con el estado actual.
    # Si necesitas devolver el objeto State completo, harías un merge manual aquí.
    # Asumimos que LangGraph hace el merge automático del dict devuelto.
    return updated_state_dict