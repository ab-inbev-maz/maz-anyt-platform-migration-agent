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
    Tool node responsible for orchestrating the extraction of artifacts.
    Acts as a 'Factory' that selects the appropriate strategy (3.0, COBOS, etc.)
    based on the pipeline configuration.
    """

    def execute(self, state: MigrationGraphState) -> Dict[str, Any]:
        """
        Ejecuta la lógica de extracción seleccionando la estrategia correcta.
        """
        pipeline_data = state.current_pipeline_data
        if not pipeline_data:
            raise InvalidInputError("No hay 'current_pipeline_data' en el estado para iniciar la extracción.")

        token = os.getenv("GITHUB_TOKEN")
        if not token:
            raise ExtractionError("GITHUB_TOKEN no encontrado en variables de entorno.")
        
        client = GitHubClient(token=token)

        source_type = pipeline_data.get("source_type", "platform_3_0") 
        
        logger.info(f"🔧 ExtractorTool activado. Estrategia seleccionada: {source_type}")

        if source_type == "platform_3_0":
            strategy = Brewdat3Strategy(github_client=client)
        
        # elif source_type == "cobos":
        #     strategy = CobosStrategy(...)
        
        else:
            raise ExtractionError(f"Tipo de fuente no soportado: {source_type}")

        pipeline_info = {
            "repo_name": pipeline_data.get("repo_name"),
            "trigger_name": pipeline_data.get("trigger_name")
        }

        result = strategy.extract(pipeline_info)

        return {
            "raw_artifacts": result["raw_artifacts"]
        }

@track_node("tool") 
def extractor_node(state: MigrationGraphState) -> MigrationGraphState:
    """
    Wrapper function to integrate the ExtractorTool class into the graph.
    """
    tool = ExtractorTool(node_name="Extractor_3_0")
    
    updated_state_dict = tool.run(state)

    return updated_state_dict