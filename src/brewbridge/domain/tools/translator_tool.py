from typing import Dict, Any
from pathlib import Path

from brewbridge.core.base_nodes import ToolNode, tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.translators.factory import TranslatorFactory
from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import InvalidInputError, ParserError

from brewbridge.infrastructure.observability.mlflow_tracer import track_node

logger = get_logger(__name__)

class TranslatorTool(ToolNode):
    """
    Translation Orchestrator.
    1. Identifies which files need to be generated.
    2. Calls the Factory to obtain the appropriate Parser.
    3. Executes the parsing.
    """
    
    def execute(self, state: MigrationGraphState) -> Dict[str, Any]:
        raw_artifacts = state.raw_artifacts
        if not raw_artifacts:
            raise InvalidInputError("No hay raw_artifacts en el estado. El Extractor falló o no corrió.")

        env = state.environment_type 
        framework = "hopsflow" if env in ["brz", "slv"] else "brewtiful"
        
        logger.info(f"🔄 Iniciando Traducción para framework: {framework}")

        # items a procesar
        items = raw_artifacts.get("items", [])
        
        # Diccionario para guardar rutas de salida y actualizar el state
        generated_files = {
            "transformations_yaml": {} # {table_name: path}
        }

        base_cache_dir = Path(state.template_path) if state.template_path else Path(f"cache/{framework}")

        for item in items:
            table_name = item["table_name"]
            
            if item.get("silver_config") or item.get("notebook_path_slv"):
                try:
                    parser = TranslatorFactory.get_parser(81122118, "transformations", raw_artifacts)
                    
                    # Definir Rutas
                    # El template vacío debería estar ahí gracias al TemplateCreator
                    template_file = base_cache_dir / table_name / f"{table_name}_transformations.yaml"
                    
                    output_file = template_file 

                    parser.parse(
                        target_table=table_name,
                        template_path=str(template_file),
                        output_path=str(output_file)
                    )
                    
                    generated_files["transformations_yaml"][table_name] = str(output_file)
                    
                except Exception as e:
                    logger.error(f"❌ Falló traducción de transformaciones para {table_name}: {e}")
                    # siguiente tabla
                    continue

        return {
            # Aquí podríamos guardar las rutas generadas en el state si fuera necesario
            # "generated_artifacts": generated_files 
            # Por ahora, el efecto es el archivo en disco (Side Effect)
        }

@track_node("tool")
def translator_node(state: MigrationGraphState) -> MigrationGraphState:
    tool = TranslatorTool(node_name="Translator_Hopsflow")
    return tool.run(state)