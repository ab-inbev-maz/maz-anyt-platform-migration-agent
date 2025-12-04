"""
Extractor Tool (Plataforma 3.0)

Función única que ejecuta la estrategia Brewdat 3.0 y deja los artefactos
en el estado. Sigue el patrón de otros tools decorados con @track_node y
@tool_node.
"""

import os
from typing import Any, Dict

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.extractor_strategies.brewdat.brewdat_3_0_strategy import Brewdat3Strategy
from brewbridge.infrastructure import GitHubClient, get_logger
from brewbridge.infrastructure.observability.mlflow_tracer import track_node
from brewbridge.utils.exceptions import ExtractionError, InvalidInputError

logger = get_logger(__name__)


@track_node("tool")
@tool_node
def extractor_node(state: MigrationGraphState) -> MigrationGraphState:
    """
    Ejecuta la estrategia de extracción para la plataforma 3.0 y
    actualiza el estado con los artefactos crudos obtenidos.
    """
    pipeline_data: Dict[str, Any] = state.pipeline_info or {}
    repo_name = pipeline_data.get("repo_name")
    trigger_name = pipeline_data.get("trigger_name")
    source_platform = pipeline_data.get("source_platform", "platform_3_0")

    if not repo_name or not trigger_name:
        raise InvalidInputError(
            "No hay 'repo_name' o 'trigger_name' en pipeline_info para iniciar la extracción."
        )

    logger.info(f"🔧 ExtractorTool activado. Estrategia seleccionada: {source_platform}")

    creds = state.credentials or {}
    github_token = creds.get("GITHUB_TOKEN") or os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ExtractionError("GITHUB_TOKEN no encontrado en credenciales ni en el entorno.")

    client = GitHubClient(token=github_token)
    if source_platform == "platform_3_0":
        strategy = Brewdat3Strategy(github_client=client)

    # elif source_platform == "cobos":
    #     strategy = CobosStrategy(...)

    else:
        raise ExtractionError(f"Tipo de fuente no soportado: {source_platform}")

    logger.info(
        "🔧 Ejecutando extractor 3.0",
        extra={"repo": repo_name, "trigger": trigger_name, "source_platform": source_platform},
    )

    result = strategy.extract({"repo_name": repo_name, "trigger_name": trigger_name})
    state.raw_artifacts = result.get("raw_artifacts")

    return state
