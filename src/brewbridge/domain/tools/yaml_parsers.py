from brewbridge.core.base_nodes import tool_node
from brewbridge.infrastructure.observability import track_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.logger import get_logger
from brewbridge.domain.parsers.runner import run_parsers_as_yaml

logger = get_logger(__name__)

@track_node("tool")
@tool_node
def yaml_parsers(state: MigrationGraphState) -> MigrationGraphState:
    """
    Clone or update required 4.0 framework repositories.

    This tool analyzes pipelines_to_migrate to determine which frameworks
    (brewtiful, hopsflow) are needed and ensures they are locally available
    under cache/ directory.

    Expected state input:
    - state.pipelines_to_migrate: List of pipelines from manifest
    - state.credentials: Dictionary containing GITHUB_TOKEN

    Updates state with:
    - state.repos_cloned: List of framework names that were cloned/updated
    """
    json_list = {
        "acl": state.normalized_schema_v4["acl"],
        "metadata": state.normalized_schema_v4["metadata"],
        "observability": state.normalized_schema_v4["observability"],
        "quality": state.normalized_schema_v4["quality"],
        "sync": state.normalized_schema_v4["sync"],
    }

    parser_config = {
        "brz": ([], "Running brz parser for brz environment_type in state"),
        "slv": (["transformations"], "Running slv parser for slv environment_type in state"),
        "gld": (["notebook"], "Running gld parser for gld environment_type in state"),
    }

    extra_fields, log_msg = parser_config.get(state.environment_type, ([], ""))

    logger.info(log_msg)

    for field in extra_fields:
        json_list[field] = state.normalized_schema_v4[field]
    
    parser_names = ["acl", "metadata", "observability", "quality", "sync"] + extra_fields
    run_parsers_as_yaml(parser_names=parser_names, json_list=json_list)
    
    return state