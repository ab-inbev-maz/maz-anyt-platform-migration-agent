"""
Repo_Cloner_Tool - Unified Framework Preparation Tool

This tool prepares the migration environment by cloning or updating the
destination frameworks repositories (brewtiful and/or hopsflow) from GitHub.

It analyzes the list of pipelines from the manifest (pipelines_to_migrate) and
determines which frameworks are required based on pipeline naming patterns:
- "gld" → Brewtiful (Gold framework)
- "brz" or "slv" → Hopsflow (Bronze/Silver framework)
"""

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.services.repo_cloner_service import RepoClonerService
from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.observability import track_node
from brewbridge.utils.exceptions import RepositoryCloneError

logger = get_logger(__name__)


@track_node("tool")
@tool_node
def repo_cloner(state: MigrationGraphState) -> MigrationGraphState:
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
    credentials = state.credentials or {}
    github_token = credentials.get("GITHUB_TOKEN")

    if not github_token:
        raise RepositoryCloneError("GITHUB_TOKEN not found in credentials")

    service = RepoClonerService()
    cloned_repos = service.prepare_repositories(github_token)

    state.repos_cloned = cloned_repos
    return state
