"""
Set up the environment for the migration.

This tool reads the manifest.yaml file, merges the credentials, and performs
pre-flight API connectivity checks using the domain-level
`ManifestPreflightService`.
"""

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.services.read_manifest_and_check_api import ManifestPreflightService
from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.observability import track_node
from brewbridge.utils.exceptions import ManifestNotFoundError, ManifestParseError
from brewbridge.utils.manifest_yaml_utils import load_manifest

logger = get_logger(__name__)


@track_node("tool")
@tool_node
def read_manifest_and_check_api(
    state: MigrationGraphState,
) -> MigrationGraphState:
    """
    Read manifest.yaml, merge credentials, and perform pre-flight API
    connectivity checks.

    This is the first deterministic node in the MigrationFlow.

    Expected state input:
    - state.manifest_path: Path to manifest.yaml file

    Updates state with:
    - manifest_path: Path to the manifest file
    - pipelines_to_migrate: List of pipelines from manifest
    - credentials: Merged credentials (env + manifest)
    - api_connectivity_ok: Boolean indicating if all required APIs
      are accessible.
    """
    manifest_path = state.manifest_path

    if not manifest_path:
        raise ManifestNotFoundError("manifest_path not provided in state")

    service = ManifestPreflightService()

    # Step 2: Merge credentials (env vars take precedence)
    credentials = service.collect_env_credentials()

    github_env_present = bool(credentials.get("GITHUB_TOKEN"))
    adf_env_present = all(
        credentials.get(key) for key in ["ADF_TENANT_ID", "ADF_CLIENT_ID", "ADF_CLIENT_SECRET"]
    )
    databricks_env_present = all(
        credentials.get(key)
        for key in ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID"]
    )
    llm_env_present = bool(
        (credentials.get("ASIMOV_URL") and credentials.get("ASIMOV_PRODUCT_TOKEN"))
        or credentials.get("OPENAI_API_KEY")
    )

    # Log warnings for missing expected credentials
    expected_creds = ["GITHUB_TOKEN"]
    missing_creds = [cred for cred in expected_creds if cred not in credentials]
    if missing_creds:
        logger.warning(
            "Missing expected credentials: %s",
            ", ".join(missing_creds),
        )

    # Step 3: Validate connectivity
    if github_env_present:
        github_ok = service.ping_github(credentials)
    else:
        logger.info("Skipping GitHub connectivity check; GITHUB_TOKEN env var not set.")
        github_ok = False

    if adf_env_present:
        adf_ok = service.ping_adf(credentials)
    else:
        logger.info("Skipping Azure Data Factory connectivity check; no ADF env vars provided.")
        adf_ok = True

    if databricks_env_present:
        databricks_ok = service.ping_databricks(credentials)
    else:
        logger.info("Skipping Databricks connectivity check; no Databricks env vars provided.")
        databricks_ok = True

    if llm_env_present:
        llm_ok = service.ping_llm_apis(credentials)
    else:
        logger.info("Skipping LLM connectivity check; no LLM env vars provided.")
        llm_ok = True

    # api_connectivity_ok is True only if all required APIs are accessible.
    # GitHub is required; ADF, Databricks and LLM are optional but should
    # be checked if credentials exist.
    api_connectivity_ok = (
        github_ok
        and (adf_ok or "ADF_TENANT_ID" not in credentials)
        and (databricks_ok or not databricks_env_present)
        and (llm_ok or not any(key in credentials for key in ["ASIMOV_URL", "OPENAI_API_KEY"]))
    )

    if not api_connectivity_ok:
        logger.warning(
            "Some API connectivity checks failed. Migration may encounter issues.",
        )

    # Step 4: Update state
    state.manifest_path = manifest_path
    state.credentials = credentials
    state.api_connectivity_ok = api_connectivity_ok

    logger.info(
        "Manifest loaded successfully. API connectivity: %s",
        "OK" if api_connectivity_ok else "FAILED",
    )

    return state
