"""
Read_Manifest_and_Check_API Tool - First deterministic node in MigrationFlow for Platform 3.0.

This tool initializes the migration process by:
- Reading and validating the manifest.yaml provided at runtime
- Loading and merging credentials from environment variables and/or manifest
- Performing pre-flight connectivity checks to required APIs (GitHub, Azure Data Factory)
- Ping LLMs APIs to grant connection for the intelligent Agents
"""
import os
import time
from typing import Dict, Any, Optional

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.github_client import GitHubClient
from brewbridge.infrastructure.datafactory_client import ADFClient
from brewbridge.utils.yaml_utils import load_manifest
from brewbridge.utils.exceptions import ManifestNotFoundError, ManifestParseError
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)


def _merge_credentials(env_creds: Dict[str, str], manifest_creds: Dict[str, str]) -> Dict[str, str]:
    """
    Merge credentials from environment variables and manifest file.
    Environment variables take precedence over manifest credentials.
    
    :param env_creds: Credentials from environment variables
    :param manifest_creds: Credentials from manifest file
    :return: Merged credentials dictionary
    """
    merged = manifest_creds.copy()
    merged.update(env_creds)  # Env vars override manifest
    return merged


def _collect_env_credentials() -> Dict[str, str]:
    """
    Collect credentials from environment variables.
    
    :return: Dictionary of credentials from environment
    """
    creds = {}
    
    # GitHub credentials
    if github_token := os.getenv("GITHUB_TOKEN"):
        creds["GITHUB_TOKEN"] = github_token
    
    # Azure Data Factory credentials
    if tenant_id := os.getenv("ADF_TENANT_ID"):
        creds["ADF_TENANT_ID"] = tenant_id
    if client_id := os.getenv("ADF_CLIENT_ID"):
        creds["ADF_CLIENT_ID"] = client_id
    if client_secret := os.getenv("ADF_CLIENT_SECRET"):
        creds["ADF_CLIENT_SECRET"] = client_secret
    
    # LLM API credentials (Asimov)
    if asimov_url := os.getenv("ASIMOV_URL"):
        creds["ASIMOV_URL"] = asimov_url
    if asimov_token := os.getenv("ASIMOV_PRODUCT_TOKEN"):
        creds["ASIMOV_PRODUCT_TOKEN"] = asimov_token
    
    # OpenAI (if used directly)
    if openai_key := os.getenv("OPENAI_API_KEY"):
        creds["OPENAI_API_KEY"] = openai_key
    
    return creds


def _ping_github(credentials: Dict[str, str]) -> bool:
    """
    Ping GitHub API to verify connectivity and token validity.
    Retries up to 3 times on failure.
    
    :param credentials: Credentials dictionary
    :return: True if ping successful, False otherwise
    """
    github_token = credentials.get("GITHUB_TOKEN")
    if not github_token:
        logger.warning("GitHub token not found in credentials. Skipping GitHub ping.")
        return False
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = GitHubClient(token=github_token)
            if client.ping():
                return True
        except Exception as e:
            logger.warning(f"GitHub ping attempt {attempt + 1}/{max_retries} failed: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(1)  # Wait 1 second before retry
    
    logger.error("GitHub ping failed after all retries.")
    return False


def _ping_adf(credentials: Dict[str, str]) -> bool:
    """
    Ping Azure Data Factory API to verify connectivity.
    Retries up to 3 times on failure.
    
    :param credentials: Credentials dictionary
    :return: True if ping successful, False otherwise
    """
    tenant_id = credentials.get("ADF_TENANT_ID")
    client_id = credentials.get("ADF_CLIENT_ID")
    client_secret = credentials.get("ADF_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        logger.warning("Azure Data Factory credentials incomplete. Skipping ADF ping.")
        return False
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = ADFClient(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
            if client.ping():
                return True
        except Exception as e:
            logger.warning(f"ADF ping attempt {attempt + 1}/{max_retries} failed: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(1)  # Wait 1 second before retry
    
    logger.error("Azure Data Factory ping failed after all retries.")
    return False


def _ping_llm_apis(credentials: Dict[str, str]) -> bool:
    """
    Ping LLM APIs to verify connectivity.
    Currently checks Asimov API.
    
    :param credentials: Credentials dictionary
    :return: True if at least one LLM API is accessible, False otherwise
    """
    asimov_url = credentials.get("ASIMOV_URL")
    asimov_token = credentials.get("ASIMOV_PRODUCT_TOKEN")
    
    if asimov_url and asimov_token:
        try:
            import requests
            # Simple connectivity check - try to access the API base URL
            response = requests.get(f"{asimov_url}/health", timeout=5)
            if response.status_code in [200, 404]:  # 404 is OK, means API is reachable
                logger.info("Asimov LLM API ping successful.")
                return True
        except Exception as e:
            logger.warning(f"Asimov LLM API ping failed: {e}")
    
    # Check OpenAI if configured
    openai_key = credentials.get("OPENAI_API_KEY")
    if openai_key:
        try:
            import requests
            response = requests.get(
                "https://api.openai.com/v1/models",
                headers={"Authorization": f"Bearer {openai_key}"},
                timeout=5
            )
            if response.status_code == 200:
                logger.info("OpenAI API ping successful.")
                return True
        except Exception as e:
            logger.warning(f"OpenAI API ping failed: {e}")
    
    logger.warning("No LLM API credentials found or all pings failed.")
    return False


@tool_node
def read_manifest_and_check_api(state: MigrationGraphState) -> MigrationGraphState:
    """
    Read manifest.yaml, merge credentials, and perform pre-flight API connectivity checks.
    
    This is the first deterministic node in the MigrationFlow for Platform 3.0 migrations.
    
    Expected state input:
    - state.manifest_path: Path to manifest.yaml file
    
    Updates state with:
    - manifest_path: Path to the manifest file
    - pipelines_to_migrate: List of pipelines from manifest
    - credentials: Merged credentials (env + manifest)
    - api_connectivity_ok: Boolean indicating if all required APIs are accessible
    """
    manifest_path = state.manifest_path
    
    if not manifest_path:
        raise ManifestNotFoundError("manifest_path not provided in state")
    
    logger.info(f"Reading manifest from: {manifest_path}")
    
    # Step 1: Read and validate manifest
    try:
        manifest_data = load_manifest(manifest_path)
    except (ManifestNotFoundError, ManifestParseError) as e:
        logger.error(f"Failed to load manifest: {e}")
        raise
    
    pipelines = manifest_data.get("pipelines_to_migrate", [])
    manifest_creds = manifest_data.get("credentials", {})
    
    logger.info(f"Found {len(pipelines)} pipelines to migrate")
    
    # Step 2: Merge credentials (env vars take precedence)
    env_creds = _collect_env_credentials()
    credentials = _merge_credentials(env_creds, manifest_creds)
    
    # Log warnings for missing expected credentials
    expected_creds = ["GITHUB_TOKEN"]
    missing_creds = [cred for cred in expected_creds if cred not in credentials]
    if missing_creds:
        logger.warning(f"Missing expected credentials: {', '.join(missing_creds)}")
    
    # Step 3: Validate connectivity
    github_ok = _ping_github(credentials)
    adf_ok = _ping_adf(credentials) if any(k in credentials for k in ["ADF_TENANT_ID", "ADF_CLIENT_ID", "ADF_CLIENT_SECRET"]) else True
    llm_ok = _ping_llm_apis(credentials)
    
    # api_connectivity_ok is True only if all required APIs are accessible
    # GitHub is required, ADF and LLM are optional but should be checked if credentials exist
    api_connectivity_ok = github_ok and (adf_ok or not any(k in credentials for k in ["ADF_TENANT_ID"])) and (llm_ok or not any(k in credentials for k in ["ASIMOV_URL", "OPENAI_API_KEY"]))
    
    if not api_connectivity_ok:
        logger.warning("Some API connectivity checks failed. Migration may encounter issues.")
    
    # Step 4: Update state
    state.manifest_path = manifest_path
    state.pipelines_to_migrate = pipelines
    state.credentials = credentials
    state.api_connectivity_ok = api_connectivity_ok
    
    logger.info(f"Manifest loaded successfully. API connectivity: {'OK' if api_connectivity_ok else 'FAILED'}")
    
    return state

