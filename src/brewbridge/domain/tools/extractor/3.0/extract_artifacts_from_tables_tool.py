"""
Extract Artifacts from Tables Tool - Extracts Python code and quality governance YAML for table items.

This tool accepts a list of table items (pre-parsed by another component) and extracts
Python code and quality governance YAML for each table, storing them in state.
"""
from typing import Dict, Any, List

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.github_client import GitHubClient
from brewbridge.domain.extractor_strategies import (
    extract_artifacts_for_table,
    TableItemInput,
)
from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import GitHubAuthError, GitHubRequestError

logger = get_logger(__name__)


@tool_node
def extract_artifacts_from_tables_tool(state: MigrationGraphState) -> MigrationGraphState:
    """
    Extract Python code and quality governance YAML for a list of table items.
    
    This is a plug-and-play tool that accepts pre-parsed table items and extracts
    artifacts for each table. The table items should be provided by another component
    that handles ADF trigger parsing.
    
    Expected state input:
    - state.current_pipeline_data: Dictionary containing:
        - 'table_items': List of dictionaries (will be converted to TableItemInput Pydantic models), each containing:
            - target_table: Target table name (required)
            - adb_notebook_path: Path to the notebook (required)
            - zone: Target zone (e.g., "maz") (optional, defaults to "maz")
            - domain: Target business domain (e.g., "tech") (optional, defaults to "tech")
            - subdomain: Subdomain (e.g., "sap_pr0") (optional, inferred from path if not provided)
            - load_to_silver: Dict indicating if quality governance should be extracted (optional)
            - source_system: Source system identifier (optional)
            - target_database: Target database name (optional)
            - other metadata fields (optional)
        - 'branch': Git branch to use (optional, defaults to "main")
        - 'extract_quality_governance': Whether to extract quality governance YAML (optional, defaults to True)
    - state.credentials: Dictionary containing 'GITHUB_TOKEN'
    
    Updates state with:
    - state.raw_artifacts_3_0: Dictionary with 'table_items' containing all extracted artifacts (as dictionaries)
    """
    # Get credentials
    credentials = state.credentials or {}
    github_token = credentials.get("GITHUB_TOKEN")
    
    if not github_token:
        raise ValueError("GITHUB_TOKEN not found in credentials")
    
    # Get pipeline data
    pipeline_data = state.current_pipeline_data or {}
    
    # Get table items (pre-parsed by another component)
    table_items = pipeline_data.get("table_items", [])
    
    if not table_items:
        logger.warning("No table_items found in current_pipeline_data. Skipping extraction.")
        if state.raw_artifacts_3_0 is None:
            state.raw_artifacts_3_0 = {}
        state.raw_artifacts_3_0["table_items"] = []
        state.raw_artifacts_3_0["extraction_summary"] = {
            "total_items": 0,
            "successful_extractions": 0,
            "failed_extractions": 0
        }
        return state
    
    logger.info(f"Found {len(table_items)} table items to process")
    
    # Initialize GitHub client
    github_client = GitHubClient(token=github_token)
    
    # Get extraction parameters
    branch = pipeline_data.get("branch", "main")
    extract_quality = pipeline_data.get("extract_quality_governance", True)
    
    logger.info(f"Extracting artifacts for tables (extract_quality_governance={extract_quality})")
    
    # Initialize raw_artifacts_3_0 if it doesn't exist
    if state.raw_artifacts_3_0 is None:
        state.raw_artifacts_3_0 = {}
    
    # Process each table item individually (caller handles iteration)
    extracted_items = []
    successful = 0
    failed = 0
    
    for table_item_dict in table_items:
        try:
            # Convert dictionary to Pydantic model (validates and infers missing fields)
            table_item = TableItemInput(**table_item_dict)
            
            # Extract artifacts for this single table
            artifact = extract_artifacts_for_table(
                table_item=table_item,
                github_client=github_client,
                branch=branch,
                extract_quality_governance=extract_quality
            )
            
            # Convert Pydantic model to dictionary for storage in state
            extracted_items.append(artifact.model_dump())
            successful += 1
        except (ValueError, GitHubAuthError, GitHubRequestError) as e:
            failed += 1
            table_name = table_item_dict.get('target_table', 'unknown') if isinstance(table_item_dict, dict) else 'unknown'
            logger.error(f"Failed to extract artifacts for table {table_name}: {e}")
            # Continue with next table instead of failing completely
            continue
        except Exception as e:
            failed += 1
            table_name = table_item_dict.get('target_table', 'unknown') if isinstance(table_item_dict, dict) else 'unknown'
            logger.error(f"Unexpected error extracting artifacts for table {table_name}: {e}")
            # Continue with next table instead of failing completely
            continue
    
    # Store extracted artifacts
    state.raw_artifacts_3_0["table_items"] = extracted_items
    state.raw_artifacts_3_0["extraction_summary"] = {
        "total_items": len(table_items),
        "successful_extractions": successful,
        "failed_extractions": failed
    }
    
    logger.info(
        f"Successfully extracted artifacts for {successful} out of {len(table_items)} tables"
    )
    
    return state

