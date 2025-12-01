"""
Framework 3.0 Strategy for extracting artifacts from Platform 3.0.

This module provides methods to extract Python notebooks and other artifacts
from GitHub repositories using the adb_notebook_path format.
"""

from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator, model_validator

from brewbridge.infrastructure.github_client import GitHubClient
from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import GitHubRequestError, GitHubAuthError

logger = get_logger(__name__)


class TableItemInput(BaseModel):
    """
    Pydantic model for table item input.
    
    Represents a single table item that needs artifact extraction.
    """
    target_table: str = Field(..., description="Target table name (e.g., 'mx_dd07t')")
    adb_notebook_path: str = Field(..., description="ADB notebook path (e.g., '//brewdat-maz-maz-tech-sap-repo-adb/data_integration/sap_pr0/load_slv_generic_parquet')")
    zone: Optional[str] = Field(default="maz", description="Target zone (e.g., 'maz')")
    domain: Optional[str] = Field(default="tech", description="Target business domain (e.g., 'tech')")
    subdomain: Optional[str] = Field(default=None, description="Subdomain (e.g., 'sap_pr0')")
    load_to_silver: Optional[Dict[str, Any]] = Field(default=None, description="Dict indicating if quality governance should be extracted")
    source_system: Optional[str] = Field(default=None, description="Source system identifier")
    target_database: Optional[str] = Field(default=None, description="Target database name")
    source_database: Optional[str] = Field(default=None, description="Source database name")
    target_business_subdomain: Optional[str] = Field(default=None, description="Target business subdomain")
    
    @model_validator(mode='after')
    def infer_subdomain_if_missing(self):
        """Infer subdomain from adb_notebook_path if not provided."""
        if not self.subdomain:
            inferred = _extract_subdomain_from_path(self.adb_notebook_path)
            if inferred:
                self.subdomain = inferred
            elif not self.source_system:
                raise ValueError(
                    f"subdomain is required for {self.target_table} and cannot be inferred from path"
                )
            else:
                self.subdomain = self.source_system
        return self


class TableArtifactMetadata(BaseModel):
    """Metadata for extracted table artifacts."""
    zone: str = Field(..., description="Target zone")
    domain: str = Field(..., description="Target business domain")
    subdomain: str = Field(..., description="Subdomain")
    source_system: Optional[str] = Field(default=None, description="Source system identifier")
    target_database: Optional[str] = Field(default=None, description="Target database name")
    source_database: Optional[str] = Field(default=None, description="Source database name")
    target_business_subdomain: Optional[str] = Field(default=None, description="Target business subdomain")


class TableArtifact(BaseModel):
    """
    Pydantic model for extracted table artifacts.
    
    Represents the complete set of artifacts extracted for a single table.
    """
    target_table: str = Field(..., description="Target table name")
    notebook_code: str = Field(..., description="Extracted Python notebook code")
    notebook_path: str = Field(..., description="Original notebook path")
    quality_governance_yaml: Optional[str] = Field(default=None, description="Extracted quality governance YAML")
    quality_governance_path: Optional[str] = Field(default=None, description="Path to quality governance YAML file")
    metadata: TableArtifactMetadata = Field(..., description="Metadata for the extracted artifacts")


def extract_python_code_from_adb_path(
    adb_notebook_path: str,
    github_client: GitHubClient,
    branch: str = "main",
    default_owner: str = "BrewDat"
) -> str:
    """
    Extract Python code from a GitHub repository using an adb_notebook_path.
    
    This method parses the adb_notebook_path format and uses the GitHubClient
    to fetch the corresponding Python file from GitHub.
    
    Args:
        adb_notebook_path: Path in format `//{repo-name}/{file-path-without-extension}`
                          Example: `//brewdat-maz-maz-tech-sap-repo-adb/data_integration/sap_pr0/load_slv_generic_parquet`
        github_client: Authenticated GitHubClient instance
        branch: Git branch to fetch from (default: "main")
        default_owner: GitHub organization/owner name (default: "BrewDat")
    
    Returns:
        str: The Python code content from the file
    
    Raises:
        ValueError: If adb_notebook_path format is invalid
        GitHubAuthError: If authentication fails
        GitHubRequestError: If file cannot be fetched
    
    Example:
        >>> client = GitHubClient(token="your_token")
        >>> path = "//brewdat-maz-maz-tech-sap-repo-adb/data_integration/sap_pr0/load_slv_generic_parquet"
        >>> code = extract_python_code_from_adb_path(path, client)
        >>> print(code)
    """
    logger.debug(f"Extracting Python code from adb_notebook_path: {adb_notebook_path}")
    
    # Validate and parse the adb_notebook_path format
    # Expected format: //{repo-name}/{file-path-without-extension}
    if not adb_notebook_path.startswith("//"):
        raise ValueError(
            f"Invalid adb_notebook_path format. Expected format: //{{repo-name}}/{{file-path}} "
            f"Got: {adb_notebook_path}"
        )
    
    # Remove the leading "//"
    path_without_prefix = adb_notebook_path[2:]
    
    # Split into repo name and file path
    parts = path_without_prefix.split("/", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Invalid adb_notebook_path format. Could not parse repo and file path. "
            f"Expected format: //{{repo-name}}/{{file-path}} Got: {adb_notebook_path}"
        )
    
    repo_name, file_path_without_ext = parts
    
    # Validate repo name is not empty
    if not repo_name:
        raise ValueError(
            f"Invalid adb_notebook_path format. Repo name cannot be empty. "
            f"Got: {adb_notebook_path}"
        )
    
    # Validate file path is not empty
    if not file_path_without_ext:
        raise ValueError(
            f"Invalid adb_notebook_path format. File path cannot be empty. "
            f"Got: {adb_notebook_path}"
        )
    
    # Construct GitHub repository name: {owner}/{repo-name}
    github_repo = f"{default_owner}/{repo_name}"
    
    # Add .py extension to the file path
    file_path = f"{file_path_without_ext}.py"
    
    logger.info(
        f"Parsed adb_notebook_path: repo={github_repo}, path={file_path}, branch={branch}"
    )
    
    try:
        # Fetch the file content using GitHubClient
        python_code = github_client.get_file(
            repo=github_repo,
            path=file_path,
            branch=branch
        )
        
        logger.info(
            f"Successfully extracted Python code from {adb_notebook_path} "
            f"({len(python_code)} characters)"
        )
        
        return python_code
    
    except GitHubAuthError as e:
        logger.error(f"GitHub authentication error while fetching {adb_notebook_path}: {e}")
        raise
    except GitHubRequestError as e:
        logger.error(f"GitHub request error while fetching {adb_notebook_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while extracting Python code from {adb_notebook_path}: {e}")
        raise GitHubRequestError(f"Failed to extract Python code: {e}") from e


def extract_quality_governance_yaml(
    target_table: str,
    zone: str,
    domain: str,
    subdomain: str,
    github_client: GitHubClient,
    repo_name: str = "brewdat-pltfrm-ghq-tech-datagovernance",
    branch: str = "main",
    default_owner: str = "BrewDat"
) -> str:
    """
    Extract quality governance YAML from the datagovernance repository.
    
    This method constructs the path to the quality governance YAML file based on
    the target table name and extracts it from GitHub.
    
    Path pattern: src/{zone}/{zone}/{domain}/{subdomain}/dq_definitions/{country}/slv_{zone}_{domain}_{subdomain}/{target_table}.yaml
    
    Args:
        target_table: Target table name (e.g., "mx_cdhdr")
                     The country code is extracted from the first part before underscore
        zone: Zone identifier (e.g., "maz")
        domain: Domain identifier (e.g., "tech")
        subdomain: Subdomain identifier (e.g., "sap_pr0")
        github_client: Authenticated GitHubClient instance
        repo_name: GitHub repository name (default: "brewdat-pltfrm-ghq-tech-datagovernance")
        branch: Git branch to fetch from (default: "main")
        default_owner: GitHub organization/owner name (default: "BrewDat")
    
    Returns:
        str: The YAML content from the file
    
    Raises:
        ValueError: If target_table format is invalid or required parameters are missing
        GitHubAuthError: If authentication fails
        GitHubRequestError: If file cannot be fetched
    
    Example:
        >>> client = GitHubClient(token="your_token")
        >>> yaml_content = extract_quality_governance_yaml(
        ...     target_table="mx_cdhdr",
        ...     zone="maz",
        ...     domain="tech",
        ...     subdomain="sap_pr0",
        ...     github_client=client
        ... )
        >>> print(yaml_content)
    """
    logger.debug(f"Extracting quality governance YAML for target_table: {target_table}")
    
    # Validate required parameters
    if not target_table:
        raise ValueError("target_table cannot be empty")
    if not zone:
        raise ValueError("zone cannot be empty")
    if not domain:
        raise ValueError("domain cannot be empty")
    if not subdomain:
        raise ValueError("subdomain cannot be empty")
    
    # Extract country code from target_table (first part before underscore)
    # Example: "mx_cdhdr" -> country="mx", table_part="cdhdr"
    if "_" not in target_table:
        raise ValueError(
            f"Invalid target_table format. Expected format: '{{country}}_{{table_name}}' "
            f"(e.g., 'mx_cdhdr'). Got: {target_table}"
        )
    
    parts = target_table.split("_", 1)
    country = parts[0]
    table_name = parts[1] if len(parts) > 1 else ""
    
    if not country or not table_name:
        raise ValueError(
            f"Invalid target_table format. Could not extract country and table name. "
            f"Got: {target_table}"
        )
    
    # Construct the path pattern:
    # src/{zone}/{zone}/{domain}/{subdomain}/dq_definitions/{country}/slv_{zone}_{domain}_{subdomain}/{target_table}.yaml
    table_prefix = f"slv_{zone}_{domain}_{subdomain}"
    yaml_path = f"src/{zone}/{zone}/{domain}/{subdomain}/dq_definitions/{country}/{table_prefix}/{target_table}.yaml"
    
    # Construct GitHub repository name
    github_repo = f"{default_owner}/{repo_name}"
    
    logger.info(
        f"Extracting quality governance YAML: repo={github_repo}, "
        f"path={yaml_path}, branch={branch}, country={country}"
    )
    
    try:
        # Fetch the file content using GitHubClient
        yaml_content = github_client.get_file(
            repo=github_repo,
            path=yaml_path,
            branch=branch
        )
        
        logger.info(
            f"Successfully extracted quality governance YAML for {target_table} "
            f"({len(yaml_content)} characters)"
        )
        
        return yaml_content
    
    except GitHubAuthError as e:
        logger.error(f"GitHub authentication error while fetching quality governance YAML: {e}")
        raise
    except GitHubRequestError as e:
        logger.error(f"GitHub request error while fetching quality governance YAML: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while extracting quality governance YAML: {e}")
        raise GitHubRequestError(f"Failed to extract quality governance YAML: {e}") from e


def extract_artifacts_for_table(
    table_item: TableItemInput,
    github_client: GitHubClient,
    branch: str = "main",
    extract_quality_governance: bool = True
) -> TableArtifact:
    """
    Extract Python code and quality governance YAML for a single table item.
    
    This is a plug-and-play method that accepts a single table item (pre-parsed by another component)
    and extracts artifacts for that table. The caller should handle iterating through multiple tables.
    
    Args:
        table_item: TableItemInput Pydantic model containing:
            - target_table: Target table name (required)
            - adb_notebook_path: Path to the notebook (required)
            - zone: Target zone (e.g., "maz") (optional, defaults to "maz")
            - domain: Target business domain (e.g., "tech") (optional, defaults to "tech")
            - subdomain: Subdomain (e.g., "sap_pr0") (optional, inferred from path if not provided)
            - load_to_silver: Dict indicating if quality governance should be extracted (optional)
            - source_system: Source system identifier (optional)
            - target_database: Target database name (optional)
            - other metadata fields (optional)
        github_client: Authenticated GitHubClient instance
        branch: Git branch to fetch from (default: "main")
        extract_quality_governance: Whether to extract quality governance YAML (default: True)
    
    Returns:
        TableArtifact Pydantic model with structure:
        {
            "target_table": "mx_dd07t",
            "notebook_code": "...",
            "notebook_path": "...",
            "quality_governance_yaml": "...",
            "quality_governance_path": "...",
            "metadata": {
                "zone": "maz",
                "domain": "tech",
                "subdomain": "sap_pr0",
                "source_system": "...",
                ...
            }
        }
    
    Raises:
        ValueError: If required fields are missing in table item
        GitHubAuthError: If authentication fails
        GitHubRequestError: If file cannot be fetched
    """
    logger.info(f"Extracting artifacts for table: {table_item.target_table}")
    
    # Extract Python code for this table
    logger.debug(f"Extracting Python code for {table_item.target_table} from {table_item.adb_notebook_path}")
    python_code = extract_python_code_from_adb_path(
        adb_notebook_path=table_item.adb_notebook_path,
        github_client=github_client,
        branch=branch
    )
    
    logger.info(f"Successfully extracted Python code for {table_item.target_table} ({len(python_code)} characters)")
    
    # Extract quality governance YAML if applicable
    quality_yaml = None
    quality_path = None
    
    # Check if we should extract quality governance (load_to_silver exists and extract_quality_governance is True)
    if extract_quality_governance and table_item.load_to_silver is not None and table_item.subdomain:
        try:
            logger.debug(f"Extracting quality governance YAML for {table_item.target_table}")
            quality_yaml = extract_quality_governance_yaml(
                target_table=table_item.target_table,
                zone=table_item.zone,
                domain=table_item.domain,
                subdomain=table_item.subdomain,
                github_client=github_client,
                branch=branch
            )
            
            # Build quality governance path
            country = table_item.target_table.split("_")[0] if "_" in table_item.target_table else ""
            quality_path = (
                f"src/{table_item.zone}/{table_item.zone}/{table_item.domain}/{table_item.subdomain}/dq_definitions/"
                f"{country}/slv_{table_item.zone}_{table_item.domain}_{table_item.subdomain}/{table_item.target_table}.yaml"
            )
            
            logger.info(
                f"Successfully extracted quality governance YAML for {table_item.target_table} "
                f"({len(quality_yaml)} characters)"
            )
        except Exception as e:
            logger.warning(
                f"Failed to extract quality governance YAML for {table_item.target_table}: {e}. "
                f"Continuing without it."
            )
    
    # Build artifact entry using Pydantic model
    artifact = TableArtifact(
        target_table=table_item.target_table,
        notebook_code=python_code,
        notebook_path=table_item.adb_notebook_path,
        quality_governance_yaml=quality_yaml,
        quality_governance_path=quality_path,
        metadata=TableArtifactMetadata(
            zone=table_item.zone,
            domain=table_item.domain,
            subdomain=table_item.subdomain,
            source_system=table_item.source_system,
            target_database=table_item.target_database,
            source_database=table_item.source_database,
            target_business_subdomain=table_item.target_business_subdomain,
        )
    )
    
    logger.info(f"✓ Successfully processed table: {table_item.target_table}")
    
    return artifact


def _extract_subdomain_from_path(adb_notebook_path: str) -> str:
    """
    Extract subdomain from adb_notebook_path.
    
    Example: //brewdat-maz-maz-tech-sap-repo-adb/data_integration/sap_pr0/load_slv_generic_parquet
    -> returns "sap_pr0"
    """
    try:
        # Remove "//" and split
        path_parts = adb_notebook_path[2:].split("/")
        # Look for common patterns like "sap_pr0", "sap_ecc", etc.
        for part in path_parts:
            if part.startswith("sap_") or part.startswith("tech_"):
                return part
        # If no pattern found, return the last directory before the file
        if len(path_parts) > 2:
            return path_parts[-2]  # Second to last part
        return ""
    except Exception:
        return ""

