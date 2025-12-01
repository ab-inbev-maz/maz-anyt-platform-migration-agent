"""
Repo_Cloner_Tool - Unified Framework Preparation Tool

This tool prepares the migration environment by cloning or updating the destination
frameworks repositories (brewtiful and/or hopsflow) from GitHub.

It analyzes the list of pipelines from the manifest (pipelines_to_migrate) and determines
which frameworks are required based on pipeline naming patterns:
- "gld" → Brewtiful (Gold framework)
- "brz" or "slv" → Hopsflow (Bronze/Silver framework)
"""
from pathlib import Path
from typing import Set

from git import Repo
from git.exc import GitCommandError, GitError

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.constans import ConstansLibrary
from brewbridge.utils.exceptions import RepositoryCloneError

logger = get_logger(__name__)


def _detect_required_frameworks(pipelines: list) -> Set[str]:
    """
    Detect which frameworks are required based on pipeline names.
    
    :param pipelines: List of pipeline dictionaries from manifest
    :return: Set of framework names ("brewtiful", "hopsflow")
    """
    repos_to_clone = set()
    
    for pipeline in pipelines:
        name = pipeline.get("name", "").lower()
        
        # Check for gold (gld) → brewtiful
        if "gld" in name:
            repos_to_clone.add("brewtiful")
        
        # Check for bronze (brz) or silver (slv) → hopsflow
        if any(x in name for x in ["brz", "slv"]):
            repos_to_clone.add("hopsflow")
    
    return repos_to_clone


def _get_repo_url(repo_name: str) -> str:
    """
    Get the GitHub URL for a framework repository.
    
    :param repo_name: Framework name ("brewtiful" or "hopsflow")
    :return: GitHub repository URL
    """
    repo_urls = {
        "brewtiful": ConstansLibrary.BREWTIFUL_REPO_URL,
        "hopsflow": ConstansLibrary.HOPSFLOW_REPO_URL,
    }
    
    if repo_name not in repo_urls:
        raise ValueError(f"Unknown framework repository: {repo_name}")
    
    return repo_urls[repo_name]


def _clone_or_pull_repo(repo_name: str, destination: Path, github_token: str) -> None:
    """
    Clone a repository if it doesn't exist, or pull updates if it does.
    
    :param repo_name: Framework name ("brewtiful" or "hopsflow")
    :param destination: Local path where repository should be cloned
    :param github_token: GitHub token for authentication
    :raises RepositoryCloneError: If clone or pull fails
    """
    repo_url = _get_repo_url(repo_name)
    
    # Construct authenticated URL with token
    # Format: https://token@github.com/owner/repo.git
    if "https://github.com/" in repo_url:
        authenticated_url = repo_url.replace(
            "https://github.com/",
            f"https://{github_token}@github.com/"
        )
    else:
        authenticated_url = repo_url
    
    try:
        if destination.exists() and (destination / ".git").exists():
            # Repository already exists, pull updates
            logger.info(f"Repository {repo_name} already exists at {destination}, pulling updates...")
            
            # Check for and remove stale lock file if it exists
            lock_file = destination / ".git" / "index.lock"
            if lock_file.exists():
                logger.warning(f"Found stale git lock file, attempting to remove it...")
                try:
                    lock_file.unlink()
                except (PermissionError, OSError) as e:
                    # Lock file might be held by another process (e.g., OneDrive sync)
                    logger.warning(f"Could not remove lock file (may be in use by another process): {e}")
                    logger.warning(f"Will attempt to proceed anyway...")
                    import time
                    time.sleep(1)  # Brief wait in case it's being released
            
            repo = Repo(destination)
            
            # Reset any local changes to ensure clean pull
            # This is safe for cache repositories that should always match remote
            if repo.is_dirty():
                logger.info(f"Repository has local changes, resetting to match remote...")
                repo.head.reset(working_tree=True)
            
            # Fetch and pull latest changes
            origin = repo.remotes.origin
            origin.fetch()
            
            # Try to pull, if it fails due to conflicts, reset hard to remote
            try:
                origin.pull()
            except GitCommandError as pull_error:
                if "would be overwritten" in str(pull_error) or "local changes" in str(pull_error):
                    logger.warning(f"Pull failed due to local changes, resetting to remote state...")
                    repo.head.reset(working_tree=True, index=True)
                    # Try pull again after reset
                    origin.pull()
                else:
                    raise
            
            logger.info(f"Successfully updated repository {repo_name}")
        elif destination.exists() and not (destination / ".git").exists():
            # Directory exists but is not a git repository (e.g., just .gitkeep file)
            # Remove it and clone fresh
            logger.info(f"Directory {destination} exists but is not a git repository. Removing and cloning fresh...")
            import shutil
            shutil.rmtree(destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cloning repository {repo_name} (this may take several minutes for large repositories)...")
            Repo.clone_from(authenticated_url, str(destination))
            logger.info(f"Successfully cloned repository {repo_name}")
        else:
            # Repository doesn't exist, clone it
            logger.info(f"Cloning repository {repo_name} to {destination} (this may take several minutes for large repositories)...")
            
            # Ensure parent directory exists
            destination.parent.mkdir(parents=True, exist_ok=True)
            
            # Clone the repository
            Repo.clone_from(authenticated_url, str(destination))
            
            logger.info(f"Successfully cloned repository {repo_name}")
            
    except GitCommandError as e:
        error_msg = f"Git command failed for {repo_name}: {e}"
        logger.error(error_msg)
        raise RepositoryCloneError(error_msg) from e
    except GitError as e:
        error_msg = f"Git error for {repo_name}: {e}"
        logger.error(error_msg)
        raise RepositoryCloneError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error cloning/updating {repo_name}: {e}"
        logger.error(error_msg)
        raise RepositoryCloneError(error_msg) from e


@tool_node
def repo_cloner_tool(state: MigrationGraphState) -> MigrationGraphState:
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
    pipelines = state.pipelines_to_migrate or []
    credentials = state.credentials or {}
    github_token = credentials.get("GITHUB_TOKEN")
    
    if not github_token:
        raise RepositoryCloneError("GITHUB_TOKEN not found in credentials")
    
    if not pipelines:
        logger.warning("No pipelines to migrate found. Skipping repository cloning.")
        state.repos_cloned = []
        return state
    
    # Step 1: Detect required frameworks
    repos_to_clone = _detect_required_frameworks(pipelines)
    
    if not repos_to_clone:
        logger.info("No framework repositories detected from pipeline names")
        state.repos_cloned = []
        return state
    
    logger.info(f"Detected frameworks to clone: {list(repos_to_clone)}")
    
    # Step 2: Clone or update each repository
    cloned_repos = []
    for repo_name in sorted(repos_to_clone):
        repo_path = Path(f"cache/{repo_name}")
        
        try:
            _clone_or_pull_repo(repo_name, repo_path, github_token)
            cloned_repos.append(repo_name)
            logger.info(f"Repository {repo_name} is ready at {repo_path}")
        except RepositoryCloneError as e:
            logger.error(f"Failed to prepare repository {repo_name}: {e}")
            raise
    
    # Step 3: Update state
    state.repos_cloned = cloned_repos
    logger.info(f"Framework repository setup completed. Cloned/updated: {cloned_repos}")
    
    return state

