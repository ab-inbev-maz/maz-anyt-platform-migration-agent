"""
Test script to execute the repo_cloner_tool feature.
"""
import os
import sys
from pathlib import Path
import dotenv

# Load environment variables from .env file if it exists
dotenv.load_dotenv()

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.tools.repo_cloner_tool import repo_cloner_tool
from brewbridge.infrastructure.logger import get_logger

logger = get_logger("test_repo_cloner")


def main():
    """Test the repo_cloner_tool with sample data."""
    
    # Get GitHub token from environment (including .env file)
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        logger.error("GITHUB_TOKEN not found!")
        logger.error("Please set it in one of these ways:")
        logger.error("  1. Create a .env file with: GITHUB_TOKEN=your_token")
        logger.error("  2. Set environment variable: $env:GITHUB_TOKEN='your_token' (PowerShell)")
        logger.error("  3. Set environment variable: export GITHUB_TOKEN='your_token' (Bash)")
        logger.error("")
        logger.error("Note: This tool requires a valid GitHub token to clone repositories.")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Testing Repo_Cloner_Tool")
    logger.info("=" * 60)
    
    # Create test state with pipelines that require both frameworks
    test_state = MigrationGraphState(
        pipelines_to_migrate=[
            {"name": "gld_maz_sales_test_pipeline"},  # Requires brewtiful
            {"name": "brz_maz_logistics_test_pipeline"},  # Requires hopsflow
        ],
        credentials={"GITHUB_TOKEN": github_token}
    )
    
    logger.info(f"Test pipelines:")
    for pipeline in test_state.pipelines_to_migrate:
        logger.info(f"  - {pipeline['name']}")
    
    logger.info("")
    logger.info("Executing repo_cloner_tool...")
    logger.info("")
    
    try:
        # Execute the tool
        result_state = repo_cloner_tool(test_state)
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("[SUCCESS] Repo_Cloner_Tool executed successfully!")
        logger.info("=" * 60)
        logger.info(f"Repositories cloned/updated: {result_state.repos_cloned}")
        
        # Verify repositories exist
        if result_state.repos_cloned:
            for repo_name in result_state.repos_cloned:
                repo_path = Path(f"cache/{repo_name}")
                if repo_path.exists() and (repo_path / ".git").exists():
                    logger.info(f"[OK] {repo_name} repository verified at {repo_path}")
                else:
                    logger.warning(f"[WARNING] {repo_name} repository path exists but .git not found")
        
        return 0
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 60)
        logger.error("[FAILED] Repo_Cloner_Tool failed!")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())

