"""
Test script to verify repo_cloner_tool works with hopsflow repository.
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
    """Test the repo_cloner_tool with hopsflow repository."""
    
    # Get GitHub token from environment
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        logger.error("GITHUB_TOKEN not found!")
        logger.error("Please set it in one of these ways:")
        logger.error("  1. Create a .env file with: GITHUB_TOKEN=your_token")
        logger.error("  2. Set environment variable: $env:GITHUB_TOKEN='your_token' (PowerShell)")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Testing Repo_Cloner_Tool - Hopsflow Repository")
    logger.info("=" * 60)
    
    # Test with hopsflow (brz/slv pipelines)
    logger.info("")
    logger.info("Testing with hopsflow (brz pipeline)...")
    logger.info("")
    
    test_state_hopsflow = MigrationGraphState(
        pipelines_to_migrate=[
            {"name": "brz_maz_logistics_test_pipeline"},  # Requires hopsflow
        ],
        credentials={"GITHUB_TOKEN": github_token}
    )
    
    try:
        logger.info("Cloning/updating hopsflow repository...")
        logger.info("Repository URL: https://github.com/BrewDat/brewdat-pltfrm-ghq-tech-hopsflow.git")
        logger.info("")
        
        result_state = repo_cloner_tool(test_state_hopsflow)
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("[SUCCESS] Hopsflow repository operation completed!")
        logger.info("=" * 60)
        logger.info(f"Repositories cloned/updated: {result_state.repos_cloned}")
        
        # Verify repository exists
        repo_path = Path("cache/hopsflow")
        if repo_path.exists() and (repo_path / ".git").exists():
            logger.info(f"[OK] hopsflow repository verified at {repo_path}")
            
            # Check if it's actually the correct repository
            try:
                from git import Repo
                repo = Repo(repo_path)
                remote_url = repo.remotes.origin.url
                logger.info(f"[OK] Remote URL: {remote_url}")
                
                expected_url = "https://github.com/BrewDat/brewdat-pltfrm-ghq-tech-hopsflow.git"
                if expected_url in remote_url or "brewdat-pltfrm-ghq-tech-hopsflow" in remote_url:
                    logger.info("[OK] Repository URL matches expected hopsflow repository!")
                else:
                    logger.warning(f"[WARNING] Repository URL doesn't match expected: {remote_url}")
                    
            except Exception as e:
                logger.warning(f"[WARNING] Could not verify repository URL: {e}")
        else:
            logger.warning(f"[WARNING] hopsflow repository path exists but .git not found")
        
        return 0
        
    except KeyboardInterrupt:
        logger.error("")
        logger.error("[INTERRUPTED] Clone operation was interrupted.")
        return 1
    except Exception as e:
        logger.error("")
        logger.error(f"[FAILED] Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())

