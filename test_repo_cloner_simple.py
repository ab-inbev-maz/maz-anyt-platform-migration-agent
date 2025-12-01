"""
Simple test script to execute the repo_cloner_tool feature - one repository at a time.
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
    """Test the repo_cloner_tool with one framework at a time."""
    
    # Get GitHub token from environment
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        logger.error("GITHUB_TOKEN not found!")
        logger.error("Please set it in one of these ways:")
        logger.error("  1. Create a .env file with: GITHUB_TOKEN=your_token")
        logger.error("  2. Set environment variable: $env:GITHUB_TOKEN='your_token' (PowerShell)")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Testing Repo_Cloner_Tool - Single Framework Test")
    logger.info("=" * 60)
    
    # Test with just brewtiful first (smaller test)
    logger.info("")
    logger.info("Step 1: Testing with brewtiful (gld pipeline)...")
    logger.info("")
    
    test_state_brewtiful = MigrationGraphState(
        pipelines_to_migrate=[
            {"name": "gld_maz_sales_test_pipeline"},  # Requires brewtiful only
        ],
        credentials={"GITHUB_TOKEN": github_token}
    )
    
    try:
        logger.info("Cloning brewtiful repository...")
        logger.info("(If this hangs, press Ctrl+C and authorize SAML SSO at: https://github.com/orgs/BrewDat/sso)")
        logger.info("")
        
        result_state = repo_cloner_tool(test_state_brewtiful)
        
        logger.info("")
        logger.info("[SUCCESS] Brewtiful repository cloned!")
        logger.info(f"Repositories cloned: {result_state.repos_cloned}")
        
        # Verify repository exists
        repo_path = Path("cache/brewtiful")
        if repo_path.exists() and (repo_path / ".git").exists():
            logger.info(f"[OK] brewtiful repository verified at {repo_path}")
        else:
            logger.warning(f"[WARNING] brewtiful repository path exists but .git not found")
        
        return 0
        
    except KeyboardInterrupt:
        logger.error("")
        logger.error("[INTERRUPTED] Clone operation was interrupted.")
        logger.error("This might be due to:")
        logger.error("  1. SAML SSO authorization needed - visit: https://github.com/orgs/BrewDat/sso")
        logger.error("  2. Large repository taking time to clone")
        logger.error("  3. Network connectivity issues")
        return 1
    except Exception as e:
        logger.error("")
        logger.error(f"[FAILED] Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())


