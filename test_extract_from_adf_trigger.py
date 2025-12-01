"""
Test script for extract_artifacts_from_tables_tool.

Tests extraction of Python code and quality governance YAML for table items.
"""
import os
import sys
import json
from pathlib import Path
import dotenv
import importlib.util

# Load environment variables from .env file if it exists
dotenv.load_dotenv()

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.logger import get_logger

logger = get_logger("test_extract_from_adf_trigger")

# Import extract_artifacts_from_tables_tool using importlib (since 3.0 directory name starts with number)
_extractor_path = src_path / "brewbridge" / "domain" / "tools" / "extractor" / "3.0" / "extract_artifacts_from_tables_tool.py"
spec = importlib.util.spec_from_file_location("extract_artifacts_from_tables_tool", _extractor_path)
_extractor_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_extractor_module)
extract_artifacts_from_tables_tool = _extractor_module.extract_artifacts_from_tables_tool


def main():
    """Test extract_from_adf_trigger_tool with example trigger JSON."""
    
    # Get GitHub token from environment
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        logger.error("GITHUB_TOKEN not found!")
        logger.error("Please set it in one of these ways:")
        logger.error("  1. Create a .env file with: GITHUB_TOKEN=your_token")
        logger.error("  2. Set environment variable: $env:GITHUB_TOKEN='your_token' (PowerShell)")
        logger.error("  3. Set environment variable: export GITHUB_TOKEN='your_token' (Bash)")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Testing extract_artifacts_from_tables_tool")
    logger.info("=" * 60)
    
    # Example table items (pre-parsed by another component)
    # These would normally come from another developer's ADF trigger parser
    table_items = [
        {
            "target_table": "mx_dd07t",
            "adb_notebook_path": "//brewdat-maz-maz-tech-sap-repo-adb/data_integration/sap_pr0/load_slv_generic_parquet",
            "zone": "maz",
            "domain": "tech",
            "subdomain": "sap_pr0",
            "source_system": "sap_pr0",
            "target_database": "slv_maz_tech_sap_pr0",
            "target_business_subdomain": "metadata",
            "load_to_silver": {}  # Empty dict indicates load_to_silver exists
        },
        {
            "target_table": "mx_dd04l",
            "adb_notebook_path": "//brewdat-maz-maz-tech-sap-repo-adb/data_integration/sap_pr0/load_slv_generic_parquet",
            "zone": "maz",
            "domain": "tech",
            "subdomain": "sap_pr0",
            "source_system": "sap_pr0",
            "target_database": "slv_maz_tech_sap_pr0",
            "target_business_subdomain": "metadata",
            "load_to_silver": {}  # Empty dict indicates load_to_silver exists
        }
    ]
    
    logger.info(f"Testing with {len(table_items)} table items")
    logger.info("")
    
    try:
        # Create initial state with table items (pre-parsed by another component)
        initial_state = MigrationGraphState(
            credentials={"GITHUB_TOKEN": github_token},
            current_pipeline_data={
                "table_items": table_items,
                "branch": "main",
                "extract_quality_governance": True
            }
        )
        
        logger.info("Initial state created:")
        logger.info(f"  - credentials: {'✓' if initial_state.credentials else '✗'}")
        logger.info(f"  - current_pipeline_data: {'✓' if initial_state.current_pipeline_data else '✗'}")
        logger.info(f"  - raw_artifacts_3_0: {initial_state.raw_artifacts_3_0}")
        logger.info("")
        
        # Run the tool
        logger.info("Running extract_artifacts_from_tables_tool...")
        result_state = extract_artifacts_from_tables_tool(initial_state)
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("[SUCCESS] Tool executed successfully!")
        logger.info("=" * 60)
        
        # Verify results in state
        if result_state.raw_artifacts_3_0:
            logger.info("✓ raw_artifacts_3_0 is populated")
            artifacts = result_state.raw_artifacts_3_0
            
            # Check summary
            summary = artifacts.get("extraction_summary", {})
            logger.info(f"  - Total items: {summary.get('total_items', 0)}")
            logger.info(f"  - Successful extractions: {summary.get('successful_extractions', 0)}")
            logger.info(f"  - Failed extractions: {summary.get('failed_extractions', 0)}")
            
            # Check items
            items = artifacts.get("table_items", [])
            logger.info(f"  - Extracted items: {len(items)}")
            
            for i, item in enumerate(items, 1):
                target_table = item.get("target_table")
                notebook_code_len = len(item.get("notebook_code", ""))
                quality_yaml_len = len(item.get("quality_governance_yaml", "")) if item.get("quality_governance_yaml") else 0
                
                logger.info(f"    Item {i}: {target_table}")
                logger.info(f"      - notebook_code: {notebook_code_len} characters")
                logger.info(f"      - quality_governance_yaml: {quality_yaml_len} characters {'✓' if quality_yaml_len > 0 else '✗'}")
            
            # Save state to JSON for inspection
            cache_dir = Path("cache/extracted_code")
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            state_file = cache_dir / "state_from_table_items.json"
            logger.info("")
            logger.info(f"Saving state to: {state_file}")
            
            # Convert state to dict and serialize (sanitize credentials)
            state_dict = result_state.model_dump()
            
            # Remove sensitive credentials before saving
            if state_dict.get("credentials"):
                sanitized_creds = {}
                for key, value in state_dict["credentials"].items():
                    if "token" in key.lower() or "secret" in key.lower() or "password" in key.lower():
                        sanitized_creds[key] = "***REDACTED***"
                    else:
                        sanitized_creds[key] = value
                state_dict["credentials"] = sanitized_creds
            
            
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(state_dict, f, indent=2, ensure_ascii=False)
            
            logger.info(f"State saved successfully!")
        else:
            logger.error("✗ raw_artifacts_3_0 is not populated!")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 60)
        logger.error("[FAILED] Test failed!")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())

