import argparse
import os
import sys
from io import BytesIO

import dotenv
from langchain_core.runnables.graph import MermaidDrawMethod
from PIL import Image

from brewbridge.core.graph_builder import MigrationGraphBuilder
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.github_client import GitHubClient
from brewbridge.utils.exceptions import ManifestNotFoundError, ManifestParseError
from brewbridge.utils.yaml_utils import load_manifest

# Import read_manifest_and_check_api tool
import importlib.util
from pathlib import Path

_extractor_path = Path(__file__).parent / "domain" / "tools" / "extractor" / "3.0" / "read_manifest_and_check_api.py"
spec = importlib.util.spec_from_file_location("read_manifest_and_check_api", _extractor_path)
_read_manifest_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_read_manifest_module)
read_manifest_and_check_api = _read_manifest_module.read_manifest_and_check_api

dotenv.load_dotenv()


def main():
    logger = get_logger("brewbridge")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="BrewBridge Migration Agent - Automate data pipeline migrations from Platform 3.0 to Platform 4.0"
    )
    parser.add_argument(
        "--manifest",
        "-m",
        type=str,
        help="Path to the manifest.yaml file",
        default=None
    )
    parser.add_argument(
        "manifest_path",
        nargs="?",
        type=str,
        help="Path to the manifest.yaml file (positional argument, alternative to --manifest)",
        default=None
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode (generates migration flow diagram)",
        default=False
    )
    parser.add_argument(
        "--test",
        "--setup-test",
        action="store_true",
        help="Run setup test only (pre-flight checks without migration)",
        default=False
    )
    parser.add_argument(
        "--test-github-only",
        action="store_true",
        help="Test GitHub token connectivity only",
        default=False
    )
    
    args = parser.parse_args()
    
    # Get manifest path from command line argument or environment variable
    manifest_path = args.manifest or args.manifest_path or os.getenv("MANIFEST_PATH")
    
    if not manifest_path:
        parser.print_help()
        logger.error("\nManifest path not provided.")
        logger.error("Usage: brewbridge --manifest <path_to_manifest.yaml>")
        logger.error("   or: brewbridge <path_to_manifest.yaml>")
        logger.error("   or: set MANIFEST_PATH environment variable")
        sys.exit(1)
    
    # If --test-github-only flag is set, test only GitHub connectivity
    if args.test_github_only:
        logger.info("=" * 60)
        logger.info("GitHub Token Test")
        logger.info("=" * 60)
        
        # Load manifest to get credentials
        try:
            manifest_data = load_manifest(manifest_path)
            manifest_creds = manifest_data.get("credentials", {})
        except Exception as e:
            logger.warning(f"Could not load manifest: {e}. Using environment variables only.")
            manifest_creds = {}
        
        # Get GitHub token from env or manifest
        github_token = os.getenv("GITHUB_TOKEN") or manifest_creds.get("GITHUB_TOKEN")
        
        if not github_token:
            logger.error("GITHUB_TOKEN not found in environment variables or manifest.")
            logger.error("Please set GITHUB_TOKEN environment variable or add it to manifest.yaml")
            sys.exit(1)
        
        logger.info(f"Testing GitHub token: {github_token[:10]}...")
        
        # Test GitHub connectivity
        try:
            client = GitHubClient(token=github_token)
            if client.ping():
                logger.info("[SUCCESS] GitHub token is valid and API is accessible!")
                sys.exit(0)
            else:
                logger.error("[FAIL] GitHub API ping failed. Token may be invalid or expired.")
                sys.exit(1)
        except Exception as e:
            logger.error(f"[FAIL] GitHub connectivity test failed: {e}")
            sys.exit(1)
    
    # Step 1: Pre-flight check - Read manifest and validate API connectivity
    logger.info("=" * 60)
    logger.info("Running pre-flight checks...")
    logger.info("=" * 60)
    
    initial_state = MigrationGraphState(manifest_path=manifest_path)
    
    try:
        initial_state = read_manifest_and_check_api(initial_state)
    except (ManifestNotFoundError, ManifestParseError) as e:
        logger.error(f"Pre-flight check failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pre-flight check failed with unexpected error: {e}")
        sys.exit(1)
    
    # Check API connectivity status
    if not initial_state.api_connectivity_ok:
        logger.warning("=" * 60)
        logger.warning("WARNING: Some API connectivity checks failed!")
        logger.warning("Migration may encounter issues. Continue anyway?")
        logger.warning("=" * 60)
        # For now, we'll continue but log the warning
        # In production, you might want to add a prompt here or exit
    
    logger.info(f"Pre-flight check completed. Found {len(initial_state.pipelines_to_migrate or [])} pipelines to migrate.")
    
    # If --test flag is set, run setup test only and exit
    if args.test:
        logger.info("=" * 60)
        logger.info("Setup Test Results")
        logger.info("=" * 60)
        logger.info(f"[OK] Manifest loaded: {initial_state.manifest_path}")
        logger.info(f"[OK] Pipelines found: {len(initial_state.pipelines_to_migrate or [])}")
        logger.info(f"[OK] API Connectivity: {'PASS' if initial_state.api_connectivity_ok else 'FAIL'}")
        
        if initial_state.credentials:
            logger.info("Credentials loaded:")
            for key in initial_state.credentials.keys():
                value_preview = initial_state.credentials[key][:10] + "..." if len(initial_state.credentials[key]) > 10 else initial_state.credentials[key]
                logger.info(f"   - {key}: {value_preview}")
        
        if initial_state.pipelines_to_migrate:
            logger.info("Pipelines to migrate:")
            for i, pipeline in enumerate(initial_state.pipelines_to_migrate[:5], 1):
                logger.info(f"   {i}. {pipeline.get('name', 'Unknown')}")
            if len(initial_state.pipelines_to_migrate) > 5:
                logger.info(f"   ... and {len(initial_state.pipelines_to_migrate) - 5} more")
        
        if initial_state.api_connectivity_ok:
            logger.info("[SUCCESS] All checks passed! Ready to run migrations.")
            sys.exit(0)
        else:
            logger.warning("[WARNING] Some connectivity checks failed. Review warnings above.")
            sys.exit(1)
    
    # Step 2: Build and run migration graph
    # If manifest was provided, use pipeline data from manifest
    # Otherwise, use the default test state (for backward compatibility)
    if initial_state.pipelines_to_migrate and len(initial_state.pipelines_to_migrate) > 0:
        first_pipeline = initial_state.pipelines_to_migrate[0]
        logger.info(f"Processing first pipeline: {first_pipeline.get('name', 'Unknown')}")
        
        # Extract environment_type from pipeline if available
        if 'environment_type' in first_pipeline:
            initial_state.environment_type = first_pipeline['environment_type']
            logger.info(f"Environment type from manifest: {initial_state.environment_type}")
        
        # Extract normalized_schema_v4 from pipeline if available
        if 'normalized_schema_v4' in first_pipeline:
            initial_state.normalized_schema_v4 = first_pipeline['normalized_schema_v4']
            logger.info("Schema data loaded from manifest")
        
        # Set current_pipeline_data for the migration flow
        initial_state.current_pipeline_data = first_pipeline
    else:
        # Fallback to previous initial state structure (for testing/backward compatibility)
        logger.info("No pipelines in manifest, using default test state")
        initial_state.environment_type = "brz"
        initial_state.normalized_schema_v4 = {
            "zone": "maz",
            "landing_zone": "maz",
            "domain": "logistics",
            "pipeline": "test_ingestion_x",
            "schedule": "* * 2 * *",
            "table_name": "raw_logistics_orders",
            "owner": "platform",
            "connector": "blob",
            "source_system": "sap-test",
            "source_entity": "sap-test",
            "target_entity": "sap-test",
            "connection_id": "sap-test-secret",
            "transformations": "",
            "acl": "yn"
        }
        initial_state.current_pipeline_data = {
            "pipeline_name": "test_ingestion_x"
        }
    
    # Alternative initial state for gold environment (commented out, preserved for reference)
    # initial_state = {
    #     "environment_type": "gld",
    #     "normalized_schema_v4": {
    #         "zone": "maz",
    #         "landing_zone": "maz",
    #         "country": "co",
    #         "domain": "sales",
    #         "owner": "platform",
    #         "schedule": "* * 2 * *",
    #         "table_scope": "transformation",
    #         "data_product_subdomain": "promo",
    #         "table_name": "test_table",
    #         "acl": "y",
    #         "trigger": "n"
    #     },
    #     "current_pipeline_data": {"pipeline_name": "test_pipeline_x"},
    # }

    # Build migration graph
    builder = MigrationGraphBuilder(logger=logger).build()
    runnable = builder.compile()

    if args.debug or os.environ.get("DEBUG") == "true":
        png_bytes = runnable.get_graph().draw_mermaid_png(
            draw_method=MermaidDrawMethod.API, output_file_path="migration_flow.png"
        )

        img = Image.open(BytesIO(png_bytes))
        img.show()

    # Run migration flow
    logger.info("=" * 60)
    logger.info("Starting migration flow...")
    logger.info("=" * 60)
    
    final_state = runnable.invoke(initial_state)
    final_state_obj = MigrationGraphState(**final_state)

    logger.info("=" * 60)
    logger.info("Migration completed!")
    logger.info(f"Final state: {final_state_obj}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
