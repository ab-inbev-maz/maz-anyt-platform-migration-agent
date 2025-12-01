"""
Setup and test script to validate environment before running migrations.

This script performs pre-flight checks:
- Validates manifest.yaml exists and is parseable
- Checks for required credentials
- Tests API connectivity (GitHub, ADF, LLMs)

Usage:
    python -m brewbridge.domain.tools.setup_test <path_to_manifest.yaml>
    or
    python src/brewbridge/domain/tools/setup_test.py <path_to_manifest.yaml>
"""
import sys
import os
import dotenv
import importlib.util
from pathlib import Path

# Add src to path (go up from domain/tools to src)
# From src/brewbridge/domain/tools -> up 3 levels to src
src_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(src_path))

from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.logger import get_logger

# Import from 3.0 extractor module (using importlib due to numeric module name)
_extractor_path = src_path / "brewbridge" / "domain" / "tools" / "extractor" / "3.0" / "read_manifest_and_check_api.py"
spec = importlib.util.spec_from_file_location("read_manifest_and_check_api", _extractor_path)
_read_manifest_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_read_manifest_module)
read_manifest_and_check_api = _read_manifest_module.read_manifest_and_check_api

dotenv.load_dotenv()

logger = get_logger("setup_test")


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m brewbridge.domain.tools.setup_test <path_to_manifest.yaml>")
        print("   or: python src/brewbridge/domain/tools/setup_test.py <path_to_manifest.yaml>")
        sys.exit(1)
    
    manifest_path = sys.argv[1]
    
    print("=" * 60)
    print("BrewBridge Setup & Test Tool")
    print("=" * 60)
    print(f"\nManifest path: {manifest_path}")
    print("\nRunning pre-flight checks...\n")
    
    # Create initial state
    initial_state = MigrationGraphState(manifest_path=manifest_path)
    
    try:
        # Run the read_manifest_and_check_api tool
        result_state = read_manifest_and_check_api(initial_state)
        
        # Print results
        print("\n" + "=" * 60)
        print("Setup Test Results")
        print("=" * 60)
        print(f"\n[OK] Manifest loaded: {result_state.manifest_path}")
        print(f"[OK] Pipelines found: {len(result_state.pipelines_to_migrate or [])}")
        print(f"[OK] API Connectivity: {'PASS' if result_state.api_connectivity_ok else 'FAIL'}")
        
        if result_state.credentials:
            print(f"\nCredentials loaded:")
            for key in result_state.credentials.keys():
                value_preview = result_state.credentials[key][:10] + "..." if len(result_state.credentials[key]) > 10 else result_state.credentials[key]
                print(f"   - {key}: {value_preview}")
        
        if result_state.pipelines_to_migrate:
            print(f"\nPipelines to migrate:")
            for i, pipeline in enumerate(result_state.pipelines_to_migrate[:5], 1):
                print(f"   {i}. {pipeline.get('name', 'Unknown')}")
            if len(result_state.pipelines_to_migrate) > 5:
                print(f"   ... and {len(result_state.pipelines_to_migrate) - 5} more")
        
        if result_state.api_connectivity_ok:
            print("\n[SUCCESS] All checks passed! Ready to run migrations.")
            sys.exit(0)
        else:
            print("\n[WARNING] Some connectivity checks failed. Review warnings above.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n[ERROR] Setup test failed: {e}")
        logger.exception("Setup test failed")
        sys.exit(1)


if __name__ == "__main__":
    main()

