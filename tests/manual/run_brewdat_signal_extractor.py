"""
Quick manual runner to test BrewdatSignalExtractor with a local JSON snapshot.

Usage:
    python scripts/run_brewdat_signal_extractor.py --path final_state.json
"""

import argparse
import json
import sys
from pathlib import Path

# Ensure local src is importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from brewbridge.domain.extractor_strategies.brewdat.signal_extractor_pipeline import (
    BrewdatSignalExtractor,
)  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BrewdatSignalExtractor against a JSON file containing raw_artifacts."
    )
    parser.add_argument(
        "--path",
        type=str,
        default="final_state.json",
        help="Path to JSON with a top-level 'raw_artifacts' key (e.g., final_state.json).",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="Indent level for pretty-printing the signal summary.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    json_path = Path(args.path)

    if not json_path.exists():
        sys.exit(f"File not found: {json_path}")

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    raw_artifacts = data.get("raw_artifacts")
    if raw_artifacts is None:
        sys.exit("Input JSON is missing the 'raw_artifacts' key.")

    extractor = BrewdatSignalExtractor()
    summary = extractor.extract(raw_artifacts)

    print(json.dumps(summary, indent=args.indent, ensure_ascii=True))


if __name__ == "__main__":
    main()
