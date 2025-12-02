import json
import yaml
from pathlib import Path
from .factory import ParserFactory
"""
JSON Input handling might be refactored depending on normalized_schema final structure.

SHARED_JSON_PATH = Path(__file__).resolve().parents[5] / "inputs" / "samples" /  "normalized_ingestion_logistics_single_task.json"
"""
def load_shared_json() -> dict:
    with open(SHARED_JSON_PATH, "r") as f:
        return json.load(f)
"""
Below code should not change if parser strategy remains the same.
"""
def run_parsers_as_yaml(parser_names: list[str]) -> str:
    json_data = load_shared_json()
    results = {}

    for name in parser_names:
        parser = ParserFactory.create(name)
        results[name] = parser.parse(json_data)

    # Convertir a YAML (string)
    yaml_output = yaml.dump(
        results,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True
    )

    return yaml_output

def run_parsers_to_file(parser_names: list[str], output_path: Path):
    yaml_string = run_parsers_as_yaml(parser_names)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml_string, encoding="utf-8")

    return output_path