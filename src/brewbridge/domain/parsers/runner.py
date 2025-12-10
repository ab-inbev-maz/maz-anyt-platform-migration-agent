from typing import Any, Dict
import yaml
from pathlib import Path
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)

def run_parsers_as_yaml(parser_names: list[str], json_list: Dict[str, Any]) -> None:
    results = {}

    for name in parser_names:
        results[name] = json_list.get(name, {})
        # Convertir a YAML (string)
        yaml_output = yaml.dump(
            results[name],
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=True,
        )
        # Following line might need update when state gets feature to match templates Path
        output_file = Path(f"outputs/{name}_output.yaml")
        # # # # #
        run_parsers_to_file(output_path = output_file, yaml_string = yaml_output)

def run_parsers_to_file(output_path: Path, yaml_string: yaml) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml_string, encoding="utf-8")
    logger.info(f"YAML output written to {output_path}")