from __future__ import annotations

import os

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.extractor_strategies.engineeringstore_input_builder import (
    build_engineeringstore_inputs,
)
from brewbridge.infrastructure.engineeringstore_cli import (
    EngineeringStoreCLI,
    EngineeringStoreCommand,
)
from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.observability import track_node
from brewbridge.utils.exceptions import TemplateCreationError

logger = get_logger(__name__)


@track_node("tool")
@tool_node
def template_creator(state: MigrationGraphState) -> MigrationGraphState:
    try:
        env = state.environment_type
        schema = state.normalized_schema_v4
        metadata = state.pipeline_info

        if env not in {"gld", "brz", "slv"}:
            raise TemplateCreationError(f"Invalid environment_type: {env}")

        framework = "brewtiful" if env == "gld" else "hopsflow"
        output_dir = os.path.join("cache", framework)
        os.makedirs(output_dir, exist_ok=True)

        cli = EngineeringStoreCLI(logger=logger)

        command = (
            ["engineeringstore", "transformation", "--create-template-files"]
            if env == "gld"
            else ["engineeringstore", "ingestion", "--create-template-files"]
        )

        es_command = EngineeringStoreCommand(
            command=command, table_type="gold" if env == "gld" else env, needs_input=True
        )

        prompt = build_engineeringstore_inputs(schema=schema, metadata=metadata, environment=env)

        cli.run(es_command, input_text=prompt)

        state.template_path = output_dir
        return state

    except Exception as exc:
        raise TemplateCreationError(str(exc))
