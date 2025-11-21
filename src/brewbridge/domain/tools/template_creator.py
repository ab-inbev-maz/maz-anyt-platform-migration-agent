import os

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.engineeringstore_cli import (
    EngineeringStoreCLI,
    EngineeringStoreCommand,
)
from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import TemplateCreationError

logger = get_logger(__name__)


@tool_node
def template_creator_node(state: MigrationGraphState) -> MigrationGraphState:
    try:
        env_type = state.environment_type
        schema = state.normalized_schema_v4
        metadata = state.current_pipeline_data

        if env_type not in {"gld", "brz", "slv"}:
            raise TemplateCreationError(f"Invalid environment_type: {env_type}")

        framework = "brewtiful" if env_type == "gld" else "hopsflow"
        output_dir = os.path.join("cache", framework)
        os.makedirs(output_dir, exist_ok=True)

        cli = EngineeringStoreCLI(logger=logger)

        command = (
            ["engineeringstore", "transformation", "--create-template-files"]
            if env_type == "gld"
            else ["engineeringstore", "ingestion", "--create-template-files"]
        )

        es_command = EngineeringStoreCommand(
            command=command, table_type="gold" if env_type == "gld" else env_type, needs_input=True
        )

        zone = schema.get("zone", "maz")
        country = schema.get("country", "co")
        pipeline = metadata.get("pipeline_name", "unknown")
        domain = schema.get("domain", "unknown")
        owner = schema.get("owner", "platform")
        schedule = schema.get("cron", "* * * * *")

        if env_type == "gld":
            prompt = f"""\
{zone}
{zone}
{country}
{domain}
{pipeline}
{schedule}
{pipeline}
{owner}
transformation

promo
yn
"""
        else:
            prompt = f"""\
{zone}
{zone}

{domain}
{pipeline}
{schedule}
{pipeline}
{owner}
blob
sap
sap
sap
sap-secret
yn
"""

        cli.run(es_command, input_text=prompt)

        state.template_path = output_dir
        return state

    except Exception as exc:
        raise TemplateCreationError(str(exc))
