import os

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.engineeringstore_cli import (
    EngineeringStoreCLI, EngineeringStoreCommand)
from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import TemplateCreationError

logger = get_logger(__name__)


@tool_node
def template_creator_node(state: MigrationGraphState) -> MigrationGraphState:
    try:
        env_type = state.environment_type
        schema = state.normalized_schema_v4
        metadata = state.current_pipeline_data

        if env_type not in {"gld", "brz", "brz/slv"}:
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
        landing_zone = schema.get("landing_zone", schema.get("zone", "maz"))
        country = schema.get("country", "co")
        pipeline = metadata.get("pipeline_name", "unknown")
        domain = schema.get("domain", "unknown")
        owner = schema.get("owner", "platform")
        schedule = schema.get("schedule", "* * * * *")
        table_scope = schema.get("table_scope", "default_task")
        table_name = schema.get("table_name", "feature_store")
        data_product_subdomain = schema.get("data_product_subdomain", "default")
        acl = schema.get("acl", "y")
        trigger = schema.get("trigger", "n")
        connector = schema.get("connector", "unknown_connector")
        source_system = schema.get("source_system", "unknown_source_system")
        source_entity = schema.get("source_entity", "unknown_source_entity")
        target_entity = schema.get("target_entity", "unknown_target_entity")
        connection_id = schema.get("connection_id", "unknown_connection_id")
        transformations = schema.get("transformations", "none")

        if env_type == "gld":
            prompt = f"""\
{zone}
{landing_zone}
{country}
{domain}
{pipeline}
{schedule}
{table_name}
{owner}
{table_scope}

{data_product_subdomain}
{acl}{trigger}
"""
        else:
            prompt = f"""\
{zone}
{landing_zone}

{domain}
{pipeline}
{schedule}
{table_name}
{owner}
{connector}
{source_system}
{source_entity}
{target_entity}
{connection_id}
{transformations if env_type == "brz/slv" else "n"}{acl}
"""

        cli.run(es_command, input_text=prompt)

        state.template_path = output_dir
        return state

    except Exception as exc:
        raise TemplateCreationError(str(exc))
