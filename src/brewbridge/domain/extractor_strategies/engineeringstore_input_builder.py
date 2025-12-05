from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict


class EngineeringStoreInputStrategy(ABC):
    @abstractmethod
    def build(self, schema: Dict, metadata: Dict) -> str:
        pass


class HopsflowBRZInputStrategy(EngineeringStoreInputStrategy):
    """
    engineeringstore ingestion --create-template-files
    BRZ variant: no transformations
    """

    def build(self, schema: Dict, metadata: Dict) -> str:
        zone = schema.get("zone", "maz")
        landing_zone = schema.get("landing_zone", zone)
        country = schema.get("country", "")
        domain = schema.get("domain", "unknown")
        pipeline = metadata.get("pipeline_name", "unknown_pipeline")
        schedule = schema.get("schedule", "* * * * *")
        table_name = schema.get("table_name", "raw_table")
        owner = schema.get("owner", "platform")
        connector = schema.get("connector", "blob")
        source_system = schema.get("source_system", "sap")
        source_entity = schema.get("source_entity", "sap")
        target_entity = schema.get("target_entity", "sap")
        connection_id = schema.get("connection_id", "sap-secret")
        acl = schema.get("acl", "n")

        return f"""\
{zone}
{landing_zone}
{country}
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
n{acl}
"""


class HopsflowSLVInputStrategy(EngineeringStoreInputStrategy):
    """
    engineeringstore ingestion --create-template-files
    SLV variant: supports transformations
    """

    def build(self, schema: Dict, metadata: Dict) -> str:
        zone = schema.get("zone", "maz")
        landing_zone = schema.get("landing_zone", zone)
        country = schema.get("country", "")
        domain = schema.get("domain", "unknown")
        pipeline = metadata.get("pipeline_name", "unknown_pipeline")
        schedule = schema.get("schedule", "* * * * *")
        table_name = schema.get("table_name", "raw_table")
        owner = schema.get("owner", "platform")
        connector = schema.get("connector", "blob")
        source_system = schema.get("source_system", "sap")
        source_entity = schema.get("source_entity", "sap")
        target_entity = schema.get("target_entity", "sap")
        connection_id = schema.get("connection_id", "sap-secret")
        transformations = schema.get("transformations", "y")
        acl = schema.get("acl", "n")

        return f"""\
{zone}
{landing_zone}
{country}
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
{transformations}{acl}
"""


class BrewtifulGLDInputStrategy(EngineeringStoreInputStrategy):
    """
    engineeringstore transformation --create-template-files
    GOLD transformation prompts
    """

    def build(self, schema: Dict, metadata: Dict) -> str:
        zone = schema.get("zone", "maz")
        landing_zone = schema.get("landing_zone", zone)
        country = schema.get("country", "mz")
        domain = schema.get("domain", "unknown")
        pipeline = metadata.get("pipeline_name", "unknown_pipeline")
        schedule = schema.get("schedule", "* * * * *")
        table_name = schema.get("table_name", "feature_table")
        owner = schema.get("owner", "platform")
        table_scope = schema.get("table_scope", "transformation")
        data_product_subdomain = schema.get("data_product_subdomain", "default")
        acl = schema.get("acl", "n")
        trigger = schema.get("trigger", "n")

        return f"""\
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


class EngineeringStoreInputBuilderFactory:
    @staticmethod
    def get(environment: str) -> EngineeringStoreInputStrategy:
        if environment == "brz":
            return HopsflowBRZInputStrategy()
        if environment == "slv":
            return HopsflowSLVInputStrategy()
        return BrewtifulGLDInputStrategy()


def build_engineeringstore_inputs(schema: Dict, metadata: Dict, environment: str) -> str:
    strategy = EngineeringStoreInputBuilderFactory.get(environment)
    return strategy.build(schema, metadata)


def build_validation_command_args(environment_type: str) -> list[str]:
    """
    Return the CLI arguments to validate dags for the given environment.

    gld -> transformation --validate-dags
    brz/slv -> ingestion --validate-dags
    """
    if environment_type == "gld":
        return ["engineeringstore", "transformation", "--validate-dags"]

    if environment_type in {"brz", "slv"}:
        return ["engineeringstore", "ingestion", "--validate-dags"]

    raise ValueError(f"Invalid environment_type '{environment_type}'. Expected brz, slv, or gld.")


def combine_cli_output(stdout: str, stderr: str) -> str:
    """Combine stdout and stderr into a single string, preserving both when present."""
    if stdout and stderr:
        return f"{stdout}\n{stderr}"
    return stdout or stderr
