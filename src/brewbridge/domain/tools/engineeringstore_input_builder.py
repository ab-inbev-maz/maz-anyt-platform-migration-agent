"""
EngineeringStore Input Builder

Builds stdin (interactive input) sequences for the engineeringstore CLI,
used for creating template files for Hopsflow (BRZ/SLV) or Brewtiful (GLD).

Applies Strategy + Factory pattern to keep input logic clean and scalable.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict

# ======================================================
#   Strategy Interface
# ======================================================


class EngineeringStoreInputStrategy(ABC):
    """
    Strategy interface for building STDIN input sequences for engineeringstore.
    """

    @abstractmethod
    def build(self, schema: Dict) -> str:
        pass


# ======================================================
#   Strategy 1: Hopsflow (BRZ / SLV)
# ======================================================


class HopsflowInputStrategy(EngineeringStoreInputStrategy):
    """
    Builds inputs for:
        engineeringstore ingestion --create-template-files
    """

    def build(self, schema: Dict) -> str:
        return (
            "\n".join(
                [
                    schema.get("zone", "maz"),
                    schema.get("country", "co"),
                    schema.get("dataset_name", schema.get("pipeline_name", "unknown_dataset")),
                    schema.get("domain", "unknown_domain"),
                    schema.get("layer", "brz"),  # brz or slv
                    schema.get("owner_team", "platform"),
                    schema.get("schedule_interval", "* * * * *"),
                ]
            )
            + "\n"
        )


# ======================================================
#   Strategy 2: Brewtiful (GLD)
# ======================================================


class BrewtifulInputStrategy(EngineeringStoreInputStrategy):
    """
    Builds inputs for:
        engineeringstore transformation --create-template-files
    """

    def build(self, schema: Dict) -> str:
        return (
            "\n".join(
                [
                    schema.get("zone", "maz"),
                    schema.get("landing_zone", schema.get("zone", "maz")),
                    schema.get("country", "co"),
                    schema.get("domain", "unknown"),
                    schema.get("pipeline_name", "unknown_pipeline"),
                    schema.get("schedule_interval", "* * * * *"),
                    schema.get("task_name", "default_task"),
                    schema.get("owner_team", "platform"),
                    schema.get("scope", "transformation"),
                    schema.get("sub_domain", "general"),
                    schema.get("data_product", "default"),
                    "yes",  # create ACL yaml?
                    "no",  # create Trigger yaml?
                ]
            )
            + "\n"
        )


# ======================================================
#   Factory — selects Strategy based on environment
# ======================================================


class EngineeringStoreInputBuilderFactory:
    """
    Factory that creates the correct input-building strategy
    depending on the environment (brz/slv/gld).
    """

    @staticmethod
    def get(environment: str) -> EngineeringStoreInputStrategy:
        if environment in ("brz", "slv"):
            return HopsflowInputStrategy()
        return BrewtifulInputStrategy()


# ======================================================
#   Public API
# ======================================================


def build_engineeringstore_inputs(schema: Dict, environment: str) -> str:
    """
    Unified entry point for TemplateCreator ToolNode.

    Returns a newline-separated string with all CLI STDIN inputs
    required by engineeringstore.
    """
    strategy = EngineeringStoreInputBuilderFactory.get(environment)
    return strategy.build(schema)
