"""
YAML utilities for parsing and validating manifest files.
"""

import yaml
from pathlib import Path
from typing import Dict, Any
from pydantic import BaseModel, Field, field_validator

from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import ManifestNotFoundError, ManifestParseError

logger = get_logger(__name__)


class ManifestModel(BaseModel):
    """Pydantic model for validating manifest.yaml structure."""

    pipelines_to_migrate: list = Field(default_factory=list)
    credentials: Dict[str, str] = Field(default_factory=dict)
    region: str = Field(default=None)
    environment: str = Field(default=None)
    repo_name: str = Field(default=None)
    target_repo_name: str = Field(default=None)

    @field_validator("environment")
    def validate_environment(cls, val):
        if val is None:
            return val
        if val not in {"dev", "prod"}:
            raise ValueError(f"Invalid environment '{val}'. Expected 'dev' or 'prod'.")
        return val


def load_manifest(manifest_path: str) -> Dict[str, Any]:
    """
    Load and parse a manifest.yaml file.

    :param manifest_path: Path to the manifest.yaml file
    :return: Parsed manifest as a dictionary
    :raises ManifestNotFoundError: If the file doesn't exist
    :raises ManifestParseError: If the file cannot be parsed or validated
    """
    path = Path(manifest_path)

    if not path.exists():
        logger.error(f"Manifest file not found: {manifest_path}")
        raise ManifestNotFoundError(f"Manifest file not found: {manifest_path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)

        if content is None:
            raise ManifestParseError("Manifest file is empty or invalid")

        # Validate structure using Pydantic
        manifest = ManifestModel(**content)
        logger.info(f"Successfully loaded and validated manifest: {manifest_path}")
        return manifest.model_dump()

    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML: {e}")
        raise ManifestParseError(f"Invalid YAML syntax: {e}")
    except Exception as e:
        logger.error(f"Failed to validate manifest: {e}")
        raise ManifestParseError(f"Manifest validation failed: {e}")
