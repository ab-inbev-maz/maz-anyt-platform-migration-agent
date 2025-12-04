"""
YAML utilities for parsing and validating manifest files.

Expected manifest shape (YAML):

pipeline_info:
  repo_name: BrewDat/brewdat-maz-maz-tech-sap-repo-adf
  trigger_name: tr_slv_maz_tech_metadata_sap_pr0_mx_d_0500
access_groups:
  - AADS_A_Brewdat-ghq-p-ghq-mark-mroi-rw
source_platform: platform_3_0
# Optional:
# quality_repo: brewdat/quality_repo
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field

from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import ManifestNotFoundError, ManifestParseError

logger = get_logger(__name__)


class PipelineInfo(BaseModel):
    repo_name: str
    trigger_name: str


class ManifestModel(BaseModel):
    """Pydantic model for validating manifest.yaml structure."""

    pipeline_info: PipelineInfo
    access_groups: List[str] = Field(default_factory=list)
    source_platform: str = Field(default="platform_3_0")
    quality_repo: Optional[str] = None


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

        manifest = ManifestModel(**content)
        logger.info(f"Successfully loaded and validated manifest: {manifest_path}")
        return manifest.model_dump()

    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML: {e}")
        raise ManifestParseError(f"Invalid YAML syntax: {e}")
    except Exception as e:
        logger.error(f"Failed to validate manifest: {e}")
        raise ManifestParseError(f"Manifest validation failed: {e}")
