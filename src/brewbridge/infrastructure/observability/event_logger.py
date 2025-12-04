"""
Helpers to log structured artifacts to the active MLflow run.

These utilities are intentionally thin wrappers around MLflow's artifact API.
They do not depend on any business logic and can be used from any node that
has an active MLflow run (typically via the `track_node` decorator).
"""

from __future__ import annotations

import json
from typing import Any, Mapping

import mlflow


def log_yaml_before(content: str, artifact_path: str = "yaml_before.yaml") -> None:
    """
    Log the "before" version of a YAML artifact for a node.

    Parameters
    ----------
    content:
        Raw YAML string to store.
    artifact_path:
        Relative path under the current run's artifact root.
    """
    mlflow.log_text(content, artifact_path)


def log_yaml_after(content: str, artifact_path: str = "yaml_after.yaml") -> None:
    """
    Log the "after" version of a YAML artifact for a node.

    Parameters
    ----------
    content:
        Raw YAML string to store.
    artifact_path:
        Relative path under the current run's artifact root.
    """
    mlflow.log_text(content, artifact_path)


def log_yaml_diff(diff: Mapping[str, Any], artifact_path: str = "yaml_diff.json") -> None:
    """
    Log a structured diff between two YAML versions as JSON.

    Parameters
    ----------
    diff:
        Arbitrary JSON-serializable diff structure.
    artifact_path:
        Relative path under the current run's artifact root.
    """
    mlflow.log_text(json.dumps(diff, ensure_ascii=False, indent=2), artifact_path)


def log_cli_output(stdout: str | None, stderr: str | None) -> None:
    """
    Log CLI stdout/stderr as separate artifacts in the current run.

    Parameters
    ----------
    stdout:
        Standard output text from the CLI, if any.
    stderr:
        Standard error text from the CLI, if any.
    """
    if stdout:
        mlflow.log_text(stdout, "cli_stdout.txt")
    if stderr:
        mlflow.log_text(stderr, "cli_stderr.txt")


def log_state_snapshot(state: Mapping[str, Any], label: str = "state_snapshot") -> None:
    """
    Log a compact JSON snapshot of the most relevant parts of the graph state.

    This intentionally avoids logging the entire state to keep artifacts small
    and focused.

    Parameters
    ----------
    state:
        The full graph state mapping.
    label:
        A short label that will be included in the artifact filename.
    """
    # Only keep a subset of keys that are useful for debugging at the node level.
    keys_of_interest = [
        "environment_type",
        "pipeline_info",
        "normalized_schema_v4",
        "pipeline_template",
        "transform_template",
        "notebook_template",
    ]

    snapshot: dict[str, Any] = {key: state.get(key) for key in keys_of_interest if key in state}

    artifact_path = f"{label}.json"
    mlflow.log_text(json.dumps(snapshot, ensure_ascii=False, indent=2), artifact_path)
