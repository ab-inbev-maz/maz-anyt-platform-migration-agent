"""
MLflow-based observability utilities for BrewBridge.

This module provides:
    - `mlflow_tracer`: helpers to start/end a pipeline run and
      a decorator to trace individual nodes.
    - `event_logger`: helpers to log structured artifacts (YAMLs,
      diffs, CLI output, state snapshots) to the active MLflow run.
"""

from .event_logger import (log_cli_output, log_state_snapshot, log_yaml_after,
                           log_yaml_before, log_yaml_diff)
from .mlflow_tracer import end_pipeline_run, start_pipeline_run, track_node

__all__ = [
    "start_pipeline_run",
    "end_pipeline_run",
    "track_node",
    "log_yaml_before",
    "log_yaml_after",
    "log_yaml_diff",
    "log_cli_output",
    "log_state_snapshot",
]


