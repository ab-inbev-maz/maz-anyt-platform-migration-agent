"""
Lightweight MLflow-based tracing utilities for BrewBridge.

This module exposes:

* `start_pipeline_run(state)` – starts a **top-level MLflow run** for a pipeline.
* `end_pipeline_run(status)` – finishes the pipeline run and tags its final status.
* `@track_node(node_type)` – decorator to wrap Tool/Agent/Human nodes and
  automatically record:
    - elapsed time (metric: `elapsed_ms`)
    - status (`success` / `error`)
    - basic contextual tags (pipeline, environment, framework, node_name, node_type)
    - exception metadata if the node fails

The implementation is intentionally generic and free of business logic so that it
can be reused across any node in the LangGraph.
"""

from __future__ import annotations

import functools
import time
from typing import Any, Callable, Dict, Mapping, Optional

import mlflow

_pipeline_run_id: Optional[str] = None


def _derive_framework(environment_type: Optional[str]) -> str:
    """
    Map an environment_type ('brz' | 'slv' | 'gld' | other) to a framework label.

    This is intentionally simple and can be extended later if needed.
    """
    if environment_type in {"brz", "slv"}:
        return "hopsflow"
    if environment_type == "gld":
        return "brewtiful"
    return "unknown"


def _extract_common_tags_from_state(state: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Extract a minimal, stable set of tags from the graph state.

    The goal is to be useful for observability without leaking the full state.
    """
    environment_type: Optional[str] = state.get("environment_type")
    current_pipeline = state.get("pipeline_info") or {}
    pipeline_name: Optional[str] = current_pipeline.get("pipeline_name")

    return {
        "pipeline": pipeline_name or "unknown",
        "environment": environment_type or "unknown",
        "framework": _derive_framework(environment_type),
    }


def start_pipeline_run(state: Mapping[str, Any]) -> None:
    """
    Start the top-level MLflow run for a single pipeline migration.

    This should be called **once per pipeline invocation**, before executing
    the LangGraph.
    """
    global _pipeline_run_id

    # End any existing active run to avoid accidental nesting at the top level.
    active_run = mlflow.active_run()
    if active_run is not None:
        mlflow.end_run()

    common_tags = _extract_common_tags_from_state(state)
    pipeline_name = common_tags["pipeline"]
    run_name = f"pipeline_{pipeline_name}"

    run = mlflow.start_run(run_name=run_name)
    _pipeline_run_id = run.info.run_id

    # Attach top-level tags so child runs inherit context via UI filters.
    mlflow.set_tags(
        {
            **common_tags,
            "run_scope": "pipeline",
        }
    )


def end_pipeline_run(status: str = "success") -> None:
    """
    End the currently active top-level pipeline run.

    This should be called after the LangGraph finishes (whether success or error).
    """
    global _pipeline_run_id

    if mlflow.active_run() is None:
        _pipeline_run_id = None
        return

    mlflow.set_tag("status", status)
    mlflow.end_run()
    _pipeline_run_id = None


def track_node(node_type: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator factory to trace an individual node (Tool / Agent / Human).

    Usage:
        @track_node("tool")
        def template_creator_node(state: dict) -> dict:
            ...

    The decorator assumes the first positional argument or a `state=` kwarg
    corresponds to the graph state. If no state is provided, node-level tags
    will be limited to node metadata.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            node_name = func.__name__

            # Try to infer the state from arguments.
            state: Optional[Mapping[str, Any]] = None
            if args:
                first_arg = args[0]
                if isinstance(first_arg, Mapping):
                    state = first_arg
            if state is None and "state" in kwargs and isinstance(kwargs["state"], Mapping):
                state = kwargs["state"]

            base_tags: Dict[str, Any] = {
                "node_name": node_name,
                "node_type": node_type,
                "run_scope": "node",
            }
            if state is not None:
                base_tags.update(_extract_common_tags_from_state(state))

            # Ensure we are inside the pipeline run if available.
            # If no pipeline run is active, this still creates a standalone run
            # so that node-level traces are never lost.
            active = mlflow.active_run()
            nested = active is not None

            with mlflow.start_run(run_name=node_name, nested=nested):
                mlflow.set_tags(base_tags)
                status = "success"

                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as exc:  # pragma: no cover - pass-through
                    status = "error"
                    mlflow.set_tag("exception_type", type(exc).__name__)
                    mlflow.set_tag("exception_message", str(exc))
                    raise
                finally:
                    elapsed_ms = (time.perf_counter() - start) * 1000.0
                    mlflow.log_metric("elapsed_ms", elapsed_ms)
                    mlflow.set_tag("status", status)

        return wrapper

    return decorator
