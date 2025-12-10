"""
SignalExtractorTool for BrewDat 3.0 artifacts.

This tool consumes state.raw_artifacts (produced by extractor_node) and
produces a compact signal summary using BrewdatSignalExtractor. The
summary is stored in state.signal_summary for downstream LLM agents.
"""

from typing import Any, Dict

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.extractor_strategies.brewdat.signal_extractor_strategy import (
    BrewdatSignalExtractor,
)
from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.observability.mlflow_tracer import track_node

logger = get_logger(__name__)


@track_node("tool")
@tool_node
def signal_extractor_node(state: MigrationGraphState) -> MigrationGraphState:
    raw_artifacts: Dict[str, Any] = state.raw_artifacts or {}
    if not raw_artifacts:
        logger.warning("signal_extractor_node: raw_artifacts is empty; skipping signal extraction.")
        return state

    extractor = BrewdatSignalExtractor()
    summary = extractor.extract(raw_artifacts)
    state.signal_summary = summary
    return state
