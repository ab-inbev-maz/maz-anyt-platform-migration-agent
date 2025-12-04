"""
Unit tests for brewbridge.infrastructure.observability.mlflow_tracer.

Tests cover:
1. track_node decorator - success and error cases
2. start_pipeline_run - MLflow run management and tagging
3. end_pipeline_run - MLflow run management and status tagging
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from brewbridge.infrastructure.observability.mlflow_tracer import (
    _derive_framework,
    _extract_common_tags_from_state,
    end_pipeline_run,
    start_pipeline_run,
    track_node,
)


class TestDeriveFramework:
    """Test the _derive_framework helper function."""

    def test_brz_maps_to_hopsflow(self) -> None:
        """Test that 'brz' environment type maps to 'hopsflow' framework."""
        assert _derive_framework("brz") == "hopsflow"

    def test_slv_maps_to_hopsflow(self) -> None:
        """Test that 'slv' environment type maps to 'hopsflow' framework."""
        assert _derive_framework("slv") == "hopsflow"

    def test_gld_maps_to_brewtiful(self) -> None:
        """Test that 'gld' environment type maps to 'brewtiful' framework."""
        assert _derive_framework("gld") == "brewtiful"

    def test_none_maps_to_unknown(self) -> None:
        """Test that None environment type maps to 'unknown' framework."""
        assert _derive_framework(None) == "unknown"

    def test_other_maps_to_unknown(self) -> None:
        """Test that unrecognized environment type maps to 'unknown' framework."""
        assert _derive_framework("unknown_env") == "unknown"


class TestExtractCommonTagsFromState:
    """Test the _extract_common_tags_from_state helper function."""

    def test_extracts_all_tags_when_present(self) -> None:
        """Test that all tags are extracted when present in state."""
        state = {
            "environment_type": "brz",
            "pipeline_info": {"pipeline_name": "my_pipeline"},
        }
        tags = _extract_common_tags_from_state(state)
        assert tags == {
            "pipeline": "my_pipeline",
            "environment": "brz",
            "framework": "hopsflow",
        }

    def test_defaults_to_unknown_when_missing(self) -> None:
        """Test that 'unknown' is used when tags are missing from state."""
        state: dict[str, Any] = {}
        tags = _extract_common_tags_from_state(state)
        assert tags == {
            "pipeline": "unknown",
            "environment": "unknown",
            "framework": "unknown",
        }

    def test_handles_none_pipeline_info(self) -> None:
        """Test that None pipeline_info is handled gracefully."""
        state = {"environment_type": "slv", "pipeline_info": None}
        tags = _extract_common_tags_from_state(state)
        assert tags == {
            "pipeline": "unknown",
            "environment": "slv",
            "framework": "hopsflow",
        }


class TestStartPipelineRun:
    """Test the start_pipeline_run function."""

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_starts_new_run_with_correct_name_and_tags(self, mock_mlflow: MagicMock) -> None:
        """Test that start_pipeline_run creates a new MLflow run with appropriate tags."""
        # Arrange
        mock_mlflow.active_run.return_value = None
        mock_run = MagicMock()
        mock_run.info.run_id = "test_run_id_123"
        mock_mlflow.start_run.return_value = mock_run

        state = {
            "environment_type": "gld",
            "pipeline_info": {"pipeline_name": "test_pipeline"},
        }

        # Act
        start_pipeline_run(state)

        # Assert
        mock_mlflow.start_run.assert_called_once_with(run_name="pipeline_test_pipeline")
        mock_mlflow.set_tags.assert_called_once_with(
            {
                "pipeline": "test_pipeline",
                "environment": "gld",
                "framework": "brewtiful",
                "run_scope": "pipeline",
            }
        )

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_ends_existing_run_before_starting_new_one(self, mock_mlflow: MagicMock) -> None:
        """Test that start_pipeline_run ends any existing active run before starting."""
        # Arrange
        mock_existing_run = MagicMock()
        mock_mlflow.active_run.return_value = mock_existing_run
        mock_new_run = MagicMock()
        mock_new_run.info.run_id = "new_run_id"
        mock_mlflow.start_run.return_value = mock_new_run

        state = {"environment_type": "brz", "pipeline_info": {"pipeline_name": "pipe1"}}

        # Act
        start_pipeline_run(state)

        # Assert
        mock_mlflow.end_run.assert_called_once()
        mock_mlflow.start_run.assert_called_once()

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_handles_missing_state_data(self, mock_mlflow: MagicMock) -> None:
        """Test that start_pipeline_run handles state with missing data gracefully."""
        # Arrange
        mock_mlflow.active_run.return_value = None
        mock_run = MagicMock()
        mock_run.info.run_id = "test_run_id"
        mock_mlflow.start_run.return_value = mock_run

        state: dict[str, Any] = {}

        # Act
        start_pipeline_run(state)

        # Assert
        mock_mlflow.start_run.assert_called_once_with(run_name="pipeline_unknown")
        mock_mlflow.set_tags.assert_called_once_with(
            {
                "pipeline": "unknown",
                "environment": "unknown",
                "framework": "unknown",
                "run_scope": "pipeline",
            }
        )


class TestEndPipelineRun:
    """Test the end_pipeline_run function."""

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_ends_run_with_success_status(self, mock_mlflow: MagicMock) -> None:
        """Test that end_pipeline_run tags run with success status and ends it."""
        # Arrange
        mock_mlflow.active_run.return_value = MagicMock()

        # Act
        end_pipeline_run(status="success")

        # Assert
        mock_mlflow.set_tag.assert_called_once_with("status", "success")
        mock_mlflow.end_run.assert_called_once()

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_ends_run_with_error_status(self, mock_mlflow: MagicMock) -> None:
        """Test that end_pipeline_run tags run with error status and ends it."""
        # Arrange
        mock_mlflow.active_run.return_value = MagicMock()

        # Act
        end_pipeline_run(status="error")

        # Assert
        mock_mlflow.set_tag.assert_called_once_with("status", "error")
        mock_mlflow.end_run.assert_called_once()

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_handles_no_active_run(self, mock_mlflow: MagicMock) -> None:
        """Test that end_pipeline_run handles case when no run is active."""
        # Arrange
        mock_mlflow.active_run.return_value = None

        # Act
        end_pipeline_run(status="success")

        # Assert
        mock_mlflow.set_tag.assert_not_called()
        mock_mlflow.end_run.assert_not_called()

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_defaults_to_success_status(self, mock_mlflow: MagicMock) -> None:
        """Test that end_pipeline_run defaults to 'success' status when not specified."""
        # Arrange
        mock_mlflow.active_run.return_value = MagicMock()

        # Act
        end_pipeline_run()

        # Assert
        mock_mlflow.set_tag.assert_called_once_with("status", "success")


class TestTrackNodeDecorator:
    """Test the track_node decorator."""

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    @patch("brewbridge.infrastructure.observability.mlflow_tracer.time.perf_counter")
    def test_logs_elapsed_time_and_success_status(
        self, mock_perf_counter: MagicMock, mock_mlflow: MagicMock
    ) -> None:
        """Test that track_node logs elapsed time and success status for successful function."""
        # Arrange
        mock_perf_counter.side_effect = [0.0, 0.123]  # Start and end times
        mock_mlflow.active_run.return_value = None

        state = {
            "environment_type": "brz",
            "pipeline_info": {"pipeline_name": "test_pipe"},
        }

        @track_node("tool")
        def test_function(state: dict[str, Any]) -> dict[str, Any]:
            return {"result": "success"}

        # Act
        result = test_function(state)

        # Assert
        assert result == {"result": "success"}

        # Verify MLflow calls
        mock_mlflow.start_run.assert_called_once_with(run_name="test_function", nested=False)
        mock_mlflow.log_metric.assert_called_once_with("elapsed_ms", 123.0)

        # Check that set_tags was called with correct arguments
        set_tags_calls = mock_mlflow.set_tags.call_args_list
        assert len(set_tags_calls) == 1
        tags = set_tags_calls[0][0][0]
        assert tags["node_name"] == "test_function"
        assert tags["node_type"] == "tool"
        assert tags["run_scope"] == "node"
        assert tags["pipeline"] == "test_pipe"
        assert tags["environment"] == "brz"
        assert tags["framework"] == "hopsflow"

        # Check that status was set to success
        set_tag_calls = mock_mlflow.set_tag.call_args_list
        assert call("status", "success") in set_tag_calls

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    @patch("brewbridge.infrastructure.observability.mlflow_tracer.time.perf_counter")
    def test_logs_exception_details_and_error_status_on_failure(
        self, mock_perf_counter: MagicMock, mock_mlflow: MagicMock
    ) -> None:
        """Test that track_node logs exception details and error status when function fails."""
        # Arrange
        mock_perf_counter.side_effect = [0.0, 0.050]  # Start and end times
        mock_mlflow.active_run.return_value = None

        state = {"environment_type": "slv"}

        @track_node("agent")
        def failing_function(state: dict[str, Any]) -> dict[str, Any]:
            raise ValueError("Something went wrong")

        # Act & Assert
        with pytest.raises(ValueError, match="Something went wrong"):
            failing_function(state)

        # Verify MLflow calls
        mock_mlflow.start_run.assert_called_once_with(run_name="failing_function", nested=False)
        mock_mlflow.log_metric.assert_called_once_with("elapsed_ms", 50.0)

        # Check that exception details were logged
        set_tag_calls = mock_mlflow.set_tag.call_args_list
        assert call("exception_type", "ValueError") in set_tag_calls
        assert call("exception_message", "Something went wrong") in set_tag_calls
        assert call("status", "error") in set_tag_calls

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_works_with_nested_runs(self, mock_mlflow: MagicMock) -> None:
        """Test that track_node creates nested runs when a parent run is active."""
        # Arrange
        mock_parent_run = MagicMock()
        mock_mlflow.active_run.return_value = mock_parent_run

        state = {"environment_type": "gld"}

        @track_node("human")
        def nested_function(state: dict[str, Any]) -> str:
            return "done"

        # Act
        result = nested_function(state)

        # Assert
        assert result == "done"
        mock_mlflow.start_run.assert_called_once_with(run_name="nested_function", nested=True)

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_extracts_state_from_kwargs(self, mock_mlflow: MagicMock) -> None:
        """Test that track_node can extract state from keyword arguments."""
        # Arrange
        mock_mlflow.active_run.return_value = None

        state = {
            "environment_type": "brz",
            "pipeline_info": {"pipeline_name": "kwarg_test"},
        }

        @track_node("tool")
        def kwarg_function(state: dict[str, Any]) -> str:
            return "ok"

        # Act
        result = kwarg_function(state=state)

        # Assert
        assert result == "ok"
        set_tags_calls = mock_mlflow.set_tags.call_args_list
        tags = set_tags_calls[0][0][0]
        assert tags["pipeline"] == "kwarg_test"

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_works_without_state_argument(self, mock_mlflow: MagicMock) -> None:
        """Test that track_node works when no state is provided."""
        # Arrange
        mock_mlflow.active_run.return_value = None

        @track_node("tool")
        def no_state_function() -> int:
            return 42

        # Act
        result = no_state_function()

        # Assert
        assert result == 42
        set_tags_calls = mock_mlflow.set_tags.call_args_list
        tags = set_tags_calls[0][0][0]
        assert tags["node_name"] == "no_state_function"
        assert tags["node_type"] == "tool"
        assert tags["run_scope"] == "node"
        # State-derived tags should not be present
        assert "pipeline" not in tags
        assert "environment" not in tags
        assert "framework" not in tags

    @patch("brewbridge.infrastructure.observability.mlflow_tracer.mlflow")
    def test_preserves_function_metadata(self, mock_mlflow: MagicMock) -> None:
        """Test that track_node preserves the original function's metadata."""
        # Arrange
        mock_mlflow.active_run.return_value = None

        @track_node("tool")
        def documented_function(state: dict[str, Any]) -> str:
            """This is a documented function."""
            return "result"

        # Assert
        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "This is a documented function."
