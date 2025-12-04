"""
Unit tests for brewbridge.infrastructure.observability.event_logger.

Tests cover:
1. log_cli_output - logs stdout and stderr as separate artifacts
2. log_state_snapshot - filters and logs relevant keys from graph state
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from brewbridge.infrastructure.observability.event_logger import (
    log_cli_output,
    log_state_snapshot,
    log_yaml_after,
    log_yaml_before,
    log_yaml_diff,
)


class TestLogYamlBefore:
    """Test the log_yaml_before function."""

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_yaml_with_default_path(self, mock_mlflow: MagicMock) -> None:
        """Test that log_yaml_before logs YAML content with default artifact path."""
        # Arrange
        yaml_content = "key: value\nfoo: bar"

        # Act
        log_yaml_before(yaml_content)

        # Assert
        mock_mlflow.log_text.assert_called_once_with(yaml_content, "yaml_before.yaml")

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_yaml_with_custom_path(self, mock_mlflow: MagicMock) -> None:
        """Test that log_yaml_before logs YAML content with custom artifact path."""
        # Arrange
        yaml_content = "key: value"
        custom_path = "custom/before.yaml"

        # Act
        log_yaml_before(yaml_content, artifact_path=custom_path)

        # Assert
        mock_mlflow.log_text.assert_called_once_with(yaml_content, custom_path)


class TestLogYamlAfter:
    """Test the log_yaml_after function."""

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_yaml_with_default_path(self, mock_mlflow: MagicMock) -> None:
        """Test that log_yaml_after logs YAML content with default artifact path."""
        # Arrange
        yaml_content = "updated: content"

        # Act
        log_yaml_after(yaml_content)

        # Assert
        mock_mlflow.log_text.assert_called_once_with(yaml_content, "yaml_after.yaml")

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_yaml_with_custom_path(self, mock_mlflow: MagicMock) -> None:
        """Test that log_yaml_after logs YAML content with custom artifact path."""
        # Arrange
        yaml_content = "updated: content"
        custom_path = "custom/after.yaml"

        # Act
        log_yaml_after(yaml_content, artifact_path=custom_path)

        # Assert
        mock_mlflow.log_text.assert_called_once_with(yaml_content, custom_path)


class TestLogYamlDiff:
    """Test the log_yaml_diff function."""

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_diff_as_json_with_default_path(self, mock_mlflow: MagicMock) -> None:
        """Test that log_yaml_diff logs diff as JSON with default artifact path."""
        # Arrange
        diff = {"added": ["new_key"], "removed": ["old_key"], "changed": {"key": "value"}}

        # Act
        log_yaml_diff(diff)

        # Assert
        expected_json = json.dumps(diff, ensure_ascii=False, indent=2)
        mock_mlflow.log_text.assert_called_once_with(expected_json, "yaml_diff.json")

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_diff_with_custom_path(self, mock_mlflow: MagicMock) -> None:
        """Test that log_yaml_diff logs diff with custom artifact path."""
        # Arrange
        diff = {"changes": ["field1", "field2"]}
        custom_path = "diffs/custom_diff.json"

        # Act
        log_yaml_diff(diff, artifact_path=custom_path)

        # Assert
        expected_json = json.dumps(diff, ensure_ascii=False, indent=2)
        mock_mlflow.log_text.assert_called_once_with(expected_json, custom_path)

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_handles_unicode_in_diff(self, mock_mlflow: MagicMock) -> None:
        """Test that log_yaml_diff properly handles unicode characters."""
        # Arrange
        diff = {"message": "Test with unicode: 你好"}

        # Act
        log_yaml_diff(diff)

        # Assert
        expected_json = json.dumps(diff, ensure_ascii=False, indent=2)
        mock_mlflow.log_text.assert_called_once_with(expected_json, "yaml_diff.json")


class TestLogCliOutput:
    """Test the log_cli_output function."""

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_stdout_only(self, mock_mlflow: MagicMock) -> None:
        """Test that log_cli_output logs only stdout when stderr is None."""
        # Arrange
        stdout = "Command executed successfully\nOutput line 2"
        stderr = None

        # Act
        log_cli_output(stdout, stderr)

        # Assert
        mock_mlflow.log_text.assert_called_once_with(stdout, "cli_stdout.txt")

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_stderr_only(self, mock_mlflow: MagicMock) -> None:
        """Test that log_cli_output logs only stderr when stdout is None."""
        # Arrange
        stdout = None
        stderr = "Error: Something went wrong\nStack trace..."

        # Act
        log_cli_output(stdout, stderr)

        # Assert
        mock_mlflow.log_text.assert_called_once_with(stderr, "cli_stderr.txt")

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_both_stdout_and_stderr(self, mock_mlflow: MagicMock) -> None:
        """Test that log_cli_output logs both stdout and stderr as separate artifacts."""
        # Arrange
        stdout = "Command output"
        stderr = "Warning: deprecated feature"

        # Act
        log_cli_output(stdout, stderr)

        # Assert
        assert mock_mlflow.log_text.call_count == 2
        calls = mock_mlflow.log_text.call_args_list
        assert call(stdout, "cli_stdout.txt") in calls
        assert call(stderr, "cli_stderr.txt") in calls

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_handles_empty_strings(self, mock_mlflow: MagicMock) -> None:
        """Test that log_cli_output treats empty strings as falsy and doesn't log them."""
        # Arrange
        stdout = ""
        stderr = ""

        # Act
        log_cli_output(stdout, stderr)

        # Assert
        mock_mlflow.log_text.assert_not_called()

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_handles_both_none(self, mock_mlflow: MagicMock) -> None:
        """Test that log_cli_output handles case when both stdout and stderr are None."""
        # Arrange
        stdout = None
        stderr = None

        # Act
        log_cli_output(stdout, stderr)

        # Assert
        mock_mlflow.log_text.assert_not_called()

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_multiline_output(self, mock_mlflow: MagicMock) -> None:
        """Test that log_cli_output correctly logs multiline output."""
        # Arrange
        stdout = "Line 1\nLine 2\nLine 3"
        stderr = "Error line 1\nError line 2"

        # Act
        log_cli_output(stdout, stderr)

        # Assert
        calls = mock_mlflow.log_text.call_args_list
        assert call(stdout, "cli_stdout.txt") in calls
        assert call(stderr, "cli_stderr.txt") in calls


class TestLogStateSnapshot:
    """Test the log_state_snapshot function."""

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_filters_and_logs_relevant_keys(self, mock_mlflow: MagicMock) -> None:
        """Test that log_state_snapshot filters and logs only relevant keys from state."""
        # Arrange
        state = {
            "environment_type": "brz",
            "pipeline_info": {"pipeline_name": "test_pipeline"},
            "normalized_schema_v4": {"tables": ["table1", "table2"]},
            "pipeline_template": {"template": "content"},
            "irrelevant_key": "should not be logged",
            "another_ignored_key": 12345,
        }

        # Act
        log_state_snapshot(state)

        # Assert
        mock_mlflow.log_text.assert_called_once()
        logged_json = mock_mlflow.log_text.call_args[0][0]
        logged_path = mock_mlflow.log_text.call_args[0][1]

        snapshot = json.loads(logged_json)
        assert "environment_type" in snapshot
        assert "pipeline_info" in snapshot
        assert "normalized_schema_v4" in snapshot
        assert "pipeline_template" in snapshot
        assert "irrelevant_key" not in snapshot
        assert "another_ignored_key" not in snapshot
        assert logged_path == "state_snapshot.json"

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_uses_custom_label(self, mock_mlflow: MagicMock) -> None:
        """Test that log_state_snapshot uses custom label in artifact filename."""
        # Arrange
        state = {"environment_type": "slv"}
        custom_label = "before_transformation"

        # Act
        log_state_snapshot(state, label=custom_label)

        # Assert
        logged_path = mock_mlflow.log_text.call_args[0][1]
        assert logged_path == "before_transformation.json"

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_handles_empty_state(self, mock_mlflow: MagicMock) -> None:
        """Test that log_state_snapshot handles empty state gracefully."""
        # Arrange
        state: dict[str, Any] = {}

        # Act
        log_state_snapshot(state)

        # Assert
        mock_mlflow.log_text.assert_called_once()
        logged_json = mock_mlflow.log_text.call_args[0][0]
        snapshot = json.loads(logged_json)
        assert snapshot == {}

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_only_includes_keys_present_in_state(self, mock_mlflow: MagicMock) -> None:
        """Test that log_state_snapshot only includes keys that are present in state."""
        # Arrange
        state = {
            "environment_type": "gld",
            "transform_template": {"config": "data"},
            "unrelated_key": "value",
        }

        # Act
        log_state_snapshot(state)

        # Assert
        logged_json = mock_mlflow.log_text.call_args[0][0]
        snapshot = json.loads(logged_json)
        # Only keys from keys_of_interest that are in state should be present
        assert "environment_type" in snapshot
        assert "transform_template" in snapshot
        assert "pipeline_info" not in snapshot  # Not in state
        assert "normalized_schema_v4" not in snapshot  # Not in state
        assert "unrelated_key" not in snapshot  # Not in keys_of_interest

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_logs_all_keys_of_interest_when_present(self, mock_mlflow: MagicMock) -> None:
        """Test that log_state_snapshot logs all keys of interest when they're in state."""
        # Arrange
        state = {
            "environment_type": "slv",
            "pipeline_info": {"pipeline_name": "full_test"},
            "normalized_schema_v4": {"version": 4},
            "pipeline_template": {"type": "pipeline"},
            "transform_template": {"type": "transform"},
            "notebook_template": {"type": "notebook"},
            "extra_key": "ignored",
        }

        # Act
        log_state_snapshot(state)

        # Assert
        logged_json = mock_mlflow.log_text.call_args[0][0]
        snapshot = json.loads(logged_json)
        assert len(snapshot) == 6  # All keys_of_interest
        assert "environment_type" in snapshot
        assert "pipeline_info" in snapshot
        assert "normalized_schema_v4" in snapshot
        assert "pipeline_template" in snapshot
        assert "transform_template" in snapshot
        assert "notebook_template" in snapshot
        assert "extra_key" not in snapshot

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_json_formatting(self, mock_mlflow: MagicMock) -> None:
        """Test that log_state_snapshot formats JSON with proper indentation."""
        # Arrange
        state = {"environment_type": "brz", "pipeline_info": {"name": "test"}}

        # Act
        log_state_snapshot(state)

        # Assert
        logged_json = mock_mlflow.log_text.call_args[0][0]
        # Check that it's valid JSON and formatted with indentation
        snapshot = json.loads(logged_json)
        assert isinstance(snapshot, dict)
        # Re-format to verify indentation
        expected = json.dumps(snapshot, ensure_ascii=False, indent=2)
        assert logged_json == expected

    @patch("brewbridge.infrastructure.observability.event_logger.mlflow")
    def test_handles_complex_nested_structures(self, mock_mlflow: MagicMock) -> None:
        """Test that log_state_snapshot handles complex nested data structures."""
        # Arrange
        state = {
            "environment_type": "gld",
            "pipeline_info": {
                "pipeline_name": "complex",
                "config": {"nested": {"deeply": {"key": "value"}}},
                "list": [1, 2, {"inner": "data"}],
            },
        }

        # Act
        log_state_snapshot(state)

        # Assert
        logged_json = mock_mlflow.log_text.call_args[0][0]
        snapshot = json.loads(logged_json)
        assert snapshot["pipeline_info"]["config"]["nested"]["deeply"]["key"] == "value"
        assert snapshot["pipeline_info"]["list"][2]["inner"] == "data"
