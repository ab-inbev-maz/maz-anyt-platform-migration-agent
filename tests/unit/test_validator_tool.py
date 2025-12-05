from __future__ import annotations

import pytest

from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.tools.validator import validator
from brewbridge.infrastructure.engineeringstore_cli import (
    EngineeringStoreCLI,
    EngineeringStoreResult,
)
from brewbridge.utils.exceptions import StateValidationError


def test_validator_runs_transformation(monkeypatch):
    captured = {}

    def fake_run(self, es_command, input_text=None, raise_on_error=True):
        captured["command"] = es_command.command
        captured["table_type"] = es_command.table_type
        return EngineeringStoreResult(stdout="ok stdout", stderr="", returncode=0)

    monkeypatch.setattr(EngineeringStoreCLI, "run_with_result", fake_run)

    state = MigrationGraphState(environment_type="gld")
    new_state = validator(state)

    assert captured["command"] == ["engineeringstore", "transformation", "--validate-dags"]
    assert captured["table_type"] == "gold"
    assert new_state.validation_passed is True
    assert new_state.validation_return_code == 0
    assert new_state.validation_output == "ok stdout"


def test_validator_runs_ingestion_and_combines_outputs(monkeypatch):
    captured = {}

    def fake_run(self, es_command, input_text=None, raise_on_error=True):
        captured["command"] = es_command.command
        captured["table_type"] = es_command.table_type
        return EngineeringStoreResult(
            stdout="validation stdout", stderr="validation stderr", returncode=3
        )

    monkeypatch.setattr(EngineeringStoreCLI, "run_with_result", fake_run)

    state = MigrationGraphState(environment_type="slv")
    new_state = validator(state)

    assert captured["command"] == ["engineeringstore", "ingestion", "--validate-dags"]
    assert captured["table_type"] == "slv"
    assert new_state.validation_passed is False
    assert new_state.validation_output == "validation stdout\nvalidation stderr"
    assert new_state.parsed_validation_errors == []


def test_validator_requires_environment_type():
    state = MigrationGraphState()
    with pytest.raises(StateValidationError):
        validator(state)


def test_validator_parses_errors(monkeypatch):
    raw_block = """\
[DAG_VALIDATION_ERROR_BASE_LEVEL]
  ├─ Yaml Key: public_dag
  ├─ Error Message: must be of boolean type
  └─ Yaml File: /tmp/sap_acl.yaml
"""

    def fake_run(self, es_command, input_text=None, raise_on_error=True):
        return EngineeringStoreResult(stdout=raw_block, stderr="", returncode=1)

    monkeypatch.setattr(EngineeringStoreCLI, "run_with_result", fake_run)

    state = MigrationGraphState(environment_type="brz")
    new_state = validator(state)

    assert new_state.validation_passed is False
    assert len(new_state.parsed_validation_errors) == 1
    entry = new_state.parsed_validation_errors[0]
    assert entry["file_path"] == "/tmp/sap_acl.yaml"
    assert entry["errors"][0]["yaml_key"] == "public_dag"
