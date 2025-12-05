from __future__ import annotations

import subprocess

import pytest

from brewbridge.infrastructure.engineeringstore_cli import (
    EngineeringStoreCLI,
    EngineeringStoreCommand,
)
from brewbridge.utils.exceptions import EngineeringStoreExecutionError


def test_run_with_result_returns_outputs(monkeypatch):
    calls = {}

    def fake_run(cmd_list, cwd, input, text, capture_output, timeout, check):
        calls["cmd_list"] = cmd_list
        calls["cwd"] = cwd
        process = subprocess.CompletedProcess(args=cmd_list, returncode=2)
        process.stdout = "stdout content"
        process.stderr = "stderr content"
        return process

    monkeypatch.setattr(subprocess, "run", fake_run)

    cli = EngineeringStoreCLI(timeout=5)
    command = EngineeringStoreCommand(
        command=["engineeringstore", "ingestion", "--validate-dags"], table_type="brz"
    )

    result = cli.run_with_result(command, raise_on_error=False)

    assert calls["cmd_list"] == ["engineeringstore", "ingestion", "--validate-dags"]
    assert "brewdat-pltfrm-ghq-tech-hopsflow" in calls["cwd"]
    assert result.returncode == 2
    assert result.stdout == "stdout content"
    assert result.stderr == "stderr content"


def test_run_with_result_raises_with_context(monkeypatch):
    def fake_run(cmd_list, cwd, input, text, capture_output, timeout, check):
        process = subprocess.CompletedProcess(args=cmd_list, returncode=1)
        process.stdout = "bad stdout"
        process.stderr = "bad stderr"
        return process

    monkeypatch.setattr(subprocess, "run", fake_run)

    cli = EngineeringStoreCLI()
    command = EngineeringStoreCommand(
        command=["engineeringstore", "transformation", "--validate-dags"], table_type="gold"
    )

    with pytest.raises(EngineeringStoreExecutionError) as excinfo:
        cli.run_with_result(command)

    err = excinfo.value
    assert err.returncode == 1
    assert err.stdout == "bad stdout"
    assert err.stderr == "bad stderr"
