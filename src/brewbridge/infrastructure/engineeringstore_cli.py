from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import List, Optional

from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.observability import log_cli_output
from brewbridge.utils.exceptions import (
    EngineeringStoreExecutionError,
    EngineeringStoreNotInstalledError,
    EngineeringStoreTimeoutError,
)


@dataclass(frozen=True)
class EngineeringStoreCommand:
    """
    Represents a command to be executed by the engineeringstore CLI.

    Parameters:
        command (List[str]): The CLI command and its arguments.
        table_type (str): Determines which repository the command is routed to.
            Accepted values:
                - "gold": routes to the brewtiful repository.
                - "brz" or "slv": routes to the hopsflow repository.
        needs_input (bool): Whether the command expects input via stdin.
    """

    command: List[str]
    table_type: str
    needs_input: bool = False


@dataclass(frozen=True)
class EngineeringStoreResult:
    """Represents the stdout, stderr and exit code of an engineeringstore CLI run."""

    stdout: str
    stderr: str
    returncode: int


class EngineeringStoreCLI:
    """Facade wrapper responsible for executing `engineeringstore` CLI commands."""

    def __init__(self, logger=None, timeout: int = 300):
        self.logger = logger or get_logger(__name__)
        self.timeout = timeout

    def _resolve_working_dir(self, table_type: str) -> str:
        table_type = table_type.lower()

        if table_type == "gold":
            return os.path.join("cache", "brewtiful")

        if table_type in ("brz", "slv"):
            return os.path.join("cache", "brewdat-pltfrm-ghq-tech-hopsflow")

        raise ValueError(f"Invalid table_type '{table_type}'. Expected 'gold', 'brz', or 'slv'.")

    def _execute(
        self, es_command: EngineeringStoreCommand, input_text: Optional[str] = None
    ) -> EngineeringStoreResult:
        cmd_list = es_command.command
        working_dir = self._resolve_working_dir(es_command.table_type)

        os.makedirs(working_dir, exist_ok=True)

        self.logger.debug("Executing engineeringstore CLI command...")
        self.logger.debug(f"CLI command: {cmd_list}")
        self.logger.debug(f"Working directory: {working_dir}")

        if input_text and es_command.needs_input:
            self.logger.debug("Passing stdin to CLI command")
        elif input_text and not es_command.needs_input:
            self.logger.warning("Input provided but command does not expect stdin")

        try:
            process = subprocess.run(
                cmd_list,
                cwd=working_dir,
                input=input_text if es_command.needs_input else None,
                text=True,
                capture_output=True,
                timeout=self.timeout,
                check=False,
            )

        except FileNotFoundError as e:  # pragma: no cover - defensive guard
            raise EngineeringStoreNotInstalledError(
                "engineeringstore CLI is not installed or available in PATH"
            ) from e

        except subprocess.TimeoutExpired as e:
            raise EngineeringStoreTimeoutError(
                f"engineeringstore CLI timed out after {self.timeout} seconds"
            ) from e

        result = EngineeringStoreResult(
            stdout=process.stdout or "", stderr=process.stderr or "", returncode=process.returncode
        )

        log_cli_output(stdout=result.stdout, stderr=result.stderr)

        debug_env = os.environ.get("DEBUG", "false").lower() == "true"
        if debug_env:
            if result.stdout.strip():
                self.logger.debug(f"[engineeringstore stdout]\n{result.stdout}")

            if result.stderr.strip():
                self.logger.debug(f"[engineeringstore stderr]\n{result.stderr}")

        return result

    def run_with_result(
        self,
        es_command: EngineeringStoreCommand,
        input_text: Optional[str] = None,
        raise_on_error: bool = True,
    ) -> EngineeringStoreResult:
        result = self._execute(es_command, input_text=input_text)

        if raise_on_error and result.returncode != 0:
            raise EngineeringStoreExecutionError(
                f"CLI returned exit code {result.returncode}.\nSTDERR:\n{result.stderr}",
                stdout=result.stdout,
                stderr=result.stderr,
                returncode=result.returncode,
            )

        return result

    def run(self, es_command: EngineeringStoreCommand, input_text: Optional[str] = None) -> str:
        result = self.run_with_result(es_command, input_text=input_text, raise_on_error=True)
        return result.stdout
