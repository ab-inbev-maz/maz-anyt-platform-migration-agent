from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import List, Optional


from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import (EngineeringStoreExecutionError,
                                         EngineeringStoreNotInstalledError,
                                         EngineeringStoreTimeoutError)

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


class EngineeringStoreCLI:
    """Facade wrapper responsible for executing `engineeringstore` CLI commands."""
    def __init__(self, logger=None, timeout: int = 300):
        self.logger = logger or get_logger(__name__)
        self.timeout = timeout

    def _resolve_working_dir(self, table_type: str) -> str:
        table_type = table_type.lower()

        if table_type == "gold":
            return os.path.join("cache", "brewtiful", "brewtiful")

        if table_type in ("brz", "slv"):
            return os.path.join("cache", "hopsflow", "brewdat-pltfrm-ghq-tech-hopsflow")

        raise ValueError(f"Invalid table_type '{table_type}'. Expected 'gold', 'brz', or 'slv'.")

    def run(self, es_command: EngineeringStoreCommand, input_text: Optional[str] = None) -> str:
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

        except FileNotFoundError as e:
            raise EngineeringStoreNotInstalledError(
                "engineeringstore CLI is not installed or available in PATH"
            ) from e

        except subprocess.TimeoutExpired as e:
            raise EngineeringStoreTimeoutError(
                f"engineeringstore CLI timed out after {self.timeout} seconds"
            ) from e

        stdout = process.stdout or ""
        stderr = process.stderr or ""

        debug_env = os.environ.get("DEBUG", "false").lower() == "true"
        if debug_env:
            if stdout.strip():
                self.logger.debug(f"[engineeringstore stdout]\n{stdout}")

            if stderr.strip():
                self.logger.debug(f"[engineeringstore stderr]\n{stderr}")

        if process.returncode != 0:
            raise EngineeringStoreExecutionError(
                f"CLI returned exit code {process.returncode}.\nSTDERR:\n{stderr}"
            )

        return stdout
