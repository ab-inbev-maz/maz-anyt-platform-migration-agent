"""
Infrastructure wrapper for executing the official BrewDat 4.0
`engineeringstore` CLI with safe subprocess handling.

Patterns:
- Command Pattern (EngineeringStoreCommand)
- Facade Pattern (EngineeringStoreCLI)
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import List, Optional

from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import (
    EngineeringStoreExecutionError,
    EngineeringStoreNotInstalledError,
    EngineeringStoreTimeoutError,
)


@dataclass(frozen=True)
class EngineeringStoreCommand:
    """
    Represents a single engineeringstore CLI command.

    table_type:
        - "gold" → brewtiful repo
        - "brz" or "slv" → hopsflow repo
    """

    command: List[str]
    table_type: str
    needs_input: bool = False


class EngineeringStoreCLI:
    """
    Facade wrapper responsible for executing `engineeringstore` CLI commands.
    """

    def __init__(self, logger=None, timeout: int = 300):
        self.logger = logger or get_logger(__name__)
        self.timeout = timeout

    def _resolve_working_dir(self, table_type: str) -> str:
        """
        Converts domain type (gold/brz/slv) into the correct working directory.
        """
        if table_type.lower() == "gold":
            return os.path.join("cache", "brewtiful", "brewtiful")

        if table_type.lower() in ("brz", "slv"):
            return os.path.join("cache", "hopsflow", "brewdat-pltfrm-ghq-tech-hopsflow")

        raise ValueError(f"Invalid table_type '{table_type}'. Expected 'gold', 'brz', or 'slv'.")

    def run(self, es_command: EngineeringStoreCommand, input_text: Optional[str] = None) -> str:
        cmd_list = es_command.command

        working_dir = self._resolve_working_dir(es_command.table_type)

        if not os.path.isdir(working_dir):
            os.makedirs(working_dir, exist_ok=True)

        self.logger.info("Executing engineeringstore CLI command...")
        self.logger.debug(f"Command: {cmd_list}")
        self.logger.debug(f"Resolved working directory: {working_dir}")

        # Handle stdin text
        if input_text and es_command.needs_input:
            self.logger.debug("Injecting prompt responses.")
            self.logger.debug(f"Input Text:\n{input_text}")
        elif input_text and not es_command.needs_input:
            self.logger.warning("Input provided but command does not expect stdin.")

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
                "engineeringstore CLI is not installed or not available in PATH."
            ) from e

        except subprocess.TimeoutExpired as e:
            raise EngineeringStoreTimeoutError(
                f"engineeringstore CLI timed out after {self.timeout} seconds."
            ) from e

        stdout = process.stdout or ""
        stderr = process.stderr or ""

        self.logger.debug(f"STDOUT:\n{stdout}")
        if stderr.strip():
            self.logger.debug(f"STDERR:\n{stderr}")

        if process.returncode != 0:
            raise EngineeringStoreExecutionError(
                f"CLI returned exit code {process.returncode}.\nSTDERR:\n{stderr}"
            )

        return stdout
