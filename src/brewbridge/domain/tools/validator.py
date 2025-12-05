from __future__ import annotations

from brewbridge.core.base_nodes import tool_node
from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.extractor_strategies.engineeringstore_input_builder import (
    build_validation_command_args,
    combine_cli_output,
)
from brewbridge.domain.tools.validation_error_parser import parse_validation_output
from brewbridge.infrastructure.engineeringstore_cli import (
    EngineeringStoreCLI,
    EngineeringStoreCommand,
)
from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.observability import track_node
from brewbridge.utils.exceptions import StateValidationError

logger = get_logger(__name__)


@track_node("tool")
@tool_node
def validator(state: MigrationGraphState) -> MigrationGraphState:
    """
    Executes engineeringstore DAG validation for the current environment.

    The command is mapped as:
    - gld → transformation --validate-dags
    - brz/slv → ingestion --validate-dags
    """
    env = state.environment_type

    if not env:
        raise StateValidationError("environment_type is required to run the validator tool")

    try:
        cmd_args = build_validation_command_args(env)
    except ValueError as exc:
        raise StateValidationError(str(exc)) from exc

    es_command = EngineeringStoreCommand(
        command=cmd_args, table_type="gold" if env == "gld" else env, needs_input=False
    )
    cli = EngineeringStoreCLI(logger=logger)

    result = cli.run_with_result(es_command, raise_on_error=False)

    state.validation_stdout = result.stdout
    state.validation_stderr = result.stderr
    state.validation_output = combine_cli_output(result.stdout, result.stderr)
    state.validation_return_code = result.returncode
    state.validation_passed = result.returncode == 0
    state.parsed_validation_errors = parse_validation_output(state.validation_output)

    return state
