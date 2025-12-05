"""
Manual smoke tests for the validator tool and underlying CLI command selection.

Run (examples):
    uv run tests/manual/test_validator_manual.py
"""

from __future__ import annotations

from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.extractor_strategies.engineeringstore_input_builder import (
    build_validation_command_args,
)
from brewbridge.domain.tools.validator import validator
from brewbridge.infrastructure.engineeringstore_cli import (
    EngineeringStoreCLI,
    EngineeringStoreCommand,
)


def run_validator(environment_type: str):
    """
    Execute validator and print the captured outputs from state.

    The working directory is resolved by EngineeringStoreCLI:
    - gld -> cache/brewtiful
    - brz/slv -> cache/brewdat-pltfrm-ghq-tech-hopsflow
    Ensure the relevant artifacts exist there before running.
    """
    print(f"\n\n>>> validator for env: {environment_type}")
    state = MigrationGraphState(environment_type=environment_type)
    try:
        new_state = validator(state)
        print(f"return_code: {new_state.validation_return_code}")
        print(f"passed: {new_state.validation_passed}")
        print("\n--- STDOUT ---")
        print(new_state.validation_stdout)
        print("\n--- STDERR ---")
        print(new_state.validation_stderr)
        print("\n--- PARSED ERRORS ---")
        print(new_state.parsed_validation_errors)
    except Exception as exc:  # pragma: no cover - manual smoke test
        print(f"\n>>> ERROR:\n{exc}")


def run_cli_direct(environment_type: str):
    """
    Run the underlying engineeringstore command directly (no state plumbing).
    """
    cmd = build_validation_command_args(environment_type)
    es_command = EngineeringStoreCommand(
        command=cmd, table_type="gold" if environment_type == "gld" else environment_type
    )

    print(f"\n\n>>> engineeringstore CLI validate for env: {environment_type}")
    cli = EngineeringStoreCLI()
    result = cli.run_with_result(es_command, raise_on_error=False)

    print(f"return_code: {result.returncode}")
    print("\n--- STDOUT ---")
    print(result.stdout)
    print("\n--- STDERR ---")
    print(result.stderr)


if __name__ == "__main__":
    # Update the environments you want to smoke-test below.
    # Requires artifacts to be present in the expected cache/ directories.
    run_validator("slv")
    run_validator("gld")

    # Uncomment to run the CLI directly
    # run_cli_direct("gld")
    # run_cli_direct("slv")
