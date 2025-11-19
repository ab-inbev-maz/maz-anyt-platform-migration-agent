class GitHubAuthError(Exception):
    """Exception for 401/403 errors."""
    pass

class GitHubRequestError(Exception):
    """Exception for general API request failures."""
"""
Domain-level exceptions for the BrewBridge / Migration Agent project.

These exceptions standardize error signaling across:
- EngineeringStore CLI wrapper
- Workflow orchestration (LangGraph)
"""


class BrewBridgeError(Exception):
    """Base class for all custom project exceptions."""

    pass


# ============================================================
# EngineeringStore CLI Exceptions
# ============================================================


class EngineeringStoreError(BrewBridgeError):
    """Base class for engineeringstore CLI related errors."""

    pass


class EngineeringStoreNotInstalledError(EngineeringStoreError):
    """
    Raised when the engineeringstore binary is not found in PATH.

    Typically caused by:
    - Missing BrewDat 4.0 CLI installation
    - Incorrect PATH configuration
    - Missing environment setup in CI/CD
    """

    pass


class EngineeringStoreExecutionError(EngineeringStoreError):
    """
    Raised when the engineeringstore CLI returns a non-zero exit code
    or stderr indicates an error condition.

    Example:
        - invalid arguments
        - DAG validation errors
        - template generation failures
    """

    pass


class EngineeringStoreTimeoutError(EngineeringStoreError):
    """
    Raised when the engineeringstore CLI process exceeds the allowed timeout.

    Typically caused by:
    - Large pipelines
    - Deadlocks
    - Unexpected CLI hangs
    """

    pass


# ============================================================
# Workflow / LangGraph Exceptions
# ============================================================


class StateValidationError(BrewBridgeError):
    """Raised when the GraphState is missing required fields."""

    pass


class AgentRoutingError(BrewBridgeError):
    """Raised when the router cannot determine the correct next translator."""

    pass
