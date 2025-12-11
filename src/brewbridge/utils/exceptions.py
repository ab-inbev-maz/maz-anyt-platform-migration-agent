"""
These exceptions standardize error signaling across:
- EngineeringStore CLI wrapper
- Workflow orchestration (LangGraph)
"""


class BrewBridgeError(Exception):
    """Base class for all custom project exceptions."""

    pass


# ============================================================
# GitHub Client Exceptions
# ============================================================
class GitHubAuthError(BrewBridgeError):
    """Exception for 401/403 errors."""

    pass


class GitHubRequestError(BrewBridgeError):
    """General exception for GitHub request failures."""

    pass


class RepositoryCloneError(BrewBridgeError):
    """Raised when a repository fails to clone or update."""

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

    def __init__(
        self, message: str, stdout: str = "", stderr: str = "", returncode: int | None = None
    ):
        super().__init__(message)
        self.stdout = stdout or ""
        self.stderr = stderr or ""
        self.returncode = returncode


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


# ============================================================
# Extraction Exceptions
# ============================================================


class ExtractionError(BrewBridgeError):
    """Raised when an extraction strategy fails or encounters invalid state."""

    pass


class InvalidInputError(BrewBridgeError):
    """Raised when required inputs for a tool/strategy are missing or malformed."""

    pass


# ============================================================
# Templates Creation Exceptions
# ============================================================


class TemplateCreationError(BrewBridgeError):
    """Raised when the TemplateCreator fails to generate template files."""

    pass


# ============================================================
# Manifest Exceptions
# ============================================================


class ManifestNotFoundError(BrewBridgeError):
    """Raised when the manifest.yaml file is not found at the specified path."""

    pass


class ManifestParseError(BrewBridgeError):
    """Raised when the manifest.yaml file cannot be parsed or validated."""

    pass


# ============================================================
# Databricks Exceptions
# ============================================================


class DatabricksClientError(Exception):
    pass


class DatabricksConfigError(DatabricksClientError):
    pass


class DatabricksAuthError(DatabricksClientError):
    pass


class DatabricksExecutionError(DatabricksClientError):
    pass


class DatabricksTimeoutError(DatabricksClientError):
    pass


class DatabricksTableNotFoundError(DatabricksClientError):
    pass


class DatabricksWarehouseNotRunningError(DatabricksClientError):
    """SQL Warehouse is stopped, starting, or not ready."""

    pass

# ============================================================
# Parser Exceptions
# ============================================================

class ParserError(BrewBridgeError):
    """Raised when there is an error parsing a file (e.g., YAML, JSON)."""

    pass