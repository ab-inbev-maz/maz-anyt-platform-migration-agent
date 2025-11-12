class GitHubAuthError(Exception):
    """Exception for 401/403 errors."""
    pass

class GitHubRequestError(Exception):
    """Exception for general API request failures."""
    pass
