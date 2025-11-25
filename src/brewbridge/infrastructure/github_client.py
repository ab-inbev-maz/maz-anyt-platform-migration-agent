import base64
import os
from requests import Response, Session
from typing import List, Dict
from brewbridge.utils.exceptions import GitHubAuthError, GitHubRequestError
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)
class GitHubClient:
    """
    Handles authenticated communication with the GitHub REST API.
    Encapsulates session management, authentication, and error handling.
    """
    BASE_URL = os.getenv("BASE_URL_GITHUB", "https://api.github.com")
    ACCEPT_HEADER = os.getenv("ACCEPT_HEADER", "application/vnd.github.v3+json")
    API_VERSION_HEADER = os.getenv("API_VERSION_HEADER", "2022-11-28")

    def __init__(self, token: str):
        if not token:
            logger.error("GitHubClient initialized without a valid token.")
            raise GitHubAuthError("Missing GitHub access token.")
        
        self.session: Session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": self.ACCEPT_HEADER,
            "X-GitHub-Api-Version": self.API_VERSION_HEADER 
        })
        logger.debug("GitHubClient initialized successfully.")

    def ping(self) -> bool:
        """
        Check GitHub connectivity and token validity by calling /user endpoint.
        """
        logger.debug("Pinging GitHub API (/user) to validate token...")
        try:
            res: Response = self.session.get(f"{self.BASE_URL}/user", timeout=10)
            
            if res.status_code == 200:
                logger.info("GitHub API ping successful.")
                return True
            
            logger.warning(f"GitHub ping failed with status code: {res.status_code}")
            return False
            
        except requests.exceptions.Timeout:
            logger.error("GitHub ping timed out.")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"GitHub ping request failed: {e}")
            return False

    def get_file(self, repo: str, path: str, branch: str = "main") -> str:
        """
        Retrieve and decode a file's content from a GitHub repository.
        
        :param repo: Repository name (e.g., 'BrewDat/brewdat-maz-repo').
        :param path: Path to the file within the repo.
        :param branch: Branch or ref to pull from (defaults to 'main').
        :return: Decoded string content of the file.
        """
        url = f"{self.BASE_URL}/repos/{repo}/contents/{path}?ref={branch}"
        logger.debug(f"Fetching file: {repo}/{path} @ {branch}")

        try:
            res: Response = self.session.get(url, timeout=20)

            # Handle auth errors
            if res.status_code in [401, 403]:
                logger.error(f"GitHub auth error ({res.status_code}). Token may be invalid, expired, or lack permissions for {repo}.")
                raise GitHubAuthError(f"Invalid/expired token or insufficient permissions for {repo}.")
            
            # Handle file not found
            if res.status_code == 404:
                 logger.error(f"File not found (404) at {url}")
                 raise GitHubRequestError(f"File not found: {path} in {repo}@{branch}")
            
            # Handle other HTTP errors
            res.raise_for_status() 

            data = res.json()
            if isinstance(data, list) or "content" not in data:
                logger.warning(f"Path '{path}' points to a directory, not a file.")
                raise GitHubRequestError(f"Path is a directory, use list_directory instead: {path}")

            # Decode Base64 content
            content_b64 = data["content"]
            decoded_content = base64.b64decode(content_b64).decode("utf-8")
            
            logger.info(f"Successfully fetched and decoded file: {repo}/{path}")
            return decoded_content

        except base64.binascii.Error as e:
            logger.error(f"Failed to decode Base64 content for {url}: {e}")
            raise GitHubRequestError(f"Failed to decode file content for {path}: {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"GitHub API HTTP error: {e}")
            raise GitHubRequestError(f"GitHub API error: {e.response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"GitHub get_file request failed: {e}")
            raise GitHubRequestError(f"Network error getting file: {e}")
        
    def list_directory(self, repo: str, path: str, branch: str = "main") -> List[Dict]:
        """
        List files and directories in a specific path.
        Required for Strategies to discover files (e.g. "Find all JSONs in /pipelines").
        """
        url = f"{self.BASE_URL}/repos/{repo}/contents/{path}?ref={branch}"
        logger.debug(f"Listing directory: {repo}/{path} @ {branch}")

        try:
            res: Response = self.session.get(url, timeout=10)

            if res.status_code in [401, 403]:
                raise GitHubAuthError(f"Permission denied for {repo}")
            
            if res.status_code == 404:
                logger.warning(f"Directory not found: {path}")
                return [] 

            res.raise_for_status()
            
            data = res.json()
            
            if not isinstance(data, list):
                logger.warning(f"Path {path} is a file, not a directory.")
                return []

            # Simplificamos la respuesta para el uso interno
            items = [
                {
                    "name": item["name"],
                    "path": item["path"],
                    "type": item["type"], 
                    "download_url": item.get("download_url")
                }
                for item in data
            ]
            return items

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list directory {path}: {e}")
            raise GitHubRequestError(f"Network error listing directory: {e}")