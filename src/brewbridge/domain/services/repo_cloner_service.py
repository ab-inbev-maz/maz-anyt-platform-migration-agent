"""
Repository cloning logic for framework preparation.

This module contains the logic to detect required frameworks from the manifest
and to clone or update the corresponding repositories.
"""

from pathlib import Path
from typing import List, Sequence, Set

from git import Repo
from git.exc import GitCommandError, GitError

from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.constans import ConstansLibrary
from brewbridge.utils.exceptions import RepositoryCloneError

logger = get_logger(__name__)


class RepoClonerService:
    """Service responsible for preparing framework repositories."""

    def _get_repo_url(self, repo_name: str) -> str:
        repo_urls = {
            "brewtiful": ConstansLibrary.BREWTIFUL_REPO_URL,
            "brewdat-pltfrm-ghq-tech-hopsflow": ConstansLibrary.HOPSFLOW_REPO_URL,
        }

        if repo_name not in repo_urls:
            raise ValueError(f"Unknown framework repository: {repo_name}")

        return repo_urls[repo_name]

    def _clone_or_pull_repo(self, repo_name: str, destination: Path, github_token: str) -> None:
        repo_url = self._get_repo_url(repo_name)

        # Construct authenticated URL with token
        authenticated_url = (
            repo_url.replace("https://github.com/", f"https://{github_token}@github.com/")
            if "https://github.com/" in repo_url
            else repo_url
        )

        shallow_depth = 10  # reduce bandwidth/time when cloning

        try:
            if destination.exists() and (destination / ".git").exists():
                logger.info(
                    "Repository %s already exists at %s, pulling updates...",
                    repo_name,
                    destination,
                )
                lock_file = destination / ".git" / "index.lock"
                if lock_file.exists():
                    logger.warning("Found stale git lock file, attempting to remove it...")
                    try:
                        lock_file.unlink()
                    except (PermissionError, OSError) as err:
                        logger.warning(
                            "Could not remove lock file (may be in use by another process): %s",
                            err,
                        )
                        logger.warning("Will attempt to proceed anyway...")
                        import time

                        time.sleep(1)

                repo = Repo(destination)

                if repo.is_dirty():
                    logger.info("Repository has local changes, resetting to match remote...")
                    repo.head.reset(working_tree=True)

                origin = repo.remotes.origin
                origin.fetch()
                try:
                    origin.pull()
                except GitCommandError as pull_error:
                    if "would be overwritten" in str(pull_error) or "local changes" in str(
                        pull_error
                    ):
                        logger.warning(
                            "Pull failed due to local changes, resetting to remote state...",
                        )
                        repo.head.reset(working_tree=True, index=True)
                        origin.pull()
                    else:
                        raise

                logger.info("Successfully updated repository %s", repo_name)
            elif destination.exists() and not (destination / ".git").exists():
                logger.info(
                    "Directory %s exists but is not a git repository. Removing and cloning fresh...",
                    destination,
                )
                import shutil

                shutil.rmtree(destination)
                destination.parent.mkdir(parents=True, exist_ok=True)
                logger.info(
                    "Cloning repository %s (this may take several minutes for large repositories)...",
                    repo_name,
                )
                Repo.clone_from(
                    authenticated_url,
                    str(destination),
                    depth=shallow_depth,
                )
                logger.info("Successfully cloned repository %s", repo_name)
            else:
                logger.info(
                    "Cloning repository %s to %s (this may take several minutes for large repositories)...",
                    repo_name,
                    destination,
                )
                destination.parent.mkdir(parents=True, exist_ok=True)
                Repo.clone_from(
                    authenticated_url,
                    str(destination),
                    depth=shallow_depth,
                )
                logger.info("Successfully cloned repository %s", repo_name)

        except GitCommandError as err:
            error_msg = f"Git command failed for {repo_name}: {err}"
            logger.error(error_msg)
            raise RepositoryCloneError(error_msg) from err
        except GitError as err:
            error_msg = f"Git error for {repo_name}: {err}"
            logger.error(error_msg)
            raise RepositoryCloneError(error_msg) from err
        except Exception as err:  # pragma: no cover - defensive logging
            error_msg = f"Unexpected error cloning/updating {repo_name}: {err}"
            logger.error(error_msg)
            raise RepositoryCloneError(error_msg) from err

    def prepare_repositories(self, github_token: str) -> List[str]:
        """
        Clone or update both framework repositories (brewtiful and hopsflow).

        :param github_token: GitHub token for authentication.
        :return: List of framework names that were cloned/updated.
        """
        repos_to_clone: Set[str] = {"brewtiful", "brewdat-pltfrm-ghq-tech-hopsflow"}
        logger.info("Cloning frameworks: %s", list(repos_to_clone))

        cloned_repos = []
        for repo_name in sorted(repos_to_clone):
            repo_path = Path(f"cache/{repo_name}")
            self._clone_or_pull_repo(repo_name, repo_path, github_token)
            cloned_repos.append(repo_name)
            logger.info("Repository %s is ready at %s", repo_name, repo_path)

        logger.info("Framework repository setup completed. Cloned/updated: %s", cloned_repos)
        return cloned_repos
