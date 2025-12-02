"""
Read_Manifest_and_Check_API pre-flight helpers.

Domain-level service to:
- collect and merge credentials,
- ping GitHub, ADF and LLM APIs.

The LangGraph ToolNode wrapper lives in
`domain.tools.set_up.py`.
"""

import os
import time
from typing import Dict, Optional

from langchain_openai import ChatOpenAI

from brewbridge.infrastructure.datafactory_client import ADFClient
from brewbridge.infrastructure.github_client import GitHubClient
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)


class ManifestPreflightService:
    """Service responsible for manifest-related pre-flight checks."""

    def __init__(self) -> None:
        # For now we just reuse the module-level logger; can be injected.
        self._logger = logger

    def build_llm(
        self,
        model_name: Optional[str] = None,
        max_tokens: int = 500,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> ChatOpenAI:
        """
        Construye un cliente ChatOpenAI apuntando al proxy ASIMOV.
        Variables requeridas (o argumentos):
        - ASIMOV_URL (ej: https://asimov.tu-dominio.com)
        - ASIMOV_PRODUCT_TOKEN
        """
        base_url = base_url or os.getenv("ASIMOV_URL")
        api_key = api_key or os.getenv("ASIMOV_PRODUCT_TOKEN")

        if not base_url or not api_key:
            raise RuntimeError(
                "Faltan credenciales ASIMOV. Define ASIMOV_URL y ASIMOV_PRODUCT_TOKEN."
            )

        model = model_name or os.getenv("CHAT_MODEL", "openai/gpt-4o")

        return ChatOpenAI(
            model_name=model,
            base_url=f"{base_url}/api/v2/",
            api_key=api_key,
            extra_body={"max_tokens": max_tokens},
            timeout=60,
        )

    def collect_env_credentials(self) -> Dict[str, str]:
        """
        Collect credentials from environment variables.

        Returns a dictionary of credentials from the environment.
        """
        creds: Dict[str, str] = {}

        # GitHub credentials
        if github_token := os.getenv("GITHUB_TOKEN"):
            creds["GITHUB_TOKEN"] = github_token

        # Azure Data Factory credentials
        if tenant_id := os.getenv("ADF_TENANT_ID"):
            creds["ADF_TENANT_ID"] = tenant_id
        if client_id := os.getenv("ADF_CLIENT_ID"):
            creds["ADF_CLIENT_ID"] = client_id
        if client_secret := os.getenv("ADF_CLIENT_SECRET"):
            creds["ADF_CLIENT_SECRET"] = client_secret

        # LLM API credentials (Asimov)
        if asimov_url := os.getenv("ASIMOV_URL"):
            creds["ASIMOV_URL"] = asimov_url
        if asimov_token := os.getenv("ASIMOV_PRODUCT_TOKEN"):
            creds["ASIMOV_PRODUCT_TOKEN"] = asimov_token

        # OpenAI (if used directly)
        if openai_key := os.getenv("OPENAI_API_KEY"):
            creds["OPENAI_API_KEY"] = openai_key

        return creds

    def ping_github(self, credentials: Dict[str, str]) -> bool:
        """
        Ping GitHub API to verify connectivity and token validity.

        Retries up to 3 times on failure.
        """
        github_token = credentials.get("GITHUB_TOKEN")
        if not github_token:
            self._logger.warning(
                "GitHub token not found in credentials. Skipping GitHub ping.",
            )
            return False

        max_retries = 3
        for attempt in range(max_retries):
            try:
                client = GitHubClient(token=github_token)
                if client.ping():
                    return True
            except Exception as exc:  # pragma: no cover - defensive logging
                self._logger.warning(
                    "GitHub ping attempt %s/%s failed: %s",
                    attempt + 1,
                    max_retries,
                    exc,
                )

            if attempt < max_retries - 1:
                time.sleep(1)  # Wait 1 second before retry

        self._logger.error("GitHub ping failed after all retries.")
        return False

    def ping_adf(self, credentials: Dict[str, str]) -> bool:
        """
        Ping Azure Data Factory API to verify connectivity.

        Retries up to 3 times on failure.
        """
        tenant_id = credentials.get("ADF_TENANT_ID")
        client_id = credentials.get("ADF_CLIENT_ID")
        client_secret = credentials.get("ADF_CLIENT_SECRET")

        if not all([tenant_id, client_id, client_secret]):
            self._logger.warning(
                "Azure Data Factory credentials incomplete. Skipping ADF ping.",
            )
            return False

        max_retries = 3
        for attempt in range(max_retries):
            try:
                client = ADFClient(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret,
                )
                if client.ping():
                    return True
            except Exception as exc:  # pragma: no cover - defensive logging
                self._logger.warning(
                    "ADF ping attempt %s/%s failed: %s",
                    attempt + 1,
                    max_retries,
                    exc,
                )

            if attempt < max_retries - 1:
                time.sleep(1)  # Wait 1 second before retry

        self._logger.error("Azure Data Factory ping failed after all retries.")
        return False

    def ping_llm_apis(self, credentials: Dict[str, str]) -> bool:
        """
        Ping LLM APIs to verify connectivity.

        Currently checks Asimov API and, if configured, OpenAI.
        """
        asimov_url = credentials.get("ASIMOV_URL")
        asimov_token = credentials.get("ASIMOV_PRODUCT_TOKEN")

        if asimov_url and asimov_token:
            try:
                llm = self.build_llm(
                    model_name=credentials.get("CHAT_MODEL"),
                    max_tokens=32,
                    base_url=asimov_url,
                    api_key=asimov_token,
                )
                # Minimal invocation to confirm auth + reachability.
                llm.invoke("ping")
                self._logger.info("Asimov LLM API ping successful.")
                return True
            except Exception as exc:  # pragma: no cover - defensive logging
                self._logger.warning("Asimov LLM API ping failed: %s", exc)

        # Check OpenAI if configured
        openai_key = credentials.get("OPENAI_API_KEY")
        if openai_key:
            try:
                import requests

                response = requests.get(
                    "https://api.openai.com/v1/models",
                    headers={"Authorization": f"Bearer {openai_key}"},
                    timeout=5,
                )
                if response.status_code == 200:
                    self._logger.info("OpenAI API ping successful.")
                    return True
            except Exception as exc:  # pragma: no cover - defensive logging
                self._logger.warning("OpenAI API ping failed: %s", exc)

        self._logger.warning(
            "No LLM API credentials found or all pings failed.",
        )
        return False
