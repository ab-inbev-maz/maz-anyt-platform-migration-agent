"""
Azure Data Factory client for connectivity checks and API operations.
"""

import requests
from typing import Optional
from requests import Response, Session
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)


class ADFClient:
    """
    Handles authenticated communication with Azure Data Factory Management API.
    Encapsulates session management, authentication, and error handling.
    """

    BASE_URL = "https://management.azure.com"

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        subscription_id: Optional[str] = None,
    ):
        """
        Initialize ADF client with Azure AD credentials.

        :param tenant_id: Azure AD tenant ID
        :param client_id: Azure AD client/application ID
        :param client_secret: Azure AD client secret
        :param subscription_id: Optional Azure subscription ID
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.subscription_id = subscription_id
        self.session: Session = requests.Session()
        self._access_token: Optional[str] = None

        logger.debug("ADFClient initialized.")

    def _get_access_token(self) -> str:
        """
        Obtain Azure AD access token using client credentials flow.

        :return: Access token string
        """
        if self._access_token:
            return self._access_token

        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://management.azure.com/.default",
            "grant_type": "client_credentials",
        }

        try:
            response = requests.post(token_url, data=data, timeout=10)
            response.raise_for_status()
            token_data = response.json()
            self._access_token = token_data["access_token"]
            logger.debug("Successfully obtained Azure AD access token.")
            return self._access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to obtain Azure AD token: {e}")
            raise

    def ping(self) -> bool:
        """
        Check Azure Data Factory connectivity by pinging the management API.

        :return: True if connectivity is successful, False otherwise
        """
        logger.debug("Pinging Azure Data Factory Management API...")

        try:
            token = self._get_access_token()
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            # Ping the subscriptions endpoint as a connectivity check
            url = f"{self.BASE_URL}/subscriptions?api-version=2020-01-01"
            response: Response = self.session.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                logger.info("Azure Data Factory API ping successful.")
                return True

            logger.warning(
                f"Azure Data Factory ping failed with status code: {response.status_code}"
            )
            return False

        except requests.exceptions.Timeout:
            logger.error("Azure Data Factory ping timed out.")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Azure Data Factory ping request failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Azure Data Factory ping failed: {e}")
            return False
