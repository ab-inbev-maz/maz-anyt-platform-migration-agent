"""
DatabricksClient
Lightweight REST SQL client for Unity Catalog via SQL Warehouses.
Inline config (no external config file).
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict

import pandas as pd
import requests

from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import (
    DatabricksAuthError,
    DatabricksConfigError,
    DatabricksClientError,
    DatabricksExecutionError,
    DatabricksTableNotFoundError,
    DatabricksTimeoutError,
    DatabricksWarehouseNotRunningError,
)

logger = get_logger(__name__)


class DatabricksClient:
    """
    Lightweight Databricks REST SQL client using SQL Warehouses.
    Consistent with GitHubClient architecture.
    """

    STATEMENTS_ENDPOINT = "/api/2.0/sql/statements"

    def __init__(self):
        """
        Loads configuration from environment variables directly.
        No DatabricksConfig class needed.
        """

        # ------------------------------
        # Load env vars + validate
        # ------------------------------

        host = os.getenv("DATABRICKS_HOST", "").strip()
        token = os.getenv("DATABRICKS_TOKEN", "").strip()
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()

        missing = [
            k
            for k, v in {
                "DATABRICKS_HOST": host,
                "DATABRICKS_TOKEN": token,
                "DATABRICKS_WAREHOUSE_ID": warehouse_id,
            }.items()
            if not v
        ]

        if missing:
            logger.error(
                "DatabricksClient missing required configuration.", extra={"missing": missing}
            )
            raise DatabricksConfigError(
                f"Missing required Databricks environment variables: {', '.join(missing)}"
            )

        # Normalize host
        if host.endswith("/"):
            host = host[:-1]
        if not host.startswith("http"):
            host = f"https://{host}"

        self.host = host
        self.token = token
        self.warehouse_id = warehouse_id

        # Polling defaults
        self.poll_interval = 2.0
        self.timeout_seconds = 120.0

        # ------------------------------
        # Init persistent session
        # ------------------------------
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        logger.info(
            "DatabricksClient initialized successfully.",
            extra={
                "host": self.host,
                "warehouse_id": self.warehouse_id,
            },
        )

    # ============================================================================
    # PUBLIC API
    # ============================================================================

    def run_query(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL and return final Databricks statement result.
        """
        preview = sql.replace("\n", " ")[:200]
        logger.debug("Executing SQL on Databricks...", extra={"sql_preview": preview})

        statement_id = self._submit_statement(sql)
        return self._poll_statement(statement_id)

    def ping(self) -> bool:
        """
        Connectivity check using a lightweight `SELECT 1`.
        Returns True when the warehouse responds successfully.
        """
        try:
            payload = self.run_query("SELECT 1")
        except DatabricksClientError:
            raise
        except Exception as exc:
            logger.error("Unexpected Databricks ping error: %s", exc)
            raise DatabricksExecutionError(f"Ping failed: {exc}") from exc

        result = payload.get("result") or {}
        rows = result.get("data_array") or []
        return bool(rows)

    def read_table(self, table_path: str, limit: int = 1000) -> pd.DataFrame:
        sql = f"SELECT * FROM {table_path} LIMIT {int(limit)}"
        payload = self.run_query(sql)
        return self._payload_to_df(payload)

    # ============================================================================
    # INTERNAL — SUBMIT
    # ============================================================================

    def _submit_statement(self, sql: str) -> str:
        url = f"{self.host}{self.STATEMENTS_ENDPOINT}"
        payload = {
            "statement": sql,
            "warehouse_id": self.warehouse_id,
        }

        try:
            res = self.session.post(url, json=payload, timeout=15)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error submitting Databricks statement: {e}")
            raise DatabricksExecutionError(f"Error submitting SQL: {e}")

        self._handle_http_errors(res)

        data = res.json()
        statement_id = data.get("statement_id") or data.get("id")

        if not statement_id:
            logger.error("Databricks response missing statement_id.")
            raise DatabricksExecutionError("Databricks response missing statement_id.")

        return statement_id

    # ============================================================================
    # INTERNAL — POLLING LOOP
    # ============================================================================

    def _poll_statement(self, statement_id: str) -> Dict[str, Any]:
        url = f"{self.host}{self.STATEMENTS_ENDPOINT}/{statement_id}"
        start = time.time()

        while True:
            if time.time() - start > self.timeout_seconds:
                raise DatabricksTimeoutError(f"Timeout waiting for statement {statement_id}")

            try:
                res = self.session.get(url, timeout=10)
            except requests.exceptions.RequestException as e:
                logger.error(f"Error polling Databricks statement {statement_id}: {e}")
                raise DatabricksExecutionError(f"Polling error: {e}")

            self._handle_http_errors(res)
            payload = res.json()

            state = (payload.get("status") or {}).get("state", "").upper()
            logger.debug("Polling Databricks...", extra={"state": state})

            if state in ("PENDING", "RUNNING"):
                time.sleep(self.poll_interval)
                continue

            if state == "SUCCEEDED":
                return payload

            # FAILED / CANCELED / WAREHOUSE DOWN
            error = payload.get("error") or {}
            msg = error.get("message", "") or "Execution failed"

            if self._is_warehouse_not_running(msg):
                raise DatabricksWarehouseNotRunningError(msg)

            if self._is_table_not_found(msg):
                raise DatabricksTableNotFoundError(msg)

            raise DatabricksExecutionError(msg)

    # ============================================================================
    # INTERNAL — HTTP ERROR HANDLING
    # ============================================================================

    def _handle_http_errors(self, res: requests.Response):
        if 200 <= res.status_code < 300:
            return

        try:
            payload = res.json()
        except ValueError:
            payload = {}

        msg = payload.get("message") or payload.get("error") or f"HTTP {res.status_code}"

        # Warehouse off BEFORE other mappings
        if self._is_warehouse_not_running(msg):
            raise DatabricksWarehouseNotRunningError(msg)

        if res.status_code in (401, 403):
            raise DatabricksAuthError(msg)

        if res.status_code == 404:
            raise DatabricksTableNotFoundError(msg)

        raise DatabricksExecutionError(msg)

    # ============================================================================
    # INTERNAL — PARSING
    # ============================================================================

    @staticmethod
    def _payload_to_df(payload: Dict[str, Any]) -> pd.DataFrame:
        result = payload.get("result") or {}
        schema = (result.get("schema") or {}).get("columns", [])
        rows = result.get("data_array") or []

        colnames = [c.get("name") for c in schema]

        if not colnames and rows:
            colnames = [f"col_{i}" for i in range(len(rows[0]))]

        return pd.DataFrame(rows, columns=colnames)

    # ============================================================================
    # INTERNAL — ERROR DETECTION HELPERS
    # ============================================================================

    @staticmethod
    def _is_warehouse_not_running(msg: str) -> bool:
        if not msg:
            return False

        msg = msg.lower()
        patterns = [
            "warehouse is not running",
            "sql warehouse is not running",
            "warehouse is in state stopped",
            "warehouse is in state starting",
            "cluster is not ready",
            "cluster not ready",
            "warehouse unavailable",
            "warehouse failed",
        ]
        return any(p in msg for p in patterns)

    @staticmethod
    def _is_table_not_found(msg: str) -> bool:
        if not msg:
            return False

        msg = msg.lower()
        patterns = [
            "table not found",
            "table or view not found",
            "object does not exist",
        ]
        return any(p in msg for p in patterns)
