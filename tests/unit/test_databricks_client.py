import pytest

from brewbridge.infrastructure.databricks_client import DatabricksClient
from brewbridge.utils.exceptions import (
    DatabricksExecutionError,
    DatabricksTimeoutError,
)


class FakeResponse:
    def __init__(self, json_data=None, status_code=200):
        self._json = json_data or {}
        self.status_code = status_code

    def json(self):
        return self._json


class FakeSession:
    """
    Minimal requests.Session stand-in to drive DatabricksClient polling logic.
    """

    def __init__(self, post_response, get_responses):
        self.post_response = post_response
        self.get_responses = list(get_responses)
        self.headers = {}

    def post(self, url, json, timeout):
        return self.post_response

    def get(self, url, timeout):
        if not self.get_responses:
            raise AssertionError("Unexpected GET call; no responses queued")
        return self.get_responses.pop(0)


@pytest.fixture(autouse=True)
def databricks_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://fake.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-123")


def test_run_query_polls_until_success(monkeypatch):
    post_response = FakeResponse({"statement_id": "abc"})
    get_responses = [
        FakeResponse({"status": {"state": "PENDING"}}),
        FakeResponse(
            {
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [[1]], "schema": {"columns": [{"name": "c1"}]}},
            }
        ),
    ]

    client = DatabricksClient()
    client.session = FakeSession(post_response, get_responses)
    client.poll_interval = 0  # speed up test

    payload = client.run_query("SELECT 1")

    assert payload["result"]["data_array"] == [[1]]


def test_run_query_missing_statement_id_raises():
    client = DatabricksClient()
    client.session = FakeSession(FakeResponse({"unexpected": "payload"}), [])

    with pytest.raises(DatabricksExecutionError):
        client.run_query("SELECT 1")


def test_ping_returns_true_when_rows(monkeypatch):
    client = DatabricksClient()

    def fake_run_query(sql):
        return {"result": {"data_array": [[1]]}}

    monkeypatch.setattr(client, "run_query", fake_run_query)

    assert client.ping() is True


def test_ping_passthrough_databricks_error(monkeypatch):
    client = DatabricksClient()

    def fake_run_query(sql):
        raise DatabricksTimeoutError("timeout")

    monkeypatch.setattr(client, "run_query", fake_run_query)

    with pytest.raises(DatabricksTimeoutError):
        client.ping()


def test_ping_wraps_unexpected_errors(monkeypatch):
    client = DatabricksClient()

    def fake_run_query(sql):
        raise ValueError("boom")

    monkeypatch.setattr(client, "run_query", fake_run_query)

    with pytest.raises(DatabricksExecutionError):
        client.ping()
