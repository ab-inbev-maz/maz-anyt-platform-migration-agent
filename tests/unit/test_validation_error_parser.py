from __future__ import annotations

from brewbridge.domain.tools.validation_error_parser import parse_validation_output


def test_parse_mixed_blocks_and_grouping():
    raw = """\
2025 | ⚠ WARNING | [DAG_VALIDATION_ERROR_TASK_LEVEL] | File Name: pipeline_factory.py
  ├─ Task Name: test_task
  ├─ Yaml Key: acl
  ├─ Error Message: null value not allowed
  └─ Yaml File: /tmp/sap_acl.yaml

[DAG_VALIDATION_ERROR_BASE_LEVEL]
  ├─ Yaml Key: public_dag
  ├─ Error Message: must be of boolean type
  └─ Yaml File: /tmp/sap_acl.yaml

2025 | ⚠ WARNING | [ERROR_OCCURRED] | File Name: error_handler.py
  ├─ Error Code: ES0010999
  ├─ Category: VALIDATION
  ├─ Severity: MEDIUM
  ├─ User Message: Internal Error
  ├─ Yaml File: //tmp/other.py
"""

    parsed = parse_validation_output(raw)
    assert len(parsed) == 2

    other_file = next(item for item in parsed if item["file_path"] == "/tmp/other.py")
    assert other_file["file_type"] == "py"
    assert other_file["errors"][0]["error_code"] == "ES0010999"
    assert other_file["errors"][0]["message"] == "Internal Error"

    acl_file = next(item for item in parsed if item["file_path"] == "/tmp/sap_acl.yaml")
    assert acl_file["file_type"] == "yaml"
    messages = {err["message"] for err in acl_file["errors"]}
    assert "null value not allowed" in messages
    assert "must be of boolean type" in messages
    tasks = {err["task_name"] for err in acl_file["errors"]}
    assert "test_task" in tasks


def test_parse_handles_missing_fields_and_ipynb():
    raw = """\
    Info line we should ignore
    Yaml File: /tmp/notebook.ipynb
    [ERROR_OCCURRED]
      ├─ Error Code: ES999
      ├─ Category: RUNTIME
      ├─ Severity: HIGH
      ├─ Technical Message: Traceback line
      ├─ File Name: /tmp/notebook.ipynb
    [DAG_VALIDATION_FAILED]
      └─ Yaml File: /tmp/notebook.ipynb
    """
    parsed = parse_validation_output(raw)
    assert len(parsed) == 1
    entry = parsed[0]
    assert entry["file_type"] == "ipynb"
    err = entry["errors"][0]
    assert err["yaml_key"] is None
    assert err["task_name"] is None
    assert err["message"] == "Traceback line"


def test_parse_cli_block_with_timestamped_prefix():
    raw = """\
2025-12-05 21:22:31 UTC | ✗ ERROR   | [CLI_DAG_VALIDATION_FAILED] | File Name: cli.py
  ├─ Error Message: [ES0010102] DAG validation error: There were errors in the DAG validation. Please check the logs for more details.
  └─ Command: ingestion
"""
    parsed = parse_validation_output(raw)
    assert len(parsed) == 1
    entry = parsed[0]
    assert entry["file_path"] == "cli.py"
    assert entry["errors"][0]["tag"] == "CLI_DAG_VALIDATION_FAILED"
