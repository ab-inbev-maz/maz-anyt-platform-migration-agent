from __future__ import annotations

import re
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, Tuple

FILE_LINE_RE = re.compile(r"(Yaml File|YAML File|File Name):\s*(.+)")
BLOCK_START_RE = re.compile(
    r"\[(ERROR_OCCURRED|DAG_VALIDATION_ERROR_TASK_LEVEL|DAG_VALIDATION_ERROR_BASE_LEVEL|CLI_DAG_VALIDATION_FAILED|DAG_VALIDATION_FAILED)\]"
)


def _normalize_file_path(raw_path: str) -> str:
    path = raw_path.strip().strip("`'\"")
    if path.startswith("//"):
        return path[1:]
    return path


def _deduce_file_type(file_path: str) -> str:
    lower = file_path.lower()
    if lower.endswith((".yaml", ".yml")):
        return "yaml"
    if lower.endswith(".py"):
        return "py"
    if lower.endswith(".ipynb"):
        return "ipynb"
    return "unknown"


def _extract_file_paths(lines: Iterable[str]) -> List[str]:
    paths: List[str] = []
    for line in lines:
        match = FILE_LINE_RE.search(line)
        if match:
            paths.append(_normalize_file_path(match.group(2)))
    return paths


def _extract_field(lines: List[str], patterns: List[str]) -> Optional[str]:
    for pat in patterns:
        for line in lines:
            if pat in line:
                value = line.rsplit(":", 1)[1].strip()
                return value or None
    return None


def _parse_message(lines: List[str]) -> Optional[str]:
    for key in ("Error Message:", "Technical Message:", "User Message:"):
        for line in lines:
            if key in line:
                return line.split(":", 1)[1].strip() or None
    return None


def _default_message_for_tag(tag: str) -> Optional[str]:
    if tag == "DAG_VALIDATION_FAILED":
        return "DAG validation failed"
    if tag == "CLI_DAG_VALIDATION_FAILED":
        return "CLI DAG validation failed"
    return None


def _parse_block(
    tag: str, block_lines: List[str], fallback_file: Optional[str]
) -> Tuple[Optional[str], Dict[str, Any]]:
    file_path = (
        _extract_field(block_lines, ["Yaml File", "YAML File", "File Name"]) or fallback_file
    )
    if file_path:
        file_path = _normalize_file_path(file_path)

    message = _parse_message(block_lines) or _default_message_for_tag(tag)

    error = {
        "error_code": _extract_field(block_lines, ["Error Code"]),
        "category": _extract_field(block_lines, ["Category"]),
        "severity": _extract_field(block_lines, ["Severity"]),
        "yaml_key": _extract_field(block_lines, ["Yaml Key", "YAML Key"]),
        "message": message,
        "task_name": _extract_field(block_lines, ["Task Name"]),
        "tag": tag,
    }
    return file_path, error


def _iter_blocks(lines: List[str]) -> Iterable[Tuple[str, List[str]]]:
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        start_match = BLOCK_START_RE.search(line)
        if not start_match:
            idx += 1
            continue

        tag = start_match.group(1)
        start = idx
        idx += 1
        while idx < len(lines) and not BLOCK_START_RE.search(lines[idx]):
            idx += 1
        yield tag, lines[start:idx]


def parse_validation_output(raw_output: str) -> List[Dict[str, Any]]:
    """
    Convert engineeringstore validation stdout/stderr into a structured list grouped by file.

    Returns:
        [
          {
            "file_path": "/path/to/file.yaml",
            "file_type": "yaml",
            "errors": [
              {
                "error_code": "ES0010999",
                "category": "VALIDATION",
                "severity": "MEDIUM",
                "yaml_key": "public_dag",
                "message": "must be of boolean type",
                "task_name": None,
              },
            ],
          },
        ]
    """
    if not raw_output:
        return []

    lines = raw_output.splitlines()
    file_map: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    last_seen_file: Optional[str] = None

    for tag, block in _iter_blocks(lines):
        block_file, error = _parse_block(tag, block, last_seen_file)
        if block_file:
            last_seen_file = block_file
            if block_file not in file_map:
                file_map[block_file] = {
                    "file_path": block_file,
                    "file_type": _deduce_file_type(block_file),
                    "errors": [],
                }
            # Only append if there is at least some message or field
            if any(v is not None for k, v in error.items() if k != "tag"):
                file_map[block_file]["errors"].append(error)

    # Deterministic ordering by file_path
    ordered = sorted(file_map.values(), key=lambda item: item["file_path"])
    return ordered
