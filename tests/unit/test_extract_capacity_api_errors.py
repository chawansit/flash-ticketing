#!/usr/bin/env python3
import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "extract_capacity_api_errors.py"
SPEC = importlib.util.spec_from_file_location("extract_capacity_api_errors", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def test_summarizes_errors_without_sensitive_fields():
    rows = [
        json.dumps({"message": "request", "time": "2026-09-19T16:00:00Z",
                    "request_id": "r1", "route": "/v1/holds", "status": 503,
                    "error_code": "DATABASE_UNAVAILABLE", "db_failure_type": "PoolTimeout",
                    "authorization": "secret-token"}),
        json.dumps({"message": "request", "status": 201, "error_code": None}),
        "INFO: unrelated server line",
    ]
    result = module.summarize(rows, 20)
    assert result["request_log_lines"] == 2
    assert result["error_codes"] == {"DATABASE_UNAVAILABLE": 1}
    assert result["db_failure_types"] == {"PoolTimeout": 1}
    assert result["last_errors"][0]["db_failure_type"] == "PoolTimeout"
    assert result["last_errors"][0]["request_id"] == "r1"
    assert "secret-token" not in json.dumps(result)


def test_rejects_unexpected_or_malformed_failure_types():
    rows = [
        json.dumps({"message": "request", "error_code": "DATABASE_UNAVAILABLE",
                    "db_failure_type": "secret=private"}),
        json.dumps({"message": "request", "error_code": "DATABASE_UNAVAILABLE",
                    "db_failure_type": ["PoolTimeout"]}),
    ]
    result = module.summarize(rows, 2)
    assert result["db_failure_types"] == {}
    assert [item["db_failure_type"] for item in result["last_errors"]] == [None, None]
    assert "private" not in json.dumps(result)
