from __future__ import annotations

import json
import sys
from types import SimpleNamespace

from serving.repository import load_dashboard_data
from serving.snapshot import build_serving_snapshot, load_serving_snapshots


class _Cursor:
    def __init__(self) -> None:
        self.query_number = 0
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, _query, _params=None) -> None:
        self.query_number += 1
        self.executions.append((_query, _params))

    def fetchone(self):
        return (32, 4, 20, 3, 5, 0.125, 0.625)

    def fetchall(self):
        values = {
            2: [("neutral", 20), ("mixed", 5)],
            3: [("high", 20), ("medium", 7)],
            4: [("economy", 12), ("policy", 7)],
            5: [
                (
                    "batch-1",
                    "completed",
                    "gpt-5.6-luna",
                    1,
                    1,
                    0,
                    100,
                    20,
                    0.000022,
                    None,
                    None,
                )
            ],
            6: [
                (
                    "event-1",
                    "neutral",
                    0.0,
                    {"positive": 0.2, "neutral": 0.6, "negative": 0.2},
                    "medium",
                    0.5,
                    [{"tone": "hope", "share": 1.0, "drivers": ["recovery"]}],
                    [{"tone": "anxiety", "share": 1.0, "drivers": ["prices"]}],
                    ["economy"],
                    ["policy"],
                    "A summary.",
                    "gpt-5.6-luna",
                    None,
                    "batch-1",
                )
            ],
        }
        return values[self.query_number]


class _Connection:
    def __init__(self) -> None:
        self.cursor_value = _Cursor()

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self):
        return self.cursor_value


def test_dashboard_reads_final_llm_tables(monkeypatch) -> None:
    connection = _Connection()
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda _dsn: connection),
    )
    result = load_dashboard_data(dsn="postgresql://test")
    assert result["analysis_version"] == "v3"
    assert all(
        params and params[0] == r"(^|-)v3($|-)"
        for _query, params in connection.cursor_value.executions
    )
    assert result["summary"]["analysis_rows"] == 32
    assert result["topics"][0] == {"topic": "economy", "row_count": 12}
    assert result["polarizations"][0] == {"polarization": "high", "row_count": 20}
    assert result["analyses"][0]["summary"] == "A summary."
    assert result["analyses"][0]["negative_tones"][0]["tone"] == "anxiety"
    assert connection.cursor_value.executions[4][1][-1] == 50
    assert connection.cursor_value.executions[5][1][-1] == 50


def test_dashboard_analysis_limit_does_not_change_batch_limit(monkeypatch) -> None:
    connection = _Connection()
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda _dsn: connection),
    )

    load_dashboard_data(dsn="postgresql://test", row_limit=120)

    assert connection.cursor_value.executions[4][1][-1] == 50
    assert connection.cursor_value.executions[5][1][-1] == 120


def test_dashboard_ui_scopes_batch_metrics_to_selected_row_limit(monkeypatch) -> None:
    import pytest

    pytest.importorskip("streamlit")
    from serving.dashboard import _load_database

    connection = _Connection()
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda _dsn: connection),
    )

    _load_database.clear()
    _load_database("postgresql://test", "v3", 120)

    assert connection.cursor_value.executions[4][1][-1] == 120
    assert connection.cursor_value.executions[5][1][-1] == 120


def test_dashboard_can_include_all_analysis_versions(monkeypatch) -> None:
    connection = _Connection()
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda _dsn: connection),
    )

    result = load_dashboard_data(
        dsn="postgresql://test", analysis_version="all"
    )

    assert result["analysis_version"] == "all"
    assert all(
        "TRUE" in query and (not params or len(params) == 1)
        for query, params in connection.cursor_value.executions
    )


def test_serving_snapshot_reads_persisted_spark_report(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_STORAGE_BACKEND", "local")
    report = tmp_path / "data/run/report.json"
    report.parent.mkdir(parents=True)
    report.write_text(
        json.dumps(
            {
                "row_accounting": {
                    "input_rows": 100,
                    "accounted_rows": 100,
                    "unique_valid_rows": 99,
                    "duplicate_event_id_rows": 1,
                    "contract_rejected_rows": 0,
                },
                "runtime": {"duration_seconds": 2.5},
                "source_counts": {"comment": 99},
                "quality_status_counts": {"accept": 99},
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "data/run/serving-snapshot.json"
    result = build_serving_snapshot(
        project_root=tmp_path,
        spark_report_path="data/run/report.json",
        minio_result={"status": "completed", "object_count": 2, "total_bytes": 1000},
        llm_result={
            "prepared_rows": 10,
            "skipped_rows": 0,
            "budget_status": "ok",
            "submission_status": "dry_run",
        },
        output_path="data/run/serving-snapshot.json",
    )
    assert result["unique_valid_rows"] == 99
    assert result["object_storage"]["object_count"] == 2
    assert json.loads(output.read_text())["llm"]["prepared_rows"] == 10
    assert load_serving_snapshots(data_root=tmp_path / "data", limit=1)[0][
        "input_rows"
    ] == 100
