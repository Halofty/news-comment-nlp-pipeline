from __future__ import annotations

import json
from pathlib import Path

import pytest

from orchestration.llm_batch import (
    prepare_config,
    prepare_requests,
    submit_or_dry_run,
    verify_submission,
)


def test_llm_airflow_helpers_complete_dry_run(tmp_path: Path) -> None:
    sample = tmp_path / "sample.jsonl"
    sample.write_text(
        json.dumps(
            {
                "event_id": "event-1",
                "source_name": "reddit",
                "text": "A synthetic comment.",
            }
        )
        + "\n"
    )
    config = prepare_config(
        project_root=tmp_path,
        params={
            "input_path": "sample.jsonl",
            "output_root": "output",
            "model": "gpt-5.6-luna",
            "limit": 10,
            "daily_budget_usd": "1.00",
            "submit": False,
        },
        airflow_run_id="manual__2026-09-02T00:00:00+00:00",
    )
    preflight = prepare_requests(config)
    submission = submit_or_dry_run(config, preflight)
    result = verify_submission(preflight, submission)
    assert result["prepared_rows"] == 1
    assert result["submission_status"] == "dry_run"


def test_llm_airflow_budget_block_prevents_submission(tmp_path: Path) -> None:
    sample = tmp_path / "sample.jsonl"
    sample.write_text(
        json.dumps({"event_id": "event-1", "source_name": "reddit", "text": "x"})
        + "\n"
    )
    config = prepare_config(
        project_root=tmp_path,
        params={
            "input_path": "sample.jsonl",
            "output_root": "output",
            "model": "gpt-5.6-luna",
            "limit": 1,
            "daily_budget_usd": "0.000001",
            "submit": False,
        },
        airflow_run_id="blocked",
    )
    preflight = prepare_requests(config)
    with pytest.raises(RuntimeError, match="budget"):
        submit_or_dry_run(config, preflight)
