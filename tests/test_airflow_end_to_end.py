from __future__ import annotations

from pathlib import Path

from orchestration.end_to_end import prepare_llm_from_spark


def test_spark_output_becomes_llm_input(tmp_path: Path) -> None:
    events = tmp_path / "data/run/output/events"
    events.mkdir(parents=True)
    config = prepare_llm_from_spark(
        project_root=tmp_path,
        pipeline_config={"airflow_run_id": "manual__end-to-end"},
        spark_config={
            "output_path": "data/run/output",
            "output_format": "parquet",
        },
        spark_verification={
            "report_path": "data/run/report.json",
            "unique_valid_rows": 81,
        },
        params={
            "llm_output_root": "data/llm",
            "model": "gpt-5.6-luna",
            "llm_limit": 100,
            "daily_budget_usd": "1.00",
            "submit": False,
        },
    )

    assert config["input_path"] == str(events)
    assert config["limit"] == 81
    assert config["spark_unique_valid_rows"] == 81
    assert config["submit"] is False
