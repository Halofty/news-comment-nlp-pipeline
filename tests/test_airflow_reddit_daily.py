from __future__ import annotations

from pathlib import Path

import pytest

from orchestration.reddit_daily import (
    build_collection_command,
    collect_daily_comments,
    prepare_collected_spark_config,
    prepare_daily_config,
)


def _params() -> dict:
    return {
        "start_date": "2016-01-01",
        "end_date": "2016-01-01",
        "limit": 1000,
        "output_root": "data/airflow-output",
        "output_format": "parquet",
        "partitions": 2,
        "spark_master": "local[2]",
    }


def test_prepare_daily_config_derives_month_and_output(tmp_path: Path) -> None:
    config = prepare_daily_config(
        project_root=tmp_path, params=_params(), airflow_run_id="day-before"
    )
    assert config["collection"]["month"] == "2016-01"
    assert config["collection"]["input_file"].endswith("reddit-2016-01-01.jsonl")


def test_prepare_daily_config_rejects_multi_day_range(tmp_path: Path) -> None:
    params = _params()
    params["end_date"] = "2016-01-02"
    with pytest.raises(ValueError, match="exactly 1"):
        prepare_daily_config(
            project_root=tmp_path, params=params, airflow_run_id="invalid"
        )


def test_collection_command_calls_reddit_collector_with_dates(tmp_path: Path) -> None:
    config = prepare_daily_config(
        project_root=tmp_path, params=_params(), airflow_run_id="day-before"
    )
    command = build_collection_command(config)
    assert command[1:3] == ["-m", "collectors.reddit"]
    assert command[command.index("--month") + 1] == "2016-01"
    assert command[command.index("--start-date") + 1] == "2016-01-01"
    assert command[command.index("--limit") + 1] == "1000"


def test_collection_output_feeds_existing_spark_config(tmp_path: Path) -> None:
    config = prepare_daily_config(
        project_root=tmp_path, params=_params(), airflow_run_id="day-before"
    )

    def fake_runner(command, *, cwd, check, text):
        assert check is False
        output = cwd / config["collection"]["input_file"]
        output.write_text(
            '{"event_time":"2016-01-01T00:00:00Z"}\n' * 1000,
            encoding="utf-8",
        )

    collected = collect_daily_comments(config, runner=fake_runner)
    spark_config = prepare_collected_spark_config(config=config, collected=collected)
    assert collected["collected_rows"] == 1000
    assert spark_config["input_file"] == config["collection"]["input_file"]
    assert spark_config["run_label"] == "reddit-2016-01-01"
