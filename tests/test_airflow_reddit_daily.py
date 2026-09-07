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


def test_prepare_daily_config_accepts_zero_as_unlimited(tmp_path: Path) -> None:
    params = _params()
    params["limit"] = 0

    config = prepare_daily_config(
        project_root=tmp_path, params=params, airflow_run_id="unlimited"
    )

    assert config["collection"]["limit"] == 0


def test_collection_command_calls_reddit_collector_with_dates(tmp_path: Path) -> None:
    config = prepare_daily_config(
        project_root=tmp_path, params=_params(), airflow_run_id="day-before"
    )
    command = build_collection_command(config)
    assert command[1:3] == ["-m", "collectors.reddit"]
    assert command[command.index("--month") + 1] == "2016-01"
    assert command[command.index("--start-date") + 1] == "2016-01-01"
    assert command[command.index("--limit") + 1] == "1000"


def test_local_month_archive_is_selected_from_date_param(tmp_path: Path) -> None:
    archive = tmp_path / "data/raw/reddit-archive/data/RC_2012-02.parquet"
    archive.parent.mkdir(parents=True)
    archive.touch()
    params = _params()
    params["start_date"] = "2012-02-01"
    params["end_date"] = "2012-02-01"

    config = prepare_daily_config(
        project_root=tmp_path, params=params, airflow_run_id="parameterized-date"
    )
    command = build_collection_command(config)

    assert config["collection"]["month"] == "2012-02"
    assert config["collection"]["input_parquet"] == (
        "data/raw/reddit-archive/data/RC_2012-02.parquet"
    )
    assert command[command.index("--input-parquet") + 1].endswith(
        "RC_2012-02.parquet"
    )


def test_remote_mode_downloads_day_even_when_local_archive_exists(tmp_path: Path) -> None:
    archive = tmp_path / "data/raw/reddit-archive/data/RC_2012-02.parquet"
    archive.parent.mkdir(parents=True)
    archive.touch()
    params = _params()
    params.update(
        {
            "start_date": "2012-02-01",
            "end_date": "2012-02-01",
            "reddit_source_mode": "remote",
        }
    )

    config = prepare_daily_config(
        project_root=tmp_path, params=params, airflow_run_id="remote-date"
    )
    command = build_collection_command(config)

    assert config["collection"]["source_mode"] == "remote"
    assert config["collection"]["input_parquet"] is None
    assert "--input-parquet" not in command


def test_multiple_analysis_groups_expand_to_subreddit_filters(tmp_path: Path) -> None:
    config_path = tmp_path / "config/analysis-groups.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        """groups:
  politics:
    label: politics-and-international
    subreddits: [politics, worldnews]
  economy:
    label: economy-and-society
    subreddits: [Economics, business]
  technology:
    label: technology-and-digital
    subreddits: [technology]
  environment:
    label: environment-and-science
    subreddits: [science]
""",
        encoding="utf-8",
    )
    params = _params()
    params["selected_groups"] = ["politics", "economy"]
    params["analysis_groups_config"] = "config/analysis-groups.yaml"

    config = prepare_daily_config(
        project_root=tmp_path, params=params, airflow_run_id="two-groups"
    )
    command = build_collection_command(config)

    assert config["collection"]["selected_groups"] == ["politics", "economy"]
    assert config["collection"]["selected_group_labels"] == [
        "politics-and-international",
        "economy-and-society",
    ]
    assert set(config["collection"]["subreddits"]) == {
        "politics",
        "worldnews",
        "Economics",
        "business",
    }
    assert command.count("--subreddit") == 4


def test_selected_groups_reject_empty_or_duplicate_values(tmp_path: Path) -> None:
    params = _params()
    params["selected_groups"] = ["economy", "economy"]
    with pytest.raises(ValueError, match="duplicates"):
        prepare_daily_config(
            project_root=tmp_path, params=params, airflow_run_id="duplicates"
        )


def test_collection_limit_has_no_upper_bound(tmp_path: Path) -> None:
    params = _params()
    params["limit"] = 1_000_000
    config = prepare_daily_config(
        project_root=tmp_path, params=params, airflow_run_id="large-limit"
    )
    assert config["collection"]["limit"] == 1_000_000


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
