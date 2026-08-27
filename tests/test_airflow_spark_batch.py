from __future__ import annotations

import json
from pathlib import Path

import pytest

from orchestration.spark_batch import (
    build_spark_command,
    prepare_run_config,
    run_spark_batch,
    verify_report,
)


def _params(input_file: str = "sample/events.jsonl") -> dict:
    return {
        "input_file": input_file,
        "run_label": "assignment run 1",
        "output_root": "data/airflow-output",
        "output_format": "jsonl",
        "partitions": 2,
        "spark_master": "local[2]",
    }


def test_prepare_run_config_resolves_unique_parameterized_paths(tmp_path: Path) -> None:
    sample = tmp_path / "sample"
    sample.mkdir()
    (sample / "events.jsonl").write_text("{}\n", encoding="utf-8")

    config = prepare_run_config(
        project_root=tmp_path,
        params=_params(),
        airflow_run_id="manual__2026-08-27T08:00:00+00:00",
    )

    assert config["input_file"] == "sample/events.jsonl"
    assert config["run_label"] == "assignment-run-1"
    assert config["run_directory"].startswith(
        "data/airflow-output/assignment-run-1/manual__2026-08-27T08-00-00-00-00"
    )
    assert config["output_format"] == "jsonl"


def test_prepare_run_config_rejects_path_escape(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="inside the project"):
        prepare_run_config(
            project_root=tmp_path,
            params=_params("../secret.jsonl"),
            airflow_run_id="manual-test",
        )


def test_build_command_uses_existing_spark_module(tmp_path: Path) -> None:
    sample = tmp_path / "sample"
    sample.mkdir()
    (sample / "events.jsonl").write_text("{}\n", encoding="utf-8")
    config = prepare_run_config(
        project_root=tmp_path,
        params=_params(),
        airflow_run_id="manual-test",
    )
    command = build_spark_command(config)
    assert command[1:3] == ["-m", "spark_jobs.process_sample"]
    assert command[command.index("--input") + 1] == "sample/events.jsonl"
    assert command[command.index("--format") + 1] == "jsonl"


def test_run_and_verify_report(tmp_path: Path) -> None:
    sample = tmp_path / "sample"
    sample.mkdir()
    (sample / "events.jsonl").write_text("{}\n", encoding="utf-8")
    config = prepare_run_config(
        project_root=tmp_path,
        params=_params(),
        airflow_run_id="manual-test",
    )

    def fake_runner(command, *, cwd, check, text):
        report = cwd / config["report_path"]
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
                    "runtime": {"duration_seconds": 3.5},
                }
            ),
            encoding="utf-8",
        )

    report_path = run_spark_batch(config, runner=fake_runner)
    summary = verify_report(project_root=tmp_path, report_path=report_path)
    assert summary["input_rows"] == 100
    assert summary["accounted_rows"] == 100
    assert summary["unique_valid_rows"] == 99
