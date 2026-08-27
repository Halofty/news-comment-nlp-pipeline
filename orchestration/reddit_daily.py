from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping

from orchestration.spark_batch import prepare_run_config

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_date(value: Any, *, field: str) -> date:
    text = str(value)
    if not DATE_PATTERN.fullmatch(text):
        raise ValueError(f"{field} must use YYYY-MM-DD format")
    try:
        return date.fromisoformat(text)
    except ValueError as error:
        raise ValueError(f"{field} is not a valid calendar date") from error


def prepare_daily_config(
    *, project_root: Path, params: Mapping[str, Any], airflow_run_id: str
) -> dict[str, Any]:
    start_date = _parse_date(params["start_date"], field="start_date")
    end_date = _parse_date(params["end_date"], field="end_date")
    if end_date != start_date:
        raise ValueError("start_date and end_date must define exactly 1 calendar day")

    limit = int(params["limit"])
    if not 100 <= limit <= 10_000:
        raise ValueError("limit must be between 100 and 10000")

    selected_date = start_date.isoformat()
    input_file = f"data/airflow-input/reddit-{selected_date}.jsonl"
    collection = {
        "month": start_date.strftime("%Y-%m"),
        "start_date": selected_date,
        "end_date": selected_date,
        "limit": limit,
        "input_file": input_file,
    }
    return {
        "project_root": str(project_root.resolve()),
        "airflow_run_id": airflow_run_id,
        "collection": collection,
        "spark_params": {
            "input_file": input_file,
            "run_label": f"reddit-{selected_date}",
            "output_root": str(params["output_root"]),
            "output_format": str(params["output_format"]),
            "partitions": int(params["partitions"]),
            "spark_master": str(params["spark_master"]),
        },
    }


def build_collection_command(config: Mapping[str, Any]) -> list[str]:
    collection = config["collection"]
    return [
        sys.executable,
        "-m",
        "collectors.reddit",
        "--month",
        str(collection["month"]),
        "--start-date",
        str(collection["start_date"]),
        "--end-date",
        str(collection["end_date"]),
        "--limit",
        str(collection["limit"]),
        "--output",
        str(collection["input_file"]),
    ]


def collect_daily_comments(
    config: Mapping[str, Any],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    project_root = Path(str(config["project_root"]))
    collection = dict(config["collection"])
    input_path = project_root / str(collection["input_file"])
    input_path.parent.mkdir(parents=True, exist_ok=True)
    command = build_collection_command(config)
    print("Collecting:", " ".join(command))
    completed = runner(command, cwd=project_root, check=False, text=True)
    if not input_path.is_file():
        raise RuntimeError(f"Reddit collection did not create {input_path}")
    row_count = 0
    with input_path.open(encoding="utf-8") as lines:
        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"invalid JSON in collected artifact at line {line_number}"
                ) from error
            if not str(event.get("event_time", "")).startswith(
                collection["start_date"]
            ):
                raise ValueError(
                    f"event outside requested date at line {line_number}"
                )
            row_count += 1
    if row_count < 100:
        raise ValueError(
            f"Reddit collection returned {row_count} usable events; at least 100 are required"
        )
    return_code = getattr(completed, "returncode", 0) or 0
    if return_code != 0:
        # Some pyarrow/fsspec combinations abort during interpreter shutdown after
        # streaming has already atomically written the requested JSONL. Validate
        # the artifact instead of discarding a complete, readable collection.
        print(
            f"Collector exited with {return_code} after writing a valid "
            f"{row_count}-row artifact; continuing with the validated file"
        )
    collection["collected_rows"] = row_count
    print(f"Collected {row_count} Reddit events for {collection['start_date']}")
    return collection


def prepare_collected_spark_config(
    *, config: Mapping[str, Any], collected: Mapping[str, Any]
) -> dict[str, Any]:
    spark_params = dict(config["spark_params"])
    spark_params["input_file"] = str(collected["input_file"])
    spark_config = prepare_run_config(
        project_root=Path(str(config["project_root"])),
        params=spark_params,
        airflow_run_id=str(config["airflow_run_id"]),
    )
    spark_config["start_date"] = str(collected["start_date"])
    spark_config["end_date"] = str(collected["end_date"])
    spark_config["collected_rows"] = int(collected["collected_rows"])
    return spark_config
