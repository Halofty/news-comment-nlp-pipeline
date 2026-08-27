from __future__ import annotations

import re
import subprocess
import sys
from datetime import date, datetime, time, timezone
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
    query = str(params["query"]).strip()
    if not query:
        raise ValueError("query must not be empty")
    max_records = int(params["max_records"])
    if not 100 <= max_records <= 250:
        raise ValueError("max_records must be between 100 and 250")

    selected_date = start_date.isoformat()
    input_file = f"data/airflow-input/gdelt-{selected_date}.jsonl"
    collection = {
        "query": query,
        "start_date": selected_date,
        "end_date": selected_date,
        "gdelt_start": datetime.combine(
            start_date, time.min, tzinfo=timezone.utc
        ).strftime("%Y%m%d%H%M%S"),
        "gdelt_end": datetime.combine(
            end_date, time.max, tzinfo=timezone.utc
        ).strftime("%Y%m%d%H%M%S"),
        "max_records": max_records,
        "input_file": input_file,
    }
    return {
        "project_root": str(project_root.resolve()),
        "airflow_run_id": airflow_run_id,
        "collection": collection,
        "spark_params": {
            "input_file": input_file,
            "run_label": f"gdelt-{selected_date}",
            "output_root": str(params["output_root"]),
            "output_format": str(params["output_format"]),
            "partitions": int(params["partitions"]),
            "spark_master": str(params["spark_master"]),
        },
    }


def build_collection_command(config: Mapping[str, Any]) -> list[str]:
    collection = config["collection"]
    # 일부 API 항목은 title/url/seendate가 없어 TextEvent로 변환되지 않는다.
    # 목표 건수보다 10건을 더 요청해 최소 100개 유효 이벤트를 확보한다.
    requested_records = min(250, int(collection["max_records"]) + 10)
    return [
        sys.executable,
        "-m",
        "collectors.gdelt",
        "--query",
        str(collection["query"]),
        "--max-records",
        str(requested_records),
        "--start",
        str(collection["gdelt_start"]),
        "--end",
        str(collection["gdelt_end"]),
        "--output",
        str(collection["input_file"]),
    ]


def collect_daily_articles(
    config: Mapping[str, Any],
    *, runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    project_root = Path(str(config["project_root"]))
    collection = dict(config["collection"])
    input_path = project_root / str(collection["input_file"])
    input_path.parent.mkdir(parents=True, exist_ok=True)
    command = build_collection_command(config)
    print("Collecting:", " ".join(command))
    runner(command, cwd=project_root, check=True, text=True)
    if not input_path.is_file():
        raise RuntimeError(f"GDELT collection did not create {input_path}")
    row_count = sum(1 for line in input_path.open(encoding="utf-8") if line.strip())
    if row_count < 100:
        raise ValueError(
            f"GDELT collection returned {row_count} usable events; at least 100 are required"
        )
    collection["collected_rows"] = row_count
    print(f"Collected {row_count} GDELT events for {collection['start_date']}")
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
    spark_config["query"] = str(collected["query"])
    spark_config["start_date"] = str(collected["start_date"])
    spark_config["end_date"] = str(collected["end_date"])
    spark_config["collected_rows"] = int(collected["collected_rows"])
    return spark_config
