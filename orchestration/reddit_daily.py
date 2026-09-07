from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping

import yaml

from orchestration.spark_batch import prepare_run_config

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ALLOWED_GROUPS = {"politics", "economy", "technology", "environment"}


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
    if limit < 0:
        raise ValueError("limit must be 0 (unlimited) or a positive integer")

    selected_groups = [str(value) for value in params.get("selected_groups", [])]
    if selected_groups:
        if not 1 <= len(selected_groups) <= 4:
            raise ValueError("selected_groups must contain between 1 and 4 groups")
        if len(selected_groups) != len(set(selected_groups)):
            raise ValueError("selected_groups must not contain duplicates")
        unknown_groups = set(selected_groups) - ALLOWED_GROUPS
        if unknown_groups:
            raise ValueError(f"unknown analysis groups: {sorted(unknown_groups)}")
        groups_config = str(
            params.get("analysis_groups_config", "config/analysis-groups.yaml")
        )
        groups_path = (project_root / groups_config).resolve()
        try:
            groups_path.relative_to(project_root.resolve())
        except ValueError as error:
            raise ValueError("analysis_groups_config must stay inside project") from error
        if not groups_path.is_file():
            raise FileNotFoundError(groups_path)
        group_definitions = yaml.safe_load(groups_path.read_text(encoding="utf-8"))[
            "groups"
        ]
        selected_subreddits = sorted(
            {
                str(subreddit)
                for group in selected_groups
                for subreddit in group_definitions[group]["subreddits"]
            },
            key=str.casefold,
        )
        selected_group_labels = [
            str(group_definitions[group]["label"]) for group in selected_groups
        ]
    else:
        selected_subreddits = []
        selected_group_labels = []

    selected_date = start_date.isoformat()
    input_file = f"data/airflow-input/reddit-{selected_date}.jsonl"
    archive_root = str(
        params.get("reddit_archive_root", "data/raw/reddit-archive/data")
    )
    archive_path = (project_root / archive_root / f"RC_{start_date:%Y-%m}.parquet").resolve()
    try:
        archive_path.relative_to(project_root.resolve())
    except ValueError as error:
        raise ValueError("reddit_archive_root must stay inside project") from error
    source_mode = str(params.get("reddit_source_mode", "auto"))
    if source_mode not in {"auto", "local", "remote"}:
        raise ValueError("reddit_source_mode must be auto, local, or remote")
    if source_mode == "local" and not archive_path.is_file():
        raise FileNotFoundError(archive_path)
    use_local_archive = source_mode != "remote" and archive_path.is_file()
    collection = {
        "month": start_date.strftime("%Y-%m"),
        "start_date": selected_date,
        "end_date": selected_date,
        "limit": limit,
        "input_file": input_file,
        "input_parquet": (
            str(archive_path.relative_to(project_root.resolve()))
            if use_local_archive
            else None
        ),
        "source_mode": "local" if use_local_archive else "remote",
        "selected_groups": selected_groups,
        "selected_group_labels": selected_group_labels,
        "subreddits": selected_subreddits,
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
    command = [
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
    if collection.get("input_parquet"):
        command.extend(["--input-parquet", str(collection["input_parquet"])])
    for subreddit in collection.get("subreddits") or []:
        command.extend(["--subreddit", str(subreddit)])
    return command


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
    minimum_rows = 100 if int(collection["limit"]) == 0 else min(
        100, int(collection["limit"])
    )
    if row_count < minimum_rows:
        raise ValueError(
            f"Reddit collection returned {row_count} usable events; "
            f"at least {minimum_rows} are required"
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
