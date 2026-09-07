from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping

from collectors.google_news import collect_month
from core.events import validate_event
from orchestration.reddit_daily import prepare_daily_config
from storage.jsonl import write_jsonl


def prepare_date_range_configs(
    *, project_root: Path, params: Mapping[str, Any], airflow_run_id: str
) -> list[dict[str, Any]]:
    """Expand an inclusive date range into isolated daily run configurations."""
    try:
        start_date = date.fromisoformat(str(params["start_date"]))
        end_date = date.fromisoformat(str(params["end_date"]))
    except ValueError as error:
        raise ValueError("start_date and end_date must use YYYY-MM-DD format") from error
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    safe_run_id = "".join(
        character if character.isalnum() or character in "-_" else "_"
        for character in airflow_run_id
    )
    configs: list[dict[str, Any]] = []
    current_date = start_date
    while current_date <= end_date:
        selected_date = current_date.isoformat()
        daily_params = {
            **params,
            "start_date": selected_date,
            "end_date": selected_date,
        }
        config = prepare_daily_config(
            project_root=project_root,
            params=daily_params,
            airflow_run_id=airflow_run_id,
        )
        run_directory = (
            Path(str(params["output_root"]))
            / f"date={selected_date}"
            / safe_run_id
        )
        config.update(
            {
                "range_start_date": start_date.isoformat(),
                "range_end_date": end_date.isoformat(),
                "run_directory": str(run_directory),
                "combined_input_file": str(run_directory / "combined-input.jsonl"),
                "web_news_root": str(params["web_news_root"]),
                "web_news_source_mode": str(params["web_news_source_mode"]),
            }
        )
        configs.append(config)
        current_date += timedelta(days=1)
    return configs


def collect_web_news(config: Mapping[str, Any], *, request_delay: float = 1.0) -> dict[str, Any]:
    project_root = Path(str(config["project_root"]))
    selected_date = date.fromisoformat(str(config["collection"]["start_date"]))
    run_directory = project_root / str(config["run_directory"])
    source_mode = str(config.get("web_news_source_mode", "auto"))
    if source_mode not in {"auto", "local", "remote"}:
        raise ValueError("web_news_source_mode must be auto, local, or remote")
    local_root = project_root / str(config.get("web_news_root", "data/raw/google-news"))
    local_daily = (
        local_root
        / f"year={selected_date:%Y}"
        / f"month={selected_date:%m}"
        / f"day={selected_date:%d}"
        / "events.jsonl"
    )
    if source_mode != "remote" and local_daily.is_file():
        row_count = sum(bool(line.strip()) for line in local_daily.read_text(encoding="utf-8").splitlines())
        return {
            "input_file": str(local_daily.relative_to(project_root)),
            "report_path": None,
            "row_count": row_count,
            "source_mode": "local",
        }
    if source_mode == "local":
        raise FileNotFoundError(local_daily)
    output_root = run_directory / "google-news"
    report_path = run_directory / "google-news-report.json"
    report = collect_month(
        start_date=selected_date,
        end_date=selected_date,
        output_root=output_root,
        report_path=report_path,
        request_delay=request_delay,
        topic_groups=tuple(config["collection"]["selected_groups"]),
    )
    daily_path = (
        output_root
        / f"year={selected_date:%Y}"
        / f"month={selected_date:%m}"
        / f"day={selected_date:%d}"
        / "events.jsonl"
    )
    if not daily_path.is_file():
        daily_path.parent.mkdir(parents=True, exist_ok=True)
        daily_path.write_text("", encoding="utf-8")
    return {
        "input_file": str(daily_path.relative_to(project_root)),
        "report_path": str(report_path.relative_to(project_root)),
        "row_count": int(report["events_within_requested_dates"]),
        "source_mode": "remote",
    }


def merge_daily_sources(
    config: Mapping[str, Any], reddit_result: Mapping[str, Any], news_result: Mapping[str, Any]
) -> dict[str, Any]:
    project_root = Path(str(config["project_root"]))
    paths = [
        project_root / str(reddit_result["input_file"]),
        project_root / str(news_result["input_file"]),
    ]
    events: dict[str, dict[str, Any]] = {}
    source_rows: dict[str, int] = {"reddit": 0, "web_news": 0}
    for path in paths:
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            event = validate_event(json.loads(line), line_number=line_number)
            source_rows[str(event["source_name"])] += 1
            events.setdefault(str(event["event_id"]), event)
    if not events:
        raise ValueError("Reddit and web-news collection produced no events")
    target = project_root / str(config["combined_input_file"])
    write_jsonl(events.values(), target)
    return {
        "input_file": str(target.relative_to(project_root)),
        "input_rows": sum(source_rows.values()),
        "unique_rows": len(events),
        "source_rows": source_rows,
    }
