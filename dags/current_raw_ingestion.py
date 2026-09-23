from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import DAG, Param, get_current_context, task

from collectors.current.models import CollectionWindow
from orchestration.current_ingestion import collect_google_news_raw, collect_reddit_raw
from orchestration.error_notifications import notify_task_failure

PROJECT_ROOT = Path("/opt/airflow/project")
KST = ZoneInfo("Asia/Seoul")


def _parse_optional(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("manual window timestamps must include a timezone")
    return parsed


with DAG(
    dag_id="current_raw_ingestion",
    description="Collect current Google News economy and four Reddit communities",
    schedule="5,35 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "news-comment-nlp-pipeline",
        "retries": 2,
        "on_failure_callback": notify_task_failure,
    },
    params={
        "window_start": Param(None, type=["null", "string"]),
        "window_end": Param(None, type=["null", "string"]),
    },
    tags=["current", "raw", "google-news", "reddit"],
) as dag:

    @task(task_id="resolve_collection_window")
    def resolve_collection_window() -> dict[str, str]:
        context = get_current_context()
        params = context["params"]
        manual_start = _parse_optional(params.get("window_start"))
        manual_end = _parse_optional(params.get("window_end"))
        if (manual_start is None) != (manual_end is None):
            raise ValueError("window_start and window_end must be provided together")
        if manual_start is not None and manual_end is not None:
            window = CollectionWindow(manual_start, manual_end)
        else:
            window = CollectionWindow(
                context["data_interval_start"].in_timezone(KST),
                context["data_interval_end"].in_timezone(KST),
            )
        return window.to_dict()

    @task(task_id="collect_google_news")
    def collect_google_news(payload: dict[str, str]) -> dict:
        return collect_google_news_raw(
            project_root=PROJECT_ROOT,
            requested_window=CollectionWindow(
                datetime.fromisoformat(payload["window_start"]),
                datetime.fromisoformat(payload["window_end"]),
            ),
        )

    @task(task_id="collect_reddit")
    def collect_reddit(payload: dict[str, str]) -> dict:
        return collect_reddit_raw(
            project_root=PROJECT_ROOT,
            requested_window=CollectionWindow(
                datetime.fromisoformat(payload["window_start"]),
                datetime.fromisoformat(payload["window_end"]),
            ),
        )

    collection_window = resolve_collection_window()
    collect_google_news(collection_window)
    collect_reddit(collection_window)

