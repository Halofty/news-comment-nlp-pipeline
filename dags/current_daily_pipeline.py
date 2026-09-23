from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import DAG, Param, get_current_context, task

from orchestration.current_daily import finalize_current_day, resolve_analysis_dates
from orchestration.error_notifications import notify_task_failure

PROJECT_ROOT = Path("/opt/airflow/project")
KST = ZoneInfo("Asia/Seoul")


with DAG(
    dag_id="current_daily_pipeline",
    description="Finalize complete current-data partitions after the KST day boundary",
    schedule="10 0 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "news-comment-nlp-pipeline",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_task_failure,
    },
    params={
        "start_date": Param(None, type=["null", "string"], pattern="^\\d{4}-\\d{2}-\\d{2}$"),
        "end_date": Param(None, type=["null", "string"], pattern="^\\d{4}-\\d{2}-\\d{2}$"),
    },
    tags=["current", "daily", "finalization"],
) as dag:

    @task(task_id="resolve_analysis_dates")
    def resolve_dates() -> list[str]:
        context = get_current_context()
        run_type = str(context["dag_run"].run_type).casefold()
        return resolve_analysis_dates(
            data_interval_end=context["data_interval_end"],
            scheduled="scheduled" in run_type,
            start_date=context["params"].get("start_date"),
            end_date=context["params"].get("end_date"),
        )

    @task(task_id="finalize_current_day")
    def finalize_day(value: str) -> dict:
        return finalize_current_day(project_root=PROJECT_ROOT, analysis_date=date.fromisoformat(value))

    finalize_day.expand(value=resolve_dates())
