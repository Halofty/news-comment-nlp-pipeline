from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task

from orchestration.llm_batch import (
    prepare_config,
    prepare_requests,
    submit_or_dry_run,
    verify_submission,
)

PROJECT_ROOT = Path("/opt/airflow/project")

with DAG(
    dag_id="llm_batch_pipeline",
    description="Prepare and optionally submit GPT-5.6 Luna Batch requests",
    schedule=None,
    start_date=datetime(2026, 9, 2, tzinfo=timezone.utc),
    catchup=False,
    default_args={"owner": "news-comment-nlp-pipeline", "retries": 0},
    params={
        "input_path": Param("sample/synthetic-events.jsonl", type="string"),
        "output_root": Param("data/airflow-output/llm-batch", type="string"),
        "model": Param("gpt-5.6-luna", const="gpt-5.6-luna"),
        "limit": Param(100, type="integer", minimum=1, maximum=50_000),
        "daily_budget_usd": Param("1.00", type="string"),
        "submit": Param(
            False,
            type="boolean",
            description="False performs a cost-safe dry-run; true requires OPENAI_API_KEY",
        ),
    },
    tags=["llm", "openai-batch", "langfuse", "manual"],
) as dag:

    @task(task_id="prepare_parameters")
    def resolve_parameters() -> dict:
        context = get_current_context()
        return prepare_config(
            project_root=PROJECT_ROOT,
            params=context["params"],
            airflow_run_id=context["run_id"],
        )

    @task(task_id="build_and_budget_check")
    def build_requests(config: dict) -> dict:
        return prepare_requests(config)

    @task(task_id="submit_or_dry_run")
    def submit(config: dict, preflight: dict) -> dict:
        return submit_or_dry_run(config, preflight)

    @task(task_id="verify_preflight_and_submission")
    def verify(preflight: dict, submission: dict) -> dict:
        result = verify_submission(preflight, submission)
        print("LLM Batch verification:", result)
        return result

    config = resolve_parameters()
    preflight = build_requests(config)
    verify(preflight, submit(config, preflight))
