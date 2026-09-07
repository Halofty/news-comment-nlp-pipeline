from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task

from jobs.watch_openai_batch import PreviewNotifier, watch_batch
from llm_analysis import OpenAIBatchClient
from notifications import SlackWebhookNotifier

PROJECT_ROOT = Path("/opt/airflow/project")


with DAG(
    dag_id="openai_batch_slack_notification",
    description="Wait for an OpenAI Batch, validate topics, and notify Slack",
    schedule=None,
    start_date=datetime(2026, 9, 4, tzinfo=timezone.utc),
    catchup=False,
    default_args={"owner": "news-comment-nlp-pipeline", "retries": 1},
    params={
        "batch_id": Param("batch_replace_me", type="string", minLength=1),
        "manifest_path": Param("data/llm/manifest.jsonl", type="string", pattern="^data/.+"),
        "output_root": Param("data/llm_response/slack-notification", type="string", pattern="^data/.+"),
        "poll_interval_seconds": Param(60, type="integer", minimum=30, maximum=3600),
        "timeout_seconds": Param(90000, type="integer", minimum=60, maximum=172800),
        "slack_enabled": Param(False, type="boolean"),
    },
    tags=["openai-batch", "slack", "notification", "manual"],
) as dag:

    @task(task_id="wait_validate_and_notify")
    def wait_validate_and_notify() -> dict:
        import os

        context = get_current_context()
        params = context["params"]
        batch_id = str(params["batch_id"])
        safe_batch_id = "".join(
            character if character.isalnum() or character in "-_" else "_"
            for character in batch_id
        )
        manifest = (PROJECT_ROOT / str(params["manifest_path"])).resolve()
        output_root = (PROJECT_ROOT / str(params["output_root"]) / safe_batch_id).resolve()
        for path in (manifest, output_root):
            try:
                path.relative_to(PROJECT_ROOT)
            except ValueError as error:
                raise ValueError("Batch notification paths must stay inside project") from error
        if not manifest.is_file():
            raise FileNotFoundError(manifest)
        if bool(params["slack_enabled"]):
            notifier = SlackWebhookNotifier(os.environ.get("SLACK_WEBHOOK_URL", ""))
        else:
            notifier = PreviewNotifier()
        return watch_batch(
            client=OpenAIBatchClient(),
            notifier=notifier,
            batch_id=batch_id,
            manifest_path=manifest,
            result_path=output_root / "raw-results.jsonl",
            validated_path=output_root / "validated-results.jsonl",
            batch_state_path=output_root / "batch-state.json",
            notification_state_path=output_root / "notification-state.json",
            poll_interval_seconds=int(params["poll_interval_seconds"]),
            timeout_seconds=int(params["timeout_seconds"]),
        )

    wait_validate_and_notify()
