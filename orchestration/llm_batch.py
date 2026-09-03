from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from llm_analysis import OpenAIBatchClient, build_batch_file


def prepare_config(
    *, project_root: Path, params: dict[str, Any], airflow_run_id: str
) -> dict[str, Any]:
    safe_run_id = "".join(
        character if character.isalnum() or character in "-_" else "_"
        for character in airflow_run_id
    )
    output_root = project_root / str(params["output_root"]) / safe_run_id
    input_path = project_root / str(params["input_path"])
    if not input_path.exists():
        raise FileNotFoundError(input_path)
    return {
        "input_path": str(input_path),
        "output_root": str(output_root),
        "request_path": str(output_root / "requests.jsonl"),
        "manifest_path": str(output_root / "manifest.jsonl"),
        "report_path": str(output_root / "preflight.json"),
        "state_path": str(output_root / "batch-state.json"),
        "model": str(params["model"]),
        "limit": int(params["limit"]),
        "daily_budget_usd": str(params["daily_budget_usd"]),
        "submit": bool(params["submit"]),
        "internal_batch_id": safe_run_id,
    }


def prepare_requests(config: dict[str, Any]) -> dict[str, Any]:
    result = build_batch_file(
        input_path=config["input_path"],
        request_path=config["request_path"],
        manifest_path=config["manifest_path"],
        report_path=config["report_path"],
        model=config["model"],
        limit=config["limit"],
        daily_budget_usd=Decimal(config["daily_budget_usd"]),
    )
    return result.as_dict()


def submit_or_dry_run(config: dict[str, Any], preflight: dict[str, Any]) -> dict[str, Any]:
    if preflight["budget_status"] == "blocked":
        raise RuntimeError("LLM Batch submission blocked by preflight budget alert")
    if not config["submit"]:
        return {
            "status": "dry_run",
            "request_rows": preflight["request_rows"],
            "request_path": preflight["request_path"],
        }
    client = OpenAIBatchClient()
    uploaded = client.upload(config["request_path"])
    batch = client.submit(
        uploaded["id"],
        metadata={
            "llm_batch_id": config["internal_batch_id"],
            "model": config["model"],
            "prompt_version": "news-comment-analysis-v1",
        },
    )
    state_path = Path(config["state_path"])
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(batch, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return {
        "status": str(batch["status"]),
        "batch_id": str(batch["id"]),
        "request_rows": preflight["request_rows"],
        "state_path": str(state_path),
    }


def verify_submission(
    preflight: dict[str, Any], submission: dict[str, Any]
) -> dict[str, Any]:
    if int(preflight["request_rows"]) < 1:
        raise ValueError("LLM Batch contains no requests")
    if submission["status"] == "dry_run" and not Path(
        submission["request_path"]
    ).exists():
        raise FileNotFoundError(submission["request_path"])
    return {
        "prepared_rows": int(preflight["request_rows"]),
        "skipped_rows": int(preflight["skipped_rows"]),
        "budget_status": preflight["budget_status"],
        "submission_status": submission["status"],
    }
