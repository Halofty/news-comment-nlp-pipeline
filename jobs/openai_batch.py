from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from llm_analysis import OpenAIBatchClient, build_batch_file, validate_batch_results
from observability import (
    BatchObservation,
    FailSafeObservabilitySink,
    LangfuseSink,
    StageObservation,
    StructuredLogSink,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and operate OpenAI Batch jobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="Build a dry-run Batch JSONL")
    prepare.add_argument("--input", type=Path, required=True)
    prepare.add_argument("--request-output", type=Path, required=True)
    prepare.add_argument("--manifest-output", type=Path, required=True)
    prepare.add_argument("--report", type=Path, required=True)
    prepare.add_argument("--model", default="gpt-5.6-luna")
    prepare.add_argument("--limit", type=int, default=100)
    prepare.add_argument("--max-text-bytes", type=int, default=16_000)
    prepare.add_argument("--max-output-tokens", type=int, default=300)
    prepare.add_argument("--daily-budget-usd", type=Decimal)

    submit = subparsers.add_parser("submit", help="Upload JSONL and create Batch")
    submit.add_argument("--request-file", type=Path, required=True)
    submit.add_argument("--preflight-report", type=Path, required=True)
    submit.add_argument("--state-output", type=Path, required=True)
    submit.add_argument("--internal-batch-id", required=True)
    submit.add_argument("--model", default="gpt-5.6-luna")
    submit.add_argument("--prompt-version", default="news-comment-analysis-v1")

    status = subparsers.add_parser("status", help="Retrieve Batch state")
    status.add_argument("--batch-id", required=True)
    status.add_argument("--state-output", type=Path, required=True)

    download = subparsers.add_parser("download", help="Download completed results")
    download.add_argument("--batch-id", required=True)
    download.add_argument("--result-output", type=Path, required=True)
    download.add_argument("--state-output", type=Path, required=True)

    validate = subparsers.add_parser("validate", help="Validate structured results")
    validate.add_argument("--results", type=Path, required=True)
    validate.add_argument("--manifest", type=Path, required=True)
    validate.add_argument("--output", type=Path, required=True)
    validate.add_argument("--report", type=Path, required=True)
    return parser


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _observability_sink(state_output: Path):
    fallback = StructuredLogSink(state_output.with_name("observability-fallback.jsonl"))
    enabled = os.environ.get("LANGFUSE_ENABLED", "false").strip().casefold()
    if enabled not in {"1", "true", "yes", "on"}:
        return fallback
    try:
        return FailSafeObservabilitySink(LangfuseSink(), fallback)
    except Exception as error:
        logging.getLogger(__name__).warning(
            "langfuse_initialization_failed error_type=%s fallback=structured-log",
            type(error).__name__,
        )
        return fallback


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: int | float) -> datetime:
    return datetime.fromtimestamp(value, tz=timezone.utc)


def run(args: argparse.Namespace) -> dict[str, object]:
    if args.command == "prepare":
        return build_batch_file(
            input_path=args.input,
            request_path=args.request_output,
            manifest_path=args.manifest_output,
            report_path=args.report,
            model=args.model,
            limit=args.limit,
            max_text_bytes=args.max_text_bytes,
            max_output_tokens=args.max_output_tokens,
            daily_budget_usd=args.daily_budget_usd,
        ).as_dict()

    if args.command == "submit":
        report = json.loads(args.preflight_report.read_text(encoding="utf-8"))
        if report.get("budget_status") == "blocked":
            raise RuntimeError("Batch submission blocked by the configured daily budget")
        sink = _observability_sink(args.state_output)
        client = OpenAIBatchClient()
        upload_started_at = _utc_now()
        try:
            uploaded = client.upload(args.request_file)
        except Exception as error:
            sink.record_stage(
                StageObservation(
                    batch_id=args.internal_batch_id,
                    stage="upload-request-file",
                    status="failed",
                    started_at=upload_started_at,
                    completed_at=_utc_now(),
                    error_code=type(error).__name__,
                )
            )
            sink.flush()
            raise
        sink.record_stage(
            StageObservation(
                batch_id=args.internal_batch_id,
                stage="upload-request-file",
                status="completed",
                started_at=upload_started_at,
                completed_at=_utc_now(),
            )
        )
        submit_started_at = _utc_now()
        try:
            value = client.submit(
                uploaded["id"],
                metadata={
                    "llm_batch_id": args.internal_batch_id,
                    "model": args.model,
                    "prompt_version": args.prompt_version,
                },
            )
        except Exception as error:
            sink.record_stage(
                StageObservation(
                    batch_id=args.internal_batch_id,
                    stage="submit-openai-batch",
                    status="failed",
                    started_at=submit_started_at,
                    completed_at=_utc_now(),
                    error_code=type(error).__name__,
                )
            )
            sink.flush()
            raise
        sink.record_stage(
            StageObservation(
                batch_id=args.internal_batch_id,
                stage="submit-openai-batch",
                status="completed",
                started_at=submit_started_at,
                completed_at=_utc_now(),
            )
        )
        sink.record_batch(
            BatchObservation(
                batch_id=args.internal_batch_id,
                openai_batch_id=str(value["id"]),
                model=str(value.get("model") or args.model),
                document_count=int((value.get("request_counts") or {}).get("total", report["request_rows"])),
                status=str(value["status"]),
                submitted_at=_timestamp(value["created_at"]),
            )
        )
        sink.flush()
        _write_json(args.state_output, value)
        return value
    if args.command == "status":
        client = OpenAIBatchClient()
        value = client.retrieve(args.batch_id)
        _write_json(args.state_output, value)
        metadata = value.get("metadata") or {}
        internal_batch_id = str(metadata.get("llm_batch_id") or value["id"])
        terminal_at = next(
            (
                value.get(name)
                for name in ("completed_at", "failed_at", "expired_at", "cancelled_at")
                if value.get(name) is not None
            ),
            None,
        )
        error_rows = ((value.get("errors") or {}).get("data") or [])
        error_code = str(error_rows[0].get("code")) if error_rows else None
        sink = _observability_sink(args.state_output)
        sink.record_batch(
            BatchObservation(
                batch_id=internal_batch_id,
                openai_batch_id=str(value["id"]),
                model=str(value.get("model") or metadata.get("model") or "unknown"),
                document_count=int((value.get("request_counts") or {}).get("total", 0)),
                status=str(value["status"]),
                submitted_at=_timestamp(value["created_at"]),
                completed_at=_timestamp(terminal_at) if terminal_at is not None else None,
            )
        )
        sink.record_stage(
            StageObservation(
                batch_id=internal_batch_id,
                stage="poll-openai-batch",
                status="failed" if error_code else "completed",
                started_at=_utc_now(),
                completed_at=_utc_now(),
                error_code=error_code,
            )
        )
        sink.flush()
        return value
    if args.command == "download":
        client = OpenAIBatchClient()
        value = client.retrieve(args.batch_id)
        if value.get("status") != "completed" or not value.get("output_file_id"):
            raise RuntimeError(f"Batch is not downloadable: {value.get('status')}")
        client.download(value["output_file_id"], args.result_output)
        _write_json(args.state_output, value)
        return {
            "batch_id": args.batch_id,
            "status": value["status"],
            "result_output": str(args.result_output),
        }
    if args.command == "validate":
        value = validate_batch_results(
            result_path=args.results,
            manifest_path=args.manifest,
            output_path=args.output,
        )
        _write_json(args.report, value)
        return value
    raise AssertionError(f"unhandled command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
