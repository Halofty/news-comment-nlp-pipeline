from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from llm_analysis import OpenAIBatchClient, validate_batch_results
from observability import (
    FailSafeObservabilitySink,
    LangfuseSink,
    PriceSchedule,
    StageObservation,
    StructuredLogSink,
    reconcile_usage,
)
from observability.openai_batch import load_sample_batch, total_cost


LONG_CONTEXT_THRESHOLD = 272_000


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download, validate, combine, and observe daily economy Batch results"
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--response-root", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--start-day", type=int, required=True)
    parser.add_argument("--end-day", type=int, required=True)
    parser.add_argument("--combined-output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _pricing(input_tokens: int) -> PriceSchedule:
    long_context = input_tokens > LONG_CONTEXT_THRESHOLD
    return PriceSchedule(
        version=(
            "gpt-5.6-luna-batch-long-context-2026-09-03"
            if long_context
            else "gpt-5.6-luna-batch-2026-09-03"
        ),
        effective_date="2026-09-03",
        input_per_million=Decimal("0.20" if long_context else "0.10"),
        cached_input_per_million=Decimal("0.02" if long_context else "0.01"),
        output_per_million=Decimal("0.90" if long_context else "0.60"),
    )


def _observability_sink(response_root: Path):
    fallback = StructuredLogSink(response_root / "observability-fallback.jsonl")
    enabled = os.environ.get("LANGFUSE_ENABLED", "false").strip().casefold()
    if enabled not in {"1", "true", "yes", "on"}:
        return fallback
    try:
        return FailSafeObservabilitySink(LangfuseSink(), fallback)
    except Exception:
        return fallback


def collect(args: argparse.Namespace, client: OpenAIBatchClient | None = None) -> dict[str, Any]:
    if not 1 <= args.start_day <= args.end_day <= 31:
        raise ValueError("day range must be within 1..31")
    client = client or OpenAIBatchClient()
    sink = _observability_sink(args.response_root)
    combined: list[dict[str, Any]] = []
    daily: list[dict[str, Any]] = []
    pending: list[str] = []
    total_cost_usd = Decimal("0")
    for day in range(args.start_day, args.end_day + 1):
        period = f"{args.year:04d}-{args.month:02d}-{day:02d}"
        artifact = args.artifact_root / period
        response = args.response_root / period
        state_path = response / "batch-state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        batch = client.retrieve(str(state["id"]))
        _write_json(state_path, batch)
        if batch.get("status") != "completed" or not batch.get("output_file_id"):
            pending.append(period)
            daily.append({"period": period, "status": str(batch.get("status"))})
            continue

        raw_path = response / "results.raw.jsonl"
        valid_path = response / "results.validated.jsonl"
        validation_path = response / "validation-report.json"
        client.download(str(batch["output_file_id"]), raw_path)
        validation = validate_batch_results(
            result_path=raw_path,
            manifest_path=artifact / "manifest.jsonl",
            output_path=valid_path,
        )
        _write_json(validation_path, validation)
        validated = _read_jsonl(valid_path)
        combined.extend(validated)

        usage = batch.get("usage") or {}
        input_tokens = int(usage.get("input_tokens", 0))
        pricing = _pricing(input_tokens)
        sample = load_sample_batch(
            batch_path=state_path,
            manifest_path=artifact / "manifest.jsonl",
            results_path=raw_path,
            pricing=pricing,
        )
        reconciliation = reconcile_usage(
            batch_id=sample.batch.batch_id,
            generations=sample.generations,
            batch_usage=sample.batch_usage,
            expected_completed_count=sample.expected_completed_count,
        )
        cost = total_cost(sample.generations)
        total_cost_usd += cost
        marker = response / "usage-observed.json"
        if not marker.exists():
            sink.record_batch(sample.batch)
            sink.record_stage(
                StageObservation(
                    batch_id=sample.batch.batch_id,
                    stage="load-and-validate-results",
                    status="completed" if validation["failed_rows"] == 0 else "failed",
                    started_at=datetime.now(timezone.utc),
                    completed_at=datetime.now(timezone.utc),
                    error_code=(None if validation["failed_rows"] == 0 else "SCHEMA_VALIDATION_FAILED"),
                )
            )
            for generation in sample.generations:
                sink.record_generation(generation)
            sink.record_reconciliation(reconciliation)
            _write_json(
                marker,
                {
                    "recorded": True,
                    "reconciliation_status": reconciliation.status,
                    "total_cost_usd": str(cost),
                },
            )
        result = validated[0] if len(validated) == 1 else {}
        daily.append(
            {
                "period": period,
                "batch_id": str(batch["id"]),
                "status": str(batch["status"]),
                "validated_rows": validation["validated_rows"],
                "failed_rows": validation["failed_rows"],
                "input_tokens": input_tokens,
                "output_tokens": int(usage.get("output_tokens", 0)),
                "cached_input_tokens": int((usage.get("input_tokens_details") or {}).get("cached_tokens", 0)),
                "reasoning_output_tokens": int((usage.get("output_tokens_details") or {}).get("reasoning_tokens", 0)),
                "long_context_pricing": input_tokens > LONG_CONTEXT_THRESHOLD,
                "cost_usd": str(cost),
                "sentiment": result.get("sentiment"),
                "sentiment_score": result.get("sentiment_score"),
                "topics": result.get("topics", []),
                "keywords": result.get("keywords", []),
                "summary": result.get("summary"),
                "reconciliation_status": reconciliation.status,
            }
        )
    sink.flush()
    combined.sort(key=lambda row: str(row.get("event_id")))
    args.combined_output.parent.mkdir(parents=True, exist_ok=True)
    with args.combined_output.open("w", encoding="utf-8", newline="\n") as handle:
        for row in combined:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    report = {
        "requested_days": args.end_day - args.start_day + 1,
        "completed_days": sum(row.get("status") == "completed" for row in daily),
        "pending_days": pending,
        "validated_rows": len(combined),
        "failed_rows": sum(int(row.get("failed_rows", 0)) for row in daily),
        "input_tokens": sum(int(row.get("input_tokens", 0)) for row in daily),
        "output_tokens": sum(int(row.get("output_tokens", 0)) for row in daily),
        "cached_input_tokens": sum(int(row.get("cached_input_tokens", 0)) for row in daily),
        "reasoning_output_tokens": sum(int(row.get("reasoning_output_tokens", 0)) for row in daily),
        "total_cost_usd": str(total_cost_usd),
        "combined_output": str(args.combined_output),
        "daily": daily,
    }
    _write_json(args.report, report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = collect(args)
    print(json.dumps({key: report[key] for key in (
        "requested_days", "completed_days", "pending_days", "validated_rows",
        "failed_rows", "input_tokens", "output_tokens", "total_cost_usd"
    )}, ensure_ascii=False, sort_keys=True))
    return 0 if not report["pending_days"] and report["failed_rows"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
