from __future__ import annotations

import argparse
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from observability import (
    FailSafeObservabilitySink,
    LangfuseSink,
    NoOpSink,
    PriceSchedule,
    StageObservation,
    StructuredLogSink,
    reconcile_usage,
)
from observability.openai_batch import load_sample_batch, total_cost


DEFAULT_BATCH = Path("sample/llm-batch-object.json")
DEFAULT_MANIFEST = Path("sample/llm-batch-request-manifest.jsonl")
DEFAULT_RESULTS = Path("sample/llm-batch-results.jsonl")
DEFAULT_OUTPUT = Path("analysis/reports/langfuse-sample-trace.jsonl")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate metadata-only Langfuse token and cost tracking"
    )
    parser.add_argument("--batch", type=Path, default=DEFAULT_BATCH)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--sink", choices=("structured-log", "langfuse", "noop"), default="structured-log"
    )
    parser.add_argument("--pricing-version", default="sample-batch-v1")
    parser.add_argument("--pricing-effective-date", default="2026-08-24")
    parser.add_argument("--input-price-per-million", default="0.50")
    parser.add_argument("--cached-input-price-per-million", default="0.25")
    parser.add_argument("--output-price-per-million", default="2.00")
    return parser


def _build_sink(name: str, output: Path):
    fallback = StructuredLogSink(output)
    if name == "structured-log":
        return fallback
    if name == "noop":
        return NoOpSink()
    return FailSafeObservabilitySink(LangfuseSink(), fallback)


def run(args: argparse.Namespace) -> dict[str, object]:
    pricing = PriceSchedule(
        version=args.pricing_version,
        effective_date=args.pricing_effective_date,
        input_per_million=Decimal(args.input_price_per_million),
        cached_input_per_million=Decimal(args.cached_input_price_per_million),
        output_per_million=Decimal(args.output_price_per_million),
    )
    sample = load_sample_batch(
        batch_path=args.batch,
        manifest_path=args.manifest,
        results_path=args.results,
        pricing=pricing,
    )
    reconciliation = reconcile_usage(
        batch_id=sample.batch.batch_id,
        generations=sample.generations,
        batch_usage=sample.batch_usage,
        expected_completed_count=sample.expected_completed_count,
    )

    if args.sink != "noop":
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text("", encoding="utf-8")
    sink = _build_sink(args.sink, args.output)
    sink.record_batch(sample.batch)

    stage_names = (
        "build-request-file",
        "submit-openai-batch",
        "poll-openai-batch",
        "load-and-validate-results",
    )
    stage_started_at = sample.batch.submitted_at
    for index, stage in enumerate(stage_names):
        started_at = stage_started_at + timedelta(seconds=index)
        sink.record_stage(
            StageObservation(
                batch_id=sample.batch.batch_id,
                stage=stage,
                status="completed",
                started_at=started_at,
                completed_at=started_at + timedelta(seconds=1),
            )
        )
    for generation in sample.generations:
        sink.record_generation(generation)
    sink.record_reconciliation(reconciliation)
    sink.flush()

    return {
        "batch_id": sample.batch.batch_id,
        "generation_count": len(sample.generations),
        "reconciliation_status": reconciliation.status,
        "input_tokens": (
            reconciliation.generation_usage.input_tokens
            if reconciliation.generation_usage
            else None
        ),
        "output_tokens": (
            reconciliation.generation_usage.output_tokens
            if reconciliation.generation_usage
            else None
        ),
        "total_tokens": (
            reconciliation.generation_usage.total_tokens
            if reconciliation.generation_usage
            else None
        ),
        "total_cost_usd": str(total_cost(sample.generations)),
        "sink": args.sink,
        "output": str(args.output) if args.sink != "noop" else None,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run(args)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0 if summary["reconciliation_status"] == "matched" else 1


if __name__ == "__main__":
    raise SystemExit(main())

