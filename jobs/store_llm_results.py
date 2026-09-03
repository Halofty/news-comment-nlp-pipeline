from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from storage.llm_postgres import (
    summarize_llm_storage,
    write_llm_batch_to_postgres,
)


LONG_CONTEXT_THRESHOLD = 272_000


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upsert validated daily and monthly LLM Batch results into PostgreSQL"
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--response-root", type=Path, required=True)
    parser.add_argument("--daily-results", type=Path, required=True)
    parser.add_argument("--monthly-results", type=Path, required=True)
    parser.add_argument("--dsn", default=os.getenv("POSTGRES_DSN", ""))
    parser.add_argument("--report", type=Path, required=True)
    return parser


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"expected JSON objects: {path}")
    return rows


def _cost_from_usage(usage: dict[str, Any]) -> Decimal:
    input_tokens = int(usage.get("input_tokens", 0))
    output_tokens = int(usage.get("output_tokens", 0))
    cached_tokens = int((usage.get("input_tokens_details") or {}).get("cached_tokens", 0))
    long_context = input_tokens > LONG_CONTEXT_THRESHOLD
    input_price = Decimal("0.20" if long_context else "0.10")
    cached_price = Decimal("0.02" if long_context else "0.01")
    output_price = Decimal("0.90" if long_context else "0.60")
    return (
        Decimal(input_tokens - cached_tokens) * input_price
        + Decimal(cached_tokens) * cached_price
        + Decimal(output_tokens) * output_price
    ) / Decimal(1_000_000)


def _batch_cost(state: dict[str, Any], marker: Path | None = None) -> Decimal:
    if marker is not None and marker.is_file():
        value = _read_json(marker).get("total_cost_usd")
        if value is not None:
            return Decimal(str(value))
    usage = state.get("usage") or {}
    if not isinstance(usage, dict):
        raise ValueError("batch usage must be an object")
    return _cost_from_usage(usage)


def store_period_results(args: argparse.Namespace) -> dict[str, Any]:
    if not args.dsn.strip():
        raise ValueError("--dsn or POSTGRES_DSN is required")
    daily_results = {
        str(row["custom_id"]): row for row in _read_jsonl(args.daily_results)
    }
    monthly_results = {
        str(row["custom_id"]): row for row in _read_jsonl(args.monthly_results)
    }

    writes: list[dict[str, Any]] = []
    batch_ids: list[str] = []
    consumed: set[str] = set()
    for artifact_dir in sorted((args.artifact_root / "days").iterdir()):
        if not artifact_dir.is_dir():
            continue
        response_dir = args.response_root / "days" / artifact_dir.name
        manifest = _read_jsonl(artifact_dir / "manifest.jsonl")
        selected = [daily_results[row["custom_id"]] for row in manifest]
        consumed.update(str(row["custom_id"]) for row in selected)
        state = _read_json(response_dir / "batch-state.json")
        result = write_llm_batch_to_postgres(
            dsn=args.dsn,
            batch_state=state,
            manifest_rows=manifest,
            result_rows=selected,
            total_cost_usd=_batch_cost(
                state, response_dir / "usage-observed.json"
            ),
        )
        batch_ids.append(result.llm_batch_id)
        writes.append(result.__dict__)

    missing_daily = set(daily_results) - consumed
    if missing_daily:
        raise ValueError(
            f"daily results were not matched to manifests: {sorted(missing_daily)}"
        )

    monthly_manifest = _read_jsonl(args.artifact_root / "monthly" / "manifest.jsonl")
    monthly_selected = [monthly_results[row["custom_id"]] for row in monthly_manifest]
    monthly_state = _read_json(args.response_root / "monthly" / "batch-state.json")
    monthly_write = write_llm_batch_to_postgres(
        dsn=args.dsn,
        batch_state=monthly_state,
        manifest_rows=monthly_manifest,
        result_rows=monthly_selected,
        total_cost_usd=_batch_cost(monthly_state),
    )
    batch_ids.append(monthly_write.llm_batch_id)
    writes.append(monthly_write.__dict__)

    summary = summarize_llm_storage(dsn=args.dsn, llm_batch_ids=batch_ids)
    expected_rows = len(daily_results) + len(monthly_results)
    if summary != {
        "batch_rows": len(batch_ids),
        "request_rows": expected_rows,
        "analysis_rows": expected_rows,
    }:
        raise RuntimeError(
            f"LLM PostgreSQL row accounting mismatch: expected={expected_rows}, actual={summary}"
        )
    report = {
        "status": "completed",
        "batch_ids": batch_ids,
        "write_count": len(writes),
        "writes": writes,
        "expected_analysis_rows": expected_rows,
        "storage": summary,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    from storage.data_lake import publish_artifact_if_enabled

    publish_artifact_if_enabled(args.report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = store_period_results(args)
    print(json.dumps(report["storage"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
