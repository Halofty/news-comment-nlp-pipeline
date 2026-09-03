from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from llm_analysis.economy_period import (
    build_economy_daily_batch,
    build_economy_monthly_batch,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare full-coverage economy-and-society period Batch requests"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    daily = commands.add_parser("prepare-daily")
    daily.add_argument("--input", type=Path, required=True)
    daily.add_argument("--config", type=Path, default=Path("config/analysis-groups.yaml"))
    daily.add_argument("--start-day", type=int, default=1)
    daily.add_argument("--end-day", type=int)
    monthly = commands.add_parser("prepare-monthly")
    monthly.add_argument("--daily-results", type=Path, required=True)
    for command in (daily, monthly):
        command.add_argument("--request-output", type=Path, required=True)
        command.add_argument("--manifest-output", type=Path, required=True)
        command.add_argument("--report", type=Path, required=True)
        command.add_argument("--year", type=int, required=True)
        command.add_argument("--month", type=int, required=True)
        command.add_argument("--model", default="gpt-5.6-luna")
        command.add_argument("--max-output-tokens", type=int, default=1_000)
        command.add_argument("--safety-multiplier", type=Decimal, default=Decimal("1.25"))
        command.add_argument("--budget-usd", type=Decimal, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    common = {
        "request_path": args.request_output,
        "manifest_path": args.manifest_output,
        "report_path": args.report,
        "year": args.year,
        "month": args.month,
        "model": args.model,
        "max_output_tokens": args.max_output_tokens,
        "safety_multiplier": args.safety_multiplier,
        "budget_usd": args.budget_usd,
    }
    if args.command == "prepare-daily":
        result = build_economy_daily_batch(
            input_path=args.input, config_path=args.config,
            start_day=args.start_day, end_day=args.end_day, **common
        )
    else:
        result = build_economy_monthly_batch(
            daily_results_path=args.daily_results, **common
        )
    print(json.dumps(result.as_dict(), ensure_ascii=False, sort_keys=True))
    return 0 if result.budget_status != "blocked" else 2


if __name__ == "__main__":
    raise SystemExit(main())
