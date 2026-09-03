from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from llm_analysis.period_summary import build_period_summary_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare 31 daily and one monthly OpenAI Batch summary requests"
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--request-output", type=Path, required=True)
    parser.add_argument("--manifest-output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--daily-reddit-samples", type=int, default=20)
    parser.add_argument("--daily-news-samples", type=int, default=20)
    parser.add_argument("--monthly-reddit-samples", type=int, default=100)
    parser.add_argument("--monthly-news-samples", type=int, default=100)
    parser.add_argument("--max-chars-per-item", type=int, default=1_000)
    parser.add_argument("--max-output-tokens", type=int, default=300)
    parser.add_argument("--daily-budget-usd", type=Decimal, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_period_summary_batch(
        input_path=args.input,
        request_path=args.request_output,
        manifest_path=args.manifest_output,
        report_path=args.report,
        year=args.year,
        month=args.month,
        model=args.model,
        daily_reddit_samples=args.daily_reddit_samples,
        daily_news_samples=args.daily_news_samples,
        monthly_reddit_samples=args.monthly_reddit_samples,
        monthly_news_samples=args.monthly_news_samples,
        max_chars_per_item=args.max_chars_per_item,
        max_output_tokens=args.max_output_tokens,
        daily_budget_usd=args.daily_budget_usd,
    )
    print(json.dumps(result.as_dict(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
