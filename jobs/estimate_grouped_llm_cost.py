from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import pyarrow.dataset as ds
import yaml


BATCH_INPUT_PRICE_PER_MILLION = Decimal("0.10")
BATCH_OUTPUT_PRICE_PER_MILLION = Decimal("0.60")


def _tokens(byte_count: int) -> int:
    return max(1, (byte_count + 3) // 4)


def _cost(input_tokens: int, output_tokens: int) -> Decimal:
    return (
        Decimal(input_tokens) * BATCH_INPUT_PRICE_PER_MILLION
        + Decimal(output_tokens) * BATCH_OUTPUT_PRICE_PER_MILLION
    ) / Decimal(1_000_000)


def _valid_text(value: object, max_text_bytes: int) -> tuple[int, bool, bool]:
    text = str(value or "").strip()
    if not text or text.casefold() in {"[deleted]", "[removed]"}:
        return 0, False, False
    byte_count = len(text.encode("utf-8"))
    return min(byte_count, max_text_bytes), True, byte_count > max_text_bytes


def estimate(
    *,
    input_path: Path,
    config_path: Path,
    output_path: Path,
    year: int,
    month: int,
    content_tokens_per_chunk: int,
    max_text_bytes: int,
) -> dict[str, Any]:
    config_path = Path(config_path)
    output_path = Path(output_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    groups = config["groups"]
    subreddit_to_group = {
        subreddit.casefold(): group
        for group, value in groups.items()
        for subreddit in value["subreddits"]
    }
    comparison = config["comparison_group"]
    askreddit_key = comparison["subreddit"].casefold()

    counts: dict[int, Counter[str]] = defaultdict(Counter)
    text_tokens: dict[int, Counter[str]] = defaultdict(Counter)
    news_tokens: dict[int, Counter[str]] = defaultdict(Counter)
    rejected: Counter[str] = Counter()
    truncated: Counter[str] = Counter()

    dataset = ds.dataset(input_path, format="parquet", partitioning="hive")
    scanner = dataset.scanner(
        columns=[
            "source_name",
            "community",
            "title",
            "text",
            "metadata_json",
            "year",
            "month",
            "day",
        ],
        filter=(ds.field("year") == year) & (ds.field("month") == month),
        batch_size=65_536,
    )
    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            day = int(row["day"])
            source = row["source_name"]
            if source == "reddit":
                community = str(row.get("community") or "").casefold()
                group = (
                    "askreddit"
                    if community == askreddit_key
                    else subreddit_to_group.get(community)
                )
                if not group:
                    continue
                byte_count, valid, was_truncated = _valid_text(
                    row.get("text"), max_text_bytes
                )
                if not valid:
                    rejected[group] += 1
                    continue
                counts[day][group] += 1
                text_tokens[day][group] += _tokens(byte_count + 16)
                if was_truncated:
                    truncated[group] += 1
            elif source == "web_news":
                byte_count, valid, was_truncated = _valid_text(
                    row.get("title"), max_text_bytes
                )
                if not valid:
                    rejected["web_news"] += 1
                    continue
                if was_truncated:
                    truncated["web_news"] += 1
                metadata = json.loads(row.get("metadata_json") or "{}")
                topics = str(metadata.get("google_news_topic_group") or "").split(",")
                for topic in {topic.strip() for topic in topics if topic.strip()}:
                    if topic in groups:
                        news_tokens[day][topic] += _tokens(byte_count + 16)

    group_names = list(groups)
    daily_rows: list[dict[str, Any]] = []
    map_input_tokens = 0
    map_requests = 0
    maximum_map_requests = 0
    monthly_counts: Counter[str] = Counter()
    monthly_tokens: Counter[str] = Counter()
    monthly_news_tokens: Counter[str] = Counter()
    askreddit_sample_count = 0
    askreddit_sample_tokens = 0
    daily_news_tokens_for_reduce = 0

    for day in sorted(counts):
        topic_count = sum(counts[day][group] for group in group_names)
        ask_target = (topic_count + len(group_names) // 2) // len(group_names)
        ask_available = counts[day]["askreddit"]
        ask_target = min(ask_target, ask_available)
        ask_average_tokens = (
            text_tokens[day]["askreddit"] / ask_available if ask_available else 0
        )
        estimated_ask_tokens = math.ceil(ask_target * ask_average_tokens)
        askreddit_sample_count += ask_target
        askreddit_sample_tokens += estimated_ask_tokens

        day_group_tokens = {
            **{group: text_tokens[day][group] for group in group_names},
            "askreddit": estimated_ask_tokens,
        }
        day_map_requests = {
            group: math.ceil(tokens / content_tokens_per_chunk) if tokens else 0
            for group, tokens in day_group_tokens.items()
        }
        day_max_map_requests = {
            group: math.ceil(tokens * 1.25 / content_tokens_per_chunk)
            if tokens
            else 0
            for group, tokens in day_group_tokens.items()
        }
        map_input_tokens += sum(day_group_tokens.values())
        map_requests += sum(day_map_requests.values())
        maximum_map_requests += sum(day_max_map_requests.values())
        daily_news_tokens_for_reduce += sum(news_tokens[day].values()) * 2
        for group in group_names:
            monthly_counts[group] += counts[day][group]
            monthly_tokens[group] += text_tokens[day][group]
            monthly_news_tokens[group] += news_tokens[day][group]
        daily_rows.append(
            {
                "day": f"{year:04d}-{month:02d}-{day:02d}",
                "topic_group_rows": {group: counts[day][group] for group in group_names},
                "askreddit_available_rows": ask_available,
                "askreddit_sample_rows": ask_target,
                "map_requests": day_map_requests,
                "maximum_map_requests": day_max_map_requests,
                "news_tokens": {group: news_tokens[day][group] for group in group_names},
            }
        )

    # Fixed prompt/schema overhead per request. Reduce requests consume prior summaries.
    map_prompt_overhead = 500
    daily_prompt_overhead = 500
    monthly_prompt_overhead = 1_000
    expected_map_output = 250
    max_map_output = 500
    expected_daily_output = 250
    max_daily_output = 500
    expected_monthly_output = 400
    max_monthly_output = 800
    daily_reduce_requests = len(daily_rows) * 5
    monthly_reduce_requests = 5

    expected_input_tokens = (
        map_input_tokens
        + map_requests * map_prompt_overhead
        + map_requests * expected_map_output
        + daily_reduce_requests * daily_prompt_overhead
        + daily_news_tokens_for_reduce
        + daily_reduce_requests * expected_daily_output
        + monthly_reduce_requests * monthly_prompt_overhead
    )
    expected_output_tokens = (
        map_requests * expected_map_output
        + daily_reduce_requests * expected_daily_output
        + monthly_reduce_requests * expected_monthly_output
    )
    maximum_input_tokens = math.ceil(
        map_input_tokens * 1.25
        + maximum_map_requests * map_prompt_overhead
        + maximum_map_requests * max_map_output
        + daily_reduce_requests * daily_prompt_overhead
        + daily_news_tokens_for_reduce * 1.25
        + daily_reduce_requests * max_daily_output
        + monthly_reduce_requests * monthly_prompt_overhead
    )
    maximum_output_tokens = (
        maximum_map_requests * max_map_output
        + daily_reduce_requests * max_daily_output
        + monthly_reduce_requests * max_monthly_output
    )
    expected_stage_tokens = {
        "map": {
            "input": map_input_tokens + map_requests * map_prompt_overhead,
            "output": map_requests * expected_map_output,
        },
        "daily_reduce": {
            "input": (
                map_requests * expected_map_output
                + daily_reduce_requests * daily_prompt_overhead
                + daily_news_tokens_for_reduce
            ),
            "output": daily_reduce_requests * expected_daily_output,
        },
        "monthly_reduce": {
            "input": (
                daily_reduce_requests * expected_daily_output
                + monthly_reduce_requests * monthly_prompt_overhead
            ),
            "output": monthly_reduce_requests * expected_monthly_output,
        },
    }
    for stage in expected_stage_tokens.values():
        stage["batch_cost_usd"] = str(_cost(stage["input"], stage["output"]))
    result = {
        "scope": {"year": year, "month": month, "days": len(daily_rows)},
        "model": "gpt-5.6-luna",
        "pricing_usd_per_million_tokens": {
            "batch_input": str(BATCH_INPUT_PRICE_PER_MILLION),
            "batch_output": str(BATCH_OUTPUT_PRICE_PER_MILLION),
        },
        "assumptions": {
            "token_estimator": "ceil(UTF-8 bytes / 4)",
            "maximum_input_safety_multiplier": 1.25,
            "content_tokens_per_map_chunk": content_tokens_per_chunk,
            "max_text_bytes_per_item": max_text_bytes,
            "map_prompt_overhead_tokens": map_prompt_overhead,
            "expected_map_output_tokens": expected_map_output,
            "maximum_map_output_tokens": max_map_output,
            "expected_daily_output_tokens": expected_daily_output,
            "maximum_daily_output_tokens": max_daily_output,
            "expected_monthly_output_tokens": expected_monthly_output,
            "maximum_monthly_output_tokens": max_monthly_output,
        },
        "reddit_rows": {
            "topic_groups": dict(monthly_counts),
            "topic_groups_total": sum(monthly_counts.values()),
            "askreddit_available": sum(counts[day]["askreddit"] for day in counts),
            "askreddit_sample": askreddit_sample_count,
        },
        "quality_gate": {"rejected_rows": dict(rejected), "truncated_rows": dict(truncated)},
        "request_counts": {
            "map_expected": map_requests,
            "map_conservative_maximum": maximum_map_requests,
            "daily_reduce": daily_reduce_requests,
            "monthly_reduce": monthly_reduce_requests,
            "total_expected": map_requests + daily_reduce_requests + monthly_reduce_requests,
            "total_conservative_maximum": (
                maximum_map_requests + daily_reduce_requests + monthly_reduce_requests
            ),
            "final_results": daily_reduce_requests + monthly_reduce_requests,
        },
        "expected": {
            "input_tokens": expected_input_tokens,
            "output_tokens": expected_output_tokens,
            "batch_cost_usd": str(_cost(expected_input_tokens, expected_output_tokens)),
            "standard_api_equivalent_usd": str(
                _cost(expected_input_tokens, expected_output_tokens) * 2
            ),
            "stages": expected_stage_tokens,
        },
        "conservative_maximum": {
            "input_tokens": maximum_input_tokens,
            "output_tokens": maximum_output_tokens,
            "batch_cost_usd": str(_cost(maximum_input_tokens, maximum_output_tokens)),
        },
        "monthly_approximate_content_tokens": {
            **dict(monthly_tokens),
            "askreddit_sample": askreddit_sample_tokens,
            "news_by_topic": dict(monthly_news_tokens),
        },
        "daily": daily_rows,
        "response_root": "data/llm_response/",
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Estimate grouped LLM Batch cost")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--config", type=Path, default=Path("config/analysis-groups.yaml")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--content-tokens-per-chunk", type=int, default=50_000)
    parser.add_argument("--max-text-bytes", type=int, default=16_000)
    args = parser.parse_args(argv)
    result = estimate(
        input_path=args.input,
        config_path=args.config,
        output_path=args.output,
        year=args.year,
        month=args.month,
        content_tokens_per_chunk=args.content_tokens_per_chunk,
        max_text_bytes=args.max_text_bytes,
    )
    print(json.dumps({key: result[key] for key in ("request_counts", "expected", "conservative_maximum")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
