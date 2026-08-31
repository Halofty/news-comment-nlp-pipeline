from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from core.subreddits import load_subreddit_allowlist

DEFAULT_PERIOD_DAYS = 1521  # 2012-01-01 through 2016-02-29, inclusive
DEFAULT_ALLOWLIST = Path(__file__).resolve().parents[1] / "config/subreddits-analysis.txt"
CORE_SUBREDDITS = {
    name.casefold() for name in load_subreddit_allowlist(DEFAULT_ALLOWLIST)
}


def parse_input(value: str) -> tuple[str, Path]:
    try:
        label, path = value.split("=", 1)
    except ValueError as error:
        raise argparse.ArgumentTypeError("input must use LABEL=PATH") from error
    return label, Path(path)


def profile(inputs: list[tuple[str, Path]], *, period_days: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    labels = [label for label, _ in inputs]
    stats: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    spellings: dict[str, Counter[str]] = defaultdict(Counter)

    for label, path in inputs:
        with path.open("rb") as lines:
            for line in lines:
                if not line.strip():
                    continue
                event = json.loads(line)
                display = str(event.get("community") or "(unknown)")
                key = display.casefold()
                spellings[key][display] += 1
                stats[key][f"{label}_rows"] += 1
                stats[key][f"{label}_bytes"] += len(line)

    rows: list[dict[str, Any]] = []
    for key, values in stats.items():
        total_rows = sum(values[f"{label}_rows"] for label in labels)
        total_bytes = sum(values[f"{label}_bytes"] for label in labels)
        average_daily_bytes = total_bytes / len(labels)
        row: dict[str, Any] = {
            "subreddit": spellings[key].most_common(1)[0][0],
            "subreddit_key": key,
            "core_analysis_candidate": key in CORE_SUBREDDITS,
            "total_rows": total_rows,
            "total_jsonl_bytes": total_bytes,
            "average_daily_mib": round(average_daily_bytes / 2**20, 4),
            "projected_period_gib": round(
                average_daily_bytes * period_days / 2**30, 4
            ),
        }
        for label in labels:
            row[f"{label}_rows"] = values[f"{label}_rows"]
            row[f"{label}_jsonl_bytes"] = values[f"{label}_bytes"]
        rows.append(row)
    rows.sort(key=lambda row: row["total_jsonl_bytes"], reverse=True)

    core_rows = [row for row in rows if row["core_analysis_candidate"]]
    summary = {
        "input_days": labels,
        "period_days": period_days,
        "distinct_subreddits": len(rows),
        "total_rows": sum(row["total_rows"] for row in rows),
        "average_daily_mib": round(
            sum(row["total_jsonl_bytes"] for row in rows)
            / len(labels)
            / 2**20,
            4,
        ),
        "projected_all_jsonl_gib": round(
            sum(row["total_jsonl_bytes"] for row in rows)
            / len(labels)
            * period_days
            / 2**30,
            4,
        ),
        "core_candidate_count": len(core_rows),
        "core_average_daily_mib": round(
            sum(row["total_jsonl_bytes"] for row in core_rows)
            / len(labels)
            / 2**20,
            4,
        ),
        "core_projected_period_gib": round(
            sum(row["total_jsonl_bytes"] for row in core_rows)
            / len(labels)
            * period_days
            / 2**30,
            4,
        ),
        "projection_warning": (
            "Projection extrapolates two 2016 days and is not an exact "
            "2012-2016 measurement."
        ),
    }
    return rows, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile JSONL bytes by subreddit")
    parser.add_argument("--input", action="append", type=parse_input, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--period-days", type=int, default=DEFAULT_PERIOD_DAYS)
    args = parser.parse_args()

    rows, summary = profile(args.input, period_days=args.period_days)
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    args.summary.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
