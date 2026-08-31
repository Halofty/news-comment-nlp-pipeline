from __future__ import annotations

import argparse
import calendar
import csv
from pathlib import Path

import requests

DATASET_ID = "fddemarco/pushshift-reddit-comments"
TREE_API = f"https://huggingface.co/api/datasets/{DATASET_ID}/tree/main/data"


def fetch_months(start_month: str, end_month: str) -> list[dict[str, object]]:
    response = requests.get(
        TREE_API, params={"expand": "true", "limit": 100}, timeout=60
    )
    response.raise_for_status()
    rows = []
    for entry in response.json():
        name = str(entry.get("path", "")).rsplit("/", 1)[-1]
        if not name.startswith("RC_") or not name.endswith(".parquet"):
            continue
        month = name[3:10]
        if start_month <= month <= end_month:
            size = int(entry["size"])
            year, month_number = (int(value) for value in month.split("-"))
            days = calendar.monthrange(year, month_number)[1]
            rows.append(
                {
                    "month": month,
                    "filename": name,
                    "bytes": size,
                    "gib": round(size / 2**30, 4),
                    "average_daily_mib": round(size / days / 2**20, 4),
                }
            )
    rows.sort(key=lambda row: row["month"])
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Catalog Reddit monthly source files")
    parser.add_argument("--start-month", default="2012-01")
    parser.add_argument("--end-month", default="2016-02")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    rows = fetch_months(args.start_month, args.end_month)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    total = sum(int(row["bytes"]) for row in rows)
    print(f"files={len(rows)} bytes={total} gib={total / 2**30:.4f}")


if __name__ == "__main__":
    main()
