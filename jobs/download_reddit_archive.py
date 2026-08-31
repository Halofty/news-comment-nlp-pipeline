from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DATASET_ID = "fddemarco/pushshift-reddit-comments"
DEFAULT_CATALOG = Path("docs/briefings/date6/reddit-monthly-source-files.csv")
DEFAULT_OUTPUT_ROOT = Path("data/raw/reddit-archive")
DEFAULT_PROGRESS_LOG = Path("data/logs/reddit-archive-download.jsonl")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_catalog(
    path: Path, *, start_month: str | None = None, end_month: str | None = None
) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as file:
        rows = [dict(row) for row in csv.DictReader(file)]
    selected = [
        row
        for row in rows
        if (start_month is None or row["month"] >= start_month)
        and (end_month is None or row["month"] <= end_month)
    ]
    if not selected:
        raise ValueError("catalog does not contain a month in the requested range")
    return selected


def emit(log_path: Path, event: str, **fields: Any) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record = {"timestamp": utc_now(), "event": event, **fields}
    with log_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")


def validate_size(path: Path, expected_bytes: int) -> bool:
    return path.is_file() and path.stat().st_size == expected_bytes


def download_with_curl(filename: str, destination: Path) -> Path:
    """Download through the Hub resolver with HTTP range-based resume."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    url = (
        f"https://huggingface.co/datasets/{DATASET_ID}/resolve/main/"
        f"data/{filename}"
    )
    subprocess.run(
        [
            "curl",
            "--fail",
            "--location",
            "--continue-at",
            "-",
            "--retry",
            "10",
            "--retry-all-errors",
            "--connect-timeout",
            "30",
            "--output",
            str(partial),
            url,
        ],
        check=True,
    )
    partial.replace(destination)
    return destination


def download_archive(
    rows: list[dict[str, Any]],
    *,
    output_root: Path,
    progress_log: Path,
    reserve_gib: int,
    retries: int,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    emit(
        progress_log,
        "run_started",
        months=len(rows),
        expected_bytes=sum(int(row["bytes"]) for row in rows),
        output_root=str(output_root),
    )

    for index, row in enumerate(rows, start=1):
        month = str(row["month"])
        filename = str(row["filename"])
        expected_bytes = int(row["bytes"])
        destination = output_root / "data" / filename
        if validate_size(destination, expected_bytes):
            emit(
                progress_log,
                "month_skipped",
                month=month,
                index=index,
                bytes=expected_bytes,
                reason="already_complete",
            )
            continue

        free_bytes = shutil.disk_usage(output_root).free
        required_bytes = expected_bytes + reserve_gib * 2**30
        if free_bytes < required_bytes:
            emit(
                progress_log,
                "run_stopped_low_disk",
                month=month,
                free_bytes=free_bytes,
                required_bytes=required_bytes,
            )
            raise RuntimeError(
                f"insufficient disk space for {month}: "
                f"free={free_bytes}, required={required_bytes}"
            )

        emit(
            progress_log,
            "month_started",
            month=month,
            index=index,
            total=len(rows),
            expected_bytes=expected_bytes,
        )
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                downloaded = download_with_curl(filename, destination)
                if not validate_size(downloaded, expected_bytes):
                    raise RuntimeError(
                        f"size mismatch for {month}: "
                        f"actual={downloaded.stat().st_size}, expected={expected_bytes}"
                    )
                emit(
                    progress_log,
                    "month_completed",
                    month=month,
                    index=index,
                    bytes=expected_bytes,
                    path=str(downloaded),
                )
                break
            except Exception as error:  # network errors differ by backend
                last_error = error
                emit(
                    progress_log,
                    "month_attempt_failed",
                    month=month,
                    attempt=attempt,
                    error=repr(error),
                )
                if attempt < retries:
                    time.sleep(min(60, 2**attempt))
        else:
            emit(progress_log, "run_failed", month=month, error=repr(last_error))
            raise RuntimeError(f"failed to download {month}") from last_error

    emit(progress_log, "run_completed", months=len(rows))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resumably download the Reddit monthly Parquet archive"
    )
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--progress-log", type=Path, default=DEFAULT_PROGRESS_LOG)
    parser.add_argument("--start-month")
    parser.add_argument("--end-month")
    parser.add_argument("--reserve-gib", type=int, default=100)
    parser.add_argument("--retries", type=int, default=5)
    args = parser.parse_args()
    if args.reserve_gib < 0 or args.retries < 1:
        raise ValueError("reserve-gib must be non-negative and retries must be positive")

    download_archive(
        read_catalog(
            args.catalog,
            start_month=args.start_month,
            end_month=args.end_month,
        ),
        output_root=args.output_root,
        progress_log=args.progress_log,
        reserve_gib=args.reserve_gib,
        retries=args.retries,
    )


if __name__ == "__main__":
    main()
