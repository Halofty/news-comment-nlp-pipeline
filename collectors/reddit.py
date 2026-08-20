from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from collectors.common import stable_event_id, utc_now_iso, write_jsonl

DATASET_ID = "fddemarco/pushshift-reddit-comments"


def validate_month(month: str) -> str:
    datetime.strptime(month, "%Y-%m")
    return month


def stream_month(month: str) -> Iterable[dict[str, Any]]:
    try:
        from datasets import load_dataset
    except ImportError as error:
        raise RuntimeError(
            "Reddit collection requires the 'datasets' package. "
            "Run: pip install -r requirements.txt"
        ) from error

    validate_month(month)
    data_file = f"data/RC_{month}.parquet"
    return load_dataset(
        DATASET_ID,
        data_files={"train": data_file},
        split="train",
        streaming=True,
    )


def comment_to_event(
    comment: dict[str, Any], *, collected_at: str
) -> dict[str, Any] | None:
    comment_id = str(comment.get("id") or "").strip()
    body = str(comment.get("body") or "").strip()
    created_utc = comment.get("created_utc")
    if (
        not comment_id
        or not body
        or body in {"[deleted]", "[removed]"}
        or created_utc is None
    ):
        return None

    event_time = datetime.fromtimestamp(
        int(created_utc), tz=timezone.utc
    ).isoformat().replace("+00:00", "Z")
    return {
        "event_id": stable_event_id("reddit", comment_id),
        "source_type": "comment",
        "source_name": "reddit",
        "event_time": event_time,
        "collected_at": collected_at,
        "language": "unknown",
        "title": None,
        "text": body,
        "url": None,
        "community": comment.get("subreddit"),
        "engagement": int(comment.get("score") or 0),
        "schema_version": 1,
        "metadata": {
            "link_id": comment.get("link_id"),
            "controversiality": int(comment.get("controversiality") or 0),
        },
    }


def collect_events(
    rows: Iterable[dict[str, Any]],
    *,
    subreddits: set[str],
    limit: int,
) -> Iterator[dict[str, Any]]:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    normalized = {name.casefold() for name in subreddits}
    collected_at = utc_now_iso()
    emitted = 0
    for row in rows:
        subreddit = str(row.get("subreddit") or "").casefold()
        if normalized and subreddit not in normalized:
            continue
        event = comment_to_event(row, collected_at=collected_at)
        if event is None:
            continue
        yield event
        emitted += 1
        if emitted >= limit:
            break


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream a sample from a monthly Reddit comments parquet"
    )
    parser.add_argument("--month", required=True, help="Month in YYYY-MM format")
    parser.add_argument(
        "--subreddit",
        action="append",
        default=[],
        help="Community to keep; repeat for multiple values",
    )
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument(
        "--output", type=Path, default=Path("data/raw/reddit.jsonl")
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    rows = stream_month(args.month)
    events = collect_events(
        rows,
        subreddits=set(args.subreddit),
        limit=args.limit,
    )
    count = write_jsonl(events, args.output)
    print(f"wrote {count} Reddit events to {args.output}")


if __name__ == "__main__":
    main()
