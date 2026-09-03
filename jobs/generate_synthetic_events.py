from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from core.events import stable_event_id
from storage.jsonl import write_jsonl

BASE_TIME = datetime(2026, 8, 20, tzinfo=timezone.utc)


def _quality_case_text(index: int) -> str:
    case = index % 100
    if case == 5:
        return "visible\u200bhidden"
    if case == 10:
        return "x" * 150
    if case == 15:
        return "https://a.example/x https://b.example/y"
    if case == 20:
        return "Contact sample.user@example.com for details."
    if case == 25:
        return "a" + "\u0301" * 10
    if case == 30:
        return "abcdefghij " * 500
    if case == 35:
        return "😀" * 16_385
    if case == 40:
        return "[removed]"
    if case == 45:
        return "control\u0000character"
    return f"Synthetic discussion text number {index} about technology and society."


def _event(index: int) -> dict[str, Any]:
    is_news = index % 2 == 0
    source_name = "gdelt" if is_news else "reddit"
    event_time = (BASE_TIME + timedelta(seconds=index)).isoformat().replace(
        "+00:00", "Z"
    )
    collected_at = (BASE_TIME + timedelta(minutes=5, seconds=index)).isoformat().replace(
        "+00:00", "Z"
    )
    text = _quality_case_text(index)
    return {
        "event_id": stable_event_id(source_name, f"synthetic-{index}"),
        "source_type": "news" if is_news else "comment",
        "source_name": source_name,
        "event_time": event_time,
        "collected_at": collected_at,
        "language": "english" if is_news else "unknown",
        "title": text if is_news else None,
        "text": text,
        "url": f"https://example.com/news/{index}" if is_news else None,
        "community": None if is_news else f"community_{index % 10}",
        "engagement": None if is_news else (index % 41) - 10,
        "schema_version": 1,
        "metadata": (
            {
                "domain": "example.com",
                "source_country": "Synthetic",
                "query": "technology society",
                "text_scope": "title_only",
            }
            if is_news
            else {"link_id": f"t3_synthetic_{index}", "controversiality": 0}
        ),
    }


def generate_events(count: int, *, start_index: int = 0) -> Iterator[dict[str, Any]]:
    if count < 1:
        raise ValueError("count must be at least 1")
    if start_index < 0:
        raise ValueError("start_index must not be negative")
    previous: dict[str, Any] | None = None
    for index in range(start_index, start_index + count):
        if index > 0 and index % 50 == 0 and previous is not None:
            event = deepcopy(previous)
        else:
            event = _event(index)
        yield event
        previous = event


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic public TextEvent samples"
    )
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    written = write_jsonl(
        generate_events(args.count, start_index=args.start_index), args.output
    )
    print(f"wrote {written} deterministic synthetic events to {args.output}")


if __name__ == "__main__":
    main()
