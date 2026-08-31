from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterator

from core.subreddits import load_subreddit_allowlist
from storage.jsonl import read_jsonl, write_jsonl


def selected_events(
    input_path: Path, *, subreddits: set[str]
) -> Iterator[dict]:
    normalized = {name.casefold() for name in subreddits}
    for event in read_jsonl(input_path):
        community = str(event.get("community") or "").casefold()
        if community in normalized:
            yield event


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter Reddit TextEvent JSONL")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--subreddit-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    subreddits = load_subreddit_allowlist(args.subreddit_file)
    count = write_jsonl(
        selected_events(args.input, subreddits=subreddits), args.output
    )
    print(f"wrote {count} selected Reddit events to {args.output}")


if __name__ == "__main__":
    main()
