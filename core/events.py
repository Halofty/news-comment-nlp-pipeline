from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any

EVENT_FIELDS = frozenset(
    {
        "event_id",
        "source_type",
        "source_name",
        "event_time",
        "collected_at",
        "language",
        "title",
        "text",
        "url",
        "community",
        "engagement",
        "schema_version",
        "metadata",
    }
)
EVENT_ID_PATTERN = re.compile(r"^[a-f0-9]{64}$")
SOURCE_TYPES = {"news", "comment"}
SOURCE_NAMES = {"gdelt", "reddit", "web_news"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_event_id(source_name: str, source_id: str) -> str:
    value = f"{source_name}:{source_id}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def parse_event_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("event_time must be an ISO-8601 string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("event_time must include a timezone")
    return parsed


def validate_event(event: Any, *, line_number: int | None = None) -> dict[str, Any]:
    location = f" on line {line_number}" if line_number is not None else ""
    if not isinstance(event, dict):
        raise ValueError(f"event{location} must be a JSON object")

    missing = sorted(EVENT_FIELDS.difference(event))
    if missing:
        raise ValueError(f"event{location} is missing fields: {', '.join(missing)}")
    unexpected = sorted(set(event).difference(EVENT_FIELDS))
    if unexpected:
        raise ValueError(
            f"event{location} has unexpected fields: {', '.join(unexpected)}"
        )
    if not isinstance(event["event_id"], str) or not EVENT_ID_PATTERN.fullmatch(
        event["event_id"]
    ):
        raise ValueError(f"event_id{location} must be a 64-character SHA-256 hex")
    if event["source_type"] not in SOURCE_TYPES:
        raise ValueError(f"source_type{location} must be news or comment")
    if event["source_name"] not in SOURCE_NAMES:
        raise ValueError(
            f"source_name{location} must be one of: {', '.join(sorted(SOURCE_NAMES))}"
        )
    if not isinstance(event["language"], str) or not event["language"].strip():
        raise ValueError(f"language{location} must be a non-empty string")
    if event["title"] is not None and not isinstance(event["title"], str):
        raise ValueError(f"title{location} must be a string or null")
    if not isinstance(event["text"], str) or not event["text"].strip():
        raise ValueError(f"text{location} must be a non-empty string")
    if event["url"] is not None and not isinstance(event["url"], str):
        raise ValueError(f"url{location} must be a string or null")
    if event["community"] is not None and not isinstance(event["community"], str):
        raise ValueError(f"community{location} must be a string or null")
    if event["engagement"] is not None and type(event["engagement"]) is not int:
        raise ValueError(f"engagement{location} must be an integer or null")
    if event["schema_version"] != 1:
        raise ValueError(f"schema_version{location} must be 1")
    if not isinstance(event["metadata"], dict):
        raise ValueError(f"metadata{location} must be an object")
    parse_event_time(event["event_time"])
    parse_event_time(event["collected_at"])
    return event
