from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from core.events import validate_event
from storage.jsonl import read_jsonl

SCHEMA_PATH = Path("sample/schema.json")
SAMPLE_PATH = Path("sample/synthetic-events.jsonl")


def load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_json_schema_is_valid() -> None:
    Draft202012Validator.check_schema(load_schema())


def test_synthetic_events_match_json_schema_and_python_contract() -> None:
    validator = Draft202012Validator(
        load_schema(), format_checker=FormatChecker()
    )
    events = list(read_jsonl(SAMPLE_PATH))

    assert {event["source_type"] for event in events} == {"news", "comment"}
    for event in events:
        validator.validate(event)
        validate_event(event)


def test_news_sample_uses_title_only_and_null_engagement() -> None:
    news = next(
        event for event in read_jsonl(SAMPLE_PATH) if event["source_type"] == "news"
    )
    assert news["text"] == news["title"]
    assert news["engagement"] is None
    assert news["metadata"]["text_scope"] == "title_only"
