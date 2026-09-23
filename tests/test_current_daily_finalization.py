from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from core.events import stable_event_id
from orchestration.current_daily import finalize_current_day, resolve_analysis_dates
from storage.collection_cursor import FileCollectionCursorStore


def _event(source: str, source_id: str, event_time: str) -> dict:
    return {
        "event_id": stable_event_id(source, source_id),
        "source_type": "news" if source == "web_news" else "comment",
        "source_name": source,
        "event_time": event_time,
        "collected_at": "2026-09-24T00:05:00+09:00",
        "language": "en",
        "title": "Economy headline" if source == "web_news" else None,
        "text": "Economy headline" if source == "web_news" else "A community comment",
        "url": "https://example.com/news" if source == "web_news" else None,
        "community": None if source == "web_news" else "Economics",
        "engagement": None if source == "web_news" else 3,
        "schema_version": 1,
        "metadata": {},
    }


def _write_window(root: Path, source: str, events: list[dict], *, status: str = "complete") -> None:
    relative = (
        Path("data/raw/current/google-news")
        if source == "google_news"
        else Path("data/raw/current/community/provider=reddit")
    )
    directory = root / relative / "collected_date=2026-09-23/window_start=2026-09-23T000000+0900"
    directory.mkdir(parents=True)
    (directory / "events.jsonl").write_text(
        "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
    )
    (directory / "manifest.json").write_text(
        json.dumps(
            {
                "source": source,
                "window_start": "2026-09-23T00:00:00+09:00",
                "window_end": "2026-09-24T00:05:00+09:00",
                "requested": 1,
                "received": len(events),
                "normalized": len(events),
                "duplicate_in_window": 0,
                "rejected": 0,
                "status": status,
            }
        ),
        encoding="utf-8",
    )


def _advance_cursors(root: Path) -> None:
    store = FileCollectionCursorStore(root / "data/raw/current/_state")
    boundary = datetime(2026, 9, 23, 15, 5, tzinfo=timezone.utc)
    store.advance(source="google_news", window_end=boundary)
    store.advance(source="reddit", window_end=boundary)


def test_scheduled_date_uses_interval_end_in_kst() -> None:
    assert resolve_analysis_dates(
        data_interval_end=datetime(2026, 9, 24, 0, 10, tzinfo=timezone.utc),
        scheduled=True,
        start_date="2020-01-01",
        end_date="2020-01-03",
    ) == ["2026-09-23"]


def test_manual_date_range_expands_inclusive_dates() -> None:
    assert resolve_analysis_dates(
        data_interval_end=datetime(2026, 9, 24, tzinfo=timezone.utc),
        scheduled=False,
        start_date="2026-09-20",
        end_date="2026-09-22",
    ) == ["2026-09-20", "2026-09-21", "2026-09-22"]


def test_finalization_filters_kst_day_and_is_idempotent(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_STORAGE_BACKEND", "local")
    shared = _event("web_news", "shared", "2026-09-23T12:00:00Z")
    _write_window(
        tmp_path,
        "google_news",
        [shared, _event("web_news", "next-day", "2026-09-24T00:00:00+09:00")],
    )
    _write_window(
        tmp_path,
        "reddit",
        [_event("reddit", "comment-1", "2026-09-23T14:30:00Z")],
    )
    _advance_cursors(tmp_path)

    first = finalize_current_day(project_root=tmp_path, analysis_date=date(2026, 9, 23))
    second = finalize_current_day(project_root=tmp_path, analysis_date=date(2026, 9, 23))

    assert first["input_rows"] == 2
    assert first["stored_rows"] == 2
    assert first["source_rows"] == {"google_news": 1, "reddit": 1}
    assert second["events_sha256"] == first["events_sha256"]


def test_finalization_fails_closed_for_partial_source(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_STORAGE_BACKEND", "local")
    _write_window(tmp_path, "google_news", [_event("web_news", "n", "2026-09-23T12:00:00Z")])
    _write_window(
        tmp_path,
        "reddit",
        [_event("reddit", "r", "2026-09-23T12:00:00Z")],
        status="partial",
    )
    _advance_cursors(tmp_path)

    with pytest.raises(RuntimeError, match="incomplete reddit"):
        finalize_current_day(project_root=tmp_path, analysis_date=date(2026, 9, 23))


def test_finalization_fails_closed_for_collection_gap(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_STORAGE_BACKEND", "local")
    _write_window(tmp_path, "google_news", [_event("web_news", "n", "2026-09-23T12:00:00Z")])
    _write_window(tmp_path, "reddit", [_event("reddit", "r", "2026-09-23T12:00:00Z")])
    manifest_path = next(
        (tmp_path / "data/raw/current/google-news").glob("**/manifest.json")
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["window_start"] = "2026-09-23T00:30:00+09:00"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    _advance_cursors(tmp_path)

    with pytest.raises(RuntimeError, match="google_news collection gap"):
        finalize_current_day(project_root=tmp_path, analysis_date=date(2026, 9, 23))
