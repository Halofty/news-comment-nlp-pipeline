from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collectors.current.models import CollectionWindow
from orchestration.current_ingestion import _effective_window


def test_effective_window_backfills_gap_from_cursor() -> None:
    requested = CollectionWindow(
        datetime(2026, 9, 23, 1, tzinfo=timezone.utc),
        datetime(2026, 9, 23, 1, 30, tzinfo=timezone.utc),
    )
    effective = _effective_window(
        requested,
        cursor={"window_end": "2026-09-23T00:30:00Z"},
    )
    assert effective.start_utc == datetime(2026, 9, 23, 0, 30, tzinfo=timezone.utc)
    assert effective.end_utc - effective.start_utc == timedelta(hours=1)

