from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from collectors.current.models import CollectionManifest, CollectionWindow
from storage.collection_cursor import FileCollectionCursorStore


def test_collection_window_is_half_open_and_timezone_aware() -> None:
    window = CollectionWindow(
        datetime(2026, 9, 23, 0, 5, tzinfo=timezone(timedelta(hours=9))),
        datetime(2026, 9, 23, 0, 35, tzinfo=timezone(timedelta(hours=9))),
    )
    assert window.collected_date == "2026-09-23"
    assert window.contains(datetime(2026, 9, 22, 15, 5, tzinfo=timezone.utc))
    assert not window.contains(datetime(2026, 9, 22, 15, 35, tzinfo=timezone.utc))


def test_collection_window_rejects_naive_or_empty_range() -> None:
    with pytest.raises(ValueError, match="timezone"):
        CollectionWindow(datetime(2026, 1, 1), datetime(2026, 1, 2))
    moment = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="after start"):
        CollectionWindow(moment, moment)


def test_manifest_validates_row_accounting() -> None:
    window = CollectionWindow(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
    )
    with pytest.raises(ValueError, match="exceeds"):
        CollectionManifest(
            source="reddit",
            window=window,
            requested=1,
            received=2,
            normalized=2,
            duplicate_in_window=1,
            rejected=0,
        )


def test_cursor_store_is_atomic_and_never_moves_backwards(tmp_path) -> None:
    store = FileCollectionCursorStore(tmp_path)
    first = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)
    store.advance(source="google_news", window_end=first, metadata={"rows": 10})
    assert store.load("google_news")["metadata"] == {"rows": 10}
    with pytest.raises(ValueError, match="backwards"):
        store.advance(
            source="google_news",
            window_end=first - timedelta(minutes=30),
        )
    assert not list(tmp_path.glob("*.tmp"))

