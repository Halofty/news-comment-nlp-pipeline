from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collectors.current.google_news import collect_google_news_window
from collectors.current.models import CollectionWindow
from core.events import validate_event

RSS = b"""<?xml version="1.0"?><rss><channel>
<item><title>Markets rise - Example</title>
<link>https://news.google.com/rss/articles/one</link>
<pubDate>Tue, 22 Sep 2026 16:10:00 GMT</pubDate>
<source>Example</source></item>
<item><title>Markets rise - Example</title>
<link>https://news.google.com/rss/articles/one</link>
<pubDate>Tue, 22 Sep 2026 16:10:00 GMT</pubDate>
<source>Example</source></item>
<item><title>Missing date</title><link>https://example.com/two</link></item>
</channel></rss>"""


class _Response:
    content = RSS

    def raise_for_status(self) -> None:
        pass


class _Session:
    def __init__(self) -> None:
        self.headers = {}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _Response()


def test_current_google_news_collects_snapshot_and_exact_publish_time() -> None:
    kst = timezone(timedelta(hours=9))
    window = CollectionWindow(
        datetime(2026, 9, 23, 0, 5, tzinfo=kst),
        datetime(2026, 9, 23, 0, 35, tzinfo=kst),
    )
    session = _Session()
    result = collect_google_news_window(window, session=session)

    assert len(session.calls) == 1
    assert len(result.events) == 1
    assert result.events[0]["event_time"] == "2026-09-22T16:10:00Z"
    assert result.events[0]["title"] == "Markets rise"
    validate_event(result.events[0])
    assert result.manifest.received == 3
    assert result.manifest.normalized == 1
    assert result.manifest.duplicate_in_window == 1
    assert result.manifest.rejected == 1


def test_current_google_news_queries_both_dates_for_cross_midnight_window() -> None:
    kst = timezone(timedelta(hours=9))
    session = _Session()
    collect_google_news_window(
        CollectionWindow(
            datetime(2026, 9, 23, 23, 35, tzinfo=kst),
            datetime(2026, 9, 24, 0, 5, tzinfo=kst),
        ),
        session=session,
    )
    assert len(session.calls) == 2

