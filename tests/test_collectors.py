from __future__ import annotations

import requests

from collectors.gdelt import article_to_event, fetch_articles
from collectors.reddit import collect_events, comment_to_event, date_bounds


class FakeResponse:
    text = ""

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"articles": [{"url": "https://example.com/article"}]}


class FakeSession:
    def __init__(self) -> None:
        self.params = None

    def get(self, url, *, params, timeout):
        self.params = params
        assert timeout == (5, 30)
        return FakeResponse()


class NonJsonResponse(FakeResponse):
    text = "Please limit requests to one every 5 seconds"

    def json(self) -> dict:
        raise requests.exceptions.JSONDecodeError("invalid", self.text, 0)


class NonJsonSession(FakeSession):
    def get(self, url, *, params, timeout):
        return NonJsonResponse()


def test_fetch_articles_builds_expected_query() -> None:
    session = FakeSession()
    articles = fetch_articles(
        "climate change",
        max_records=10,
        start="20260101000000",
        end="20260102000000",
        session=session,
    )
    assert articles == [{"url": "https://example.com/article"}]
    assert session.params["query"] == "climate change"
    assert session.params["maxrecords"] == 10
    assert session.params["startdatetime"] == "20260101000000"


def test_fetch_articles_reports_non_json_rate_limit_response() -> None:
    try:
        fetch_articles("example", session=NonJsonSession())
    except RuntimeError as error:
        assert "limit requests" in str(error)
    else:
        raise AssertionError("non-JSON GDELT response must raise RuntimeError")


def test_article_to_event_uses_title_as_analysis_text() -> None:
    event = article_to_event(
        {
            "url": "https://example.com/news",
            "title": "Example headline",
            "seendate": "20260816T190000Z",
            "language": "English",
            "domain": "example.com",
            "sourcecountry": "United States",
        },
        query="example",
        collected_at="2026-08-17T00:00:00Z",
    )
    assert event is not None
    assert event["source_type"] == "news"
    assert event["event_time"] == "2026-08-16T19:00:00Z"
    assert event["text"] == "Example headline"
    assert event["engagement"] is None
    assert event["metadata"]["text_scope"] == "title_only"
    assert len(event["event_id"]) == 64


def test_comment_to_event_removes_author_and_maps_fields() -> None:
    event = comment_to_event(
        {
            "id": "abc123",
            "author": "must-not-be-copied",
            "body": "A useful comment",
            "created_utc": 0,
            "subreddit": "worldnews",
            "score": 7,
            "link_id": "t3_post",
            "controversiality": 1,
        },
        collected_at="2026-08-17T00:00:00Z",
    )
    assert event is not None
    assert event["event_time"] == "1970-01-01T00:00:00Z"
    assert event["community"] == "worldnews"
    assert event["engagement"] == 7
    assert "author" not in event
    assert "author" not in event["metadata"]


def test_collect_events_filters_and_stops_at_limit() -> None:
    rows = [
        {
            "id": "1",
            "body": "skip",
            "created_utc": 1,
            "subreddit": "other",
        },
        {
            "id": "2",
            "body": "keep one",
            "created_utc": 2,
            "subreddit": "WorldNews",
        },
        {
            "id": "3",
            "body": "keep two",
            "created_utc": 3,
            "subreddit": "worldnews",
        },
    ]
    events = list(collect_events(rows, subreddits={"worldnews"}, limit=1))
    assert [event["text"] for event in events] == ["keep one"]


def test_collect_events_filters_inclusive_utc_date_range() -> None:
    start, end = date_bounds("1970-01-02", "1970-01-02")
    rows = [
        {"id": "1", "body": "before", "created_utc": 86_399},
        {"id": "2", "body": "keep", "created_utc": 86_400},
        {"id": "3", "body": "after", "created_utc": 172_800},
    ]
    events = list(
        collect_events(
            rows,
            subreddits=set(),
            limit=10,
            start_timestamp=start,
            end_timestamp=end,
        )
    )
    assert [event["text"] for event in events] == ["keep"]


def test_deleted_comment_is_dropped() -> None:
    assert (
        comment_to_event(
            {"id": "1", "body": "[deleted]", "created_utc": 1},
            collected_at="2026-08-17T00:00:00Z",
        )
        is None
    )
