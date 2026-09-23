from __future__ import annotations

from datetime import datetime, timezone

from collectors.current.models import CollectionWindow
from collectors.current.reddit import RedditCredentials, collect_reddit_window
from core.events import validate_event


class _Response:
    def __init__(self, payload) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self):
        return self.payload


class _Session:
    def __init__(self, timestamp: float) -> None:
        self.timestamp = timestamp
        self.post_calls = []
        self.get_calls = []

    def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        return _Response({"access_token": "test-token"})

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        subreddit = url.split("/r/", 1)[1].split("/", 1)[0]
        return _Response(
            {
                "data": {
                    "after": None,
                    "children": [
                        {
                            "data": {
                                "id": f"id-{subreddit}",
                                "body": f"comment from {subreddit}",
                                "subreddit": subreddit,
                                "created_utc": self.timestamp,
                                "score": 3,
                                "permalink": f"/r/{subreddit}/comments/x/y/z/",
                                "link_id": "t3_x",
                                "controversiality": 0,
                                "author": "must-not-be-saved",
                            }
                        }
                    ],
                }
            }
        )


def test_current_reddit_uses_four_sources_and_removes_author() -> None:
    start = datetime(2026, 9, 23, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 9, 23, 0, 30, tzinfo=timezone.utc)
    session = _Session(datetime(2026, 9, 23, 0, 10, tzinfo=timezone.utc).timestamp())
    result = collect_reddit_window(
        CollectionWindow(start, end),
        credentials=RedditCredentials("client", "secret", "test-agent"),
        session=session,
    )

    assert len(session.get_calls) == 4
    assert {event["community"] for event in result.events} == {
        "Economics",
        "business",
        "TrueReddit",
        "changemyview",
    }
    assert all("author" not in row for row in result.raw_rows)
    assert all(validate_event(event) for event in result.events)
    assert result.manifest.status == "complete"
    assert result.manifest.normalized == 4


def test_current_reddit_marks_page_limit_as_partial() -> None:
    start = datetime(2026, 9, 23, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 9, 23, 0, 30, tzinfo=timezone.utc)

    class _AlwaysMore(_Session):
        def get(self, url, **kwargs):
            response = super().get(url, **kwargs)
            response.payload["data"]["after"] = "next-page"
            return response

    session = _AlwaysMore(datetime(2026, 9, 23, 0, 10, tzinfo=timezone.utc).timestamp())
    result = collect_reddit_window(
        CollectionWindow(start, end),
        credentials=RedditCredentials("client", "secret", "test-agent"),
        session=session,
        max_pages_per_subreddit=1,
    )
    assert result.manifest.status == "partial"
    assert result.manifest.cursor_after is None
    assert result.manifest.metadata["truncated_subreddits"] == [
        "Economics",
        "business",
        "TrueReddit",
        "changemyview",
    ]

