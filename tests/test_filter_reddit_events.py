import json

from jobs.filter_reddit_events import selected_events


def test_selected_events_uses_case_insensitive_allowlist(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event_id": "a" * 64,
                        "source_type": "comment",
                        "source_name": "reddit",
                        "event_time": "2016-01-01T00:00:00Z",
                        "collected_at": "2026-08-31T00:00:00Z",
                        "language": "unknown",
                        "title": None,
                        "text": "keep",
                        "url": None,
                        "community": "WorldNews",
                        "engagement": 1,
                        "schema_version": 1,
                        "metadata": {},
                    }
                ),
                json.dumps(
                    {
                        "event_id": "b" * 64,
                        "source_type": "comment",
                        "source_name": "reddit",
                        "event_time": "2016-01-01T00:00:01Z",
                        "collected_at": "2026-08-31T00:00:00Z",
                        "language": "unknown",
                        "title": None,
                        "text": "skip",
                        "url": None,
                        "community": "other",
                        "engagement": 1,
                        "schema_version": 1,
                        "metadata": {},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    rows = list(selected_events(path, subreddits={"worldnews"}))

    assert [row["text"] for row in rows] == ["keep"]
