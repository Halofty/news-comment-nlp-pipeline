from core.events import stable_event_id, validate_event


def test_text_event_contract_accepts_web_news() -> None:
    event = {
        "event_id": stable_event_id("web_news", "https://example.com/story"),
        "source_type": "news",
        "source_name": "web_news",
        "event_time": "2016-02-29T00:00:00Z",
        "collected_at": "2026-08-29T00:00:00Z",
        "language": "en",
        "title": "Example title",
        "text": "Example title",
        "url": "https://example.com/story",
        "community": None,
        "engagement": None,
        "schema_version": 1,
        "metadata": {"publisher": "Example"},
    }

    assert validate_event(event) == event
