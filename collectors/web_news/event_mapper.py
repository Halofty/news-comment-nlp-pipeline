from __future__ import annotations

from datetime import date, datetime, time, timezone

from core.events import stable_event_id
from collectors.web_news.normalization import canonicalize_url, normalize_title


def news_title_to_event(
    *,
    title: str,
    url: str,
    published_date: date,
    collected_at: str,
    publisher: str,
    source_page_url: str,
    matched_keywords: tuple[str, ...],
) -> dict[str, object]:
    canonical_url = canonicalize_url(url)
    normalized_title = normalize_title(title)
    event_time = datetime.combine(
        published_date, time.min, tzinfo=timezone.utc
    ).isoformat().replace("+00:00", "Z")
    return {
        "event_id": stable_event_id("web_news", canonical_url),
        "source_type": "news",
        "source_name": "web_news",
        "event_time": event_time,
        "collected_at": collected_at,
        "language": "en",
        "title": title.strip(),
        "text": title.strip(),
        "url": canonical_url,
        "community": None,
        "engagement": None,
        "schema_version": 1,
        "metadata": {
            "publisher": publisher,
            "source_page_url": canonicalize_url(source_page_url),
            "normalized_title": normalized_title,
            "matched_keywords": ",".join(matched_keywords),
            "language_status": "confirmed",
            "language_evidence": "publisher_english_archive",
            "text_scope": "title_only",
            "fallback_dedup_key": (
                f"{publisher.casefold()}|{published_date.isoformat()}|"
                f"{normalized_title.casefold()}"
            ),
        },
    }
