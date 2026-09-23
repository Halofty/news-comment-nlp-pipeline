from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterable

import requests

from collectors.current.models import CollectionManifest, CollectionWindow, KST
from collectors.google_news import RSS_URL, TOPIC_GROUPS, build_query
from collectors.web_news.normalization import canonicalize_url, normalize_title
from core.events import stable_event_id, utc_now_iso


@dataclass(frozen=True)
class GoogleNewsWindowResult:
    raw_responses: tuple[bytes, ...]
    events: tuple[dict[str, Any], ...]
    manifest: CollectionManifest


def _days_touched(window: CollectionWindow) -> Iterable[date]:
    current = window.start.astimezone(KST).date()
    final = (window.end.astimezone(KST) - timedelta(microseconds=1)).date()
    while current <= final:
        yield current
        current += timedelta(days=1)


def _parse_items(xml: bytes, *, topic_group: str, collected_at: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml)
    events: list[dict[str, Any]] = []
    for item in root.findall("./channel/item"):
        raw_title = (item.findtext("title") or "").strip()
        raw_url = (item.findtext("link") or "").strip()
        raw_published = (item.findtext("pubDate") or "").strip()
        source = item.find("source")
        publisher = ((source.text if source is not None else "") or "Unknown").strip()
        if not raw_title or not raw_url or not raw_published:
            continue
        try:
            published_at = parsedate_to_datetime(raw_published)
        except (TypeError, ValueError):
            continue
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        published_at = published_at.astimezone(timezone.utc)
        title = raw_title
        suffix = f" - {publisher}"
        if publisher != "Unknown" and title.endswith(suffix):
            title = title[: -len(suffix)].strip()
        url = canonicalize_url(raw_url)
        events.append(
            {
                "event_id": stable_event_id("web_news", url),
                "source_type": "news",
                "source_name": "web_news",
                "event_time": published_at.isoformat().replace("+00:00", "Z"),
                "collected_at": collected_at,
                "language": "en",
                "title": title,
                "text": title,
                "url": url,
                "community": None,
                "engagement": None,
                "schema_version": 1,
                "metadata": {
                    "publisher": publisher,
                    "source_page_url": RSS_URL,
                    "normalized_title": normalize_title(title),
                    "matched_keywords": topic_group,
                    "google_news_topic_group": topic_group,
                    "google_news_pub_date": raw_published,
                    "text_scope": "title_only",
                },
            }
        )
    return events


def collect_google_news_window(
    window: CollectionWindow,
    *,
    topic_group: str = "economy",
    session: requests.Session | None = None,
    timeout: float = 60,
) -> GoogleNewsWindowResult:
    if topic_group not in TOPIC_GROUPS:
        raise ValueError(f"unknown Google News topic group: {topic_group}")
    client = session or requests.Session()
    client.headers.setdefault(
        "User-Agent", "news-comment-nlp-pipeline/0.2 (current data collector)"
    )
    collected_at = utc_now_iso()
    raw_responses: list[bytes] = []
    received = 0
    rejected = 0
    unique: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    capped_requests = 0
    days = list(_days_touched(window))
    for selected_date in days:
        response = client.get(
            RSS_URL,
            params={
                "q": build_query(TOPIC_GROUPS[topic_group], selected_date),
                "hl": "en-US",
                "gl": "US",
                "ceid": "US:en",
            },
            timeout=timeout,
        )
        response.raise_for_status()
        raw_responses.append(response.content)
        parsed = _parse_items(
            response.content, topic_group=topic_group, collected_at=collected_at
        )
        item_count = len(ET.fromstring(response.content).findall("./channel/item"))
        received += item_count
        rejected += item_count - len(parsed)
        capped_requests += int(item_count >= 100)
        for event in parsed:
            event_id = str(event["event_id"])
            if event_id in unique:
                duplicate_count += 1
            else:
                unique[event_id] = event
    manifest = CollectionManifest(
        source="google_news",
        window=window,
        requested=len(days),
        received=received,
        normalized=len(unique),
        duplicate_in_window=duplicate_count,
        rejected=rejected,
        cursor_before=window.start_utc.isoformat(),
        cursor_after=window.end_utc.isoformat(),
        metadata={
            "topic_group": topic_group,
            "requests_hitting_100_result_cap": capped_requests,
            "completeness": "search_index_snapshot_not_complete_news_corpus",
        },
    )
    return GoogleNewsWindowResult(tuple(raw_responses), tuple(unique.values()), manifest)

