from __future__ import annotations

import argparse
import json
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterator

import requests

from collectors.web_news.event_mapper import news_title_to_event
from core.events import utc_now_iso
from storage.jsonl import write_jsonl


RSS_URL = "https://news.google.com/rss/search"
TOPIC_GROUPS = {
    "politics": ("government", "election", "president", "parliament"),
    "economy": ("economy", "economic", "market", "business"),
    "technology": ("technology", "internet", "digital"),
    "environment": ("climate", "energy", "environment"),
}


def dates(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_query(keywords: tuple[str, ...], day: date) -> str:
    following = day + timedelta(days=1)
    terms = " OR ".join(f'"{keyword}"' for keyword in keywords)
    return f"({terms}) after:{day.isoformat()} before:{following.isoformat()}"


def parse_rss(xml: bytes, *, topic_group: str, collected_at: str) -> list[dict[str, Any]]:
    events = []
    root = ET.fromstring(xml)
    for item in root.findall("./channel/item"):
        raw_title = (item.findtext("title") or "").strip()
        url = (item.findtext("link") or "").strip()
        published = (item.findtext("pubDate") or "").strip()
        source = item.find("source")
        publisher = ((source.text if source is not None else "") or "Unknown").strip()
        if not raw_title or not url or not published:
            continue
        title = raw_title
        suffix = f" - {publisher}"
        if publisher != "Unknown" and title.endswith(suffix):
            title = title[: -len(suffix)].strip()
        published_at = parsedate_to_datetime(published)
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        event = news_title_to_event(
            title=title,
            url=url,
            published_date=published_at.astimezone(timezone.utc).date(),
            collected_at=collected_at,
            publisher=publisher,
            source_page_url=RSS_URL,
            matched_keywords=(topic_group,),
        )
        event["metadata"]["google_news_topic_group"] = topic_group
        event["metadata"]["google_news_pub_date"] = published
        events.append(event)
    return events


def collect_month(
    *,
    start_date: date,
    end_date: date,
    output_root: Path,
    report_path: Path,
    request_delay: float,
) -> dict[str, Any]:
    session = requests.Session()
    session.headers["User-Agent"] = (
        "news-comment-nlp-pipeline/0.1 (educational research collector)"
    )
    collected_at = utc_now_iso()
    unique: dict[str, dict[str, Any]] = {}
    request_rows = []

    for day in dates(start_date, end_date):
        for group, keywords in TOPIC_GROUPS.items():
            query = build_query(keywords, day)
            response = session.get(
                RSS_URL,
                params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"},
                timeout=120,
            )
            response.raise_for_status()
            events = parse_rss(
                response.content, topic_group=group, collected_at=collected_at
            )
            request_rows.append(
                {
                    "date": day.isoformat(),
                    "topic_group": group,
                    "returned": len(events),
                    "hit_100_result_cap": len(events) >= 100,
                }
            )
            for event in events:
                existing = unique.get(event["url"])
                if existing is None:
                    unique[event["url"]] = event
                else:
                    groups = set(
                        existing["metadata"]["google_news_topic_group"].split(",")
                    )
                    groups.add(group)
                    existing["metadata"]["google_news_topic_group"] = ",".join(
                        sorted(groups)
                    )
            if request_delay:
                time.sleep(request_delay)

    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    publishers: Counter[str] = Counter()
    for event in unique.values():
        day = event["event_time"][:10]
        if start_date.isoformat() <= day <= end_date.isoformat():
            by_day[day].append(event)
            publishers[event["metadata"]["publisher"]] += 1

    for day, events in sorted(by_day.items()):
        year, month, day_number = day.split("-")
        output = (
            output_root
            / f"year={year}"
            / f"month={month}"
            / f"day={day_number}"
            / "events.jsonl"
        )
        write_jsonl(sorted(events, key=lambda event: event["event_id"]), output)

    report = {
        "source": "Google News RSS search index",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "topic_groups": TOPIC_GROUPS,
        "requests": len(request_rows),
        "requests_hitting_100_result_cap": sum(
            int(row["hit_100_result_cap"]) for row in request_rows
        ),
        "unique_events": len(unique),
        "events_within_requested_dates": sum(map(len, by_day.values())),
        "days_with_results": len(by_day),
        "top_publishers": publishers.most_common(30),
        "request_results": request_rows,
        "completeness_warning": (
            "Google News RSS is a capped search index, not a complete publisher archive."
        ),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    from storage.data_lake import publish_artifact_if_enabled

    publish_artifact_if_enabled(report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect historical Google News RSS titles")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--output-root", type=Path, default=Path("data/raw/google-news")
    )
    parser.add_argument(
        "--report", type=Path, default=Path("data/reports/google-news-report.json")
    )
    parser.add_argument("--request-delay", type=float, default=1.0)
    args = parser.parse_args()
    if args.end_date < args.start_date:
        raise ValueError("end-date must not be before start-date")
    report = collect_month(
        start_date=args.start_date,
        end_date=args.end_date,
        output_root=args.output_root,
        report_path=args.report,
        request_delay=args.request_delay,
    )
    print(json.dumps({key: report[key] for key in (
        "requests", "unique_events", "events_within_requested_dates",
        "requests_hitting_100_result_cap",
    )}, ensure_ascii=False))


if __name__ == "__main__":
    main()
