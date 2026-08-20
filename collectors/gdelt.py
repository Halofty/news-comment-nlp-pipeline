from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.events import stable_event_id, utc_now_iso
from storage.jsonl import write_jsonl

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_DATETIME_FORMAT = "%Y%m%d%H%M%S"


def build_session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "news-comment-nlp-pipeline/0.1"
    return session


def parse_datetime(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = datetime.strptime(value, GDELT_DATETIME_FORMAT)
    return parsed.strftime(GDELT_DATETIME_FORMAT)


def fetch_articles(
    query: str,
    *,
    max_records: int = 250,
    start: str | None = None,
    end: str | None = None,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    if not query.strip():
        raise ValueError("query must not be empty")
    if not 1 <= max_records <= 250:
        raise ValueError("max_records must be between 1 and 250")

    params: dict[str, str | int] = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": max_records,
        "format": "json",
        "sort": "DateDesc",
    }
    if start:
        params["startdatetime"] = parse_datetime(start)
    if end:
        params["enddatetime"] = parse_datetime(end)

    client = session or build_session()
    response = client.get(GDELT_DOC_API, params=params, timeout=(5, 30))
    response.raise_for_status()
    try:
        payload = response.json()
    except requests.exceptions.JSONDecodeError as error:
        preview = response.text.strip().replace("\n", " ")[:160]
        raise RuntimeError(
            f"GDELT returned a non-JSON response: {preview or 'empty response'}"
        ) from error
    articles = payload.get("articles", [])
    if not isinstance(articles, list):
        raise ValueError("GDELT response does not contain an article list")
    return articles


def article_to_event(
    article: dict[str, Any], *, query: str, collected_at: str
) -> dict[str, Any] | None:
    url = str(article.get("url") or "").strip()
    title = str(article.get("title") or "").strip()
    seen_date = str(article.get("seendate") or "").strip()
    if not url or not title or not seen_date:
        return None

    event_time = datetime.strptime(seen_date, "%Y%m%dT%H%M%SZ").isoformat() + "Z"
    # MVP에서는 GDELT가 직접 제공하는 제목만 분석한다. 향후 원문 URL에서
    # 본문을 추출할 때는 text를 정제 본문으로 교체하고 text_scope를
    # "full_text"로 기록한다. 원문 수집 실패 시에는 현재 title_only 이벤트를
    # fallback으로 유지한다.
    return {
        "event_id": stable_event_id("gdelt", url),
        "source_type": "news",
        "source_name": "gdelt",
        "event_time": event_time,
        "collected_at": collected_at,
        "language": str(article.get("language") or "unknown").lower(),
        "title": title,
        "text": title,
        "url": url,
        "community": None,
        "engagement": None,
        "schema_version": 1,
        "metadata": {
            "domain": article.get("domain"),
            "source_country": article.get("sourcecountry"),
            "query": query,
            "text_scope": "title_only",
        },
    }


def collect_events(
    query: str,
    *,
    max_records: int = 250,
    start: str | None = None,
    end: str | None = None,
    session: requests.Session | None = None,
) -> Iterable[dict[str, Any]]:
    collected_at = utc_now_iso()
    for article in fetch_articles(
        query,
        max_records=max_records,
        start=start,
        end=end,
        session=session,
    ):
        event = article_to_event(article, query=query, collected_at=collected_at)
        if event is not None:
            yield event


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect GDELT news metadata")
    parser.add_argument("--query", required=True, help="GDELT search query")
    parser.add_argument("--max-records", type=int, default=250)
    parser.add_argument("--start", help="UTC time in YYYYMMDDHHMMSS format")
    parser.add_argument("--end", help="UTC time in YYYYMMDDHHMMSS format")
    parser.add_argument(
        "--output", type=Path, default=Path("data/raw/gdelt.jsonl")
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    events = collect_events(
        args.query,
        max_records=args.max_records,
        start=args.start,
        end=args.end,
    )
    count = write_jsonl(events, args.output)
    print(f"wrote {count} GDELT events to {args.output}")


if __name__ == "__main__":
    main()
