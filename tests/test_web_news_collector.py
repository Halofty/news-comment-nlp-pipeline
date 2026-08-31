from __future__ import annotations

from datetime import date
from pathlib import Path

from scrapy.http import HtmlResponse, Request

from collectors.web_news.event_mapper import news_title_to_event
from collectors.web_news.normalization import canonicalize_url, normalize_title
from collectors.web_news.spiders.global_voices_archive import (
    GlobalVoicesArchiveSpider,
    month_starts,
)
from core.events import validate_event

FIXTURE = Path("tests/fixtures/web_news/global_voices_archive.html")


def fixture_response() -> HtmlResponse:
    url = "https://globalvoices.org/2012/12/"
    return HtmlResponse(
        url=url,
        request=Request(url=url),
        body=FIXTURE.read_bytes(),
        encoding="utf-8",
    )


def test_normalization_preserves_separate_original_and_canonical_values() -> None:
    assert normalize_title("  Election\u00a0&amp;   Climate  ") == "Election & Climate"
    assert canonicalize_url(
        "HTTPS://GlobalVoices.org/story/?utm_source=x&b=2&a=1#top"
    ) == "https://globalvoices.org/story/?a=1&b=2"


def test_month_starts_includes_both_boundary_months() -> None:
    assert list(month_starts(date(2015, 12, 31), date(2016, 2, 1))) == [
        date(2015, 12, 1),
        date(2016, 1, 1),
        date(2016, 2, 1),
    ]


def test_spider_builds_one_initial_request_per_month() -> None:
    spider = GlobalVoicesArchiveSpider(
        start_date="2016-01-31", end_date="2016-02-01"
    )

    assert [request.url for request in spider.start_requests()] == [
        "https://globalvoices.org/2016/01/",
        "https://globalvoices.org/2016/02/",
    ]


def test_archive_spider_filters_keywords_dates_and_duplicate_urls() -> None:
    spider = GlobalVoicesArchiveSpider(
        start_date="2012-12-31",
        end_date="2012-12-31",
        keywords="election,digital",
        max_pages_per_month="2",
    )
    output = list(spider.parse(fixture_response()))
    events = [value for value in output if isinstance(value, dict)]
    requests = [value for value in output if isinstance(value, Request)]

    assert len(events) == 1
    assert len(requests) == 1
    event = validate_event(events[0])
    assert event["title"] == "Election & the Digital Public"
    assert event["text"] == event["title"]
    assert event["source_name"] == "web_news"
    assert event["event_time"] == "2012-12-31T00:00:00Z"
    assert event["url"] == (
        "https://globalvoices.org/2012/12/31/election-and-the-digital-public/"
    )
    assert event["metadata"]["matched_keywords"] == "election,digital"
    assert requests[0].url == "https://globalvoices.org/2012/12/page/2/"


def test_stable_id_uses_canonical_url() -> None:
    base = dict(
        title="Government &amp; Economy",
        published_date=date(2016, 2, 29),
        collected_at="2026-08-29T00:00:00Z",
        publisher="Global Voices",
        source_page_url="https://globalvoices.org/2016/02/",
        matched_keywords=("government", "economy"),
    )
    first = news_title_to_event(
        url="https://globalvoices.org/story/?utm_source=test", **base
    )
    second = news_title_to_event(url="https://globalvoices.org/story/", **base)

    assert first["event_id"] == second["event_id"]
    assert first["title"] == "Government &amp; Economy"
    assert first["metadata"]["normalized_title"] == "Government & Economy"
