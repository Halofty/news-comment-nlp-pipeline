from __future__ import annotations

from datetime import date, datetime
from typing import Iterator

import scrapy
from scrapy.exceptions import CloseSpider

from collectors.web_news.event_mapper import news_title_to_event
from collectors.web_news.normalization import canonicalize_url, matching_keywords
from core.events import utc_now_iso

DEFAULT_KEYWORDS = (
    "government",
    "election",
    "president",
    "parliament",
    "economy",
    "economic",
    "market",
    "business",
    "technology",
    "internet",
    "digital",
    "climate",
    "energy",
    "environment",
)
SUPPORTED_START = date(2012, 1, 1)
SUPPORTED_END = date(2016, 2, 29)


def month_starts(start: date, end: date) -> Iterator[date]:
    current = start.replace(day=1)
    while current <= end:
        yield current
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


class GlobalVoicesArchiveSpider(scrapy.Spider):
    name = "global_voices_archive"
    allowed_domains = ["globalvoices.org"]

    def __init__(
        self,
        start_date: str = SUPPORTED_START.isoformat(),
        end_date: str = SUPPORTED_END.isoformat(),
        keywords: str = ",".join(DEFAULT_KEYWORDS),
        max_pages_per_month: str = "0",
        max_items: str = "0",
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.start_date = date.fromisoformat(start_date)
        self.end_date = date.fromisoformat(end_date)
        if self.end_date < self.start_date:
            raise ValueError("end_date must not be before start_date")
        if self.start_date < SUPPORTED_START or self.end_date > SUPPORTED_END:
            raise ValueError(
                f"date range must stay within {SUPPORTED_START}..{SUPPORTED_END}"
            )
        self.keywords = tuple(
            keyword.strip() for keyword in keywords.split(",") if keyword.strip()
        )
        self.max_pages_per_month = int(max_pages_per_month)
        self.max_items = int(max_items)
        self.collected_at = utc_now_iso()
        self.seen_urls: set[str] = set()
        self.emitted = 0

    def _initial_requests(self) -> Iterator[scrapy.Request]:
        for month in month_starts(self.start_date, self.end_date):
            url = f"https://globalvoices.org/{month.year:04d}/{month.month:02d}/"
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"page_number": 1})

    async def start(self):
        """Scrapy 2.13+ asynchronous spider entry point."""
        for request in self._initial_requests():
            yield request

    def start_requests(self) -> Iterator[scrapy.Request]:
        """Compatibility entry point for Scrapy releases before 2.13."""
        yield from self._initial_requests()

    def parse(self, response: scrapy.http.Response, page_number: int = 1):
        for card in response.css("article.gv-post-promo-card"):
            href = card.css("h3.post-title a::attr(href)").get()
            title = card.css("h3.post-title a::text").get()
            date_text = card.css("span.datestamp::text").get()
            if not href or not title or not date_text:
                continue
            try:
                published_date = datetime.strptime(
                    date_text.strip(), "%d %B %Y"
                ).date()
            except ValueError:
                self.logger.warning("Skipping unparseable date %r", date_text)
                continue
            if not self.start_date <= published_date <= self.end_date:
                continue

            url = canonicalize_url(response.urljoin(href))
            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)
            matches = matching_keywords(title, self.keywords)
            if self.keywords and not matches:
                continue
            if self.max_items and self.emitted >= self.max_items:
                raise CloseSpider("max_items reached")
            self.emitted += 1
            yield news_title_to_event(
                title=title,
                url=url,
                published_date=published_date,
                collected_at=self.collected_at,
                publisher="Global Voices",
                source_page_url=response.url,
                matched_keywords=matches,
            )

        can_follow = not self.max_pages_per_month or page_number < self.max_pages_per_month
        older_url = response.xpath(
            "(//div[contains(@class, 'navigation')]//a["
            "contains(normalize-space(.), 'Older stories')]/@href)[1]"
        ).get()
        if can_follow and older_url:
            yield response.follow(
                older_url,
                callback=self.parse,
                cb_kwargs={"page_number": page_number + 1},
            )
