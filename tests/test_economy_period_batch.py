from __future__ import annotations

import json
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from llm_analysis.economy_period import (
    build_economy_daily_batch,
    build_economy_monthly_batch,
)


def test_builds_all_economy_rows_without_sampling(tmp_path) -> None:
    rows = []
    for day in range(1, 32):
        rows.extend(
            [
                {
                    "event_id": f"r-{day}", "source_name": "reddit", "community": "Economics",
                    "title": "", "text": f"economy comment {day}", "metadata_json": "{}",
                    "year": 2012, "month": 1, "day": day,
                },
                {
                    "event_id": f"x-{day}", "source_name": "reddit", "community": "AskReddit",
                    "title": "", "text": "excluded", "metadata_json": "{}",
                    "year": 2012, "month": 1, "day": day,
                },
                {
                    "event_id": f"n-{day}", "source_name": "web_news", "community": "",
                    "title": f"economy news {day}", "text": "",
                    "metadata_json": json.dumps({"google_news_topic_group": "economy"}),
                    "year": 2012, "month": 1, "day": day,
                },
            ]
        )
    source = tmp_path / "input.parquet"
    pq.write_table(pa.Table.from_pylist(rows), source)
    config = tmp_path / "groups.yaml"
    config.write_text(
        "groups:\n  economy:\n    news_topic: economy\n    subreddits: [Economics, business, news, TrueReddit, changemyview]\n",
        encoding="utf-8",
    )
    result = build_economy_daily_batch(
        input_path=source, config_path=config,
        request_path=tmp_path / "requests.jsonl", manifest_path=tmp_path / "manifest.jsonl",
        report_path=tmp_path / "report.json", year=2012, month=1,
        budget_usd=Decimal("1"),
    )
    report = json.loads((tmp_path / "report.json").read_text())
    assert result.request_rows == 31
    assert report["reddit_rows"] == report["web_news_rows"] == 31
    assert report["sampling"] == "none"
    assert "excluded" not in (tmp_path / "requests.jsonl").read_text()


def test_builds_monthly_request_from_31_daily_results(tmp_path) -> None:
    source = tmp_path / "daily.jsonl"
    source.write_text(
        "".join(
            json.dumps({
                "event_id": f"period:economy-society:2012-01-{day:02d}",
                "sentiment": "neutral", "sentiment_score": 0,
                "topics": ["economy"], "keywords": ["market"], "summary": "Daily summary.",
            }) + "\n"
            for day in range(1, 32)
        ),
        encoding="utf-8",
    )
    result = build_economy_monthly_batch(
        daily_results_path=source, request_path=tmp_path / "monthly.jsonl",
        manifest_path=tmp_path / "monthly-manifest.jsonl", report_path=tmp_path / "monthly-report.json",
        year=2012, month=1, budget_usd=Decimal("1"),
    )
    assert result.request_rows == 1
    assert len((tmp_path / "monthly.jsonl").read_text().splitlines()) == 1


def test_builds_selected_daily_range(tmp_path) -> None:
    rows = [
        {
            "event_id": f"r-{day}", "source_name": "reddit", "community": "business",
            "title": "", "text": f"comment {day}", "metadata_json": "{}",
            "year": 2012, "month": 1, "day": day,
        }
        for day in range(1, 32)
    ]
    source = tmp_path / "input.parquet"
    pq.write_table(pa.Table.from_pylist(rows), source)
    config = tmp_path / "groups.yaml"
    config.write_text(
        "groups:\n  economy:\n    news_topic: economy\n    subreddits: [Economics, business, news, TrueReddit, changemyview]\n",
        encoding="utf-8",
    )
    result = build_economy_daily_batch(
        input_path=source, config_path=config, request_path=tmp_path / "requests.jsonl",
        manifest_path=tmp_path / "manifest.jsonl", report_path=tmp_path / "report.json",
        year=2012, month=1, start_day=1, end_day=15, budget_usd=Decimal("1"),
    )
    report = json.loads((tmp_path / "report.json").read_text())
    assert result.request_rows == 15
    assert report["day_range"] == {"start": 1, "end": 15}
    assert report["reddit_rows"] == 15
