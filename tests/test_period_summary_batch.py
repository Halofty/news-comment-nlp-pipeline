from __future__ import annotations

import json
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from llm_analysis.period_summary import build_period_summary_batch


def test_builds_31_daily_and_one_monthly_request(tmp_path) -> None:
    rows = []
    for day in range(1, 32):
        rows.extend(
            [
                {
                    "source_name": "reddit",
                    "title": "",
                    "text": f"Reddit comment for day {day}",
                    "community": "news",
                    "year": 2012,
                    "month": 1,
                    "day": day,
                },
                {
                    "source_name": "web_news",
                    "title": f"News headline for day {day}",
                    "text": "",
                    "community": "",
                    "year": 2012,
                    "month": 1,
                    "day": day,
                },
            ]
        )
    source = tmp_path / "input.parquet"
    pq.write_table(pa.Table.from_pylist(rows), source)
    request = tmp_path / "requests.jsonl"
    manifest = tmp_path / "manifest.jsonl"
    report = tmp_path / "preflight.json"

    result = build_period_summary_batch(
        input_path=source,
        request_path=request,
        manifest_path=manifest,
        report_path=report,
        year=2012,
        month=1,
        daily_budget_usd=Decimal("1"),
    )

    requests = [json.loads(line) for line in request.read_text().splitlines()]
    manifests = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert result.request_rows == 32
    assert len(requests) == len(manifests) == 32
    assert manifests[0]["period"] == "2012-01-01"
    assert manifests[-1]["period"] == "2012-01"
    assert manifests[-1]["total_rows"] == 62
    assert all(row["body"]["reasoning"]["effort"] == "low" for row in requests)
    assert json.loads(report.read_text())["complete_source_rows"] == {
        "reddit": 31,
        "web_news": 31,
    }
