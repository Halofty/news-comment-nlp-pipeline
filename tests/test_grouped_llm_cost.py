from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

from jobs.estimate_grouped_llm_cost import estimate


def test_estimate_groups_all_topics_and_balances_askreddit(tmp_path) -> None:
    rows = [
        {
            "source_name": "reddit",
            "community": community,
            "title": "",
            "text": f"comment from {community}",
            "metadata_json": "{}",
            "year": 2012,
            "month": 1,
            "day": 1,
        }
        for community in (
            "politics",
            "Economics",
            "technology",
            "science",
            "AskReddit",
            "AskReddit",
            "AskReddit",
            "AskReddit",
        )
    ]
    for topic in ("politics", "economy", "technology", "environment"):
        rows.append(
            {
                "source_name": "web_news",
                "community": None,
                "title": f"{topic} headline",
                "text": f"{topic} headline",
                "metadata_json": json.dumps({"google_news_topic_group": topic}),
                "year": 2012,
                "month": 1,
                "day": 1,
            }
        )
    source = tmp_path / "input.parquet"
    pq.write_table(pa.Table.from_pylist(rows), source)

    result = estimate(
        input_path=source,
        config_path="config/analysis-groups.yaml",
        output_path=tmp_path / "estimate.json",
        year=2012,
        month=1,
        content_tokens_per_chunk=50_000,
        max_text_bytes=16_000,
    )

    assert result["reddit_rows"]["topic_groups_total"] == 4
    assert result["reddit_rows"]["askreddit_available"] == 4
    assert result["reddit_rows"]["askreddit_sample"] == 1
    assert result["request_counts"] == {
        "map_expected": 5,
        "map_conservative_maximum": 5,
        "daily_reduce": 5,
        "monthly_reduce": 5,
        "total_expected": 15,
        "total_conservative_maximum": 15,
        "final_results": 10,
    }
