from __future__ import annotations

import json
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from core.events import stable_event_id
from llm_analysis.group_daily import build_group_daily_batch
from orchestration.unified_daily import merge_daily_sources, prepare_date_range_configs


def _event(source: str, event_id: str) -> dict:
    is_news = source == "web_news"
    return {
        "event_id": stable_event_id(source, event_id),
        "source_type": "news" if is_news else "comment",
        "source_name": source,
        "event_time": "2012-02-01T00:00:00Z",
        "collected_at": "2026-09-06T00:00:00Z",
        "language": "en",
        "title": "Markets recover" if is_news else None,
        "text": "Markets recover" if is_news else "The economy is getting worse.",
        "url": "https://example.com/news" if is_news else None,
        "community": None if is_news else "Economics",
        "engagement": None,
        "schema_version": 1,
        "metadata": {"google_news_topic_group": "economy"} if is_news else {},
    }


def test_merge_daily_sources_preserves_both_sources(tmp_path) -> None:
    reddit = tmp_path / "reddit.jsonl"
    news = tmp_path / "news.jsonl"
    reddit.write_text(json.dumps(_event("reddit", "r1")) + "\n", encoding="utf-8")
    news.write_text(json.dumps(_event("web_news", "n1")) + "\n", encoding="utf-8")
    config = {
        "project_root": str(tmp_path),
        "combined_input_file": "data/combined.jsonl",
    }
    result = merge_daily_sources(
        config,
        {"input_file": "reddit.jsonl"},
        {"input_file": "news.jsonl"},
    )
    assert result["source_rows"] == {"reddit": 1, "web_news": 1}
    assert result["unique_rows"] == 2


def test_group_daily_batch_builds_one_request_per_selected_group(tmp_path) -> None:
    source = tmp_path / "events"
    rows = [
        {
            "source_name": "reddit", "community": "Economics", "title": None,
            "text": "The economy is getting worse.", "text_clean": "The economy is getting worse.",
            "metadata": {}, "quality_status": "accept", "source_type": "comment",
        },
        {
            "source_name": "web_news", "community": None, "title": "Markets recover",
            "text": "Markets recover", "text_clean": "Markets recover",
            "metadata": {"google_news_topic_group": "economy"},
            "quality_status": "accept", "source_type": "news",
        },
    ]
    pq.write_to_dataset(pa.Table.from_pylist(rows), root_path=source, partition_cols=["source_type"])
    config = tmp_path / "groups.yaml"
    config.write_text(
        "groups:\n  economy:\n    label: economy-and-society\n    subreddits: [Economics]\n",
        encoding="utf-8",
    )
    result = build_group_daily_batch(
        input_path=source,
        config_path=config,
        selected_groups=["economy"],
        analysis_date=date(2012, 2, 1),
        request_path=tmp_path / "requests.jsonl",
        manifest_path=tmp_path / "manifest.jsonl",
        report_path=tmp_path / "report.json",
    )
    request = json.loads((tmp_path / "requests.jsonl").read_text())
    manifest = json.loads((tmp_path / "manifest.jsonl").read_text())
    assert result["request_rows"] == 1
    assert result["groups"]["economy"] == {"reddit": 1, "web_news": 1}
    assert "polarization_score" in request["body"]["text"]["format"]["schema"]["properties"]
    assert "positive_tones" in request["body"]["text"]["format"]["schema"]["properties"]
    assert "negative_tones" in request["body"]["text"]["format"]["schema"]["properties"]
    assert request["body"]["max_output_tokens"] == 900
    assert "exactly 50% weight for Reddit and 50% weight for web news" in request["body"]["instructions"]
    assert '"source_weight":{"reddit":0.5,"web_news":0.5}' in request["body"]["input"]
    assert '"per_record_weight":{"reddit":0.5,"web_news":0.5}' in request["body"]["input"]
    assert "[REDDIT_COMMENTS weight=0.5]" in request["body"]["input"]
    assert "[WEB_NEWS_HEADLINES weight=0.5]" in request["body"]["input"]
    assert request["body"]["input"].index("The economy is getting worse.") < request["body"]["input"].index("Markets recover")
    assert manifest["schema_version"] == 3
    assert manifest["prompt_version"].endswith("source-balanced")
    assert manifest["source_weights"] == {"reddit": 0.5, "web_news": 0.5}
    assert result["weighting_policy"] == "equal_source_weight_when_both_present"
    assert manifest["validation_result"] == "preflight_passed"
    assert manifest["submitted_at"].endswith("Z")
    assert manifest["period"] == "2012-02-01"


def test_prepare_date_range_configs_creates_isolated_daily_runs(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "analysis-groups.yaml").write_text(
        "groups:\n"
        "  economy:\n"
        "    label: economy-and-society\n"
        "    subreddits: [Economics]\n",
        encoding="utf-8",
    )
    configs = prepare_date_range_configs(
        project_root=tmp_path,
        params={
            "start_date": "2012-02-02",
            "end_date": "2012-02-04",
            "limit": 0,
            "selected_groups": ["economy"],
            "analysis_groups_config": "config/analysis-groups.yaml",
            "reddit_archive_root": "data/raw/reddit-archive/data",
            "reddit_source_mode": "auto",
            "web_news_root": "data/raw/google-news",
            "web_news_source_mode": "auto",
            "output_root": "data/airflow-output/unified",
            "output_format": "parquet",
            "partitions": 2,
            "spark_master": "local[2]",
        },
        airflow_run_id="manual__range-test",
    )

    assert [item["collection"]["start_date"] for item in configs] == [
        "2012-02-02",
        "2012-02-03",
        "2012-02-04",
    ]
    assert len({item["run_directory"] for item in configs}) == 3
    assert all(item["range_start_date"] == "2012-02-02" for item in configs)
    assert all(item["range_end_date"] == "2012-02-04" for item in configs)


def test_only_one_airflow_dag_is_active() -> None:
    patterns = set(
        line.strip()
        for line in open("dags/.airflowignore", encoding="utf-8")
        if line.strip()
    )
    assert patterns == {
        "gdelt_daily_spark_batch.py",
        "reddit_daily_spark_batch.py",
        "reddit_spark_llm_pipeline.py",
        "llm_batch_pipeline.py",
        "openai_batch_slack_notification.py",
    }
