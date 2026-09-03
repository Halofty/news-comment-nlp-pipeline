from __future__ import annotations

import json
from decimal import Decimal

from llm_analysis.batch import build_batch_file, validate_batch_results


def _event(event_id: str, text: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "source_name": "reddit",
        "source_type": "comment",
        "text": text,
    }


def test_build_batch_file_uses_responses_and_metadata_only_manifest(tmp_path) -> None:
    source = tmp_path / "events.jsonl"
    source.write_text(
        "\n".join(
            json.dumps(row)
            for row in (
                _event("a" * 64, "A short synthetic comment."),
                _event("b" * 64, "Another synthetic comment."),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    request = tmp_path / "batch.jsonl"
    manifest = tmp_path / "manifest.jsonl"
    report = tmp_path / "report.json"

    result = build_batch_file(
        input_path=source,
        request_path=request,
        manifest_path=manifest,
        report_path=report,
        limit=2,
        max_output_tokens=100,
        daily_budget_usd=Decimal("0.001"),
    )

    requests = [json.loads(line) for line in request.read_text().splitlines()]
    manifests = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert result.request_rows == 2
    assert all(row["url"] == "/v1/responses" for row in requests)
    assert all(row["body"]["model"] == "gpt-5.6-luna" for row in requests)
    assert requests[0]["body"]["text"]["format"]["type"] == "json_schema"
    assert all("text" not in row and "title" not in row for row in manifests)
    assert json.loads(report.read_text())["budget_status"] == "ok"


def test_build_batch_file_skips_duplicate_empty_and_oversize(tmp_path) -> None:
    source = tmp_path / "events.jsonl"
    source.write_text(
        "\n".join(
            json.dumps(row)
            for row in (
                _event("same", "valid"),
                _event("same", "duplicate"),
                _event("empty", ""),
                _event("large", "x" * 20),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    result = build_batch_file(
        input_path=source,
        request_path=tmp_path / "batch.jsonl",
        manifest_path=tmp_path / "manifest.jsonl",
        report_path=tmp_path / "report.json",
        limit=10,
        max_text_bytes=10,
    )
    assert result.input_rows == 4
    assert result.request_rows == 1
    assert result.skipped_rows == 3


def test_budget_alert_blocks_when_estimate_reaches_limit(tmp_path) -> None:
    source = tmp_path / "events.jsonl"
    source.write_text(json.dumps(_event("event", "text")) + "\n")
    result = build_batch_file(
        input_path=source,
        request_path=tmp_path / "batch.jsonl",
        manifest_path=tmp_path / "manifest.jsonl",
        report_path=tmp_path / "report.json",
        max_output_tokens=300,
        daily_budget_usd=Decimal("0.00001"),
    )
    assert result.budget_status == "blocked"


def test_validate_batch_results_writes_only_schema_valid_rows(tmp_path) -> None:
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        json.dumps(
            {
                "custom_id": "request-1",
                "event_id": "event-1",
                "model": "gpt-5.6-luna",
                "prompt_version": "news-comment-analysis-v1",
                "schema_version": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    analysis = {
        "sentiment": "neutral",
        "sentiment_score": 0,
        "topics": ["technology"],
        "keywords": ["policy"],
        "summary": "A technology policy is discussed.",
    }
    results = tmp_path / "results.jsonl"
    results.write_text(
        json.dumps(
            {
                "custom_id": "request-1",
                "response": {
                    "status_code": 200,
                    "body": {
                        "model": "gpt-5.6-luna",
                        "output_text": json.dumps(analysis),
                        "usage": {"input_tokens": 20, "output_tokens": 10, "total_tokens": 30},
                    },
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "validated.jsonl"
    report = validate_batch_results(
        result_path=results, manifest_path=manifest, output_path=output
    )
    assert report == {
        "manifest_rows": 1,
        "result_rows": 1,
        "validated_rows": 1,
        "failed_rows": 0,
        "missing_rows": 0,
    }
    assert json.loads(output.read_text())["event_id"] == "event-1"
