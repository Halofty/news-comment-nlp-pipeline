from __future__ import annotations

import json

from llm_analysis.quality import apply_quality_gate, clean_label


def test_clean_label_normalizes_controls_and_smart_punctuation() -> None:
    cleaned, reasons = clean_label("China’s debt\u200b\u200b", field="keywords")

    assert cleaned == "China's debt"
    assert "unicode_or_whitespace_normalized" in reasons


def test_clean_label_rejects_non_english_meta_and_merged_values() -> None:
    assert clean_label("income inequalityถ్", field="keywords")[0] is None
    assert clean_label("keep short", field="keywords")[0] is None
    assert clean_label(
        "SOPA piracy abuse scandal subsidies debt globalization", field="keywords"
    )[0] is None
    assert clean_label("QE2A8B3A1A7B8B2A5B8B4A4B8B3A2", field="keywords")[0] is None


def test_quality_gate_preserves_rows_and_records_changes(tmp_path) -> None:
    source = tmp_path / "source.jsonl"
    target = tmp_path / "clean.jsonl"
    report_path = tmp_path / "report.json"
    source.write_text(
        json.dumps(
            {
                "event_id": "period:economy-society:2012-01-01",
                "sentiment": "mixed",
                "sentiment_score": -0.2,
                "topics": ["China’s debt\u200b", "keep short"],
                "keywords": ["market", "Market", "income inequalityถ్"],
                "summary": "A daily summary.\u200b",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    report = apply_quality_gate(
        input_path=source, output_path=target, report_path=report_path
    )
    row = json.loads(target.read_text(encoding="utf-8"))

    assert report["input_rows"] == report["output_rows"] == 1
    assert report["removed_labels"] == 3
    assert row["topics"] == ["China's debt"]
    assert row["keywords"] == ["market"]
    assert row["summary"] == "A daily summary."
    assert row["quality_gate"]["status"] == "modified"

