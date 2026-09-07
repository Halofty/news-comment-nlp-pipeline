from __future__ import annotations

from serving.presentation import format_analysis_rows, format_tones


def test_format_tones_renders_share_and_drivers() -> None:
    value = [
        {
            "tone": "hope",
            "share": 0.625,
            "drivers": ["job growth", "economic recovery"],
        },
        {"tone": "trust", "share": 0.375, "drivers": ["public institutions"]},
    ]

    assert format_tones(value) == (
        "hope 62% (job growth; economic recovery) · "
        "trust 38% (public institutions)"
    )


def test_format_analysis_rows_handles_legacy_empty_tones() -> None:
    rows = [
        {
            "event_id": "period:economy:2012-01-01",
            "positive_tones": None,
            "negative_tones": [],
        }
    ]

    result = format_analysis_rows(rows)

    assert result[0]["positive_tones"] == "-"
    assert result[0]["negative_tones"] == "-"
    assert rows[0]["positive_tones"] is None
