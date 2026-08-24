from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from core.text_quality import (
    HARD_CHARACTER_LIMIT,
    HARD_UTF8_BYTE_LIMIT,
    QUALITY_FLAGS,
    SOFT_CHARACTER_LIMIT,
    analyze_text_quality,
    estimated_token_limit_exceeded,
)

FIXTURE_PATH = Path("analysis/quality/text-quality-fixtures.jsonl")
FIXTURE_SCHEMA_PATH = Path("analysis/quality/text-quality-fixtures.schema.json")


def load_fixtures() -> list[dict]:
    return [
        json.loads(line)
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def materialize_text(fixture: dict) -> str:
    specification = fixture["input"]
    if "text" in specification:
        return specification["text"]
    generator = specification["generator"]
    assert generator["kind"] == "repeat"
    return (
        generator.get("prefix", "")
        + generator["value"] * generator["count"]
        + generator.get("suffix", "")
    )


def test_quality_fixture_schema_is_valid() -> None:
    schema = json.loads(FIXTURE_SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)


def test_quality_fixtures_match_schema_and_use_known_flags() -> None:
    schema = json.loads(FIXTURE_SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    fixtures = load_fixtures()

    assert len(fixtures) >= 10
    assert len({fixture["case_id"] for fixture in fixtures}) == len(fixtures)
    for fixture in fixtures:
        validator.validate(fixture)
        expected_flags = set(fixture["expected"]["required_flags"])
        expected_flags.update(fixture["expected"]["forbidden_flags"])
        assert expected_flags.issubset(QUALITY_FLAGS)


def test_reference_quality_policy_matches_all_fixtures() -> None:
    for fixture in load_fixtures():
        result = analyze_text_quality(materialize_text(fixture))
        expected = fixture["expected"]
        flags = set(result.quality_flags)

        assert result.quality_status == expected["status"], fixture["case_id"]
        assert set(expected["required_flags"]).issubset(flags), fixture["case_id"]
        assert flags.isdisjoint(expected["forbidden_flags"]), fixture["case_id"]
        if "clean_text" in expected:
            assert result.text_clean == expected["clean_text"], fixture["case_id"]
        if "was_normalized" in expected:
            assert result.was_normalized is expected["was_normalized"], fixture[
                "case_id"
            ]
        if "exclusion_reason" in expected:
            assert result.exclusion_reason == expected["exclusion_reason"], fixture[
                "case_id"
            ]


def test_normal_multilingual_and_emoji_fixtures_are_accepted() -> None:
    normal_fixtures = [
        fixture for fixture in load_fixtures() if fixture["category"] == "normal"
    ]
    assert len(normal_fixtures) >= 5
    for fixture in normal_fixtures:
        result = analyze_text_quality(materialize_text(fixture))
        assert result.quality_status == "accept", fixture["case_id"]


def test_character_and_utf8_limits_have_distinct_boundaries() -> None:
    at_soft_limit = analyze_text_quality("가" * SOFT_CHARACTER_LIMIT)
    above_soft_limit = analyze_text_quality("가" * (SOFT_CHARACTER_LIMIT + 1))
    above_hard_character_limit = analyze_text_quality("a" * (HARD_CHARACTER_LIMIT + 1))
    above_hard_byte_limit = analyze_text_quality(
        "😀" * ((HARD_UTF8_BYTE_LIMIT // 4) + 1)
    )

    assert "EXCESSIVE_LENGTH" not in at_soft_limit.quality_flags
    assert "EXCESSIVE_LENGTH" in above_soft_limit.quality_flags
    assert above_hard_character_limit.quality_status == "quarantine"
    assert "EXCESSIVE_UTF8_BYTES" in above_hard_byte_limit.quality_flags


def test_quality_result_exposes_spark_output_columns() -> None:
    result = analyze_text_quality("normal text").to_dict()
    assert set(result) == {
        "quality_policy_version",
        "text_clean",
        "character_count",
        "utf8_byte_count",
        "control_character_count",
        "zero_width_count",
        "max_combining_mark_run",
        "url_count",
        "url_ratio",
        "repetition_ratio",
        "quality_status",
        "quality_flags",
        "exclusion_reason",
        "was_normalized",
        "was_truncated",
    }
    assert result["quality_policy_version"] == 1


def test_token_limit_is_checked_from_model_tokenizer_count() -> None:
    assert estimated_token_limit_exceeded(2_001, limit=2_000)
    assert not estimated_token_limit_exceeded(2_000, limit=2_000)
