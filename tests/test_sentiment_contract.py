from __future__ import annotations

import pytest
from jsonschema import Draft202012Validator

from llm_analysis.contract import (
    ANALYSIS_SCHEMA,
    derive_sentiment_fields,
    normalize_dominant_sentiment,
    normalize_tone_shares,
    validate_sentiment_semantics,
)


def _result() -> dict[str, object]:
    return {
        "dominant_sentiment": "negative",
        "sentiment_score": -0.42,
        "estimated_sentiment_distribution": {
            "positive": 0.14,
            "neutral": 0.26,
            "negative": 0.60,
        },
        "polarization": "high",
        "polarization_score": 0.78,
        "positive_tones": [
            {
                "tone": "hope",
                "share": 0.65,
                "drivers": ["possible recovery"],
            },
            {
                "tone": "trust",
                "share": 0.35,
                "drivers": ["public institutions"],
            },
        ],
        "negative_tones": [
            {
                "tone": "anxiety",
                "share": 0.60,
                "drivers": ["job security"],
            },
            {
                "tone": "frustration",
                "share": 0.40,
                "drivers": ["economic policy"],
            },
        ],
        "topics": ["economic policy"],
        "keywords": ["wages"],
        "summary": "Economic discussion is predominantly negative and divided.",
    }


def _model_output(result: dict[str, object]) -> dict[str, object]:
    """The current schema no longer asks the model for dominant_sentiment or
    sentiment_score; both are derived from estimated_sentiment_distribution."""
    return {
        key: value
        for key, value in result.items()
        if key not in ("dominant_sentiment", "sentiment_score")
    }


def test_polarization_contract_accepts_consistent_result() -> None:
    result = _result()
    model_output = _model_output(result)
    Draft202012Validator(ANALYSIS_SCHEMA).validate(model_output)
    derived = derive_sentiment_fields(model_output)
    assert derived["dominant_sentiment"] == "negative"
    assert derived["sentiment_score"] == round(0.14 - 0.60, 6)
    validate_sentiment_semantics(derived)


def test_polarization_contract_rejects_distribution_that_does_not_sum_to_one() -> None:
    result = _result()
    result["estimated_sentiment_distribution"] = {
        "positive": 0.2,
        "neutral": 0.2,
        "negative": 0.2,
    }
    with pytest.raises(ValueError, match="sum to 1.0"):
        validate_sentiment_semantics(result)


def test_polarization_contract_rejects_mismatched_dominant_label() -> None:
    result = _result()
    result["dominant_sentiment"] = "positive"
    with pytest.raises(ValueError, match="largest estimated share"):
        validate_sentiment_semantics(result)


def test_emotional_tone_contract_rejects_invalid_share_total() -> None:
    result = _result()
    result["negative_tones"][0]["share"] = 0.2
    with pytest.raises(ValueError, match="negative_tones shares"):
        validate_sentiment_semantics(result)


def test_emotional_tone_contract_rejects_duplicate_tones() -> None:
    result = _result()
    result["positive_tones"][1]["tone"] = "hope"
    with pytest.raises(ValueError, match="duplicate tones"):
        validate_sentiment_semantics(result)


def test_emotional_tone_contract_allows_empty_profile_for_zero_polarity() -> None:
    result = _result()
    result["estimated_sentiment_distribution"] = {
        "positive": 0.0,
        "neutral": 0.4,
        "negative": 0.6,
    }
    result["positive_tones"] = []
    model_output = _model_output(result)
    Draft202012Validator(ANALYSIS_SCHEMA).validate(model_output)
    derived = derive_sentiment_fields(model_output)
    validate_sentiment_semantics(derived)


def test_emotional_tone_contract_requires_profile_for_nonzero_polarity() -> None:
    result = _result()
    result["positive_tones"] = []
    with pytest.raises(ValueError, match="non-zero polarity"):
        validate_sentiment_semantics(result)


def test_emotional_tone_shares_normalize_small_model_drift() -> None:
    result = _result()
    result["positive_tones"][0]["share"] = 0.45
    result["positive_tones"][1]["share"] = 0.48
    result["negative_tones"][0]["share"] = 0.5
    result["negative_tones"][1]["share"] = 0.4

    normalized = normalize_tone_shares(result)

    assert sum(item["share"] for item in normalized["positive_tones"]) == 1.0
    assert sum(item["share"] for item in normalized["negative_tones"]) == 1.0
    validate_sentiment_semantics(normalized)


def test_emotional_tone_shares_normalize_omitted_minor_tones() -> None:
    result = _result()
    result["negative_tones"][0]["share"] = 0.1
    result["negative_tones"][1]["share"] = 0.1

    normalized = normalize_tone_shares(result)

    assert sum(item["share"] for item in normalized["negative_tones"]) == 1.0
    validate_sentiment_semantics(normalized)


def test_derive_sentiment_fields_picks_largest_share_as_dominant() -> None:
    model_output = _model_output(_result())
    model_output["estimated_sentiment_distribution"] = {
        "positive": 0.15,
        "neutral": 0.45,
        "negative": 0.40,
    }

    derived = derive_sentiment_fields(model_output)

    assert derived["dominant_sentiment"] == "neutral"
    assert derived["sentiment_score"] == round(0.15 - 0.40, 6)
    validate_sentiment_semantics(derived)


def test_derive_sentiment_fields_computes_score_as_positive_minus_negative() -> None:
    model_output = _model_output(_result())
    model_output["estimated_sentiment_distribution"] = {
        "positive": 0.7,
        "neutral": 0.2,
        "negative": 0.1,
    }

    derived = derive_sentiment_fields(model_output)

    assert derived["dominant_sentiment"] == "positive"
    assert derived["sentiment_score"] == 0.6


def test_derive_sentiment_fields_rejects_missing_distribution() -> None:
    model_output = _model_output(_result())
    del model_output["estimated_sentiment_distribution"]

    with pytest.raises(ValueError, match="estimated_sentiment_distribution"):
        derive_sentiment_fields(model_output)


def test_derive_sentiment_fields_does_not_mutate_input() -> None:
    model_output = _model_output(_result())

    derive_sentiment_fields(model_output)

    assert "dominant_sentiment" not in model_output
    assert "sentiment_score" not in model_output


def test_normalize_dominant_sentiment_corrects_mismatched_label() -> None:
    result = _result()
    result["dominant_sentiment"] = "negative"
    result["estimated_sentiment_distribution"] = {
        "positive": 0.15,
        "neutral": 0.45,
        "negative": 0.40,
    }

    normalized = normalize_dominant_sentiment(result)

    assert normalized["dominant_sentiment"] == "neutral"
    validate_sentiment_semantics(normalized)


def test_normalize_dominant_sentiment_leaves_consistent_result_unchanged() -> None:
    result = _result()

    normalized = normalize_dominant_sentiment(result)

    assert normalized == result


def test_emotional_tone_shares_reject_zero_or_excessive_total() -> None:
    result = _result()
    result["negative_tones"][0]["share"] = 0.0
    result["negative_tones"][1]["share"] = 0.0
    with pytest.raises(ValueError, match="outside normalization bounds"):
        normalize_tone_shares(result)

    result["negative_tones"][0]["share"] = 0.8
    result["negative_tones"][1]["share"] = 0.8
    with pytest.raises(ValueError, match="outside normalization bounds"):
        normalize_tone_shares(result)
