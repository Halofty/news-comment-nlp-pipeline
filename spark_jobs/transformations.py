from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame, functions as F

from core.text_quality import analyze_text_quality
from spark_jobs.schemas import TEXT_QUALITY_SCHEMA

EVENT_ID_REGEX = "^[a-f0-9]{64}$"
EXPECTED_EVENT_FIELDS = (
    "event_id",
    "source_type",
    "source_name",
    "event_time",
    "collected_at",
    "language",
    "title",
    "text",
    "url",
    "community",
    "engagement",
    "schema_version",
    "metadata",
)


def _quality_payload(text: str | None) -> dict[str, Any] | None:
    if text is None:
        return None
    return analyze_text_quality(text).to_dict()


def _error_when(condition: Column, code: str) -> Column:
    return F.when(condition, F.lit(code))


def add_contract_columns(events: DataFrame) -> DataFrame:
    with_timestamps = (
        events.withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn("collected_timestamp", F.to_timestamp("collected_at"))
        .withColumn("text_original", F.col("text"))
    )

    json_keys = F.coalesce(
        F.col("_json_keys"), F.array().cast("array<string>")
    )
    expected_fields = F.array(*(F.lit(name) for name in EXPECTED_EVENT_FIELDS))
    errors = F.array(
        _error_when(F.col("_corrupt_record").isNotNull(), "MALFORMED_JSON"),
        _error_when(
            F.size(F.array_except(expected_fields, json_keys)) > 0,
            "MISSING_FIELDS",
        ),
        _error_when(
            F.size(F.array_except(json_keys, expected_fields)) > 0,
            "UNEXPECTED_FIELDS",
        ),
        _error_when(
            F.col("event_id").isNull()
            | ~F.col("event_id").rlike(EVENT_ID_REGEX),
            "INVALID_EVENT_ID",
        ),
        _error_when(
            ~F.coalesce(F.col("source_type").isin("news", "comment"), F.lit(False)),
            "INVALID_SOURCE_TYPE",
        ),
        _error_when(
            ~F.coalesce(F.col("source_name").isin("gdelt", "reddit"), F.lit(False)),
            "INVALID_SOURCE_NAME",
        ),
        _error_when(F.col("event_timestamp").isNull(), "INVALID_EVENT_TIME"),
        _error_when(F.col("collected_timestamp").isNull(), "INVALID_COLLECTED_AT"),
        _error_when(
            F.col("language").isNull()
            | (F.length(F.trim(F.col("language"))) == 0),
            "MISSING_LANGUAGE",
        ),
        _error_when(
            F.col("text").isNull() | (F.length(F.trim(F.col("text"))) == 0),
            "MISSING_TEXT",
        ),
        _error_when(
            F.col("schema_version").isNull() | (F.col("schema_version") != 1),
            "UNSUPPORTED_SCHEMA_VERSION",
        ),
        _error_when(F.col("metadata").isNull(), "MISSING_METADATA"),
    )
    contract_errors = F.filter(errors, lambda item: item.isNotNull())
    return with_timestamps.withColumn("contract_errors", contract_errors).withColumn(
        "contract_valid", F.size("contract_errors") == 0
    )


def add_quality_columns(events: DataFrame) -> DataFrame:
    quality_udf = F.udf(_quality_payload, TEXT_QUALITY_SCHEMA)
    with_quality = events.withColumn(
        "_quality",
        F.when(F.col("contract_valid"), quality_udf(F.col("text"))),
    )
    return with_quality.select("*", "_quality.*").drop("_quality")


def transform_events(events: DataFrame) -> DataFrame:
    return add_quality_columns(add_contract_columns(events))


def deduplicate_valid_events(events: DataFrame) -> tuple[DataFrame, DataFrame]:
    rejected = events.filter(~F.col("contract_valid"))
    valid_unique = (
        events.filter(F.col("contract_valid"))
        .dropDuplicates(["event_id"])
        .drop("_raw_json", "_json_keys", "_corrupt_record")
    )
    return valid_unique, rejected
