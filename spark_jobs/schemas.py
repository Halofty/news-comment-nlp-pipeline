from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

TEXT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("collected_at", StringType(), True),
        StructField("language", StringType(), True),
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("url", StringType(), True),
        StructField("community", StringType(), True),
        StructField("engagement", LongType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField(
            "metadata", MapType(StringType(), StringType(), True), True
        ),
    ]
)

RAW_EVENT_SCHEMA = StructType(
    [*TEXT_EVENT_SCHEMA.fields, StructField("_corrupt_record", StringType(), True)]
)

TEXT_QUALITY_SCHEMA = StructType(
    [
        StructField("quality_policy_version", IntegerType(), False),
        StructField("text_clean", StringType(), False),
        StructField("character_count", LongType(), False),
        StructField("utf8_byte_count", LongType(), False),
        StructField("control_character_count", LongType(), False),
        StructField("zero_width_count", LongType(), False),
        StructField("max_combining_mark_run", IntegerType(), False),
        StructField("url_count", IntegerType(), False),
        StructField("url_ratio", DoubleType(), False),
        StructField("repetition_ratio", DoubleType(), False),
        StructField("quality_status", StringType(), False),
        StructField("quality_flags", ArrayType(StringType(), False), False),
        StructField("exclusion_reason", StringType(), True),
        StructField("was_normalized", BooleanType(), False),
        StructField("was_truncated", BooleanType(), False),
    ]
)
