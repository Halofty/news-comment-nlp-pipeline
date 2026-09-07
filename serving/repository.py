from __future__ import annotations

from typing import Any


SUMMARY_SQL = """
SELECT
    count(*)::bigint AS analysis_rows,
    count(*) FILTER (WHERE sentiment = 'positive')::bigint AS positive_rows,
    count(*) FILTER (WHERE sentiment = 'neutral')::bigint AS neutral_rows,
    count(*) FILTER (WHERE sentiment = 'negative')::bigint AS negative_rows,
    count(*) FILTER (WHERE sentiment = 'mixed')::bigint AS mixed_rows,
    coalesce(avg(sentiment_score), 0)::double precision AS average_sentiment_score,
    coalesce(avg(polarization_score), 0)::double precision AS average_polarization_score
FROM document_analyses AS analyses
WHERE {version_filter}
"""

SENTIMENT_SQL = """
SELECT sentiment, count(*)::bigint AS row_count
FROM document_analyses AS analyses
WHERE {version_filter}
GROUP BY sentiment
ORDER BY row_count DESC, sentiment
"""

POLARIZATION_SQL = """
SELECT polarization, count(*)::bigint AS row_count
FROM document_analyses AS analyses
WHERE {version_filter} AND polarization IS NOT NULL
GROUP BY polarization
ORDER BY row_count DESC, polarization
"""

TOPICS_SQL = """
SELECT topic, count(*)::bigint AS row_count
FROM document_analyses AS analyses
CROSS JOIN LATERAL unnest(topics) AS topic
WHERE {version_filter} AND btrim(topic) <> ''
GROUP BY topic
ORDER BY row_count DESC, topic
LIMIT %s
"""

BATCHES_SQL = """
SELECT
    llm_batch_id, status, model, requested_count, completed_count,
    failed_count, input_tokens, output_tokens, total_cost_usd,
    submitted_at, completed_at
FROM llm_batch_jobs
WHERE {version_filter}
ORDER BY coalesce(completed_at, submitted_at, created_at) DESC
LIMIT %s
"""

ANALYSES_SQL = """
SELECT
    analyses.event_id, analyses.sentiment, analyses.sentiment_score,
    analyses.sentiment_distribution, analyses.polarization,
    analyses.polarization_score, analyses.positive_tones,
    analyses.negative_tones,
    analyses.topics, analyses.keywords, analyses.summary, analyses.model,
    analyses.analyzed_at, requests.llm_batch_id
FROM document_analyses AS analyses
JOIN llm_batch_requests AS requests USING (custom_id)
WHERE {version_filter}
ORDER BY analyses.analyzed_at DESC, analyses.event_id
LIMIT %s
"""


ANALYSIS_VERSIONS = ("v3", "v2", "v1", "all")
DEFAULT_ANALYSIS_ROW_LIMIT = 50
DEFAULT_BATCH_ROW_LIMIT = 50


def _version_filter(version: str, *, alias: str) -> tuple[str, tuple[str, ...]]:
    if version not in ANALYSIS_VERSIONS:
        raise ValueError(f"analysis_version must be one of {ANALYSIS_VERSIONS}")
    if version == "all":
        return "TRUE", ()
    # Prompt names use tokens such as ``daily-v1`` and ``group-daily-v3-*``.
    return f"{alias}.prompt_version ~ %s", (rf"(^|-){version}($|-)",)


def _serialize(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _rows(cursor: Any, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    return [
        {column: _serialize(value) for column, value in zip(columns, row)}
        for row in cursor.fetchall()
    ]


def load_dashboard_data(
    *,
    dsn: str,
    analysis_version: str = "v3",
    topic_limit: int = 15,
    row_limit: int = DEFAULT_ANALYSIS_ROW_LIMIT,
    batch_limit: int = DEFAULT_BATCH_ROW_LIMIT,
) -> dict[str, Any]:
    if not dsn.strip():
        raise ValueError("POSTGRES_DSN must not be empty")
    if topic_limit < 1 or row_limit < 1 or batch_limit < 1:
        raise ValueError("dashboard limits must be positive")
    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError("dashboard requires psycopg") from error

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            analysis_filter, analysis_params = _version_filter(
                analysis_version, alias="analyses"
            )
            batch_filter, batch_params = _version_filter(
                analysis_version, alias="llm_batch_jobs"
            )

            cursor.execute(
                SUMMARY_SQL.format(version_filter=analysis_filter), analysis_params
            )
            summary_row = cursor.fetchone()
            if summary_row is None:
                raise RuntimeError("summary query returned no row")
            summary_columns = (
                "analysis_rows",
                "positive_rows",
                "neutral_rows",
                "negative_rows",
                "mixed_rows",
                "average_sentiment_score",
                "average_polarization_score",
            )
            summary = {
                column: _serialize(value)
                for column, value in zip(summary_columns, summary_row)
            }

            cursor.execute(
                SENTIMENT_SQL.format(version_filter=analysis_filter), analysis_params
            )
            sentiments = _rows(cursor, ("sentiment", "row_count"))

            cursor.execute(
                POLARIZATION_SQL.format(version_filter=analysis_filter), analysis_params
            )
            polarizations = _rows(cursor, ("polarization", "row_count"))

            cursor.execute(
                TOPICS_SQL.format(version_filter=analysis_filter),
                (*analysis_params, topic_limit),
            )
            topics = _rows(cursor, ("topic", "row_count"))

            cursor.execute(
                BATCHES_SQL.format(version_filter=batch_filter),
                (*batch_params, batch_limit),
            )
            batches = _rows(
                cursor,
                (
                    "llm_batch_id",
                    "status",
                    "model",
                    "requested_count",
                    "completed_count",
                    "failed_count",
                    "input_tokens",
                    "output_tokens",
                    "total_cost_usd",
                    "submitted_at",
                    "completed_at",
                ),
            )

            cursor.execute(
                ANALYSES_SQL.format(version_filter=analysis_filter),
                (*analysis_params, row_limit),
            )
            analyses = _rows(
                cursor,
                (
                    "event_id",
                    "sentiment",
                    "sentiment_score",
                    "sentiment_distribution",
                    "polarization",
                    "polarization_score",
                    "positive_tones",
                    "negative_tones",
                    "topics",
                    "keywords",
                    "summary",
                    "model",
                    "analyzed_at",
                    "llm_batch_id",
                ),
            )
    return {
        "analysis_version": analysis_version,
        "summary": summary,
        "sentiments": sentiments,
        "polarizations": polarizations,
        "topics": topics,
        "batches": batches,
        "analyses": analyses,
    }
