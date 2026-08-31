from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.subreddits import load_subreddit_allowlist
from spark_jobs.process_sample import create_spark_session
from spark_jobs.schemas import TEXT_EVENT_SCHEMA


class InjectedProcessingFailure(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def run(
    *,
    reddit_input: Path,
    google_news_glob: str,
    output_root: Path,
    subreddit_file: Path,
    report_path: Path,
    inject_failure_before_write: bool,
) -> dict[str, Any]:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    started = time.perf_counter()
    report: dict[str, Any] = {
        "started_at": utc_now(),
        "status": "running",
        "reddit_input": str(reddit_input),
        "google_news_input": google_news_glob,
        "output_root": str(output_root),
        "failure_injected": inject_failure_before_write,
    }
    spark = None
    combined = None
    try:
        os.environ.setdefault(
            "PYSPARK_SUBMIT_ARGS", "--driver-memory 8g pyspark-shell"
        )
        spark = create_spark_session(
            master="local[4]", app_name="january-processing-failure-experiment"
        )
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.sql.shuffle.partitions", "64")

        reddit_raw = spark.read.parquet(str(reddit_input.resolve()))
        google_raw = spark.read.schema(TEXT_EVENT_SCHEMA).json(google_news_glob)
        report["reddit_input_rows"] = reddit_raw.count()
        report["google_news_input_rows"] = google_raw.count()

        names = sorted(
            name.casefold() for name in load_subreddit_allowlist(subreddit_file)
        )
        collected_at = report["started_at"]
        reddit_events = (
            reddit_raw.filter(F.lower("subreddit").isin(names))
            .filter(
                F.col("created_utc").isNotNull()
                & F.col("id").isNotNull()
                & F.col("body").isNotNull()
                & (F.length(F.trim("body")) > 0)
                & (~F.col("body").isin("[deleted]", "[removed]"))
            )
            .select(
                F.sha2(F.concat(F.lit("reddit:"), F.col("id")), 256).alias(
                    "event_id"
                ),
                F.lit("comment").alias("source_type"),
                F.lit("reddit").alias("source_name"),
                F.from_unixtime(F.col("created_utc").cast("long"))
                .cast("timestamp")
                .alias("event_timestamp"),
                F.to_timestamp(F.lit(collected_at)).alias("collected_timestamp"),
                F.lit("unknown").alias("language"),
                F.lit(None).cast("string").alias("title"),
                F.col("body").alias("text"),
                F.lit(None).cast("string").alias("url"),
                F.col("subreddit").alias("community"),
                F.col("score").cast("long").alias("engagement"),
                F.lit(1).alias("schema_version"),
                F.to_json(
                    F.struct(
                        F.col("link_id").alias("link_id"),
                        F.col("controversiality").alias("controversiality"),
                    )
                ).alias("metadata_json"),
            )
        )
        google_events = google_raw.select(
            "event_id",
            "source_type",
            "source_name",
            F.to_timestamp("event_time").alias("event_timestamp"),
            F.to_timestamp("collected_at").alias("collected_timestamp"),
            "language",
            "title",
            "text",
            "url",
            "community",
            F.col("engagement").cast("long").alias("engagement"),
            "schema_version",
            F.to_json("metadata").alias("metadata_json"),
        )
        reddit_processed_rows = reddit_events.count()
        google_processed_rows = google_events.count()
        report["processed_source_rows"] = {
            "reddit": reddit_processed_rows,
            "web_news": google_processed_rows,
        }
        report["processed_rows"] = reddit_processed_rows + google_processed_rows
        combined = (
            reddit_events.unionByName(google_events)
            .withColumn("year", F.date_format("event_timestamp", "yyyy"))
            .withColumn("month", F.date_format("event_timestamp", "MM"))
            .withColumn("day", F.date_format("event_timestamp", "dd"))
            .persist(StorageLevel.DISK_ONLY)
        )
        if inject_failure_before_write:
            raise InjectedProcessingFailure(
                "intentional failure after processing and before output write"
            )

        (
            combined.write.mode("overwrite")
            .option("compression", "zstd")
            .partitionBy("year", "month", "day", "source_name")
            .parquet(str(output_root.resolve()))
        )
        stored = spark.read.parquet(str(output_root.resolve())).cache()
        report["stored_rows"] = stored.count()
        report["distinct_event_ids"] = stored.select("event_id").distinct().count()
        report["duplicate_rows"] = (
            report["stored_rows"] - report["distinct_event_ids"]
        )
        report["stored_source_rows"] = {
            row["source_name"]: row["count"]
            for row in stored.groupBy("source_name").count().collect()
        }
        report["missing_rows"] = report["processed_rows"] - report["stored_rows"]
        report["output_parquet_files"] = len(list(output_root.rglob("*.parquet")))
        report["output_bytes"] = sum(
            path.stat().st_size for path in output_root.rglob("*.parquet")
        )
        stored.unpersist()
        report["status"] = "success"
        return report
    except Exception as error:
        report["status"] = "failed"
        report["error_type"] = type(error).__name__
        report["error"] = str(error)
        raise
    finally:
        if combined is not None:
            try:
                combined.unpersist()
            except Exception:
                pass
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
        report["finished_at"] = utc_now()
        report["duration_seconds"] = round(time.perf_counter() - started, 3)
        report["output_exists_after_run"] = output_root.exists()
        write_report(report_path, report)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process January Reddit and Google News with safe failure injection"
    )
    parser.add_argument("--reddit-input", type=Path, required=True)
    parser.add_argument("--google-news-glob", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--subreddit-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--inject-failure-before-write", action="store_true")
    args = parser.parse_args()
    result = run(
        reddit_input=args.reddit_input,
        google_news_glob=args.google_news_glob,
        output_root=args.output_root,
        subreddit_file=args.subreddit_file,
        report_path=args.report,
        inject_failure_before_write=args.inject_failure_before_write,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
