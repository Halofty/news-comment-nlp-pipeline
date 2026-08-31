from __future__ import annotations

import argparse
from pathlib import Path

from core.subreddits import load_subreddit_allowlist
from spark_jobs.process_sample import create_spark_session


OUTPUT_COLUMNS = [
    "id",
    "body",
    "created_utc",
    "subreddit",
    "score",
    "link_id",
    "controversiality",
]


def filter_and_write_daily(
    input_path: Path,
    output_root: Path,
    *,
    subreddits: set[str],
) -> int:
    """Filter one source Parquet and overwrite only its UTC daily partitions."""
    from pyspark.sql import functions as F

    spark = create_spark_session(
        master="local[*]", app_name="reddit-archive-daily-filter"
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    normalized = sorted(name.casefold() for name in subreddits)
    source = spark.read.parquet(str(input_path))
    missing = sorted(set(OUTPUT_COLUMNS) - set(source.columns))
    if missing:
        raise ValueError(f"source Parquet is missing columns: {', '.join(missing)}")

    selected = (
        source.filter(F.lower(F.col("subreddit")).isin(normalized))
        .filter(F.col("created_utc").isNotNull())
        .select(*OUTPUT_COLUMNS)
        .withColumn(
            "event_date",
            F.to_date(F.from_unixtime(F.col("created_utc").cast("long"))),
        )
        .withColumn("year", F.date_format("event_date", "yyyy"))
        .withColumn("month", F.date_format("event_date", "MM"))
        .withColumn("day", F.date_format("event_date", "dd"))
        .drop("event_date")
    )

    row_count = selected.count()
    (
        selected.write.mode("overwrite")
        .option("compression", "zstd")
        .partitionBy("year", "month", "day")
        .parquet(str(output_root))
    )
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter a Reddit monthly Parquet into UTC daily partitions"
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--subreddit-file", type=Path, required=True)
    args = parser.parse_args()

    count = filter_and_write_daily(
        args.input,
        args.output_root,
        subreddits=load_subreddit_allowlist(args.subreddit_file),
    )
    print(f"wrote {count} rows below {args.output_root}")


if __name__ == "__main__":
    main()
