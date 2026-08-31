from pathlib import Path

import pytest

from spark_jobs.filter_reddit_archive import filter_and_write_daily
from spark_jobs.process_sample import create_spark_session


def test_filter_and_write_daily_uses_utc_date_partitions(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    input_path = tmp_path / "input.parquet"
    output_path = tmp_path / "selected"
    spark = create_spark_session(master="local[1]", app_name="fixture")
    spark.createDataFrame(
        [
            ("1", "keep", 1451606399, "news", 1, "t3_a", 0),
            ("2", "keep", 1451606400, "WorldNews", 2, "t3_b", 0),
            ("3", "skip", 1451606400, "gaming", 3, "t3_c", 0),
        ],
        [
            "id",
            "body",
            "created_utc",
            "subreddit",
            "score",
            "link_id",
            "controversiality",
        ],
    ).write.parquet(str(input_path))
    count = filter_and_write_daily(
        input_path,
        output_path,
        subreddits={"news", "worldnews"},
    )

    assert count == 2
    assert list((output_path / "year=2015/month=12/day=31").glob("*.parquet"))
    assert list((output_path / "year=2016/month=01/day=01").glob("*.parquet"))
    spark.stop()
