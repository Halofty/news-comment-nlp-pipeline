from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from jobs.postgres_recovery_experiment import sample_records


def test_sample_records_takes_equal_deterministic_source_samples(tmp_path: Path) -> None:
    rows = []
    for source in ("reddit", "web_news"):
        for index in range(2):
            rows.append(
                {
                    "event_id": f"{source}-{index}",
                    "source_type": "comment" if source == "reddit" else "news",
                    "source_name": source,
                    "event_timestamp": None,
                    "collected_timestamp": None,
                    "language": "en",
                    "title": None,
                    "text": "text",
                    "url": None,
                    "community": None,
                    "engagement": None,
                    "schema_version": 1,
                    "metadata_json": "{}",
                    "year": "2012",
                    "month": "01",
                    "day": "01",
                }
            )
    pq.write_to_dataset(
        pa.Table.from_pylist(rows),
        root_path=tmp_path,
        partition_cols=["year", "month", "day", "source_name"],
    )

    records = sample_records(tmp_path, per_source=1)

    assert [record["source_name"] for record in records] == ["reddit", "web_news"]
    assert [record["kafka_partition"] for record in records] == [0, 1]
