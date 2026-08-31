import csv
from pathlib import Path

from jobs.download_reddit_archive import read_catalog, validate_size


def test_read_catalog_selects_inclusive_month_range(tmp_path: Path) -> None:
    catalog = tmp_path / "months.csv"
    with catalog.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["month", "filename", "bytes"])
        writer.writeheader()
        writer.writerows(
            [
                {"month": "2012-01", "filename": "a", "bytes": "1"},
                {"month": "2012-02", "filename": "b", "bytes": "2"},
                {"month": "2012-03", "filename": "c", "bytes": "3"},
            ]
        )

    rows = read_catalog(catalog, start_month="2012-02", end_month="2012-03")

    assert [row["month"] for row in rows] == ["2012-02", "2012-03"]


def test_validate_size_requires_exact_size(tmp_path: Path) -> None:
    path = tmp_path / "source.parquet"
    path.write_bytes(b"abc")
    assert validate_size(path, 3)
    assert not validate_size(path, 2)
