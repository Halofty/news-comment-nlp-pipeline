from pathlib import Path

from jobs.january_processing_experiment import write_report


def test_write_report_records_utf8_json(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "report.json"
    write_report(output, {"status": "실패"})
    assert '"status": "실패"' in output.read_text(encoding="utf-8")
