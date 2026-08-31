import json

from jobs.profile_subreddit_storage import profile


def test_profile_merges_subreddit_case_and_measures_line_bytes(tmp_path) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    first_line = json.dumps({"community": "WorldNews"}) + "\n"
    second_line = json.dumps({"community": "worldnews"}) + "\n"
    first.write_text(first_line, encoding="utf-8")
    second.write_text(second_line, encoding="utf-8")

    rows, summary = profile(
        [("first", first), ("second", second)], period_days=10
    )

    assert len(rows) == 1
    assert rows[0]["subreddit_key"] == "worldnews"
    assert rows[0]["total_rows"] == 2
    assert rows[0]["total_jsonl_bytes"] == len(first_line) + len(second_line)
    assert rows[0]["core_analysis_candidate"] is True
    assert summary["distinct_subreddits"] == 1
