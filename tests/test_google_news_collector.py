from collectors.google_news import build_query, parse_rss
from datetime import date


RSS = b"""<?xml version="1.0"?><rss><channel><item>
<title>Climate policy changes - Example News</title>
<link>https://news.google.com/rss/articles/abc</link>
<pubDate>Tue, 03 Jan 2012 08:00:00 GMT</pubDate>
<source url="https://example.com">Example News</source>
</item></channel></rss>"""


def test_build_query_uses_one_day_exclusive_end() -> None:
    query = build_query(("climate", "energy"), date(2012, 1, 31))
    assert '"climate" OR "energy"' in query
    assert "after:2012-01-31 before:2012-02-01" in query


def test_parse_rss_maps_title_publisher_and_date() -> None:
    events = parse_rss(RSS, topic_group="environment", collected_at="now")
    assert len(events) == 1
    assert events[0]["title"] == "Climate policy changes"
    assert events[0]["event_time"] == "2012-01-03T00:00:00Z"
    assert events[0]["metadata"]["publisher"] == "Example News"
