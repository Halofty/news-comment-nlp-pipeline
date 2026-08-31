-- TextEvent v1에 웹 언론사 수집 이벤트를 저장할 수 있도록 허용한다.
ALTER TABLE raw_text_events
    DROP CONSTRAINT IF EXISTS raw_text_events_source_name_check;

ALTER TABLE raw_text_events
    ADD CONSTRAINT raw_text_events_source_name_check
    CHECK (source_name IN ('gdelt', 'reddit', 'web_news'));
