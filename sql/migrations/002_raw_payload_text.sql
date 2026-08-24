ALTER TABLE raw_text_events
    ALTER COLUMN raw_payload TYPE text USING raw_payload::text;
