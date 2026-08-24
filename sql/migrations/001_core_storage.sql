CREATE TABLE IF NOT EXISTS raw_text_events (
    event_id text PRIMARY KEY,
    source_type text NOT NULL CHECK (source_type IN ('news', 'comment')),
    source_name text NOT NULL CHECK (source_name IN ('gdelt', 'reddit')),
    event_time timestamptz NOT NULL,
    collected_at timestamptz NOT NULL,
    language text NOT NULL,
    title text,
    text_original text NOT NULL,
    url text,
    community text,
    engagement bigint,
    schema_version smallint NOT NULL,
    metadata jsonb NOT NULL,
    raw_payload text NOT NULL,
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS raw_text_events_event_time_idx
    ON raw_text_events (event_time DESC);
CREATE INDEX IF NOT EXISTS raw_text_events_source_time_idx
    ON raw_text_events (source_name, event_time DESC);

CREATE TABLE IF NOT EXISTS text_documents_clean (
    event_id text PRIMARY KEY REFERENCES raw_text_events(event_id) ON DELETE CASCADE,
    text_clean text NOT NULL,
    quality_policy_version integer NOT NULL,
    quality_status text NOT NULL CHECK (quality_status IN ('accept', 'flag', 'quarantine', 'reject')),
    quality_flags text[] NOT NULL,
    exclusion_reason text,
    character_count bigint NOT NULL,
    utf8_byte_count bigint NOT NULL,
    control_character_count bigint NOT NULL,
    zero_width_count bigint NOT NULL,
    max_combining_mark_run integer NOT NULL,
    url_count integer NOT NULL,
    url_ratio double precision NOT NULL,
    repetition_ratio double precision NOT NULL,
    was_normalized boolean NOT NULL,
    was_truncated boolean NOT NULL,
    output_route text NOT NULL CHECK (output_route IN ('processed', 'quarantine', 'quality_rejected')),
    batch_id bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS text_documents_clean_route_idx
    ON text_documents_clean (output_route, batch_id);

CREATE TABLE IF NOT EXISTS contract_rejected_events (
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    kafka_timestamp timestamptz,
    kafka_key text,
    raw_event text,
    contract_errors text[] NOT NULL,
    batch_id bigint NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS stream_batch_commits (
    consumer_name text NOT NULL,
    batch_id bigint NOT NULL,
    input_rows bigint NOT NULL,
    route_counts jsonb NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, batch_id)
);
