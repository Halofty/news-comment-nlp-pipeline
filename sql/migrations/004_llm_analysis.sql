CREATE TABLE IF NOT EXISTS llm_batch_jobs (
    llm_batch_id text PRIMARY KEY,
    openai_batch_id text UNIQUE,
    input_file_id text,
    output_file_id text,
    model text NOT NULL,
    prompt_version text NOT NULL,
    status text NOT NULL CHECK (
        status IN (
            'prepared', 'budget_blocked', 'submitted', 'validating',
            'in_progress', 'finalizing', 'completed', 'failed',
            'expired', 'cancelled'
        )
    ),
    requested_count bigint NOT NULL DEFAULT 0 CHECK (requested_count >= 0),
    completed_count bigint NOT NULL DEFAULT 0 CHECK (completed_count >= 0),
    failed_count bigint NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
    input_tokens bigint CHECK (input_tokens >= 0),
    output_tokens bigint CHECK (output_tokens >= 0),
    total_cost_usd numeric(18, 8) CHECK (total_cost_usd >= 0),
    submitted_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS llm_batch_requests (
    custom_id text PRIMARY KEY,
    llm_batch_id text NOT NULL REFERENCES llm_batch_jobs(llm_batch_id),
    event_id text NOT NULL,
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
    status text NOT NULL CHECK (
        status IN ('pending', 'submitted', 'completed', 'retry', 'failed')
    ),
    validation_result text,
    error_code text,
    input_tokens bigint CHECK (input_tokens >= 0),
    output_tokens bigint CHECK (output_tokens >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_id, llm_batch_id, attempt)
);

CREATE TABLE IF NOT EXISTS document_analyses (
    event_id text NOT NULL,
    prompt_version text NOT NULL,
    model text NOT NULL,
    sentiment text NOT NULL CHECK (
        sentiment IN ('positive', 'neutral', 'negative', 'mixed')
    ),
    sentiment_score double precision NOT NULL CHECK (
        sentiment_score BETWEEN -1 AND 1
    ),
    topics text[] NOT NULL,
    keywords text[] NOT NULL,
    summary text NOT NULL,
    custom_id text NOT NULL REFERENCES llm_batch_requests(custom_id),
    analyzed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, prompt_version)
);

CREATE INDEX IF NOT EXISTS llm_batch_jobs_status_idx
    ON llm_batch_jobs (status, created_at);
CREATE INDEX IF NOT EXISTS llm_batch_requests_event_idx
    ON llm_batch_requests (event_id, status);
CREATE INDEX IF NOT EXISTS document_analyses_sentiment_idx
    ON document_analyses (sentiment, analyzed_at);
