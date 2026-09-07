ALTER TABLE document_analyses
    ADD COLUMN IF NOT EXISTS sentiment_distribution jsonb,
    ADD COLUMN IF NOT EXISTS polarization text,
    ADD COLUMN IF NOT EXISTS polarization_score double precision;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'document_analyses_polarization_check'
    ) THEN
        ALTER TABLE document_analyses ADD CONSTRAINT
            document_analyses_polarization_check
            CHECK (polarization IS NULL OR polarization IN ('low', 'medium', 'high'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'document_analyses_polarization_score_check'
    ) THEN
        ALTER TABLE document_analyses ADD CONSTRAINT
            document_analyses_polarization_score_check
            CHECK (polarization_score IS NULL OR polarization_score BETWEEN 0 AND 1);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS document_analyses_polarization_idx
    ON document_analyses (polarization, analyzed_at);

COMMENT ON COLUMN document_analyses.sentiment IS
    'v1: sentiment including mixed; v2+: dominant sentiment only';
