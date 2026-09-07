ALTER TABLE document_analyses
    ADD COLUMN IF NOT EXISTS positive_tones jsonb,
    ADD COLUMN IF NOT EXISTS negative_tones jsonb;

ALTER TABLE document_analyses
    DROP CONSTRAINT IF EXISTS document_analyses_positive_tones_array_check,
    ADD CONSTRAINT document_analyses_positive_tones_array_check
        CHECK (positive_tones IS NULL OR jsonb_typeof(positive_tones) = 'array'),
    DROP CONSTRAINT IF EXISTS document_analyses_negative_tones_array_check,
    ADD CONSTRAINT document_analyses_negative_tones_array_check
        CHECK (negative_tones IS NULL OR jsonb_typeof(negative_tones) = 'array');
