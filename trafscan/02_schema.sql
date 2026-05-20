-- =============================================================
-- TRAFSCAN - Solution A v3.2: Schema additions
-- Run AFTER 01_schema.sql (which handles base tables and v3.1 ENUMs).
-- Safe to re-run - all statements are idempotent.
-- =============================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- FIX S1: Add Q8 / Q9 anomaly_type ENUM values (missing from 01_schema.sql)
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'VOLUME_ZERO';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'VOLUME_DROP_CRITICAL';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'VOLUME_DROP_WARNING';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'VOLUME_SURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'DAILY_VOLUME_DROP';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;


-- ─────────────────────────────────────────────────────────────────────────────
-- FIX S2: Prediction tables (missing from 01_schema.sql entirely)
-- ─────────────────────────────────────────────────────────────────────────────

-- prediction_baseline: stores hourly/daily predictions per (operator x cdr_type)
CREATE TABLE IF NOT EXISTS public.prediction_baseline (
    prediction_id     UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    operator_id       VARCHAR(50)    NOT NULL REFERENCES public.operators(operator_id),
    cdr_type          VARCHAR(50)    NOT NULL,       -- varchar, not enum: future-proof
    slot_start        TIMESTAMP      NOT NULL,       -- start of the predicted slot
    slot_end          TIMESTAMP      NOT NULL,       -- slot_start + 1h for hourly; +1d for daily
    granularity       VARCHAR(10)    NOT NULL DEFAULT 'hourly',  -- 'hourly' or 'daily'
    predicted_files   DECIMAL(10,2)  NOT NULL,       -- expected file count
    predicted_records DECIMAL(18,2),                 -- expected record count
    predicted_bytes   DECIMAL(18,2),                 -- expected volume in bytes
    confidence_pct    DECIMAL(5,2),                  -- % variance from historical samples
    model_version     VARCHAR(20),
    generated_at      TIMESTAMP      NOT NULL DEFAULT NOW(),
    baseline_samples  INTEGER,                       -- how many historical points were used

    -- one prediction per (operator, type, slot, granularity) at a time
    CONSTRAINT uq_prediction UNIQUE (operator_id, cdr_type, slot_start, granularity)
);

CREATE INDEX IF NOT EXISTS idx_prediction_slot
    ON public.prediction_baseline (operator_id, cdr_type, slot_start, granularity);


-- prediction_actuals: links each prediction to the real observed volume
-- Populated by checker_realtime.py (Q8) and checker_daily.py (Q9)
CREATE TABLE IF NOT EXISTS public.prediction_actuals (
    actual_id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    prediction_id      UUID          REFERENCES public.prediction_baseline(prediction_id)
                                     ON DELETE SET NULL,
    operator_id        VARCHAR(50)   NOT NULL,
    cdr_type           VARCHAR(50)   NOT NULL,
    slot_start         TIMESTAMP     NOT NULL,
    granularity        VARCHAR(10)   NOT NULL DEFAULT 'hourly',
    actual_files       INTEGER       NOT NULL DEFAULT 0,
    actual_records     BIGINT,
    actual_bytes       BIGINT,
    delta_files_pct    DECIMAL(8,2),   -- (actual - predicted) / predicted * 100
    anomaly_triggered  BOOLEAN       NOT NULL DEFAULT FALSE,
    computed_at        TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_actuals_slot
    ON public.prediction_actuals (operator_id, cdr_type, slot_start);


-- ─────────────────────────────────────────────────────────────────────────────
-- Already in 01_schema.sql but repeated here for safety (idempotent)
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE public.cdr_registry
    ADD COLUMN IF NOT EXISTS is_pre_compressed BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS file_size_bytes   BIGINT;


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification (uncomment to run after migration)
-- ─────────────────────────────────────────────────────────────────────────────
-- SELECT enum_range(NULL::anomaly_type);     -- should include all 5 new values
-- \d prediction_baseline
-- \d prediction_actuals
-- SELECT column_name FROM information_schema.columns
--   WHERE table_name = 'cdr_registry' AND column_name IN ('is_pre_compressed','file_size_bytes');