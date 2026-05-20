-- =============================================================
-- TRAFSCAN - Solution A: CDR Registry Schema
-- PostgreSQL 16
-- =============================================================
--
-- Run once against the existing Trafscan PostgreSQL database.
-- Safe to run multiple times — all CREATE/ALTER statements are idempotent.
--
-- IMPORTANT: Run this in the SAME database as the Trafscan application.
--   Do NOT create a separate database. The Q7 check requires access to
--   public.daily_state_forfait and public.daily_state_unknown_forfait,
--   which already exist in the Trafscan DB.
--
-- Changes:
--   • anomaly_type ENUM: added PROCESSING_TIME_ANOMALY, FORFAIT_COUNT_DROP
--   • cdr_registry: added voice_db/es, sms_db/es, data_db/es, recharge_db/es
--   • cdr_registry: added updated_at (trigger-managed, used by Q_PT fallback)
--   • cdr_registry: added processing_started_at / processing_ended_at
--   • cdr_type ENUM renamed cdr_type → cdr_type_enum to avoid PostgreSQL
--     column/type name collision (the old schema had both named 'cdr_type')
--   • 'other' removed from cdr_type_enum (watcher always sets a known type)
--
-- MIGRATION NOTE for teams upgrading from v3.0:
--   If you already have a cdr_registry table with cdr_type='other' rows,
--   run this first before applying the rest of this script:
--
--     UPDATE cdr_registry SET cdr_type = 'in' WHERE cdr_type::text = 'other';
--
--   Then re-run this script. The ALTER TABLE blocks below are safe.
-- ============================================================================
 

ALTER TABLE public.cdr_registry
    ADD COLUMN IF NOT EXISTS is_pre_compressed BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT;


-- ─────────────────────────────────────────────────────────────────────────────
-- ENUM: processing_status
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE processing_status AS ENUM (
        'PENDING',
        'PROCESSING',
        'DONE',
        'ERROR',
        'MISMATCH'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- ENUM: cdr_type_enum
-- Renamed from 'cdr_type' (v3.0) to avoid collision with the column name.
-- 'other' removed — the watcher always maps to a known type from the folder path.
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE cdr_type_enum AS ENUM (
        'in',    -- IN CDRs: recharge, voucher (text, AIR format)
        'msc',   -- MSC CDRs: voice (binary Ericsson)
        'pgw',   -- PGW/GGSN CDRs: data (binary IPDR)
        'cnn',   -- CNN/CCN CDRs (binary ASN.1/BER)
        'sdp',   -- SDP CDRs (ASN, ADJ)
        'air',   -- AIR CDRs (text)
        'occ'    -- OCC CDRs
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
 
-- If the old 'cdr_type' ENUM exists (v3.0 schema), rename it.
-- Safe no-op if already renamed or doesn't exist.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'cdr_type'
        AND typtype = 'e'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'cdr_type_enum'
    ) THEN
        ALTER TYPE cdr_type RENAME TO cdr_type_enum;
        RAISE NOTICE 'Renamed ENUM cdr_type → cdr_type_enum';
    END IF;
END $$;
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- ENUM: anomaly_type
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE anomaly_type AS ENUM (
        -- Core checks (v1.0)
        'CHECKSUM_MISMATCH',
        'MISSING_RECORDS',
        'MINUTES_DIVERGENCE',
        'TIMEOUT',
        'FILE_MISSING',
        -- DB↔ES traffic dimensions (v3.0)
        'VOICE_DIVERGENCE',
        'SMS_DIVERGENCE',
        'DATA_DIVERGENCE',
        'RECHARGE_DIVERGENCE',
        -- New in v3.1
        'PROCESSING_TIME_ANOMALY',   -- Q_PT: processing duration outlier
        'FORFAIT_COUNT_DROP'         -- Q7:  daily forfait total collapse
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
 
-- Add new v3.1 values to an existing anomaly_type ENUM (safe/idempotent)
DO $$ BEGIN
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'VOICE_DIVERGENCE';
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'SMS_DIVERGENCE';
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'DATA_DIVERGENCE';
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'RECHARGE_DIVERGENCE';
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'PROCESSING_TIME_ANOMALY';
    ALTER TYPE anomaly_type ADD VALUE IF NOT EXISTS 'FORFAIT_COUNT_DROP';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- ENUM: severity_level
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE severity_level AS ENUM ('INFO', 'WARNING', 'CRITICAL');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: operators (reference)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.operators (
    operator_id   VARCHAR(50)  PRIMARY KEY,
    operator_name VARCHAR(255) NOT NULL,
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);
 
INSERT INTO public.operators (operator_id, operator_name) VALUES
    ('orange', 'Orange'),
    ('atel',   'Airtel'),
    ('moov',   'Moov')
ON CONFLICT DO NOTHING;
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: cdr_registry (source of truth — one row per CDR file)
-- ─────────────────────────────────────────────────────────────────────────────
-- Business columns (_db / _es pairs) are populated by Java hooks:
--   Hook 2 → all _db columns
--   Hook 4 → all _es columns
--
-- Binary CDR types (msc, pgw, cnn): record_count_expected is a raw newline
-- count of a binary file — meaningless. These types are excluded from
-- record-count and minutes divergence checks by the batch.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.cdr_registry (
 
    -- Identity
    file_id                  UUID              PRIMARY KEY DEFAULT gen_random_uuid(),
    file_name                VARCHAR(255)      NOT NULL UNIQUE,   -- basename only, never full path
    operator_id              VARCHAR(50)       NOT NULL REFERENCES public.operators(operator_id),
    cdr_type                 cdr_type_enum     NOT NULL,
 
    -- Timestamps
    received_at              TIMESTAMP         NOT NULL DEFAULT NOW(),
    -- processing_started_at and processing_ended_at are populated by Java
    -- Hooks 1 and 2/3 respectively. NULL until the Java team integrates them.
    -- Q_PT will use them automatically once they are populated.
    processing_started_at    TIMESTAMP,
    processing_ended_at      TIMESTAMP,
    archived_at              TIMESTAMP,
    -- updated_at is set by trigger on every UPDATE — used as Q_PT fallback
    -- before Java hooks are live.
    updated_at               TIMESTAMP         NOT NULL DEFAULT NOW(),
 
    -- Checksums — SHA-256 at three pipeline stages (invariant I2)
    checksum_raw             CHAR(64)          NOT NULL,   -- at DCS reception
    checksum_compressed      CHAR(64),                    -- after gzip
    checksum_transferred     CHAR(64),                    -- verified at regulator
 
    -- Record counts (invariant I3)
    record_count_expected    BIGINT            NOT NULL DEFAULT 0,
    record_count_processed   BIGINT,
 
    -- Q3a: Voice minutes — Java DB vs ElasticSearch
    total_minutes_db         DECIMAL(18,3),
    total_minutes_es         DECIMAL(18,3),
 
    -- Q3b: Voice traffic — AbonneTable.voice_counted_balance aggregate
    voice_db                 DECIMAL(18,3),
    voice_es                 DECIMAL(18,3),
 
    -- Q3c: SMS traffic — AbonneTable.sms_counted_balance aggregate
    sms_db                   DECIMAL(18,3),
    sms_es                   DECIMAL(18,3),
 
    -- Q3d: Data traffic — AbonneTable.data_counted_balance aggregate
    data_db                  DECIMAL(18,3),
    data_es                  DECIMAL(18,3),
 
    -- Q3e: Recharge — AbonneTable.recharge_balance aggregate
    --      Mainly applies to cdr_type='in' files. NULL for other types.
    recharge_db              DECIMAL(18,3),
    recharge_es              DECIMAL(18,3),
 
    -- Pipeline state
    processing_status        processing_status NOT NULL DEFAULT 'PENDING',
 
    -- Archive
    archive_path             VARCHAR(512),
 
    -- Free-form notes / error messages
    notes                    TEXT
);
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- TRIGGER: auto-update updated_at on every row change
-- Used by Q_PT fallback (updated_at - received_at as processing time proxy)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.cdr_registry_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;
 
DROP TRIGGER IF EXISTS trg_cdr_registry_updated_at ON public.cdr_registry;
CREATE TRIGGER trg_cdr_registry_updated_at
    BEFORE UPDATE ON public.cdr_registry
    FOR EACH ROW EXECUTE FUNCTION public.cdr_registry_set_updated_at();
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- MIGRATION: Add v3.0/v3.1 columns to an existing installation
-- All ADD COLUMN IF NOT EXISTS operations are idempotent — safe to re-run.
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE public.cdr_registry
    ADD COLUMN IF NOT EXISTS voice_db              DECIMAL(18,3),
    ADD COLUMN IF NOT EXISTS voice_es              DECIMAL(18,3),
    ADD COLUMN IF NOT EXISTS sms_db                DECIMAL(18,3),
    ADD COLUMN IF NOT EXISTS sms_es                DECIMAL(18,3),
    ADD COLUMN IF NOT EXISTS data_db               DECIMAL(18,3),
    ADD COLUMN IF NOT EXISTS data_es               DECIMAL(18,3),
    ADD COLUMN IF NOT EXISTS recharge_db           DECIMAL(18,3),
    ADD COLUMN IF NOT EXISTS recharge_es           DECIMAL(18,3),
    ADD COLUMN IF NOT EXISTS updated_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS processing_ended_at   TIMESTAMP,
    ADD COLUMN IF NOT EXISTS archive_path          VARCHAR(512);
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: anomaly_log (one row per detected anomaly)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.anomaly_log (
    anomaly_id       UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id          UUID           REFERENCES public.cdr_registry(file_id) ON DELETE SET NULL,
    anomaly_type     anomaly_type   NOT NULL,
    delta_value      DECIMAL(18,4),
    threshold_value  DECIMAL(18,4),
    severity         severity_level NOT NULL,
    detected_at      TIMESTAMP      NOT NULL DEFAULT NOW(),
    resolved         BOOLEAN        NOT NULL DEFAULT FALSE,
    resolution_notes TEXT
);
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- INDEXES
-- ─────────────────────────────────────────────────────────────────────────────
 
-- Primary batch filter: files by status
CREATE INDEX IF NOT EXISTS idx_registry_status
    ON public.cdr_registry (processing_status);
 
-- Operator + date range reports
CREATE INDEX IF NOT EXISTS idx_registry_operator_date
    ON public.cdr_registry (operator_id, received_at);
 
CREATE INDEX IF NOT EXISTS idx_registry_operator_type
    ON public.cdr_registry (operator_id, cdr_type);
 
-- Date-only lookups (stuck file detection, batch window)
CREATE INDEX IF NOT EXISTS idx_registry_received_at
    ON public.cdr_registry (received_at);
 
-- Fast name lookup
CREATE INDEX IF NOT EXISTS idx_registry_filename
    ON public.cdr_registry (file_name);
 
-- Q_PT: 7-day rolling median query
CREATE INDEX IF NOT EXISTS idx_registry_processing_time
    ON public.cdr_registry (cdr_type, processing_status, received_at, updated_at)
    WHERE processing_status = 'DONE';
 
-- anomaly JOIN + time filter
CREATE INDEX IF NOT EXISTS idx_anomaly_file
    ON public.anomaly_log (file_id, detected_at);
 
-- Open critical anomalies
CREATE INDEX IF NOT EXISTS idx_anomaly_severity
    ON public.anomaly_log (severity, resolved);
 
-- Anomaly type + date (for Q7 trend queries)
CREATE INDEX IF NOT EXISTS idx_anomaly_type_date
    ON public.anomaly_log (anomaly_type, detected_at);
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- Q7 PREREQUISITE CHECK
-- Verify that the forfait tables exist in this database.
-- If this raises an exception, you are running against the wrong database.
-- ─────────────────────────────────────────────────────────────────────────────
-- DO $$
-- BEGIN
--     IF NOT EXISTS (
--         SELECT 1 FROM information_schema.tables
--         WHERE table_schema = 'public' AND table_name = 'daily_state_forfait'
--     ) THEN
--         RAISE EXCEPTION
--             'Q7 SETUP ERROR: public.daily_state_forfait not found. '
--             'This schema must be applied to the SAME database as the Trafscan '
--             'application (same host, same dbname). Do not use a separate DB.';
--     END IF;
 
--     IF NOT EXISTS (
--         SELECT 1 FROM information_schema.tables
--         WHERE table_schema = 'public' AND table_name = 'daily_state_unknown_forfait'
--     ) THEN
--         RAISE EXCEPTION
--             'Q7 SETUP ERROR: public.daily_state_unknown_forfait not found. '
--             'Ensure the Trafscan application DB schema is up to date.';
--     END IF;
 
--     RAISE NOTICE 'Q7 prerequisite check OK: both forfait tables found.';
-- END $$;
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- PARTITIONING NOTE
-- For volumes > 5 000 files/day, convert cdr_registry to a partitioned table:
--
--   CREATE TABLE cdr_registry_2025_06
--       PARTITION OF cdr_registry
--       FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
--
-- Or use pg_partman for automated monthly partition management.
-- The batch already runs incrementally (today's data only for most checks)
-- so partitioning is not required at moderate volumes.
-- ─────────────────────────────────────────────────────────────────────────────
 
 
-- ─────────────────────────────────────────────────────────────────────────────
-- VERIFICATION QUERIES  (uncomment to run after migration)
-- ─────────────────────────────────────────────────────────────────────────────
 
-- Schema sanity check
-- SELECT COUNT(*) FROM cdr_registry;    -- expect 0 on fresh install
-- SELECT COUNT(*) FROM operators;       -- expect 3
-- \d cdr_registry                       -- confirm all columns present
 
-- Q3a: Minutes divergence DB vs ES (text CDRs only)
/*
SELECT file_name, operator_id, cdr_type,
       total_minutes_db, total_minutes_es,
       ROUND(ABS(total_minutes_db - total_minutes_es)
             / NULLIF(total_minutes_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND total_minutes_db IS NOT NULL AND total_minutes_es IS NOT NULL
  AND ABS(total_minutes_db - total_minutes_es) / NULLIF(total_minutes_db, 0) > 0.005
  AND cdr_type NOT IN ('msc', 'pgw', 'cnn')
ORDER BY delta_pct DESC;
*/
 
-- Q3b: Voice divergence
/*
SELECT file_name, operator_id, cdr_type,
       voice_db, voice_es,
       ROUND(ABS(voice_db - voice_es) / NULLIF(voice_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND voice_db IS NOT NULL AND voice_es IS NOT NULL
  AND ABS(voice_db - voice_es) / NULLIF(voice_db, 0) > 0.005
ORDER BY delta_pct DESC;
*/
 
-- Q3c: SMS divergence (same pattern, replace voice_ with sms_)
-- Q3d: Data divergence (replace voice_ with data_)
-- Q3e: Recharge divergence (replace voice_ with recharge_, add AND cdr_type='in')
 
-- Q_PT: Processing time outliers (7-day rolling median, fallback mode)
/*
WITH medians AS (
    SELECT cdr_type::text,
           PERCENTILE_CONT(0.5) WITHIN GROUP (
               ORDER BY EXTRACT(EPOCH FROM (updated_at - received_at))
           ) AS median_s
    FROM cdr_registry
    WHERE processing_status = 'DONE'
      AND received_at >= NOW() - INTERVAL '7 days'
    GROUP BY cdr_type
)
SELECT cr.file_name, cr.operator_id, cr.cdr_type::text,
       ROUND(EXTRACT(EPOCH FROM (cr.updated_at - cr.received_at))::numeric / 60, 1) AS duration_min,
       ROUND(m.median_s::numeric / 60, 1) AS median_min,
       ROUND(EXTRACT(EPOCH FROM (cr.updated_at - cr.received_at)) / m.median_s, 2) AS ratio
FROM cdr_registry cr
JOIN medians m ON cr.cdr_type::text = m.cdr_type
WHERE cr.processing_status = 'DONE'
  AND cr.received_at >= CURRENT_DATE
  AND EXTRACT(EPOCH FROM (cr.updated_at - cr.received_at)) > 3 * m.median_s
ORDER BY ratio DESC;
*/
 
-- Q7: Today's forfait totals
/*
SELECT
    CURRENT_DATE AS date,
    (SELECT COALESCE(SUM(count),0) FROM daily_state_forfait         WHERE date = CURRENT_DATE) AS known,
    (SELECT COALESCE(SUM(count),0) FROM daily_state_unknown_forfait WHERE date = CURRENT_DATE) AS unknown,
    (SELECT COALESCE(SUM(count),0) FROM daily_state_forfait         WHERE date = CURRENT_DATE)
  + (SELECT COALESCE(SUM(count),0) FROM daily_state_unknown_forfait WHERE date = CURRENT_DATE) AS total;
*/