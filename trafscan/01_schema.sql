-- =============================================================
-- TRAFSCAN - Solution A: CDR Registry Schema
-- PostgreSQL 16
-- =============================================================

-- ─────────────────────────────────────────────────────────────
-- ENUM: processing_status
-- ─────────────────────────────────────────────────────────────
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

-- ─────────────────────────────────────────────────────────────
-- ENUM: anomaly_type
-- Extended to cover all 5 divergence dimensions:
--   minutes, voice, SMS, data, recharge
-- NOTE: FORFAITS not yet included — pending team confirmation
--   of table names (see TODO block below)
-- ─────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE anomaly_type AS ENUM (
        'CHECKSUM_MISMATCH',
        'MISSING_RECORDS',
        'MINUTES_DIVERGENCE',
        'VOICE_DIVERGENCE',
        'SMS_DIVERGENCE',
        'DATA_DIVERGENCE',
        'RECHARGE_DIVERGENCE',
        -- TODO FORFAITS: add 'FORFAIT_DIVERGENCE' once team confirms
        --   the exact table names (tables known/unknown forfaits in Trafscan DB)
        --   and whether totals are per-file or globally aggregated.
        'TIMEOUT',
        'FILE_MISSING'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ─────────────────────────────────────────────────────────────
-- ENUM: severity
-- ─────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE severity_level AS ENUM (
        'INFO',
        'WARNING',
        'CRITICAL'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ─────────────────────────────────────────────────────────────
-- ENUM: cdr_type
-- Matches folder structure: /opt/backup/{operator}/{cdr_type}/
-- ─────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE cdr_type AS ENUM (
        'in',    -- IN CDRs: recharge, voucher (text, AIR format)
        'msc',   -- MSC CDRs: voice (binary Ericsson)
        'pgw',   -- PGW/GGSN CDRs: data (binary IPDR)
        'cnn',   -- CNN/CCN CDRs (binary ASN.1/BER)
        'sdp',   -- SDP CDRs (ASN, ADJ)
        'air',   -- AIR CDRs (text)
        'occ',   -- OCC CDRs
        'other'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ─────────────────────────────────────────────────────────────
-- TABLE: operators
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS operators (
    operator_id   VARCHAR(50)  PRIMARY KEY,
    operator_name VARCHAR(255) NOT NULL,
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Known operators
INSERT INTO operators (operator_id, operator_name) VALUES
    ('orange', 'Orange'),
    ('atel',   'Airtel'),
    ('moov',   'Moov')
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────────────────────────
-- TABLE: cdr_registry  (source of truth)
-- ─────────────────────────────────────────────────────────────
-- Each row = one CDR file tracked end-to-end through the pipeline.
-- Business value columns (_db / _es pairs) are populated by Java
-- hooks after processing and ES indexing respectively.
-- Binary CDR types (msc, pgw, cnn): record counts are raw newline
-- counts and are NOT meaningful for comparison — excluded from checks.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cdr_registry (

    -- ── Identity ──────────────────────────────────────────────
    file_id                  UUID              PRIMARY KEY DEFAULT gen_random_uuid(),
    file_name                VARCHAR(255)      NOT NULL UNIQUE,
    operator_id              VARCHAR(50)       NOT NULL REFERENCES operators(operator_id),

    -- Source classification (from folder path, not filename)
    cdr_type                 cdr_type          NOT NULL DEFAULT 'other',

    -- ── Timestamps ────────────────────────────────────────────
    received_at              TIMESTAMP         NOT NULL DEFAULT NOW(),
    -- processing_started_at and processing_ended_at are set by Java hooks.
    -- IMPORTANT: These are ONLY populated once the Trafscan team
    -- integrates the hook templates provided in README.md.
    -- Until then they remain NULL. Do NOT use them for processing-time
    -- alerts until the team confirms integration.
    processing_started_at    TIMESTAMP,
    processing_ended_at      TIMESTAMP,
    archived_at              TIMESTAMP,

    -- ── Checksums (3 stages — invariant I2) ───────────────────
    checksum_raw             CHAR(64)          NOT NULL,  -- SHA-256 on DCS reception
    checksum_compressed      CHAR(64),                   -- SHA-256 after gzip
    checksum_transferred     CHAR(64),                   -- SHA-256 verified at regulator

    -- ── Record counts (invariant I3) ──────────────────────────
    record_count_expected    BIGINT            NOT NULL,  -- Counted by Python at reception
    record_count_processed   BIGINT,                     -- Parsed by Java (set via Hook 2)

    -- ── Minutes: Java DB vs ElasticSearch ─────────────────────
    -- Populated by Java Hook 2 (DB) and Hook 4 (ES)
    total_minutes_db         DECIMAL(18, 3),
    total_minutes_es         DECIMAL(18, 3),

    -- ── Voice traffic: Java DB vs ElasticSearch ───────────────
    -- Source in Trafscan DB: AbonneTable.voice_counted_balance /
    --   voice_real_balance aggregated at file level by Java
    -- Source in ES: equivalent voice fields indexed per file
    -- Populated by Java Hook 2 (DB) and Hook 4 (ES) — requires
    -- Trafscan team to expose these aggregates in hooks
    voice_db                 DECIMAL(18, 3),
    voice_es                 DECIMAL(18, 3),

    -- ── SMS traffic: Java DB vs ElasticSearch ─────────────────
    -- Source in Trafscan DB: AbonneTable.sms_counted_balance /
    --   sms_real_balance aggregated at file level by Java
    sms_db                   DECIMAL(18, 3),
    sms_es                   DECIMAL(18, 3),

    -- ── Data traffic: Java DB vs ElasticSearch ────────────────
    -- Source in Trafscan DB: AbonneTable.data_counted_balance /
    --   data_real_balance aggregated at file level by Java
    data_db                  DECIMAL(18, 3),
    data_es                  DECIMAL(18, 3),

    -- ── Recharge: Java DB vs ElasticSearch ────────────────────
    -- Source in Trafscan DB: AbonneTable.recharge_balance /
    --   recharge_count aggregated at file level by Java
    -- Applies mainly to cdr_type='in' files (recharge CDRs)
    recharge_db              DECIMAL(18, 3),
    recharge_es              DECIMAL(18, 3),

    -- ── Forfaits ──────────────────────────────────────────────
    -- TODO FORFAITS: Forfait reconciliation is NOT per-file.
    -- There are two separate tables for known and unknown forfaits. Their exact names and field structure
    -- are pending confirmation. Once received:
    --   1. Add forfait_db / forfait_es columns here, OR
    --   2. Implement as a separate reconciliation query against
    --      those tables rather than per-file columns — depending
    --      on how Trafscan aggregates forfait data.

    -- ── Pipeline state machine ────────────────────────────────
    processing_status        processing_status NOT NULL DEFAULT 'PENDING',

    -- ── Archive ───────────────────────────────────────────────
    archive_path             VARCHAR(512),

    -- ── Free-form notes / errors ──────────────────────────────
    notes                    TEXT,

    -- ── Audit ─────────────────────────────────────────────────
    updated_at               TIMESTAMP         NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────
-- TRIGGER: auto-update updated_at
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_cdr_registry_updated_at ON cdr_registry;
CREATE TRIGGER trg_cdr_registry_updated_at
BEFORE UPDATE ON cdr_registry
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ─────────────────────────────────────────────────────────────
-- TABLE: anomaly_log
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_log (
    anomaly_id        UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id           UUID           REFERENCES cdr_registry(file_id) ON DELETE SET NULL,
    anomaly_type      anomaly_type   NOT NULL,
    delta_value       DECIMAL(18, 4),
    threshold_value   DECIMAL(18, 4),
    severity          severity_level NOT NULL,
    detected_at       TIMESTAMP      NOT NULL DEFAULT NOW(),
    resolved          BOOLEAN        NOT NULL DEFAULT FALSE,
    resolution_notes  TEXT
);

-- ─────────────────────────────────────────────────────────────
-- INDEXES
-- ─────────────────────────────────────────────────────────────

-- Primary batch filter: files by status
CREATE INDEX IF NOT EXISTS idx_registry_status
    ON cdr_registry (processing_status);

-- Operator + date range reports
CREATE INDEX IF NOT EXISTS idx_registry_operator_date
    ON cdr_registry (operator_id, received_at);

CREATE INDEX IF NOT EXISTS idx_registry_operator_type
    ON cdr_registry (operator_id, cdr_type);

CREATE INDEX IF NOT EXISTS idx_registry_cdr_type
    ON cdr_registry (cdr_type);

-- Date-only lookups (stuck-file detection, batch window)
CREATE INDEX IF NOT EXISTS idx_registry_received_at
    ON cdr_registry (received_at);

-- Fast name lookup (UNIQUE already creates a btree, this is explicit)
CREATE INDEX IF NOT EXISTS idx_registry_filename
    ON cdr_registry (file_name);

-- Anomaly JOIN + time filter
CREATE INDEX IF NOT EXISTS idx_anomaly_file
    ON anomaly_log (file_id, detected_at);

-- Open critical anomalies
CREATE INDEX IF NOT EXISTS idx_anomaly_severity
    ON anomaly_log (severity, resolved);

-- ─────────────────────────────────────────────────────────────
-- BIG DATA PARTITIONING
-- For volumes > 5000 files/day, partition cdr_registry by month.
-- Apply once daily file counts exceed PostgreSQL planner thresholds.
-- Migration path (run once when ready):
--
--   CREATE TABLE cdr_registry_2025_04
--       PARTITION OF cdr_registry
--       FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
--
-- Alternatively, use pg_partman extension for automated monthly
-- partition creation and retention management.
-- ─────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────
-- MIGRATION: Add new columns to existing installation
-- Run this block if upgrading from schema without traffic dimensions.
-- Safe to run multiple times (ADD COLUMN IF NOT EXISTS).
-- ─────────────────────────────────────────────────────────────
DO $$
BEGIN
    -- Voice
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='cdr_registry' AND column_name='voice_db') THEN
        ALTER TABLE cdr_registry ADD COLUMN voice_db DECIMAL(18, 3);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='cdr_registry' AND column_name='voice_es') THEN
        ALTER TABLE cdr_registry ADD COLUMN voice_es DECIMAL(18, 3);
    END IF;
    -- SMS
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='cdr_registry' AND column_name='sms_db') THEN
        ALTER TABLE cdr_registry ADD COLUMN sms_db DECIMAL(18, 3);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='cdr_registry' AND column_name='sms_es') THEN
        ALTER TABLE cdr_registry ADD COLUMN sms_es DECIMAL(18, 3);
    END IF;
    -- Data
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='cdr_registry' AND column_name='data_db') THEN
        ALTER TABLE cdr_registry ADD COLUMN data_db DECIMAL(18, 3);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='cdr_registry' AND column_name='data_es') THEN
        ALTER TABLE cdr_registry ADD COLUMN data_es DECIMAL(18, 3);
    END IF;
    -- Recharge
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='cdr_registry' AND column_name='recharge_db') THEN
        ALTER TABLE cdr_registry ADD COLUMN recharge_db DECIMAL(18, 3);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='cdr_registry' AND column_name='recharge_es') THEN
        ALTER TABLE cdr_registry ADD COLUMN recharge_es DECIMAL(18, 3);
    END IF;
END $$;


-- =============================================================
-- VERIFICATION QUERIES (run after migration to confirm)
-- =============================================================

-- Q0: Basic sanity
-- SELECT COUNT(*) FROM cdr_registry;   -- expect 0 on fresh install
-- SELECT COUNT(*) FROM operators;      -- expect 3
-- \d cdr_registry                      -- confirm all columns present

-- Q1: Stuck files (> 2h)
/*
SELECT file_name, operator_id, cdr_type, received_at,
       EXTRACT(EPOCH FROM (NOW() - received_at)) / 60 AS minutes_waiting,
       processing_status
FROM cdr_registry
WHERE processing_status IN ('PENDING', 'PROCESSING')
  AND received_at < NOW() - INTERVAL '2 hours'
ORDER BY received_at ASC;
*/

-- Q2: Checksum mismatch
/*
SELECT file_name, operator_id, cdr_type, checksum_raw,
       checksum_compressed, checksum_transferred,
       CASE
           WHEN checksum_compressed IS NOT NULL
                AND checksum_compressed != checksum_transferred THEN 'ALTERED_TRANSFER'
           WHEN checksum_compressed IS NULL
                AND checksum_raw != checksum_transferred        THEN 'ALTERED_TRANSFER'
           ELSE 'OK'
       END AS integrity_status
FROM cdr_registry
WHERE checksum_transferred IS NOT NULL
  AND (
      (checksum_compressed IS NOT NULL AND checksum_compressed != checksum_transferred)
   OR (checksum_compressed IS NULL     AND checksum_raw        != checksum_transferred)
  )
ORDER BY received_at DESC;
*/

-- Q3a: Minutes divergence DB vs ES (text CDRs only)
/*
SELECT file_name, operator_id, cdr_type,
       total_minutes_db, total_minutes_es,
       ABS(total_minutes_db - total_minutes_es) AS delta_minutes,
       ROUND(ABS(total_minutes_db - total_minutes_es)
             / NULLIF(total_minutes_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND total_minutes_db IS NOT NULL AND total_minutes_es IS NOT NULL
  AND ABS(total_minutes_db - total_minutes_es) / NULLIF(total_minutes_db, 0) > 0.01
  AND cdr_type NOT IN ('msc', 'pgw', 'cnn')
ORDER BY delta_pct DESC;
*/

-- Q3b: Voice divergence DB vs ES
/*
SELECT file_name, operator_id, cdr_type,
       voice_db, voice_es,
       ROUND(ABS(voice_db - voice_es) / NULLIF(voice_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND voice_db IS NOT NULL AND voice_es IS NOT NULL
  AND ABS(voice_db - voice_es) / NULLIF(voice_db, 0) > 0.01
ORDER BY delta_pct DESC;
*/

-- Q3c: SMS divergence DB vs ES
/*
SELECT file_name, operator_id, cdr_type,
       sms_db, sms_es,
       ROUND(ABS(sms_db - sms_es) / NULLIF(sms_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND sms_db IS NOT NULL AND sms_es IS NOT NULL
  AND ABS(sms_db - sms_es) / NULLIF(sms_db, 0) > 0.01
ORDER BY delta_pct DESC;
*/

-- Q3d: Data divergence DB vs ES
/*
SELECT file_name, operator_id, cdr_type,
       data_db, data_es,
       ROUND(ABS(data_db - data_es) / NULLIF(data_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND data_db IS NOT NULL AND data_es IS NOT NULL
  AND ABS(data_db - data_es) / NULLIF(data_db, 0) > 0.01
ORDER BY delta_pct DESC;
*/

-- Q3e: Recharge divergence DB vs ES (in CDRs only)
/*
SELECT file_name, operator_id,
       recharge_db, recharge_es,
       ROUND(ABS(recharge_db - recharge_es) / NULLIF(recharge_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND recharge_db IS NOT NULL AND recharge_es IS NOT NULL
  AND ABS(recharge_db - recharge_es) / NULLIF(recharge_db, 0) > 0.01
  AND cdr_type = 'in'
ORDER BY delta_pct DESC;
*/

-- Q4: Missing records (text CDRs only)
/*
SELECT file_name, operator_id, cdr_type,
       record_count_expected, record_count_processed,
       record_count_expected - record_count_processed AS missing_records,
       ROUND((record_count_expected - record_count_processed)
             / NULLIF(record_count_expected, 0) * 100, 2) AS missing_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND record_count_processed < record_count_expected
  AND notes NOT LIKE '%Binary CDR%'
ORDER BY missing_records DESC;
*/

-- Q5: Daily summary per operator and type
/*
SELECT operator_id, cdr_type, DATE(received_at) AS day,
       COUNT(*) AS total_files,
       SUM(CASE WHEN processing_status = 'DONE'      THEN 1 ELSE 0 END) AS done,
       SUM(CASE WHEN processing_status = 'PENDING'   THEN 1 ELSE 0 END) AS pending,
       SUM(CASE WHEN processing_status = 'MISMATCH'  THEN 1 ELSE 0 END) AS mismatch,
       SUM(CASE WHEN processing_status = 'ERROR'     THEN 1 ELSE 0 END) AS error
FROM cdr_registry
GROUP BY operator_id, cdr_type, DATE(received_at)
ORDER BY day DESC, operator_id, cdr_type;
*/
