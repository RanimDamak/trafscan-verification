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
-- ─────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE anomaly_type AS ENUM (
        'CHECKSUM_MISMATCH',
        'MISSING_RECORDS',
        'MINUTES_DIVERGENCE',
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
-- ─────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE cdr_type AS ENUM (
        'in',
        'msc',
        'pgw',
        'cnn',
        'sdp',
        'air',
        'occ',
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

-- Known operators — add if any is missing here
INSERT INTO operators (operator_id, operator_name) VALUES
    ('orange', 'Orange'),
    ('atel',   'Airtel'),
    ('moov',   'Moov')
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────────────────────────
-- TABLE: cdr_registry  (source of truth)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cdr_registry (
    -- Identity
    file_id                  UUID              PRIMARY KEY DEFAULT gen_random_uuid(),
    file_name                VARCHAR(255)      NOT NULL UNIQUE,
    operator_id              VARCHAR(50)       NOT NULL REFERENCES operators(operator_id),

    -- Source classification (from folder path, not filename)
    -- e.g. 'in', 'msc', 'pgw', 'cnn', 'sdp', 'air', 'occ'
    cdr_type                 cdr_type          NOT NULL DEFAULT 'other',

    -- Timestamps
    received_at              TIMESTAMP         NOT NULL DEFAULT NOW(),
    processing_started_at    TIMESTAMP,
    processing_ended_at      TIMESTAMP,
    archived_at              TIMESTAMP,

    -- Checksums (3 stages — invariant I2)
    checksum_raw             CHAR(64)          NOT NULL,
    checksum_compressed      CHAR(64),
    checksum_transferred     CHAR(64),

    -- Record counts (invariant I3)
    -- NOTE: for binary CDR types (msc, pgw, cnn) record_count_expected
    -- holds the raw newline count which is NOT meaningful for comparison.
    -- Check the notes column for 'Binary CDR' flag.
    record_count_expected    BIGINT            NOT NULL,
    record_count_processed   BIGINT,

    -- Business values (populated by Java hooks)
    total_minutes_db         DECIMAL(18, 3),
    total_minutes_es         DECIMAL(18, 3),

    -- State machine
    processing_status        processing_status NOT NULL DEFAULT 'PENDING',

    -- Archive
    archive_path             VARCHAR(512),

    -- Free-form notes / errors
    notes                    TEXT,

    -- Audit
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

-- Date-only lookups (stuck-file detection)
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


-- =============================================================
-- VERIFICATION QUERIES (run these after migration to confirm)
-- =============================================================

-- Q0: Basic sanity
-- SELECT COUNT(*) FROM cdr_registry;   -- expect 0
-- SELECT COUNT(*) FROM operators;      -- expect 3 (orange, atel, moov)
-- \d cdr_registry                      -- confirm all columns incl. cdr_type

-- Q1: Files stuck for more than 2 hours (PostgreSQL syntax)
/*
SELECT
    file_name,
    operator_id,
    cdr_type,
    received_at,
    EXTRACT(EPOCH FROM (NOW() - received_at)) / 60 AS minutes_waiting,
    processing_status
FROM cdr_registry
WHERE
    processing_status IN ('PENDING', 'PROCESSING')
    AND received_at < NOW() - INTERVAL '2 hours'
ORDER BY received_at ASC;
*/

-- Q2: Checksum mismatch detection
/*
SELECT
    file_name,
    operator_id,
    cdr_type,
    checksum_raw,
    checksum_compressed,
    checksum_transferred,
    CASE
        WHEN checksum_compressed IS NOT NULL
             AND checksum_raw = checksum_compressed      THEN 'WARN: raw=compressed (compression failed?)'
        WHEN checksum_compressed != checksum_transferred THEN 'ALTERED_TRANSFER'
        ELSE 'OK'
    END AS integrity_status
FROM cdr_registry
WHERE
    checksum_transferred IS NOT NULL
    AND (checksum_compressed IS NULL OR checksum_compressed != checksum_transferred)
ORDER BY received_at DESC;
*/

-- Q3: Minutes divergence DB vs ES (PostgreSQL syntax, text CDRs only)
/*
SELECT
    file_name,
    operator_id,
    cdr_type,
    total_minutes_db,
    total_minutes_es,
    ABS(total_minutes_db - total_minutes_es) AS delta_minutes,
    ROUND(
        ABS(total_minutes_db - total_minutes_es)
        / NULLIF(total_minutes_db, 0) * 100,
        2
    ) AS delta_pct
FROM cdr_registry
WHERE
    processing_status = 'DONE'
    AND total_minutes_db IS NOT NULL
    AND total_minutes_es IS NOT NULL
    AND ABS(total_minutes_db - total_minutes_es)
        / NULLIF(total_minutes_db, 0) > 0.01
    AND received_at >= CURRENT_DATE - INTERVAL '1 day'
    AND cdr_type NOT IN ('msc', 'pgw', 'cnn')   -- binary types excluded
ORDER BY delta_pct DESC;
*/

-- Q4: Missing records (text CDRs only — binary types excluded)
/*
SELECT
    file_name,
    operator_id,
    cdr_type,
    record_count_expected,
    record_count_processed,
    record_count_expected - record_count_processed AS missing_records,
    ROUND(
        (record_count_expected - record_count_processed)
        / NULLIF(record_count_expected, 0) * 100,
        2
    ) AS missing_pct
FROM cdr_registry
WHERE
    processing_status = 'DONE'
    AND record_count_processed < record_count_expected
    AND notes NOT LIKE '%Binary CDR%'
ORDER BY missing_records DESC;
*/

-- Q5: Daily summary per operator and type
/*
SELECT
    operator_id,
    cdr_type,
    DATE(received_at)         AS day,
    COUNT(*)                  AS total_files,
    SUM(CASE WHEN processing_status = 'DONE'      THEN 1 ELSE 0 END) AS done,
    SUM(CASE WHEN processing_status = 'PENDING'   THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN processing_status = 'MISMATCH'  THEN 1 ELSE 0 END) AS mismatch,
    SUM(CASE WHEN processing_status = 'ERROR'     THEN 1 ELSE 0 END) AS error
FROM cdr_registry
GROUP BY operator_id, cdr_type, DATE(received_at)
ORDER BY day DESC, operator_id, cdr_type;
*/
