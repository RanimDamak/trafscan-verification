"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A | Layer 3: Reconciliation Batch
────────────────────────────────────────────────────────────────────────────

Runs all checks against the CDR Registry and the backup filesystem.
Writes anomalies to anomaly_log. Prints a full report to stdout and
optionally saves it to a file.

Designed to run at 02:00 every night via cron/Task Scheduler.
Can also be run manually at any time for immediate diagnosis.

Checks
──────
Q1      Stuck files            : PENDING/PROCESSING older than N hours
Q2      Checksum mismatch      : checksum_transferred != checksum_raw/compressed
Q3a     Minutes divergence     : |total_minutes_db - total_minutes_es| > threshold%
Q3b     Voice divergence       : |voice_db - voice_es| > threshold%
Q3c     SMS divergence         : |sms_db - sms_es| > threshold%
Q3d     Data divergence        : |data_db - data_es| > threshold%
Q3e     Recharge divergence    : |recharge_db - recharge_es| > threshold%
Q4      Missing records        : record_count_processed < record_count_expected
Q5a     File count baseline    : files received today vs configured baseline ± tolerance
Q5b     Pipeline funnel        : files received vs processed by Java vs completed
Q5c     Files on disk not in DB: files present in backup tree but not registered
Q6      Daily summary          : per operator/type counts for the report
Q_PT    Processing time        : processing duration vs 7-day rolling median
Q7      Forfait daily count    : daily_state_forfait + daily_state_unknown_forfait vs baseline

Usage
─────
python batch.py                          # uses config.yaml in current dir
python batch.py --config config.yaml    # explicit config path
python batch.py --report report.txt     # save report to file
python batch.py --dry-run               # run checks, print report, no DB writes
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import yaml

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_conn(config: dict):
    db = config.get("database", {})
    return psycopg2.connect(
        host=db.get("host", "localhost"),
        port=int(db.get("port", 5432)),
        dbname=db.get("dbname", "trafscan"),
        user=db.get("user", "trafscan_user"),
        password=db.get("password", ""),
    )


def log_anomaly(conn, file_id: Optional[str], anomaly_type: str,
                severity: str, delta_value=None, threshold_value=None,
                dry_run: bool = False) -> None:
    """
    Insert one row into anomaly_log.

    anomaly_type values:
        CHECKSUM_MISMATCH | MISSING_RECORDS | MINUTES_DIVERGENCE |
        VOICE_DIVERGENCE | SMS_DIVERGENCE | DATA_DIVERGENCE |
        RECHARGE_DIVERGENCE | TIMEOUT | FILE_MISSING |
        PROCESSING_TIME_ANOMALY | FORFAIT_COUNT_DROP
    severity: INFO | WARNING | CRITICAL
    """
    if dry_run:
        log.debug("[batch] DRY-RUN: would insert anomaly %s / %s for file_id=%s",
                  anomaly_type, severity, file_id)
        return

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO anomaly_log
                (file_id, anomaly_type, delta_value, threshold_value, severity)
            VALUES (%s, %s::anomaly_type, %s, %s, %s::severity_level)
            ON CONFLICT DO NOTHING
        """, (file_id, anomaly_type, delta_value, threshold_value, severity))
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Q1 – Stuck files
# ─────────────────────────────────────────────────────────────────────────────

def check_stuck_files(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q1: Files stuck in PENDING or PROCESSING for longer than
    `stuck_file_hours` hours.  These indicate a pipeline failure:
    either the Java processor never picked up the file, or it crashed
    without updating the status.

    Severity:
        WARNING  → stuck between stuck_file_hours and 2× that
        CRITICAL → stuck longer than 2× stuck_file_hours
    """
    hours = config.get("batch", {}).get("stuck_file_hours", 2)
    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                file_id,
                file_name,
                operator_id,
                cdr_type,
                received_at,
                processing_status,
                EXTRACT(EPOCH FROM (NOW() - received_at)) / 3600 AS hours_waiting
            FROM cdr_registry
            WHERE
                processing_status IN ('PENDING', 'PROCESSING')
                AND received_at < NOW() - INTERVAL '1 hour' * %s
            ORDER BY received_at ASC
        """, (hours,))
        rows = cur.fetchall()

    for row in rows:
        file_id, file_name, operator_id, cdr_type, received_at, status, hours_waiting = row
        severity = 'CRITICAL' if hours_waiting > hours * 2 else 'WARNING'
        results.append({
            "file_id": str(file_id),
            "file_name": file_name,
            "operator_id": operator_id,
            "cdr_type": cdr_type,
            "status": status,
            "hours_waiting": round(float(hours_waiting), 1),
            "severity": severity,
        })
        log_anomaly(conn, str(file_id), 'TIMEOUT', severity,
                    delta_value=round(float(hours_waiting), 2),
                    threshold_value=hours, dry_run=dry_run)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q2 – Checksum mismatches
# ─────────────────────────────────────────────────────────────────────────────

def check_checksum_mismatches(conn, dry_run: bool) -> list[dict]:
    """
    Q2: Files where checksum_transferred differs from the expected checksum.
 
    Expected checksum resolution order:
      1. checksum_compressed  (always set for pre-compressed files after hasher fix)
      2. checksum_raw         (fallback: file was never compressed)
 
    Pre-compressed files: checksum_compressed = checksum_raw (set by hasher),
    so BOTH comparisons produce the same result. No special case needed in SQL,
    but the result dict now includes is_pre_compressed for report clarity.
 
    Always CRITICAL — a corrupted file at the regulator is never acceptable.
    """
    results = []
 
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                file_id,
                file_name,
                operator_id,
                cdr_type,
                checksum_raw,
                checksum_compressed,
                checksum_transferred,
                COALESCE(is_pre_compressed, FALSE) AS is_pre_compressed
            FROM cdr_registry
            WHERE
                checksum_transferred IS NOT NULL
                AND (
                    (checksum_compressed IS NOT NULL
                     AND checksum_compressed != checksum_transferred)
                    OR
                    (checksum_compressed IS NULL
                     AND checksum_raw != checksum_transferred)
                )
        """)
        rows = cur.fetchall()
 
    for row in rows:
        file_id, file_name, operator_id, cdr_type, raw, compressed, transferred, pre_comp = row
        expected = compressed if compressed else raw
        results.append({
            "file_id": str(file_id),
            "file_name": file_name,
            "operator_id": operator_id,
            "cdr_type": cdr_type,
            "expected": expected,
            "transferred": transferred,
            "is_pre_compressed": pre_comp,
            "severity": "CRITICAL",
        })
        log_anomaly(conn, str(file_id), 'CHECKSUM_MISMATCH', 'CRITICAL',
                    dry_run=dry_run)
 
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q3 – DB vs ES divergence (5 dimensions)
# ─────────────────────────────────────────────────────────────────────────────

def _check_dimension_divergence(conn, config: dict, dry_run: bool,
                                 db_col: str, es_col: str,
                                 anomaly_type: str,
                                 exclude_types: tuple = ('msc', 'pgw', 'cnn')
                                 ) -> list[dict]:
    """
    Generic helper for Q3a–Q3e.
    Checks |db_col - es_col| / db_col > threshold%.
    Returns a list of anomaly dicts.
    """
    threshold_pct = config.get("batch", {}).get("minutes_divergence_pct", 0.5)
    results = []

    exclude_sql = ", ".join(f"'{t}'" for t in exclude_types)

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                file_id,
                file_name,
                operator_id,
                cdr_type,
                {db_col},
                {es_col},
                ROUND(
                    ABS({db_col} - {es_col})
                    / NULLIF({db_col}, 0) * 100,
                    4
                ) AS delta_pct
            FROM cdr_registry
            WHERE
                processing_status = 'DONE'
                AND {db_col} IS NOT NULL
                AND {es_col} IS NOT NULL
                AND ABS({db_col} - {es_col})
                    / NULLIF({db_col}, 0) * 100 > %s
                AND cdr_type NOT IN ({exclude_sql})
            ORDER BY delta_pct DESC
        """, (threshold_pct,))
        rows = cur.fetchall()

    for row in rows:
        file_id, file_name, operator_id, cdr_type, db_val, es_val, delta = row
        severity = 'CRITICAL' if float(delta) > threshold_pct * 2 else 'WARNING'
        results.append({
            "file_id": str(file_id),
            "file_name": file_name,
            "operator_id": operator_id,
            "cdr_type": cdr_type,
            "db_value": float(db_val),
            "es_value": float(es_val),
            "delta_pct": float(delta),
            "dimension": anomaly_type,
            "severity": severity,
        })
        log_anomaly(conn, str(file_id), anomaly_type, severity,
                    delta_value=float(delta), threshold_value=threshold_pct,
                    dry_run=dry_run)

    return results


def check_minutes_divergence(conn, config: dict, dry_run: bool) -> list[dict]:
    """Q3a: total_minutes_db vs total_minutes_es"""
    return _check_dimension_divergence(
        conn, config, dry_run,
        db_col="total_minutes_db", es_col="total_minutes_es",
        anomaly_type="MINUTES_DIVERGENCE",
    )


def check_voice_divergence(conn, config: dict, dry_run: bool) -> list[dict]:
    """Q3b: voice_db vs voice_es  (AbonneTable.voice_counted_balance)"""
    return _check_dimension_divergence(
        conn, config, dry_run,
        db_col="voice_db", es_col="voice_es",
        anomaly_type="VOICE_DIVERGENCE",
    )


def check_sms_divergence(conn, config: dict, dry_run: bool) -> list[dict]:
    """Q3c: sms_db vs sms_es  (AbonneTable.sms_counted_balance)"""
    return _check_dimension_divergence(
        conn, config, dry_run,
        db_col="sms_db", es_col="sms_es",
        anomaly_type="SMS_DIVERGENCE",
    )


def check_data_divergence(conn, config: dict, dry_run: bool) -> list[dict]:
    """Q3d: data_db vs data_es  (AbonneTable.data_counted_balance)"""
    return _check_dimension_divergence(
        conn, config, dry_run,
        db_col="data_db", es_col="data_es",
        anomaly_type="DATA_DIVERGENCE",
    )


def check_recharge_divergence(conn, config: dict, dry_run: bool) -> list[dict]:
    """Q3e: recharge_db vs recharge_es  (AbonneTable.recharge_balance)"""
    return _check_dimension_divergence(
        conn, config, dry_run,
        db_col="recharge_db", es_col="recharge_es",
        anomaly_type="RECHARGE_DIVERGENCE",
        exclude_types=('msc', 'pgw', 'cnn', 'sdp'),  # recharge only in in/air/occ
    )


# ─────────────────────────────────────────────────────────────────────────────
# Q4 – Missing records
# ─────────────────────────────────────────────────────────────────────────────

def check_missing_records(conn, dry_run: bool) -> list[dict]:
    """
    Q4: Files where record_count_processed < record_count_expected.
    Binary CDR types excluded (their expected count is meaningless line count).
    Pre-compressed files included — count_records_smart() gives the correct
    decompressed record count, so the comparison is valid.
 
    Severity:
        WARNING  → missing < 1% of expected
        CRITICAL → missing >= 1% of expected
    """
    results = []
 
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                file_id,
                file_name,
                operator_id,
                cdr_type,
                record_count_expected,
                record_count_processed,
                record_count_expected - record_count_processed AS missing,
                ROUND(
                    (record_count_expected - record_count_processed)::numeric
                    / NULLIF(record_count_expected, 0) * 100,
                    2
                ) AS missing_pct
            FROM cdr_registry
            WHERE
                processing_status = 'DONE'
                AND record_count_processed IS NOT NULL
                AND record_count_processed < record_count_expected
                AND (notes IS NULL OR notes NOT LIKE '%%Binary CDR%%')
            ORDER BY missing DESC
        """)
        rows = cur.fetchall()
 
    for row in rows:
        file_id, file_name, operator_id, cdr_type, expected, processed, missing, pct = row
        severity = 'CRITICAL' if float(pct) >= 1.0 else 'WARNING'
        results.append({
            "file_id": str(file_id),
            "file_name": file_name,
            "operator_id": operator_id,
            "cdr_type": cdr_type,
            "expected": int(expected),
            "processed": int(processed),
            "missing": int(missing),
            "missing_pct": float(pct),
            "severity": severity,
        })
        log_anomaly(conn, str(file_id), 'MISSING_RECORDS', severity,
                    delta_value=float(pct), threshold_value=1.0,
                    dry_run=dry_run)
 
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q5 – Completeness checks (a / b / c)
# ─────────────────────────────────────────────────────────────────────────────

def check_file_count_baseline(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q5a: Compare number of files received today vs configured baseline
    per operator × cdr_type.
    Baseline is set in config.yaml under batch.file_count_baseline.
    Zéro files received when baseline > 0 → always CRITICAL.
    """
    baselines = config.get("batch", {}).get("file_count_baseline", {})
    tolerance_pct = config.get("batch", {}).get("baseline_tolerance_pct", 20)

    if not baselines:
        log.info("[batch] Q5a: No baseline configured — skipping.")
        return []

    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT operator_id, cdr_type::text, COUNT(*) AS cnt
            FROM cdr_registry
            WHERE received_at >= CURRENT_DATE
            GROUP BY operator_id, cdr_type
        """)
        today_counts = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    for key, baseline in baselines.items():
        # key format: "orange/in" or "atel/msc"
        parts = key.split("/")
        if len(parts) != 2:
            log.warning("[batch] Q5a: invalid baseline key '%s' — expected 'operator/type'", key)
            continue
        operator_id, cdr_type = parts[0].strip(), parts[1].strip()
        actual = today_counts.get((operator_id, cdr_type), 0)
        low = baseline * (1 - tolerance_pct / 100)
        high = baseline * (1 + tolerance_pct / 100)

        if actual == 0 and baseline > 0:
            severity = "CRITICAL"
            detail = f"0 files received — baseline is {baseline}"
        elif actual < low:
            drop_pct = round((baseline - actual) / baseline * 100, 1)
            severity = "CRITICAL" if drop_pct > tolerance_pct * 2 else "WARNING"
            detail = f"{actual} files received — {drop_pct}% below baseline {baseline}"
        elif actual > high:
            excess_pct = round((actual - baseline) / baseline * 100, 1)
            severity = "WARNING"
            detail = f"{actual} files received — {excess_pct}% above baseline {baseline}"
        else:
            continue  # within tolerance, no anomaly

        results.append({
            "operator_id": operator_id,
            "cdr_type": cdr_type,
            "actual": actual,
            "baseline": baseline,
            "detail": detail,
            "severity": severity,
        })
        log_anomaly(conn, None, 'FILE_MISSING', severity,
                    delta_value=float(actual), threshold_value=float(baseline),
                    dry_run=dry_run)

    return results


def check_pipeline_funnel(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q5b: Pipeline funnel — verify files progress from PENDING → PROCESSING → DONE.
    Detects files stuck between stages without triggering Q1 (which needs the 2h window).
    Reports a WARNING if > X% of today's files are still PENDING/PROCESSING at batch time.
    """
    threshold_pct = config.get("batch", {}).get("funnel_pending_warning_pct", 10)
    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                operator_id,
                cdr_type::text,
                COUNT(*) AS total,
                SUM(CASE WHEN processing_status IN ('PENDING','PROCESSING') THEN 1 ELSE 0 END) AS still_pending,
                SUM(CASE WHEN processing_status = 'DONE' THEN 1 ELSE 0 END) AS completed
            FROM cdr_registry
            WHERE received_at >= CURRENT_DATE
            GROUP BY operator_id, cdr_type
            HAVING COUNT(*) > 0
        """)
        rows = cur.fetchall()

    for row in rows:
        operator_id, cdr_type, total, still_pending, completed = row
        if total == 0:
            continue
        pending_pct = round(still_pending / total * 100, 1)
        if pending_pct > threshold_pct:
            severity = "CRITICAL" if pending_pct > threshold_pct * 3 else "WARNING"
            results.append({
                "operator_id": operator_id,
                "cdr_type": cdr_type,
                "total": int(total),
                "still_pending": int(still_pending),
                "completed": int(completed),
                "pending_pct": pending_pct,
                "detail": f"{still_pending}/{total} files still PENDING/PROCESSING ({pending_pct}%)",
                "severity": severity,
            })
            log_anomaly(conn, None, 'TIMEOUT', severity,
                        delta_value=pending_pct, threshold_value=threshold_pct,
                        dry_run=dry_run)

    return results


def check_files_missing_from_db(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q5c: Files present on disk in the backup tree but NOT registered in
    the cdr_registry. Catches files the watcher missed.
    """
    sources = config.get("watcher", {}).get("sources", [])
    results = []

    with conn.cursor() as cur:
        cur.execute("SELECT file_name FROM cdr_registry")
        registered = {row[0] for row in cur.fetchall()}

    for source in sources:
        directory = source.get("directory", "")
        operator = source.get("operator", "")
        cdr_type = source.get("cdr_type", "")
        extensions = [e.lower() for e in source.get("extensions", [])]

        if not os.path.isdir(directory):
            continue

        for fname in os.listdir(directory):
            fpath = os.path.join(directory, fname)
            if not os.path.isfile(fpath):
                continue

            lower = fname.lower()
            matched = any(
                (ext == "" and "." not in fname) or (ext != "" and lower.endswith(ext))
                for ext in extensions
            )
            if not matched:
                continue

            if fname not in registered:
                results.append({
                    "file_name": fname,
                    "operator_id": operator,
                    "cdr_type": cdr_type,
                    "directory": directory,
                    "severity": "CRITICAL",
                })
                log_anomaly(conn, None, 'FILE_MISSING', 'CRITICAL', dry_run=dry_run)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q6 – Daily summary
# ─────────────────────────────────────────────────────────────────────────────

def get_daily_summary(conn) -> list[dict]:
    """
    Q6: Daily summary per operator/type/status for today.
    Informational — not an anomaly check.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                operator_id,
                cdr_type,
                COUNT(*)                                                         AS total,
                SUM(CASE WHEN processing_status = 'DONE'       THEN 1 ELSE 0 END) AS done,
                SUM(CASE WHEN processing_status = 'PENDING'    THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN processing_status = 'PROCESSING' THEN 1 ELSE 0 END) AS processing,
                SUM(CASE WHEN processing_status = 'MISMATCH'   THEN 1 ELSE 0 END) AS mismatch,
                SUM(CASE WHEN processing_status = 'ERROR'      THEN 1 ELSE 0 END) AS error
            FROM cdr_registry
            WHERE received_at >= CURRENT_DATE
            GROUP BY operator_id, cdr_type
            ORDER BY operator_id, cdr_type
        """)
        rows = cur.fetchall()

    return [
        {
            "operator_id": r[0], "cdr_type": r[1],
            "total": r[2], "done": r[3], "pending": r[4],
            "processing": r[5], "mismatch": r[6], "error": r[7],
        }
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Q_PT – Processing time anomaly (new in v3.1)
# ─────────────────────────────────────────────────────────────────────────────

def check_processing_time(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q_PT: Detect files whose processing duration is abnormally long compared
    to the 7-day rolling median for the same cdr_type.

    Strategy:
    - Use processing_ended_at - processing_started_at when both are set
      (requires Java hooks Hook 1+2 to be integrated).
    - Fallback: updated_at - received_at (includes queue wait time, but
      acceptable as a general indicator per the team lead's suggestion).
    - Skip files where the fallback would be 0 (not yet touched).
    - Only activates once >= min_samples files exist in the 7-day window
      per cdr_type (configurable, default 10).

    Severity:
        WARNING  → duration > warning_multiplier  × median  (default 3×)
        CRITICAL → duration > critical_multiplier × median  (default 5×)

    When Java hooks are live: set use_java_timestamps: true in config.yaml.
    This switches to the more precise processing_started_at/ended_at columns.
    """
    pt_cfg = config.get("batch", {}).get("processing_time", {})
    min_samples       = pt_cfg.get("min_samples", 10)
    warning_mult      = pt_cfg.get("warning_multiplier", 3.0)
    critical_mult     = pt_cfg.get("critical_multiplier", 5.0)
    use_java_ts       = pt_cfg.get("use_java_timestamps", False)

    if use_java_ts:
        duration_expr    = "EXTRACT(EPOCH FROM (processing_ended_at - processing_started_at))"
        valid_condition  = "processing_ended_at IS NOT NULL AND processing_started_at IS NOT NULL"
        ts_mode          = "java_hooks"
    else:
        # Fallback: updated_at - received_at
        # Exclude files not yet touched (updated_at ≈ received_at, diff < 5s)
        duration_expr    = "EXTRACT(EPOCH FROM (updated_at - received_at))"
        valid_condition  = ("updated_at IS NOT NULL AND received_at IS NOT NULL "
                            "AND EXTRACT(EPOCH FROM (updated_at - received_at)) > 5")
        ts_mode          = "fallback_updated_at"

    results = []

    with conn.cursor() as cur:
        # Step 1: compute 7-day rolling median per cdr_type
        cur.execute(f"""
            WITH durations AS (
                SELECT
                    cdr_type::text                           AS cdr_type,
                    {duration_expr}                          AS duration_s,
                    COUNT(*) OVER (PARTITION BY cdr_type)    AS sample_count
                FROM cdr_registry
                WHERE
                    processing_status = 'DONE'
                    AND {valid_condition}
                    AND received_at >= NOW() - INTERVAL '7 days'
            ),
            medians AS (
                SELECT
                    cdr_type,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_s) AS median_s,
                    MAX(sample_count) AS sample_count
                FROM durations
                GROUP BY cdr_type
            )
            SELECT cdr_type, median_s, sample_count
            FROM medians
            WHERE sample_count >= %s
        """, (min_samples,))
        medians = {row[0]: (float(row[1]), int(row[2])) for row in cur.fetchall()}

    if not medians:
        log.info("[batch] Q_PT: Not enough historical data yet (min_samples=%d per type). Skipping.", min_samples)
        return []

    with conn.cursor() as cur:
        # Step 2: find today's files that exceed thresholds
        # We iterate per cdr_type so we can parameterise the median
        for cdr_type, (median_s, sample_count) in medians.items():
            if median_s <= 0:
                continue

            cur.execute(f"""
                SELECT
                    file_id,
                    file_name,
                    operator_id,
                    cdr_type::text,
                    {duration_expr}                          AS duration_s,
                    ROUND(({duration_expr}) / %s, 2)         AS ratio
                FROM cdr_registry
                WHERE
                    processing_status = 'DONE'
                    AND {valid_condition}
                    AND cdr_type::text = %s
                    AND received_at >= CURRENT_DATE
                    AND {duration_expr} > %s * %s
                ORDER BY ratio DESC
            """, (median_s, cdr_type, warning_mult, median_s))

            rows = cur.fetchall()
            for row in rows:
                file_id, file_name, operator_id, cdr_type_r, duration_s, ratio = row
                severity = 'CRITICAL' if float(ratio) >= critical_mult else 'WARNING'
                results.append({
                    "file_id": str(file_id),
                    "file_name": file_name,
                    "operator_id": operator_id,
                    "cdr_type": cdr_type_r,
                    "duration_s": round(float(duration_s), 1),
                    "median_s": round(median_s, 1),
                    "ratio": float(ratio),
                    "ts_mode": ts_mode,
                    "severity": severity,
                    "detail": (
                        f"Duration {round(float(duration_s)/60,1)}min "
                        f"= {float(ratio):.1f}× median "
                        f"({round(median_s/60,1)}min over {sample_count} samples, 7d)"
                    ),
                })
                log_anomaly(conn, str(file_id), 'PROCESSING_TIME_ANOMALY', severity,
                            delta_value=float(ratio), threshold_value=warning_mult,
                            dry_run=dry_run)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q7 – Forfait daily count (new in v3.1)
# ─────────────────────────────────────────────────────────────────────────────

def check_forfait_daily(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q7: Verify daily forfait counts don't collapse unexpectedly.

    Part 1 — DB internal consistency (implemented, certain):
        total_db = daily_state_forfait.SUM(count)        [known forfaits]
                 + daily_state_unknown_forfait.SUM(count) [unknown forfaits]

        We compare today's total against the 7-day median.
        Alert if it drops significantly (configurable thresholds).
        Zero when median > 0 → always CRITICAL.

    Part 2 — ES vs DB comparison (placeholder):
        # TODO: Confirm with team — does the ES index expose a count of
        # CDR records with cdrType='forfait' (or equivalent) for a given date?
        # If yes, add ES query here and compare against total_db.
        # Until confirmed, Part 2 is intentionally NOT implemented.

    Tables used (same PostgreSQL database as CDR registry):
        public.daily_state_forfait         (id, date, count, id_forfait)
        public.daily_state_unknown_forfait (id, forfait, date, count)

    Config (config.yaml):
        batch:
          forfait:
            enabled: true
            drop_warning_pct:  30    # alert if today < 7d-median × (1 - 30%)
            drop_critical_pct: 70   # alert if today < 7d-median × (1 - 70%)
    """
    forfait_cfg = config.get("batch", {}).get("forfait", {})
    enabled           = forfait_cfg.get("enabled", True)
    drop_warn_pct     = forfait_cfg.get("drop_warning_pct", 30)
    drop_crit_pct     = forfait_cfg.get("drop_critical_pct", 70)

    if not enabled:
        log.info("[batch] Q7: Forfait check disabled in config.")
        return []

    # Guard: check forfait tables exist before querying them.
    # They only exist in the Trafscan application DB (production).
    # In a local test environment this check is skipped gracefully.
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM public.daily_state_forfait LIMIT 1")
    except Exception:
        conn.rollback()
        log.info("[batch] Q7: Tables daily_state_forfait / daily_state_unknown_forfait "
                 "not found — skipping. "
                 "(Expected in local test env. In production, use the Trafscan app DB.)")
        return []

    results = []

    with conn.cursor() as cur:
        # 7-day median of daily totals (known + unknown)
        cur.execute("""
            WITH daily_known AS (
                SELECT date, SUM(count) AS known_count
                FROM public.daily_state_forfait
                WHERE date >= CURRENT_DATE - INTERVAL '8 days'
                  AND date < CURRENT_DATE
                GROUP BY date
            ),
            daily_unknown AS (
                SELECT date, SUM(count) AS unknown_count
                FROM public.daily_state_unknown_forfait
                WHERE date >= CURRENT_DATE - INTERVAL '8 days'
                  AND date < CURRENT_DATE
                GROUP BY date
            ),
            daily_totals AS (
                SELECT
                    COALESCE(k.date, u.date)                          AS date,
                    COALESCE(k.known_count, 0)                        AS known,
                    COALESCE(u.unknown_count, 0)                      AS unknown,
                    COALESCE(k.known_count, 0) + COALESCE(u.unknown_count, 0) AS total
                FROM daily_known k
                FULL OUTER JOIN daily_unknown u ON k.date = u.date
            )
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total) AS median_total,
                COUNT(*) AS days_with_data
            FROM daily_totals
            WHERE total > 0
        """)
        row = cur.fetchone()
        if not row or row[0] is None:
            log.info("[batch] Q7: No historical forfait data available (< 1 day). Skipping.")
            return []

        median_total, days_with_data = float(row[0]), int(row[1])

        # Today's total
        cur.execute("""
            SELECT
                COALESCE(
                    (SELECT SUM(count) FROM public.daily_state_forfait
                     WHERE date = CURRENT_DATE), 0
                ) +
                COALESCE(
                    (SELECT SUM(count) FROM public.daily_state_unknown_forfait
                     WHERE date = CURRENT_DATE), 0
                ) AS today_total
        """)
        today_total = float(cur.fetchone()[0])

        # Known / unknown breakdown for the report
        cur.execute("""
            SELECT COALESCE(SUM(count), 0)
            FROM public.daily_state_forfait
            WHERE date = CURRENT_DATE
        """)
        today_known = float(cur.fetchone()[0])

        cur.execute("""
            SELECT COALESCE(SUM(count), 0)
            FROM public.daily_state_unknown_forfait
            WHERE date = CURRENT_DATE
        """)
        today_unknown = float(cur.fetchone()[0])

    drop_pct = 0.0
    severity = None

    if today_total == 0 and median_total > 0:
        severity = "CRITICAL"
        drop_pct = 100.0
        detail = (f"Today: 0 forfaits — median over {days_with_data} days is "
                  f"{median_total:,.0f}. Forfait processing may be broken.")
    elif median_total > 0:
        drop_pct = round((median_total - today_total) / median_total * 100, 1)
        if drop_pct >= drop_crit_pct:
            severity = "CRITICAL"
            detail = (f"Today: {today_total:,.0f} — {drop_pct}% below 7d-median "
                      f"({median_total:,.0f}). Possible processing failure.")
        elif drop_pct >= drop_warn_pct:
            severity = "WARNING"
            detail = (f"Today: {today_total:,.0f} — {drop_pct}% below 7d-median "
                      f"({median_total:,.0f}). Monitor closely.")

    if severity:
        results.append({
            "today_total": today_total,
            "today_known": today_known,
            "today_unknown": today_unknown,
            "median_total": median_total,
            "days_with_data": days_with_data,
            "drop_pct": drop_pct,
            "detail": detail,
            "severity": severity,
            # Part 2 placeholder
            "es_total": None,   # TODO: fill once ES field confirmed with team
        })
        log_anomaly(conn, None, 'FORFAIT_COUNT_DROP', severity,
                    delta_value=drop_pct, threshold_value=drop_warn_pct,
                    dry_run=dry_run)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Report builder
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds/60:.1f}min"
    return f"{seconds/3600:.1f}h"


def build_report(results: dict, config: dict) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []

    def section(title):
        lines.append("")
        lines.append("=" * 70)
        lines.append(f"  {title}")
        lines.append("=" * 70)

    def ok(msg):
        lines.append(f"  ✓ {msg}")

    def warn(msg):
        lines.append(f"  ⚠ {msg}")

    def crit(msg):
        lines.append(f"  ✗ {msg}")

    lines.append("=" * 70)
    lines.append("  TRAFSCAN : Reconciliation Batch Report v3.1")
    lines.append(f"  Generated: {now}")
    lines.append("=" * 70)

    # Q1
    section("Q1 · Stuck files (PENDING / PROCESSING too long)")
    stuck = results.get("stuck", [])
    if not stuck:
        ok("No stuck files.")
    else:
        for r in stuck:
            msg = (f"{r['file_name']} | op={r['operator_id']} "
                   f"type={r['cdr_type']} status={r['status']} "
                   f"waiting={r['hours_waiting']}h")
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # Q2
    section("Q2 · Checksum mismatches (transfer corruption)")
    mismatches = results.get("mismatches", [])
    if not mismatches:
        ok("All transferred files have matching checksums.")
    else:
        for r in mismatches:
            crit(f"{r['file_name']} | op={r['operator_id']} "
                 f"expected={r['expected'][:16]}... "
                 f"received={r['transferred'][:16]}...")

    # Q3a–e
    divergence_checks = [
        ("Q3a · Minutes divergence (DB vs ES)",        "divergences_minutes"),
        ("Q3b · Voice traffic divergence (DB vs ES)",  "divergences_voice"),
        ("Q3c · SMS traffic divergence (DB vs ES)",    "divergences_sms"),
        ("Q3d · Data traffic divergence (DB vs ES)",   "divergences_data"),
        ("Q3e · Recharge divergence (DB vs ES)",       "divergences_recharge"),
    ]
    threshold = config.get("batch", {}).get("minutes_divergence_pct", 0.5)
    for title, key in divergence_checks:
        section(title)
        divs = results.get(key, [])
        if not divs:
            ok(f"No divergence above {threshold}% threshold.")
        else:
            for r in divs:
                msg = (f"{r['file_name']} | op={r['operator_id']} "
                       f"db={r['db_value']:.3f} es={r['es_value']:.3f} "
                       f"delta={r['delta_pct']:.2f}%")
                crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # Q4
    section("Q4 · Missing records (record_count_processed < expected)")
    missing = results.get("missing_records", [])
    if not missing:
        ok("No missing records detected.")
    else:
        for r in missing:
            msg = (f"{r['file_name']} | op={r['operator_id']} "
                   f"expected={r['expected']} processed={r['processed']} "
                   f"missing={r['missing']} ({r['missing_pct']:.2f}%)")
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # Q5a
    section("Q5a · File count baseline check")
    baseline_issues = results.get("baseline_issues", [])
    if not baseline_issues:
        ok("All operator/type counts within baseline tolerance.")
    else:
        for r in baseline_issues:
            msg = f"{r['operator_id']}/{r['cdr_type']}: {r['detail']}"
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # Q5b
    section("Q5b · Pipeline funnel check")
    funnel_issues = results.get("funnel_issues", [])
    if not funnel_issues:
        ok("Pipeline funnel healthy — all files progressing normally.")
    else:
        for r in funnel_issues:
            msg = f"{r['operator_id']}/{r['cdr_type']}: {r['detail']}"
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # Q5c
    section("Q5c · Files on disk not registered in DB")
    unregistered = results.get("unregistered", [])
    if not unregistered:
        ok("All files on disk are registered in the DB.")
    else:
        for r in unregistered:
            crit(f"{r['file_name']} | op={r['operator_id']} "
                 f"type={r['cdr_type']} dir={r['directory']}")

    # Q6
    section("Q6 · Daily summary (files received today)")
    summary = results.get("summary", [])
    if not summary:
        lines.append("  No files received today.")
    else:
        lines.append(f"  {'Operator':<10} {'Type':<6} {'Total':>6} "
                     f"{'Done':>6} {'Pending':>8} {'Mismatch':>9} {'Error':>6}")
        lines.append("  " + "-" * 56)
        for r in summary:
            lines.append(
                f"  {r['operator_id']:<10} {str(r['cdr_type']):<6} "
                f"{r['total']:>6} {r['done']:>6} {r['pending']:>8} "
                f"{r['mismatch']:>9} {r['error']:>6}"
            )

    # Q_PT
    section("Q_PT · Processing time anomalies (vs 7-day median)")
    pt_issues = results.get("pt_issues", [])
    if not pt_issues:
        ok("All processing times within expected range.")
    else:
        for r in pt_issues:
            mode_note = " [fallback: updated_at]" if r.get("ts_mode") == "fallback_updated_at" else ""
            msg = f"{r['file_name']} | op={r['operator_id']}: {r['detail']}{mode_note}"
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # Q7
    section("Q7 · Forfait daily count (daily_state_forfait + daily_state_unknown_forfait)")
    forfait_issues = results.get("forfait_issues", [])
    if not forfait_issues:
        ok("Forfait daily count within expected range.")
    else:
        for r in forfait_issues:
            es_note = " | ES=TODO" if r.get("es_total") is None else f" | ES={r['es_total']:,.0f}"
            msg = (f"{r['detail']} "
                   f"[known={r['today_known']:,.0f} unknown={r['today_unknown']:,.0f}{es_note}]")
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # Totals
    section("Summary")
    all_issues = (stuck + mismatches
                  + results.get("divergences_minutes", [])
                  + results.get("divergences_voice", [])
                  + results.get("divergences_sms", [])
                  + results.get("divergences_data", [])
                  + results.get("divergences_recharge", [])
                  + missing + baseline_issues + funnel_issues
                  + unregistered + pt_issues + forfait_issues)
    total_anomalies = len(all_issues)
    critical = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    warnings = total_anomalies - critical

    if total_anomalies == 0:
        ok("All checks passed. System healthy.")
    else:
        lines.append(f"  Total anomalies : {total_anomalies}")
        lines.append(f"  Critical        : {critical}")
        lines.append(f"  Warnings        : {warnings}")
        if critical > 0:
            lines.append("")
            lines.append("  !! ACTION REQUIRED: Review CRITICAL anomalies above !!")

    lines.append("")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Trafscan Reconciliation Batch v3.1")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--report", default=None,
                        help="Path to save the text report (optional)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run checks and print report but do not write to anomaly_log")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with open(args.config) as f:
        config = yaml.safe_load(f)

    log.info("[batch] Starting reconciliation batch v3.1 ...")
    conn = get_conn(config)

    try:
        log.info("[batch] Q1:   Checking stuck files ...")
        stuck = check_stuck_files(conn, config, args.dry_run)
        log.info("[batch] Q1:   %d stuck file(s).", len(stuck))

        log.info("[batch] Q2:   Checking checksum mismatches ...")
        mismatches = check_checksum_mismatches(conn, args.dry_run)
        log.info("[batch] Q2:   %d mismatch(es).", len(mismatches))

        log.info("[batch] Q3a:  Checking minutes divergence ...")
        div_minutes = check_minutes_divergence(conn, config, args.dry_run)
        log.info("[batch] Q3a:  %d divergence(s).", len(div_minutes))

        log.info("[batch] Q3b:  Checking voice divergence ...")
        div_voice = check_voice_divergence(conn, config, args.dry_run)
        log.info("[batch] Q3b:  %d divergence(s).", len(div_voice))

        log.info("[batch] Q3c:  Checking SMS divergence ...")
        div_sms = check_sms_divergence(conn, config, args.dry_run)
        log.info("[batch] Q3c:  %d divergence(s).", len(div_sms))

        log.info("[batch] Q3d:  Checking data divergence ...")
        div_data = check_data_divergence(conn, config, args.dry_run)
        log.info("[batch] Q3d:  %d divergence(s).", len(div_data))

        log.info("[batch] Q3e:  Checking recharge divergence ...")
        div_recharge = check_recharge_divergence(conn, config, args.dry_run)
        log.info("[batch] Q3e:  %d divergence(s).", len(div_recharge))

        log.info("[batch] Q4:   Checking missing records ...")
        missing_records = check_missing_records(conn, args.dry_run)
        log.info("[batch] Q4:   %d file(s) with missing records.", len(missing_records))

        log.info("[batch] Q5a:  Checking file count vs baseline ...")
        baseline_issues = check_file_count_baseline(conn, config, args.dry_run)
        log.info("[batch] Q5a:  %d issue(s).", len(baseline_issues))

        log.info("[batch] Q5b:  Checking pipeline funnel ...")
        funnel_issues = check_pipeline_funnel(conn, config, args.dry_run)
        log.info("[batch] Q5b:  %d issue(s).", len(funnel_issues))

        log.info("[batch] Q5c:  Checking files on disk vs DB ...")
        unregistered = check_files_missing_from_db(conn, config, args.dry_run)
        log.info("[batch] Q5c:  %d unregistered file(s).", len(unregistered))

        log.info("[batch] Q6:   Building daily summary ...")
        summary = get_daily_summary(conn)

        log.info("[batch] Q_PT: Checking processing times ...")
        pt_issues = check_processing_time(conn, config, args.dry_run)
        log.info("[batch] Q_PT: %d processing time anomaly(ies).", len(pt_issues))

        log.info("[batch] Q7:   Checking forfait daily counts ...")
        forfait_issues = check_forfait_daily(conn, config, args.dry_run)
        log.info("[batch] Q7:   %d forfait issue(s).", len(forfait_issues))

    finally:
        conn.close()

    results = {
        "stuck":                stuck,
        "mismatches":           mismatches,
        "divergences_minutes":  div_minutes,
        "divergences_voice":    div_voice,
        "divergences_sms":      div_sms,
        "divergences_data":     div_data,
        "divergences_recharge": div_recharge,
        "missing_records":      missing_records,
        "baseline_issues":      baseline_issues,
        "funnel_issues":        funnel_issues,
        "unregistered":         unregistered,
        "summary":              summary,
        "pt_issues":            pt_issues,
        "forfait_issues":       forfait_issues,
    }

    # Text report
    report = build_report(results, config)
    print(report)
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            f.write(report)
        log.info("[batch] Text report saved to: %s", args.report)

    # HTML report + email + NOC
    from alerting import build_html_report, send_email_alert, build_noc_payload, send_noc_notification
    html_report = build_html_report(results, config)

    report_dir = config.get("batch", {}).get("report_dir", "reports")
    date_str   = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path  = Path(report_dir) / f"report_{date_str}.html"
    html_path.parent.mkdir(parents=True, exist_ok=True)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_report)
    log.info("[batch] HTML report saved to: %s", html_path)

    if not args.dry_run:
        send_email_alert(results, config, html_report)
        noc_payload = build_noc_payload(results)
        send_noc_notification(noc_payload, config)
    else:
        log.info("[batch] DRY-RUN: skipping email and NOC notification.")

    all_issues = (stuck + mismatches + div_minutes + div_voice + div_sms
                  + div_data + div_recharge + missing_records
                  + baseline_issues + funnel_issues + unregistered
                  + pt_issues + forfait_issues)
    total = len(all_issues)
    log.info("[batch] Done. %d anomaly(ies) detected.", total)

    critical = any(r.get("severity") == "CRITICAL" for r in all_issues)
    sys.exit(1 if critical else 0)


if __name__ == "__main__":
    main()