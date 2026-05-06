"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A | Layer 3: Reconciliation Batch
────────────────────────────────────────────────────────────────────────────
Runs checks against the CDR Registry and the backup filesystem.
Writes anomalies to anomaly_log. Prints a full report to stdout and
optionally saves it to a file.

Designed to run at 02:00 every night via cron/Task Scheduler.
Can also be run manually at any time for immediate diagnosis.

The checks
──────────
Q1   Stuck files           : PENDING/PROCESSING older than N hours
Q2   Checksum mismatch     : checksum_transferred != checksum_raw/compressed
Q3   Divergences DB↔ES     : 5 sub-checks (minutes, voice, SMS, data, recharge)
Q4   Missing records       : record_count_processed < record_count_expected
Q5a  Daily file count      : files received today vs expected baseline ± tolerance
Q5b  Pipeline funnel       : files flow correctly through each processing step
Q5c  Files on disk not     : files in backup tree but not in cdr_registry
     in DB
Q6   Daily summary         : per operator/type counts for the report

Note on Q5a and Q5b baselines
──────────────────────────────
The expected daily file count per operator/type is configured in config.yaml
under `baselines.operator_type`. Values are left as null until the Trafscan
team provides approximate daily counts. Any combination with a null baseline
is skipped with an INFO log — no false alerts.

Tolerance is configured as `baselines.tolerance_pct` (default 3.0%).
  WARNING  → count outside baseline ± tolerance_pct
  CRITICAL → count outside baseline ± (2 × tolerance_pct), or zero files

Note on processing time anomaly detection
──────────────────────────────────────────
processing_started_at / processing_ended_at are only populated once the
Trafscan Java team integrates the hook templates (README.md). The team lead
confirmed this requires enriching the Java decoder. Once integrated, a
processing-time outlier check will be added here.

Note on forfait reconciliation
──────────────────────────────
Forfait data lives in two separate tables (known / unknown forfaits).
Not per-file aggregates. A dedicated query will be added once the team
confirms exact table names and comparable ES fields.
See TODO FORFAITS markers throughout this file.

Usage
─────
  python batch.py                     # uses config.yaml in current dir
  python batch.py --config cfg.yaml   # explicit config path
  python batch.py --report out.txt    # save text report to file
  python batch.py --dry-run           # checks only, no DB writes, no email
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
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

    anomaly_type : 'CHECKSUM_MISMATCH' | 'MISSING_RECORDS'    |
                   'MINUTES_DIVERGENCE' | 'VOICE_DIVERGENCE'   |
                   'SMS_DIVERGENCE'     | 'DATA_DIVERGENCE'    |
                   'RECHARGE_DIVERGENCE'| 'TIMEOUT'            |
                   'FILE_MISSING'
    severity     : 'INFO' | 'WARNING' | 'CRITICAL'
    """
    if dry_run:
        log.debug("[batch] DRY-RUN: would insert anomaly %s/%s file_id=%s",
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
# Helper: generic DB↔ES divergence check
# ─────────────────────────────────────────────────────────────────────────────

def _check_divergence(conn, config: dict, dry_run: bool,
                      db_col: str, es_col: str,
                      anomaly_type: str, label: str,
                      extra_where: str = "") -> list[dict]:
    """
    Generic divergence check for any numeric _db / _es column pair.

    WARNING  → delta between threshold_pct and 2× threshold_pct
    CRITICAL → delta > 2× threshold_pct
    """
    threshold_pct = config.get("batch", {}).get("divergence_pct", 1.0)
    results = []

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                file_id, file_name, operator_id, cdr_type,
                {db_col}, {es_col},
                ROUND(
                    ABS({db_col} - {es_col})
                    / NULLIF({db_col}, 0) * 100, 4
                ) AS delta_pct
            FROM cdr_registry
            WHERE
                processing_status = 'DONE'
                AND {db_col} IS NOT NULL
                AND {es_col} IS NOT NULL
                AND ABS({db_col} - {es_col})
                    / NULLIF({db_col}, 0) * 100 > %s
                {extra_where}
            ORDER BY delta_pct DESC
        """, (threshold_pct,))
        rows = cur.fetchall()

    for row in rows:
        file_id, file_name, operator_id, cdr_type, db_val, es_val, delta = row
        severity = "CRITICAL" if float(delta) > threshold_pct * 2 else "WARNING"
        results.append({
            "file_id":     str(file_id),
            "file_name":   file_name,
            "operator_id": operator_id,
            "cdr_type":    cdr_type,
            "dimension":   label,
            "db_value":    float(db_val),
            "es_value":    float(es_val),
            "delta_pct":   float(delta),
            "severity":    severity,
        })
        log_anomaly(conn, str(file_id), anomaly_type, severity,
                    delta_value=float(delta), threshold_value=threshold_pct,
                    dry_run=dry_run)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q1: Stuck files
# ─────────────────────────────────────────────────────────────────────────────

def check_stuck_files(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q1: Files in PENDING or PROCESSING longer than `stuck_file_hours`.
    WARNING  → stuck between stuck_file_hours and 2× that
    CRITICAL → stuck longer than 2× stuck_file_hours
    """
    hours = config.get("batch", {}).get("stuck_file_hours", 2)
    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                file_id, file_name, operator_id, cdr_type,
                received_at, processing_status,
                EXTRACT(EPOCH FROM (NOW() - received_at)) / 3600 AS hours_waiting
            FROM cdr_registry
            WHERE
                processing_status IN ('PENDING', 'PROCESSING')
                AND received_at < NOW() - INTERVAL '1 hour' * %s
            ORDER BY received_at ASC
        """, (hours,))
        rows = cur.fetchall()

    for row in rows:
        file_id, file_name, operator_id, cdr_type, received_at, status, hw = row
        severity = "CRITICAL" if float(hw) > hours * 2 else "WARNING"
        results.append({
            "file_id":       str(file_id),
            "file_name":     file_name,
            "operator_id":   operator_id,
            "cdr_type":      cdr_type,
            "status":        status,
            "hours_waiting": round(float(hw), 1),
            "severity":      severity,
        })
        log_anomaly(conn, str(file_id), "TIMEOUT", severity,
                    delta_value=round(float(hw), 2),
                    threshold_value=hours, dry_run=dry_run)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q2: Checksum mismatches
# ─────────────────────────────────────────────────────────────────────────────

def check_checksum_mismatches(conn, dry_run: bool) -> list[dict]:
    """
    Q2: checksum_transferred differs from expected (compressed or raw).
    Always CRITICAL — any corruption during FTP transfer is unacceptable.
    """
    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                file_id, file_name, operator_id, cdr_type,
                checksum_raw, checksum_compressed, checksum_transferred
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
        file_id, file_name, operator_id, cdr_type, raw, compressed, transferred = row
        expected = compressed if compressed else raw
        results.append({
            "file_id":     str(file_id),
            "file_name":   file_name,
            "operator_id": operator_id,
            "cdr_type":    cdr_type,
            "expected":    expected,
            "transferred": transferred,
            "severity":    "CRITICAL",
        })
        log_anomaly(conn, str(file_id), "CHECKSUM_MISMATCH", "CRITICAL",
                    dry_run=dry_run)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q3: All divergence checks (5 dimensions)
# ─────────────────────────────────────────────────────────────────────────────

def check_all_divergences(conn, config: dict, dry_run: bool) -> dict[str, list[dict]]:
    """
    Q3: Compare DB vs ES values for all 5 traffic dimensions.

    minutes  : total call/session minutes  — text CDR types only
    voice    : AbonneTable.voice_counted_balance aggregate
    sms      : AbonneTable.sms_counted_balance aggregate
    data     : AbonneTable.data_counted_balance aggregate
    recharge : AbonneTable.recharge_balance aggregate — in CDRs only

    TODO FORFAITS: add once team confirms forfait table names.
    """
    EXCL = "AND cdr_type NOT IN ('msc', 'pgw', 'cnn')"
    return {
        "minutes":  _check_divergence(conn, config, dry_run,
                                      "total_minutes_db", "total_minutes_es",
                                      "MINUTES_DIVERGENCE", "minutes",
                                      extra_where=EXCL),
        "voice":    _check_divergence(conn, config, dry_run,
                                      "voice_db", "voice_es",
                                      "VOICE_DIVERGENCE", "voice"),
        "sms":      _check_divergence(conn, config, dry_run,
                                      "sms_db", "sms_es",
                                      "SMS_DIVERGENCE", "SMS"),
        "data":     _check_divergence(conn, config, dry_run,
                                      "data_db", "data_es",
                                      "DATA_DIVERGENCE", "data"),
        "recharge": _check_divergence(conn, config, dry_run,
                                      "recharge_db", "recharge_es",
                                      "RECHARGE_DIVERGENCE", "recharge",
                                      extra_where="AND cdr_type = 'in'"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Q4: Missing records per file
# ─────────────────────────────────────────────────────────────────────────────

def check_missing_records(conn, dry_run: bool) -> list[dict]:
    """
    Q4: record_count_processed < record_count_expected for a given file.

    record_count_expected = raw line count at reception (Python watcher).
    record_count_processed = records actually parsed by Java.

    Note: this is a per-file check. For binary CDR types (msc, pgw, cnn)
    the line count is meaningless and these are automatically excluded.
    For text CDRs, a discrepancy means Java dropped records during parsing.

    WARNING  → missing < 1% of expected
    CRITICAL → missing >= 1% of expected
    """
    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                file_id, file_name, operator_id, cdr_type,
                record_count_expected, record_count_processed,
                record_count_expected - record_count_processed AS missing,
                ROUND(
                    (record_count_expected - record_count_processed)::numeric
                    / NULLIF(record_count_expected, 0) * 100, 2
                ) AS missing_pct
            FROM cdr_registry
            WHERE
                processing_status IN ('DONE', 'MISMATCH')
                AND record_count_processed IS NOT NULL
                AND record_count_processed < record_count_expected
                AND (notes IS NULL OR notes NOT LIKE '%%Binary CDR%%')
            ORDER BY missing DESC
        """)
        rows = cur.fetchall()

    for row in rows:
        file_id, file_name, operator_id, cdr_type, exp, proc, missing, pct = row
        severity = "CRITICAL" if float(pct) >= 1.0 else "WARNING"
        results.append({
            "file_id":     str(file_id),
            "file_name":   file_name,
            "operator_id": operator_id,
            "cdr_type":    cdr_type,
            "expected":    int(exp),
            "processed":   int(proc),
            "missing":     int(missing),
            "missing_pct": float(pct),
            "severity":    severity,
        })
        log_anomaly(conn, str(file_id), "MISSING_RECORDS", severity,
                    delta_value=float(pct), threshold_value=1.0,
                    dry_run=dry_run)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q5a: Daily file count vs baseline
# ─────────────────────────────────────────────────────────────────────────────

def check_daily_file_count(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q5a: For each operator × CDR type with a baseline configured, compare
    today's received file count against the expected range.

    Expected range = [baseline × (1 - tol%), baseline × (1 + tol%)]

    Severity:
      WARNING  → count outside baseline ± tolerance_pct
      CRITICAL → count outside baseline ± (2 × tolerance_pct)
               → OR zero files received when baseline > 0

    Types with null baseline in config.yaml are silently skipped.
    """
    baselines_cfg = config.get("baselines", {})
    tolerance_pct = float(baselines_cfg.get("tolerance_pct", 3.0))
    op_type_cfg   = baselines_cfg.get("operator_type", {})

    if not op_type_cfg:
        log.info("[batch] Q5a: No baselines configured — skipping file count check.")
        return []

    # Count files received today per operator × type
    with conn.cursor() as cur:
        cur.execute("""
            SELECT operator_id, cdr_type::text, COUNT(*) AS received_today
            FROM cdr_registry
            WHERE received_at >= CURRENT_DATE
            GROUP BY operator_id, cdr_type
        """)
        today_counts = {
            (row[0], row[1]): int(row[2])
            for row in cur.fetchall()
        }

    results = []

    for operator, types in op_type_cfg.items():
        if not isinstance(types, dict):
            continue
        for cdr_type, baseline in types.items():
            if baseline is None:
                log.info(
                    "[batch] Q5a: baseline not set for %s/%s — skipping.",
                    operator, cdr_type
                )
                continue

            baseline    = int(baseline)
            received    = today_counts.get((operator, cdr_type), 0)
            tol_1       = tolerance_pct
            tol_2       = tolerance_pct * 2
            lower_1     = baseline * (1 - tol_1 / 100)
            upper_1     = baseline * (1 + tol_1 / 100)
            lower_2     = baseline * (1 - tol_2 / 100)
            upper_2     = baseline * (1 + tol_2 / 100)

            # Within normal range — no anomaly
            if lower_1 <= received <= upper_1:
                continue

            # Zero files when we expect some → always CRITICAL
            if received == 0 and baseline > 0:
                severity   = "CRITICAL"
                delta_pct  = -100.0
                detail     = f"0 files received, expected ~{baseline}"
            elif received < lower_2 or received > upper_2:
                severity   = "CRITICAL"
                delta_pct  = round((received - baseline) / baseline * 100, 1)
                detail     = (f"{received} files received, "
                              f"expected {baseline} ± {tol_2:.0f}%")
            else:
                severity   = "WARNING"
                delta_pct  = round((received - baseline) / baseline * 100, 1)
                detail     = (f"{received} files received, "
                              f"expected {baseline} ± {tol_1:.0f}%")

            results.append({
                "operator_id": operator,
                "cdr_type":    cdr_type,
                "received":    received,
                "baseline":    baseline,
                "delta_pct":   delta_pct,
                "detail":      detail,
                "severity":    severity,
            })
            # No file_id — this is an operator-level anomaly, not file-level
            log_anomaly(conn, None, "FILE_MISSING", severity,
                        delta_value=delta_pct,
                        threshold_value=tolerance_pct,
                        dry_run=dry_run)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q5b: Pipeline funnel check
# ─────────────────────────────────────────────────────────────────────────────

def check_pipeline_funnel(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q5b: Verify that files flow correctly through each pipeline step today.

    For each operator × CDR type, we expect:
      received  = total files inserted into the registry today (any status)
      touched   = files Java has touched (PROCESSING / DONE / ERROR / MISMATCH)
      completed = files that reached a terminal state (DONE / ERROR / MISMATCH)

    Healthy funnel:
      received ≈ touched ≈ completed   (within tolerance)

    Anomaly patterns:
      received >> touched   → Java never picked up some files (watcher ok, Java not)
      touched  >> completed → Java started processing but never finished (crash, hang)
      completed < received  × (1 - 2×tol) → significant pipeline loss

    Severity:
      WARNING  → gap between received and completed within (tol, 2×tol)
      CRITICAL → gap exceeds 2×tol, or completed = 0 when received > 0

    Note: files still in PROCESSING are not anomalous if they arrived recently.
    We allow a grace period (stuck_file_hours) before counting them as lost.
    """
    baselines_cfg = config.get("baselines", {})
    tolerance_pct = float(baselines_cfg.get("tolerance_pct", 3.0))
    stuck_hours   = float(config.get("batch", {}).get("stuck_file_hours", 2))

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                operator_id,
                cdr_type::text,
                COUNT(*)                                                   AS received,
                SUM(CASE WHEN processing_status != 'PENDING' THEN 1
                         ELSE 0 END)                                       AS touched,
                SUM(CASE WHEN processing_status IN
                         ('DONE','ERROR','MISMATCH') THEN 1
                         ELSE 0 END)                                       AS completed,
                SUM(CASE WHEN processing_status = 'PENDING'
                         AND received_at < NOW() - INTERVAL '1 hour' * %s
                         THEN 1 ELSE 0 END)                                AS stuck_pending,
                SUM(CASE WHEN processing_status = 'PROCESSING'
                         AND received_at < NOW() - INTERVAL '1 hour' * %s
                         THEN 1 ELSE 0 END)                                AS stuck_processing
            FROM cdr_registry
            WHERE received_at >= CURRENT_DATE
            GROUP BY operator_id, cdr_type
            ORDER BY operator_id, cdr_type
        """, (stuck_hours, stuck_hours))
        rows = cur.fetchall()

    results = []

    for row in rows:
        (operator_id, cdr_type, received, touched,
         completed, stuck_pending, stuck_processing) = row

        received   = int(received)
        touched    = int(touched)
        completed  = int(completed)
        stuck_p    = int(stuck_pending)
        stuck_proc = int(stuck_processing)

        if received == 0:
            continue

        # Calculate what percentage of received files are not yet completed
        not_completed = received - completed
        gap_pct = not_completed / received * 100

        anomalies_found = []

        # Files Java never picked up (still PENDING past grace period)
        if stuck_p > 0:
            anomalies_found.append(
                f"{stuck_p} file(s) stuck PENDING > {stuck_hours:.0f}h"
            )

        # Files Java started but never finished
        if stuck_proc > 0:
            anomalies_found.append(
                f"{stuck_proc} file(s) stuck PROCESSING > {stuck_hours:.0f}h"
            )

        # Overall funnel gap check (only if not already covered by Q1)
        # We only flag here if gap exceeds tolerance AND is not already
        # accounted for by the stuck files above
        unexplained_gap = not_completed - stuck_p - stuck_proc
        if unexplained_gap > 0:
            unexplained_pct = unexplained_gap / received * 100
            if unexplained_pct > tolerance_pct:
                anomalies_found.append(
                    f"{unexplained_gap} file(s) unaccounted for in pipeline "
                    f"({unexplained_pct:.1f}%)"
                )

        if not anomalies_found:
            continue

        # Determine severity
        if stuck_p > 0 or stuck_proc > 0 or gap_pct > tolerance_pct * 2:
            severity = "CRITICAL"
        else:
            severity = "WARNING"

        results.append({
            "operator_id":     operator_id,
            "cdr_type":        cdr_type,
            "received":        received,
            "touched":         touched,
            "completed":       completed,
            "stuck_pending":   stuck_p,
            "stuck_processing": stuck_proc,
            "gap_pct":         round(gap_pct, 1),
            "detail":          " | ".join(anomalies_found),
            "severity":        severity,
        })
        log_anomaly(conn, None, "FILE_MISSING", severity,
                    delta_value=round(gap_pct, 2),
                    threshold_value=tolerance_pct,
                    dry_run=dry_run)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q5c: Files on disk not registered in DB
# ─────────────────────────────────────────────────────────────────────────────

def check_files_missing_from_db(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q5c: Files present on disk in the backup tree but NOT in cdr_registry.
    Catches files the watcher missed (was down, extension mismatch, etc.).
    This is the raw DCS → DB completeness check (invariant I1).
    """
    sources = config.get("watcher", {}).get("sources", [])
    results = []

    with conn.cursor() as cur:
        cur.execute("SELECT file_name FROM cdr_registry")
        registered = {row[0] for row in cur.fetchall()}

    for source in sources:
        directory  = source.get("directory", "")
        operator   = source.get("operator", "")
        cdr_type   = source.get("cdr_type", "")
        extensions = [e.lower() for e in source.get("extensions", [])]

        if not os.path.isdir(directory):
            continue

        for fname in os.listdir(directory):
            if not os.path.isfile(os.path.join(directory, fname)):
                continue
            lower   = fname.lower()
            matched = any(
                (ext == "" and "." not in fname) or
                (ext != "" and lower.endswith(ext))
                for ext in extensions
            )
            if not matched or fname in registered:
                continue

            results.append({
                "file_name":   fname,
                "operator_id": operator,
                "cdr_type":    cdr_type,
                "directory":   directory,
                "severity":    "CRITICAL",
            })
            log_anomaly(conn, None, "FILE_MISSING", "CRITICAL",
                        dry_run=dry_run)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Q6: Daily summary
# ─────────────────────────────────────────────────────────────────────────────

def get_daily_summary(conn) -> list[dict]:
    """Q6: Count of files per operator/type/status received today."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                operator_id, cdr_type,
                COUNT(*) AS total,
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
# Report builder
# ─────────────────────────────────────────────────────────────────────────────

def build_report(results: dict, config: dict) -> str:
    now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []

    def section(title):
        lines.append("")
        lines.append("=" * 72)
        lines.append(f"  {title}")
        lines.append("=" * 72)

    def ok(msg):   lines.append(f"  ✓  {msg}")
    def warn(msg): lines.append(f"  ⚠  {msg}")
    def crit(msg): lines.append(f"  ✗  {msg}")

    lines.append("=" * 72)
    lines.append("  TRAFSCAN : Reconciliation Batch Report")
    lines.append(f"  Generated : {now}")
    lines.append("=" * 72)

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
            crit(msg) if r["severity"] == "CRITICAL" else warn(msg)

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

    # Q3 — 5 dimensions
    threshold   = config.get("batch", {}).get("divergence_pct", 1.0)
    div_results = results.get("divergences", {})
    for dim_key, dim_title in [
        ("minutes",  "Q3a · Minutes divergence (DB vs ES)"),
        ("voice",    "Q3b · Voice traffic divergence (DB vs ES)"),
        ("sms",      "Q3c · SMS traffic divergence (DB vs ES)"),
        ("data",     "Q3d · Data traffic divergence (DB vs ES)"),
        ("recharge", "Q3e · Recharge divergence (DB vs ES) — in CDRs only"),
    ]:
        section(dim_title)
        dim_list = div_results.get(dim_key, [])
        if not dim_list:
            ok(f"No divergence above {threshold}% threshold.")
        else:
            for r in dim_list:
                msg = (f"{r['file_name']} | op={r['operator_id']} "
                       f"db={r['db_value']:.3f} es={r['es_value']:.3f} "
                       f"delta={r['delta_pct']:.2f}%")
                crit(msg) if r["severity"] == "CRITICAL" else warn(msg)

    # TODO FORFAITS: add Q3f here once forfait table names confirmed

    # Q4
    section("Q4 · Missing records per file (record_count_processed < expected)")
    missing = results.get("missing_records", [])
    if not missing:
        ok("No missing records detected.")
    else:
        for r in missing:
            msg = (f"{r['file_name']} | op={r['operator_id']} "
                   f"expected={r['expected']} processed={r['processed']} "
                   f"missing={r['missing']} ({r['missing_pct']:.2f}%)")
            crit(msg) if r["severity"] == "CRITICAL" else warn(msg)

    # Q5a
    tol    = config.get("baselines", {}).get("tolerance_pct", 3.0)
    section(f"Q5a · Daily file count vs baseline (tolerance ±{tol}%)")
    count_anomalies = results.get("count_anomalies", [])
    if not count_anomalies:
        ok("All operator/type file counts within expected range.")
    else:
        for r in count_anomalies:
            msg = (f"op={r['operator_id']} type={r['cdr_type']} — "
                   f"{r['detail']} (delta={r['delta_pct']:+.1f}%)")
            crit(msg) if r["severity"] == "CRITICAL" else warn(msg)

    # Q5b
    section("Q5b · Pipeline funnel (files flow through each step)")
    funnel_anomalies = results.get("funnel_anomalies", [])
    if not funnel_anomalies:
        ok("Pipeline funnel healthy for all operator/type combinations.")
    else:
        for r in funnel_anomalies:
            msg = (f"op={r['operator_id']} type={r['cdr_type']} | "
                   f"received={r['received']} completed={r['completed']} "
                   f"gap={r['gap_pct']}% | {r['detail']}")
            crit(msg) if r["severity"] == "CRITICAL" else warn(msg)

    # Q5c
    section("Q5c · Files on disk not registered in DB (watcher miss)")
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
        lines.append("  " + "-" * 60)
        for r in summary:
            lines.append(
                f"  {r['operator_id']:<10} {str(r['cdr_type']):<6} "
                f"{r['total']:>6} {r['done']:>6} {r['pending']:>8} "
                f"{r['mismatch']:>9} {r['error']:>6}"
            )

    # Totals
    section("Summary")
    all_div   = [i for lst in div_results.values() for i in lst]
    all_issues = (stuck + mismatches + all_div + missing +
                  count_anomalies + funnel_anomalies + unregistered)
    total_a   = len(all_issues)
    critical  = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    warnings  = total_a - critical

    if total_a == 0:
        ok("All checks passed. System healthy.")
    else:
        lines.append(f"  Total anomalies : {total_a}")
        lines.append(f"  Critical        : {critical}")
        lines.append(f"  Warnings        : {warnings}")
        if critical > 0:
            lines.append("")
            lines.append("  !! ACTION REQUIRED: Review CRITICAL anomalies above !!")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Trafscan Reconciliation Batch")
    parser.add_argument("--config",  default="config.yaml")
    parser.add_argument("--report",  default=None,
                        help="Path to save the text report (optional)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run checks and print report, no DB writes, no email")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with open(args.config) as f:
        config = yaml.safe_load(f)

    log.info("[batch] Starting reconciliation batch...")
    conn = get_conn(config)

    try:
        log.info("[batch] Q1:  Checking stuck files...")
        stuck = check_stuck_files(conn, config, args.dry_run)
        log.info("[batch] Q1:  %d stuck file(s).", len(stuck))

        log.info("[batch] Q2:  Checking checksum mismatches...")
        mismatches = check_checksum_mismatches(conn, args.dry_run)
        log.info("[batch] Q2:  %d mismatch(es).", len(mismatches))

        log.info("[batch] Q3:  Checking divergences (5 dimensions)...")
        divergences = check_all_divergences(conn, config, args.dry_run)
        total_div   = sum(len(v) for v in divergences.values())
        log.info("[batch] Q3:  %d divergence(s) across all dimensions.", total_div)

        log.info("[batch] Q4:  Checking missing records per file...")
        missing_records = check_missing_records(conn, args.dry_run)
        log.info("[batch] Q4:  %d file(s) with missing records.", len(missing_records))

        log.info("[batch] Q5a: Checking daily file count vs baselines...")
        count_anomalies = check_daily_file_count(conn, config, args.dry_run)
        log.info("[batch] Q5a: %d count anomaly(ies).", len(count_anomalies))

        log.info("[batch] Q5b: Checking pipeline funnel...")
        funnel_anomalies = check_pipeline_funnel(conn, config, args.dry_run)
        log.info("[batch] Q5b: %d funnel anomaly(ies).", len(funnel_anomalies))

        log.info("[batch] Q5c: Checking files on disk vs DB...")
        unregistered = check_files_missing_from_db(conn, config, args.dry_run)
        log.info("[batch] Q5c: %d unregistered file(s) on disk.", len(unregistered))

        log.info("[batch] Q6:  Building daily summary...")
        summary = get_daily_summary(conn)

    finally:
        conn.close()

    results = {
        "stuck":           stuck,
        "mismatches":      mismatches,
        "divergences":     divergences,
        "missing_records": missing_records,
        "count_anomalies": count_anomalies,
        "funnel_anomalies": funnel_anomalies,
        "unregistered":    unregistered,
        "summary":         summary,
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
    from alerting import (build_html_report, send_email_alert,
                           build_noc_payload, send_noc_notification)

    html_report = build_html_report(results, config)
    report_dir  = config.get("batch", {}).get("report_dir", "reports")
    date_str    = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path   = Path(report_dir) / f"report_{date_str}.html"
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

    all_div_flat = [i for lst in divergences.values() for i in lst]
    all_issues   = (stuck + mismatches + all_div_flat + missing_records +
                    count_anomalies + funnel_anomalies + unregistered)
    total = len(all_issues)
    log.info("[batch] Done. %d anomaly(ies) detected.", total)

    critical = any(r.get("severity") == "CRITICAL" for r in all_issues)
    sys.exit(1 if critical else 0)


if __name__ == "__main__":
    main()
    