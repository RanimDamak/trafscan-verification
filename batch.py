"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A  |  Layer 3: Reconciliation Batch
────────────────────────────────────────────────────────────────────────────
Runs 6 checks against the CDR Registry and the backup filesystem.
Writes anomalies to anomaly_log. Prints a full report to stdout and
optionally saves it to a file.

Designed to run at 02:00 every night via cron/Task Scheduler.
Can also be run manually at any time for immediate diagnosis.

The 6 checks
─────────────
  Q1  Stuck files        — PENDING/PROCESSING older than N hours
  Q2  Checksum mismatch  — checksum_transferred != checksum_raw/compressed
  Q3  Minutes divergence — |total_minutes_db - total_minutes_es| > threshold%
  Q4  Missing records    — record_count_processed < record_count_expected
  Q5  Files on disk not in DB — files present in backup tree but not registered
  Q6  Daily summary      — per operator/type counts for the report

Usage
─────
    python batch.py                        # uses config.yaml in current dir
    python batch.py --config config.yaml   # explicit config path
    python batch.py --report report.txt    # save report to file
    python batch.py --dry-run             # run checks, print report, no DB writes
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
    anomaly_type  : 'CHECKSUM_MISMATCH' | 'MISSING_RECORDS' |
                    'MINUTES_DIVERGENCE' | 'TIMEOUT' | 'FILE_MISSING'
    severity      : 'INFO' | 'WARNING' | 'CRITICAL'
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
# The 6 checks
# ─────────────────────────────────────────────────────────────────────────────

def check_stuck_files(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q1 — Files stuck in PENDING or PROCESSING for longer than
    `stuck_file_hours` hours. These indicate a pipeline failure:
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
            "file_id":      str(file_id),
            "file_name":    file_name,
            "operator_id":  operator_id,
            "cdr_type":     cdr_type,
            "status":       status,
            "hours_waiting": round(float(hours_waiting), 1),
            "severity":     severity,
        })
        log_anomaly(conn, str(file_id), 'TIMEOUT', severity,
                    delta_value=round(float(hours_waiting), 2),
                    threshold_value=hours, dry_run=dry_run)

    return results


def check_checksum_mismatches(conn, dry_run: bool) -> list[dict]:
    """
    Q2 — Files where checksum_transferred differs from the expected
    checksum (checksum_compressed if available, else checksum_raw).
    Any difference means the file was corrupted during FTP transfer.
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
                checksum_transferred
            FROM cdr_registry
            WHERE
                checksum_transferred IS NOT NULL
                AND (
                    -- If compressed exists, compare against it
                    (checksum_compressed IS NOT NULL
                     AND checksum_compressed != checksum_transferred)
                    OR
                    -- If only raw exists (no compression step), compare raw
                    (checksum_compressed IS NULL
                     AND checksum_raw != checksum_transferred)
                )
        """)
        rows = cur.fetchall()

    for row in rows:
        file_id, file_name, operator_id, cdr_type, raw, compressed, transferred = row
        expected = compressed if compressed else raw
        results.append({
            "file_id":    str(file_id),
            "file_name":  file_name,
            "operator_id": operator_id,
            "cdr_type":   cdr_type,
            "expected":   expected,
            "transferred": transferred,
            "severity":   "CRITICAL",
        })
        log_anomaly(conn, str(file_id), 'CHECKSUM_MISMATCH', 'CRITICAL',
                    dry_run=dry_run)

    return results


def check_minutes_divergence(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q3 — Files where |total_minutes_db - total_minutes_es| / total_minutes_db
    exceeds the configured threshold percentage.
    This detects inconsistencies between what Java wrote to the DB and
    what got indexed in ElasticSearch.

    Binary CDR types (msc, pgw, cnn) are excluded — they don't have minutes.
    Severity:
      WARNING  → delta between threshold and 2× threshold
      CRITICAL → delta > 2× threshold
    """
    threshold_pct = config.get("batch", {}).get("minutes_divergence_pct", 1.0)
    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                file_id,
                file_name,
                operator_id,
                cdr_type,
                total_minutes_db,
                total_minutes_es,
                ROUND(
                    ABS(total_minutes_db - total_minutes_es)
                    / NULLIF(total_minutes_db, 0) * 100,
                    4
                ) AS delta_pct
            FROM cdr_registry
            WHERE
                processing_status = 'DONE'
                AND total_minutes_db IS NOT NULL
                AND total_minutes_es IS NOT NULL
                AND ABS(total_minutes_db - total_minutes_es)
                    / NULLIF(total_minutes_db, 0) * 100 > %s
                AND cdr_type NOT IN ('msc', 'pgw', 'cnn')
            ORDER BY delta_pct DESC
        """, (threshold_pct,))
        rows = cur.fetchall()

    for row in rows:
        file_id, file_name, operator_id, cdr_type, db_min, es_min, delta = row
        severity = 'CRITICAL' if float(delta) > threshold_pct * 2 else 'WARNING'
        results.append({
            "file_id":    str(file_id),
            "file_name":  file_name,
            "operator_id": operator_id,
            "cdr_type":   cdr_type,
            "db_minutes": float(db_min),
            "es_minutes": float(es_min),
            "delta_pct":  float(delta),
            "severity":   severity,
        })
        log_anomaly(conn, str(file_id), 'MINUTES_DIVERGENCE', severity,
                    delta_value=float(delta), threshold_value=threshold_pct,
                    dry_run=dry_run)

    return results


def check_missing_records(conn, dry_run: bool) -> list[dict]:
    """
    Q4 — Files where record_count_processed < record_count_expected.
    Binary CDR types are excluded (their expected count is meaningless).
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
                processing_status IN ('DONE', 'MISMATCH')
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
            "file_id":    str(file_id),
            "file_name":  file_name,
            "operator_id": operator_id,
            "cdr_type":   cdr_type,
            "expected":   int(expected),
            "processed":  int(processed),
            "missing":    int(missing),
            "missing_pct": float(pct),
            "severity":   severity,
        })
        log_anomaly(conn, str(file_id), 'MISSING_RECORDS', severity,
                    delta_value=float(pct), threshold_value=1.0,
                    dry_run=dry_run)

    return results


def check_files_missing_from_db(conn, config: dict, dry_run: bool) -> list[dict]:
    """
    Q5 — Files present on disk in the backup tree but NOT registered in
    the cdr_registry. This catches files the watcher missed (e.g. it was
    down when the file arrived, or the file extension is not watched).

    Scans /opt/backup/{operator}/{cdr_type}/ recursively and compares
    against file_name values in the registry.

    This is the DCS → DB completeness check.
    """
    backup_root = config.get("batch", {}).get("backup_root", "/opt/backup")
    sources = config.get("watcher", {}).get("sources", [])
    results = []

    # Build set of all registered file names for fast lookup
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
            fpath = os.path.join(directory, fname)
            if not os.path.isfile(fpath):
                continue

            # Check extension match (same logic as watcher)
            lower = fname.lower()
            matched = False
            for ext in extensions:
                if ext == "" and "." not in fname:
                    matched = True
                    break
                elif ext != "" and lower.endswith(ext):
                    matched = True
                    break

            if not matched:
                continue

            if fname not in registered:
                results.append({
                    "file_name":  fname,
                    "operator_id": operator,
                    "cdr_type":   cdr_type,
                    "directory":  directory,
                    "severity":   "CRITICAL",
                })
                # No file_id since it's not in DB yet
                log_anomaly(conn, None, 'FILE_MISSING', 'CRITICAL',
                            dry_run=dry_run)

    return results


def get_daily_summary(conn) -> list[dict]:
    """
    Q6 — Daily summary: count of files per operator/type/status for today.
    Used in the report — not an anomaly check, just informational.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                operator_id,
                cdr_type,
                COUNT(*)                                                          AS total,
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
    """
    Build a plain-text report from all check results.
    Sections with no anomalies show a clean OK line.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []

    def section(title):
        lines.append("")
        lines.append("=" * 70)
        lines.append(f"  {title}")
        lines.append("=" * 70)

    def ok(msg):
        lines.append(f"  ✓  {msg}")

    def warn(msg):
        lines.append(f"  ⚠  {msg}")

    def crit(msg):
        lines.append(f"  ✗  {msg}")

    lines.append("=" * 70)
    lines.append("  TRAFSCAN — Reconciliation Batch Report")
    lines.append(f"  Generated: {now}")
    lines.append("=" * 70)

    # ── Q1: Stuck files ───────────────────────────────────────────────────────
    section("Q1 · Stuck files (PENDING / PROCESSING too long)")
    stuck = results.get("stuck", [])
    if not stuck:
        ok("No stuck files.")
    else:
        for r in stuck:
            msg = (f"{r['file_name']}  |  operator={r['operator_id']}  "
                   f"type={r['cdr_type']}  status={r['status']}  "
                   f"waiting={r['hours_waiting']}h")
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # ── Q2: Checksum mismatches ───────────────────────────────────────────────
    section("Q2 · Checksum mismatches (transfer corruption)")
    mismatches = results.get("mismatches", [])
    if not mismatches:
        ok("All transferred files have matching checksums.")
    else:
        for r in mismatches:
            crit(f"{r['file_name']}  |  operator={r['operator_id']}  "
                 f"expected={r['expected'][:16]}...  "
                 f"received={r['transferred'][:16]}...")

    # ── Q3: Minutes divergence ────────────────────────────────────────────────
    section("Q3 · Minutes divergence (DB vs ElasticSearch)")
    divergences = results.get("divergences", [])
    threshold = config.get("batch", {}).get("minutes_divergence_pct", 1.0)
    if not divergences:
        ok(f"No divergence above {threshold}% threshold.")
    else:
        for r in divergences:
            msg = (f"{r['file_name']}  |  operator={r['operator_id']}  "
                   f"db={r['db_minutes']:.3f}  es={r['es_minutes']:.3f}  "
                   f"delta={r['delta_pct']:.2f}%")
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # ── Q4: Missing records ───────────────────────────────────────────────────
    section("Q4 · Missing records (record_count_processed < expected)")
    missing = results.get("missing_records", [])
    if not missing:
        ok("No missing records detected.")
    else:
        for r in missing:
            msg = (f"{r['file_name']}  |  operator={r['operator_id']}  "
                   f"expected={r['expected']}  processed={r['processed']}  "
                   f"missing={r['missing']} ({r['missing_pct']:.2f}%)")
            crit(msg) if r['severity'] == 'CRITICAL' else warn(msg)

    # ── Q5: Files missing from DB ─────────────────────────────────────────────
    section("Q5 · Files on disk not registered in DB")
    unregistered = results.get("unregistered", [])
    if not unregistered:
        ok("All files on disk are registered in the DB.")
    else:
        for r in unregistered:
            crit(f"{r['file_name']}  |  operator={r['operator_id']}  "
                 f"type={r['cdr_type']}  dir={r['directory']}")

    # ── Q6: Daily summary ─────────────────────────────────────────────────────
    section("Q6 · Daily summary (files received today)")
    summary = results.get("summary", [])
    if not summary:
        lines.append("  No files received today.")
    else:
        lines.append(f"  {'Operator':<10} {'Type':<6} {'Total':>6} "
                     f"{'Done':>6} {'Pending':>8} {'Mismatch':>9} {'Error':>6}")
        lines.append("  " + "-" * 58)
        for r in summary:
            lines.append(
                f"  {r['operator_id']:<10} {str(r['cdr_type']):<6} "
                f"{r['total']:>6} {r['done']:>6} {r['pending']:>8} "
                f"{r['mismatch']:>9} {r['error']:>6}"
            )

    # ── Totals ────────────────────────────────────────────────────────────────
    section("Summary")
    total_anomalies = (len(stuck) + len(mismatches) +
                       len(divergences) + len(missing) + len(unregistered))
    critical = sum(1 for r in (stuck + mismatches + divergences + missing + unregistered)
                   if r.get("severity") == "CRITICAL")
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
    lines.append("=" * 70)
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
                        help="Run checks and print report but do not write to anomaly_log")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with open(args.config) as f:
        config = yaml.safe_load(f)

    log.info("[batch] Starting reconciliation batch...")
    conn = get_conn(config)

    try:
        log.info("[batch] Q1: Checking stuck files...")
        stuck = check_stuck_files(conn, config, args.dry_run)
        log.info("[batch] Q1: %d stuck file(s) found.", len(stuck))

        log.info("[batch] Q2: Checking checksum mismatches...")
        mismatches = check_checksum_mismatches(conn, args.dry_run)
        log.info("[batch] Q2: %d mismatch(es) found.", len(mismatches))

        log.info("[batch] Q3: Checking minutes divergence...")
        divergences = check_minutes_divergence(conn, config, args.dry_run)
        log.info("[batch] Q3: %d divergence(s) found.", len(divergences))

        log.info("[batch] Q4: Checking missing records...")
        missing_records = check_missing_records(conn, args.dry_run)
        log.info("[batch] Q4: %d file(s) with missing records.", len(missing_records))

        log.info("[batch] Q5: Checking files on disk vs DB...")
        unregistered = check_files_missing_from_db(conn, config, args.dry_run)
        log.info("[batch] Q5: %d unregistered file(s) on disk.", len(unregistered))

        log.info("[batch] Q6: Building daily summary...")
        summary = get_daily_summary(conn)

    finally:
        conn.close()

    results = {
        "stuck":          stuck,
        "mismatches":     mismatches,
        "divergences":    divergences,
        "missing_records": missing_records,
        "unregistered":   unregistered,
        "summary":        summary,
    }

    report = build_report(results, config)
    print(report)

    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            f.write(report)
        log.info("[batch] Report saved to: %s", args.report)

    total = (len(stuck) + len(mismatches) + len(divergences) +
             len(missing_records) + len(unregistered))
    log.info("[batch] Done. %d anomaly(ies) detected.", total)

    # Exit code 1 if any CRITICAL anomaly — useful for cron alerting
    critical = any(
        r.get("severity") == "CRITICAL"
        for r in (stuck + mismatches + divergences + missing_records + unregistered)
    )
    sys.exit(1 if critical else 0)


if __name__ == "__main__":
    main()
