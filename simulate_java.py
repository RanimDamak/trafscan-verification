"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A  |  Java Hooks Simulator
────────────────────────────────────────────────────────────────────────────
This script simulates what the instrumented Java code does.
Use it for testing when you don't yet have access to the Java codebase.

In production, these SQL calls are made directly by Java via JDBC.
The SQL templates to give the Java team are in hasher.py (sql_hook_*).

Usage:
    # Simulate successful processing
    python simulate_java.py --file cbs_cdr_vou_20250401_601_101_528010.add.gz --records 186 --minutes 1250.5

    # Simulate processing error
    python simulate_java.py --file cbs_cdr_vou_20250401_601_101_528010.add.gz --error "Parser timeout at line 150"

    # Simulate missing records (triggers MISMATCH)
    python simulate_java.py --file cbs_cdr_vou_20250401_601_101_528010.add.gz --records 150 --minutes 1100.0

    # Simulate ES minutes update
    python simulate_java.py --file cbs_cdr_vou_20250401_601_101_528010.add.gz --es-minutes 1251.2
"""

from __future__ import annotations

import argparse
import time
import sys
import yaml
import psycopg2


def get_conn(config: dict):
    db = config.get("database", {})
    return psycopg2.connect(
        host=db.get("host", "localhost"),
        port=int(db.get("port", 5432)),
        dbname=db.get("dbname", "trafscan"),
        user=db.get("user", "trafscan_user"),
        password=db.get("password", ""),
    )


def simulate_processing(conn, file_name: str, records: int, minutes: float):
    """
    Simulate Java processing a CDR file successfully.
    Step 1: Set PROCESSING + processing_started_at
    Step 2: Wait a moment (simulating real processing time)
    Step 3: Set DONE (or MISMATCH) + record_count_processed + total_minutes_db
    """
    with conn.cursor() as cur:

        # ── Step 1: Java calls this at the start of its processing loop ───────
        print(f"[java-sim] START processing: {file_name}")
        cur.execute("""
            UPDATE cdr_registry
            SET processing_status     = 'PROCESSING'::processing_status,
                processing_started_at = NOW(),
                updated_at            = NOW()
            WHERE file_name = %s
        """, (file_name,))
        conn.commit()

        if cur.rowcount == 0:
            print(f"[java-sim] ERROR: file not found in registry: {file_name}")
            print("           Make sure the watcher has already processed this file.")
            return

        print(f"[java-sim] Status → PROCESSING")

        # ── Simulate processing time ──────────────────────────────────────────
        print(f"[java-sim] Simulating CDR parsing... (2 second delay)")
        time.sleep(2)

        # ── Step 2: Java calls this at the end of its processing loop ─────────
        # The CASE statement automatically sets MISMATCH if counts differ.
        cur.execute("""
            UPDATE cdr_registry
            SET processing_status      = CASE
                                            WHEN %s < record_count_expected
                                            THEN 'MISMATCH'::processing_status
                                            ELSE 'DONE'::processing_status
                                         END,
                processing_ended_at    = NOW(),
                record_count_processed = %s,
                total_minutes_db       = %s,
                updated_at             = NOW()
            WHERE file_name = %s
        """, (records, records, minutes, file_name))
        conn.commit()

        # Check what status was set
        cur.execute("""
            SELECT processing_status, record_count_expected, record_count_processed,
                   total_minutes_db
            FROM cdr_registry WHERE file_name = %s
        """, (file_name,))
        row = cur.fetchone()
        status, expected, processed, mins = row

        print(f"[java-sim] Status → {status}")
        print(f"[java-sim] Records: expected={expected}  processed={processed}")
        print(f"[java-sim] Minutes DB: {mins}")
        if status == 'MISMATCH':
            print(f"[java-sim] ⚠ MISMATCH: {expected - processed} records missing!")


def simulate_error(conn, file_name: str, error_msg: str):
    """
    Simulate Java crashing during CDR processing.
    This is what goes in the Java catch block.
    """
    with conn.cursor() as cur:
        print(f"[java-sim] START processing: {file_name}")
        cur.execute("""
            UPDATE cdr_registry
            SET processing_status     = 'PROCESSING'::processing_status,
                processing_started_at = NOW(),
                updated_at            = NOW()
            WHERE file_name = %s
        """, (file_name,))
        conn.commit()

        if cur.rowcount == 0:
            print(f"[java-sim] ERROR: file not found in registry: {file_name}")
            return

        time.sleep(1)
        print(f"[java-sim] Simulating Java crash...")

        cur.execute("""
            UPDATE cdr_registry
            SET processing_status = 'ERROR'::processing_status,
                notes             = COALESCE(notes || ' | ', '') || %s,
                updated_at        = NOW()
            WHERE file_name = %s
        """, (f"Java error: {error_msg}", file_name))
        conn.commit()
        print(f"[java-sim] Status → ERROR")
        print(f"[java-sim] Note: {error_msg}")


def simulate_es_minutes(conn, file_name: str, es_minutes: float):
    """
    Simulate the ES minutes update.
    In production: Java calls this after indexing into ElasticSearch,
    OR a Python script queries ES and does this update.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE cdr_registry
            SET total_minutes_es = %s,
                updated_at       = NOW()
            WHERE file_name = %s
        """, (es_minutes, file_name))
        conn.commit()

        if cur.rowcount == 0:
            print(f"[java-sim] ERROR: file not found: {file_name}")
            return

        # Check divergence
        cur.execute("""
            SELECT total_minutes_db, total_minutes_es,
                   ROUND(
                       ABS(total_minutes_db - %s)
                       / NULLIF(total_minutes_db, 0) * 100,
                       2
                   ) AS delta_pct
            FROM cdr_registry WHERE file_name = %s
        """, (es_minutes, file_name))
        row = cur.fetchone()
        db_mins, es_mins, delta = row

        print(f"[java-sim] ES minutes updated: {es_minutes}")
        print(f"[java-sim] DB minutes: {db_mins}  |  ES minutes: {es_mins}  |  delta: {delta}%")
        if delta and delta > 1.0:
            print(f"[java-sim] ⚠ DIVERGENCE > 1%: batch will flag this file!")


def main():
    parser = argparse.ArgumentParser(description="Trafscan Java Hooks Simulator")
    parser.add_argument("--config",     default="config.yaml")
    parser.add_argument("--file",       required=True, help="file_name as stored in cdr_registry")
    parser.add_argument("--records",    type=int,   help="record_count_processed (simulate DONE)")
    parser.add_argument("--minutes",    type=float, help="total_minutes_db")
    parser.add_argument("--error",      type=str,   help="error message (simulate ERROR status)")
    parser.add_argument("--es-minutes", type=float, help="total_minutes_es (simulate ES update)")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    conn = get_conn(config)

    try:
        if args.error:
            simulate_error(conn, args.file, args.error)
        elif args.es_minutes is not None:
            simulate_es_minutes(conn, args.file, args.es_minutes)
        elif args.records is not None and args.minutes is not None:
            simulate_processing(conn, args.file, args.records, args.minutes)
        else:
            print("ERROR: provide either --records + --minutes, --error, or --es-minutes")
            sys.exit(1)
    finally:
        conn.close()

    print("\n[java-sim] Done. Check registry:")
    print(f'  psql -U trafscan_user -d trafscan -h localhost -c "SELECT file_name, processing_status, record_count_expected, record_count_processed, total_minutes_db, total_minutes_es FROM cdr_registry WHERE file_name = \'{args.file}\';"')


if __name__ == "__main__":
    main()
