"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A  |  seed.py
────────────────────────────────────────────────────────────────────────────
Reads CDRTextfiles/ and seeds prediction_baseline with real July 2025
historical volume data so predictor.py has a baseline to work from.

For each text file (one per day per operator/type):
  - Counts lines  → total files received that day
  - Distributes count across 24 hourly slots using realistic weights
  - INSERTs rows into prediction_baseline (granularity='hourly' and 'daily')

Does NOT touch cdr_registry. Keeps it clean for real watcher data only.

Usage
─────
    python seed.py --config config.yaml --textfiles-dir cdr_test\\CDRTextfiles
    python seed.py --config config.yaml --textfiles-dir cdr_test\CDRTextfiles --dry-run
    python seed.py --config config.yaml --clear-seed

Options
───────
  --dry-run     Print summary without inserting anything.
  --clear-seed  DELETE all rows inserted by a previous seed run, then exit.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import uuid
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
import yaml

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Folder → cdr_type mapping (case-insensitive)
# ─────────────────────────────────────────────────────────────────────────────
FOLDER_TYPE_MAP = {
    "in":     "in",
    "msc":    "msc",
    "pgw":    "pgw",
    "ggsn":   "pgw",
    "ccn":    "cnn",
    "cnn":    "cnn",
    "sdp":    "sdp",
    "air":    "air",
    "occ":    "occ",
    "surpay": "in",
}

# Realistic hourly distribution weights (index = hour 0-23)
HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 1,
    2, 3, 4, 5, 5, 5,
    5, 5, 5, 5, 5, 4,
    4, 3, 3, 2, 2, 1,
]
HOUR_WEIGHT_TOTAL = sum(HOUR_WEIGHTS)

MODEL_VERSION = "seed-july2025"


# ─────────────────────────────────────────────────────────────────────────────
# Folder walker
# ─────────────────────────────────────────────────────────────────────────────

def parse_date_from_filename(fname: str) -> Optional[date]:
    m = re.match(r'(\d{4}-\d{2}-\d{2})', fname)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def walk_textfiles(base_dir: str):
    """
    Yield (operator_id, cdr_type, target_date, file_count) for every
    text file found under CDRTextfiles/.
    """
    base = Path(base_dir)
    if not base.is_dir():
        log.error("CDRTextfiles directory not found: %s", base_dir)
        return

    for regulator_dir in sorted(base.iterdir()):
        if not regulator_dir.is_dir():
            continue
        for operator_dir in sorted(regulator_dir.iterdir()):
            if not operator_dir.is_dir():
                continue
            operator_id = operator_dir.name.lower()

            for type_dir in sorted(operator_dir.iterdir()):
                if not type_dir.is_dir():
                    continue
                cdr_type = FOLDER_TYPE_MAP.get(type_dir.name.lower())
                if cdr_type is None:
                    log.warning("Unknown CDR type folder '%s' — skipping", type_dir)
                    continue

                for txt_file in sorted(type_dir.glob("*.txt")):
                    target_date = parse_date_from_filename(txt_file.name)
                    if target_date is None:
                        log.warning("Cannot parse date from '%s' — skipping", txt_file)
                        continue
                    try:
                        lines = txt_file.read_text(
                            encoding="utf-8", errors="replace"
                        ).splitlines()
                    except Exception as exc:
                        log.error("Cannot read %s: %s", txt_file, exc)
                        continue

                    file_count = sum(1 for l in lines if l.strip())
                    if file_count > 0:
                        yield operator_id, cdr_type, target_date, file_count


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


def ensure_operators(conn, operator_ids: set) -> None:
    with conn.cursor() as cur:
        for op in sorted(operator_ids):
            cur.execute(
                """
                INSERT INTO operators (operator_id, operator_name)
                VALUES (%s, %s)
                ON CONFLICT (operator_id) DO NOTHING
                """,
                (op, op.capitalize())
            )
    conn.commit()
    log.info("[seed] Operators ensured: %s", sorted(operator_ids))


def clear_seed_data(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM prediction_baseline WHERE model_version = %s",
            (MODEL_VERSION,)
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def insert_batch(conn, rows: list) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO prediction_baseline (
            prediction_id,
            operator_id,
            cdr_type,
            slot_start,
            slot_end,
            granularity,
            predicted_files,
            predicted_records,
            predicted_bytes,
            confidence_pct,
            model_version,
            generated_at,
            baseline_samples
        ) VALUES (
            %(prediction_id)s,
            %(operator_id)s,
            %(cdr_type)s,
            %(slot_start)s,
            %(slot_end)s,
            %(granularity)s,
            %(predicted_files)s,
            NULL,
            NULL,
            NULL,
            %(model_version)s,
            NOW(),
            %(baseline_samples)s
        )
        ON CONFLICT (operator_id, cdr_type, slot_start, granularity) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Build prediction_baseline rows from a daily count
# ─────────────────────────────────────────────────────────────────────────────

def build_rows(operator_id: str, cdr_type: str,
               target_date: date, file_count: int) -> list:
    """
    For one (operator, type, day, count) entry, produce:
    - 24 hourly rows  (granularity='hourly')
    -  1 daily row    (granularity='daily')
    """
    rows = []
    day_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)

    # Hourly rows
    for hour in range(24):
        weight = HOUR_WEIGHTS[hour]
        hourly_count = round(file_count * weight / HOUR_WEIGHT_TOTAL, 2)
        slot_start = day_start + timedelta(hours=hour)
        slot_end   = slot_start + timedelta(hours=1)
        rows.append({
            "prediction_id":   str(uuid.uuid4()),
            "operator_id":     operator_id,
            "cdr_type":        cdr_type,
            "slot_start":      slot_start,
            "slot_end":        slot_end,
            "granularity":     "hourly",
            "predicted_files": hourly_count,
            "model_version":   MODEL_VERSION,
            "baseline_samples": 1,
        })

    # Daily row
    rows.append({
        "prediction_id":   str(uuid.uuid4()),
        "operator_id":     operator_id,
        "cdr_type":        cdr_type,
        "slot_start":      day_start,
        "slot_end":        day_start + timedelta(days=1),
        "granularity":     "daily",
        "predicted_files": float(file_count),
        "model_version":   MODEL_VERSION,
        "baseline_samples": 1,
    })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run_seed(config: dict, textfiles_dir: str, dry_run: bool) -> None:
    log.info("[seed] Scanning CDRTextfiles folder: %s", textfiles_dir)

    all_operators = set()
    all_entries = []

    for operator_id, cdr_type, target_date, file_count in walk_textfiles(textfiles_dir):
        all_operators.add(operator_id)
        all_entries.append((operator_id, cdr_type, target_date, file_count))

    if not all_entries:
        log.error("[seed] No data found in %s", textfiles_dir)
        return

    # Summary
    summary = defaultdict(lambda: defaultdict(lambda: {"days": 0, "total_files": 0}))
    for op, ctype, d, count in all_entries:
        summary[op][ctype]["days"] += 1
        summary[op][ctype]["total_files"] += count

    log.info("[seed] Found %d day-files across %d operators", len(all_entries), len(all_operators))
    log.info("[seed] Volume summary (will become prediction_baseline rows):")
    total_db_rows = 0
    for op in sorted(summary):
        for ctype, info in sorted(summary[op].items()):
            avg = info["total_files"] // info["days"]
            db_rows = info["days"] * 25  # 24 hourly + 1 daily per day
            total_db_rows += db_rows
            log.info(
                "  %-12s / %-6s : %d days | avg %d files/day | %d prediction_baseline rows",
                op, ctype, info["days"], avg, db_rows
            )

    log.info("[seed] Total prediction_baseline rows to insert: %s", f"{total_db_rows:,}")

    if dry_run:
        log.info("[seed] DRY-RUN: no DB writes. Exiting.")
        return

    conn = get_conn(config)
    ensure_operators(conn, all_operators)

    batch = []
    BATCH_SIZE = 1000
    total_inserted = 0

    for operator_id, cdr_type, target_date, file_count in all_entries:
        batch.extend(build_rows(operator_id, cdr_type, target_date, file_count))
        if len(batch) >= BATCH_SIZE:
            insert_batch(conn, batch)
            total_inserted += len(batch)
            batch = []
            if total_inserted % 5000 == 0:
                log.info("[seed] Inserted %s rows so far...", f"{total_inserted:,}")

    if batch:
        insert_batch(conn, batch)
        total_inserted += len(batch)

    conn.close()
    log.info("[seed] Done. %s rows inserted into prediction_baseline.", f"{total_inserted:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Seed prediction_baseline with July 2025 CDR history"
    )
    parser.add_argument("--config",        default="config.yaml")
    parser.add_argument("--textfiles-dir", default="cdr_test\\CDRTextfiles")
    parser.add_argument("--dry-run",       action="store_true")
    parser.add_argument("--clear-seed",    action="store_true",
                        help="Remove all seeded rows from prediction_baseline and exit")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    with open(args.config) as f:
        config = yaml.safe_load(f)

    if args.clear_seed:
        conn = get_conn(config)
        deleted = clear_seed_data(conn)
        conn.close()
        log.info("[seed] Cleared %s seeded rows from prediction_baseline.", f"{deleted:,}")
        sys.exit(0)

    run_seed(config, args.textfiles_dir, args.dry_run)


if __name__ == "__main__":
    main()