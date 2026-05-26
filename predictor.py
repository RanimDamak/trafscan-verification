"""
predictor.py — Trafscan Solution A, Layer 5: Imputation & Prediction
======================================================================
Computes predicted CDR file volumes for the next 24 hours and writes
them into prediction_baseline. Designed to run every hour via cron/scheduler.

Algorithm (weighted average):
  predicted = 0.60 * weekly_baseline + 0.30 * recent_trend + 0.10 * hour_weight_adjustment

  - weekly_baseline : average of the same weekday + same hour across last 4 weeks
                      (from existing prediction_baseline rows, granularity='hourly')
  - recent_trend    : average of the same hour across the last 7 days
                      (from existing prediction_baseline rows, granularity='hourly')
  - hour_weight     : relative weight of this hour vs the daily average
                      (normalised from the HOUR_WEIGHTS distribution below)

Writes:
  - granularity='hourly'  : one row per (operator, cdr_type, hour) for next 24h
  - granularity='daily'   : one row per (operator, cdr_type, day) summing the 24 hourly values

Conflict policy: ON CONFLICT DO NOTHING
  Predictions are written once per slot. The slot is identified by the unique
  constraint on (operator_id, cdr_type, slot_start, granularity).
  To force a refresh, use --clear-future (deletes future unconfirmed rows then re-runs).

Usage:
  python predictor.py --config config.yaml
  python predictor.py --config config.yaml --dry-run
  python predictor.py --config config.yaml --clear-future
  python predictor.py --config config.yaml --hours-ahead 48

Cron example (run every hour at minute 5):
  5 * * * * cd /path/to/trafscan && python predictor.py --config config.yaml >> logs/predictor.log 2>&1
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import psycopg2
import psycopg2.extras
import yaml

# ---------------------------------------------------------------------------
# Hourly weight distribution
# Represents the relative likelihood of CDR files arriving in each hour.
# Must sum to 24.0 (so that daily total = sum of hourly predictions).
# Peaks at business hours (08-12, 14-18), low overnight.
# ---------------------------------------------------------------------------
HOUR_WEIGHTS = {
    0:  0.30,  1:  0.25,  2:  0.20,  3:  0.20,
    4:  0.25,  5:  0.30,  6:  0.50,  7:  0.80,
    8:  1.30,  9:  1.40, 10:  1.40, 11:  1.30,
    12: 1.10, 13: 1.00, 14: 1.30, 15: 1.40,
    16: 1.40, 17: 1.30, 18: 1.00, 19: 0.80,
    20: 0.70, 21: 0.60, 22: 0.50, 23: 0.40,
}

# Normalise weights so they sum to 24 (one per hour of the day)
_hw_sum = sum(HOUR_WEIGHTS.values())
HOUR_WEIGHTS = {h: v * 24.0 / _hw_sum for h, v in HOUR_WEIGHTS.items()}

MODEL_VERSION = "v1.0"
MIN_SAMPLES = 3   # minimum historical data points needed to make a prediction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def connect(cfg: dict) -> psycopg2.extensions.connection:
    db = cfg["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db.get("port", 5432),
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
    )


def get_operators(conn) -> list[tuple[str, str]]:
    """Return all (operator_id, cdr_type) pairs that have baseline data."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT operator_id, cdr_type
            FROM prediction_baseline
            ORDER BY operator_id, cdr_type
        """)
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Core prediction logic
# ---------------------------------------------------------------------------

def fetch_weekly_baseline(conn, operator_id: str, cdr_type: str,
                          target_weekday: int, target_hour: int,
                          reference_dt: datetime) -> list[float]:
    """
    Fetch predicted_files for the same weekday + hour over the last 4 weeks.
    Returns a list of float values (may be empty if no data).
    """
    # Look back 4 weeks = 28 days; search window ±30 min around exact hour
    lookback_start = reference_dt - timedelta(days=28)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT predicted_files
            FROM prediction_baseline
            WHERE operator_id = %s
              AND cdr_type = %s
              AND granularity = 'hourly'
              AND slot_start >= %s
              AND slot_start < %s
              AND EXTRACT(DOW FROM slot_start) = %s
              AND EXTRACT(HOUR FROM slot_start) = %s
            ORDER BY slot_start DESC
            LIMIT 4
        """, (operator_id, cdr_type, lookback_start, reference_dt,
              target_weekday, target_hour))
        rows = cur.fetchall()
    return [float(r[0]) for r in rows]


def fetch_recent_trend(conn, operator_id: str, cdr_type: str,
                       target_hour: int, reference_dt: datetime) -> list[float]:
    """
    Fetch predicted_files for the same hour over the last 7 days (any weekday).
    Returns a list of float values (may be empty if no data).
    """
    lookback_start = reference_dt - timedelta(days=7)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT predicted_files
            FROM prediction_baseline
            WHERE operator_id = %s
              AND cdr_type = %s
              AND granularity = 'hourly'
              AND slot_start >= %s
              AND slot_start < %s
              AND EXTRACT(HOUR FROM slot_start) = %s
            ORDER BY slot_start DESC
            LIMIT 7
        """, (operator_id, cdr_type, lookback_start, reference_dt, target_hour))
        rows = cur.fetchall()
    return [float(r[0]) for r in rows]


def compute_prediction(weekly_vals: list[float], trend_vals: list[float],
                       hour: int) -> tuple[float, float, int]:
    """
    Apply the weighted average formula.
    Returns (predicted_files, confidence_pct, baseline_samples).

    confidence_pct reflects data availability:
      - Full 4 weekly + 7 trend samples = 95%
      - Partial data degrades confidence proportionally
      - Below MIN_SAMPLES total: returns (None, None, 0) — caller should skip
    """
    n_weekly = len(weekly_vals)
    n_trend = len(trend_vals)
    total_samples = n_weekly + n_trend

    if total_samples < MIN_SAMPLES:
        return None, None, total_samples

    weekly_avg = sum(weekly_vals) / n_weekly if n_weekly > 0 else None
    trend_avg = sum(trend_vals) / n_trend if n_trend > 0 else None

    # Hour weight: normalised so that average hour weight = 1.0
    # (HOUR_WEIGHTS sums to 24, so dividing by 24/24 = 1 means weight IS the multiplier)
    hour_w = HOUR_WEIGHTS[hour]

    # Blend weights — adjust if one source is missing
    if weekly_avg is not None and trend_avg is not None:
        base = 0.60 * weekly_avg + 0.30 * trend_avg
        w_factor = 0.10
    elif weekly_avg is not None:
        base = weekly_avg
        w_factor = 0.10
    else:
        base = trend_avg
        w_factor = 0.10

    # Apply hour weight as a mild modulator around the base prediction
    # hour_w / 1.0 = relative to average hour; we scale the 10% portion
    avg_hour_w = 24.0 / 24  # = 1.0 by construction
    predicted = base * (1.0 - w_factor) + base * (hour_w / avg_hour_w) * w_factor
    predicted = max(0.0, round(predicted, 2))

    # Confidence: max 95%, scales with data completeness
    max_possible = 4 + 7  # 4 weekly + 7 trend
    confidence = round(min(95.0, (total_samples / max_possible) * 95.0), 1)

    return predicted, confidence, total_samples


def insert_hourly_prediction(cur, operator_id: str, cdr_type: str,
                             slot_start: datetime, predicted_files: float,
                             confidence_pct: float, baseline_samples: int,
                             dry_run: bool) -> bool:
    """Insert one hourly prediction row. Returns True if inserted (or would be)."""
    slot_end = slot_start + timedelta(hours=1)
    if dry_run:
        return True
    cur.execute("""
        INSERT INTO prediction_baseline
            (operator_id, cdr_type, slot_start, slot_end, granularity,
             predicted_files, confidence_pct, model_version, generated_at, baseline_samples)
        VALUES (%s, %s, %s, %s, 'hourly', %s, %s, %s, NOW(), %s)
        ON CONFLICT DO NOTHING
    """, (operator_id, cdr_type, slot_start, slot_end,
          predicted_files, confidence_pct, MODEL_VERSION, baseline_samples))
    return cur.rowcount > 0


def insert_daily_prediction(cur, operator_id: str, cdr_type: str,
                            day_start: datetime, total_predicted: float,
                            avg_confidence: float, total_samples: int,
                            dry_run: bool) -> bool:
    """Insert one daily prediction row (sum of 24 hourly). Returns True if inserted."""
    day_end = day_start + timedelta(days=1)
    if dry_run:
        return True
    cur.execute("""
        INSERT INTO prediction_baseline
            (operator_id, cdr_type, slot_start, slot_end, granularity,
             predicted_files, confidence_pct, model_version, generated_at, baseline_samples)
        VALUES (%s, %s, %s, %s, 'daily', %s, %s, %s, NOW(), %s)
        ON CONFLICT DO NOTHING
    """, (operator_id, cdr_type, day_start, day_end,
          round(total_predicted, 2), avg_confidence, MODEL_VERSION, total_samples))
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Clear-future helper
# ---------------------------------------------------------------------------

def clear_future_predictions(conn, reference_dt: datetime):
    """
    Delete future hourly/daily prediction rows that have not yet been confirmed
    by prediction_actuals. Used with --clear-future to force a re-run.
    """
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM prediction_baseline
            WHERE slot_start > %s
              AND granularity IN ('hourly', 'daily')
              AND model_version = %s
              AND prediction_id NOT IN (
                  SELECT DISTINCT prediction_id
                  FROM prediction_actuals
                  WHERE prediction_id IS NOT NULL
              )
        """, (reference_dt, MODEL_VERSION))
        deleted = cur.rowcount
    conn.commit()
    log.info(f"[clear-future] Deleted {deleted} unconfirmed future prediction rows.")


# ---------------------------------------------------------------------------
# Main prediction run
# ---------------------------------------------------------------------------

#def run_predictions(conn, hours_ahead: int, dry_run: bool) -> dict:

#for_testing
def run_predictions(conn, hours_ahead: int, dry_run: bool, reference_dt: datetime = None) -> dict:

    """
    For each (operator, cdr_type) pair, predict hourly volumes for the next
    `hours_ahead` hours, then roll up daily totals.
    Returns a summary dict for logging/reporting.
    """
    #now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    #for_testing
    now = reference_dt if reference_dt else datetime.now(timezone.utc)
    now = now.replace(minute=0, second=0, microsecond=0)

    operators = get_operators(conn)

    if not operators:
        log.warning("No (operator, cdr_type) pairs found in prediction_baseline. "
                    "Run seed.py first.")
        return {}

    log.info(f"Running predictions for {len(operators)} operator/type pairs, "
             f"{hours_ahead}h ahead from {now.strftime('%Y-%m-%d %H:%M')} UTC")

    summary = defaultdict(lambda: {"inserted": 0, "skipped": 0, "no_data": 0})

    # Group slots by day for daily rollup
    # Structure: day_buckets[operator_id][cdr_type][day_start] = [hourly_predictions]
    day_buckets = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    with conn.cursor() as cur:
        for operator_id, cdr_type in operators:
            key = f"{operator_id}/{cdr_type}"

            for h in range(hours_ahead):
                slot_start = now + timedelta(hours=h)
                weekday = slot_start.weekday()  # 0=Monday, 6=Sunday
                hour = slot_start.hour

                # Fetch historical data for this slot
                weekly_vals = fetch_weekly_baseline(
                    conn, operator_id, cdr_type, weekday, hour, slot_start)
                trend_vals = fetch_recent_trend(
                    conn, operator_id, cdr_type, hour, slot_start)

                predicted, confidence, n_samples = compute_prediction(
                    weekly_vals, trend_vals, hour)

                if predicted is None:
                    summary[key]["no_data"] += 1
                    log.debug(f"  {key} h+{h:02d} ({slot_start.strftime('%m-%d %Hh')}): "
                              f"insufficient data ({n_samples} samples)")
                    continue

                inserted = insert_hourly_prediction(
                    cur, operator_id, cdr_type, slot_start,
                    predicted, confidence, n_samples, dry_run)

                if inserted:
                    summary[key]["inserted"] += 1
                    day_buckets[operator_id][cdr_type][
                        slot_start.replace(hour=0, minute=0)
                    ].append((predicted, confidence, n_samples))
                else:
                    summary[key]["skipped"] += 1

    # Daily rollup — one row per (operator, cdr_type, day)
    with conn.cursor() as cur:
        for operator_id, types in day_buckets.items():
            for cdr_type, days in types.items():
                for day_start, hourly_preds in days.items():
                    if not hourly_preds:
                        continue
                    total_files = sum(p[0] for p in hourly_preds)
                    avg_conf = round(
                        sum(p[1] for p in hourly_preds) / len(hourly_preds), 1)
                    total_samples = max(p[2] for p in hourly_preds)
                    insert_daily_prediction(
                        cur, operator_id, cdr_type, day_start,
                        total_files, avg_conf, total_samples, dry_run)

    if not dry_run:
        conn.commit()

    return summary


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(summary: dict, dry_run: bool):
    if not summary:
        return

    header = "DRY RUN — " if dry_run else ""
    print(f"\n{'='*60}")
    print(f"  {header}Predictor Summary")
    print(f"{'='*60}")
    print(f"  {'Operator/Type':<30} {'Inserted':>9} {'Skipped':>9} {'No data':>9}")
    print(f"  {'-'*57}")

    total_ins = total_skip = total_nd = 0
    for key in sorted(summary):
        ins = summary[key]["inserted"]
        skip = summary[key]["skipped"]
        nd = summary[key]["no_data"]
        total_ins += ins
        total_skip += skip
        total_nd += nd
        skip_note = " (already exists)" if skip > 0 else ""
        nd_note = " (seed more data)" if nd > 0 else ""
        print(f"  {key:<30} {ins:>9} {skip:>9}{skip_note} {nd:>9}{nd_note}")

    print(f"  {'-'*57}")
    print(f"  {'TOTAL':<30} {total_ins:>9} {total_skip:>9} {total_nd:>9}")
    print(f"{'='*60}\n")

    if dry_run:
        print("  [DRY RUN] No rows were written to the database.")
    else:
        print(f"  {total_ins} hourly prediction rows inserted.")
        if total_skip > 0:
            print(f"  {total_skip} slots already had predictions (ON CONFLICT DO NOTHING).")
        if total_nd > 0:
            print(f"  {total_nd} slots skipped — not enough historical data yet "
                  f"(min_samples={MIN_SAMPLES}).")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Compute CDR volume predictions for the next N hours.")
    parser.add_argument("--config", default="config.yaml",
                        help="Path to config.yaml (default: config.yaml)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be inserted without touching the DB")
    parser.add_argument("--clear-future", action="store_true",
                        help="Delete unconfirmed future predictions then re-run")
    parser.add_argument("--hours-ahead", type=int, default=24,
                        help="How many hours ahead to predict (default: 24)")
    #for_testing
    parser.add_argument("--reference-date", default=None,
                        help="Override 'now' for testing, e.g. 2025-07-25 (YYYY-MM-DD)")
    args = parser.parse_args()

    # Load config
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        log.error(f"Config file not found: {args.config}")
        sys.exit(1)
    except yaml.YAMLError as e:
        log.error(f"Failed to parse config: {e}")
        sys.exit(1)

    # Connect
    try:
        conn = connect(cfg)
    except psycopg2.Error as e:
        log.error(f"DB connection failed: {e}")
        sys.exit(1)

    try:
        # Optional: clear unconfirmed future rows and re-predict
        if args.clear_future:
            now = datetime.now(timezone.utc)
            clear_future_predictions(conn, now)

        # Run predictions
        # summary = run_predictions(conn, args.hours_ahead, args.dry_run)

        # for_testing
        if args.reference_date:
            from datetime import timezone
            ref_dt = datetime.strptime(args.reference_date, "%Y-%m-%d").replace(
                hour=0, tzinfo=timezone.utc)
        else:
            ref_dt = None
        summary = run_predictions(conn, args.hours_ahead, args.dry_run, ref_dt)

        print_summary(summary, args.dry_run)

    except psycopg2.Error as e:
        log.error(f"DB error during prediction run: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()