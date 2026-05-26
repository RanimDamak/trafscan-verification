# TRAFSCAN VERIFICATION - Solution A: CDR Registry

## Integration Guide for Trafscan

**Version 3.1 - May 2026**

---

## What this is

This system adds a **CDR Registry** layer to Trafscan. Every CDR file that
arrives on the frontale is automatically tracked from the moment it lands on
disk until it is processed by Java and indexed in ElasticSearch.

The registry answers these questions in real time with a single SQL query:
- Did all CDR files arrive and get processed?
- Did Java and ElasticSearch produce the same values (minutes, voice, SMS, data, recharge)?
- Was any file corrupted during FTP transfer?
- Are there any records lost between reception and processing?
- Is processing taking abnormally long compared to recent history?
- Are forfait counts collapsing unexpectedly?

It runs alongside the existing Trafscan system. It does not modify any
existing code - it only **adds** SQL calls at key points in the Java pipeline.

---

## Getting started

```cmd
# 1. Clone the repo
git clone https://github.com/RanimDamak/trafscan-verification.git
cd trafscan-verification

# 2. Install dependencies
pip install psycopg2-binary pyyaml watchdog

# 3. Create your local config from the template
copy config.example.yaml config.yaml   # Windows
# cp config.example.yaml config.yaml   # Linux/Mac

# 4. Fill in config.yaml: database credentials, SMTP, watcher paths
#    (config.yaml is gitignored - never committed)

# 5. Apply the schema to the Trafscan PostgreSQL database
#    (must be the same DB as the Trafscan application)
psql -h localhost -p 5432 -U trafscan_verify -d trafscan -f 01_schema.sql
psql -h localhost -p 5432 -U trafscan_verify -d trafscan -f 02_schema.sql

# 6. Test the HTML report template (no DB needed)
python alerting.py --config config.yaml --test

# 7. Run the batch in dry-run mode (checks only, no DB writes, no email)
python batch.py --dry-run

# 8. Start the file watcher (runs continuously)
python -m trafscan --config config.yaml
```

---


## Critical: Database placement

> **The CDR Registry tables must be in the SAME PostgreSQL database as the
> existing Trafscan application.**

**Why:** The tables `daily_state_forfait` and `daily_state_unknown_forfait`
(used by check Q7) already exist in the Trafscan DB. If the registry were in a
separate DB, Q7 would need a second connection and a cross-DB query, which
PostgreSQL does not support natively. The Java hooks also benefit from reusing
the existing JDBC connection pool - no new datasource to configure.

**In practice:** same `host`, same `dbname` in `config.yaml`. Use a dedicated
PostgreSQL user `trafscan_verify` with INSERT/UPDATE/SELECT on `cdr_registry`,
`anomaly_log`, `operators`, and SELECT on `daily_state_forfait`,
`daily_state_unknown_forfait`.

> **Note:** An earlier version of this README said "The registry DB is separate
> from the main Trafscan application DB." That was incorrect and has been fixed.

---

## Architecture overview

```
DCS servers (one per operator)
    │
    │  files land via FTP/SFTP
    ▼
Frontale: /opt/backup/{operator}/{cdr_type}/
    │
    │  Python File Watcher detects new files
    ▼
Layer 1: File Watcher + Hasher (Python)
    - Detects new CDR files instantly (inotify/watchdog)
    - Waits for file to be fully written (size stability check)
    - Computes SHA-256 checksum of raw file
    - Counts CDR records (lines) for text formats
    - Inserts row into cdr_registry with status PENDING
    │
    ▼
Layer 2: CDR Registry (PostgreSQL - same DB as Trafscan app)
    - Table: cdr_registry    ← source of truth for every file
    - Table: anomaly_log     ← all detected anomalies
    - Table: operators       ← operator reference
    - Tables used by Q7 (already in Trafscan DB, no new tables needed):
        daily_state_forfait, daily_state_unknown_forfait
    │
    ▼
Layer 2 (Java side): Instrumented Java hooks
    - Hook 1: update status to PROCESSING at start
    - Hook 2: update status to DONE + all traffic values at end
    - Hook 3: update status to ERROR on exception
    - Hook 4: store ES values after ElasticSearch indexing
    │
    ▼
Layer 3: Reconciliation Batch (Python, runs at 02:00)
    - 13 checks (Q1, Q2, Q3a–e, Q4, Q5a, Q5b, Q5c, Q6, Q_PT, Q7)
    - Writes to anomaly_log
    - Generates HTML report + NOC JSON payload
    │
    ▼
Layer 4: Alerting + Reporting
    - Email alert (SMTP - requires configuration, see below)
    - HTML daily report saved to reports/
    - noc_latest.json for frontend / NOC panel integration
```

---

## Folder structure on the frontale

Files must be organized exactly as follows for the operator and CDR type
to be detected automatically:

```
/opt/backup/
    orange/
        in/       ← IN CDRs (recharge, voucher - text format)
        msc/      ← MSC CDRs (voice - binary Ericsson format)
        pgw/      ← PGW/GGSN CDRs (data - binary IPDR format)
        cnn/      ← CNN/CCN CDRs (binary ASN.1/BER format)
        sdp/      ← SDP CDRs (ASN, ADJ formats)
        air/      ← AIR CDRs (text format)
        occ/      ← OCC CDRs
    atel/
        in/
        msc/
        ...
    moov/
        in/
        msc/
        ...
```

The operator name and CDR type come from the **folder path**, never from
the filename. This is the authoritative source.

---

## All checks (v3.2)

| Check | What it detects | Severity |
|-------|-----------------|----------|
| **Q1** | Files stuck PENDING/PROCESSING > 2h | WARNING / CRITICAL |
| **Q2** | File corruption during FTP transfer (checksum mismatch) | CRITICAL |
| **Q3a** | `total_minutes_db` vs `total_minutes_es` > threshold% | WARNING / CRITICAL |
| **Q3b** | `voice_db` vs `voice_es` > threshold% | WARNING / CRITICAL |
| **Q3c** | `sms_db` vs `sms_es` > threshold% | WARNING / CRITICAL |
| **Q3d** | `data_db` vs `data_es` > threshold% | WARNING / CRITICAL |
| **Q3e** | `recharge_db` vs `recharge_es` > threshold% | WARNING / CRITICAL |
| **Q4** | Java processed fewer records than expected | WARNING / CRITICAL |
| **Q5a** | Files received today vs configured baseline ± tolerance | WARNING / CRITICAL |
| **Q5b** | Pipeline funnel: files stuck PENDING or not completing | WARNING / CRITICAL |
| **Q5c** | Files on disk but absent from DB (watcher missed them) | CRITICAL |
| **Q6** | Daily summary per operator/type (informational) | INFO |
| **Q_PT** | Processing duration > N× 7-day rolling median per cdr_type | WARNING / CRITICAL |
| **Q7** | Daily forfait count vs 7-day median (daily_state_forfait + daily_state_unknown_forfait) | WARNING / CRITICAL |
| **Q8** | Hourly volume vs prediction - drop or surge vs expected files/records | WARNING / CRITICAL |
| **Q9** | Daily volume vs prediction - end-of-day completeness check | WARNING / CRITICAL |

---

## Database schema

### cdr_registry (main table)

| Column | Type | Description |
|--------|------|-------------|
| file_id | UUID | Primary key, auto-generated |
| file_name | VARCHAR(255) | Original filename, UNIQUE (basename only) |
| operator_id | VARCHAR(50) | Operator: orange / atel / moov |
| cdr_type | ENUM | in / msc / pgw / cnn / sdp / air / occ |
| received_at | TIMESTAMP | When file was detected by the watcher |
| processing_started_at | TIMESTAMP | Set by Java Hook 1 (requires hook integration) |
| processing_ended_at | TIMESTAMP | Set by Java Hook 2/3 (requires hook integration) |
| updated_at | TIMESTAMP | Auto-updated on every row change (trigger-managed) |
| checksum_raw | CHAR(64) | SHA-256 of the original file on disk |
| checksum_compressed | CHAR(64) | SHA-256 after gzip compression (if applicable) |
| checksum_transferred | CHAR(64) | SHA-256 verified at regulator after FTP |
| record_count_expected | BIGINT | Lines counted by Python at reception |
| record_count_processed | BIGINT | Records actually parsed by Java |
| total_minutes_db | DECIMAL(18,3) | Minutes calculated by Java → DB (Hook 2) |
| total_minutes_es | DECIMAL(18,3) | Minutes indexed in ElasticSearch (Hook 4) |
| voice_db | DECIMAL(18,3) | Voice traffic balance from Java → DB (Hook 2) |
| voice_es | DECIMAL(18,3) | Voice traffic balance from ElasticSearch (Hook 4) |
| sms_db | DECIMAL(18,3) | SMS traffic balance from Java → DB (Hook 2) |
| sms_es | DECIMAL(18,3) | SMS traffic balance from ElasticSearch (Hook 4) |
| data_db | DECIMAL(18,3) | Data traffic balance from Java → DB (Hook 2) |
| data_es | DECIMAL(18,3) | Data traffic balance from ElasticSearch (Hook 4) |
| recharge_db | DECIMAL(18,3) | Recharge balance from Java → DB (Hook 2) - in CDRs only |
| recharge_es | DECIMAL(18,3) | Recharge balance from ElasticSearch (Hook 4) |
| processing_status | ENUM | PENDING / PROCESSING / DONE / ERROR / MISMATCH |
| archive_path | VARCHAR(512) | Path to warm/cold archive (NULL if not archived yet) |
| notes | TEXT | Error messages, flags, free-form notes |
| is_pre_compressed | BOOLEAN | True if file arrived already compressed (.gz) - record count is decompressed line count |
| file_size_bytes | BIGINT | File size in bytes at reception (optional, populated by hasher) |

**Source mapping (Trafscan DB → registry columns):**
- `voice_db` ← `AbonneTable.voice_counted_balance` aggregated per file
- `sms_db` ← `AbonneTable.sms_counted_balance` aggregated per file
- `data_db` ← `AbonneTable.data_counted_balance` aggregated per file
- `recharge_db` ← `AbonneTable.recharge_balance` aggregated per file

### processing_status lifecycle

```
File arrives on disk
      │
      ▼
   PENDING          ← inserted by Python watcher immediately
      │
      ▼ (Java picks up the file)
  PROCESSING        ← Java calls Hook 1 at start of processing
      │
      ├──► DONE     ← Java calls Hook 2 at end, record counts match
      ├──► MISMATCH ← Java Hook 2: record_count_processed < expected
      │              OR checksum_transferred != checksum_compressed
      └──► ERROR    ← Java Hook 3 (catch block) with error message
```

---

## What the Java team needs to add

### Overview

You need to add **4 SQL calls** to the existing CDR processing code.
These are standard JDBC PreparedStatement calls against the Trafscan
PostgreSQL database.

Use the **same connection pool** as the existing Trafscan application -
same host, same dbname. No new datasource is needed.

### JDBC connection

```java
// Reuse your existing Trafscan DB connection pool - same host, same dbname.
Connection trafscanConn = /* your existing pool */ ;
```

### Hook 1 - Before processing starts

Call immediately before your CDR parsing loop begins.
`fileName` is the basename of the file (e.g. `CDR_20250401_atel.gz`).

```java
String sqlStart = """
    UPDATE cdr_registry
    SET processing_status     = 'PROCESSING'::processing_status,
        processing_started_at = NOW(),
        updated_at            = NOW()
    WHERE file_name = ?
""";

try (PreparedStatement ps = trafscanConn.prepareStatement(sqlStart)) {
    ps.setString(1, fileName);
    int updated = ps.executeUpdate();
    trafscanConn.commit();
    if (updated == 0) {
        logger.warn("TRAFSCAN: file not in registry: " + fileName);
        // Do NOT block processing - log and continue
    }
}
```

### Hook 2 - After processing succeeds

Call after your CDR parsing loop completes successfully. Include all
traffic aggregates computed during parsing.

```java
String sqlDone = """
    UPDATE cdr_registry
    SET processing_status      = CASE
                                    WHEN ? < record_count_expected
                                    THEN 'MISMATCH'::processing_status
                                    ELSE 'DONE'::processing_status
                                 END,
        processing_ended_at    = NOW(),
        record_count_processed = ?,
        total_minutes_db       = ?,
        voice_db               = ?,
        sms_db                 = ?,
        data_db                = ?,
        recharge_db            = ?,
        updated_at             = NOW()
    WHERE file_name = ?
""";

try (PreparedStatement ps = trafscanConn.prepareStatement(sqlDone)) {
    ps.setLong(1,   recordCount);       // for CASE comparison
    ps.setLong(2,   recordCount);       // record_count_processed
    ps.setDouble(3, totalMinutesDb);
    ps.setDouble(4, voiceBalance);      // AbonneTable.voice_counted_balance aggregate
    ps.setDouble(5, smsBalance);        // AbonneTable.sms_counted_balance aggregate
    ps.setDouble(6, dataBalance);       // AbonneTable.data_counted_balance aggregate
    ps.setDouble(7, rechargeBalance);   // AbonneTable.recharge_balance aggregate (in CDRs)
    ps.setString(8, fileName);
    ps.executeUpdate();
    trafscanConn.commit();
}
```

> **Note:** For CDR types where a value is not applicable (e.g. `recharge_db`
> for MSC voice files, or `data_db` for IN recharge files), pass `null`:
> `ps.setNull(7, Types.DECIMAL)`. All batch checks handle NULL gracefully.

### Hook 3 - In the catch block (Java error)

Call inside your existing catch block.

```java
String sqlError = """
    UPDATE cdr_registry
    SET processing_status = 'ERROR'::processing_status,
        notes             = COALESCE(notes || ' | ', '') || ?,
        updated_at        = NOW()
    WHERE file_name = ?
""";

try (PreparedStatement ps = trafscanConn.prepareStatement(sqlError)) {
    ps.setString(1, "Java error: " + exception.getMessage());
    ps.setString(2, fileName);
    ps.executeUpdate();
    trafscanConn.commit();
} catch (SQLException sqlEx) {
    logger.error("TRAFSCAN: failed to update error status: " + sqlEx.getMessage());
    // Never let registry updates block your main error handling
}
```

### Hook 4 - After ElasticSearch indexing

Call after indexing the file's data in ES. Pass the same traffic values
as reported by ES so the divergence check is meaningful.

```java
String sqlEs = """
    UPDATE cdr_registry
    SET total_minutes_es = ?,
        voice_es         = ?,
        sms_es           = ?,
        data_es          = ?,
        recharge_es      = ?,
        updated_at       = NOW()
    WHERE file_name = ?
""";

try (PreparedStatement ps = trafscanConn.prepareStatement(sqlEs)) {
    ps.setDouble(1, totalMinutesEs);
    ps.setDouble(2, voiceEs);
    ps.setDouble(3, smsEs);
    ps.setDouble(4, dataEs);
    ps.setDouble(5, rechargeEs);
    ps.setString(6, fileName);
    ps.executeUpdate();
    trafscanConn.commit();
}
```

### Where exactly in your code

```
YourCDRProcessor.process(file):
    fileName = file.getName()              ← basename only

    [Hook 1] → status = PROCESSING, processing_started_at

    try:
        recordCount     = 0
        totalMinutes    = 0.0
        voiceBalance    = 0.0
        smsBalance      = 0.0
        dataBalance     = 0.0
        rechargeBalance = 0.0

        for each record in file:
            parse(record)
            recordCount++
            totalMinutes    += record.getDuration()
            voiceBalance    += record.getVoiceBalance()
            smsBalance      += record.getSmsBalance()
            dataBalance     += record.getDataBalance()
            rechargeBalance += record.getRechargeBalance()

        indexInElasticSearch(records)

        [Hook 4] → total_minutes_es, voice_es, sms_es, data_es, recharge_es

        [Hook 2] → status = DONE or MISMATCH
                 → record_count_processed, total_minutes_db,
                   voice_db, sms_db, data_db, recharge_db

    catch Exception e:
        [Hook 3] → status = ERROR
        throw e   ← re-throw, do not swallow
```

---

## Q5a - Daily file count vs baseline

For each operator × CDR type, compares today's received file count against
a configured expected baseline. This detects days where significantly fewer
(or more) files arrived than normal - something the watcher alone cannot catch
because it only reacts to files that actually land.

**Severity:**
- WARNING → count outside baseline ± `baseline_tolerance_pct` (default 20%)
- CRITICAL → count outside 2× `baseline_tolerance_pct`, or zero files received when baseline > 0

**Configuration in `config.yaml`:**

```yaml
batch:
  file_count_baseline:
    orange/in:  4        # ← approximate files/day - fill in once team provides counts
    orange/msc: 24
    orange/pgw: 0        # ← 0 = disabled for this combination (no alert)
    atel/in:    4
    atel/msc:   24
    moov/in:    4
    moov/msc:   24
  baseline_tolerance_pct: 20
```

Leave any value as `0` until the team provides the approximate daily count
for that combination. The batch will skip it - no false alerts during the
initial learning phase.

---

## Q5b - Pipeline funnel check

For each operator × CDR type, verifies that files flow correctly through
every processing step today:

```
received (any status)
    └─► touched by Java (not PENDING)
            └─► completed (DONE / ERROR / MISMATCH)
```

Detects three types of gaps:
- Files stuck **PENDING** past the grace period → Java never picked them up
- Files stuck **PROCESSING** past the grace period → Java started but crashed or hung
- **Unexplained gap** between received and completed → something else failed silently

Uses the same `batch.stuck_file_hours` grace period as Q1 to avoid alerting
on files still legitimately in progress at batch time (02:00).

---

## Q_PT - Processing time check

**Activates** once ≥ 10 DONE files exist per `cdr_type` in the 7-day window
(configurable via `min_samples`). Until that threshold is reached it skips
silently - no false alerts during initial rollout.

**Fallback mode** (default, `use_java_timestamps: false`):
Uses `updated_at - received_at`. This includes queue wait time but is
immediately useful as a general indicator, and the team lead confirmed
it is an acceptable `repère`.

**Precise mode** (`use_java_timestamps: true`):
Uses `processing_ended_at - processing_started_at`. Switch this on in
`config.yaml` once Java Hooks 1+2 are confirmed live and populating those columns.

Thresholds (configurable in `config.yaml`):
- **WARNING**: duration > 3× 7-day median per cdr_type
- **CRITICAL**: duration > 5× 7-day median per cdr_type

---

## Q7 - Forfait daily count

Q7 checks that the daily total of forfait records does not collapse
unexpectedly. It uses two tables already in the Trafscan DB - no new
tables are needed:

```sql
-- Known forfaits (linked to a reference catalogue)
daily_state_forfait         (id, date, count, id_forfait)

-- Unknown forfaits (no matching catalogue entry)
daily_state_unknown_forfait (id, forfait VARCHAR, date, count)
```

**Total DB forfaits for a given date:**
```sql
SELECT
    COALESCE((SELECT SUM(count) FROM daily_state_forfait         WHERE date = $1), 0)
  + COALESCE((SELECT SUM(count) FROM daily_state_unknown_forfait WHERE date = $1), 0)
  AS total_db_forfaits;
```

**Thresholds:** configurable via `batch.forfait.drop_warning_pct` (default 30%)
and `drop_critical_pct` (default 70%). Zero when 7-day median > 0 → always CRITICAL.

**Part 2 - ES comparison (PENDING):**
> **TODO for Baha:** Does the ElasticSearch index expose a count of CDR records
> with `cdrType = 'forfait'` (or an equivalent field) per date?
> If yes, set `es_enabled: true` in `config.yaml` and fill in `es_index`,
> `es_field`, `es_value`. The Part 2 placeholder in `batch.py` will then
> be completed with one query. Until confirmed, only Part 1 (DB internal
> consistency) runs.

---

## Running the system

### Start the file watcher (runs continuously)

```bash
# On the frontale server
python -m trafscan --config /opt/trafscan/config.yaml
```

Systemd service for production:

```ini
[Unit]
Description=Trafscan CDR File Watcher
After=network.target postgresql.service

[Service]
ExecStart=/usr/bin/python3 -m trafscan --config /opt/trafscan/config.yaml
WorkingDirectory=/opt/trafscan
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Run the nightly batch (02:00 every night)

```bash
# Linux - add to crontab
0 2 * * * cd /opt/trafscan && python batch.py --report /var/trafscan/reports/$(date +\%Y\%m\%d).txt
```

Windows Task Scheduler:
- Program: `python`
- Arguments: `batch.py --config config.yaml --report reports\batch.txt`
- Start in: `C:\path\to\trafscan`
- Trigger: Daily at 02:00

### Run batch manually (for immediate diagnosis)

```bash
python batch.py --dry-run           # checks only, no DB writes, no email
python batch.py --report report.txt # full run, save text report
```

### Test the HTML report template

```bash
python alerting.py --config config.yaml --test
# Generates test_report_v31.html with fake anomaly data
# Does NOT send email - safe to run anytime to verify the template
```

---

## Email alerting configuration

⚠ **Email is not yet configured.** Fill in the `alerting` section of
`config.yaml` to enable email notifications:

```yaml
alerting:
  smtp_host:     smtp.example.com       # ← your SMTP server
  smtp_port:     587
  smtp_user:     trafscan@example.com   # ← sender account
  smtp_password: your_app_password      # ← app password or token
  smtp_tls:      true
  from_email:    trafscan@example.com
  to_emails:
    - ops-team@example.com              # ← recipients
    - noc@example.com
  alert_on: [CRITICAL, WARNING]         # or [CRITICAL] only

  # Optional: POST JSON payload to NOC frontend after each batch run
  noc_webhook_url: ""                   # ← leave empty to use file polling instead
```

To verify SMTP settings before the first real batch run:

```bash
python alerting.py --config config.yaml --test
```

This sends a test email with fake anomaly data so you can confirm the
email arrives correctly without waiting for the nightly batch.

---

## NOC / Frontend integration

After every batch run, two files are written to `reports/`:

| File | Description |
|------|-------------|
| `reports/report_YYYYMMDD_HHMMSS.html` | Full HTML report - open in any browser or NOC panel |
| `reports/noc_latest.json` | JSON payload - always reflects the most recent batch run |

### Option A - Polling (no server required)

The frontend periodically fetches `noc_latest.json`. Suggested poll interval: 5–15 minutes.

### Option B - Webhook (real-time push)

Set `alerting.noc_webhook_url` in `config.yaml`. The batch POSTs the full
payload to that URL immediately after each run.

### Payload structure (v3.1)

```json
{
  "generated_at": "2025-05-01T02:00:00",
  "version":      "3.1",
  "status":       "CRITICAL",
  "summary": {
    "total_anomalies": 3,
    "critical":        2,
    "warnings":        1,
    "files_done_today": 247
  },
  "anomalies": [
    {
      "type":        "MINUTES_DIVERGENCE",
      "severity":    "CRITICAL",
      "file_name":   "CDR_20250501.gz",
      "operator_id": "atel",
      "cdr_type":    "in",
      "detail":      "db=1000.25 es=1050.10 delta=4.97%"
    },
    {
      "type":        "VOICE_DIVERGENCE",
      "severity":    "WARNING",
      "file_name":   "CDR_20250501.gz",
      "operator_id": "atel",
      "cdr_type":    "in",
      "detail":      "db=500.10 es=503.20 delta=0.62%"
    },
    {
      "type":        "PROCESSING_TIME_ANOMALY",
      "severity":    "WARNING",
      "file_name":   "CDR_20250501_pgw.gz",
      "operator_id": "orange",
      "cdr_type":    "pgw",
      "detail":      "Duration 45.0min = 3.2× median (14.1min over 32 samples, 7d)"
    }
  ],
  "forfait": {
    "today_known":   72000,
    "today_unknown": 4500,
    "today_total":   76500,
    "median_total":  85000,
    "drop_pct":      10.0,
    "es_total":      null
  }
}
```

The `status` field drives the NOC panel color:
- `"OK"` → green
- `"WARNING"` → orange
- `"CRITICAL"` → red

---

## FTP transfer verification

After transferring the compressed CDR file to the regulator, run:

```bash
python verify_ftp.py \
    --file CDR_20250401_atel.gz \
    --local /path/to/received/file.gz
```

This computes the SHA-256 of the file at the regulator and compares it to
the checksum stored in the registry. If they differ → status set to MISMATCH
and anomaly logged.

---

## Key SQL queries for manual diagnosis

### Files stuck in pipeline (> 2 hours)

```sql
SELECT file_name, operator_id, cdr_type, processing_status,
       EXTRACT(EPOCH FROM (NOW() - received_at)) / 3600 AS hours_waiting
FROM cdr_registry
WHERE processing_status IN ('PENDING', 'PROCESSING')
  AND received_at < NOW() - INTERVAL '2 hours'
ORDER BY received_at ASC;
```

### All anomalies from today (any severity)

```sql
SELECT al.anomaly_type, al.severity, al.detected_at,
       cr.file_name, cr.operator_id, cr.cdr_type,
       al.delta_value, al.threshold_value
FROM anomaly_log al
LEFT JOIN cdr_registry cr ON al.file_id = cr.file_id
WHERE al.detected_at >= CURRENT_DATE
  AND al.resolved = FALSE
ORDER BY al.severity DESC, al.detected_at DESC;
```

### All DB vs ES divergences (any dimension, today)

```sql
SELECT file_name, operator_id,
       total_minutes_db, total_minutes_es,
       voice_db, voice_es,
       sms_db,   sms_es,
       data_db,  data_es,
       recharge_db, recharge_es
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND received_at >= CURRENT_DATE
  AND (
      ABS(total_minutes_db - total_minutes_es) / NULLIF(total_minutes_db, 0) > 0.005
      OR ABS(voice_db    - voice_es)    / NULLIF(voice_db, 0)    > 0.005
      OR ABS(sms_db      - sms_es)      / NULLIF(sms_db, 0)      > 0.005
      OR ABS(data_db     - data_es)     / NULLIF(data_db, 0)     > 0.005
      OR ABS(recharge_db - recharge_es) / NULLIF(recharge_db, 0) > 0.005
  );
```

### Voice/SMS/Data divergence (single dimension)

```sql
-- Replace voice_db/voice_es with sms_db/sms_es or data_db/data_es as needed
SELECT file_name, operator_id,
       voice_db, voice_es,
       ROUND(ABS(voice_db - voice_es) / NULLIF(voice_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND voice_db IS NOT NULL AND voice_es IS NOT NULL
  AND ABS(voice_db - voice_es) / NULLIF(voice_db, 0) * 100 > 1.0
ORDER BY delta_pct DESC;
```

### Daily summary per operator

```sql
SELECT operator_id, cdr_type,
       COUNT(*) AS total,
       SUM(CASE WHEN processing_status='DONE'     THEN 1 ELSE 0 END) AS done,
       SUM(CASE WHEN processing_status='PENDING'  THEN 1 ELSE 0 END) AS pending,
       SUM(CASE WHEN processing_status='MISMATCH' THEN 1 ELSE 0 END) AS mismatch,
       SUM(CASE WHEN processing_status='ERROR'    THEN 1 ELSE 0 END) AS error
FROM cdr_registry
WHERE received_at >= CURRENT_DATE
GROUP BY operator_id, cdr_type
ORDER BY operator_id, cdr_type;
```

### Pipeline funnel - received vs completed today

```sql
SELECT
    operator_id,
    cdr_type::text,
    COUNT(*)                                                             AS received,
    SUM(CASE WHEN processing_status != 'PENDING' THEN 1 ELSE 0 END)     AS touched_by_java,
    SUM(CASE WHEN processing_status IN ('DONE','ERROR','MISMATCH')
             THEN 1 ELSE 0 END)                                          AS completed,
    SUM(CASE WHEN processing_status = 'PENDING'
             AND received_at < NOW() - INTERVAL '2 hours'
             THEN 1 ELSE 0 END)                                          AS stuck_pending,
    SUM(CASE WHEN processing_status = 'PROCESSING'
             AND received_at < NOW() - INTERVAL '2 hours'
             THEN 1 ELSE 0 END)                                          AS stuck_processing
FROM cdr_registry
WHERE received_at >= CURRENT_DATE
GROUP BY operator_id, cdr_type
ORDER BY operator_id, cdr_type;
```

### Today's forfait totals

```sql
SELECT
    CURRENT_DATE AS date,
    (SELECT COALESCE(SUM(count),0) FROM daily_state_forfait         WHERE date = CURRENT_DATE) AS known,
    (SELECT COALESCE(SUM(count),0) FROM daily_state_unknown_forfait WHERE date = CURRENT_DATE) AS unknown,
    (SELECT COALESCE(SUM(count),0) FROM daily_state_forfait         WHERE date = CURRENT_DATE)
  + (SELECT COALESCE(SUM(count),0) FROM daily_state_unknown_forfait WHERE date = CURRENT_DATE) AS total;
```

### Processing time outliers (7-day perspective)

```sql
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
       ROUND(EXTRACT(EPOCH FROM (cr.updated_at - cr.received_at)) / m.median_s, 2)  AS ratio
FROM cdr_registry cr
JOIN medians m ON cr.cdr_type::text = m.cdr_type
WHERE cr.processing_status = 'DONE'
  AND cr.received_at >= CURRENT_DATE
  AND EXTRACT(EPOCH FROM (cr.updated_at - cr.received_at)) > 3 * m.median_s
ORDER BY ratio DESC;
```

---

## Validated test scenarios

| # | Scenario | Result |
|---|----------|--------|
| 1 | Normal Java processing | PENDING → PROCESSING → DONE ✅ |
| 2 | ES minutes update | delta 0.06% < 0.5% - no alert ✅ |
| 3 | FTP transfer verification | checksums match, integrity confirmed ✅ |
| 4 | Missing records (36 missing) | MISMATCH auto-detected ✅ |
| 5 | Java crash | ERROR + error message written to DB ✅ |

---

## Files in this project

| File | Purpose |
|------|---------|
| `trafscan/watcher.py` | File watcher - detects new CDR files, triggers hasher |
| `trafscan/hasher.py` | SHA-256, line count, DB insert; also contains Java SQL templates |
| `trafscan/db.py` | PostgreSQL connection pool |
| `trafscan/__main__.py` | Entry point: `python -m trafscan` |
| `batch.py` | Nightly reconciliation batch - all checks (Q1–Q7 + Q_PT) |
| `alerting.py` | HTML report builder, email sender, NOC JSON payload |
| `simulate_java.py` | Test tool - simulates Java hooks without touching Java code |
| `verify_ftp.py` | FTP transfer integrity verifier |
| `config.yaml` | All configuration (DB, watched folders, thresholds, email) |
| `01_schema.sql` | PostgreSQL schema - run once to initialize |
| `02_schema_v32.sql` | v3.2 schema migration - prediction tables, Q8/Q9 ENUM values, pre-compressed columns |
| `seed.py` | Dev utility - seeds prediction_baseline with historical CDR volume data from CDRTextfiles/ for local testing |

---

## Important notes for integration

**Binary CDR types (msc, pgw, cnn):**
`record_count_expected` holds the raw newline count of the binary file, which
is not meaningful for record comparison. The batch automatically excludes these
types from record count and recharge checks. They are flagged with
`notes = 'Binary CDR: ...'`.

**file_name is always the basename:**
`CDR_20250501.gz`, never a full path. Java hooks and the watcher both use
only the basename when referencing a file.

**Never block on registry failures:**
The registry is a monitoring layer. If a SQL update fails (DB down, network
issue), log the error and continue CDR processing. CDR processing is always
more important than registry updates.

**ENUM casting in PostgreSQL:**
All status values must be cast explicitly:
`'DONE'::processing_status`, `'ERROR'::processing_status`, etc.
Already included in all Java hook templates above.

**Volume and partitioning:**
For volumes above 5 000 files/day, consider enabling PostgreSQL monthly
table partitioning on `cdr_registry` using `received_at` as the partition
key. The batch already runs incrementally (today's files only for most
checks). See the comments in `01_schema.sql` for the migration path.

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Apr 2025 | Initial: Q1, Q2, Q3a (minutes only), Q4, Q5c, Q6 |
| 2.0 | Apr 2025 | Java hooks refined, FTP verifier |
| 3.0 | May 2025 | Q3b–Q3e (voice/SMS/data/recharge), Q5a, Q5b |
| **3.1** | **May 2025** | **Q_PT (processing time), Q7 (forfaits), DB placement clarified, NOC payload v3.1** |
| **3.2** | **May 2025** | **Couche 5: Q8/Q9 prediction checks, pre-compressed CDR fix (hasher), prediction_baseline and prediction_actuals tables** |
| **3.2.1** | **May 2025** | **seed.py: seeds prediction_baseline from CDRTextfiles July 2025 history (Option B — aggregated daily/hourly counts, not individual rows in cdr_registry)** |