# TRAFSCAN VERIFICATION - Solution A: CDR Registry

## Integration Guide for the Trafscan Team

Version 3.0 - May 2025

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

It runs alongside your existing Trafscan system. It does not modify any
existing code - it only adds SQL calls at key points in the Java pipeline.

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
Layer 2: CDR Registry (PostgreSQL)
    - Table: cdr_registry   ← source of truth for every file
    - Table: anomaly_log    ← all detected anomalies
    - Table: operators      ← operator reference
    │
    ▼
Layer 2 (Java side): Instrumented Java hooks
    - Java updates status to PROCESSING at start           (Hook 1)
    - Java updates status to DONE + all traffic values     (Hook 2)
    - Java updates status to ERROR on exception            (Hook 3)
    - Java updates ES values after ES indexing             (Hook 4)
    │
    ▼
Layer 3: Reconciliation Batch (Python, runs at 02:00)
    - 8 checks against the registry (Q1, Q2, Q3a–e, Q4, Q5a, Q5b, Q5c, Q6)
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

## Database schema

### cdr_registry (main table)

| Column | Type | Description |
|---|---|---|
| file_id | UUID | Primary key, auto-generated |
| file_name | VARCHAR(255) | Original filename, UNIQUE |
| operator_id | VARCHAR(50) | Operator: orange / atel / moov |
| cdr_type | ENUM | in / msc / pgw / cnn / sdp / air / occ |
| received_at | TIMESTAMP | When file was detected by the watcher |
| processing_started_at | TIMESTAMP | Set by Java Hook 1 (requires hook integration) |
| processing_ended_at | TIMESTAMP | Set by Java Hook 2/3 (requires hook integration) |
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
| notes | TEXT | Error messages, flags, free-form notes |

> **Source mapping (Trafscan DB → registry columns)**
> - `voice_db` ← `AbonneTable.voice_counted_balance` aggregated per file
> - `sms_db` ← `AbonneTable.sms_counted_balance` aggregated per file
> - `data_db` ← `AbonneTable.data_counted_balance` aggregated per file
> - `recharge_db` ← `AbonneTable.recharge_balance` aggregated per file
>
> **Forfaits (pending):** Forfait data is stored in two separate tables
> (known / unknown forfaits). Reconciliation for forfaits will be added
> once the team confirms the exact table names and comparable ES fields.
> It will be implemented as a dedicated query rather than per-file columns.

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

You need one connection (or connection pool) to the registry DB
in addition to your existing connections.

### JDBC connection

```java
// Add to your configuration / dependency injection
String registryUrl = "jdbc:postgresql://localhost:5432/trafscan";
Connection registryConn = DriverManager.getConnection(
    registryUrl, "trafscan_user", "your_password"
);
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

try (PreparedStatement ps = registryConn.prepareStatement(sqlStart)) {
    ps.setString(1, fileName);
    int updated = ps.executeUpdate();
    registryConn.commit();
    if (updated == 0) {
        logger.warn("TRAFSCAN: file not in registry: " + fileName);
    }
}
```

### Hook 2 - After processing succeeds

Call after your CDR parsing loop completes successfully.
Include all traffic aggregates computed during parsing.

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

try (PreparedStatement ps = registryConn.prepareStatement(sqlDone)) {
    ps.setLong(1, recordCount);       // for CASE comparison
    ps.setLong(2, recordCount);       // record_count_processed
    ps.setDouble(3, totalMinutesDb);  // AbonneTable.voice_counted_balance aggregate
    ps.setDouble(4, voiceBalance);    // AbonneTable.voice_counted_balance aggregate
    ps.setDouble(5, smsBalance);      // AbonneTable.sms_counted_balance aggregate
    ps.setDouble(6, dataBalance);     // AbonneTable.data_counted_balance aggregate
    ps.setDouble(7, rechargeBalance); // AbonneTable.recharge_balance aggregate (in CDRs)
    ps.setString(8, fileName);
    ps.executeUpdate();
    registryConn.commit();
}
```

> **Note:** For CDR types where a value is not applicable (e.g. recharge for
> MSC voice files), pass `null` (`ps.setNull(7, Types.DECIMAL)`).

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

try (PreparedStatement ps = registryConn.prepareStatement(sqlError)) {
    ps.setString(1, "Java error: " + exception.getMessage());
    ps.setString(2, fileName);
    ps.executeUpdate();
    registryConn.commit();
} catch (SQLException sqlEx) {
    logger.error("TRAFSCAN: failed to update error status: " + sqlEx.getMessage());
    // Never let registry updates block your main error handling
}
```

### Hook 4 - After ElasticSearch indexing

Call after indexing the file's data in ES. Pass the same traffic values
as reported by ES for the divergence check to be meaningful.

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

try (PreparedStatement ps = registryConn.prepareStatement(sqlEs)) {
    ps.setDouble(1, totalMinutesEs);
    ps.setDouble(2, voiceEs);
    ps.setDouble(3, smsEs);
    ps.setDouble(4, dataEs);
    ps.setDouble(5, rechargeEs);
    ps.setString(6, fileName);
    ps.executeUpdate();
    registryConn.commit();
}
```

### Where exactly in your code

```
YourCDRProcessor.process(file):
    fileName = file.getName()              ← basename only

    [Hook 1] → status = PROCESSING

    try:
        recordCount    = 0
        totalMinutes   = 0.0
        voiceBalance   = 0.0
        smsBalance     = 0.0
        dataBalance    = 0.0
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

## Note on processing time detection

Processing time (`processing_started_at` / `processing_ended_at`) will be
available in the registry **once Hook 1 and Hook 2/3 are integrated** by
the Java team. The team lead confirmed this requires enriching the Java
decoder - it is not yet implemented.

Once the timestamps are populated, they can be used to detect files with
suspiciously long processing times (potential silent corruption, infinite
loops, or resource saturation). The batch will be extended with this check
at that point.

---

## Note on forfait reconciliation

Forfait data is stored in two separate tables in the Trafscan DB (known
and unknown forfaits) and is **not a per-file aggregate**. A dedicated
reconciliation query will be added to the batch once the team confirms:

1. The exact names of the two forfait tables
2. Which fields are comparable to what ElasticSearch indexes

This will be implemented as a standalone check rather than columns in
`cdr_registry`, since forfait data spans multiple files and operators.

---

## Batch reconciliation checks (Q5a, Q5b, Q5c)

The old single "Q5 - files on disk not in DB" check has been replaced by
three complementary completeness checks that together give a full picture
of file flow at every step of the pipeline.

### Q5a - Daily file count vs baseline

For each operator × CDR type, compares today's received file count against
a configured expected baseline. This detects days where significantly fewer
(or more) files arrived than normal - which the watcher alone cannot catch
because it only reacts to files that actually land.

**Severity:**
- WARNING → count outside baseline ± `tolerance_pct` (default 3%)
- CRITICAL → count outside baseline ± 2× `tolerance_pct`, or zero files received

**Configuration - baselines section in `config.yaml`:**

```yaml
baselines:
  tolerance_pct: 3.0          # ±3% is normal daily variation
  operator_type:
    orange:
      in:   120               # ← approximate files/day for orange/in
      msc:  80                # ← fill in once team provides counts
      pgw:  null              # ← null = skip this combination, no alert
    atel:
      in:   90
      msc:  null
    moov:
      in:   50
      msc:  null
```

Leave any value as `null` until the team provides the approximate daily
count for that combination. The batch will skip it with an INFO log -
no false alerts during the learning phase.

### Q5b - Pipeline funnel check

For each operator × CDR type, verifies that files flow correctly through
every processing step today:

```
received (any status)
    └─► touched by Java (PROCESSING / DONE / ERROR / MISMATCH)
            └─► completed (DONE / ERROR / MISMATCH)
```

Detects three types of gaps:
- Files stuck **PENDING** past the grace period → Java never picked them up
- Files stuck **PROCESSING** past the grace period → Java started but crashed
- **Unexplained gap** between received and completed → something else failed

Uses the same `batch.stuck_file_hours` grace period as Q1 to avoid alerting
on files still legitimately in progress.

### Q5c - Files on disk not registered in DB

The original check: scans the backup directories and alerts on any file
present on disk that has no corresponding row in `cdr_registry`. Catches
files the watcher missed entirely (was down when file arrived, extension
mismatch, etc.).

---


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

## Running the system

### Start the file watcher (runs continuously)

```bash
python -m trafscan --config config.yaml
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
# Add to crontab
0 2 * * * cd /opt/trafscan && python batch.py --report /var/trafscan/reports/$(date +\%Y\%m\%d).txt
```

Windows Task Scheduler:
- Program: `python`
- Arguments: `batch.py --config config.yaml --report reports\batch.txt`
- Trigger: Daily at 02:00

### Run batch manually (for immediate diagnosis)

```bash
python batch.py --dry-run           # checks only, no DB writes, no email
python batch.py --report report.txt # checks + save report
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

  # Optional: webhook for NOC frontend (see section below)
  noc_webhook_url: ""                   # ← leave empty to disable
```

To test the email configuration without running a full batch:

```bash
python alerting.py --config config.yaml --test
```

This sends a test email with fake data so you can verify SMTP settings
before the first real batch run.

---

## NOC / Frontend alerting integration

After every batch run, two files are written to `reports/`:

| File | Description |
|---|---|
| `reports/report_YYYYMMDD_HHMMSS.html` | Full HTML report - open in any browser or NOC panel |
| `reports/noc_latest.json` | JSON payload - always reflects the most recent batch run |

### Option A - Polling (no server required)

The frontend periodically fetches `noc_latest.json`:

```
GET /reports/noc_latest.json
```

Suggested poll interval: every 5–15 minutes.

### Option B - Webhook (real-time push)

Set `alerting.noc_webhook_url` in `config.yaml`. The batch will POST
the payload to that URL immediately after each run:

```
POST /api/noc/alerts
Content-Type: application/json

{
  "generated_at": "2025-04-23T02:00:00",
  "status": "CRITICAL",
  "summary": {
    "total_anomalies": 3,
    "critical": 2,
    "warnings": 1,
    "files_done_today": 247
  },
  "anomalies": [
    {
      "type":        "MINUTES_DIVERGENCE",
      "severity":    "CRITICAL",
      "file_name":   "CDR_20250423.gz",
      "operator_id": "atel",
      "cdr_type":    "in",
      "dimension":   "minutes",
      "detail":      "db=1000.25 es=1050.10 delta=4.97%"
    },
    {
      "type":        "VOICE_DIVERGENCE",
      "severity":    "WARNING",
      "file_name":   "CDR_20250423.gz",
      "operator_id": "atel",
      "cdr_type":    "in",
      "dimension":   "voice",
      "detail":      "db=500.10 es=503.20 delta=0.62%"
    }
  ]
}
```

The `status` field gives the frontend the overall color to show:
- `"OK"` → green
- `"WARNING"` → orange
- `"CRITICAL"` → red

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

### All anomalies from today

```sql
SELECT file_name, operator_id, cdr_type,
       processing_status, record_count_expected, record_count_processed,
       total_minutes_db, total_minutes_es, notes
FROM cdr_registry
WHERE processing_status IN ('ERROR', 'MISMATCH')
  AND received_at >= CURRENT_DATE
ORDER BY received_at DESC;
```

### Minutes divergence DB vs ES

```sql
SELECT file_name, operator_id,
       total_minutes_db, total_minutes_es,
       ROUND(ABS(total_minutes_db - total_minutes_es)
             / NULLIF(total_minutes_db, 0) * 100, 2) AS delta_pct
FROM cdr_registry
WHERE processing_status = 'DONE'
  AND total_minutes_db IS NOT NULL AND total_minutes_es IS NOT NULL
  AND ABS(total_minutes_db - total_minutes_es)
      / NULLIF(total_minutes_db, 0) * 100 > 1.0
ORDER BY delta_pct DESC;
```

### Voice/SMS/Data divergence DB vs ES

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
    COUNT(*)                                                      AS received,
    SUM(CASE WHEN processing_status != 'PENDING' THEN 1 ELSE 0 END) AS touched_by_java,
    SUM(CASE WHEN processing_status IN ('DONE','ERROR','MISMATCH')
             THEN 1 ELSE 0 END)                                   AS completed,
    SUM(CASE WHEN processing_status = 'PENDING'
             AND received_at < NOW() - INTERVAL '2 hours'
             THEN 1 ELSE 0 END)                                   AS stuck_pending,
    SUM(CASE WHEN processing_status = 'PROCESSING'
             AND received_at < NOW() - INTERVAL '2 hours'
             THEN 1 ELSE 0 END)                                   AS stuck_processing
FROM cdr_registry
WHERE received_at >= CURRENT_DATE
GROUP BY operator_id, cdr_type
ORDER BY operator_id, cdr_type;
```

---

## Important notes for integration

**Binary CDR types (msc, pgw, cnn):**
`record_count_expected` holds the raw newline count of the binary file,
which is not meaningful for comparison. The batch automatically excludes
these types from record count and minutes divergence checks. They are
flagged with `notes = 'Binary CDR: ...'`.

**File naming:**
`file_name` in the registry is always the basename only (e.g. `CDR_20250401.gz`),
never the full path. Your Java hooks must use the same basename.

**Database connection:**
The registry DB is separate from the main Trafscan application DB.
Use a dedicated connection or pool. Credentials are in `config.yaml`.

**Never block on registry failures:**
The registry is a monitoring layer. If a SQL update fails (DB down,
network issue), log the error and continue CDR processing. CDR processing
is always more important than registry updates.

**ENUM casting in PostgreSQL:**
All status values must be cast explicitly in SQL:
`'DONE'::processing_status`, `'ERROR'::processing_status`, etc.
Already included in all templates above.

**Volume and partitioning:**
For volumes above 5 000 files/day, consider enabling PostgreSQL monthly
table partitioning on `cdr_registry` using `received_at` as the partition
key. The batch already runs incrementally (today's files only for most
checks). See comments in `01_schema.sql` for the migration path.

---

## Validated test scenarios

| # | Scenario | Result |
|---|---|---|
| 1 | Normal Java processing | PENDING → PROCESSING → DONE ✅ |
| 2 | ES minutes update | delta 0.06% < 0.5%, no alert ✅ |
| 3 | FTP transfer verification | checksums match, integrity confirmed ✅ |
| 4 | Missing records | 36 missing → MISMATCH auto-detected ✅ |
| 5 | Java crash | ERROR + note written to DB ✅ |

---

## Files in this project

| File | Purpose |
|---|---|
| `trafscan/watcher.py` | File watcher - detects new CDR files, triggers hasher |
| `trafscan/hasher.py` | SHA-256, line count, DB insert; also contains Java SQL templates |
| `trafscan/db.py` | PostgreSQL connection pool |
| `trafscan/__main__.py` | Entry point: `python -m trafscan` |
| `batch.py` | Nightly reconciliation batch - 8 checks (Q1, Q2, Q3a–e, Q4, Q5a, Q5b, Q5c, Q6), report generation |
| `alerting.py` | HTML report builder, email sender, NOC JSON payload |
| `simulate_java.py` | Test tool - simulates Java hooks without touching Java code |
| `verify_ftp.py` | FTP transfer integrity verifier |
| `config.yaml` | All configuration (DB, watched folders, thresholds, email) |
| `01_schema.sql` | PostgreSQL schema - run once to initialize the database |
