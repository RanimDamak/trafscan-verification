# TRAFSCAN VERIFICATION - Solution A: CDR Registry
## Integration Guide for Trafscan

Version 2.1 - April 2025

---

## What this is

This system adds a **CDR Registry** layer to Trafscan. Every CDR file that
arrives on the frontale is automatically tracked from the moment it lands on
disk until it is processed by Java and indexed in ElasticSearch.

The registry answers three questions in real time with a single SQL query:
- Did all CDR files arrive and get processed?
- Did Java and ElasticSearch produce the same minute counts?
- Was any file corrupted during FTP transfer?

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
    - Java updates status to PROCESSING at start
    - Java updates status to DONE + minutes + record count at end
    - Java updates status to ERROR on exception
    │
    ▼
Layer 3: Reconciliation Batch (Python, runs at 02:00)
    - 6 SQL checks against the registry
    - Writes to anomaly_log
    - Generates daily report
```

---

## Folder structure on the frontale

Files must be organized exactly as follows for the operator and CDR type
to be detected automatically:

```
/opt/backup/
    orange/
        in/       ← IN CDRs (recharge, voucher)
        msc/      ← MSC CDRs (voice, binary Ericsson format)
        pgw/      ← PGW/GGSN CDRs (data, binary IPDR format)
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

The operator name and CDR type come from the folder path, never from
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
| received_at | TIMESTAMP | When the file was detected by the watcher |
| processing_started_at | TIMESTAMP | When Java started processing |
| processing_ended_at | TIMESTAMP | When Java finished processing |
| checksum_raw | CHAR(64) | SHA-256 of the original file on disk |
| checksum_compressed | CHAR(64) | SHA-256 after gzip compression (if applicable) |
| checksum_transferred | CHAR(64) | SHA-256 verified at regulator after FTP |
| record_count_expected | BIGINT | Lines counted by Python at reception |
| record_count_processed | BIGINT | Records actually parsed by Java |
| total_minutes_db | DECIMAL(18,3) | Minutes calculated by Java → stored in DB |
| total_minutes_es | DECIMAL(18,3) | Minutes indexed in ElasticSearch |
| processing_status | ENUM | PENDING / PROCESSING / DONE / ERROR / MISMATCH |
| notes | TEXT | Error messages, flags, free-form notes |

### processing_status lifecycle

```
File arrives on disk
      │
      ▼
   PENDING          ← inserted by Python watcher immediately
      │
      ▼ (Java picks up the file)
  PROCESSING        ← Java calls hook at start of processing loop
      │
      ├──► DONE     ← Java calls hook at end, record counts match
      ├──► MISMATCH ← Java calls hook at end, record_count_processed < expected
      │              OR checksum_transferred != checksum_compressed
      └──► ERROR    ← Java catch block calls hook with error message
```

---

## What the Java team needs to add

### Overview

You need to add **3 SQL calls** to the existing CDR processing code.
These are standard JDBC PreparedStatement calls against the trafscan
PostgreSQL database.

You need one connection (or use a connection pool) to the trafscan DB
in addition to your existing connections.

### JDBC connection

```java
// Add to your configuration / dependency injection
String trafscanUrl = "jdbc:postgresql://localhost:5432/trafscan";
Connection trafscanConn = DriverManager.getConnection(
    trafscanUrl, "trafscan_user", "your_password"
);
```

### Hook 1 - Before processing starts

Call this immediately before your CDR parsing loop begins.
`fileName` is the basename of the file (e.g. `CDR_20250401_atel.gz`).

```java
// Hook 1: mark file as PROCESSING
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
        // File not in registry - log a warning, do not block processing
        logger.warn("TRAFSCAN: file not in registry: " + fileName);
    }
}
```

### Hook 2 - After processing succeeds

Call this immediately after your CDR parsing loop completes successfully.
`recordCount` is the number of CDR records actually parsed.
`totalMinutesDb` is the sum of call/session minutes calculated by Java.

```java
// Hook 2: mark file as DONE (or MISMATCH if record count differs)
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
        updated_at             = NOW()
    WHERE file_name = ?
""";

try (PreparedStatement ps = trafscanConn.prepareStatement(sqlDone)) {
    ps.setLong(1, recordCount);    // for the CASE comparison
    ps.setLong(2, recordCount);    // for record_count_processed
    ps.setDouble(3, totalMinutesDb);
    ps.setString(4, fileName);
    ps.executeUpdate();
    trafscanConn.commit();
}
```

### Hook 3 - In the catch block (Java error)

Call this inside your existing catch block that handles processing failures.

```java
// Hook 3: mark file as ERROR with the exception message
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
    // Never let registry update failure block your main error handling
}
```

### Hook 4 - After ElasticSearch indexing (optional but recommended)

Call this after you have indexed the file's data in ElasticSearch.
`totalMinutesEs` is the sum of minutes as stored in ES.

```java
// Hook 4: store ES minutes for divergence detection
String sqlEs = """
    UPDATE cdr_registry
    SET total_minutes_es = ?,
        updated_at       = NOW()
    WHERE file_name = ?
""";

try (PreparedStatement ps = trafscanConn.prepareStatement(sqlEs)) {
    ps.setDouble(1, totalMinutesEs);
    ps.setString(2, fileName);
    ps.executeUpdate();
    trafscanConn.commit();
}
```

### Where exactly in your code

```
YourCDRProcessor.process(file):
    fileName = file.getName()                    ← just the basename

    [Hook 1 here] → status = PROCESSING

    try:
        recordCount = 0
        totalMinutes = 0.0

        for each record in file:
            parse(record)
            recordCount++
            totalMinutes += record.getDuration()

        indexInElasticSearch(records)

        [Hook 4 here] → total_minutes_es = totalMinutes

        [Hook 2 here] → status = DONE or MISMATCH
                      → record_count_processed = recordCount
                      → total_minutes_db = totalMinutes

    catch Exception e:
        [Hook 3 here] → status = ERROR
        throw e        ← re-throw as before, don't swallow
```

---

## FTP transfer verification

After transferring the compressed CDR file to the regulator, run:

```bash
python verify_ftp.py \
    --file CDR_20250401_atel.gz \
    --local /path/to/received/file.gz
```

This computes the SHA-256 of the file received at the regulator and
compares it to the checksum stored in the registry at reception.
If they differ → status set to MISMATCH and anomaly logged.

---

## Running the system

### Start the file watcher (runs continuously)

```bash
# On the frontale server
python -m trafscan --config config.yaml
```

Set up as a systemd service for production:

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

On Windows Task Scheduler:
- Program: `python`
- Arguments: `batch.py --config config.yaml --report reports\batch.txt`
- Start in: `C:\path\to\trafscan`
- Trigger: Daily at 02:00

### Run batch manually (for immediate diagnosis)

```bash
# Run checks without writing to anomaly_log
python batch.py --dry-run

# Run checks and save report
python batch.py --report report.txt
```

---

## Key SQL queries for manual diagnosis

All queries run against the `trafscan` PostgreSQL database.

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
  AND total_minutes_db IS NOT NULL
  AND total_minutes_es IS NOT NULL
  AND ABS(total_minutes_db - total_minutes_es)
      / NULLIF(total_minutes_db, 0) * 100 > 1.0
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

---

## Important notes for integration

**Binary CDR types (msc, pgw, cnn):**
The `record_count_expected` column for these files contains the raw
newline count of the binary file, which is meaningless. Do not compare
`record_count_expected` vs `record_count_processed` for these types.
The batch automatically excludes them from record count checks.
These files are flagged with `notes = 'Binary CDR: ...'`.

**File naming:**
The `file_name` stored in the registry is always the basename only
(e.g. `CDR_20250401.gz`), never the full path. Your Java hooks must
use the same basename when calling the UPDATE queries.

**Database connection:**
The trafscan database is separate from the main Trafscan application
database. Use a dedicated connection or pool. Credentials are in
`config.yaml` under the `database` section.

**Never block on registry failures:**
The registry is a monitoring layer. If a SQL update to the registry
fails (network issue, DB down), log the error but do not stop CDR
processing. CDR processing is more important than registry updates.

**ENUM casting in PostgreSQL:**
All status values must be cast explicitly:
`'DONE'::processing_status`, `'ERROR'::processing_status`, etc.
This is already included in all SQL templates above.

---

## Files in this project

| File | Purpose |
|---|---|
| `trafscan/watcher.py` | File watcher - detects new CDR files, triggers hasher |
| `trafscan/hasher.py` | SHA-256, line count, DB insert - also contains Java SQL templates |
| `trafscan/db.py` | PostgreSQL connection pool |
| `trafscan/__main__.py` | Entry point: `python -m trafscan` |
| `batch.py` | Nightly reconciliation batch - 6 checks, report generation |
| `simulate_java.py` | Test tool - simulates Java hooks without touching Java code |
| `verify_ftp.py` | FTP transfer integrity verifier |
| `config.yaml` | All configuration (DB, watched folders, thresholds) |
| `01_schema.sql` | PostgreSQL schema - run once to initialize the database |
