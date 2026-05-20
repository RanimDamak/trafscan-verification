"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A  |  Layer 1 (cont.): Hasher + INSERT
────────────────────────────────────────────────────────────────────────────
Responsibilities
────────────────
1. Compute SHA-256 of the raw CDR file (streaming, handles multi-GB files).
2. Count CDR records (lines) in the file.
   - Plain text / CSV / pipe-delimited  → streaming line count.
   - Gzip files (.gz)                   → transparent decompression + count.
   - Binary files (msc, pgw, cnn .dat)  → line count stored but flagged;
     meaningful record count comes from Java processing later.
3. Auto-register operator in operators table if not already present.
4. INSERT a row into cdr_registry with status PENDING.
5. Optionally compress and update checksum_compressed.

Integration contract
─────────────────────
    hasher = Hasher(db_pool, config)
    hasher.process(file_path, file_name, operator_id, cdr_type)

operator_id and cdr_type come from the folder path structure on the
frontale (/opt/backup/{operator}/{cdr_type}/), never from the filename.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_CHUNK = 8192

# CDR types that are binary — line count is meaningless for these.
# We still store it (bytes with \n) but flag it in notes so the
# batch knows not to compare record_count_expected vs record_count_processed.
BINARY_CDR_TYPES = {"msc", "pgw", "cnn"}

# Extensions that indicate a file is already compressed on arrival.
# These files must NOT be re-compressed by the hasher.
PRE_COMPRESSED_EXTENSIONS = (".gz",)


# ─────────────────────────────────────────────────────────────────────────────
# Low-level helpers
# ─────────────────────────────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    """Streaming SHA-256. Safe for multi-GB files."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def is_pre_compressed(file_path: str) -> bool:
    """Return True if the file arrived already compressed (e.g. .gz)."""
    return file_path.lower().endswith(PRE_COMPRESSED_EXTENSIONS)


def count_records_smart(file_path: str) -> int:
    """
    Count CDR records (lines) in a file.

    - For pre-compressed files (.gz): decompresses in streaming mode
      (no full decompression to disk) and counts newlines in the raw content.
      This gives the actual number of CDR records, not the compressed line count.
    - For plain files: streaming newline count.

    Safe for multi-GB files in both cases.
    Falls back to raw byte count if the gzip header is invalid.
    """
    if file_path.lower().endswith(".gz"):
        return _count_lines_gz(file_path)
    return _count_lines_plain(file_path)


def _count_lines_plain(path: str) -> int:
    count = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            count += chunk.count(b"\n")
    return count


def _count_lines_gz(path: str) -> int:
    count = 0
    try:
        with gzip.open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                count += chunk.count(b"\n")
    except gzip.BadGzipFile as exc:
        log.warning("[hasher] Not valid gzip, falling back to raw count: %s — %s", path, exc)
        return _count_lines_plain(path)
    return count


def compress_gzip(src_path: str, dst_path: Optional[str] = None) -> str:
    if dst_path is None:
        dst_path = src_path if src_path.endswith(".gz") else src_path + ".gz"
    result = subprocess.run(
        ["gzip", "-k", "-f", src_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gzip failed: {result.stderr}")
    expected = src_path + ".gz"
    if expected != dst_path and os.path.exists(expected):
        os.rename(expected, dst_path)
    return dst_path


# ─────────────────────────────────────────────────────────────────────────────
# Hasher class
# ─────────────────────────────────────────────────────────────────────────────

class Hasher:
    """
    Stateless processor: one `process()` call per file.
    Thread-safe when db_pool provides connection-per-call semantics.
    """

    def __init__(self, db_pool, config: dict):
        self.pool = db_pool
        self.cfg = config
        self._compress = config.get("hasher", {}).get("compress_after_hash", False)
        self._compressed_dir = config.get("hasher", {}).get("compressed_dir", None)

    # ── public API ────────────────────────────────────────────────────────────

    def process(
        self,
        file_path: str,
        file_name: str,
        operator_id: str,
        cdr_type: str,
    ) -> Optional[str]:
        """
        Full processing pipeline for one CDR file.

        Parameters
        ----------
        file_path   : absolute path to the file
        file_name   : basename of the file
        operator_id : operator name from folder path (e.g. 'orange', 'atel')
        cdr_type    : CDR type from folder path (e.g. 'in', 'msc', 'pgw')
        """
        file_id = str(uuid.uuid4())
        t0 = time.monotonic()
        is_binary = cdr_type.lower() in BINARY_CDR_TYPES

        # FIX H2/H3: detect pre-compression status once, used in multiple steps below
        pre_compressed = is_pre_compressed(file_path)

        log.info(
            "[hasher] Processing: %s  (id=%s  operator=%s  type=%s  binary=%s  pre_compressed=%s)",
            file_name, file_id, operator_id, cdr_type, is_binary, pre_compressed
        )

        # ── Step 1: SHA-256 raw ───────────────────────────────────────────────
        try:
            checksum_raw = sha256_file(file_path)
        except OSError as exc:
            log.error("[hasher] Cannot read file %s: %s", file_path, exc)
            return None

        log.info("[hasher] SHA-256 raw: %s", checksum_raw)

        # ── Step 2: Count records (FIX H1: use count_records_smart) ──────────
        # count_records_smart() decompresses .gz in streaming mode → actual
        # CDR record count, not compressed binary line count.
        try:
            record_count = count_records_smart(file_path)
        except Exception as exc:
            log.error("[hasher] Record count failed for %s: %s", file_path, exc)
            record_count = 0

        if is_binary:
            log.info(
                "[hasher] Raw byte-line count (binary CDR, not meaningful): %d", record_count
            )
        else:
            log.info("[hasher] Record count: %d", record_count)

        # ── Step 3: Ensure operator exists, then INSERT ───────────────────────
        conn = self.pool.getconn()
        try:
            self._ensure_operator(conn, operator_id)
            inserted = self._insert_registry(
                conn=conn,
                file_id=file_id,
                file_name=file_name,
                operator_id=operator_id,
                cdr_type=cdr_type,
                checksum_raw=checksum_raw,
                record_count_expected=record_count,
                is_binary=is_binary,
                pre_compressed=pre_compressed,   # FIX H2: pass flag to INSERT
            )
            if not inserted:
                log.warning("[hasher] Duplicate file, skipping: %s", file_name)
                return None
        finally:
            self.pool.putconn(conn)

        elapsed = time.monotonic() - t0
        log.info("[hasher] INSERT OK in %.2fs  |  file=%s", elapsed, file_name)

        # ── Step 4: Compression / checksum_compressed ─────────────────────────
        # FIX H3: For pre-compressed files, checksum_compressed = checksum_raw
        # (the file is already its own compressed form). No re-compression.
        if pre_compressed:
            self._set_checksum_compressed(file_id, checksum_raw)
            log.info(
                "[hasher] Pre-compressed file: checksum_compressed set to checksum_raw (%s)",
                checksum_raw
            )
        elif self._compress:
            self._compress_and_update(file_path, file_id, file_name, checksum_raw)

        return file_id

    # ── private ───────────────────────────────────────────────────────────────

    def _ensure_operator(self, conn, operator_id: str) -> None:
        """
        Insert the operator into the operators table if not already present.
        This prevents FK violations when a new operator folder appears.
        operator_name defaults to the operator_id — update manually in DB later.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO operators (operator_id, operator_name)
                VALUES (%s, %s)
                ON CONFLICT (operator_id) DO NOTHING
                """,
                (operator_id, operator_id.capitalize())
            )
            conn.commit()

    def _insert_registry(
        self,
        conn,
        file_id: str,
        file_name: str,
        operator_id: str,
        cdr_type: str,
        checksum_raw: str,
        record_count_expected: int,
        is_binary: bool,
        pre_compressed: bool,       # FIX H2: new parameter
    ) -> bool:
        """
        INSERT a PENDING row. Returns False if file_name already exists (duplicate).
        """
        notes_parts = []
        if is_binary:
            notes_parts.append(
                "Binary CDR: record_count_expected is raw line count, not meaningful."
            )
        if pre_compressed:
            notes_parts.append(
                "Pre-compressed: file arrived as .gz; record_count is decompressed line count."
            )
        notes = " | ".join(notes_parts) if notes_parts else None

        sql = """
            INSERT INTO cdr_registry (
                file_id,
                file_name,
                operator_id,
                cdr_type,
                received_at,
                checksum_raw,
                record_count_expected,
                processing_status,
                is_pre_compressed,
                notes
            )
            VALUES (
                %s, %s, %s, %s, NOW(), %s, %s, 'PENDING', %s, %s
            )
            ON CONFLICT (file_name) DO NOTHING
        """
        with conn.cursor() as cur:
            cur.execute(sql, (
                file_id,
                file_name,
                operator_id,
                cdr_type,
                checksum_raw,
                record_count_expected,
                pre_compressed,     # FIX H2: write is_pre_compressed to DB
                notes,
            ))
            inserted = cur.rowcount > 0
            conn.commit()
        return inserted

    def _set_checksum_compressed(self, file_id: str, checksum: str) -> None:
        """
        FIX H3: For pre-compressed files, set checksum_compressed = checksum_raw
        immediately after INSERT. No gzip subprocess needed.
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE cdr_registry SET checksum_compressed=%s, updated_at=NOW() WHERE file_id=%s",
                    (checksum, file_id),
                )
                conn.commit()
        finally:
            self.pool.putconn(conn)

    def _compress_and_update(self, src_path, file_id, file_name, checksum_raw):
        try:
            dst_path = compress_gzip(src_path, self._compressed_dir)
            checksum_compressed = sha256_file(dst_path)
        except Exception as exc:
            log.error("[hasher] Compression failed for %s: %s", file_name, exc)
            self._update_notes(file_id, f"Compression error: {exc}")
            return

        log.info("[hasher] SHA-256 compressed: %s", checksum_compressed)
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE cdr_registry SET checksum_compressed=%s, updated_at=NOW() WHERE file_id=%s",
                    (checksum_compressed, file_id),
                )
                conn.commit()
        finally:
            self.pool.putconn(conn)

    def _update_notes(self, file_id: str, note: str) -> None:
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cdr_registry
                    SET notes = COALESCE(notes || ' | ', '') || %s, updated_at = NOW()
                    WHERE file_id = %s
                    """,
                    (note, file_id),
                )
                conn.commit()
        finally:
            self.pool.putconn(conn)

    # ── SQL hooks for Java ────────────────────────────────────────────────────

    @staticmethod
    def sql_hook_processing_start() -> str:
        return """
            UPDATE cdr_registry
            SET processing_status     = 'PROCESSING',
                processing_started_at = NOW(),
                updated_at            = NOW()
            WHERE file_name = ?
        """

    @staticmethod
    def sql_hook_processing_done() -> str:
        return """
            UPDATE cdr_registry
            SET processing_status      = CASE
                                            WHEN record_count_processed < record_count_expected
                                            THEN 'MISMATCH'
                                            ELSE 'DONE'
                                         END,
                processing_ended_at    = NOW(),
                record_count_processed = ?,
                total_minutes_db       = ?,
                updated_at             = NOW()
            WHERE file_name = ?
        """

    @staticmethod
    def sql_hook_processing_error() -> str:
        return """
            UPDATE cdr_registry
            SET processing_status = 'ERROR',
                notes             = ?,
                updated_at        = NOW()
            WHERE file_name = ?
        """

    @staticmethod
    def sql_hook_es_minutes() -> str:
        return """
            UPDATE cdr_registry
            SET total_minutes_es = ?,
                updated_at       = NOW()
            WHERE file_name = ?
        """

    @staticmethod
    def sql_hook_checksum_transferred() -> str:
        return """
            UPDATE cdr_registry
            SET checksum_transferred = ?,
                processing_status    = CASE
                                          WHEN checksum_compressed != ?
                                          THEN 'MISMATCH'
                                          ELSE processing_status
                                       END,
                updated_at           = NOW()
            WHERE file_name = ?
        """