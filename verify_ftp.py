"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A  |  FTP Transfer Verification
────────────────────────────────────────────────────────────────────────────
After a CDR file is transferred to the regulator via FTP, this script:
  1. Re-downloads the file from the regulator (or uses a local copy for testing)
  2. Computes its SHA-256
  3. Compares it to checksum_compressed in the registry
  4. Updates checksum_transferred in the registry
  5. Sets status to MISMATCH if hashes differ

In production: run this after each FTP transfer, passing the file that
was actually received on the regulator side (re-downloaded via FTP/SFTP).

For testing: pass a local file path with --local to simulate a transfer.
If you pass the same file → checksums match → transfer OK.
If you pass a different file → checksums differ → MISMATCH detected.

Usage:
    # Test with local file (simulates perfect transfer)
    python verify_ftp.py --file cbs_cdr_vou_20250401_601_101_528010.add.gz --local "C:/cdr_test/atel/in/cbs_cdr_vou_20250401_601_101_528010.add.gz"

    # Production: file downloaded from regulator
    python verify_ftp.py --file cbs_cdr_vou_20250401_601_101_528010.add.gz --local "C:/downloads/received.gz"
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import yaml
import psycopg2


_CHUNK = 8192


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def get_conn(config: dict):
    db = config.get("database", {})
    return psycopg2.connect(
        host=db.get("host", "localhost"),
        port=int(db.get("port", 5432)),
        dbname=db.get("dbname", "trafscan"),
        user=db.get("user", "trafscan_user"),
        password=db.get("password", ""),
    )


def verify_transfer(conn, file_name: str, received_path: str):
    """
    Compute SHA-256 of the received file and compare to checksum_compressed.
    Updates checksum_transferred and flags MISMATCH if they differ.
    """
    # ── Get the expected checksum from registry ───────────────────────────────
    with conn.cursor() as cur:
        cur.execute("""
            SELECT file_id, checksum_raw, checksum_compressed, processing_status
            FROM cdr_registry
            WHERE file_name = %s
        """, (file_name,))
        row = cur.fetchone()

    if not row:
        print(f"[ftp-verify] ERROR: file not found in registry: {file_name}")
        sys.exit(1)

    file_id, checksum_raw, checksum_compressed, current_status = row

    # Use checksum_raw as fallback if checksum_compressed is NULL
    # (happens when compress_after_hash=false — the file is transferred as-is)
    expected_checksum = checksum_compressed if checksum_compressed else checksum_raw
    checksum_stage = "compressed" if checksum_compressed else "raw"

    print(f"[ftp-verify] File: {file_name}")
    print(f"[ftp-verify] Expected checksum ({checksum_stage}): {expected_checksum}")

    # ── Compute SHA-256 of the received file ──────────────────────────────────
    try:
        received_checksum = sha256_file(received_path)
    except FileNotFoundError:
        print(f"[ftp-verify] ERROR: received file not found: {received_path}")
        sys.exit(1)

    print(f"[ftp-verify] Received checksum:           {received_checksum}")

    # ── Compare and update registry ───────────────────────────────────────────
    match = received_checksum == expected_checksum

    with conn.cursor() as cur:
        if match:
            # Transfer OK — just store the checksum, keep current status
            cur.execute("""
                UPDATE cdr_registry
                SET checksum_transferred = %s,
                    updated_at           = NOW()
                WHERE file_name = %s
            """, (received_checksum, file_name))
            conn.commit()
            print(f"[ftp-verify] ✓ MATCH — Transfer integrity confirmed.")
            print(f"[ftp-verify] Status unchanged: {current_status}")
        else:
            # Transfer corrupted — set MISMATCH regardless of current status
            cur.execute("""
                UPDATE cdr_registry
                SET checksum_transferred = %s,
                    processing_status    = 'MISMATCH'::processing_status,
                    notes                = COALESCE(notes || ' | ', '') || %s,
                    updated_at           = NOW()
                WHERE file_name = %s
            """, (
                received_checksum,
                f"FTP transfer corruption: expected {expected_checksum[:16]}... got {received_checksum[:16]}...",
                file_name
            ))
            conn.commit()
            print(f"[ftp-verify] ✗ MISMATCH — File corrupted during transfer!")
            print(f"[ftp-verify] Status → MISMATCH")
            print(f"[ftp-verify] Delta: expected={expected_checksum}")
            print(f"[ftp-verify]        received={received_checksum}")


def main():
    parser = argparse.ArgumentParser(description="Trafscan FTP Transfer Verifier")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--file",   required=True,
                        help="file_name as stored in cdr_registry")
    parser.add_argument("--local",  required=True,
                        help="path to the file received on regulator side (local path for testing)")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    conn = get_conn(config)
    try:
        verify_transfer(conn, args.file, args.local)
    finally:
        conn.close()

    print("\n[ftp-verify] Check registry:")
    print(f'  psql -U trafscan_user -d trafscan -h localhost -c "SELECT file_name, processing_status, checksum_raw, checksum_transferred FROM cdr_registry WHERE file_name = \'{args.file}\';"')


if __name__ == "__main__":
    main()
