"""
tests/test_hasher.py
────────────────────────────────────────────────────────────────────────────
Unit tests for trafscan.hasher – run without a live DB or DCS.

Run:
    pytest tests/test_hasher.py -v
────────────────────────────────────────────────────────────────────────────
"""

import gzip
import hashlib
import os
import tempfile

import pytest

from trafscan.hasher import sha256_file, count_lines, compress_gzip
from trafscan.watcher import extract_operator


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def tmp_plain_file(tmp_path):
    """A plain-text file with 100 lines."""
    f = tmp_path / "CDR_20250412_TUNISIE.txt"
    f.write_text("\n".join(f"record_{i}" for i in range(100)) + "\n")
    return str(f)


@pytest.fixture()
def tmp_gz_file(tmp_path):
    """A gzip-compressed file with 200 lines."""
    raw = "\n".join(f"record_{i}" for i in range(200)) + "\n"
    gz = tmp_path / "CDR_20250412_MAROC.gz"
    with gzip.open(str(gz), "wt") as fh:
        fh.write(raw)
    return str(gz)


# ─────────────────────────────────────────────────────────────────────────────
# sha256_file
# ─────────────────────────────────────────────────────────────────────────────

class TestSha256:
    def test_returns_64_char_hex(self, tmp_plain_file):
        result = sha256_file(tmp_plain_file)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self, tmp_plain_file):
        assert sha256_file(tmp_plain_file) == sha256_file(tmp_plain_file)

    def test_matches_hashlib_reference(self, tmp_plain_file):
        expected = hashlib.sha256(open(tmp_plain_file, "rb").read()).hexdigest()
        assert sha256_file(tmp_plain_file) == expected

    def test_different_files_different_hash(self, tmp_path):
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_text("hello")
        b.write_text("world")
        assert sha256_file(str(a)) != sha256_file(str(b))

    def test_file_not_found(self):
        with pytest.raises(OSError):
            sha256_file("/nonexistent/path.gz")


# ─────────────────────────────────────────────────────────────────────────────
# count_lines
# ─────────────────────────────────────────────────────────────────────────────

class TestCountLines:
    def test_plain_100_lines(self, tmp_plain_file):
        assert count_lines(tmp_plain_file) == 100

    def test_gz_200_lines(self, tmp_gz_file):
        assert count_lines(tmp_gz_file) == 200

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_bytes(b"")
        assert count_lines(str(f)) == 0

    def test_single_line_no_trailing_newline(self, tmp_path):
        f = tmp_path / "one.txt"
        f.write_text("single record")  # no trailing \n
        # count_lines counts newline characters; no trailing → 0 counted
        # This is expected behaviour: wc -l agrees.
        assert count_lines(str(f)) == 0

    def test_single_line_with_newline(self, tmp_path):
        f = tmp_path / "one.txt"
        f.write_text("single record\n")
        assert count_lines(str(f)) == 1


# ─────────────────────────────────────────────────────────────────────────────
# extract_operator
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractOperator:
    def test_standard_pattern(self):
        assert extract_operator("CDR_20250412_TUNISIE.gz") == "TUNISIE"

    def test_maroc(self):
        assert extract_operator("CDR_20250412_MAROC.gz") == "MAROC"

    def test_no_match_returns_unknown(self):
        assert extract_operator("some_random_file.txt") == "UNKNOWN"

    def test_custom_regex(self):
        result = extract_operator(
            "OP-ALGERIE-20250412.gz",
            pattern=r"OP-([A-Z]+)-",
        )
        assert result == "ALGERIE"


# ─────────────────────────────────────────────────────────────────────────────
# compress_gzip
# ─────────────────────────────────────────────────────────────────────────────

class TestCompressGzip:
    def test_creates_gz_file(self, tmp_plain_file, tmp_path):
        dst = str(tmp_path / "output.gz")
        result = compress_gzip(tmp_plain_file, dst)
        assert os.path.exists(result)

    def test_compressed_is_decompressible(self, tmp_plain_file, tmp_path):
        dst = str(tmp_path / "output.gz")
        compress_gzip(tmp_plain_file, dst)
        with gzip.open(dst, "rt") as fh:
            content = fh.read()
        assert "record_0" in content

    def test_compressed_hash_differs_from_raw(self, tmp_plain_file, tmp_path):
        dst = str(tmp_path / "output.gz")
        compress_gzip(tmp_plain_file, dst)
        assert sha256_file(tmp_plain_file) != sha256_file(dst)
