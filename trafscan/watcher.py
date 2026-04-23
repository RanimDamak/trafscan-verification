"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A  |  Layer 1: File Watcher
────────────────────────────────────────────────────────────────────────────
Watches multiple directories, one per (operator, cdr_type) pair.

Folder structure on the frontale:
    /opt/backup/{operator}/{cdr_type}/

operator and cdr_type come from the config, NOT from the filename.
This is reliable because the folder path is the authoritative source:
each operator has its own DCS server, and files are organized into
type subfolders when they arrive on the frontale.

Each source entry in config.yaml becomes one watchdog Observer thread.

Integration contract
─────────────────────
Import `start_watcher(config, db_pool)` from your Trafscan entrypoint.
`config` is the loaded YAML config dict (see config.yaml).
`db_pool` is a psycopg2 connection pool shared by all layers.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from pathlib import Path

from watchdog.events import FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from trafscan.hasher import Hasher

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_stable(path: str, checks: int = 6, interval: float = 5.0) -> bool:
    """
    Poll file size until unchanged for `checks` consecutive reads.
    Returns True once stable; False if the file disappears mid-poll.
    """
    prev_size: int = -1
    stable_count: int = 0

    while stable_count < checks:
        try:
            size = os.path.getsize(path)
        except FileNotFoundError:
            log.warning("[watcher] File disappeared during stability check: %s", path)
            return False

        if size == prev_size:
            stable_count += 1
        else:
            stable_count = 0
            prev_size = size

        time.sleep(interval)

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Event Handler
# ─────────────────────────────────────────────────────────────────────────────

class CDRHandler(FileSystemEventHandler):
    """
    watchdog event handler for one (operator, cdr_type) source directory.

    operator and cdr_type are injected from config — derived from the
    folder path structure, never parsed from the filename.

    Parameters
    ----------
    hasher          : Hasher instance
    operator        : operator name (e.g. 'orange', 'atel', 'moov')
    cdr_type        : CDR type (e.g. 'in', 'msc', 'pgw', 'cnn', 'sdp')
    extensions      : file extensions to watch (e.g. ['.gz', '.dat', ''])
    stable_checks   : consecutive size-equal polls required
    stable_interval : seconds between polls
    extra_delay     : fixed extra wait after stability (seconds)
    sentinel_suffix : if set, wait for a companion .done file instead of polling
    """

    def __init__(
        self,
        hasher: Hasher,
        operator: str,
        cdr_type: str,
        extensions: list[str],
        stable_checks: int = 6,
        stable_interval: float = 5.0,
        extra_delay: float = 30.0,
        sentinel_suffix: str | None = None,
    ):
        super().__init__()
        self.hasher = hasher
        self.operator = operator
        self.cdr_type = cdr_type
        self.extensions = [e.lower() for e in extensions]
        self.stable_checks = stable_checks
        self.stable_interval = stable_interval
        self.extra_delay = extra_delay
        self.sentinel_suffix = sentinel_suffix
        self._in_progress: set[str] = set()

    # ── internal ──────────────────────────────────────────────────────────────

    def _is_target(self, path: str) -> bool:
        """
        Check if the file matches our extension list.
        Empty string '' in extensions means "no extension" (extensionless files).
        """
        lower = path.lower()
        for ext in self.extensions:
            if ext == "":
                # Match files with no extension at all
                if "." not in os.path.basename(lower):
                    return True
            else:
                if lower.endswith(ext):
                    return True
        return False

    def _wait_for_sentinel(self, path: str, poll: float = 1.0, timeout: float = 600.0) -> bool:
        sentinel = path + self.sentinel_suffix
        elapsed = 0.0
        while not os.path.exists(sentinel):
            time.sleep(poll)
            elapsed += poll
            if elapsed >= timeout:
                log.error("[watcher] Sentinel timeout for %s", path)
                return False
        try:
            os.remove(sentinel)
        except OSError:
            pass
        return True

    def _wait_ready(self, path: str) -> bool:
        if self.sentinel_suffix:
            log.info("[watcher] Waiting for sentinel %s%s", path, self.sentinel_suffix)
            return self._wait_for_sentinel(path)

        log.info("[watcher] Polling size stability for %s", path)
        stable = _is_stable(path, self.stable_checks, self.stable_interval)
        if not stable:
            return False

        log.info("[watcher] File stable. Applying extra delay (%ss).", self.extra_delay)
        time.sleep(self.extra_delay)
        return True

    # ── watchdog callback ─────────────────────────────────────────────────────

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.is_directory:
            return

        path: str = event.src_path

        if not self._is_target(path):
            log.debug("[watcher] Skipping (extension not watched): %s", path)
            return

        if path in self._in_progress:
            log.debug("[watcher] Already processing, skipping: %s", path)
            return

        self._in_progress.add(path)
        log.info(
            "[watcher] New CDR file detected: %s  |  operator=%s  type=%s",
            path, self.operator, self.cdr_type
        )

        try:
            ready = self._wait_ready(path)
            if not ready:
                log.error("[watcher] File not ready, skipping: %s", path)
                return

            file_name = os.path.basename(path)

            self.hasher.process(
                file_path=path,
                file_name=file_name,
                operator_id=self.operator,   # from folder path, always reliable
                cdr_type=self.cdr_type,       # from folder path, always reliable
            )

        except Exception as exc:
            log.exception("[watcher] Unhandled error processing %s: %s", path, exc)
        finally:
            self._in_progress.discard(path)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def start_watcher(config: dict, db_pool) -> None:
    """
    Start one watchdog Observer per source directory defined in config.
    Blocks until SIGINT / SIGTERM.

    config.yaml structure expected:
        watcher:
          stable_checks: 6
          stable_interval: 5.0
          extra_delay: 30.0
          sources:
            - directory: /opt/backup/orange/in
              operator:  orange
              cdr_type:  in
              extensions: [".gz", ".add.gz"]
            - directory: /opt/backup/atel/msc
              operator:  atel
              cdr_type:  msc
              extensions: [".dat", ".dat.gz", ""]
    """
    watch_cfg = config.get("watcher", {})
    stable_checks   = watch_cfg.get("stable_checks", 6)
    stable_interval = watch_cfg.get("stable_interval", 5.0)
    extra_delay     = watch_cfg.get("extra_delay", 30.0)
    sentinel_suffix = watch_cfg.get("sentinel_suffix", None)
    sources         = watch_cfg.get("sources", [])

    if not sources:
        log.critical("[watcher] No sources defined in config. Add at least one entry under watcher.sources.")
        sys.exit(1)

    hasher = Hasher(db_pool=db_pool, config=config)
    observers: list[Observer] = []

    for source in sources:
        directory = source.get("directory")
        operator  = source.get("operator")
        cdr_type  = source.get("cdr_type")
        extensions = source.get("extensions", [".gz"])

        if not directory or not operator or not cdr_type:
            log.error("[watcher] Source entry missing directory/operator/cdr_type: %s", source)
            continue

        if not os.path.isdir(directory):
            log.warning(
                "[watcher] Directory does not exist, skipping: %s  (operator=%s type=%s)",
                directory, operator, cdr_type
            )
            continue

        handler = CDRHandler(
            hasher=hasher,
            operator=operator,
            cdr_type=cdr_type,
            extensions=extensions,
            stable_checks=stable_checks,
            stable_interval=stable_interval,
            extra_delay=extra_delay,
            sentinel_suffix=sentinel_suffix,
        )

        observer = Observer()
        observer.schedule(handler, directory, recursive=False)
        observer.start()
        observers.append(observer)

        log.info(
            "[watcher] Watching: %s  |  operator=%-8s  type=%-5s  extensions=%s",
            directory, operator, cdr_type, extensions
        )

    if not observers:
        log.critical("[watcher] No valid source directories found. Check your config and that directories exist.")
        sys.exit(1)

    log.info("[watcher] %d source(s) active.", len(observers))

    def _shutdown(signum, frame):
        log.info("[watcher] Shutting down (signal %s)...", signum)
        for obs in observers:
            obs.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for obs in observers:
        obs.join()

    log.info("[watcher] All watchers stopped.")
