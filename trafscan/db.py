"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A  |  Database helpers
────────────────────────────────────────────────────────────────────────────
Provides a psycopg2 connection pool configured from config.yaml.

Usage:
    from trafscan.db import create_pool, get_connection

    pool = create_pool(config)          # at startup
    conn = pool.getconn()               # borrow
    pool.putconn(conn)                  # return
"""

from __future__ import annotations

import logging
from typing import Optional

import psycopg2
from psycopg2 import pool as pg_pool

log = logging.getLogger(__name__)


def create_pool(config: dict) -> pg_pool.ThreadedConnectionPool:
    """
    Create a threaded connection pool from config.yaml's `database` section.

    config.yaml example:
        database:
          host: localhost
          port: 5432
          dbname: trafscan
          user: trafscan_user
          password: secret
          pool_min: 2
          pool_max: 10
    """
    db_cfg = config.get("database", {})
    dsn = {
        "host":     db_cfg.get("host", "localhost"),
        "port":     int(db_cfg.get("port", 5432)),
        "dbname":   db_cfg.get("dbname", "trafscan"),
        "user":     db_cfg.get("user", "trafscan"),
        "password": db_cfg.get("password", ""),
    }
    min_conn = int(db_cfg.get("pool_min", 2))
    max_conn = int(db_cfg.get("pool_max", 10))

    log.info("[db] Connecting to PostgreSQL at %s:%s/%s", dsn["host"], dsn["port"], dsn["dbname"])
    pool = pg_pool.ThreadedConnectionPool(min_conn, max_conn, **dsn)
    log.info("[db] Connection pool ready (min=%d, max=%d)", min_conn, max_conn)
    return pool


def get_connection(pool: pg_pool.ThreadedConnectionPool):
    """Borrow a connection from the pool (context manager not needed – caller must putconn)."""
    return pool.getconn()
