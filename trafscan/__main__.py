"""
────────────────────────────────────────────────────────────────────────────
Entry-point:  python -m trafscan  [--config config.yaml]
────────────────────────────────────────────────────────────────────────────
"""
import argparse
import logging
import sys

import yaml

from trafscan.db import create_pool
from trafscan.watcher import start_watcher


def setup_logging(cfg: dict) -> None:
    log_cfg = cfg.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file = log_cfg.get("file", None)

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        import os
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trafscan CDR Registry – File Watcher (Solution A)"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    args = parser.parse_args()

    with open(args.config) as fh:
        config = yaml.safe_load(fh)

    setup_logging(config)
    log = logging.getLogger("trafscan")
    log.info("Trafscan Solution A starting up...")

    db_pool = create_pool(config)
    start_watcher(config, db_pool)


if __name__ == "__main__":
    main()
