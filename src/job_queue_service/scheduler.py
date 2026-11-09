"""Scheduler: periodically copies due pending jobs from Postgres into the Redis queue."""

import logging
import signal
import threading

import psycopg
import redis
from psycopg.rows import dict_row

from job_queue_service import db, redis_client
from job_queue_service.config import load_settings
from job_queue_service.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def relay_due_pending(conn: psycopg.Connection, r: redis.Redis) -> int:
    """Add every due pending job to the ready queue. Re-adding a job already there is harmless."""
    due = db.select_due_pending(conn)
    redis_client.enqueue(r, due)
    return len(due)


def main() -> None:
    settings = load_settings()
    setup_logging(settings.log_level)
    r = redis_client.connect(settings.redis_url)

    # A process running as PID 1 in a container ignores SIGTERM unless it installs a handler.
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    logger.info("scheduler started")
    with psycopg.connect(settings.database_url, autocommit=True, row_factory=dict_row) as conn:
        while not stop.is_set():
            relay_due_pending(conn, r)
            stop.wait(settings.relay_interval_seconds)


if __name__ == "__main__":
    main()
