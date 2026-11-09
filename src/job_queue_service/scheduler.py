"""Scheduler: promotes due scheduled jobs, relays due pending jobs to Redis, expires old keys."""

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


def run_relay_cycle(conn: psycopg.Connection, r: redis.Redis) -> int:
    """Promote scheduled jobs that are due, then relay all due pending jobs. Returns the relay count."""
    promoted = db.promote_scheduled(conn)
    if promoted:
        logger.info("promoted %d scheduled jobs to pending", len(promoted))
    return relay_due_pending(conn, r)


def main() -> None:
    settings = load_settings()
    setup_logging(settings.log_level)
    r = redis_client.connect(settings.redis_url)

    # A process running as PID 1 in a container ignores SIGTERM unless it installs a handler.
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    # The slower duties run every N relay cycles. Counting cycles means the scheduler never
    # has to read the clock itself, and a missed cycle only delays cleanup, which is harmless.
    cleanup_every = max(1, round(settings.idempotency_cleanup_seconds / settings.relay_interval_seconds))

    logger.info("scheduler started")
    with psycopg.connect(settings.database_url, autocommit=True, row_factory=dict_row) as conn:
        cycle = 0
        while not stop.is_set():
            run_relay_cycle(conn, r)
            if cycle % cleanup_every == 0:
                expired = db.expire_idempotency_keys(conn, settings.idempotency_ttl_hours)
                if expired:
                    logger.info("expired %d idempotency keys", expired)
            cycle += 1
            stop.wait(settings.relay_interval_seconds)


if __name__ == "__main__":
    main()
