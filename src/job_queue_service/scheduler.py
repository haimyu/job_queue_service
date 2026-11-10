"""Scheduler: promotes scheduled jobs, relays pending jobs to Redis, reaps dead leases, expires keys."""

import logging
import signal
import threading
import time

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

    logger.info("scheduler started")
    with psycopg.connect(
        settings.database_url, autocommit=True, row_factory=dict_row
    ) as conn:
        # time.monotonic() only paces this loop: when each duty runs next. It never decides job
        # state. Every "is this job due or expired" question is answered by Postgres now().
        next_relay = next_reaper = next_cleanup = time.monotonic()
        while not stop.is_set():
            now = time.monotonic()
            if now >= next_relay:
                run_relay_cycle(conn, r)
                next_relay = now + settings.relay_interval_seconds
            if now >= next_reaper:
                for job in db.reap_expired_leases(conn):
                    logger.warning(
                        "lease expired, job reaped to %s",
                        job["status"],
                        extra={"job_id": job["id"]},
                    )
                next_reaper = now + settings.reaper_interval_seconds
            if now >= next_cleanup:
                expired = db.expire_idempotency_keys(
                    conn, settings.idempotency_ttl_hours
                )
                if expired:
                    logger.info("expired %d idempotency keys", expired)
                next_cleanup = now + settings.idempotency_cleanup_seconds
            # Sleep until the earliest duty is due. Event.wait returns early on SIGTERM.
            stop.wait(
                max(0.0, min(next_relay, next_reaper, next_cleanup) - time.monotonic())
            )


if __name__ == "__main__":
    main()
