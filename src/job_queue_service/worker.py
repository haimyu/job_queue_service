"""Worker: pops a job id from Redis, claims it in Postgres, runs it, and records the outcome."""

import logging
import os
import secrets
import socket
from uuid import UUID

import psycopg
import redis
from psycopg.rows import dict_row

from job_queue_service import db, handlers, redis_client
from job_queue_service.config import Settings, load_settings
from job_queue_service.handlers import PermanentJobError, RetryableJobError
from job_queue_service.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}"


def record_failure(
    conn: psycopg.Connection,
    settings: Settings,
    job: dict,
    worker_id: str,
    error: Exception,
    context: dict,
) -> None:
    """Record a failed attempt: retry with backoff, or fail the job for good.

    Any exception except PermanentJobError is treated as retryable.
    """
    attempt = job["attempts"]
    # attempts counts the run that just failed, so attempt 1 uses the first delay. A job with
    # more attempts than configured delays keeps using the last delay.
    delay = settings.backoff_seconds[min(attempt, len(settings.backoff_seconds)) - 1]
    message = f"{type(error).__name__}: {error}"
    status = db.fail_job(
        conn, job["id"], worker_id, attempt, message, delay, isinstance(error, PermanentJobError)
    )
    if status is None:
        # Nothing is running any more, so the worker drops the failure and carries on.
        logger.warning("ownership lost, discarding failure", extra=context)
        return
    # Stack traces are only useful for unexpected exceptions, not for the expected error types.
    expected = isinstance(error, (RetryableJobError, PermanentJobError))
    outcome = "retry scheduled" if status == "pending" else "permanently failed"
    logger.warning(
        "job attempt failed, %s: %s", outcome, message, extra=context, exc_info=None if expected else error
    )


def process_next(
    conn: psycopg.Connection, r: redis.Redis, settings: Settings, worker_id: str
) -> UUID | None:
    """Run one loop iteration. Returns the id of the job that was run, or None if there was none."""
    raw_id = redis_client.pop_ready(r, settings.pop_timeout_seconds)
    if raw_id is None:
        return None
    try:
        job_id = UUID(raw_id)
    except ValueError:
        logger.warning("ignoring invalid job id in queue: %r", raw_id, extra={"worker_id": worker_id})
        return None

    job = db.claim_job(conn, job_id, worker_id, settings.lease_seconds)
    if job is None:
        # The id was stale: the job was cancelled, already claimed, or is not due yet.
        logger.info("skipping stale queue entry", extra={"job_id": job_id, "worker_id": worker_id})
        return None

    context = {
        "job_id": job["id"],
        "job_type": job["type"],
        "attempt": job["attempts"],
        "worker_id": worker_id,
    }
    logger.info("job claimed", extra=context)

    # The handler runs in this thread until a later phase moves it to a thread with heartbeats.
    # Whatever it raises must not kill the worker, so every exception goes through the failure path.
    try:
        result = handlers.run_handler(job["type"], job["payload"], lambda percent: None, settings)
    except Exception as error:  # noqa: BLE001 - a handler must never kill the worker
        record_failure(conn, settings, job, worker_id, error, context)
        return job["id"]

    rows = db.complete_job(conn, job["id"], worker_id, job["attempts"], result)
    if rows == 0:
        # Ownership was lost while the handler ran. Nothing is running now, so the
        # worker discards its result and carries on with the next job.
        logger.warning("ownership lost, discarding result", extra=context)
    else:
        logger.info("job completed", extra=context)
    return job["id"]


def main() -> None:
    settings = load_settings()
    setup_logging(settings.log_level)
    worker_id = make_worker_id()
    logger.info("worker started", extra={"worker_id": worker_id})
    r = redis_client.connect(settings.redis_url)
    with psycopg.connect(settings.database_url, autocommit=True, row_factory=dict_row) as conn:
        while True:
            process_next(conn, r, settings, worker_id)


if __name__ == "__main__":
    main()
