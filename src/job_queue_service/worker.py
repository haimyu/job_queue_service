"""Worker: pops a job id from Redis, claims it in Postgres, runs it, and records the outcome.

Threads. The main thread does everything that touches Postgres or Redis: pop, claim,
heartbeat and finalize. The handler runs alone in a daemon thread and talks to the main
thread only through a HandlerRun object (progress while it runs, result or error when it ends).

Supervision. While the handler runs, the main thread wakes up at least every
HEARTBEAT_SECONDS to extend the lease. It also enforces the per-type timeout and, for jobs
without a timeout, the stall limit. When the worker can no longer be sure it owns the job it
calls os._exit(1): Python cannot kill a thread, and exiting the process is the only reliable
way to stop a hung or orphaned handler. Docker restarts the worker.

Self-fencing. A third thread, the LeaseWatchdog, stops the process if no heartbeat has
succeeded for LEASE_SECONDS - HEARTBEAT_SECONDS. It must be separate from the main thread: a
frozen database keeps the TCP connection open, so a heartbeat can block forever without
raising, and a check placed after the heartbeat call would only run once the database thaws.

Clock. time.monotonic() is used here only as a local stopwatch for those deadlines. Everything
stored in Postgres (leases, due times, backoff) comes from Postgres now().
"""

import contextlib
import logging
import os
import secrets
import signal
import socket
import threading
import time
from dataclasses import dataclass
from typing import NoReturn
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


@dataclass
class HandlerRun:
    """State shared between the main thread and the handler thread.

    The handler thread writes progress, then either result or error once. The main thread
    only reads them. Each field is a single assignment, which is atomic in Python.
    """

    progress: int = 0
    result: dict | None = None
    error: Exception | None = None

    def report_progress(self, percent: int) -> None:
        self.progress = percent


class LeaseWatchdog:
    """Stops the whole process if the lease could expire without a successful heartbeat.

    The reaper may give the job to another worker once the lease expires, so this worker must
    be gone before that, with one heartbeat interval of margin. The watchdog runs in its own
    thread and never touches Postgres or Redis, so it still fires when the main thread is stuck
    inside a database call. It only reads and writes one float, which is atomic in Python.
    """

    def __init__(self, started: float, max_silence: float, context: dict) -> None:
        # Monotonic time of the last successful heartbeat. The claim counts as the first one.
        self.last_heartbeat = started
        self.max_silence = max_silence
        self.context = context
        self._stopped = threading.Event()

    def start(self) -> None:
        threading.Thread(target=self._watch, daemon=True, name="lease-watchdog").start()

    def stop(self) -> None:
        self._stopped.set()

    def heartbeat_succeeded(self, sent_at: float) -> None:
        self.last_heartbeat = sent_at

    def _watch(self) -> None:
        while True:
            remaining = self.last_heartbeat + self.max_silence - time.monotonic()
            if remaining <= 0:
                logger.error(
                    "no successful heartbeat before the lease could expire, exiting",
                    extra=self.context,
                )
                os._exit(1)
            # Wake at the current deadline. A heartbeat that succeeds meanwhile moves the
            # deadline, and the next pass of the loop waits for the new one.
            if self._stopped.wait(remaining):
                return


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
        conn,
        job["id"],
        worker_id,
        attempt,
        message,
        delay,
        isinstance(error, PermanentJobError),
    )
    if status is None:
        # Nothing is running any more, so the worker drops the failure and carries on.
        logger.warning("ownership lost, discarding failure", extra=context)
        return
    # Stack traces are only useful for unexpected exceptions, not for the expected error types.
    expected = isinstance(error, (RetryableJobError, PermanentJobError))
    outcome = "retry scheduled" if status == "pending" else "permanently failed"
    logger.warning(
        "job attempt failed, %s: %s",
        outcome,
        message,
        extra=context,
        exc_info=None if expected else error,
    )


class Worker:
    def __init__(
        self,
        settings: Settings,
        worker_id: str,
        conn: psycopg.Connection | None = None,
        r: redis.Redis | None = None,
    ) -> None:
        self.settings = settings
        self.worker_id = worker_id
        # Set by the SIGTERM/SIGINT handler. The worker finishes its current job and then stops.
        self.shutdown = threading.Event()
        self.conn = conn if conn is not None else self._connect_db()
        self.r = r if r is not None else redis_client.connect(settings.redis_url)

    def _connect_db(self) -> psycopg.Connection:
        return psycopg.connect(
            self.settings.database_url, autocommit=True, row_factory=dict_row
        )

    def _reconnect_db(self) -> None:
        """Replace the Postgres connection. If Postgres is still down, the next statement fails
        and the caller retries later. (redis-py reconnects by itself on the next command.)"""
        with contextlib.suppress(psycopg.Error):
            self.conn.close()
        try:
            self.conn = self._connect_db()
        except psycopg.Error:
            logger.warning(
                "could not reconnect to postgres yet",
                extra={"worker_id": self.worker_id},
            )

    def run(self) -> None:
        """Process jobs until shutdown is requested."""
        logger.info("worker started", extra={"worker_id": self.worker_id})
        while not self.shutdown.is_set():
            try:
                redis_client.refresh_worker(self.r, self.worker_id)
                self.process_next()
            except psycopg.Error, redis.RedisError:
                logger.exception(
                    "connection error, reconnecting",
                    extra={"worker_id": self.worker_id},
                )
                # Event.wait, not time.sleep, so that SIGTERM ends the pause at once.
                self.shutdown.wait(1)
                self._reconnect_db()
        with contextlib.suppress(redis.RedisError):
            redis_client.remove_worker(self.r, self.worker_id)
        logger.info("worker stopped", extra={"worker_id": self.worker_id})

    def process_next(self) -> UUID | None:
        """Run one loop iteration. Returns the id of the job that was run, or None if there was none."""
        raw_id = redis_client.pop_ready(self.r, self.settings.pop_timeout_seconds)
        if raw_id is None:
            return None
        try:
            job_id = UUID(raw_id)
        except ValueError:
            logger.warning(
                "ignoring invalid job id in queue: %r",
                raw_id,
                extra={"worker_id": self.worker_id},
            )
            return None

        if self.shutdown.is_set():
            # The job stays pending. The scheduler relay puts it back in Redis for another worker.
            logger.info(
                "shutdown requested, not claiming popped job",
                extra={"job_id": job_id, "worker_id": self.worker_id},
            )
            return None

        # Taken before the claim statement runs: the lease is measured from Postgres now(), which
        # is no earlier than this moment, so the worker's own deadline can only be too early.
        lease_started = time.monotonic()
        job = db.claim_job(
            self.conn, job_id, self.worker_id, self.settings.lease_seconds
        )
        if job is None:
            # The id was stale: the job was cancelled, already claimed, or is not due yet.
            logger.info(
                "skipping stale queue entry",
                extra={"job_id": job_id, "worker_id": self.worker_id},
            )
            return None

        context = {
            "job_id": job["id"],
            "job_type": job["type"],
            "attempt": job["attempts"],
            "worker_id": self.worker_id,
        }
        logger.info("job claimed", extra=context)

        # Started before the handler, so a claim that returned too late to be safe is caught at once.
        watchdog = LeaseWatchdog(
            lease_started,
            self.settings.lease_seconds - self.settings.heartbeat_seconds,
            context,
        )
        watchdog.start()
        run = HandlerRun()
        thread = threading.Thread(
            target=self._run_handler,
            args=(job, run),
            daemon=True,
            name=f"job-{job['id']}",
        )
        thread.start()
        try:
            self._supervise(job, run, thread, watchdog, context)
        finally:
            # Once the handler has ended nothing is running, so there is nothing left to fence.
            watchdog.stop()
        self._finalize(job, run, context)
        return job["id"]

    def _run_handler(self, job: dict, run: HandlerRun) -> None:
        """Thread body. Whatever the handler raises is stored, never allowed to escape."""
        try:
            run.result = handlers.run_handler(
                job["type"], job["payload"], run.report_progress, self.settings
            )
        except Exception as error:  # noqa: BLE001 - a handler must never kill the worker
            run.error = error

    def _supervise(
        self,
        job: dict,
        run: HandlerRun,
        thread: threading.Thread,
        watchdog: LeaseWatchdog,
        context: dict,
    ) -> None:
        """Wait for the handler thread while keeping the lease alive. Returns when it has ended."""
        settings = self.settings
        started = time.monotonic()
        # A type without a timeout (batch) is watched for stalls instead.
        timeout = settings.job_timeouts.get(job["type"])
        next_heartbeat = started + settings.heartbeat_seconds
        last_progress, progress_changed_at = run.progress, started

        while True:
            deadlines = [next_heartbeat]
            if timeout is not None:
                deadlines.append(started + timeout)
            else:
                deadlines.append(progress_changed_at + settings.stall_seconds)
            thread.join(max(0.0, min(deadlines) - time.monotonic()))
            if not thread.is_alive():
                return
            now = time.monotonic()

            if timeout is not None and now - started >= timeout:
                self._abort(
                    job,
                    RetryableJobError(f"job timed out after {timeout:g} seconds"),
                    context,
                )
            if run.progress != last_progress:
                last_progress, progress_changed_at = run.progress, now
            elif (
                timeout is None and now - progress_changed_at >= settings.stall_seconds
            ):
                self._abort(
                    job,
                    RetryableJobError(
                        f"job stalled: no progress for {settings.stall_seconds:g} seconds"
                    ),
                    context,
                )

            if now >= next_heartbeat:
                sent = time.monotonic()
                if self._heartbeat(job, run, context):
                    watchdog.heartbeat_succeeded(sent)
                next_heartbeat = sent + settings.heartbeat_seconds

    def _heartbeat(self, job: dict, run: HandlerRun, context: dict) -> bool:
        """Extend the lease and store progress. Returns False if Postgres could not be reached."""
        try:
            redis_client.refresh_worker(self.r, self.worker_id)
        except redis.RedisError:
            logger.warning("could not refresh the worker liveness key", extra=context)
        try:
            rows = db.heartbeat_job(
                self.conn,
                job["id"],
                self.worker_id,
                job["attempts"],
                run.progress,
                self.settings.lease_seconds,
            )
        except psycopg.Error:
            logger.warning(
                "heartbeat failed, reconnecting and retrying at the next wake-up",
                extra=context,
                exc_info=True,
            )
            self._reconnect_db()
            return False
        if rows == 0:
            # The handler is still running on a job this worker no longer owns.
            logger.error("ownership lost, exiting", extra=context)
            os._exit(1)
        return True

    def _abort(self, job: dict, error: Exception, context: dict) -> NoReturn:
        """Record a retryable failure for a job whose handler is stuck, then kill the process.

        The failure is written first so the job retries with backoff instead of waiting for
        the lease to expire. The write is fenced, so it does nothing if ownership was lost.
        """
        try:
            record_failure(
                self.conn, self.settings, job, self.worker_id, error, context
            )
        except psycopg.Error:
            logger.exception(
                "could not record the failure, the reaper will recover the job",
                extra=context,
            )
        os._exit(1)

    def _finalize(self, job: dict, run: HandlerRun, context: dict) -> None:
        """Write the outcome of a handler that has ended."""
        if run.error is None and run.result is not None:
            rows = db.complete_job(
                self.conn, job["id"], self.worker_id, job["attempts"], run.result
            )
            if rows == 0:
                # Ownership was lost while the handler ran. Nothing is running now, so the
                # worker discards its result and carries on with the next job.
                logger.warning("ownership lost, discarding result", extra=context)
            else:
                logger.info("job completed", extra=context)
            return
        error = run.error or RuntimeError("handler ended without a result")
        record_failure(self.conn, self.settings, job, self.worker_id, error, context)


def process_next(
    conn: psycopg.Connection, r: redis.Redis, settings: Settings, worker_id: str
) -> UUID | None:
    """Run one iteration on connections the caller supplies. Used by tests."""
    return Worker(settings, worker_id, conn, r).process_next()


def main() -> None:
    settings = load_settings()
    setup_logging(settings.log_level)
    worker = Worker(settings, make_worker_id())
    # A process running as PID 1 in a container ignores SIGTERM unless it installs a handler.
    signal.signal(signal.SIGTERM, lambda *_: worker.shutdown.set())
    signal.signal(signal.SIGINT, lambda *_: worker.shutdown.set())
    worker.run()


if __name__ == "__main__":
    main()
