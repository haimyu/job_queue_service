import os
import threading
import time
from dataclasses import dataclass, replace
from uuid import uuid7

import psycopg
import pytest

from job_queue_service import db, handlers, redis_client, scheduler
from job_queue_service.redis_client import QUEUE_READY
from job_queue_service.worker import Worker

PAYLOADS = {
    "email": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
    "webhook": {"url": "https://example.com/hook"},
    "report": {"report_type": "sales"},
    "batch": {"items": [1, 2, 3, 4]},
}


def new_job(conn, r, job_type="email", max_attempts=3):
    """Insert a due pending job and put it in the Redis queue, like the API does."""
    job = db.insert_job(conn, uuid7(), job_type, PAYLOADS[job_type], 0, max_attempts)
    redis_client.enqueue(r, [job])
    return job["id"]


def log_messages(conn, job_id) -> list[str]:
    return [entry["message"] for entry in db.list_logs(conn, job_id)]


@dataclass
class ExitCall:
    thread: str
    at: float
    code: int


class Exited(BaseException):
    """Raised on the main thread instead of really exiting the process."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


@pytest.fixture(autouse=True)
def exit_calls(monkeypatch):
    """Replaces os._exit for every test here, so a bug can never kill the pytest process.

    Returns the list of calls, each with the name of the thread that made it. That matters:
    the main thread exits for lost ownership and timeouts, the lease watchdog thread exits
    for a missing heartbeat.
    """
    calls = []

    def exit_(code):
        calls.append(ExitCall(threading.current_thread().name, time.monotonic(), code))
        if threading.current_thread() is threading.main_thread():
            raise Exited(code)
        # A real exit never returns, in any thread. Park this daemon thread for good.
        threading.Event().wait()

    monkeypatch.setattr(os, "_exit", exit_)
    return calls


@pytest.fixture
def release():
    """Event that fake handlers wait on. Set at teardown so no handler thread stays blocked."""
    event = threading.Event()
    yield event
    event.set()


@pytest.fixture
def make_worker(settings):
    """Workers with their own connections, so tests can run handlers that use the test's own."""
    made = []

    def make(worker_id="w1", **overrides):
        w = Worker(replace(settings, **overrides), worker_id)
        made.append(w)
        return w

    yield make
    for w in made:
        w.conn.close()
        w.r.close()


def process_in_background(worker_under_test) -> threading.Thread:
    """Run one worker iteration in a thread, so the test can watch the watchdog from outside."""
    runner = threading.Thread(target=worker_under_test.process_next, daemon=True)
    runner.start()
    return runner


def blocking_handler(release):
    def handler(payload, report_progress, settings):
        release.wait(10)
        return {"never": "used"}

    return handler


# ---------- worker supervision (in-process, real Postgres and Redis) ----------


def test_heartbeat_writes_progress_extends_lease_and_refreshes_liveness(
    conn, r, make_worker, monkeypatch, wait_until
):
    job_id = new_job(conn, r)
    seen = {}

    def handler(payload, report_progress, settings):
        seen["lease_at_claim"] = db.get_job(conn, job_id)["lease_expires_at"]
        report_progress(40)
        # Only a heartbeat from the main thread can write this progress to Postgres.
        wait_until(lambda: db.get_job(conn, job_id)["progress"] == 40, timeout=5)
        seen["lease_after"] = db.get_job(conn, job_id)["lease_expires_at"]
        # process_next never refreshes the liveness key itself, only run() and heartbeats do.
        seen["alive"] = r.exists("worker:w1")
        return {"ok": True}

    monkeypatch.setitem(handlers.HANDLERS, "email", handler)

    make_worker(heartbeat_seconds=0.1, lease_seconds=5).process_next()

    assert db.get_job(conn, job_id)["status"] == "completed"
    assert seen["lease_after"] > seen["lease_at_claim"]
    assert seen["alive"] == 1


def test_heartbeat_that_matches_no_row_exits_without_writing(
    conn, r, make_worker, release, monkeypatch
):
    job_id = new_job(conn, r)

    def handler(payload, report_progress, settings):
        # Stands in for the reaper having handed the job to another worker.
        conn.execute(
            "UPDATE jobs SET locked_by = 'someone-else' WHERE id = %(id)s",
            {"id": job_id},
        )
        release.wait(10)
        return {"never": "used"}

    monkeypatch.setitem(handlers.HANDLERS, "email", handler)

    with pytest.raises(Exited) as exited:
        make_worker(heartbeat_seconds=0.1, lease_seconds=5).process_next()

    assert exited.value.code == 1
    job = db.get_job(conn, job_id)
    assert (job["status"], job["locked_by"]) == ("processing", "someone-else")
    assert job["error"] is None  # the exiting worker wrote nothing


def test_timeout_records_a_retryable_failure_then_exits(
    conn, r, make_worker, release, monkeypatch
):
    job_id = new_job(conn, r, "webhook")
    monkeypatch.setitem(handlers.HANDLERS, "webhook", blocking_handler(release))

    with pytest.raises(Exited) as exited:
        make_worker(
            job_timeouts={"webhook": 0.3}, heartbeat_seconds=0.1, lease_seconds=5
        ).process_next()

    assert exited.value.code == 1
    job = db.get_job(conn, job_id)
    assert job["status"] == "pending"  # retry scheduled, not left for the reaper
    assert job["attempts"] == 1
    assert job["locked_by"] is None
    assert "timed out after 0.3 seconds" in job["error"]
    assert log_messages(conn, job_id) == [
        "job created",
        "job claimed",
        "attempt failed",
        "retry scheduled",
    ]


def test_batch_without_progress_is_stalled_then_exits(
    conn, r, make_worker, release, monkeypatch
):
    job_id = new_job(conn, r, "batch")

    def stuck(payload, report_progress, settings):
        report_progress(10)
        release.wait(10)

    monkeypatch.setitem(handlers.HANDLERS, "batch", stuck)

    with pytest.raises(Exited):
        make_worker(
            stall_seconds=0.3, heartbeat_seconds=0.1, lease_seconds=5
        ).process_next()

    job = db.get_job(conn, job_id)
    assert job["status"] == "pending"
    assert "stalled" in job["error"]


def test_batch_that_keeps_making_progress_is_not_stalled(conn, r, make_worker):
    job_id = new_job(conn, r, "batch")  # 4 items, each takes 0.1 to 0.3 s

    make_worker(
        job_sleep_scale=1.0, stall_seconds=1.5, heartbeat_seconds=0.1, lease_seconds=5
    ).process_next()

    assert db.get_job(conn, job_id)["status"] == "completed"


def test_worker_exits_before_its_lease_expires_when_heartbeats_keep_failing(
    conn, r, make_worker, exit_calls, release, monkeypatch, wait_until
):
    job_id = new_job(conn, r)
    monkeypatch.setitem(handlers.HANDLERS, "email", blocking_handler(release))
    attempts = []

    def broken_heartbeat(*args, **kwargs):
        attempts.append(1)
        raise psycopg.OperationalError("simulated outage")

    monkeypatch.setattr(db, "heartbeat_job", broken_heartbeat)
    # The last good heartbeat is the claim, so the worker must stop lease - heartbeat = 1.6 s later.
    worker_under_test = make_worker(heartbeat_seconds=0.4, lease_seconds=2.0)

    started = time.monotonic()
    runner = process_in_background(worker_under_test)
    wait_until(lambda: exit_calls, timeout=5, message="the worker never stopped itself")
    job = db.get_job(conn, job_id)  # the state at the moment of the exit
    release.set()
    runner.join(5)

    assert (exit_calls[0].thread, exit_calls[0].code) == ("lease-watchdog", 1)
    assert len(attempts) >= 3  # failed heartbeats were retried, not an immediate exit
    assert 1.6 <= exit_calls[0].at - started < 2.0  # gone before the lease could expire
    assert (job["status"], job["attempts"]) == ("processing", 1)
    assert job["error"] is None  # nothing was written by the dying worker


def test_worker_exits_before_its_lease_expires_even_when_a_heartbeat_blocks(
    conn, r, make_worker, exit_calls, release, monkeypatch, wait_until
):
    """A frozen Postgres keeps the connection open: the heartbeat neither returns nor raises."""
    job_id = new_job(conn, r)
    monkeypatch.setitem(handlers.HANDLERS, "email", blocking_handler(release))
    frozen = threading.Event()
    heartbeat_returned = []

    def frozen_heartbeat(*args, **kwargs):
        frozen.wait(10)
        heartbeat_returned.append(True)
        return 1

    monkeypatch.setattr(db, "heartbeat_job", frozen_heartbeat)
    worker_under_test = make_worker(heartbeat_seconds=0.4, lease_seconds=2.0)

    started = time.monotonic()
    runner = process_in_background(worker_under_test)
    wait_until(lambda: exit_calls, timeout=5, message="the worker never stopped itself")
    # The main thread is still inside the heartbeat call: the exit did not wait for it.
    main_thread_still_blocked = runner.is_alive() and not heartbeat_returned
    job = db.get_job(conn, job_id)
    frozen.set()
    release.set()
    runner.join(5)

    assert main_thread_still_blocked
    assert (exit_calls[0].thread, exit_calls[0].code) == ("lease-watchdog", 1)
    assert 1.6 <= exit_calls[0].at - started < 2.0  # gone before the lease could expire
    assert (job["status"], job["attempts"]) == ("processing", 1)


def test_a_failed_heartbeat_is_retried_and_the_job_still_completes(
    conn, r, make_worker, monkeypatch, wait_until
):
    job_id = new_job(conn, r)
    real_heartbeat = db.heartbeat_job
    calls = []

    def flaky_heartbeat(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise psycopg.OperationalError("simulated blip")
        return real_heartbeat(*args, **kwargs)

    def handler(payload, report_progress, settings):
        wait_until(
            lambda: len(calls) >= 2, timeout=5
        )  # lives through the failure and the retry
        return {"ok": True}

    monkeypatch.setattr(db, "heartbeat_job", flaky_heartbeat)
    monkeypatch.setitem(handlers.HANDLERS, "email", handler)

    make_worker(heartbeat_seconds=0.1, lease_seconds=3).process_next()

    assert db.get_job(conn, job_id)["status"] == "completed"
    assert len(calls) >= 2


def test_popped_job_is_not_claimed_once_shutdown_is_requested(conn, r, make_worker):
    job_id = new_job(conn, r)
    worker_under_test = make_worker()
    worker_under_test.shutdown.set()

    assert worker_under_test.process_next() is None

    job = db.get_job(conn, job_id)
    assert (job["status"], job["attempts"]) == ("pending", 0)
    assert r.zscore(QUEUE_READY, str(job_id)) is None  # popped from Redis
    assert scheduler.relay_due_pending(conn, r) == 1  # and the relay puts it back
