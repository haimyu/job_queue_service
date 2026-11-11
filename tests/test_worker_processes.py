import signal
import sys
import time
from datetime import UTC, datetime
from uuid import uuid7

import pytest

from job_queue_service import db, redis_client, scheduler

skip_on_windows = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows has no inter-process SIGTERM: Popen.send_signal(SIGTERM) calls "
    "TerminateProcess, which hard-kills with exit code 1, so graceful shutdown "
    "cannot be exercised.",
)

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


# ---------- real worker processes ----------


@skip_on_windows
def test_three_worker_processes_complete_200_jobs_with_one_claim_each(
    conn, r, spawn_worker, wait_until
):
    ids = [new_job(conn, r) for _ in range(200)]
    processes = [spawn_worker(JOB_SLEEP_SCALE="0.02") for _ in range(3)]

    wait_until(
        lambda: len(db.list_jobs(conn, "completed", None, 200, 0)) == 200,
        timeout=60,
        message="not all 200 jobs completed",
    )

    for process in processes:
        process.send_signal(signal.SIGTERM)
    assert [process.wait(timeout=15) for process in processes] == [0, 0, 0]
    claimers = set()
    for job_id in ids:
        claims = [
            e for e in db.list_logs(conn, job_id) if e["message"] == "job claimed"
        ]
        assert len(claims) == 1, f"job {job_id} was claimed {len(claims)} times"
        assert db.get_job(conn, job_id)["attempts"] == 1
        claimers.add(claims[0]["metadata"]["worker_id"])
    assert len(claimers) >= 2  # the work really was shared


def test_job_of_a_killed_worker_is_recovered_and_finished_by_another(
    conn, r, spawn_worker, wait_until
):
    job_id = new_job(conn, r, "report")
    short_lease = {"LEASE_SECONDS": "2", "HEARTBEAT_SECONDS": "1"}
    first = spawn_worker(
        JOB_SLEEP_SCALE="1", **short_lease
    )  # a report takes 3 to 5 seconds
    wait_until(lambda: db.get_job(conn, job_id)["status"] == "processing")

    first.kill()  # SIGKILL: no cleanup of any kind
    first.wait()
    # The lease runs out, then the reaper returns the job to pending and the relay queues it.
    wait_until(
        lambda: db.reap_expired_leases(conn), message="the lease was never reaped"
    )
    assert db.get_job(conn, job_id)["status"] == "pending"
    assert scheduler.relay_due_pending(conn, r) == 1
    spawn_worker(JOB_SLEEP_SCALE="0.01", **short_lease)

    wait_until(lambda: db.get_job(conn, job_id)["status"] == "completed")
    assert db.get_job(conn, job_id)["attempts"] == 2
    messages = log_messages(conn, job_id)
    assert messages.count("job claimed") == 2
    assert messages.count("lease expired, job reaped") == 1


@skip_on_windows
def test_sigterm_finishes_the_running_job_then_exits_with_code_zero(
    conn, r, spawn_worker, wait_until
):
    running = new_job(conn, r, "report")  # 1.5 to 2.5 s at this scale
    process = spawn_worker(JOB_SLEEP_SCALE="0.5")
    wait_until(lambda: db.get_job(conn, running)["status"] == "processing")
    waiting = new_job(conn, r, "report")

    process.send_signal(signal.SIGTERM)

    assert process.wait(timeout=20) == 0
    assert db.get_job(conn, running)["status"] == "completed"
    job = db.get_job(conn, waiting)
    assert (job["status"], job["attempts"]) == ("pending", 0)  # never started


@skip_on_windows
def test_idle_worker_stops_promptly_and_removes_its_liveness_key(
    r, spawn_worker, wait_until
):
    process = spawn_worker(POP_TIMEOUT_SECONDS="1")
    wait_until(lambda: r.keys("worker:*"), message="the worker never registered")

    started = time.monotonic()
    process.send_signal(signal.SIGTERM)

    assert process.wait(timeout=10) == 0
    assert (
        time.monotonic() - started < 3
    )  # about one pop timeout, not a 30 s grace period
    assert r.keys("worker:*") == []


def test_worker_process_stops_itself_before_its_lease_expires_when_postgres_freezes(
    conn, r, spawn_worker, freezable_proxy, wait_until
):
    """The `docker compose pause postgres` case, with a real worker and the real os._exit."""
    job = db.insert_job(
        conn, uuid7(), "batch", {"items": list(range(1000))}, 0, 3
    )  # minutes long
    redis_client.enqueue(r, [job])
    process = spawn_worker(
        DATABASE_URL=freezable_proxy.database_url,
        LEASE_SECONDS="3",
        HEARTBEAT_SECONDS="1",
        JOB_SLEEP_SCALE="1",
    )
    first_lease = wait_until(lambda: db.get_job(conn, job["id"])["lease_expires_at"])
    # Wait for one real heartbeat to go through, so the freeze hits a worker that is mid-job.
    wait_until(lambda: db.get_job(conn, job["id"])["lease_expires_at"] > first_lease)

    freezable_proxy.frozen.set()

    # Its last good heartbeat was at most 1 s before the freeze. The worker must stop 2 s after
    # that, while the freeze is still on, so a timeout here means it kept running.
    assert process.wait(timeout=5) == 1
    exited_at = datetime.now(UTC)
    job = db.get_job(conn, job["id"])
    assert (job["status"], job["attempts"]) == (
        "processing",
        1,
    )  # it wrote nothing on the way out
    assert (
        exited_at < job["lease_expires_at"]
    )  # and was gone before the lease could expire
    assert (
        "no successful heartbeat before the lease could expire"
        in process.log_path.read_text()
    )
