import uuid
from datetime import UTC, datetime, timedelta

from conftest import process_next

from job_queue_service import db, redis_client, scheduler
from job_queue_service.redis_client import QUEUE_READY

EMAIL = {
    "type": "email",
    "payload": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
}


def submit(client, body, **kwargs):
    return client.post("/jobs", json=body, **kwargs)


def make_due(conn, job_id):
    """Move a job's available_at into the past, standing in for time passing (tests never sleep)."""
    conn.execute(
        "UPDATE jobs SET available_at = now() - interval '1 second' WHERE id = %(id)s",
        {"id": job_id},
    )


def in_one_hour() -> str:
    return (datetime.now(UTC) + timedelta(hours=1)).isoformat()


# ---------- priority ----------


def test_higher_priority_pops_first_and_equal_priority_is_fifo(client, r):
    ids = {
        name: submit(client, {**EMAIL, "priority": priority}).json()["id"]
        for name, priority in [("low", 0), ("mid_a", 5), ("mid_b", 5), ("high", 9)]
    }

    popped = [redis_client.pop_ready(r, 1) for _ in range(4)]

    assert popped == [ids["high"], ids["mid_a"], ids["mid_b"], ids["low"]]


# ---------- scheduling ----------


def test_future_run_at_creates_scheduled_job_that_is_not_queued_or_claimable(
    client, conn, r, settings
):
    job = submit(client, {**EMAIL, "run_at": in_one_hour()}).json()

    assert job["status"] == "scheduled"
    assert r.zscore(QUEUE_READY, job["id"]) is None
    assert scheduler.relay_due_pending(conn, r) == 0
    assert db.claim_job(conn, job["id"], "w1", settings.lease_seconds) is None
    # Even a stale id forced into Redis cannot start the job early.
    r.zadd(QUEUE_READY, {job["id"]: 0})
    assert process_next(conn, r, settings, "w1") is None
    assert db.get_job(conn, job["id"])["status"] == "scheduled"


def test_scheduler_promotes_due_scheduled_job_and_worker_runs_it(
    client, conn, r, settings
):
    job_id = submit(client, {**EMAIL, "run_at": in_one_hour()}).json()["id"]
    assert scheduler.run_relay_cycle(conn, r) == 0  # not due yet

    make_due(conn, job_id)
    assert scheduler.run_relay_cycle(conn, r) == 1

    assert db.get_job(conn, job_id)["status"] == "pending"
    process_next(conn, r, settings, "w1")
    assert db.get_job(conn, job_id)["status"] == "completed"


def test_past_run_at_is_pending_and_queued_at_once(client, r):
    past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()

    job = submit(client, {**EMAIL, "run_at": past}).json()

    assert job["status"] == "pending"
    assert r.zscore(QUEUE_READY, job["id"]) is not None


def test_run_at_without_timezone_is_rejected(client):
    assert submit(client, {**EMAIL, "run_at": "2030-01-01T10:00:00"}).status_code == 422


def test_relay_sends_high_priority_job_even_when_backlog_exceeds_one_batch(conn, r):
    # 1001 older low-priority jobs, then one newer high-priority job, and an empty Redis
    # (as after a wipe). One relay cycle sends at most 1000 jobs: the high one must be in.
    conn.execute(
        """
        INSERT INTO jobs (id, type, payload, status, priority, available_at)
        SELECT gen_random_uuid(), 'email', '{}', 'pending', 0, now() - interval '1 hour'
        FROM generate_series(1, 1001)
        """
    )
    high = uuid.uuid7()
    conn.execute(
        "INSERT INTO jobs (id, type, payload, status, priority) "
        "VALUES (%s, 'email', '{}', 'pending', 100)",
        (high,),
    )
    r.flushdb()

    scheduler.relay_due_pending(conn, r)

    assert r.zscore(QUEUE_READY, str(high)) == -100
