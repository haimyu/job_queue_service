import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid7

import psycopg
from psycopg.rows import dict_row

from job_queue_service import db, redis_client, scheduler

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


# ---------- reaper and fencing (database level) ----------


def test_expired_lease_is_reaped_back_to_pending(conn, r):
    job_id = new_job(conn, r)
    db.claim_job(conn, job_id, "w1", 0)  # a lease of 0 seconds is already expired

    reaped = db.reap_expired_leases(conn)

    assert [(job["id"], job["status"]) for job in reaped] == [(job_id, "pending")]
    job = db.get_job(conn, job_id)
    assert job["status"] == "pending"
    assert job["attempts"] == 1
    assert job["locked_by"] is None
    assert job["lease_expires_at"] is None
    assert job["error"].startswith("lease expired")
    assert scheduler.relay_due_pending(conn, r) == 1
    assert log_messages(conn, job_id) == [
        "job created",
        "job claimed",
        "lease expired, job reaped",
    ]


def test_expired_lease_fails_the_job_when_attempts_are_used_up(conn, r):
    job_id = new_job(conn, r, max_attempts=1)
    db.claim_job(conn, job_id, "w1", 0)

    reaped = db.reap_expired_leases(conn)

    assert [job["status"] for job in reaped] == ["failed"]
    job = db.get_job(conn, job_id)
    assert job["status"] == "failed"
    assert job["completed_at"] is not None
    assert scheduler.relay_due_pending(conn, r) == 0


def test_live_lease_is_not_reaped(conn, r):
    job_id = new_job(conn, r)
    db.claim_job(conn, job_id, "w1", 30)
    assert db.reap_expired_leases(conn) == []
    assert db.get_job(conn, job_id)["status"] == "processing"


def test_heartbeat_extends_the_lease_and_stores_progress(conn, r):
    job_id = new_job(conn, r)
    claimed = db.claim_job(conn, job_id, "w1", 5)

    assert db.heartbeat_job(conn, job_id, "w1", 1, 55, 60) == 1

    job = db.get_job(conn, job_id)
    assert job["progress"] == 55
    assert job["lease_expires_at"] > claimed["lease_expires_at"]


def test_old_owner_is_fenced_out_after_its_job_was_reaped_and_reclaimed(conn, r):
    job_id = new_job(conn, r)
    db.claim_job(conn, job_id, "w1", 0)  # attempt 1
    db.reap_expired_leases(conn)
    db.claim_job(conn, job_id, "w1", 30)  # the same worker id claims again: attempt 2

    assert db.heartbeat_job(conn, job_id, "w1", 1, 50, 30) == 0  # stale attempt number
    assert db.complete_job(conn, job_id, "w1", 1, {}) == 0
    assert db.fail_job(conn, job_id, "w1", 1, "late", 0, False) is None
    assert db.heartbeat_job(conn, job_id, "w2", 2, 50, 30) == 0  # another worker
    assert db.heartbeat_job(conn, job_id, "w1", 2, 50, 30) == 1  # the real owner
    assert db.get_job(conn, job_id)["status"] == "processing"


def test_concurrent_reapers_reap_each_job_exactly_once(conn, r, settings):
    ids = []
    for i in range(30):
        job_id = new_job(conn, r)
        db.claim_job(conn, job_id, f"w{i}", 0)
        ids.append(job_id)
    barrier = threading.Barrier(4)

    def reap_everything(_):
        with psycopg.connect(
            settings.database_url, autocommit=True, row_factory=dict_row
        ) as c:
            barrier.wait()  # start all reapers together
            reaped = []
            while batch := db.reap_expired_leases(c):
                reaped += [job["id"] for job in batch]
            return reaped

    with ThreadPoolExecutor(max_workers=4) as pool:
        reaped = [
            job_id for part in pool.map(reap_everything, range(4)) for job_id in part
        ]

    assert sorted(reaped) == sorted(ids)  # every job once, none twice, none missed
    for job_id in ids:
        assert log_messages(conn, job_id).count("lease expired, job reaped") == 1
