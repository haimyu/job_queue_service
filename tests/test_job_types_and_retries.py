from dataclasses import replace
from uuid import uuid7

import pytest
from conftest import process_next

from job_queue_service import db, handlers, redis_client, scheduler
from job_queue_service.redis_client import QUEUE_READY

EMAIL = {
    "type": "email",
    "payload": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
}
WEBHOOK = {"type": "webhook", "payload": {"url": "https://example.com/hook"}}
REPORT = {"type": "report", "payload": {"report_type": "sales"}}
BATCH = {"type": "batch", "payload": {"items": [1, 2, 3, 4, 5]}}


def submit(client, body) -> str:
    return client.post("/jobs", json=body).json()["id"]


def log_messages(client, job_id) -> list[str]:
    return [entry["message"] for entry in client.get(f"/jobs/{job_id}/logs").json()]


@pytest.mark.parametrize("body", [EMAIL, WEBHOOK, REPORT, BATCH])
def test_every_job_type_completes(client, conn, r, settings, body):
    s = replace(settings, webhook_failure_rate=0.0)
    job_id = submit(client, body)

    process_next(conn, r, s, "w1")

    job = db.get_job(conn, job_id)
    assert job["status"] == "completed"
    assert job["progress"] == 100
    assert job["result"]


def test_batch_reports_progress_after_each_item(settings):
    reported = []

    result = handlers.run_handler(
        "batch", {"items": ["a", "b", "c", "d"]}, reported.append, settings
    )

    assert reported == [25, 50, 75, 100]
    assert result == {"processed": 4, "succeeded": 4, "failed": 0}


def test_failed_attempt_schedules_retry_after_first_backoff(client, conn, r, settings):
    # The default first delay (0.1 s) is too short to assert "not yet due", so use a long one.
    s = replace(
        settings, webhook_failure_rate=1.0, backoff_seconds=(30.0, 120.0, 120.0)
    )
    job_id = submit(client, WEBHOOK)

    process_next(conn, r, s, "w1")

    job = db.get_job(conn, job_id)
    assert job["status"] == "pending"
    assert job["attempts"] == 1
    assert job["locked_by"] is None
    assert "simulated webhook failure" in job["error"]
    # available_at and updated_at are both Postgres now() in the same statement.
    delay = (job["available_at"] - job["updated_at"]).total_seconds()
    assert delay == pytest.approx(30, abs=1)
    # While it waits for the backoff, the job is neither relayed nor claimable.
    assert scheduler.relay_due_pending(conn, r) == 0
    assert db.claim_job(conn, job["id"], "w2", s.lease_seconds) is None
    assert log_messages(client, job_id) == [
        "job created",
        "job claimed",
        "attempt failed",
        "retry scheduled",
    ]


def test_fourth_failure_fails_the_job(client, conn, r, settings):
    # The default is 4 attempts: the first run plus 3 retries.
    s = replace(settings, webhook_failure_rate=1.0, backoff_seconds=(0.0, 0.0, 0.0))
    job_id = submit(client, WEBHOOK)

    for _ in range(4):
        # After a failure the id is no longer in Redis. The relay puts the due retry back.
        assert scheduler.relay_due_pending(conn, r) == 1
        process_next(conn, r, s, "w1")

    job = db.get_job(conn, job_id)
    assert job["status"] == "failed"
    assert job["attempts"] == 4
    assert job["completed_at"] is not None
    assert scheduler.relay_due_pending(conn, r) == 0
    messages = log_messages(client, job_id)
    assert messages.count("retry scheduled") == 3
    assert messages[-1] == "job permanently failed"


def test_permanent_error_fails_after_one_attempt(conn, r, settings):
    # A payload that skipped API validation, for example a row written some other way.
    job = db.insert_job(conn, uuid7(), "webhook", {"body": {}}, 0, 3)
    redis_client.enqueue(r, [job])

    process_next(conn, r, settings, "w1")

    job = db.get_job(conn, job["id"])
    assert job["status"] == "failed"
    assert job["attempts"] == 1
    assert job["error"].startswith("PermanentJobError: invalid payload")


def test_unexpected_exception_is_retried_and_does_not_kill_the_worker(
    client, conn, r, settings, monkeypatch
):
    def explode(payload, report_progress, settings):
        raise RuntimeError("boom")

    monkeypatch.setitem(handlers.HANDLERS, "email", explode)
    job_id = submit(client, EMAIL)

    processed = process_next(conn, r, settings, "w1")

    assert str(processed) == job_id
    job = db.get_job(conn, job_id)
    assert job["status"] == "pending"
    assert job["error"] == "RuntimeError: boom"


def test_more_attempts_than_backoff_delays_reuses_the_last_delay(
    client, conn, r, settings
):
    # One configured delay but three attempts: attempt 2 has no delay of its own.
    s = replace(settings, webhook_failure_rate=1.0, backoff_seconds=(0.0,))
    job_id = submit(client, {**WEBHOOK, "max_attempts": 3})

    for _ in range(3):
        scheduler.relay_due_pending(conn, r)
        process_next(conn, r, s, "w1")

    job = db.get_job(conn, job_id)
    assert job["status"] == "failed"
    assert job["attempts"] == 3


@pytest.mark.parametrize(
    "body",
    [
        {"type": "webhook", "payload": {}},
        {"type": "webhook", "payload": {"url": "not a url"}},
        {"type": "report", "payload": {}},
        {"type": "batch", "payload": {"items": []}},
        {"type": "batch", "payload": {"items": list(range(1001))}},
        {"type": "sms", "payload": {}},
    ],
)
def test_invalid_payload_is_rejected_with_422(client, body):
    assert client.post("/jobs", json=body).status_code == 422


def test_logs_endpoint_lists_transitions_oldest_first(client, conn, r, settings):
    job_id = submit(client, EMAIL)
    process_next(conn, r, settings, "w1")

    logs = client.get(f"/jobs/{job_id}/logs").json()

    assert [entry["message"] for entry in logs] == [
        "job created",
        "job claimed",
        "job completed",
    ]
    assert logs[1]["metadata"] == {"attempt": 1, "worker_id": "w1"}


def test_logs_of_unknown_job_is_404(client):
    assert client.get(f"/jobs/{uuid7()}/logs").status_code == 404


def test_worker_skips_invalid_and_stale_queue_entries(conn, r, settings):
    r.zadd(QUEUE_READY, {"not-a-uuid": 0})
    assert process_next(conn, r, settings, "w1") is None

    r.zadd(QUEUE_READY, {str(uuid7()): 0})  # a valid id with no job row
    assert process_next(conn, r, settings, "w1") is None
