from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from conftest import process_next

from job_queue_service import db
from job_queue_service.redis_client import QUEUE_READY

EMAIL = {
    "type": "email",
    "payload": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
}
WEBHOOK = {"type": "webhook", "payload": {"url": "https://example.com/hook"}}


def submit(client, body, **kwargs):
    return client.post("/jobs", json=body, **kwargs)


def in_one_hour() -> str:
    return (datetime.now(UTC) + timedelta(hours=1)).isoformat()


# ---------- cancel ----------


def test_cancel_pending_job_and_worker_skips_the_stale_queue_entry(
    client, conn, r, settings
):
    job_id = submit(client, EMAIL).json()["id"]

    response = client.post(f"/jobs/{job_id}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert response.json()["completed_at"] is not None
    # The id is still in Redis, and the conditional claim rejects it.
    assert r.zscore(QUEUE_READY, job_id) is not None
    assert process_next(conn, r, settings, "w1") is None
    job = db.get_job(conn, job_id)
    assert job["status"] == "cancelled"
    assert job["attempts"] == 0
    messages = [log["message"] for log in client.get(f"/jobs/{job_id}/logs").json()]
    assert messages == ["job created", "job cancelled"]


def test_cancel_scheduled_job(client):
    job_id = submit(client, {**EMAIL, "run_at": in_one_hour()}).json()["id"]
    assert client.post(f"/jobs/{job_id}/cancel").json()["status"] == "cancelled"


def test_cancel_completed_job_is_409_with_current_status(client, conn, r, settings):
    job_id = submit(client, EMAIL).json()["id"]
    process_next(conn, r, settings, "w1")

    response = client.post(f"/jobs/{job_id}/cancel")

    assert response.status_code == 409
    assert "completed" in response.json()["detail"]
    assert db.get_job(conn, job_id)["status"] == "completed"


def test_cancel_processing_job_is_409(client, conn, settings):
    job_id = submit(client, EMAIL).json()["id"]
    db.claim_job(conn, job_id, "w1", settings.lease_seconds)
    assert client.post(f"/jobs/{job_id}/cancel").status_code == 409


def test_cancel_unknown_job_is_404(client):
    assert (
        client.post("/jobs/01a1c275-b4f3-7477-ab9e-6cd0055bab14/cancel").status_code
        == 404
    )


# ---------- manual retry ----------


def make_failed_job(client, conn, r, settings) -> str:
    s = replace(settings, webhook_failure_rate=1.0)
    job_id = submit(client, {**WEBHOOK, "max_attempts": 1}).json()["id"]
    process_next(conn, r, s, "w1")
    assert db.get_job(conn, job_id)["status"] == "failed"
    return job_id


def test_manual_retry_resets_a_failed_job_and_it_can_complete(
    client, conn, r, settings
):
    job_id = make_failed_job(client, conn, r, settings)
    r.flushdb()

    response = client.post(f"/jobs/{job_id}/retry")

    assert response.status_code == 200
    job = response.json()
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert job["error"] is None
    assert job["progress"] == 0
    assert job["completed_at"] is None
    assert r.zscore(QUEUE_READY, job_id) is not None  # fast path
    last_log = client.get(f"/jobs/{job_id}/logs").json()[-1]
    assert last_log["message"] == "job manually retried"
    assert "simulated webhook failure" in last_log["metadata"]["previous_error"]

    process_next(conn, r, replace(settings, webhook_failure_rate=0.0), "w1")
    assert db.get_job(conn, job_id)["status"] == "completed"


def test_dead_letter_lists_only_failed_jobs_with_their_error(client, conn, r, settings):
    dead = make_failed_job(client, conn, r, settings)
    submit(client, EMAIL)  # still pending, so not dead
    r.flushdb()

    listed = client.get("/dead-letter").json()

    assert [job["id"] for job in listed] == [dead]
    assert "simulated webhook failure" in listed[0]["error"]
    assert client.get("/dead-letter?type=email").json() == []
    assert client.get("/dead-letter?limit=0").status_code == 422


def test_a_manually_retried_job_leaves_the_dead_letter_list(client, conn, r, settings):
    dead = make_failed_job(client, conn, r, settings)

    client.post(f"/jobs/{dead}/retry")

    assert client.get("/dead-letter").json() == []


@pytest.mark.parametrize("final_state", ["pending", "completed", "cancelled"])
def test_manual_retry_is_only_allowed_from_failed(
    client, conn, r, settings, final_state
):
    job_id = submit(client, EMAIL).json()["id"]
    if final_state == "completed":
        process_next(conn, r, settings, "w1")
    elif final_state == "cancelled":
        client.post(f"/jobs/{job_id}/cancel")

    response = client.post(f"/jobs/{job_id}/retry")

    assert response.status_code == 409
    assert final_state in response.json()["detail"]
    assert db.get_job(conn, job_id)["status"] == final_state


def test_manual_retry_of_unknown_job_is_404(client):
    assert (
        client.post("/jobs/01a1c275-b4f3-7477-ab9e-6cd0055bab14/retry").status_code
        == 404
    )
