import json
import uuid

from conftest import process_next

from job_queue_service import db, scheduler
from job_queue_service.models import MAX_PAYLOAD_BYTES
from job_queue_service.redis_client import QUEUE_READY

EMAIL = {
    "type": "email",
    "payload": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
}


def test_submit_and_retrieve_job(client, r):
    created = client.post("/jobs", json={**EMAIL, "priority": 5})
    assert created.status_code == 201
    job = created.json()
    assert job["status"] == "pending"
    assert job["priority"] == 5
    assert job["attempts"] == 0

    fetched = client.get(f"/jobs/{job['id']}")
    assert fetched.status_code == 200
    assert fetched.json() == job

    # The API's fast path put the id in the Redis ready queue.
    assert r.zscore(QUEUE_READY, job["id"]) == -5


def test_get_unknown_job_is_404(client):
    assert client.get(f"/jobs/{uuid.uuid7()}").status_code == 404


def test_invalid_payload_is_422(client):
    bad = {
        "type": "email",
        "payload": {"to": "not-an-email", "subject": "Hi", "body": "x"},
    }
    assert client.post("/jobs", json=bad).status_code == 422


def test_worker_completes_job_end_to_end(client, conn, r, settings):
    job_id = client.post("/jobs", json=EMAIL).json()["id"]

    processed = process_next(conn, r, settings, "worker-1")

    assert str(processed) == job_id
    job = client.get(f"/jobs/{job_id}").json()
    assert job["status"] == "completed"
    assert job["progress"] == 100
    assert job["attempts"] == 1
    assert job["locked_by"] is None
    assert job["lease_expires_at"] is None
    assert job["completed_at"] is not None
    assert "message_id" in job["result"]


def test_claim_of_non_pending_job_returns_no_row(client, conn, settings):
    job_id = client.post("/jobs", json=EMAIL).json()["id"]

    first = db.claim_job(conn, job_id, "worker-1", settings.lease_seconds)
    second = db.claim_job(conn, job_id, "worker-2", settings.lease_seconds)

    assert first["status"] == "processing"
    assert first["locked_by"] == "worker-1"
    assert second is None  # already processing, so a second worker cannot take it


def test_complete_with_wrong_owner_or_attempt_affects_no_rows(client, conn, settings):
    job_id = client.post("/jobs", json=EMAIL).json()["id"]
    job = db.claim_job(conn, job_id, "worker-1", settings.lease_seconds)

    assert db.complete_job(conn, job["id"], "worker-2", job["attempts"], {}) == 0
    assert db.complete_job(conn, job["id"], "worker-1", job["attempts"] + 1, {}) == 0
    assert db.get_job(conn, job["id"])["status"] == "processing"

    assert (
        db.complete_job(conn, job["id"], "worker-1", job["attempts"], {"ok": True}) == 1
    )
    assert db.get_job(conn, job["id"])["status"] == "completed"


def test_relay_recovers_job_missing_from_redis(client, conn, r):
    job_id = client.post("/jobs", json=EMAIL).json()["id"]
    r.flushdb()  # simulate Redis losing everything

    relayed = scheduler.relay_due_pending(conn, r)

    assert relayed == 1
    assert r.zscore(QUEUE_READY, job_id) is not None


def test_health_reports_postgres_and_redis(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["postgres"] is True
    assert response.json()["redis"] is True


def webhook_of_size(size: int) -> dict:
    """A valid webhook job whose payload serializes to exactly `size` characters."""
    payload = {"url": "https://example.com/hook", "body": {"x": ""}}
    padding = size - len(json.dumps(payload))
    payload["body"]["x"] = "a" * padding
    assert len(json.dumps(payload)) == size
    return {"type": "webhook", "payload": payload}


def test_payload_at_the_size_limit_is_accepted(client):
    response = client.post("/jobs", json=webhook_of_size(MAX_PAYLOAD_BYTES))
    assert response.status_code == 201


def test_payload_over_the_size_limit_is_rejected_and_creates_no_row(client, conn):
    response = client.post("/jobs", json=webhook_of_size(MAX_PAYLOAD_BYTES + 1))
    assert response.status_code == 422
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 0
