from conftest import process_next

from job_queue_service import redis_client
from job_queue_service.redis_client import QUEUE_READY

PAYLOADS = {
    "email": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
    "webhook": {"url": "https://example.com/hook"},
    "report": {"report_type": "sales"},
    "batch": {"items": [1, 2, 3, 4]},
}


# ---------- /health ----------


def test_health_reports_counts_queue_size_and_live_workers(client, conn, r, settings):
    email = {"type": "email", "payload": PAYLOADS["email"]}
    client.post("/jobs", json=email)
    process_next(conn, r, settings, "w1")  # one completed job
    client.post("/jobs", json=email)  # one pending job
    cancelled = client.post("/jobs", json=email).json()["id"]
    client.post(
        f"/jobs/{cancelled}/cancel"
    )  # its id stays in Redis until a worker pops it
    redis_client.refresh_worker(r, "w1")
    redis_client.refresh_worker(r, "w2")

    body = client.get("/health").json()

    assert body["postgres"] is True
    assert body["redis"] is True
    assert body["jobs"] == {
        "scheduled": 0,
        "pending": 1,
        "processing": 0,
        "completed": 1,
        "failed": 0,
        "cancelled": 1,
    }
    assert body["queue_ready"] == r.zcard(QUEUE_READY) == 2
    assert body["live_workers"] == 2
    assert 0 <= body["oldest_due_pending_age_seconds"] < 10
