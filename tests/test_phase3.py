import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from job_queue_service import db, redis_client, scheduler, worker
from job_queue_service.redis_client import QUEUE_READY

EMAIL = {
    "type": "email",
    "payload": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
}
WEBHOOK = {"type": "webhook", "payload": {"url": "https://example.com/hook"}}


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
    assert worker.process_next(conn, r, settings, "w1") is None
    assert db.get_job(conn, job["id"])["status"] == "scheduled"


def test_scheduler_promotes_due_scheduled_job_and_worker_runs_it(
    client, conn, r, settings
):
    job_id = submit(client, {**EMAIL, "run_at": in_one_hour()}).json()["id"]
    assert scheduler.run_relay_cycle(conn, r) == 0  # not due yet

    make_due(conn, job_id)
    assert scheduler.run_relay_cycle(conn, r) == 1

    assert db.get_job(conn, job_id)["status"] == "pending"
    worker.process_next(conn, r, settings, "w1")
    assert db.get_job(conn, job_id)["status"] == "completed"


def test_past_run_at_is_pending_and_queued_at_once(client, r):
    past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()

    job = submit(client, {**EMAIL, "run_at": past}).json()

    assert job["status"] == "pending"
    assert r.zscore(QUEUE_READY, job["id"]) is not None


def test_run_at_without_timezone_is_rejected(client):
    assert submit(client, {**EMAIL, "run_at": "2030-01-01T10:00:00"}).status_code == 422


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
    assert worker.process_next(conn, r, settings, "w1") is None
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
    worker.process_next(conn, r, settings, "w1")

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
    worker.process_next(conn, r, s, "w1")
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

    worker.process_next(conn, r, replace(settings, webhook_failure_rate=0.0), "w1")
    assert db.get_job(conn, job_id)["status"] == "completed"


@pytest.mark.parametrize("final_state", ["pending", "completed", "cancelled"])
def test_manual_retry_is_only_allowed_from_failed(
    client, conn, r, settings, final_state
):
    job_id = submit(client, EMAIL).json()["id"]
    if final_state == "completed":
        worker.process_next(conn, r, settings, "w1")
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


# ---------- list ----------


def test_list_filters_orders_newest_first_and_paginates(client, conn, r, settings):
    first = submit(client, EMAIL).json()["id"]
    second = submit(client, WEBHOOK).json()["id"]
    third = submit(client, EMAIL).json()["id"]
    client.post(f"/jobs/{third}/cancel")

    def ids(query: str) -> list[str]:
        return [job["id"] for job in client.get(f"/jobs{query}").json()]

    assert ids("") == [third, second, first]
    assert ids("?type=email") == [third, first]
    assert ids("?status=pending") == [second, first]
    assert ids("?status=pending&type=email") == [first]
    assert ids("?limit=1") == [third]
    assert ids("?limit=1&offset=1") == [second]
    assert ids("?status=failed") == []


@pytest.mark.parametrize(
    "query", ["?status=bogus", "?type=sms", "?limit=0", "?limit=201", "?offset=-1"]
)
def test_list_rejects_invalid_parameters(client, query):
    assert client.get(f"/jobs{query}").status_code == 422


def test_list_filter_value_is_not_interpreted_as_sql(client):
    # The value is rejected by the whitelist, and in any case is only ever bound as a parameter.
    assert (
        client.get(
            "/jobs", params={"status": "pending'; DROP TABLE jobs; --"}
        ).status_code
        == 422
    )


# ---------- idempotency ----------


def test_same_idempotency_key_and_request_returns_the_existing_job(client):
    headers = {"Idempotency-Key": "key-1"}

    first = submit(client, EMAIL, headers=headers)
    second = submit(client, EMAIL, headers=headers)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert len(client.get("/jobs").json()) == 1


def test_same_key_with_different_payload_or_type_is_422(client):
    headers = {"Idempotency-Key": "key-1"}
    submit(client, EMAIL, headers=headers)

    other_payload = {**EMAIL, "payload": {**EMAIL["payload"], "subject": "Different"}}
    for body in (other_payload, WEBHOOK):
        response = submit(client, body, headers=headers)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == "idempotency key reused with a different request"
        )
    assert len(client.get("/jobs").json()) == 1


def test_requests_without_a_key_never_conflict(client):
    first = submit(client, EMAIL).json()["id"]
    second = submit(client, EMAIL).json()["id"]
    assert first != second


def test_concurrent_submits_with_one_key_create_exactly_one_job(client):
    headers = {"Idempotency-Key": "race"}
    barrier = threading.Barrier(8)

    def send(_):
        barrier.wait()  # release all threads together to maximise the overlap
        return submit(client, EMAIL, headers=headers)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(send, range(8)))

    assert sorted(response.status_code for response in responses) == [200] * 7 + [201]
    assert len({response.json()["id"] for response in responses}) == 1
    assert len(client.get("/jobs").json()) == 1


def test_expired_key_can_be_used_again_and_fresh_keys_are_kept(client, conn):
    first = submit(client, EMAIL, headers={"Idempotency-Key": "key-1"}).json()["id"]

    assert (
        db.expire_idempotency_keys(conn, 24) == 0
    )  # created just now, well inside 24 hours
    assert db.get_job(conn, first)["idempotency_key"] == "key-1"

    assert db.expire_idempotency_keys(conn, 0) == 1
    assert db.get_job(conn, first)["idempotency_key"] is None
    again = submit(client, EMAIL, headers={"Idempotency-Key": "key-1"})
    assert again.status_code == 201
    assert again.json()["id"] != first


def test_key_that_expires_between_insert_and_select_is_inserted_on_the_second_try(
    client, conn, monkeypatch
):
    first = submit(client, EMAIL, headers={"Idempotency-Key": "key-1"}).json()["id"]
    selects = []

    def expire_then_find_nothing(pool_conn, key, job_type, payload):
        selects.append(key)
        db.expire_idempotency_keys(
            conn, 0
        )  # another connection expires the key right now

    monkeypatch.setattr(db, "get_job_by_idempotency_key", expire_then_find_nothing)

    response = submit(client, EMAIL, headers={"Idempotency-Key": "key-1"})

    assert response.status_code == 201
    assert response.json()["id"] != first
    assert selects == ["key-1"]
