import threading
from concurrent.futures import ThreadPoolExecutor

from job_queue_service import db

EMAIL = {
    "type": "email",
    "payload": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
}
WEBHOOK = {"type": "webhook", "payload": {"url": "https://example.com/hook"}}


def submit(client, body, **kwargs):
    return client.post("/jobs", json=body, **kwargs)


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


def test_same_key_with_different_priority_or_max_attempts_is_422(client):
    headers = {"Idempotency-Key": "key-1"}
    submit(client, {**EMAIL, "priority": 5, "max_attempts": 3}, headers=headers)

    for change in ({"priority": 6}, {"max_attempts": 2}):
        response = submit(
            client,
            {**EMAIL, "priority": 5, "max_attempts": 3, **change},
            headers=headers,
        )
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == "idempotency key reused with a different request"
        )
    assert len(client.get("/jobs").json()) == 1


def test_identical_request_with_priority_and_max_attempts_returns_existing_job(client):
    headers = {"Idempotency-Key": "key-1"}
    body = {**EMAIL, "priority": 5, "max_attempts": 3}

    first = submit(client, body, headers=headers)
    second = submit(client, body, headers=headers)

    assert (first.status_code, second.status_code) == (201, 200)
    assert second.json()["id"] == first.json()["id"]


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

    def expire_then_find_nothing(pool_conn, key, *_):
        selects.append(key)
        db.expire_idempotency_keys(
            conn, 0
        )  # another connection expires the key right now

    monkeypatch.setattr(db, "get_job_by_idempotency_key", expire_then_find_nothing)

    response = submit(client, EMAIL, headers={"Idempotency-Key": "key-1"})

    assert response.status_code == 201
    assert response.json()["id"] != first
    assert selects == ["key-1"]
