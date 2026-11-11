import pytest

EMAIL = {
    "type": "email",
    "payload": {"to": "a@example.com", "subject": "Hi", "body": "Hello"},
}
WEBHOOK = {"type": "webhook", "payload": {"url": "https://example.com/hook"}}


def submit(client, body, **kwargs):
    return client.post("/jobs", json=body, **kwargs)


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
