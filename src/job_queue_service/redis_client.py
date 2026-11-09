"""Redis access. Redis only holds the ready queue, which Postgres can always rebuild."""

import redis

QUEUE_READY = "queue:ready"


def connect(url: str) -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=True)


def enqueue(r: redis.Redis, jobs: list[dict]) -> None:
    """Add jobs (dicts with id and priority) to the ready queue.

    The score is the negated priority, so BZPOPMIN returns the highest priority
    first. Adding a member that is already present only updates its score.
    """
    if jobs:
        r.zadd(QUEUE_READY, {str(job["id"]): -job["priority"] for job in jobs})


def pop_ready(r: redis.Redis, timeout: float) -> str | None:
    """Block up to `timeout` seconds for the next job id, or return None."""
    popped = r.bzpopmin(QUEUE_READY, timeout=timeout)
    if popped is None:
        return None
    _key, job_id, _score = popped
    return job_id
