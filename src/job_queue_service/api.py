"""HTTP API: accepts job submissions and answers status queries."""

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, get_args
from uuid import UUID

import psycopg
import redis
from fastapi import APIRouter, FastAPI, Header, HTTPException, Query, Request, Response
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from job_queue_service import db, redis_client
from job_queue_service.config import Settings, load_settings
from job_queue_service.logging_setup import setup_logging
from job_queue_service.models import JobCreate, JobStatus, JobType

logger = logging.getLogger(__name__)
router = APIRouter()


def enqueue_best_effort(request: Request, job: dict) -> None:
    """Fast path: put a due pending job in the Redis queue right after its transaction commits."""
    if job["status"] != "pending":
        return
    try:
        redis_client.enqueue(request.app.state.redis, [job])
    except redis.RedisError:
        # The scheduler relay re-adds every due pending job within about a second.
        logger.warning(
            "could not enqueue in redis, relay will recover it",
            extra={"job_id": job["id"]},
        )


@router.post("/jobs", status_code=201)
def create_job(
    body: JobCreate,
    request: Request,
    response: Response,
    idempotency_key: Annotated[str | None, Header(min_length=1, max_length=255)] = None,
) -> dict:
    job_id = uuid.uuid7()
    # Leaving this block commits the insert. Postgres is the source of truth,
    # so the job is durable before Redis hears about it.
    with request.app.state.pool.connection() as conn:
        # A second pass happens only if the key expired between the insert and the select.
        for _ in range(2):
            job = db.insert_job(
                conn,
                job_id,
                body.type,
                body.payload,
                body.priority,
                body.max_attempts,
                body.run_at,
                idempotency_key,
            )
            if job is not None:
                break
            found = db.get_job_by_idempotency_key(
                conn, idempotency_key, body.type, body.payload
            )
            if found is None:
                continue
            existing, same_request = found
            if not same_request:
                raise HTTPException(
                    status_code=422,
                    detail="idempotency key reused with a different request",
                )
            response.status_code = 200
            return existing
        else:
            raise RuntimeError("idempotency key kept conflicting without a visible job")
    enqueue_best_effort(request, job)
    logger.info("job created", extra={"job_id": job_id, "job_type": body.type})
    return job


@router.get("/jobs")
def list_jobs(
    request: Request,
    status: JobStatus | None = None,
    job_type: Annotated[JobType | None, Query(alias="type")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[dict]:
    with request.app.state.pool.connection() as conn:
        return db.list_jobs(conn, status, job_type, limit, offset)


@router.get("/dead-letter")
def list_dead_letter(
    request: Request,
    job_type: Annotated[JobType | None, Query(alias="type")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[dict]:
    """The dead letter queue is the set of failed jobs: they used all their attempts or hit a
    permanent error. Each keeps its last error. Retry one with POST /jobs/{id}/retry."""
    with request.app.state.pool.connection() as conn:
        return db.list_jobs(conn, "failed", job_type, limit, offset)


@router.get("/jobs/{job_id}")
def get_job(job_id: UUID, request: Request) -> dict:
    with request.app.state.pool.connection() as conn:
        job = db.get_job(conn, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: UUID, request: Request) -> dict:
    with request.app.state.pool.connection() as conn:
        job = db.cancel_job(conn, job_id)
        if job is None:
            current = db.get_job(conn, job_id)
            if current is None:
                raise HTTPException(status_code=404, detail="job not found")
            raise HTTPException(
                status_code=409,
                detail=f"cannot cancel a job that is {current['status']}",
            )
    logger.info("job cancelled", extra={"job_id": job_id, "job_type": job["type"]})
    return job


@router.post("/jobs/{job_id}/retry")
def retry_job(job_id: UUID, request: Request) -> dict:
    with request.app.state.pool.connection() as conn:
        job = db.retry_job(conn, job_id)
        if job is None:
            current = db.get_job(conn, job_id)
            if current is None:
                raise HTTPException(status_code=404, detail="job not found")
            raise HTTPException(
                status_code=409,
                detail=f"only failed jobs can be retried, this job is {current['status']}",
            )
    enqueue_best_effort(request, job)
    logger.info(
        "job manually retried", extra={"job_id": job_id, "job_type": job["type"]}
    )
    return job


@router.get("/jobs/{job_id}/logs")
def get_job_logs(job_id: UUID, request: Request) -> list[dict]:
    with request.app.state.pool.connection() as conn:
        if db.get_job(conn, job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return db.list_logs(conn, job_id)


@router.get("/health")
def health(request: Request, response: Response) -> dict:
    jobs = oldest_age = queue_ready = live_workers = None
    try:
        with request.app.state.pool.connection() as conn:
            counts = db.job_counts(conn)
            oldest_age = db.oldest_due_pending_age(conn)
        jobs = {status: counts.get(status, 0) for status in get_args(JobStatus)}
        postgres_ok = True
    except psycopg.Error:
        postgres_ok = False
    try:
        queue_ready = redis_client.queue_size(request.app.state.redis)
        live_workers = redis_client.count_workers(request.app.state.redis)
        redis_ok = True
    except redis.RedisError:
        redis_ok = False
    # Redis being down is degraded but not fatal: Postgres still accepts and keeps jobs.
    if not postgres_ok:
        response.status_code = 503
    return {
        "postgres": postgres_ok,
        "redis": redis_ok,
        "jobs": jobs,
        "oldest_due_pending_age_seconds": oldest_age,
        "queue_ready": queue_ready,
        "live_workers": live_workers,
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Loading settings here, not at import, means a missing DATABASE_URL or REDIS_URL
        # stops the server at startup with a clear error, while importing this module stays safe.
        active = settings or load_settings()
        setup_logging(active.log_level)
        pool = ConnectionPool(
            active.database_url,
            kwargs={"row_factory": dict_row},
            open=False,
            timeout=5,
        )
        pool.open(wait=True, timeout=10)
        app.state.pool = pool
        app.state.redis = redis_client.connect(active.redis_url)
        yield
        pool.close()
        app.state.redis.close()

    app = FastAPI(title="Job Queue Service", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
