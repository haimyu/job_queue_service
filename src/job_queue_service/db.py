"""Every SQL statement in the project lives in this file.

All functions take a psycopg connection whose row_factory is dict_row and pass
values as parameters, never through string formatting. Time comes from
Postgres now(), never from Python.

A state change and its job_logs row are written in one short transaction, so a
log row exists if and only if the change happened. For worker writes the log row
is added only when the fenced UPDATE matched a row.
"""

from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb


def ping(conn: Connection) -> None:
    conn.execute("SELECT 1")


def _add_log(
    conn: Connection, job_id: UUID, level: str, message: str, metadata: dict
) -> None:
    conn.execute(
        """
        INSERT INTO job_logs (job_id, level, message, metadata)
        VALUES (%(job_id)s, %(level)s, %(message)s, %(metadata)s)
        """,
        {
            "job_id": job_id,
            "level": level,
            "message": message,
            "metadata": Jsonb(metadata),
        },
    )


def insert_job(
    conn: Connection,
    job_id: UUID,
    job_type: str,
    payload: dict,
    priority: int,
    max_attempts: int,
) -> dict:
    with conn.transaction():
        job = conn.execute(
            """
            INSERT INTO jobs (id, type, payload, status, priority, max_attempts)
            VALUES (%(id)s, %(type)s, %(payload)s, 'pending', %(priority)s, %(max_attempts)s)
            RETURNING *
            """,
            {
                "id": job_id,
                "type": job_type,
                "payload": Jsonb(payload),
                "priority": priority,
                "max_attempts": max_attempts,
            },
        ).fetchone()
        _add_log(
            conn,
            job_id,
            "info",
            "job created",
            {"type": job_type, "priority": priority},
        )
    return job


def get_job(conn: Connection, job_id: UUID) -> dict | None:
    return conn.execute(
        "SELECT * FROM jobs WHERE id = %(id)s", {"id": job_id}
    ).fetchone()


def list_logs(conn: Connection, job_id: UUID) -> list[dict]:
    """Logs for one job, oldest first. Rows written in one transaction share a created_at, so id breaks ties."""
    return conn.execute(
        "SELECT * FROM job_logs WHERE job_id = %(id)s ORDER BY created_at, id",
        {"id": job_id},
    ).fetchall()


def select_due_pending(conn: Connection) -> list[dict]:
    """Relay select: pending jobs whose available_at has passed, oldest first."""
    return conn.execute(
        """
        SELECT id, priority FROM jobs
        WHERE status = 'pending' AND available_at <= now()
        ORDER BY available_at
        LIMIT 1000
        """
    ).fetchall()


def claim_job(
    conn: Connection, job_id: UUID, worker_id: str, lease_seconds: float
) -> dict | None:
    """Atomically move one pending job to processing for this worker.

    Returns None when the job is not claimable (cancelled, already claimed, or
    not yet due), which is how a stale or duplicate queue entry is rejected.
    """
    with conn.transaction():
        job = conn.execute(
            """
            UPDATE jobs
            SET status = 'processing',
                locked_by = %(worker)s,
                attempts = attempts + 1,
                lease_expires_at = now() + make_interval(secs => %(lease)s),
                started_at = COALESCE(started_at, now()),
                updated_at = now()
            WHERE id = %(id)s AND status = 'pending' AND available_at <= now()
            RETURNING *
            """,
            {"id": job_id, "worker": worker_id, "lease": lease_seconds},
        ).fetchone()
        if job is not None:
            _add_log(
                conn,
                job_id,
                "info",
                "job claimed",
                {"attempt": job["attempts"], "worker_id": worker_id},
            )
    return job


def complete_job(
    conn: Connection, job_id: UUID, worker_id: str, attempt: int, result: dict
) -> int:
    """Fenced completion. Returns the rowcount: 0 means this worker no longer owns the job."""
    with conn.transaction():
        rows = conn.execute(
            """
            UPDATE jobs
            SET status = 'completed', result = %(result)s, progress = 100,
                error = NULL, locked_by = NULL, lease_expires_at = NULL,
                completed_at = now(), updated_at = now()
            WHERE id = %(id)s AND status = 'processing'
              AND locked_by = %(worker)s AND attempts = %(attempt)s
            """,
            {
                "id": job_id,
                "worker": worker_id,
                "attempt": attempt,
                "result": Jsonb(result),
            },
        ).rowcount
        if rows:
            _add_log(conn, job_id, "info", "job completed", {"attempt": attempt})
    return rows


def fail_job(
    conn: Connection,
    job_id: UUID,
    worker_id: str,
    attempt: int,
    error: str,
    delay_seconds: float,
    permanent: bool,
) -> str | None:
    """Fenced failure of one attempt.

    A retryable failure returns the job to pending with a backoff delay while
    attempts remain, and fails it once they are used up. A permanent failure
    always fails the job. Returns the new status, or None if this worker no
    longer owns the job.
    """
    with conn.transaction():
        row = conn.execute(
            """
            UPDATE jobs
            SET status = CASE WHEN attempts < max_attempts AND NOT %(permanent)s
                              THEN 'pending' ELSE 'failed' END,
                available_at = CASE WHEN attempts < max_attempts AND NOT %(permanent)s
                                    THEN now() + make_interval(secs => %(delay)s)
                                    ELSE available_at END,
                completed_at = CASE WHEN attempts < max_attempts AND NOT %(permanent)s
                                    THEN NULL ELSE now() END,
                error = %(error)s, locked_by = NULL, lease_expires_at = NULL, updated_at = now()
            WHERE id = %(id)s AND status = 'processing'
              AND locked_by = %(worker)s AND attempts = %(attempt)s
            RETURNING status
            """,
            {
                "id": job_id,
                "worker": worker_id,
                "attempt": attempt,
                "error": error,
                "delay": delay_seconds,
                "permanent": permanent,
            },
        ).fetchone()
        if row is None:
            return None
        _add_log(
            conn,
            job_id,
            "warning",
            "attempt failed",
            {"attempt": attempt, "error": error},
        )
        if row["status"] == "pending":
            _add_log(
                conn,
                job_id,
                "info",
                "retry scheduled",
                {"attempt": attempt, "delay_seconds": delay_seconds},
            )
        else:
            _add_log(
                conn,
                job_id,
                "error",
                "job permanently failed",
                {"attempt": attempt, "error": error},
            )
    return row["status"]
