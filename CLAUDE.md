# CLAUDE.md — Job Queue Service

You are implementing a take-home assignment: a distributed background job queue.
The assignment PDF is provided separately. This file is the **binding design**. When
the PDF and this file differ in detail, this file wins. When you believe a decision
here is wrong, STOP and ask — do not silently change the design.

---

## 0. How you work in this repo (read first)

1. Work **one phase at a time** (section 9). Never start a phase until the user
   writes `start phase N`.
2. At the end of every phase, STOP and print:
   - what you built (files changed, one line each)
   - exact commands for the user to run and test it manually
   - test results you ran (`uv run pytest ...` output summary)
   - a suggested commit message
   - any deviation from this file, and why
3. **Never run `git commit`, `git push`, or rewrite git history.** The user commits.
4. Every SQL statement lives in `src/job_queue_service/db.py`. No SQL anywhere else.
5. Keep code small and readable. The user must explain every line in an interview.
   No clever abstractions, no frameworks beyond the stack below, no speculative features.
6. Do not write content for `AI_USAGE.md` beyond its headings. The user writes it.
7. If a test is flaky, fix the cause. Never add sleeps or retries to make tests pass.

---

## 1. Priorities (in order)

1. **Correctness** — a job is owned by at most one worker at a time; no lost jobs;
   state transitions are exact.
2. **Reliability** — any process can crash at any line; the system recovers by itself.
3. **Scalability** — horizontal workers; Postgres touched only by indexed, single-row
   or small-batch statements.
4. **Shippable** — every phase ends in a working, tested system.

Delivery guarantee: **at-least-once execution, exactly-once state transitions.**
A handler may run more than once (crash after a side effect); the database never
records two owners or two outcomes for one attempt.

---

## 2. Stack (fixed)

- Python **3.14** (uses `uuid.uuid7()`), FastAPI, uvicorn
- PostgreSQL 17, psycopg 3 (`psycopg[binary,pool]`) with **raw SQL** and parameters
- Redis 7, `redis` (redis-py, sync)
- pytest, httpx
- uv, `src/` layout, package name `job_queue_service`
- docker compose

Forbidden:
- Any ORM or SQLAlchemy (Core included)
- Celery, RQ, Dramatiq, arq, Huey, or any job library
- `time.sleep()` as a retry/backoff mechanism
- String formatting of SQL (f-strings, `%`, `.format`) — always pass parameters
- Reading time from Python for any job state decision (due, lease, backoff, expiry) — use
  Postgres `now()`. `time.monotonic()` is allowed only to pace the scheduler's own loop and as
  the worker's local stopwatch for heartbeat, timeout, stall and self-fence deadlines.
- Storing authoritative job state in Redis
- Check-then-insert for idempotency (must be `ON CONFLICT`)
- Any write by a worker to a job row without the fencing condition (section 5)

---

## 3. Architecture

```
Client ──HTTP──> API ──INSERT/COMMIT──> Postgres (source of truth)
                  └──ZADD (fast path, best effort)──> Redis ZSET queue:ready
Scheduler ──every 1s: promote scheduled, relay due pending──> Redis
Scheduler ──every 5s: reap expired leases; every 5min: expire idempotency keys
Worker ×N ──BZPOPMIN queue:ready──> claim/heartbeat/finalize in Postgres (fenced)
```

Containers: `api`, `worker` (scalable), `scheduler`, `postgres`, `redis`.

- **Postgres** holds all job state. Everything else is rebuildable from it.
- **Redis** holds:
  - `queue:ready` — ZSET, member = job id (UUIDv7 string), score = `-priority`.
    `BZPOPMIN` returns highest priority first; equal scores order by member
    lexicographically, and UUIDv7 strings sort by creation time → FIFO ties.
  - `worker:<worker_id>` — liveness key with 30 s TTL, refreshed by each worker loop.
  - Redis may lose everything at any time without losing a job.
- **Fast path:** API commits, then `ZADD`. If Redis fails, log a warning and still
  return success — the relay re-adds the job within ~1 s.
- **Relay:** every cycle, `ZADD` every due `pending` job (re-adding existing members is
  harmless). A stale or duplicate id in Redis is harmless: the conditional claim
  rejects it.

---

## 4. Data model — `db/001_schema.sql`

```sql
CREATE TABLE jobs (
    id               uuid PRIMARY KEY,                 -- uuid7, generated in Python
    type             text NOT NULL CHECK (type IN ('email','webhook','report','batch')),
    payload          jsonb NOT NULL,
    status           text NOT NULL CHECK (status IN
                       ('scheduled','pending','processing','completed','failed','cancelled')),
    priority         integer NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 100),
    attempts         integer NOT NULL DEFAULT 0,
    max_attempts     integer NOT NULL DEFAULT 4 CHECK (max_attempts >= 1),
    available_at     timestamptz NOT NULL DEFAULT now(),
    locked_by        text,
    lease_expires_at timestamptz,
    progress         integer NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
    result           jsonb,
    error            text,
    idempotency_key  text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    completed_at     timestamptz,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- relay: due pending jobs
CREATE INDEX jobs_pending_due_idx ON jobs (available_at) WHERE status = 'pending';
-- scheduler: due scheduled jobs
CREATE INDEX jobs_scheduled_due_idx ON jobs (available_at) WHERE status = 'scheduled';
-- reaper: expired leases
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at) WHERE status = 'processing';
-- list endpoint
CREATE INDEX jobs_status_created_idx ON jobs (status, created_at DESC);
CREATE INDEX jobs_type_created_idx ON jobs (type, created_at DESC);
-- idempotency (NULLs never conflict)
CREATE UNIQUE INDEX jobs_idempotency_key_idx ON jobs (idempotency_key);
-- idempotency cleanup: only jobs that still hold a key, oldest first
CREATE INDEX jobs_idempotency_expiry_idx ON jobs (created_at) WHERE idempotency_key IS NOT NULL;

CREATE TABLE job_logs (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id     uuid NOT NULL REFERENCES jobs(id),
    level      text NOT NULL CHECK (level IN ('info','warning','error')),
    message    text NOT NULL,
    metadata   jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX job_logs_job_idx ON job_logs (job_id, created_at);
```

`db/000_test_db.sql`: `CREATE DATABASE jobs_test;` (runs first, creates the test DB).

Status semantics:
- `scheduled` — has a future run time; promoted to `pending` by the scheduler when due.
- `pending` with `available_at > now()` — waiting for a retry backoff.
- `pending` with `available_at <= now()` — ready; the relay puts it in Redis.

---

## 5. Critical SQL (implement exactly these semantics)

All worker statements are single statements on an autocommit connection, so each is
atomic on its own. `%(lease)s` etc. are seconds from config.

**Fencing condition** — every worker write after the claim includes:
`WHERE id = %(id)s AND status = 'processing' AND locked_by = %(worker)s AND attempts = %(attempt)s`
Rowcount 0 ⇒ ownership lost ⇒ write nothing else, log a warning, abandon the job.

**Claim**
```sql
UPDATE jobs
SET status = 'processing',
    locked_by = %(worker)s,
    attempts = attempts + 1,
    lease_expires_at = now() + make_interval(secs => %(lease)s),
    started_at = COALESCE(started_at, now()),
    updated_at = now()
WHERE id = %(id)s AND status = 'pending' AND available_at <= now()
RETURNING *;
```
No row returned ⇒ stale id (cancelled, already claimed, not yet due) ⇒ skip.

**Heartbeat** (also writes progress)
```sql
UPDATE jobs
SET lease_expires_at = now() + make_interval(secs => %(lease)s),
    progress = %(progress)s, updated_at = now()
WHERE <fencing condition>;
```

**Complete**
```sql
UPDATE jobs
SET status = 'completed', result = %(result)s, progress = 100,
    error = NULL, locked_by = NULL, lease_expires_at = NULL,
    completed_at = now(), updated_at = now()
WHERE <fencing condition>;
```

**Retryable failure** (`%(delay)s` computed in Python from `attempts`, see section 6)
```sql
UPDATE jobs
SET status       = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END,
    available_at = CASE WHEN attempts < max_attempts
                        THEN now() + make_interval(secs => %(delay)s) ELSE available_at END,
    completed_at = CASE WHEN attempts < max_attempts THEN NULL ELSE now() END,
    error = %(error)s, locked_by = NULL, lease_expires_at = NULL, updated_at = now()
WHERE <fencing condition>
RETURNING status;
```

**Permanent failure**: same, but always `status = 'failed'`, `completed_at = now()`.

**Reaper** (expired leases; safe with multiple schedulers)
```sql
UPDATE jobs
SET status       = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END,
    available_at = now(),
    completed_at = CASE WHEN attempts < max_attempts THEN NULL ELSE now() END,
    error = 'lease expired: worker presumed dead',
    locked_by = NULL, lease_expires_at = NULL, updated_at = now()
WHERE id IN (
    SELECT id FROM jobs
    WHERE status = 'processing' AND lease_expires_at < now()
    ORDER BY lease_expires_at
    LIMIT 100
    FOR UPDATE SKIP LOCKED
)
AND status = 'processing' AND lease_expires_at < now()
RETURNING id, status;
```

**Promote scheduled**
```sql
UPDATE jobs SET status = 'pending', updated_at = now()
WHERE status = 'scheduled' AND available_at <= now()
RETURNING id, priority;
```

**Relay select**
```sql
SELECT id, priority FROM jobs
WHERE status = 'pending' AND available_at <= now()
ORDER BY available_at
LIMIT 1000;
```
Then one pipelined `ZADD queue:ready {str(id): -priority ...}`.

**Insert (idempotent)**
```sql
INSERT INTO jobs (id, type, payload, status, priority, max_attempts,
                  available_at, idempotency_key)
VALUES (...)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;
```
If nothing returned: `SELECT * FROM jobs WHERE idempotency_key = %(key)s`.
- Same `type` and `payload` (compare with jsonb equality) ⇒ return existing job, HTTP 200.
- Different ⇒ HTTP 422 `idempotency key reused with a different request`.
- SELECT finds nothing (key expired between the two statements) ⇒ retry the insert once.
New job ⇒ HTTP 201.

**Cancel**
```sql
UPDATE jobs SET status = 'cancelled', completed_at = now(), updated_at = now()
WHERE id = %(id)s AND status IN ('pending','scheduled')
RETURNING *;
```
No row ⇒ SELECT the job: missing ⇒ 404; exists ⇒ 409 with its current status.

**Manual retry** (one transaction): `SELECT ... FOR UPDATE`; if status != `failed` ⇒ 409;
insert a `job_logs` row with the previous `error`; then
`UPDATE jobs SET status='pending', attempts=0, available_at=now(), error=NULL,
result=NULL, progress=0, started_at=NULL, completed_at=NULL, updated_at=now()`.
Then fast-path `ZADD`.

**Idempotency expiry**
```sql
UPDATE jobs SET idempotency_key = NULL
WHERE idempotency_key IS NOT NULL
  AND created_at < now() - make_interval(hours => %(ttl_hours)s);
```
Keys therefore live at least 24 h (24 h + cleanup interval at most).

---

## 6. Behaviour

**Retries / backoff.** `attempts` is incremented only at claim. On failure, delay is
`BACKOFF_SECONDS[attempts - 1]` with `BACKOFF_SECONDS = [0.1, 30, 120]`.
Run 1 immediate → run 2 after 0.1 s → run 3 after 30 s → run 4 after 120 s → then `failed`
(the first run plus 3 retries; `max_attempts` defaults to 4).

**Error types** (in `handlers.py`):
- `RetryableJobError` — transient (network, timeout, simulated webhook failure).
- `PermanentJobError` — retrying can't help (invalid payload). Goes straight to `failed`.
- Any other exception ⇒ treated as retryable.

**Job types** (in `handlers.py`; all sleeps multiplied by `JOB_SLEEP_SCALE`):
| type | payload (validated by pydantic) | behaviour | timeout |
|---|---|---|---|
| email | `to` (email), `subject`, `body` | sleep 1–3 s, return `{"message_id": ...}` | 10 s |
| webhook | `url`, optional `body` | sleep 1–2 s; fail with probability `WEBHOOK_FAILURE_RATE` (default 0.2) by raising `RetryableJobError` | 10 s |
| report | `report_type`, optional `params` | sleep 3–5 s, return `{"file_url": ...}` | 15 s |
| batch | `items` (list, 1–1000) | per item sleep 0.1–0.3 s, report progress after each item, return `{"processed": n, "succeeded": n, "failed": 0}` | none; stall after `STALL_SECONDS` |

Handlers receive `(payload, report_progress)` where `report_progress(percent: int)`
updates a value the worker's main thread reads.

**Worker loop** (`worker.py`, one job at a time per process):
1. Refresh `worker:<id>` (TTL 30 s). If shutdown requested ⇒ delete the key, exit 0.
2. `BZPOPMIN queue:ready` with timeout `POP_TIMEOUT_SECONDS` (2). Nothing ⇒ loop.
   An id that is not a valid UUID ⇒ log a warning, skip.
3. If shutdown was requested, do **not** claim: the job stays `pending` and the relay
   re-adds it. Otherwise claim. No row (stale id) ⇒ log info, loop.
4. Start the handler in a daemon thread. The main thread does all Postgres/Redis work and
   loops on `thread.join(wait)`, where `wait` is the time to the nearest deadline (next
   heartbeat, timeout, stall check, self-fence), at most `HEARTBEAT_SECONDS`:
   - finished ⇒ finalize (complete / retryable / permanent), loop to 1. Rowcount 0 on
     finalize ⇒ log a warning, discard the result, **do not exit** (nothing is running).
   - still running, heartbeat due ⇒ fenced heartbeat (also writes progress) and refresh
     `worker:<id>`. Rowcount 0 ⇒ ownership lost while the handler still runs ⇒ log error,
     `os._exit(1)`.
   - heartbeat raises (connection error) ⇒ log, reconnect, retry at the next wake-up.
     Track the last successful heartbeat (the claim counts as one, timed before the claim
     statement). If none succeeded for `LEASE_SECONDS - HEARTBEAT_SECONDS`, log and
     `os._exit(1)`: the worker must stop before its lease could expire, not after being told.
     This check runs in a **dedicated watchdog thread** that never touches Postgres or Redis,
     started before the handler and stopped when the handler ends. It cannot live in the main
     thread: a frozen Postgres keeps the TCP connection open, so a heartbeat can block forever
     without raising, and the check would only run after the database thawed.
   - timeout exceeded, or progress unchanged for `STALL_SECONDS` (types without a timeout,
     i.e. batch) ⇒ record a retryable failure (fenced), log, `os._exit(1)`.
     (Python cannot kill a thread; exiting the process kills the hung handler.
     Docker restarts the worker.)
5. `worker_id` = `f"{hostname}-{pid}-{short random}"`.
6. On Postgres/Redis connection errors in the loop: log, wait 1 s (interruptible by shutdown),
   reconnect, continue.

`LEASE_SECONDS` must be greater than `HEARTBEAT_SECONDS` (checked at startup).

**Graceful shutdown:** SIGTERM/SIGINT set a flag. The worker stops popping, finishes and
finalizes the current job, then exits 0. A worker as PID 1 in a container only receives
SIGTERM if it installs a handler, so it must. Idle workers stop within one pop timeout.
Compose `stop_grace_period: 30s` on worker.

**Scheduler** (`scheduler.py`): single loop.
- every `RELAY_INTERVAL_SECONDS` (1): promote scheduled → relay (ZADD due pending)
- every `REAPER_INTERVAL_SECONDS` (5): reap expired leases
- every `IDEMPOTENCY_CLEANUP_SECONDS` (300): expire idempotency keys
Each duty has its own next-run time kept with `time.monotonic()`, and the loop waits for the
earliest one. The clock only paces the loop. Whether a job is due or its lease expired is
always decided by Postgres `now()` inside the SQL.
Every operation is idempotent or uses SKIP LOCKED, so running 2 schedulers is safe.

**job_logs entries:** created, claimed (with attempt + worker), completed, failed attempt
(with error), retry scheduled (with delay), permanently failed, reaped, cancelled,
manually retried (with previous error).

**Logging:** stdlib `logging` with a JSON formatter to stdout. Every job-related line
includes `job_id`, `job_type`, `attempt`, `worker_id` where known.

---

## 7. API (`api.py`)

| Method | Path | Notes |
|---|---|---|
| POST | `/jobs` | body: `type`, `payload`, `priority` (0–100, default 0), `run_at` (optional ISO datetime; future ⇒ `scheduled`), `max_attempts` (1–10, default 4). Optional header `Idempotency-Key`. 201 new / 200 existing / 422 mismatch or invalid |
| GET | `/jobs/{id}` | full job incl. status, result, error, progress, attempts. 404 if missing |
| GET | `/jobs` | filters `status`, `type`; `limit` (default 50, max 200), `offset`; newest first. Filters composed with `psycopg.sql`, whitelisted |
| POST | `/jobs/{id}/cancel` | 200 / 404 / 409 |
| POST | `/jobs/{id}/retry` | only from `failed`; 200 / 404 / 409 |
| GET | `/dead-letter` | the dead letter queue: jobs with status `failed` (attempts used up or permanent error), newest first; filter `type`; `limit`, `offset` as `/jobs`. There is no separate store: `failed` is the dead state, `POST /jobs/{id}/retry` replays it |
| GET | `/jobs/{id}/logs` | job_logs for the job, oldest first |
| GET | `/health` | Postgres ok, Redis ok, counts per status, oldest due pending age (s), `queue:ready` size (ZCARD), live workers (count of `worker:*` keys). 503 if Postgres down |

Payload is validated per type with pydantic at the API (invalid ⇒ 422).
Use sync endpoints (`def`) and a `psycopg_pool.ConnectionPool` opened in the FastAPI lifespan.

---

## 8. Config (`config.py`, env vars with defaults)

```
DATABASE_URL, REDIS_URL
LEASE_SECONDS=30            HEARTBEAT_SECONDS=10       POP_TIMEOUT_SECONDS=2
STALL_SECONDS=60            BACKOFF_SECONDS=0.1,30,120 JOB_TIMEOUTS=email:10,webhook:10,report:15
RELAY_INTERVAL_SECONDS=1    REAPER_INTERVAL_SECONDS=5  IDEMPOTENCY_CLEANUP_SECONDS=300
IDEMPOTENCY_TTL_HOURS=24    JOB_SLEEP_SCALE=1.0        WEBHOOK_FAILURE_RATE=0.2
LOG_LEVEL=INFO
```
Tests override these (short lease, scale 0.01, failure rate 0 or 1) for speed and determinism.

---

## 9. Layout

```
src/job_queue_service/
  __init__.py  config.py  db.py  redis_client.py  models.py
  handlers.py  api.py  worker.py  scheduler.py  logging_setup.py
db/000_test_db.sql  db/001_schema.sql
tests/conftest.py  tests/test_*.py
Dockerfile  docker-compose.yml  pyproject.toml  uv.lock  .env.example
README.md  DECISIONS.md  AI_USAGE.md  CLAUDE.md
```

Docker:
- `FROM python:3.14-slim`, `requires-python = ">=3.14"`. If a dependency lacks 3.14
  wheels, STOP and tell the user.
- Mount `./db` to `/docker-entrypoint-initdb.d:ro` (runs only on an empty volume;
  schema changes need `docker compose down -v`).
- `api`, `worker`, `scheduler` share one image (YAML anchor); `depends_on` healthy
  postgres/redis; `restart: unless-stopped`; worker `stop_grace_period: 30s`.
- Credentials only from `.env` (gitignored); `.env.example` committed.

Tests:
- Run on the host: `docker compose up -d postgres redis` then `uv run pytest`.
- `conftest.py`: connect to `jobs_test`, drop/recreate the `public` schema and apply
  `db/001_schema.sql` once per session; truncate tables before each test; Redis DB 15,
  `FLUSHDB` before each test.
- Real Postgres and Redis only. No mocks of the database or Redis.

---

## 10. Phases — STOP after each one

### Phase 1 — Happy path (submit → queue → process → complete)
- Python 3.14 in Dockerfile and pyproject; `config.py`, `logging_setup.py`, `db.py`,
  `redis_client.py`, `models.py` (email payload only), full schema from section 4.
- API: `POST /jobs` (email only, no idempotency, no `run_at` yet), `GET /jobs/{id}`,
  `GET /health` (Postgres + Redis ok only). Fast-path ZADD.
- Scheduler: relay only.
- Worker: pop → claim → run email handler **in the main thread** (no heartbeat yet) →
  complete (fenced).
- Compose: add `scheduler` service, mount `./db`.
- Tests: submission + retrieval; completion flow (worker processes one job end to end
  by calling the worker's single-iteration function directly); claim of a non-pending
  job returns no row; finalize with the wrong `locked_by` affects 0 rows.
- Done when: `docker compose down -v && docker compose up --build`, curl a job, and
  `GET` shows `completed` within a few seconds.

### Phase 2 — All job types, failures, retries
- Webhook, report, batch handlers; error types; per-type payload validation.
- Retryable/permanent failure statements; backoff via `available_at`; max attempts.
- `job_logs` writes for every transition so far; `GET /jobs/{id}/logs`.
- Tests: failure → retry scheduled with `available_at` ≈ now + the first backoff delay (assert with a
  tolerance from Postgres `now()`); the last allowed attempt failing ⇒ `failed`; permanent error ⇒ `failed`
  after 1 attempt; batch completes with progress 100.

### Phase 3 — Priority, scheduling, cancel, retry, list, idempotency
- `run_at` ⇒ `scheduled`; scheduler promotes when due.
- Cancel, manual retry, list with filters (`psycopg.sql`, whitelist).
- `Idempotency-Key` header: 201 / 200 / 422; expiry cleanup in scheduler.
- Tests: priority ordering (enqueue low then high, the next pop returns high; equal
  priority pops FIFO); cancel pending ⇒ cancelled and the worker skips the stale id;
  cancel completed ⇒ 409; idempotency same request ⇒ same id, different payload ⇒ 422;
  **concurrent** idempotent submits (threads) ⇒ exactly one row; scheduled job not
  claimable before its time; manual retry only from failed.

### Phase 4 — Crash recovery and concurrency
- Handler in a daemon thread; heartbeat + progress; timeouts; stall detection;
  ownership-loss and hang ⇒ fenced write then `os._exit(1)`.
- Reaper in scheduler. Graceful shutdown. Worker liveness keys. Full `/health` stats.
- Compose: worker `deploy.replicas: 3`.
- Tests: expired lease is reaped to `pending` (attempts < max) or `failed`; heartbeat
  with a stale attempt returns 0 rows; **concurrency**: 3 worker subprocesses, 200 jobs,
  all complete, exactly one `claimed` log per job per attempt; **crash**: start a worker
  subprocess on a long report job, `kill -9` it, start another worker with a short lease,
  assert the job completes with `attempts = 2`; graceful shutdown: SIGTERM mid-job ⇒ job
  completes, process exits 0.

### Phase 5 — Documentation and polish
- `README.md`: setup (`cp .env.example .env`), run, run tests, example curl for each
  job type, architecture diagram (ASCII), scaling with `--scale worker=N`.
- `DECISIONS.md`: the five sections required by the PDF, plus "Guarantees",
  "Why Redis is not the source of truth", "Known limitations". Write it from this file;
  the user will edit it into their own words.
- `AI_USAGE.md`: headings from the PDF only.
- Final pass: `uv run ruff check`, all tests green from a clean `down -v`.

### Phase 6 (optional, only if the user asks) — Throughput benchmark
- Script that submits N jobs with `JOB_SLEEP_SCALE=0` and measures jobs/s for
  1, 2, 4, 8 workers; results table in README.