# Job Queue Service

Clients submit jobs over HTTP and separate worker processes run them. PostgreSQL holds the
state of every job. Redis only passes job ids to the workers.

Stack: Python 3.14, FastAPI, PostgreSQL 17 (psycopg 3, raw SQL), Redis 7, Docker Compose.
The design reasons are in [DECISIONS.md](DECISIONS.md).

## 1. Setup

You need Docker with the compose plugin.

```bash
cp .env.example .env               # working defaults; .env is git-ignored
docker compose up -d --build       # api, 3 workers, scheduler, postgres, redis
curl -s localhost:8000/health      # ready when "live_workers" is 3
```

- API: http://localhost:8000. Interactive docs: http://localhost:8000/docs
- Stop: `docker compose down`. Stop and delete the database: `docker compose down -v`.
- The schema (`db/001_schema.sql`) is applied only when the database volume is empty.
  After you change it, run `docker compose down -v`.
- All settings are in `.env` (see `.env.example`). After you edit `.env`, run
  `docker compose up -d` to apply the changes.

## 2. Logs

Every service writes one JSON line per event, with the job id, type, attempt and worker id
where known.

```bash
docker compose logs -f worker
docker compose logs | grep <job_id>    # follow one job across api, worker and scheduler
```

## 3. Test

The tests use the compose PostgreSQL and Redis, but in their own database (`jobs_test`) and
Redis database 15, so your dev data is never touched. They read the `POSTGRES_` values from
`.env`.

```bash
docker compose up -d postgres redis
uv run pytest       # about 25 s: some tests start, kill and stall real worker processes

```

**Windows:** use the same commands. Three process-level shutdown tests are skipped. Windows has
no inter-process SIGTERM: `Popen.send_signal(SIGTERM)` calls `TerminateProcess`, which is a hard
kill, so graceful shutdown can't be tested there. The worker itself runs fine on Windows.

To run all 91 tests on any OS, run them in a Linux container. This needs host networking: on
Docker Desktop, turn on "Enable host networking". Alternatively, run the commands above inside
WSL 2.

```bash
docker compose up -d postgres redis
docker run --rm --network host -v "$PWD":/app:z -w /app \
  -e UV_PROJECT_ENVIRONMENT=/tmp/venv -e UV_LINK_MODE=copy -e PYTHONDONTWRITEBYTECODE=1 \
  ghcr.io/astral-sh/uv:python3.14-bookworm-slim uv run pytest -p no:cacheprovider
```

## 4. Run it

### Stress test

`scripts/stress.py` sends a burst of mixed jobs to the running stack, including webhooks that
fail and retry. It cancels about 10% of them, adds scheduled jobs, and prints a live view, the
queue wait per priority and a summary.

It exits 1 if any check fails:

- it timed out
- a job got stuck
- one idempotency key created more than one job
- a cancel and a claim both won, or neither did
- a scheduled job started early
- a job completed twice

For a fast run, add these lines to `.env` and run `docker compose up -d`:

```
WEBHOOK_FAILURE_RATE=0.2
JOB_SLEEP_SCALE=0.1
BACKOFF_SECONDS=2,4,8
```

```bash
uv run python scripts/stress.py --jobs 120
```

### Add workers

Each worker runs one job at a time, so 8 workers run 8 jobs at once.

```bash
docker compose up -d --scale worker=8
```

### Submit jobs

Every request returns the job. Save its `id`, then poll `GET /jobs/{id}` until `status` is
`completed` or `failed`.

```bash
# email
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "type": "email",
  "payload": {"to": "user@example.com", "subject": "Hello", "body": "Hi there"}
}'

# webhook (WEBHOOK_FAILURE_RATE of attempts fail on purpose)
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "type": "webhook",
  "payload": {"url": "https://example.com/hook", "body": {"event": "created"}}
}'

# report
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "type": "report",
  "payload": {"report_type": "daily", "params": {"day": "2026-11-10"}}
}'

# batch (1 to 1000 items, reports progress after each item)
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "type": "batch",
  "payload": {"items": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
}'
```

Optional fields on `POST /jobs`:

- `priority`: 0 to 100, default 0. Higher runs first.
- `max_attempts`: 1 to 10, default 4 (the first run plus 3 retries).
- `run_at`: an ISO datetime with a timezone. A future time makes the job `scheduled`.
- the `Idempotency-Key` header: sending the same key with the same type and payload returns
  the same job (HTTP 200 instead of 201). The same key with a different type or payload
  returns 422.

```bash
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' -d '{
  "type": "email", "priority": 90,
  "payload": {"to": "user@example.com", "subject": "Urgent", "body": "Hi"}
}'
```

Other endpoints:

```bash
curl -s localhost:8000/jobs/<id>                # status, result, error, progress, attempts
curl -s localhost:8000/jobs/<id>/logs           # every state change of the job
curl -s 'localhost:8000/jobs?status=failed&type=webhook&limit=20'
curl -s -X POST localhost:8000/jobs/<id>/cancel # pending or scheduled jobs only
curl -s -X POST localhost:8000/jobs/<id>/retry  # failed jobs only
curl -s localhost:8000/dead-letter              # failed jobs and their last error
curl -s localhost:8000/health                   # counts per status, queue size, live workers
```

## 5. Architecture

```
  client ──HTTP──►  api (FastAPI)
                     │
          1. INSERT + COMMIT         2. ZADD (best effort)
                     ▼                        ▼
             ┌───────────────┐        ┌────────────────────┐
             │  PostgreSQL   │        │  Redis             │
             │  jobs, logs   │        │  queue:ready       │
             │  = the truth  │        │  worker:<id> keys  │
             └──▲────────▲───┘        └──▲──────────┬──────┘
                │        │               │ ZADD     │ BZPOPMIN
                │        │               │          ▼
      reap, promote      │          ┌────┴─────┐  ┌────────────────────┐
      (every 1 s / 5 s)  │          │scheduler │  │ worker x N         │
                │        │          │ relay    │  │ claim, heartbeat,  │
                └────────┼──────────┤ reaper   │  │ finish             │
                         │          └──────────┘  └─────────┬──────────┘
                         └────────────────────────────────┘
                     claim / heartbeat / finish: every write checks
                     that this worker still owns the job
```

1. **Submit.** The API commits the job to PostgreSQL, then adds its id to Redis. If Redis is
   down, the API still returns success.
2. **Claim.** A worker pops the highest-priority id (`BZPOPMIN`) and claims the job with one
   conditional `UPDATE`. Only one worker can win.
3. **Run.** The worker extends its lease every `HEARTBEAT_SECONDS`. Every write it makes
   checks that it still owns the job.
4. **Schedule and reap.** Every second, the scheduler makes due `scheduled` jobs `pending` and
   re-adds every due `pending` job to Redis. Every 5 seconds, it returns jobs with an expired
   lease (a dead worker) to `pending`, or to `failed` if no attempts are left.
5. **Recover.** If Redis loses its data, nothing is lost: the scheduler re-adds the jobs
   within a second.

Job states: `scheduled` → `pending` → `processing` → `completed` or `failed`. A failed attempt
goes back to `pending` after a backoff delay. A job can be `cancelled` only from `pending` or
`scheduled`.