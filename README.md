# Job Queue Service

A background job queue. Clients submit jobs over HTTP, separate worker processes run them,
and PostgreSQL keeps the state of every job. Redis only passes job ids to the workers.

Stack: Python 3.14, FastAPI, PostgreSQL 17 (psycopg 3, raw SQL), Redis 7, docker compose.

The design reasons are in [DECISIONS.md](DECISIONS.md).

## Run it

You need Docker with the compose plugin.

```bash
cp .env.example .env
```

Open `.env` and give the three `POSTGRES_` values a value (for example `jobs` for all three).
They are the database credentials. The file is git-ignored.

```bash
docker compose up -d --build       # api, 3 workers, scheduler, postgres, redis
curl -s localhost:8000/health      # ready when "live_workers" is 3
```

- API: http://localhost:8000
- Interactive API docs (try every endpoint in the browser): http://localhost:8000/docs
- Stop: `docker compose down`. Stop and delete the database: `docker compose down -v`.
- The database schema is applied only when the database volume is empty. After changing
  `db/001_schema.sql`, run `docker compose down -v`.

### Logs

Every service writes JSON lines to stdout, one line per event, with the job id, type, attempt
and worker id where known.

```bash
docker compose logs -f worker
docker compose logs | grep <job_id>      # follow one job through api, worker and scheduler
```

## Run the tests

The tests use the real PostgreSQL and Redis from compose. They use their own database
(`jobs_test`) and Redis database 15, so they never touch your dev data.

```bash
docker compose up -d postgres redis
uv sync
uv run pytest
uv run ruff check
```

The tests read the `POSTGRES_` values from your `.env`. Some tests start real worker
processes, kill them, and freeze the database connection, so the full run takes about 25 seconds.

## Submit jobs

Every request returns the job. Save its `id`, then poll `GET /jobs/{id}` until `status` is
`completed` or `failed`.

```bash
# email
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "type": "email",
  "payload": {"to": "user@example.com", "subject": "Hello", "body": "Hi there"}
}'

# webhook (20% of attempts fail on purpose, see WEBHOOK_FAILURE_RATE)
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

Optional fields on `POST /jobs`: `priority` (0 to 100, default 0, higher runs first),
`max_attempts` (1 to 10, default 4: the first run plus 3 retries), `run_at` (an ISO datetime
with a timezone; a future time makes the job `scheduled`), and the header `Idempotency-Key`.

```bash
curl -s localhost:8000/jobs/<id>                # status, result, error, progress, attempts
curl -s localhost:8000/jobs/<id>/logs           # every state change of the job
curl -s 'localhost:8000/jobs?status=failed&type=webhook&limit=20'
curl -s -X POST localhost:8000/jobs/<id>/cancel # only pending or scheduled jobs
curl -s -X POST localhost:8000/jobs/<id>/retry  # only failed jobs
curl -s localhost:8000/dead-letter              # the dead letter queue: failed jobs and their last error
curl -s localhost:8000/health                   # counts per status, queue size, live workers
```

A high priority job, and a job with an idempotency key:

```bash
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' -d '{
  "type": "email", "priority": 90,
  "payload": {"to": "user@example.com", "subject": "Urgent", "body": "Hi"}
}'
```

Sending the same key with the same type and payload again returns the same job (HTTP 200
instead of 201). The same key with a different type or payload returns 422.

## Architecture

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

1. The API saves the job in PostgreSQL, then adds its id to Redis so a worker can start
   it at once. If Redis is down, the API still answers success.
2. A worker takes the highest priority id from Redis (`BZPOPMIN`) and claims the job with one
   conditional `UPDATE`. Only one worker can win the claim.
3. While the job runs, the worker extends its lease every 10 seconds (a heartbeat). Every
   write by a worker checks that the worker still owns the job.
4. The scheduler, every second, makes `scheduled` jobs `pending` when their time comes and
   adds every due `pending` job to Redis again. Every 5 seconds it returns jobs with an
   expired lease (a dead worker) to `pending`, or marks them `failed` if no attempts are left.
5. If Redis loses its data, nothing is lost. The scheduler puts the jobs back within a second.

Job states: `scheduled` → `pending` → `processing` → `completed` or `failed`. A failed attempt
goes back to `pending` with a delay. `cancelled` is reached from `pending` or `scheduled`.

## Scale the workers

The compose file starts 3 workers. Change the number with `--scale`:

```bash
docker compose up -d --scale worker=8
```

Each worker runs one job at a time, so 8 workers run 8 jobs at the same time.

## Stress test

`scripts/stress.py` sends a burst of mixed jobs (including webhooks that fail and retry) to
the running stack, cancels about 10% of them, adds scheduled jobs, and prints a live view, a
queue-wait table per priority and a summary. It exits 1 if a check fails: it timed out, a job
is stuck, the idempotency key created more than one job, a cancel and a claim both won (or
neither), a scheduled job started early, or a job completed twice. For a fast run, put these lines in
`.env` and run `docker compose up -d`:

```
WEBHOOK_FAILURE_RATE=0.2
JOB_SLEEP_SCALE=0.1
BACKOFF_SECONDS=2,4,8
```

```bash
uv run python scripts/stress.py --jobs 120
```

## Settings

All settings are environment variables with defaults, listed in [.env.example](.env.example).
The compose file passes `.env` to the api, workers and scheduler. `DATABASE_URL` and
`REDIS_URL` are set by compose and have no default: a service without them stops at startup
and names the missing variable.

| Setting | Default | Meaning |
|---|---|---|
| `LEASE_SECONDS` | 30 | How long a worker owns a job without a heartbeat. Must be larger than `HEARTBEAT_SECONDS`. |
| `HEARTBEAT_SECONDS` | 10 | How often a running worker extends its lease. |
| `BACKOFF_SECONDS` | 0.1,30,120 | Delay before retry 1, 2, 3. |
| `JOB_TIMEOUTS` | email:10,webhook:10,report:15 | Maximum run time per type. Batch has none and is watched for stalls (`STALL_SECONDS`, 60). |
| `WEBHOOK_FAILURE_RATE` | 0.2 | Chance that a webhook attempt fails on purpose. |
| `JOB_SLEEP_SCALE` | 1.0 | Multiplies the simulated work time of every handler. |
