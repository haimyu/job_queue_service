# Design Decisions

## 1. Job Pickup Strategy

- **Approach chosen:** workers `BZPOPMIN` a job ID from a Redis ZSET, then claim it in
  Postgres with a conditional `UPDATE` (`status = 'pending' AND available_at <= now()`).
  Rowcount 1 = owned, 0 = skip.
- **Why:** Postgres is the source of truth, Redis a rebuildable index. If the API's
  `ZADD` is lost, the relay re-adds due jobs every second.
- **Trade-offs:**
  - Gained: scales with workers (atomic pops instead of contended row locks), no polling,
    and Redis can be wiped safely.
  - Gave up: two systems, an extra round trip, and up to 1 s delay if the `ZADD` fails.

## 2. Worker Crash Recovery

- **Approach chosen:** 30 s lease, heartbeat every 10 s, reaper every 5 s, and every
  worker write fenced by `locked_by` + `attempts`.
- **Why:** a dead worker can't report itself, so missing heartbeats are the signal. A
  watchdog exits the worker after 20 s without a heartbeat, before its lease expires.
- **What happens if a worker crashes mid-job:** the lease expires, the reaper returns the
  job to `pending` (or `failed` if out of attempts), and another worker claims it. A waking
  zombie's writes match 0 rows. Execution is at-least-once.

## 3. Priority Queue Implementation

- **Approach chosen:** ZSET score = `-priority`, popped with `BZPOPMIN`. UUIDv7 IDs keep
  ties FIFO.
- **Why:** atomic, blocking, O(log n), rebuildable from Postgres.

## 4. Retry Backoff Strategy

- **Approach chosen:** backoff stored in `available_at`, not `sleep()`, so the worker is
  free at once and a crash can't lose the retry. Permanent errors fail immediately.
- **Timing:** 3 retries by default (the DB counts the first run too, so `max_attempts` = 4):
  the job runs at once → retry 1 after 0.1 s → retry 2 after 30 s → retry 3 after 120 s →
  `failed`.

## 5. Things I Would Do Differently With More Time

- **Relay redesign (the main one).** Every second the relay selects up to 1000 due jobs and
  re-adds all of them to Redis, even those already queued. The work per cycle is bounded by
  the `LIMIT`, so it does not blow up, but it is wasted work on every cycle and the scalability
  ceiling of the design. I would track what is already queued, or relay only newly due jobs.
- **Priority aging.** Workers always pop the highest priority first, so a steady stream of
  high-priority jobs can starve low-priority ones. I would raise a job's effective priority
  the longer it waits.
- **Checkpoint long jobs.** A batch stopped by a deploy or a crash restarts from the first
  item. I would save progress at every heartbeat, release the lease on shutdown, and resume
  from the checkpoint.
- **Improve the dead letter queue.** Failed jobs only keep an error message. I would add a
  failure reason and bulk replay.
