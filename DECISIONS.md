# Design Decisions

## 1. Job Pickup Strategy
Approach: workers BZPOPMIN a job ID from a Redis ZSET, then claim it in
Postgres with a conditional UPDATE (status='pending' AND available_at<=now()).
Rowcount 1 = owned, 0 = skip.
Why: Postgres is the source of truth, Redis a rebuildable index. If the
API's ZADD is lost, the relay re-adds due jobs every second.
Trade-offs: scales with workers (atomic pops instead of contended row
locks), no polling, and Redis can be wiped safely, but two systems, an
extra round trip, and up to 1 s delay if the ZADD fails.

## 2. Worker Crash Recovery
Approach: 30 s lease, heartbeat every 10 s, reaper every 5 s, and every
worker write fenced by locked_by + attempts.
Why: a dead worker can't report itself, so missing heartbeats are the
signal. A watchdog exits the worker after 20 s without a heartbeat, before
its lease expires.
Crash mid-job: the lease expires, the reaper returns the job to pending (or
failed if out of attempts), and another worker claims it. A waking zombie's
writes match 0 rows. Execution is at-least-once.

## 3. Priority Queue Implementation
Approach: ZSET score = -priority, popped with BZPOPMIN. UUIDv7 IDs keep ties
FIFO.
Why: atomic, blocking, O(log n), rebuildable from Postgres. No aging, so
low priority can starve.

## 4. Retry Backoff Strategy
Approach: backoff stored in available_at, not sleep(), so the worker is
free at once and a crash can't lose the retry. Permanent errors fail
immediately.
Timing: immediate → 30 s → 2 min → failed (3 attempts).