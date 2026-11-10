## Known limitations

- **A timeout that fires as the handler finishes causes a retry, so   the handler
  finishes in the instant between the deadline check and that write, its work is done but the
  job is retried. This is inherent in at-least-once delivery: the system promises that a job
  runs at least once, never that it runs exactly once.
- **A worker that loses Postgres while finalizing leaves the job to the reaper.** The job is
  retried after its lease expires, so a handler can run again after it already succeeded.
  Same cause as above.
- **FIFO among equal priorities relies on UUIDv7 ordering.** Python's `uuid7()` is monotonic
  inside one process. Two API replicas creating jobs in the same millisecond may order
  slightly differently.
- **`/health` counts jobs per status with a `GROUP BY`.** It reads the whole status index, so
  its cost grows with the table. Fine for this scale; a larger deployment would cache it or
  use estimates.
- **There is no client-side timeout on database calls.** The lease watchdog stops a worker whose
  handler is running when Postgres freezes, but a worker that is idle, claiming or finalizing
  waits for the database to answer (there is no `connect_timeout`, `statement_timeout` or TCP
  user timeout). Nothing is running in those cases, so it only delays recovery. A restarted
  worker also waits at startup until Postgres answers.
- **A claim that returns very late can start the handler briefly.** If Postgres freezes during
  the claim statement and answers after the lease margin has passed, the watchdog fires as soon
  as it starts, but the handler thread is started right after it and may run for a few
  milliseconds first.

