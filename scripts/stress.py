"""Stress test for the running stack (docker compose up -d).

Submits many jobs at once, cancels about 10% of them right away, adds a few scheduled jobs,
then prints a live line every second (job counts, queue size, live workers, oldest waiting
job) and a summary at the end.

    uv run python scripts/stress.py --jobs 200 --clients 20

It exits 1 when a check fails: the wait timed out, a job is not terminal, 20 parallel submits
with one Idempotency-Key did not create exactly one job, a cancel that returned 200 did not
cancel (or the job was claimed anyway), a cancel that returned 409 hit a job that was never
claimed, a scheduled job started before its run_at, or a job has more than one "job completed"
log (a completed job must have exactly one).

A type written as webhook@2 means a webhook job with max_attempts=2 (1 is the minimum, so
webhook@1 fails for good on its first failure). The default mix has webhook@1, @2 and @3.

The webhook failure rate comes from the containers, not from this script. For a realistic
and fast run put this in .env and run `docker compose up -d`:

    WEBHOOK_FAILURE_RATE=0.2   # 20% of webhook attempts fail
    JOB_SLEEP_SCALE=0.1        # jobs run 10x faster
    BACKOFF_SECONDS=2,4,8      # retries wait 2 s, 4 s and 8 s instead of 0.1 s, 30 s and 120 s

WEBHOOK_FAILURE_RATE=1 makes every webhook attempt fail, so each webhook job uses all its
attempts and ends as failed (failed is a normal end state, not a stuck job).
"""

import argparse
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import httpx

PAYLOADS = {
    "email": {"to": "a@b.com", "subject": "stress", "body": "x"},
    "webhook": {"url": "http://example.com/hook"},
    "report": {"report_type": "daily"},
    "batch": {"items": list(range(10))},
}
TERMINAL = ("completed", "failed", "cancelled")
# Log messages written by db.py.
CLAIMED = "job claimed"
COMPLETED = "job completed"


def submit(
    client: httpx.Client,
    label: str,
    priority: int,
    key: str | None = None,
    run_at: str | None = None,
):
    """Submit a job. The label is a type, or type@max_attempts such as webhook@2."""
    job_type, _, max_attempts = label.partition("@")
    headers = {"Idempotency-Key": key} if key else {}
    body = {"type": job_type, "payload": PAYLOADS[job_type], "priority": priority}
    if max_attempts:
        body["max_attempts"] = int(max_attempts)
    if run_at:
        body["run_at"] = run_at
    return client.post("/jobs", json=body, headers=headers)


def body(response: httpx.Response):
    """The JSON body, or None when there is none (a 500 from FastAPI is plain text)."""
    try:
        return response.json()
    except ValueError:
        return None


def seconds(text: str) -> float:
    return datetime.fromisoformat(text).timestamp()


def percentile(values: list[float], p: float) -> float:
    return sorted(values)[min(len(values) - 1, int(len(values) * p))]


def show(value, width: int, spec: str = "") -> str:
    """Format a /health number. It is null (shown as -) while Redis is unreachable."""
    return f"{'-' if value is None else format(value, spec):>{width}}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--jobs", type=int, default=200)
    parser.add_argument("--clients", type=int, default=20, help="parallel HTTP clients")
    parser.add_argument(
        "--types", default="email,report,batch,webhook@1,webhook@2,webhook@3"
    )
    parser.add_argument(
        "--timeout", type=float, default=300, help="give up waiting after this"
    )
    parser.add_argument(
        "--force", action="store_true", help="run even if the queue is not empty"
    )
    args = parser.parse_args()
    types = args.types.split(",")

    client = httpx.Client(base_url=args.url, timeout=30)
    health = client.get("/health").json()
    if not health["postgres"]:
        sys.exit("API says postgres is down")
    # Finished jobs from an earlier run would be counted as ours and end the wait too early.
    counts = health["jobs"]
    waiting = counts["scheduled"] + counts["pending"] + counts["processing"]
    if waiting and not args.force:
        sys.exit(
            f"{waiting} jobs are still scheduled, queued or running. "
            "Let them finish, or use --force."
        )
    failures = []  # one line per failed check; any entry makes the exit code 1

    # Snapshot before we submit anything, so every job we add is counted exactly once.
    done_before = sum(counts[s] for s in TERMINAL)

    # 1) Idempotency under concurrency: 20 parallel submits, one key, must be one job.
    key = f"stress-{uuid.uuid4()}"
    with ThreadPoolExecutor(20) as pool:
        replies = list(pool.map(lambda _: submit(client, "email", 0, key), range(20)))
    created = sum(r.status_code == 201 for r in replies)
    odd = sorted({r.status_code for r in replies} - {200, 201})
    ids = {body(r)["id"] for r in replies if r.status_code in (200, 201)}
    if odd:
        failures.append(f"idempotency: unexpected status codes {odd}")
    print(
        f"idempotency: 20 parallel submits -> {created} created, {len(ids)} distinct id(s)"
    )
    if created != 1 or len(ids) != 1:
        failures.append(f"idempotency: {created} created, {len(ids)} distinct ids")

    # 2) Burst of jobs, mixed types and priorities. The priority steps every time the type
    # list has been used once, so each type gets all three priorities. Every 10th job is
    # cancelled right after it is accepted, while the workers are running.
    def send(i: int):
        reply = submit(client, types[i % len(types)], (i // len(types)) % 3 * 50)
        cancel = None
        if reply.status_code == 201 and i % 10 == 0:
            cancel = client.post(f"/jobs/{reply.json()['id']}/cancel").status_code
        return reply, cancel

    started = time.monotonic()
    with ThreadPoolExecutor(args.clients) as pool:
        sent = list(pool.map(send, range(args.jobs)))
    submit_time = time.monotonic() - started
    bad = [r.status_code for r, _ in sent if r.status_code != 201]
    if bad:
        failures.append(f"{len(bad)} submits were not 201, e.g. {bad[:5]}")
    ids = [r.json()["id"] for r, _ in sent if r.status_code == 201]
    labels = {
        r.json()["id"]: types[i % len(types)]
        for i, (r, _) in enumerate(sent)
        if r.status_code == 201
    }
    cancels = {r.json()["id"]: c for r, c in sent if c is not None}
    print(
        f"submitted {len(ids)}/{args.jobs} in {submit_time:.2f}s "
        f"({len(ids) / submit_time:.0f} jobs/s accepted), errors: {bad or 'none'}"
    )
    print(
        f"cancel: {len(cancels)} tried, {list(cancels.values()).count(200)} got 200, "
        f"{list(cancels.values()).count(409)} got 409"
    )

    # 3) A few jobs for later: they must not start before their run_at.
    run_at = (datetime.now(UTC) + timedelta(seconds=5)).isoformat()
    scheduled = [submit(client, "email", 0, run_at=run_at) for _ in range(3)]
    scheduled_ids = [r.json()["id"] for r in scheduled if r.status_code == 201]
    if len(scheduled_ids) != 3:
        failures.append("could not submit the 3 scheduled jobs")
    print(f"scheduled {len(scheduled_ids)} jobs to start at {run_at}")

    # 4) Watch the system drain. All our jobs end terminal: the idempotent one, the burst
    # (cancelled ones included) and the scheduled ones.
    expected = 1 + len(ids) + len(scheduled_ids)
    print(
        f"{'t':>5}  {'pending':>7} {'processing':>10} {'completed':>9} {'failed':>6}  "
        f"{'queue':>5} {'workers':>7} {'oldest wait':>11}"
    )
    timed_out = True
    while time.monotonic() - started < args.timeout:
        health = client.get("/health").json()
        jobs = health["jobs"]
        # jobs is null if Postgres is down; then there is nothing to count this round.
        if jobs:
            print(
                f"{time.monotonic() - started:5.0f}  {jobs['pending']:7} {jobs['processing']:10} "
                f"{jobs['completed']:9} {jobs['failed']:6}  {show(health['queue_ready'], 5)} "
                f"{show(health['live_workers'], 7)} "
                f"{show(health['oldest_due_pending_age_seconds'], 10, '.1f')}s"
            )
            if sum(jobs[s] for s in TERMINAL) - done_before >= expected:
                timed_out = False
                break
        time.sleep(1)
    total = time.monotonic() - started
    if timed_out:
        print(f"timed out after {args.timeout:.0f}s")
        failures.append(f"timed out after {args.timeout:.0f}s")

    # 5) Summary from the jobs themselves.
    def fetch(job_id: str):
        job = client.get(f"/jobs/{job_id}")
        logs = client.get(f"/jobs/{job_id}/logs")
        if job.status_code != 200 or logs.status_code != 200:
            failures.append(
                f"GET {job_id} returned {job.status_code}/{logs.status_code}"
            )
            return None
        return job.json(), logs.json()

    with ThreadPoolExecutor(min(args.clients, 50)) as pool:
        fetched = list(pool.map(fetch, ids + scheduled_ids))
    both = {i: pair for i, pair in zip(ids + scheduled_ids, fetched) if pair}
    final = [job for job, _ in both.values()]
    status = {}
    attempts = {}
    for job in final:
        status[job["status"]] = status.get(job["status"], 0) + 1
        attempts[job["attempts"]] = attempts.get(job["attempts"], 0) + 1
    waits = [
        seconds(j["started_at"]) - seconds(j["created_at"])
        for j in final
        if j["started_at"]
    ]
    runs = [
        seconds(j["completed_at"]) - seconds(j["started_at"])
        for j in final
        if j["completed_at"] and j["started_at"] and j["status"] == "completed"
    ]
    print("\n--- summary ---")
    burst = [both[i][0] for i in ids if i in both and both[i][0]["completed_at"]]
    print(
        f"total time          {total:.1f}s (includes the 5 s wait for the scheduled jobs)"
    )
    if burst:
        # Server timestamps: first burst job created -> last burst job finished.
        span = max(seconds(j["completed_at"]) for j in burst) - min(
            seconds(j["created_at"]) for j in burst
        )
        print(
            f"burst throughput    {len(burst) / span:.1f} jobs/s ({len(burst)} jobs in {span:.1f}s)"
        )
    print(f"final statuses      {status}")
    print(f"attempts per job    {dict(sorted(attempts.items()))}")
    if waits:
        print(
            f"queue wait          p50 {statistics.median(waits):.2f}s  "
            f"p95 {percentile(waits, 0.95):.2f}s  max {max(waits):.2f}s"
        )
    if runs:
        print(
            f"run time            p50 {statistics.median(runs):.2f}s  "
            f"p95 {percentile(runs, 0.95):.2f}s"
        )
    print("\nqueue wait by priority (started_at - created_at; scheduled jobs excluded)")
    print("  priority  jobs   p50      p95      max")
    median_wait = {}
    for priority in (100, 50, 0):
        mine = [
            seconds(j["started_at"]) - seconds(j["created_at"])
            for j in final
            if j["priority"] == priority
            and j["started_at"]
            and j["id"] not in scheduled_ids
        ]
        if mine:
            median_wait[priority] = statistics.median(mine)
            print(
                f"  {priority:>8} {len(mine):5} {median_wait[priority]:6.2f}s "
                f"{percentile(mine, 0.95):7.2f}s {max(mine):7.2f}s"
            )
    # Only judge priority when there was a real backlog: with an empty queue every job
    # starts at once and the three medians are equal noise.
    if median_wait.get(0, 0) > 1 and median_wait.get(100, 0) >= median_wait[0]:
        failures.append(
            f"priority: p50 wait {median_wait[100]:.2f}s at 100 is not below "
            f"{median_wait[0]:.2f}s at 0"
        )
    print(
        "\nper job kind        jobs  completed  failed  avg attempts  max attempts allowed"
    )
    for label in types:
        mine = [j for j in final if labels.get(j["id"]) == label]
        if mine:
            print(
                f"  {label:<16} {len(mine):5} {sum(j['status'] == 'completed' for j in mine):10} "
                f"{sum(j['status'] == 'failed' for j in mine):7} "
                f"{sum(j['attempts'] for j in mine) / len(mine):13.2f} {mine[0]['max_attempts']:>10}"
            )
    hooks = [j for j in final if j["type"] == "webhook"]
    if hooks:
        # Every attempt that did not end in "completed" counts, whatever the cause: the
        # simulated failure, a timeout, or a lease that expired.
        tries = sum(j["attempts"] for j in hooks)
        lost = tries - sum(j["status"] == "completed" for j in hooks)
        print(
            f"webhook attempts    {tries} total, {lost} not successful (any cause) "
            f"-> {lost / tries:.0%}"
        )
    retried = [j for j in final if j["attempts"] > 1]
    if retried:
        print(
            f"\nlog of one retried job ({retried[0]['id']}, now {retried[0]['status']}):"
        )
        for entry in both[retried[0]["id"]][1]:
            print(
                f"  {entry['created_at'][11:23]} {entry['level']:<7} {entry['message']}"
            )

    # 6) Checks on the jobs.
    stuck = [j["id"] for j in final if j["status"] not in TERMINAL]
    print(f"\nstuck (not terminal) {len(stuck)}" + (f": {stuck[:5]}" if stuck else ""))
    if stuck:
        failures.append(f"{len(stuck)} jobs not terminal")
    for job_id, code in cancels.items():
        if job_id not in both:
            continue  # the fetch failure is already recorded
        job, logs = both[job_id]
        claimed = any(e["message"] == CLAIMED for e in logs)
        if code == 200 and (job["status"] != "cancelled" or job["attempts"] or claimed):
            failures.append(
                f"cancel 200 but {job_id} is {job['status']}, claimed={claimed}"
            )
        if code == 409 and not claimed:
            failures.append(f"cancel 409 but {job_id} was never claimed")
        if code not in (200, 409):
            failures.append(f"cancel of {job_id} returned {code}")
    for job_id in scheduled_ids:
        if job_id not in both:
            continue
        job = both[job_id][0]
        # Compare with the run_at we sent. available_at would be wrong after a reap, which
        # sets it to now().
        if job["started_at"] and seconds(job["started_at"]) < seconds(run_at):
            failures.append(
                f"scheduled {job_id} started {job['started_at']} before {run_at}"
            )
    for job_id, (job, logs) in both.items():
        done = sum(e["message"] == COMPLETED for e in logs)
        if done > 1 or (job["status"] == "completed") != (done == 1):
            failures.append(
                f"{job_id} is {job['status']} with {done} '{COMPLETED}' logs"
            )
    print(
        "check the logs:      docker compose logs | grep <job_id>     |     GET /jobs/<id>/logs"
    )
    if failures:
        print(f"\nFAILED ({len(failures)}):")
        for line in failures[:20]:
            print(f"  - {line}")
        sys.exit(1)
    print("\nall checks passed")


if __name__ == "__main__":
    main()
