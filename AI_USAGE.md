# AI Tool Usage

## Tools I Used

    Claude (chat)
    Claude Code

## What Helped Most

I designed the skeleton and the ai found the blind spots for exemple 
- fencing: every worker write checks locked_by + attempts, so a zombie can't
  overwrite the real owner.
- backoff as data: retries set available_at = now() + delay instead of
  sleep(), so no worker sits idle and a crash can't lose a pending retry.

## What I Had to Fix

- Missing self-fence. Claude Code's worker did nothing when the heartbeat
   couldn't reach Postgres, so the lease expired and two workers ran the same
   job. Caught in review. fix, no successful heartbeat for 20 s os._exit(1).
   Verified by pausing Postgres mid-job.

- Claim design. The AI had workers claim from Postgres with SKIP LOCKED. I
   changed it to Redis dispatch plus a Postgres relay: no claim contention,
   and Redis can be wiped without losing a job.

## What AI Struggled With
Not much. 
