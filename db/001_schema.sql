CREATE TABLE jobs (
    id               uuid PRIMARY KEY,                 -- uuid7, generated in Python
    type             text NOT NULL CHECK (type IN ('email','webhook','report','batch')),
    payload          jsonb NOT NULL,
    status           text NOT NULL CHECK (status IN
                       ('scheduled','pending','processing','completed','failed','cancelled')),
    priority         integer NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 100),
    attempts         integer NOT NULL DEFAULT 0,
    max_attempts     integer NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
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
