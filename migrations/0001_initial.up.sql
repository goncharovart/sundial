-- Sundial schema, migration 0001 (initial).
-- Apply to a Postgres 14+ database. Idempotent within a single migration run.

CREATE TABLE IF NOT EXISTS sundial_jobs (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text UNIQUE NOT NULL,
    schedule        text NOT NULL,
    schedule_kind   text NOT NULL CHECK (schedule_kind IN ('cron', 'every', 'at')),
    next_fire_time  timestamptz NOT NULL,
    last_run_at     timestamptz,
    last_status     text CHECK (last_status IN ('success', 'failed', 'dead-letter') OR last_status IS NULL),
    payload         jsonb,
    options         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sundial_jobs_due_idx
    ON sundial_jobs (next_fire_time);

CREATE TABLE IF NOT EXISTS sundial_runs (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      uuid NOT NULL REFERENCES sundial_jobs(id) ON DELETE CASCADE,
    fire_time   timestamptz NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    attempt     int NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    status      text NOT NULL CHECK (status IN ('running', 'success', 'failed', 'dead-letter')),
    error       text,
    node_id     text NOT NULL
);

CREATE INDEX IF NOT EXISTS sundial_runs_job_id_idx
    ON sundial_runs (job_id, fire_time DESC);

CREATE INDEX IF NOT EXISTS sundial_runs_status_idx
    ON sundial_runs (status)
    WHERE status IN ('running', 'failed');
