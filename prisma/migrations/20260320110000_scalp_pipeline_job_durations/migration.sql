ALTER TABLE scalp_pipeline_jobs
    ADD COLUMN IF NOT EXISTS last_duration_ms integer;

CREATE TABLE IF NOT EXISTS scalp_pipeline_job_runs (
    id bigserial PRIMARY KEY,
    job_kind text NOT NULL,
    status text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    duration_ms integer NOT NULL CHECK (duration_ms >= 0),
    processed integer NOT NULL DEFAULT 0,
    succeeded integer NOT NULL DEFAULT 0,
    retried integer NOT NULL DEFAULT 0,
    failed integer NOT NULL DEFAULT 0,
    pending_after integer NOT NULL DEFAULT 0,
    downstream_requested boolean NOT NULL DEFAULT false,
    error text,
    progress_label text,
    details_json jsonb,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_pipeline_job_runs_kind_started_idx
    ON scalp_pipeline_job_runs(job_kind, started_at DESC);

CREATE INDEX IF NOT EXISTS scalp_pipeline_job_runs_finished_idx
    ON scalp_pipeline_job_runs(finished_at DESC);
