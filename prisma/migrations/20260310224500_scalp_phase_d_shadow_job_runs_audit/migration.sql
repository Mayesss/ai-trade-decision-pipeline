CREATE TYPE scalp_shadow_job_outcome AS ENUM (
  'processed',
  'skipped'
);

CREATE TABLE scalp_shadow_job_runs (
  id BIGSERIAL PRIMARY KEY,
  job_id BIGINT NOT NULL,
  kind scalp_job_kind NOT NULL,
  dedupe_key TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  attempt_no INT NOT NULL CHECK (attempt_no >= 1),
  outcome scalp_shadow_job_outcome NOT NULL,
  success BOOLEAN NOT NULL,
  completion_applied BOOLEAN NOT NULL DEFAULT TRUE,
  error_code TEXT,
  error_message TEXT,
  claimed_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ NOT NULL,
  duration_ms INT NOT NULL CHECK (duration_ms >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_shadow_job_runs_unique_job_attempt UNIQUE (job_id, attempt_no)
);

CREATE INDEX scalp_shadow_job_runs_job_created_idx
  ON scalp_shadow_job_runs(job_id, created_at DESC);

CREATE INDEX scalp_shadow_job_runs_kind_created_idx
  ON scalp_shadow_job_runs(kind, created_at DESC);

CREATE INDEX scalp_shadow_job_runs_success_created_idx
  ON scalp_shadow_job_runs(success, created_at DESC);

CREATE INDEX scalp_shadow_job_runs_worker_created_idx
  ON scalp_shadow_job_runs(worker_id, created_at DESC);

CREATE INDEX scalp_shadow_job_runs_created_idx
  ON scalp_shadow_job_runs(created_at DESC);
