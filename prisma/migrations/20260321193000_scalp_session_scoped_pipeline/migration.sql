-- Session-scoped scalp planner/worker/executor persistence.

ALTER TABLE scalp_deployments
    ADD COLUMN IF NOT EXISTS entry_session_profile text;

UPDATE scalp_deployments
SET entry_session_profile = CASE
    WHEN LOWER(COALESCE(NULLIF(entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork')
      THEN LOWER(entry_session_profile)
    WHEN LOWER(COALESCE(NULLIF(config_override #>> '{sessions,entrySessionProfile}', ''), '')) IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork')
      THEN LOWER(config_override #>> '{sessions,entrySessionProfile}')
    ELSE 'berlin'
END;

ALTER TABLE scalp_deployments
    ALTER COLUMN entry_session_profile SET DEFAULT 'berlin',
    ALTER COLUMN entry_session_profile SET NOT NULL;

ALTER TABLE scalp_deployments
    DROP CONSTRAINT IF EXISTS scalp_deployments_entry_session_profile_check;

ALTER TABLE scalp_deployments
    ADD CONSTRAINT scalp_deployments_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork'));

CREATE INDEX IF NOT EXISTS scalp_deployments_entry_session_profile_idx
    ON scalp_deployments(entry_session_profile);
CREATE INDEX IF NOT EXISTS scalp_deployments_session_symbol_idx
    ON scalp_deployments(entry_session_profile, symbol);

ALTER TABLE scalp_pipeline_jobs
    ADD COLUMN IF NOT EXISTS entry_session_profile text;

UPDATE scalp_pipeline_jobs
SET entry_session_profile = CASE
    WHEN LOWER(COALESCE(NULLIF(entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork')
      THEN LOWER(entry_session_profile)
    ELSE 'berlin'
END;

ALTER TABLE scalp_pipeline_jobs
    ALTER COLUMN entry_session_profile SET DEFAULT 'berlin',
    ALTER COLUMN entry_session_profile SET NOT NULL;

ALTER TABLE scalp_pipeline_jobs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_jobs_entry_session_profile_check;

ALTER TABLE scalp_pipeline_jobs
    ADD CONSTRAINT scalp_pipeline_jobs_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork'));

ALTER TABLE scalp_pipeline_jobs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_jobs_pkey;

ALTER TABLE scalp_pipeline_jobs
    ADD CONSTRAINT scalp_pipeline_jobs_pkey PRIMARY KEY(job_kind, entry_session_profile);

CREATE INDEX IF NOT EXISTS scalp_pipeline_jobs_session_status_idx
    ON scalp_pipeline_jobs(entry_session_profile, status);
CREATE INDEX IF NOT EXISTS scalp_pipeline_jobs_session_next_run_idx
    ON scalp_pipeline_jobs(entry_session_profile, next_run_at);

ALTER TABLE scalp_pipeline_job_runs
    ADD COLUMN IF NOT EXISTS entry_session_profile text;

UPDATE scalp_pipeline_job_runs
SET entry_session_profile = CASE
    WHEN LOWER(COALESCE(NULLIF(entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork')
      THEN LOWER(entry_session_profile)
    ELSE 'berlin'
END;

ALTER TABLE scalp_pipeline_job_runs
    ALTER COLUMN entry_session_profile SET DEFAULT 'berlin',
    ALTER COLUMN entry_session_profile SET NOT NULL;

ALTER TABLE scalp_pipeline_job_runs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_job_runs_entry_session_profile_check;

ALTER TABLE scalp_pipeline_job_runs
    ADD CONSTRAINT scalp_pipeline_job_runs_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork'));

CREATE INDEX IF NOT EXISTS scalp_pipeline_job_runs_session_kind_started_idx
    ON scalp_pipeline_job_runs(entry_session_profile, job_kind, started_at DESC);
CREATE INDEX IF NOT EXISTS scalp_pipeline_job_runs_session_finished_idx
    ON scalp_pipeline_job_runs(entry_session_profile, finished_at DESC);

ALTER TABLE scalp_deployment_weekly_metrics
    ADD COLUMN IF NOT EXISTS entry_session_profile text;

UPDATE scalp_deployment_weekly_metrics m
SET entry_session_profile = CASE
    WHEN LOWER(COALESCE(NULLIF(m.entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork')
      THEN LOWER(m.entry_session_profile)
    WHEN LOWER(COALESCE(NULLIF(d.entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork')
      THEN LOWER(d.entry_session_profile)
    WHEN LOWER(COALESCE(NULLIF(d.config_override #>> '{sessions,entrySessionProfile}', ''), '')) IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork')
      THEN LOWER(d.config_override #>> '{sessions,entrySessionProfile}')
    ELSE 'berlin'
END
FROM scalp_deployments d
WHERE d.deployment_id = m.deployment_id;

ALTER TABLE scalp_deployment_weekly_metrics
    ALTER COLUMN entry_session_profile SET DEFAULT 'berlin',
    ALTER COLUMN entry_session_profile SET NOT NULL;

ALTER TABLE scalp_deployment_weekly_metrics
    DROP CONSTRAINT IF EXISTS scalp_deployment_weekly_metrics_entry_session_profile_check;

ALTER TABLE scalp_deployment_weekly_metrics
    ADD CONSTRAINT scalp_deployment_weekly_metrics_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'tokyo_london_overlap', 'newyork'));

CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_session_claim_idx
    ON scalp_deployment_weekly_metrics(entry_session_profile, status, next_run_at, week_start);
CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_session_deployment_week_idx
    ON scalp_deployment_weekly_metrics(entry_session_profile, deployment_id, week_start DESC);
