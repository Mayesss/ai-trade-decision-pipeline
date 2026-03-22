-- Add Sydney as a supported scalp entry session profile.

ALTER TABLE scalp_deployments
    DROP CONSTRAINT IF EXISTS scalp_deployments_entry_session_profile_check;

ALTER TABLE scalp_deployments
    ADD CONSTRAINT scalp_deployments_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork', 'sydney'));

ALTER TABLE scalp_pipeline_jobs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_jobs_entry_session_profile_check;

ALTER TABLE scalp_pipeline_jobs
    ADD CONSTRAINT scalp_pipeline_jobs_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork', 'sydney'));

ALTER TABLE scalp_pipeline_job_runs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_job_runs_entry_session_profile_check;

ALTER TABLE scalp_pipeline_job_runs
    ADD CONSTRAINT scalp_pipeline_job_runs_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork', 'sydney'));

ALTER TABLE scalp_deployment_weekly_metrics
    DROP CONSTRAINT IF EXISTS scalp_deployment_weekly_metrics_entry_session_profile_check;

ALTER TABLE scalp_deployment_weekly_metrics
    ADD CONSTRAINT scalp_deployment_weekly_metrics_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork', 'sydney'));
