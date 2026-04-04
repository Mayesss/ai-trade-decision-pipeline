-- Add Pacific as a supported scalp entry session profile.
-- Pacific covers the NY-Sydney gap: 10:00–14:00 America/Los_Angeles (~18:00–22:00 UTC).

-- v1 tables (ALTER existing constraints)

ALTER TABLE scalp_deployments
    DROP CONSTRAINT IF EXISTS scalp_deployments_entry_session_profile_check;

ALTER TABLE scalp_deployments
    ADD CONSTRAINT scalp_deployments_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_pipeline_jobs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_jobs_entry_session_profile_check;

ALTER TABLE scalp_pipeline_jobs
    ADD CONSTRAINT scalp_pipeline_jobs_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_pipeline_job_runs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_job_runs_entry_session_profile_check;

ALTER TABLE scalp_pipeline_job_runs
    ADD CONSTRAINT scalp_pipeline_job_runs_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_deployment_weekly_metrics
    DROP CONSTRAINT IF EXISTS scalp_deployment_weekly_metrics_entry_session_profile_check;

ALTER TABLE scalp_deployment_weekly_metrics
    ADD CONSTRAINT scalp_deployment_weekly_metrics_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork', 'pacific', 'sydney'));

-- adaptive tables

ALTER TABLE scalp_adaptive_selector_snapshots
    DROP CONSTRAINT IF EXISTS scalp_adaptive_selector_snapshots_entry_session_profile_check;

ALTER TABLE scalp_adaptive_selector_snapshots
    ADD CONSTRAINT scalp_adaptive_selector_snapshots_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_adaptive_selector_decisions
    DROP CONSTRAINT IF EXISTS scalp_adaptive_selector_decisions_entry_session_profile_check;

ALTER TABLE scalp_adaptive_selector_decisions
    ADD CONSTRAINT scalp_adaptive_selector_decisions_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));

-- v2 tables (inline CHECK constraints need to be dropped by auto-generated name and re-added)

ALTER TABLE scalp_v2_candidates
    DROP CONSTRAINT IF EXISTS scalp_v2_candidates_entry_session_profile_check;

ALTER TABLE scalp_v2_candidates
    ADD CONSTRAINT scalp_v2_candidates_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_v2_deployments
    DROP CONSTRAINT IF EXISTS scalp_v2_deployments_entry_session_profile_check;

ALTER TABLE scalp_v2_deployments
    ADD CONSTRAINT scalp_v2_deployments_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_v2_execution_events
    DROP CONSTRAINT IF EXISTS scalp_v2_execution_events_entry_session_profile_check;

ALTER TABLE scalp_v2_execution_events
    ADD CONSTRAINT scalp_v2_execution_events_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_v2_ledger
    DROP CONSTRAINT IF EXISTS scalp_v2_ledger_entry_session_profile_check;

ALTER TABLE scalp_v2_ledger
    ADD CONSTRAINT scalp_v2_ledger_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_v2_metrics_daily
    DROP CONSTRAINT IF EXISTS scalp_v2_metrics_daily_entry_session_profile_check;

ALTER TABLE scalp_v2_metrics_daily
    ADD CONSTRAINT scalp_v2_metrics_daily_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_v2_research_cursor
    DROP CONSTRAINT IF EXISTS scalp_v2_research_cursor_entry_session_profile_check;

ALTER TABLE scalp_v2_research_cursor
    ADD CONSTRAINT scalp_v2_research_cursor_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));

ALTER TABLE scalp_v2_research_highlights
    DROP CONSTRAINT IF EXISTS scalp_v2_research_highlights_entry_session_profile_check;

ALTER TABLE scalp_v2_research_highlights
    ADD CONSTRAINT scalp_v2_research_highlights_entry_session_profile_check
    CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney'));
