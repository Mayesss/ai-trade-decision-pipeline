-- Remove deprecated overlap session profile from runtime constraints/data.

-- Deployments: normalize any non-supported session profile to berlin.
UPDATE scalp_deployments
SET entry_session_profile = CASE
    WHEN LOWER(COALESCE(NULLIF(entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'newyork')
      THEN LOWER(entry_session_profile)
    ELSE 'berlin'
END;

-- Keep config_override session profile aligned with supported values.
UPDATE scalp_deployments
SET config_override = jsonb_set(
    COALESCE(config_override, '{}'::jsonb),
    '{sessions,entrySessionProfile}',
    to_jsonb('berlin'::text),
    true
)
WHERE LOWER(COALESCE(NULLIF(config_override #>> '{sessions,entrySessionProfile}', ''), '')) NOT IN ('', 'berlin', 'tokyo', 'newyork');

ALTER TABLE scalp_deployments
    DROP CONSTRAINT IF EXISTS scalp_deployments_entry_session_profile_check;

ALTER TABLE scalp_deployments
    ADD CONSTRAINT scalp_deployments_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork'));

-- Pipeline jobs: collapse rows that normalize to the same (job_kind, session) pair.
WITH normalized AS (
    SELECT
        ctid,
        job_kind,
        CASE
            WHEN LOWER(COALESCE(NULLIF(entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'newyork')
              THEN LOWER(entry_session_profile)
            ELSE 'berlin'
        END AS normalized_profile,
        updated_at
    FROM scalp_pipeline_jobs
),
ranked AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY job_kind, normalized_profile
            ORDER BY updated_at DESC NULLS LAST, ctid DESC
        ) AS rn
    FROM normalized
)
DELETE FROM scalp_pipeline_jobs j
USING ranked r
WHERE j.ctid = r.ctid
  AND r.rn > 1;

UPDATE scalp_pipeline_jobs
SET entry_session_profile = CASE
    WHEN LOWER(COALESCE(NULLIF(entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'newyork')
      THEN LOWER(entry_session_profile)
    ELSE 'berlin'
END;

ALTER TABLE scalp_pipeline_jobs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_jobs_entry_session_profile_check;

ALTER TABLE scalp_pipeline_jobs
    ADD CONSTRAINT scalp_pipeline_jobs_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork'));

-- Pipeline job runs: normalize any unsupported profile to berlin.
UPDATE scalp_pipeline_job_runs
SET entry_session_profile = CASE
    WHEN LOWER(COALESCE(NULLIF(entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'newyork')
      THEN LOWER(entry_session_profile)
    ELSE 'berlin'
END;

ALTER TABLE scalp_pipeline_job_runs
    DROP CONSTRAINT IF EXISTS scalp_pipeline_job_runs_entry_session_profile_check;

ALTER TABLE scalp_pipeline_job_runs
    ADD CONSTRAINT scalp_pipeline_job_runs_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork'));

-- Weekly metrics: normalize any unsupported profile to berlin.
UPDATE scalp_deployment_weekly_metrics
SET entry_session_profile = CASE
    WHEN LOWER(COALESCE(NULLIF(entry_session_profile, ''), '')) IN ('berlin', 'tokyo', 'newyork')
      THEN LOWER(entry_session_profile)
    ELSE 'berlin'
END;

ALTER TABLE scalp_deployment_weekly_metrics
    DROP CONSTRAINT IF EXISTS scalp_deployment_weekly_metrics_entry_session_profile_check;

ALTER TABLE scalp_deployment_weekly_metrics
    ADD CONSTRAINT scalp_deployment_weekly_metrics_entry_session_profile_check
    CHECK (entry_session_profile IN ('berlin', 'tokyo', 'newyork'));
