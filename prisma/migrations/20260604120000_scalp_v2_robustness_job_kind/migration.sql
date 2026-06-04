-- Add 'robustness' to the scalp_v2_jobs job_kind constraint for the
-- post-bulk finalist robustness singleton.

ALTER TABLE scalp_v2_jobs
  DROP CONSTRAINT IF EXISTS scalp_v2_jobs_job_kind_check;

ALTER TABLE scalp_v2_jobs
  ADD CONSTRAINT scalp_v2_jobs_job_kind_check
  CHECK (job_kind IN ('discover', 'evaluate', 'worker', 'research', 'robustness', 'promote', 'execute', 'reconcile'));
