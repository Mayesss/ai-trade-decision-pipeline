-- Allow native v2 staged replay worker in singleton jobs table.

ALTER TABLE scalp_v2_jobs
  DROP CONSTRAINT IF EXISTS scalp_v2_jobs_job_kind_check;

ALTER TABLE scalp_v2_jobs
  ADD CONSTRAINT scalp_v2_jobs_job_kind_check
  CHECK (job_kind IN ('discover', 'evaluate', 'worker', 'promote', 'execute', 'reconcile'));
