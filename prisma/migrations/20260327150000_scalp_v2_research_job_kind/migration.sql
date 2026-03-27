-- Add 'research' to the scalp_v2_jobs job_kind constraint.
-- The research job merges evaluate + worker into a single in-memory pipeline
-- that only persists candidates passing the backtest stage gate.

ALTER TABLE scalp_v2_jobs
  DROP CONSTRAINT IF EXISTS scalp_v2_jobs_job_kind_check;

ALTER TABLE scalp_v2_jobs
  ADD CONSTRAINT scalp_v2_jobs_job_kind_check
  CHECK (job_kind IN ('discover', 'evaluate', 'worker', 'research', 'promote', 'execute', 'reconcile'));
