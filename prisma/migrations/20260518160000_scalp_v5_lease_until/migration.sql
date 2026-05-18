-- v5 work lease: a TTL stamp set when a worker claims a deployment for
-- evaluation, cleared when the worker successfully writes evidence. Lets
-- the hourly cron coexist with multiple local bulk processes — each worker
-- atomically claims a disjoint set of rows via FOR UPDATE SKIP LOCKED.
-- See lib/scalp-v5/pg.ts:loadScalpV5DeploymentsForEvaluation.

ALTER TABLE scalp_v2_deployments
  ADD COLUMN IF NOT EXISTS v5_lease_until TIMESTAMPTZ;

-- Partial index makes "find unleased rows" cheap and skips the index for
-- rows that don't have an active lease (the vast majority).
CREATE INDEX IF NOT EXISTS scalp_v2_deployments_v5_lease_until_idx
  ON scalp_v2_deployments(v5_lease_until)
  WHERE v5_lease_until IS NOT NULL;
