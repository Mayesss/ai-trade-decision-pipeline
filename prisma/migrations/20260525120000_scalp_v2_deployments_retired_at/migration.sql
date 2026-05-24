-- Permanent retirement marker: retired_at is set by trim-tail and
-- cull-bottom when a deployment is removed from the active pool. Refill
-- pool queries (stagec / mutation / exploration) exclude any
-- (venue, symbol, strategy_id, tune_id, entry_session_profile) tuple
-- whose row has a non-null retired_at — a hard ban that prevents v2
-- research from regenerating the same combo and burning compute proving
-- the strategy doesn't work twice.
--
-- Existing tombstones (candidate_id IS NULL today, pre-migration) are
-- NOT backfilled with retired_at. They could come back through refill
-- ONCE under the new logic; subsequent retirements will write retired_at
-- and the ban will stick from that point forward. Backfilling can be
-- done manually if a strict cut-over from history is required.

ALTER TABLE scalp_v2_deployments
  ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;

-- Partial index over only the retired rows — keeps lookup cheap for the
-- exclusion subquery without indexing the much larger active-row set.
CREATE INDEX IF NOT EXISTS scalp_v2_deployments_retired_lookup_idx
  ON scalp_v2_deployments (venue, symbol, strategy_id, tune_id, entry_session_profile)
  WHERE retired_at IS NOT NULL;
