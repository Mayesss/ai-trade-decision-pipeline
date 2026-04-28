-- Candidate-level research leases for parallel workers.
-- Workers claim discovered candidates with short leases instead of relying on
-- one global research singleton lock.

ALTER TABLE scalp_v2_candidates
  ADD COLUMN IF NOT EXISTS research_locked_by TEXT,
  ADD COLUMN IF NOT EXISTS research_claimed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS research_lease_until TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS research_attempts INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS scalp_v2_candidates_research_lease_idx
  ON scalp_v2_candidates(status, research_lease_until, score DESC, updated_at DESC)
  WHERE status = 'discovered';

CREATE INDEX IF NOT EXISTS scalp_v2_candidates_research_lock_owner_idx
  ON scalp_v2_candidates(research_locked_by, research_lease_until)
  WHERE research_locked_by IS NOT NULL;
