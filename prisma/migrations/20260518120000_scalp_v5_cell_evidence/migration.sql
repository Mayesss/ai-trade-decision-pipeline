-- v5 = v3 12-week recency + per-regime-cell entry gate.
-- New columns hold per-cell evidence and a gate flag on existing
-- scalp_v2_deployments rows. The v5 evaluator updates them out of band
-- via a targeted three-column UPDATE so it doesn't race the main
-- promotion pipeline that writes promotion_gate JSONB.

ALTER TABLE scalp_v2_deployments
  ADD COLUMN IF NOT EXISTS v5_cell_evidence JSONB,
  ADD COLUMN IF NOT EXISTS v5_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS v5_evaluated_at TIMESTAMPTZ;

-- Partial index over enabled v5 deployments, for the live entry gate.
CREATE INDEX IF NOT EXISTS scalp_v2_deployments_v5_enabled_idx
  ON scalp_v2_deployments(v5_enabled, updated_at DESC)
  WHERE v5_enabled = TRUE;

-- Partial index for the evaluator scan (rows that need (re-)evaluation).
CREATE INDEX IF NOT EXISTS scalp_v2_deployments_v5_evaluated_at_idx
  ON scalp_v2_deployments(v5_evaluated_at NULLS FIRST)
  WHERE enabled = TRUE;
