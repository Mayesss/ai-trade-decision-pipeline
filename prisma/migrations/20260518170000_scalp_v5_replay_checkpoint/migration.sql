-- v5 replay checkpoint: serialized strategy / position / closed-candle-tail
-- state captured at the end of each full replay. Used by the incremental
-- evaluator to resume from the previous week's end without re-replaying the
-- whole 12-week holdout (see lib/scalp-v5/evaluator.ts + lib/scalp/replay/
-- harness.ts). Cleared by invalidateAllScalpV5Evidence and rewritten by
-- upsertScalpV5DeploymentEvidence so it always stays in sync with the
-- evidence row it pairs with.
--
-- TOAST handles the large payload (~100-200KB per row) out-of-line. Dashboard
-- SELECTs that don't request this column pay zero size cost.

ALTER TABLE scalp_v2_deployments
  ADD COLUMN IF NOT EXISTS v5_replay_checkpoint JSONB;
