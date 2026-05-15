-- Incremental walk-forward: avoid re-replaying historical windows every Sunday.
-- Walk-forward state per (candidate, classifier_version) is cached as cumulative
-- per-cell stats, plus a watermark of where the next sweep should start. New
-- windows are appended; old windows are never recomputed or dropped.

ALTER TABLE scalp_regime_walkforward_results
  ADD COLUMN IF NOT EXISTS incremental_state_json JSONB,
  ADD COLUMN IF NOT EXISTS next_window_start TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS scalp_regime_walkforward_incremental_idx
  ON scalp_regime_walkforward_results(candidate_id, classifier_version, next_window_start);
