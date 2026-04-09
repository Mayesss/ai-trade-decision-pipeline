-- Reset candidates that have a worker key but no actual stage results.
-- These got stuck in evaluated/rejected state with empty worker data,
-- causing the research cache to skip them and the promote job to ignore them.
-- Clearing the worker key lets the research job re-backtest them next cycle.

UPDATE scalp_v2_candidates
SET
  metadata_json = metadata_json - 'worker',
  status = 'pending',
  updated_at = NOW()
WHERE (metadata_json->'worker') IS NOT NULL
  AND (metadata_json->'worker'->'stageA'->>'passed') IS NULL
  AND (metadata_json->'worker'->'stageC'->>'passed') IS NULL
  AND symbol NOT IN (
    SELECT DISTINCT symbol FROM scalp_v2_deployments d
    WHERE d.symbol = scalp_v2_candidates.symbol
      AND d.strategy_id = scalp_v2_candidates.strategy_id
      AND d.tune_id = scalp_v2_candidates.tune_id
      AND d.entry_session_profile = scalp_v2_candidates.entry_session_profile
  );
