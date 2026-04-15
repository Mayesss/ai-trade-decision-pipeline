-- Research read-path indexes for faster local/cron bulk evaluation.
-- 1) status+symbol+score: improves discovered candidate chunk loads and symbol scans.
-- 2) status+symbol+worker window: improves previous-week worker result lookups.

CREATE INDEX IF NOT EXISTS scalp_v2_candidates_status_symbol_score_idx
  ON scalp_v2_candidates(status, symbol, score DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_candidates_status_symbol_worker_window_idx
  ON scalp_v2_candidates(
    status,
    symbol,
    ((metadata_json->'worker'->>'windowToTs')::bigint) DESC,
    updated_at DESC
  )
  WHERE metadata_json->'worker'->'stageA' IS NOT NULL;

