-- Re-create the weekly cache table for incremental research replays.
-- Each row stores full per-week metrics for one candidate+stage+week,
-- so subsequent weeks only need to replay the newest week slice.

CREATE TABLE IF NOT EXISTS scalp_v2_worker_stage_weekly_cache (
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (
    entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney')
  ),
  stage_id TEXT NOT NULL CHECK (stage_id IN ('a', 'b', 'c')),
  week_start_ts BIGINT NOT NULL,
  week_to_ts BIGINT NOT NULL,
  metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (
    venue,
    symbol,
    strategy_id,
    tune_id,
    entry_session_profile,
    stage_id,
    week_start_ts
  )
);

CREATE INDEX IF NOT EXISTS scalp_v2_worker_stage_weekly_cache_week_idx
  ON scalp_v2_worker_stage_weekly_cache(week_start_ts DESC);
