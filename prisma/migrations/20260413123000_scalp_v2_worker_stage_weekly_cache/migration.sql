-- Stage-week cache for v2 research worker.
-- Enables delta re-evaluation by reusing prior completed weeks and replaying
-- only the newest weekly slice when rolling the completed-week window.

CREATE TABLE IF NOT EXISTS scalp_v2_worker_stage_weekly_stats (
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
  cache_version TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS scalp_v2_worker_stage_weekly_stats_lookup_idx
  ON scalp_v2_worker_stage_weekly_stats(
    venue,
    symbol,
    strategy_id,
    tune_id,
    entry_session_profile,
    stage_id,
    week_start_ts DESC
  );

CREATE INDEX IF NOT EXISTS scalp_v2_worker_stage_weekly_stats_week_idx
  ON scalp_v2_worker_stage_weekly_stats(week_start_ts DESC, week_to_ts DESC);
