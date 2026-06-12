-- Measure-only cross-symbol pattern evidence for scalp v2 session-composer research.
-- This does not affect promotion or execution paths.

CREATE TABLE IF NOT EXISTS scalp_v2_pattern_trade_vectors (
  candidate_id BIGINT,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (
    entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney')
  ),
  window_to_ts BIGINT NOT NULL,
  stage_id TEXT NOT NULL CHECK (stage_id IN ('c')),
  replay_trade_index INTEGER NOT NULL,
  behavior_fingerprint TEXT NOT NULL,
  pattern_key TEXT NOT NULL,
  entry_ts BIGINT NOT NULL,
  exit_ts BIGINT NOT NULL,
  bucket_start_ts BIGINT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
  exit_reason TEXT NOT NULL,
  r_multiple DOUBLE PRECISION NOT NULL,
  fee_r DOUBLE PRECISION,
  gross_r_multiple DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (
    venue,
    symbol,
    strategy_id,
    tune_id,
    entry_session_profile,
    window_to_ts,
    stage_id,
    replay_trade_index
  )
);

CREATE INDEX IF NOT EXISTS scalp_v2_pattern_trade_vectors_pattern_idx
  ON scalp_v2_pattern_trade_vectors(pattern_key, window_to_ts, bucket_start_ts);

CREATE INDEX IF NOT EXISTS scalp_v2_pattern_trade_vectors_candidate_idx
  ON scalp_v2_pattern_trade_vectors(candidate_id)
  WHERE candidate_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS scalp_v2_pattern_edges (
  pattern_key TEXT NOT NULL,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  entry_session_profile TEXT NOT NULL CHECK (
    entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney')
  ),
  behavior_fingerprint TEXT NOT NULL,
  window_to_ts BIGINT NOT NULL,
  bucket_minutes INTEGER NOT NULL,
  population_scope TEXT NOT NULL,
  candidate_count INTEGER NOT NULL DEFAULT 0,
  representative_candidate_count INTEGER NOT NULL DEFAULT 0,
  symbol_count INTEGER NOT NULL DEFAULT 0,
  positive_symbol_count INTEGER NOT NULL DEFAULT 0,
  positive_symbol_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
  top_symbol TEXT,
  top_symbol_net_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  top_symbol_concentration_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
  raw_trades INTEGER NOT NULL DEFAULT 0,
  raw_net_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  raw_mean_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  raw_std_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  raw_lower_bound_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  bucket_count INTEGER NOT NULL DEFAULT 0,
  bucket_net_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  bucket_mean_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  bucket_std_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  bucket_lower_bound_r DOUBLE PRECISION NOT NULL DEFAULT 0,
  leave_one_symbol_out_bucket_lower_bound_r DOUBLE PRECISION,
  score_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (
    pattern_key,
    window_to_ts,
    bucket_minutes,
    population_scope
  )
);

CREATE INDEX IF NOT EXISTS scalp_v2_pattern_edges_rank_idx
  ON scalp_v2_pattern_edges(
    window_to_ts DESC,
    bucket_minutes,
    population_scope,
    bucket_lower_bound_r DESC,
    bucket_net_r DESC
  );
