-- Scalp V2 strangler foundation (cost-first, broker-truth).
-- Phase-1 schema only. Runtime uses direct PG client, no Prisma models.

CREATE TABLE IF NOT EXISTS scalp_v2_runtime_config (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
  config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (singleton)
);

CREATE TABLE IF NOT EXISTS scalp_v2_candidates (
  id BIGSERIAL PRIMARY KEY,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney')),
  score NUMERIC(20, 8) NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK (status IN ('discovered', 'evaluated', 'promoted', 'rejected', 'shadow')),
  reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_v2_candidates_identity_unique
    UNIQUE (venue, symbol, strategy_id, tune_id, entry_session_profile)
);

CREATE INDEX IF NOT EXISTS scalp_v2_candidates_status_score_idx
  ON scalp_v2_candidates(status, score DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_candidates_symbol_score_idx
  ON scalp_v2_candidates(venue, symbol, score DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_deployments (
  deployment_id TEXT PRIMARY KEY,
  candidate_id BIGINT REFERENCES scalp_v2_candidates(id) ON DELETE SET NULL,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney')),
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  live_mode TEXT NOT NULL DEFAULT 'shadow' CHECK (live_mode IN ('shadow', 'live')),
  promotion_gate JSONB NOT NULL DEFAULT '{}'::jsonb,
  risk_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_promoted_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS scalp_v2_deployments_candidate_unique
  ON scalp_v2_deployments(candidate_id)
  WHERE candidate_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS scalp_v2_deployments_enabled_idx
  ON scalp_v2_deployments(enabled, live_mode, updated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_deployments_symbol_idx
  ON scalp_v2_deployments(venue, symbol, entry_session_profile, updated_at DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_execution_events (
  id TEXT PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  deployment_id TEXT NOT NULL REFERENCES scalp_v2_deployments(deployment_id) ON DELETE CASCADE,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney')),
  event_type TEXT NOT NULL CHECK (event_type IN ('order_submitted', 'order_rejected', 'position_snapshot', 'fill', 'stop_loss', 'liquidation', 'manual_close', 'reconcile_close')),
  broker_ref TEXT,
  reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  source_of_truth TEXT NOT NULL CHECK (source_of_truth IN ('broker', 'reconciler', 'system', 'legacy_v1_import')),
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_v2_execution_events_ts_idx
  ON scalp_v2_execution_events(ts DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_execution_events_deployment_ts_idx
  ON scalp_v2_execution_events(deployment_id, ts DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_execution_events_symbol_ts_idx
  ON scalp_v2_execution_events(venue, symbol, ts DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_orders (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deployment_id TEXT NOT NULL REFERENCES scalp_v2_deployments(deployment_id) ON DELETE CASCADE,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  broker_order_id TEXT,
  client_order_id TEXT,
  side TEXT,
  status TEXT NOT NULL DEFAULT 'submitted',
  size NUMERIC(20, 8),
  price NUMERIC(20, 8),
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_v2_orders_deployment_ts_idx
  ON scalp_v2_orders(deployment_id, ts DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_orders_symbol_ts_idx
  ON scalp_v2_orders(venue, symbol, ts DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_positions (
  deployment_id TEXT PRIMARY KEY REFERENCES scalp_v2_deployments(deployment_id) ON DELETE CASCADE,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  side TEXT CHECK (side IS NULL OR side IN ('long', 'short')),
  entry_price NUMERIC(20, 8),
  leverage NUMERIC(20, 8),
  size NUMERIC(20, 8),
  deal_id TEXT,
  deal_reference TEXT,
  broker_snapshot_at TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('open', 'flat')),
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_v2_positions_status_updated_idx
  ON scalp_v2_positions(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_positions_symbol_status_idx
  ON scalp_v2_positions(venue, symbol, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_ledger (
  id TEXT PRIMARY KEY,
  ts_exit TIMESTAMPTZ NOT NULL,
  deployment_id TEXT NOT NULL,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney')),
  entry_ref TEXT,
  exit_ref TEXT,
  close_type TEXT NOT NULL CHECK (close_type IN ('fill', 'stop_loss', 'liquidation', 'manual_close', 'reconcile_close')),
  r_multiple NUMERIC(20, 8) NOT NULL,
  pnl_usd NUMERIC(20, 8),
  source_of_truth TEXT NOT NULL CHECK (source_of_truth IN ('broker', 'reconciler', 'system', 'legacy_v1_import')),
  reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_v2_ledger_exit_idx
  ON scalp_v2_ledger(ts_exit DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_ledger_deployment_exit_idx
  ON scalp_v2_ledger(deployment_id, ts_exit DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_ledger_symbol_exit_idx
  ON scalp_v2_ledger(venue, symbol, ts_exit DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_metrics_daily (
  day_key DATE NOT NULL,
  deployment_id TEXT NOT NULL,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney')),
  trades INT NOT NULL DEFAULT 0,
  wins INT NOT NULL DEFAULT 0,
  losses INT NOT NULL DEFAULT 0,
  net_r NUMERIC(20, 8) NOT NULL DEFAULT 0,
  net_pnl_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (day_key, deployment_id)
);

CREATE INDEX IF NOT EXISTS scalp_v2_metrics_daily_symbol_day_idx
  ON scalp_v2_metrics_daily(venue, symbol, day_key DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_jobs (
  id BIGSERIAL PRIMARY KEY,
  job_kind TEXT NOT NULL CHECK (job_kind IN ('discover', 'evaluate', 'promote', 'execute', 'reconcile')),
  dedupe_key TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
  attempts INT NOT NULL DEFAULT 0,
  next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_by TEXT,
  locked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_v2_jobs_unique_kind_dedupe UNIQUE (job_kind, dedupe_key)
);

CREATE INDEX IF NOT EXISTS scalp_v2_jobs_claim_idx
  ON scalp_v2_jobs(status, next_run_at, updated_at);
