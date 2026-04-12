-- Scalp V2 cutover runtime-state tables.
-- Adds v2-native session state and journal persistence to remove runtime writes
-- to legacy scalp_sessions/scalp_journal tables.

CREATE TABLE IF NOT EXISTS scalp_v2_sessions (
  deployment_id TEXT NOT NULL REFERENCES scalp_v2_deployments(deployment_id) ON DELETE CASCADE,
  day_key DATE NOT NULL,
  state_json JSONB NOT NULL,
  last_reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (deployment_id, day_key)
);

CREATE INDEX IF NOT EXISTS scalp_v2_sessions_updated_at_idx
  ON scalp_v2_sessions(updated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_sessions_day_key_idx
  ON scalp_v2_sessions(day_key DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_journal (
  id TEXT PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  deployment_id TEXT REFERENCES scalp_v2_deployments(deployment_id) ON DELETE SET NULL,
  venue TEXT CHECK (venue IS NULL OR venue IN ('bitget', 'capital')),
  symbol TEXT,
  strategy_id TEXT,
  tune_id TEXT,
  entry_session_profile TEXT CHECK (
    entry_session_profile IS NULL
    OR entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'pacific', 'sydney')
  ),
  day_key DATE,
  level TEXT NOT NULL CHECK (level IN ('info', 'warn', 'error')),
  type TEXT NOT NULL CHECK (type IN ('execution', 'state', 'risk', 'error')),
  reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_v2_journal_ts_idx
  ON scalp_v2_journal(ts DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_journal_deployment_idx
  ON scalp_v2_journal(deployment_id, ts DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_journal_symbol_idx
  ON scalp_v2_journal(symbol, ts DESC);
