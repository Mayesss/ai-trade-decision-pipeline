-- Scalp V2 phase-1 research persistence:
-- - cursor/checkpoint state per venue+symbol+session
-- - sparse highlights for remarkable 12-week outcomes

CREATE TABLE IF NOT EXISTS scalp_v2_research_cursor (
  cursor_key TEXT PRIMARY KEY,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney')),
  phase TEXT NOT NULL DEFAULT 'scan' CHECK (phase IN ('scan', 'score', 'validate', 'promote')),
  last_candidate_offset INT NOT NULL DEFAULT 0,
  last_week_start TIMESTAMPTZ,
  progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_v2_research_cursor_symbol_phase_idx
  ON scalp_v2_research_cursor(venue, symbol, entry_session_profile, phase, updated_at DESC);

CREATE TABLE IF NOT EXISTS scalp_v2_research_highlights (
  id BIGSERIAL PRIMARY KEY,
  candidate_id TEXT NOT NULL,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  entry_session_profile TEXT NOT NULL CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney')),
  score NUMERIC(20, 8) NOT NULL DEFAULT 0,
  trades_12w INT NOT NULL DEFAULT 0,
  winning_weeks_12w INT NOT NULL DEFAULT 0,
  consecutive_winning_weeks INT NOT NULL DEFAULT 0,
  robustness_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  dsl_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT,
  remarkable BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_v2_research_highlights_unique_candidate
    UNIQUE (candidate_id, venue, symbol, entry_session_profile)
);

CREATE INDEX IF NOT EXISTS scalp_v2_research_highlights_symbol_score_idx
  ON scalp_v2_research_highlights(venue, symbol, entry_session_profile, score DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_v2_research_highlights_recent_idx
  ON scalp_v2_research_highlights(updated_at DESC);
