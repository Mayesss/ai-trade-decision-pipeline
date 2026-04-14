-- Tracks warm-up state per weekly window so research can skip
-- candidate generation after the first run of each week.
CREATE TABLE IF NOT EXISTS scalp_v2_research_warm_up (
  window_to_ts BIGINT PRIMARY KEY,
  scope_hash TEXT NOT NULL,
  candidate_count INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
