CREATE TABLE IF NOT EXISTS scalp_candle_load_runs (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  trigger TEXT,
  ok BOOLEAN NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  scope_count INTEGER NOT NULL DEFAULT 0,
  processed INTEGER NOT NULL DEFAULT 0,
  succeeded INTEGER NOT NULL DEFAULT 0,
  failed INTEGER NOT NULL DEFAULT 0,
  params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  errors_json JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS scalp_candle_load_runs_started_at_idx
  ON scalp_candle_load_runs(started_at DESC);

CREATE INDEX IF NOT EXISTS scalp_candle_load_runs_source_idx
  ON scalp_candle_load_runs(source);

CREATE INDEX IF NOT EXISTS scalp_candle_load_runs_ok_idx
  ON scalp_candle_load_runs(ok);
