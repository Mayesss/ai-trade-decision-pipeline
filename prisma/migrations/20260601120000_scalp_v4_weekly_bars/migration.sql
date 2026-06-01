CREATE TABLE IF NOT EXISTS scalp_v4_weekly_bars (
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  week_start TIMESTAMPTZ NOT NULL,
  open NUMERIC(20, 8) NOT NULL,
  high NUMERIC(20, 8) NOT NULL,
  low NUMERIC(20, 8) NOT NULL,
  close NUMERIC(20, 8) NOT NULL,
  volume NUMERIC(30, 8) NOT NULL DEFAULT 0,
  source TEXT NOT NULL DEFAULT 'candle_history',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (venue, symbol, week_start)
);

CREATE INDEX IF NOT EXISTS scalp_v4_weekly_bars_lookup_idx
  ON scalp_v4_weekly_bars(venue, symbol, week_start DESC);

CREATE INDEX IF NOT EXISTS scalp_v4_weekly_bars_symbol_week_idx
  ON scalp_v4_weekly_bars(symbol, week_start DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'scalp_set_updated_at') THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgname = 'scalp_v4_weekly_bars_set_updated_at'
    ) THEN
      CREATE TRIGGER scalp_v4_weekly_bars_set_updated_at
        BEFORE UPDATE ON scalp_v4_weekly_bars
        FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();
    END IF;
  END IF;
END
$$;
