CREATE TABLE IF NOT EXISTS scalp_candle_history (
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  epic TEXT,
  source TEXT NOT NULL DEFAULT 'capital',
  candles_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (symbol, timeframe)
);

CREATE INDEX IF NOT EXISTS scalp_candle_history_timeframe_symbol_idx
  ON scalp_candle_history(timeframe, symbol);

CREATE INDEX IF NOT EXISTS scalp_candle_history_updated_idx
  ON scalp_candle_history(updated_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'scalp_set_updated_at') THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgname = 'scalp_candle_history_set_updated_at'
    ) THEN
      CREATE TRIGGER scalp_candle_history_set_updated_at
        BEFORE UPDATE ON scalp_candle_history
        FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();
    END IF;
  END IF;
END
$$;
