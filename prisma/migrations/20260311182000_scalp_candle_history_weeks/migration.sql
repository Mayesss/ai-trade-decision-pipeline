CREATE TABLE IF NOT EXISTS scalp_candle_history_weeks (
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  week_start TIMESTAMPTZ NOT NULL,
  epic TEXT,
  source TEXT NOT NULL DEFAULT 'capital',
  candles_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (symbol, timeframe, week_start)
);

CREATE INDEX IF NOT EXISTS scalp_candle_history_weeks_lookup_idx
  ON scalp_candle_history_weeks(symbol, timeframe, week_start DESC);

CREATE INDEX IF NOT EXISTS scalp_candle_history_weeks_timeframe_symbol_idx
  ON scalp_candle_history_weeks(timeframe, symbol);

CREATE INDEX IF NOT EXISTS scalp_candle_history_weeks_updated_idx
  ON scalp_candle_history_weeks(updated_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'scalp_set_updated_at') THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgname = 'scalp_candle_history_weeks_set_updated_at'
    ) THEN
      CREATE TRIGGER scalp_candle_history_weeks_set_updated_at
        BEFORE UPDATE ON scalp_candle_history_weeks
        FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();
    END IF;
  END IF;
END
$$;

-- Backfill from legacy symbol-level block table when present.
INSERT INTO scalp_candle_history_weeks(symbol, timeframe, week_start, epic, source, candles_json, created_at, updated_at)
SELECT
  ch.symbol,
  ch.timeframe,
  (
    date_trunc('day', to_timestamp((elem->>0)::bigint / 1000.0))
    - (((EXTRACT(DOW FROM date_trunc('day', to_timestamp((elem->>0)::bigint / 1000.0)))::int + 6) % 7) * INTERVAL '1 day')
  )::timestamptz AS week_start,
  ch.epic,
  COALESCE(NULLIF(ch.source, ''), 'capital') AS source,
  jsonb_agg(elem ORDER BY (elem->>0)::bigint) AS candles_json,
  MIN(ch.created_at) AS created_at,
  MAX(ch.updated_at) AS updated_at
FROM scalp_candle_history ch
CROSS JOIN LATERAL jsonb_array_elements(ch.candles_json) elem
WHERE jsonb_typeof(ch.candles_json) = 'array'
GROUP BY ch.symbol, ch.timeframe, week_start, ch.epic, ch.source
ON CONFLICT(symbol, timeframe, week_start)
DO UPDATE SET
  epic = EXCLUDED.epic,
  source = EXCLUDED.source,
  candles_json = EXCLUDED.candles_json,
  updated_at = NOW();
