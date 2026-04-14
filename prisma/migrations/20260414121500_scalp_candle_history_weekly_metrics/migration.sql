ALTER TABLE scalp_candle_history_weeks
  ADD COLUMN IF NOT EXISTS candle_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS first_ts_ms BIGINT,
  ADD COLUMN IF NOT EXISTS last_ts_ms BIGINT;

CREATE OR REPLACE FUNCTION scalp_candle_history_weeks_compute_metrics()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.candle_count := COALESCE(jsonb_array_length(NEW.candles_json), 0);

  NEW.first_ts_ms := CASE
    WHEN NEW.candle_count > 0
         AND (NEW.candles_json -> 0 ->> 0) ~ '^[0-9]+$'
    THEN (NEW.candles_json -> 0 ->> 0)::bigint
    ELSE NULL
  END;

  NEW.last_ts_ms := CASE
    WHEN NEW.candle_count > 0
         AND (NEW.candles_json -> (NEW.candle_count - 1) ->> 0) ~ '^[0-9]+$'
    THEN (NEW.candles_json -> (NEW.candle_count - 1) ->> 0)::bigint
    ELSE NULL
  END;

  RETURN NEW;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'scalp_candle_history_weeks_compute_metrics_trg'
      AND tgrelid = 'scalp_candle_history_weeks'::regclass
  ) THEN
    CREATE TRIGGER scalp_candle_history_weeks_compute_metrics_trg
      BEFORE INSERT OR UPDATE OF candles_json ON scalp_candle_history_weeks
      FOR EACH ROW EXECUTE FUNCTION scalp_candle_history_weeks_compute_metrics();
  END IF;
END
$$;

DO $$
DECLARE
  has_updated_at_trigger boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'scalp_candle_history_weeks_set_updated_at'
      AND tgrelid = 'scalp_candle_history_weeks'::regclass
  )
  INTO has_updated_at_trigger;

  IF has_updated_at_trigger THEN
    EXECUTE 'ALTER TABLE scalp_candle_history_weeks DISABLE TRIGGER scalp_candle_history_weeks_set_updated_at';
  END IF;

  UPDATE scalp_candle_history_weeks
  SET
    candle_count = COALESCE(jsonb_array_length(candles_json), 0),
    first_ts_ms = CASE
      WHEN jsonb_array_length(candles_json) > 0
           AND (candles_json -> 0 ->> 0) ~ '^[0-9]+$'
      THEN (candles_json -> 0 ->> 0)::bigint
      ELSE NULL
    END,
    last_ts_ms = CASE
      WHEN jsonb_array_length(candles_json) > 0
           AND (candles_json -> (jsonb_array_length(candles_json) - 1) ->> 0) ~ '^[0-9]+$'
      THEN (candles_json -> (jsonb_array_length(candles_json) - 1) ->> 0)::bigint
      ELSE NULL
    END;

  IF has_updated_at_trigger THEN
    EXECUTE 'ALTER TABLE scalp_candle_history_weeks ENABLE TRIGGER scalp_candle_history_weeks_set_updated_at';
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS scalp_candle_history_weeks_stats_lookup_idx
  ON scalp_candle_history_weeks(timeframe, symbol)
  INCLUDE (epic, updated_at, candle_count, first_ts_ms, last_ts_ms);
