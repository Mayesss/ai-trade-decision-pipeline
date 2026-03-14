CREATE TABLE IF NOT EXISTS scalp_symbol_market_metadata (
  symbol TEXT PRIMARY KEY,
  epic TEXT,
  source TEXT NOT NULL DEFAULT 'capital',
  asset_category TEXT NOT NULL,
  instrument_type TEXT,
  market_status TEXT,
  pip_size NUMERIC(20, 10) NOT NULL,
  pip_position INT,
  tick_size NUMERIC(20, 10),
  decimal_places_factor INT,
  scaling_factor INT,
  min_deal_size NUMERIC(20, 10),
  size_decimals INT,
  opening_hours_json JSONB,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_symbol_market_metadata_category_updated_idx
  ON scalp_symbol_market_metadata(asset_category, updated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_symbol_market_metadata_updated_idx
  ON scalp_symbol_market_metadata(updated_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'scalp_set_updated_at') THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgname = 'scalp_symbol_market_metadata_set_updated_at'
    ) THEN
      CREATE TRIGGER scalp_symbol_market_metadata_set_updated_at
        BEFORE UPDATE ON scalp_symbol_market_metadata
        FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();
    END IF;
  END IF;
END
$$;
