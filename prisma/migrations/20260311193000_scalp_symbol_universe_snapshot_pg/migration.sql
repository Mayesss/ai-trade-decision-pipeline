CREATE TABLE IF NOT EXISTS scalp_symbol_universe_snapshots (
  snapshot_key TEXT PRIMARY KEY,
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_symbol_universe_snapshots_updated_idx
  ON scalp_symbol_universe_snapshots(updated_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'scalp_set_updated_at') THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgname = 'scalp_symbol_universe_snapshots_set_updated_at'
    ) THEN
      CREATE TRIGGER scalp_symbol_universe_snapshots_set_updated_at
        BEFORE UPDATE ON scalp_symbol_universe_snapshots
        FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();
    END IF;
  END IF;
END
$$;
