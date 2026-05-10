CREATE TABLE IF NOT EXISTS scalp_regime_snapshots (
  id BIGSERIAL PRIMARY KEY,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  granularity TEXT NOT NULL DEFAULT 'week' CHECK (granularity = 'week'),
  week_start TIMESTAMPTZ NOT NULL,
  classifier_version TEXT NOT NULL,
  raw_cell_id TEXT NOT NULL,
  cell_id TEXT NOT NULL,
  pending_cell_id TEXT,
  pending_weeks INTEGER NOT NULL DEFAULT 0,
  vol_axis TEXT NOT NULL,
  trend_axis TEXT NOT NULL,
  risk_axis TEXT NOT NULL,
  confidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_coverage_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_regime_snapshots_unique_week
    UNIQUE (venue, symbol, granularity, week_start, classifier_version)
);

CREATE INDEX IF NOT EXISTS scalp_regime_snapshots_lookup_idx
  ON scalp_regime_snapshots(venue, symbol, granularity, week_start DESC, classifier_version);

CREATE INDEX IF NOT EXISTS scalp_regime_snapshots_cell_idx
  ON scalp_regime_snapshots(classifier_version, cell_id, week_start DESC);

CREATE TABLE IF NOT EXISTS scalp_regime_transitions (
  id BIGSERIAL PRIMARY KEY,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  transition_week_start TIMESTAMPTZ NOT NULL,
  classifier_version TEXT NOT NULL,
  from_cell_id TEXT,
  to_cell_id TEXT NOT NULL,
  details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_regime_transitions_unique_week
    UNIQUE (venue, symbol, transition_week_start, classifier_version)
);

CREATE INDEX IF NOT EXISTS scalp_regime_transitions_lookup_idx
  ON scalp_regime_transitions(venue, symbol, transition_week_start DESC, classifier_version);

CREATE TABLE IF NOT EXISTS scalp_regime_walkforward_results (
  id BIGSERIAL PRIMARY KEY,
  candidate_id BIGINT,
  deployment_id TEXT NOT NULL,
  venue TEXT NOT NULL CHECK (venue IN ('bitget', 'capital')),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  classifier_version TEXT NOT NULL,
  window_from TIMESTAMPTZ NOT NULL,
  window_to TIMESTAMPTZ NOT NULL,
  effective_trials BIGINT NOT NULL DEFAULT 1,
  status TEXT NOT NULL,
  manual_approved BOOLEAN NOT NULL DEFAULT FALSE,
  auto_reject_after TIMESTAMPTZ,
  envelope_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  window_results_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_regime_walkforward_results_unique_window
    UNIQUE (deployment_id, classifier_version, window_from, window_to)
);

CREATE INDEX IF NOT EXISTS scalp_regime_walkforward_results_candidate_idx
  ON scalp_regime_walkforward_results(candidate_id, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_regime_walkforward_results_deployment_idx
  ON scalp_regime_walkforward_results(deployment_id, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS scalp_regime_walkforward_results_status_idx
  ON scalp_regime_walkforward_results(status, evaluated_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'scalp_set_updated_at') THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_trigger
      WHERE tgname = 'scalp_regime_snapshots_set_updated_at'
    ) THEN
      CREATE TRIGGER scalp_regime_snapshots_set_updated_at
        BEFORE UPDATE ON scalp_regime_snapshots
        FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();
    END IF;
    IF NOT EXISTS (
      SELECT 1 FROM pg_trigger
      WHERE tgname = 'scalp_regime_walkforward_results_set_updated_at'
    ) THEN
      CREATE TRIGGER scalp_regime_walkforward_results_set_updated_at
        BEFORE UPDATE ON scalp_regime_walkforward_results
        FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();
    END IF;
  END IF;
END
$$;
