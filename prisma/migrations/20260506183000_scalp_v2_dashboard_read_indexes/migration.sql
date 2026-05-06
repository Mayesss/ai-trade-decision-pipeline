-- Read indexes for the Scalp V2/V3 dashboard.
-- These match the all-session dashboard path: latest runtime session per
-- deployment, recent journal/event reads, and deployment inventory ordering.

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_sessions_deployment_updated_idx
  ON scalp_v2_sessions(deployment_id, updated_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_deployments_dashboard_idx
  ON scalp_v2_deployments(enabled DESC, updated_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_deployments_session_dashboard_idx
  ON scalp_v2_deployments(entry_session_profile, enabled DESC, updated_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_deployments_venue_session_dashboard_idx
  ON scalp_v2_deployments(venue, entry_session_profile, enabled DESC, updated_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_execution_events_venue_session_ts_idx
  ON scalp_v2_execution_events(venue, entry_session_profile, ts DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_journal_venue_session_ts_idx
  ON scalp_v2_journal(venue, entry_session_profile, ts DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_v3_holdout_idx
  ON scalp_v2_candidates(id)
  WHERE metadata_json->'worker'->'holdout' IS NOT NULL
     OR metadata_json->'v3Holdout' IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_v3_temporal_idx
  ON scalp_v2_candidates(id)
  WHERE metadata_json->'v3TemporalFilter'->>'variantKind' IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_v3_temporal_floor_idx
  ON scalp_v2_candidates(id)
  WHERE metadata_json->'v3TemporalFilter'->>'variantKind' IS NOT NULL
    AND COALESCE((metadata_json->'v3Ranking'->'stageA'->>'variantTradeFloorPassed')::boolean, false);

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_v3_single_axis_idx
  ON scalp_v2_candidates(id)
  WHERE metadata_json->'v3TemporalFilter'->>'variantKind' IS NOT NULL
    AND metadata_json->'v3TemporalFilter'->>'variantKind' <> 'slot_weekday'
    AND metadata_json->'v3Ranking'->'stageA' IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_coverage_gap_symbol_idx
  ON scalp_v2_candidates(symbol)
  WHERE (metadata_json->'worker'->'stageC'->>'passed') IS NULL
    AND status <> 'rejected';

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_total_net_r_idx
  ON scalp_v2_candidates (
    (COALESCE(
      (metadata_json->'worker'->'stageC'->>'netR')::double precision,
      (metadata_json->'worker'->'stageB'->>'netR')::double precision,
      (metadata_json->'worker'->'stageA'->>'netR')::double precision,
      score::double precision,
      -999
    )) DESC,
    score DESC,
    updated_at DESC
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_session_total_net_r_idx
  ON scalp_v2_candidates (
    entry_session_profile,
    (COALESCE(
      (metadata_json->'worker'->'stageC'->>'netR')::double precision,
      (metadata_json->'worker'->'stageB'->>'netR')::double precision,
      (metadata_json->'worker'->'stageA'->>'netR')::double precision,
      score::double precision,
      -999
    )) DESC,
    score DESC,
    updated_at DESC
  );
