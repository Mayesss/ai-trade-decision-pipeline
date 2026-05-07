-- Exact dashboard ordering indexes for candidate pagination.
-- Keep these expressions aligned with paginateScalpV2Candidates().

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_total_net_r_v2_idx
  ON scalp_v2_candidates (
    (COALESCE(
      (metadata_json->'worker'->'stageC'->>'netR')::double precision,
      (metadata_json->'worker'->'stageB'->>'netR')::double precision,
      (metadata_json->'worker'->'stageA'->>'netR')::double precision,
      -999
    )) DESC,
    score DESC,
    updated_at DESC,
    id DESC
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_candidates_session_total_net_r_v2_idx
  ON scalp_v2_candidates (
    entry_session_profile,
    (COALESCE(
      (metadata_json->'worker'->'stageC'->>'netR')::double precision,
      (metadata_json->'worker'->'stageB'->>'netR')::double precision,
      (metadata_json->'worker'->'stageA'->>'netR')::double precision,
      -999
    )) DESC,
    score DESC,
    updated_at DESC,
    id DESC
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS scalp_v2_deployments_candidate_lookup_idx
  ON scalp_v2_deployments (
    venue,
    symbol,
    strategy_id,
    tune_id,
    entry_session_profile,
    updated_at DESC
  );
