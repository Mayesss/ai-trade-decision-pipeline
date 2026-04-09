-- Expression index for the dashboard sort order on scalp_v2_candidates.
-- The query sorts by (metadata_json->'worker'->'stageC'->>'netR')::float DESC, score DESC.
-- Without this index, every dashboard load does a full-table JSONB extraction + sort.

CREATE INDEX IF NOT EXISTS scalp_v2_candidates_stage_c_net_r_idx
  ON scalp_v2_candidates (
    (COALESCE((metadata_json->'worker'->'stageC'->>'netR')::double precision, -999)) DESC,
    score DESC
  );

-- Composite index for session-filtered queries (the common dashboard path).
CREATE INDEX IF NOT EXISTS scalp_v2_candidates_session_stage_c_idx
  ON scalp_v2_candidates (
    entry_session_profile,
    (COALESCE((metadata_json->'worker'->'stageC'->>'netR')::double precision, -999)) DESC,
    score DESC
  );
