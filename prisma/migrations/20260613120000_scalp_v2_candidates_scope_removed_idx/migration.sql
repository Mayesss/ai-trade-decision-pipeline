-- Partial expression index supporting the v5 coverage dashboard
-- (/api/scalp/research/coverage). That endpoint excludes deployments whose
-- candidate was scope-removed via
-- `metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'`
-- in a NOT EXISTS subquery. Without this index Postgres detoasts the (large)
-- metadata_json for every linked candidate (~17.6k), taking ~125s and timing
-- the endpoint out (HTTP 500). The partial index contains only scope-removed
-- candidates, so the NOT EXISTS becomes an index probe (~1.3s total).
--
-- NOTE: production was indexed live with CREATE INDEX CONCURRENTLY (the
-- candidate table is hot during research bulks). This migration uses a plain
-- IF NOT EXISTS create so it is a no-op there and builds on other environments.
CREATE INDEX IF NOT EXISTS scalp_v2_candidates_scope_removed_idx
  ON scalp_v2_candidates (id)
  WHERE (metadata_json->'scopeRemoval'->>'reason') = 'bitget_symbol_removed_no_candles';
