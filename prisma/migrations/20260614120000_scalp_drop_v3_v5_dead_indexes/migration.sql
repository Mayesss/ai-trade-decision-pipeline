-- Strip-down cleanup: drop indexes that backed the removed v3 (holdout /
-- temporal-variant) and v5 (cell-evidence / lease) query paths. Those code
-- paths are gone — the composer now runs one strategy
-- (session_structure_composer_v1) with a stage-A/B/C backtest + pooled
-- lowerBoundR significance promote. The only live consumer of the v3 candidate
-- indexes was the diagnostics block in loadScalpComposerSummary, which has been
-- removed in the same change, so these indexes now back zero queries and only
-- add write/maintenance overhead.
--
-- DROP INDEX takes a brief ACCESS EXCLUSIVE lock; harmless here (no live
-- executions). In a hot production DB you could instead run
-- `DROP INDEX CONCURRENTLY` manually outside this migration's transaction.

-- v3 candidate indexes (holdout / temporal variants) — removed in Step 4.
DROP INDEX IF EXISTS scalp_v2_candidates_v3_holdout_idx;
DROP INDEX IF EXISTS scalp_v2_candidates_v3_temporal_idx;
DROP INDEX IF EXISTS scalp_v2_candidates_v3_temporal_floor_idx;
DROP INDEX IF EXISTS scalp_v2_candidates_v3_single_axis_idx;

-- Partial expression index for the deleted v5 coverage dashboard
-- (/api/scalp/research/coverage). The NOT EXISTS subquery it accelerated no
-- longer exists.
DROP INDEX IF EXISTS scalp_v2_candidates_scope_removed_idx;

-- v5 deployment indexes (cell-evidence enablement + work leases) — the entire
-- v5 layer (lib/scalp/research) was removed.
DROP INDEX IF EXISTS scalp_v2_deployments_v5_enabled_idx;
DROP INDEX IF EXISTS scalp_v2_deployments_v5_eval_scope_idx;
DROP INDEX IF EXISTS scalp_v2_deployments_v5_evaluated_at_idx;
DROP INDEX IF EXISTS scalp_v2_deployments_v5_lease_until_idx;
