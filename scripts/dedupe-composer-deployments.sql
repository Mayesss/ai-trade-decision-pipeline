-- ─────────────────────────────────────────────────────────────────────────────
-- One-shot cleanup: retire behavior-equivalent composer deployments.
--
-- Context: prior to the dedup fix in lib/scalp-v2/research.ts, the
-- model_guided_composer candidate generator multiplied each base arm by 4
-- exit-rule profiles × N regime-gate variants. The composer strategy
-- itself doesn't consume those overrides (its exit logic is internal), so
-- the resulting deployments produce byte-identical replays — wasted DB,
-- wasted CPU, wasted promotion slots. The fix prevents NEW duplicates;
-- this script retires the existing ones.
--
-- Run order:
--   1. STEP A (preview): see how many groups + rows would be retired.
--   2. STEP B (commit): actually retire. Reversible by re-running
--      promotion (which would re-create the rows if needed — but the new
--      dedup logic prevents that).
--
-- Best-of-group selection within each behavior-equivalent group:
--   (1) v5_enabled = TRUE wins over FALSE
--   (2) most-recently-evaluated wins
--   (3) most-recently-promoted wins (tiebreaker)
--   (4) most-recently-updated wins (final tiebreaker)
--
-- Retire action (STEP B):
--   - candidate_id  = NULL  → removes from the v5 evaluation queue
--   - enabled       = FALSE → stops new live entries on this row
--   - v5_evaluated_at = NULL → drops from dashboard's "evaluated" counts
--   - v5_lease_until  = NULL → release any in-flight lease
--   Existing open positions on retired rows still reconcile normally
--   through the execute cron until they close naturally.
--
-- Reversal: set candidate_id back to its previous value (kept in the
-- preview output's history) and the row re-enters the eval pool. This is
-- a "safe disable", not a delete.
-- ─────────────────────────────────────────────────────────────────────────────


-- ═════════════════════════════════════════════════════════════════════════════
-- STEP A — preview: list duplicate groups, what would be kept / retired.
-- Run this FIRST. Verify the counts look right. Then run STEP B.
-- ═════════════════════════════════════════════════════════════════════════════
WITH arms AS (
  SELECT
    deployment_id,
    venue,
    symbol,
    entry_session_profile,
    enabled,
    COALESCE(v5_enabled, FALSE) AS v5_enabled,
    v5_evaluated_at,
    last_promoted_at,
    updated_at,
    tune_id,
    -- Extract the arm key: `mdl_<baseArm>_<tfVariant>` (e.g. `mdl_basis_m5_m1`).
    -- The tfVariant always matches `m<digits>_m<digits>` per
    -- COMPOSER_TIMEFRAME_VARIANTS (m15_m3 / m5_m1 / m5_m3). Anything after
    -- that — exit/entry/risk/sm/regime short codes and the 10-hex digest
    -- token — gets stripped. Variants that share the same arm_key are
    -- behavior-equivalent for the composer.
    regexp_replace(tune_id, '^(mdl_.+?_m[0-9]+_m[0-9]+)(_.*)?$', '\1') AS arm_key
  FROM scalp_v2_deployments
  WHERE strategy_id = 'model_guided_composer_v2'
    AND candidate_id IS NOT NULL
),
groups AS (
  SELECT
    venue, symbol, entry_session_profile, arm_key,
    COUNT(*) AS dupe_count,
    SUM(CASE WHEN enabled THEN 1 ELSE 0 END) AS enabled_count,
    array_agg(deployment_id ORDER BY
      v5_enabled DESC,
      v5_evaluated_at DESC NULLS LAST,
      last_promoted_at DESC NULLS LAST,
      updated_at DESC
    ) AS deployment_ids
  FROM arms
  GROUP BY 1,2,3,4
  HAVING COUNT(*) > 1
)
SELECT
  venue, symbol, entry_session_profile, arm_key,
  dupe_count,
  enabled_count,
  deployment_ids[1]                AS keep_deployment,
  array_length(deployment_ids, 1) - 1 AS retire_count,
  deployment_ids[2:]               AS retire_deployments
FROM groups
ORDER BY dupe_count DESC, enabled_count DESC, venue, symbol, entry_session_profile, arm_key
LIMIT 50;

-- Aggregate summary (run separately):
--   WITH arms AS (
--     SELECT
--       regexp_replace(tune_id, '^(mdl_.+?_m[0-9]+_m[0-9]+)(_.*)?$', '\1') AS arm_key,
--       venue, symbol, entry_session_profile, deployment_id, enabled
--     FROM scalp_v2_deployments
--     WHERE strategy_id = 'model_guided_composer_v2' AND candidate_id IS NOT NULL
--   )
--   SELECT
--     COUNT(*) AS total_composer_deployments,
--     COUNT(DISTINCT (venue, symbol, entry_session_profile, arm_key)) AS unique_behaviors,
--     COUNT(*) - COUNT(DISTINCT (venue, symbol, entry_session_profile, arm_key)) AS duplicates,
--     SUM(CASE WHEN enabled THEN 1 ELSE 0 END) AS currently_enabled
--   FROM arms;


-- ═════════════════════════════════════════════════════════════════════════════
-- STEP B — commit: retire all-but-best per behavior-equivalent group.
-- ONLY RUN AFTER STEP A LOOKS RIGHT. RETURNING gives back the count.
-- ═════════════════════════════════════════════════════════════════════════════
-- WITH arms AS (
--   SELECT
--     deployment_id,
--     venue, symbol, entry_session_profile,
--     COALESCE(v5_enabled, FALSE) AS v5_enabled,
--     v5_evaluated_at,
--     last_promoted_at,
--     updated_at,
--     regexp_replace(tune_id, '^(mdl_.+?_m[0-9]+_m[0-9]+)(_.*)?$', '\1') AS arm_key
--   FROM scalp_v2_deployments
--   WHERE strategy_id = 'model_guided_composer_v2'
--     AND candidate_id IS NOT NULL
-- ),
-- ranked AS (
--   SELECT
--     deployment_id,
--     ROW_NUMBER() OVER (
--       PARTITION BY venue, symbol, entry_session_profile, arm_key
--       ORDER BY
--         v5_enabled DESC,
--         v5_evaluated_at DESC NULLS LAST,
--         last_promoted_at DESC NULLS LAST,
--         updated_at DESC
--     ) AS rank_in_group
--   FROM arms
-- )
-- UPDATE scalp_v2_deployments d
-- SET candidate_id    = NULL,
--     enabled         = FALSE,
--     v5_evaluated_at = NULL,
--     v5_lease_until  = NULL,
--     updated_at      = NOW()
-- FROM ranked r
-- WHERE d.deployment_id = r.deployment_id
--   AND r.rank_in_group > 1
-- RETURNING d.deployment_id;
