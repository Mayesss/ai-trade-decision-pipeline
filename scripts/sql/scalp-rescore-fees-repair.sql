-- ============================================================================
-- Bulk repair: charge round-trip trading fees against already-evaluated
-- v5_cell_evidence, recompute eligibility, and disable deployments left with
-- no eligible cell. Pairs with the harness fix (lib/scalp/replay/harness.ts)
-- so future evaluations are fee-correct; this fixes the CURRENTLY stored rows
-- without re-running the full evaluation.
--
-- Model: flat per-venue round-trip fee in R.
--   bitget  = :fee_r  (empirically ~0.30 R/trade; see scalp-rescore-fees.ts)
--   capital = 0       (embedded spread, not an explicit fee — left untouched)
-- This is an APPROXIMATION of the exact per-trade fee (roundTripFeeRate *
-- notional / risk) the patched harness computes; the stored aggregates lack
-- notional/risk, so a flat per-trade R is the faithful repair. For exactness,
-- re-run the evaluation instead.
--
-- Gate (mirrors lib/scalp-v5/index.ts): a cell is eligible iff
--   trades >= minTradesPerCell  AND  expectancyR > 0.
--
-- ⚠️  This DISABLES live deployments. Run STEP 1 (read-only) first, eyeball the
--     plan, then run STEP 2 inside the BEGIN/COMMIT block.
-- ============================================================================

\set fee_r 0.30

-- ---------------------------------------------------------------------------
-- Reusable CTE that produces the repaired evidence + new enabled flag.
-- (Used by both the dry-run SELECT and the UPDATE.)
-- ---------------------------------------------------------------------------
-- STEP 1 — DRY RUN (read-only): preview what changes.
WITH target AS (
  SELECT d.deployment_id, d.symbol, d.enabled,
         d.v5_cell_evidence AS ev,
         COALESCE((d.v5_cell_evidence->>'minTradesPerCell')::int, 8) AS min_trades,
         :fee_r::numeric AS fee_r
  FROM scalp_v2_deployments d
  WHERE d.venue = 'bitget'
    AND d.live_mode = 'live'
    AND d.retired_at IS NULL
    AND d.v5_cell_evidence ? 'cells'
),
cell AS (
  SELECT t.*, c.key AS cell_id, c.value AS cv,
         (c.value->>'trades')::numeric AS trades,
         (c.value->>'netR')::numeric   AS net_r
  FROM target t
  CROSS JOIN LATERAL jsonb_each(t.ev->'cells') AS c(key, value)
),
cell_adj AS (
  SELECT c.*,
    c.net_r - c.fee_r * c.trades AS adj_net_r,
    CASE WHEN c.trades > 0 THEN (c.net_r - c.fee_r * c.trades) / c.trades ELSE 0 END AS adj_exp,
    -- weeklyNetR[i] -= fee_r * weeklyTrades[i]
    (
      SELECT jsonb_agg(
               round(wn.v::numeric - c.fee_r * COALESCE(wt.v::numeric, 0), 8)
               ORDER BY wn.idx)
      FROM jsonb_array_elements_text(c.cv->'weeklyNetR') WITH ORDINALITY AS wn(v, idx)
      LEFT JOIN jsonb_array_elements_text(c.cv->'weeklyTrades') WITH ORDINALITY AS wt(v, idx)
        ON wt.idx = wn.idx
    ) AS adj_weekly
  FROM cell c
),
per_cell AS (
  SELECT deployment_id, symbol, enabled, min_trades, ev, cell_id,
         (cv
           || jsonb_build_object('netR', adj_net_r)
           || jsonb_build_object('expectancyR', adj_exp)
           || COALESCE(jsonb_build_object('weeklyNetR', adj_weekly), '{}'::jsonb)
         ) AS new_cell,
         (trades >= min_trades AND adj_exp > 0) AS adj_eligible
  FROM cell_adj
),
rebuilt AS (
  SELECT deployment_id, symbol, enabled, ev,
         jsonb_object_agg(cell_id, new_cell) AS new_cells,
         COALESCE(
           jsonb_agg(cell_id) FILTER (WHERE adj_eligible),
           '[]'::jsonb) AS new_eligible,
         COUNT(*) FILTER (WHERE adj_eligible) AS n_eligible
  FROM per_cell
  GROUP BY deployment_id, symbol, enabled, ev
)
SELECT symbol,
       enabled AS was_enabled,
       (n_eligible > 0) AS will_be_enabled,
       n_eligible,
       new_eligible
FROM rebuilt
ORDER BY n_eligible DESC, symbol;

-- ---------------------------------------------------------------------------
-- STEP 2 — APPLY (uncomment to run). Wrapped in a transaction; ROLLBACK to abort.
-- ---------------------------------------------------------------------------
-- BEGIN;
-- WITH target AS (
--   SELECT d.deployment_id, d.v5_cell_evidence AS ev,
--          COALESCE((d.v5_cell_evidence->>'minTradesPerCell')::int, 8) AS min_trades,
--          :fee_r::numeric AS fee_r
--   FROM scalp_v2_deployments d
--   WHERE d.venue = 'bitget' AND d.live_mode = 'live' AND d.retired_at IS NULL
--     AND d.v5_cell_evidence ? 'cells'
-- ),
-- cell AS (
--   SELECT t.*, c.key AS cell_id, c.value AS cv,
--          (c.value->>'trades')::numeric AS trades,
--          (c.value->>'netR')::numeric   AS net_r
--   FROM target t CROSS JOIN LATERAL jsonb_each(t.ev->'cells') AS c(key, value)
-- ),
-- cell_adj AS (
--   SELECT c.*,
--     c.net_r - c.fee_r * c.trades AS adj_net_r,
--     CASE WHEN c.trades > 0 THEN (c.net_r - c.fee_r * c.trades) / c.trades ELSE 0 END AS adj_exp,
--     (SELECT jsonb_agg(round(wn.v::numeric - c.fee_r * COALESCE(wt.v::numeric, 0), 8) ORDER BY wn.idx)
--        FROM jsonb_array_elements_text(c.cv->'weeklyNetR') WITH ORDINALITY AS wn(v, idx)
--        LEFT JOIN jsonb_array_elements_text(c.cv->'weeklyTrades') WITH ORDINALITY AS wt(v, idx)
--          ON wt.idx = wn.idx) AS adj_weekly
--   FROM cell c
-- ),
-- per_cell AS (
--   SELECT deployment_id, min_trades, ev, fee_r, cell_id,
--          (cv || jsonb_build_object('netR', adj_net_r)
--             || jsonb_build_object('expectancyR', adj_exp)
--             || COALESCE(jsonb_build_object('weeklyNetR', adj_weekly), '{}'::jsonb)) AS new_cell,
--          (trades >= min_trades AND adj_exp > 0) AS adj_eligible
--   FROM cell_adj
-- ),
-- rebuilt AS (
--   SELECT deployment_id, ev, fee_r,
--          jsonb_object_agg(cell_id, new_cell) AS new_cells,
--          COALESCE(jsonb_agg(cell_id) FILTER (WHERE adj_eligible), '[]'::jsonb) AS new_eligible,
--          COUNT(*) FILTER (WHERE adj_eligible) AS n_eligible
--   FROM per_cell GROUP BY deployment_id, ev, fee_r
-- )
-- UPDATE scalp_v2_deployments d
-- SET v5_cell_evidence = r.ev
--       || jsonb_build_object('cells', r.new_cells)
--       || jsonb_build_object('eligibleCells', r.new_eligible)
--       || jsonb_build_object('feeAdjusted',
--            jsonb_build_object('feeRPerTrade', r.fee_r, 'model', 'flat_round_trip_R')),
--     enabled = (r.n_eligible > 0),
--     v5_enabled = (r.n_eligible > 0),
--     updated_at = now()
-- FROM rebuilt r
-- WHERE d.deployment_id = r.deployment_id;
-- COMMIT;
