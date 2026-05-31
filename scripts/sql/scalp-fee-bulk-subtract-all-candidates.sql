-- ============================================================================
-- Universe-wide fee/spread bulk-subtract across ALL candidate-backed deployments.
-- Pairs with the deployed harness cost fix; this re-costs the CURRENTLY stored
-- (08:00 Sunday, fee-free) evidence without a 14h full re-eval. The harness
-- doesn't change the trade path, so netR_new = netR - feeR*trades reproduces a
-- re-run (approx, using per-(venue,symbol) average feeR instead of per-trade).
--
-- feeR (round-trip cost in R per trade):
--   bitget  = 0.30 flat (live-measured, stable across symbols)
--   capital = per-symbol, measured via scalp-measure-forex-feer.ts
--             (0-trade reps -> 0.20 FX median fallback; metals ~0 because the
--              harness models a flat 1.1-pip spread, negligible vs gold/silver
--              stops — a known harness under-model for metals)
--
-- Targets ONLY the fee-free baseline to avoid double-charging the rows already
-- made fee-aware this session:
--   - feeAdjusted IS NULL            (skips bitget SQL-repaired rows)
--   - v5_evaluated_at < 13:00 today  (skips harness re-evals: 4 capital + probe)
--
-- Gate mirrors lib/scalp-v5/index.ts: cell eligible iff trades>=minTrades AND expR>0.
-- v5_enabled := has eligible cell; enabled demoted only (never auto-promoted here).
--
-- ⚠️ Run STEP 1 (read-only) first. STEP 2 is the UPDATE (commented).
-- ============================================================================

\set cutoff '2026-05-31 13:00:00+00'

-- Per-(venue,symbol) feeR. bitget handled by CASE; capital listed explicitly.
-- (Reused by both steps via the `feer` CTE.)

-- ---------------------------------------------------------------------------
-- STEP 1 — DRY RUN: per-venue impact summary.
-- ---------------------------------------------------------------------------
WITH feer(symbol, fee_r) AS (VALUES
  ('AUDCAD',0.20),('AUDJPY',0.13288),('AUDNZD',0.21109),('AUDUSD',0.17129),
  ('CADCHF',0.20),('CADJPY',0.20),('CHFJPY',0.23626),('EURAUD',0.14858),
  ('EURCAD',0.20),('EURGBP',0.37985),('EURJPY',0.10283),('EURUSD',0.20367),
  ('GBPCAD',0.25417),('GBPCHF',0.20),('GBPJPY',0.1234),('GBPUSD',0.11317),
  ('NZDJPY',0.20),('NZDUSD',0.22249),('USDCAD',0.32686),('USDCHF',0.2253),
  ('USDJPY',0.15356),('XAGUSD',0.04033),('XAUUSD',0.00082)
),
target AS (
  SELECT d.deployment_id, d.venue, d.symbol, d.enabled,
         d.v5_cell_evidence AS ev,
         COALESCE((d.v5_cell_evidence->>'minTradesPerCell')::int, 8) AS min_trades,
         CASE WHEN d.venue='bitget' THEN 0.30::numeric
              ELSE COALESCE(f.fee_r, 0.20)::numeric END AS fee_r
  FROM scalp_v2_deployments d
  LEFT JOIN feer f ON f.symbol = d.symbol
  WHERE d.candidate_id IS NOT NULL AND d.retired_at IS NULL
    AND d.v5_cell_evidence ? 'cells'
    AND (d.v5_cell_evidence->'feeAdjusted') IS NULL
    AND (d.v5_evaluated_at IS NULL OR d.v5_evaluated_at < :'cutoff')
),
cell AS (
  SELECT t.deployment_id, t.venue, t.enabled, t.min_trades, t.fee_r,
         (c.value->>'trades')::numeric AS trades,
         (c.value->>'netR')::numeric   AS net_r
  FROM target t CROSS JOIN LATERAL jsonb_each(t.ev->'cells') AS c(key, value)
),
cell_adj AS (
  SELECT deployment_id, venue, enabled,
         (trades >= min_trades
          AND CASE WHEN trades>0 THEN (net_r - fee_r*trades)/trades ELSE 0 END > 0) AS adj_eligible
  FROM cell
),
dep AS (
  SELECT deployment_id, venue, enabled,
         COUNT(*) FILTER (WHERE adj_eligible) AS n_eligible
  FROM cell_adj GROUP BY 1,2,3
)
SELECT venue,
       COUNT(*) AS deployments,
       COUNT(*) FILTER (WHERE n_eligible > 0) AS will_have_eligible,
       COUNT(*) FILTER (WHERE n_eligible = 0) AS loses_all_eligible,
       COUNT(*) FILTER (WHERE enabled) AS currently_enabled,
       COUNT(*) FILTER (WHERE enabled AND n_eligible = 0) AS enabled_to_demote
FROM dep GROUP BY venue ORDER BY venue;

-- ---------------------------------------------------------------------------
-- STEP 2 — APPLY (uncomment). Recomputes netR/expectancyR/weeklyNetR per cell,
-- rebuilds eligibleCells, stamps feeAdjusted, sets v5_enabled, demotes enabled.
-- ---------------------------------------------------------------------------
-- BEGIN;
-- WITH feer(symbol, fee_r) AS (VALUES
--   ('AUDCAD',0.20),('AUDJPY',0.13288),('AUDNZD',0.21109),('AUDUSD',0.17129),
--   ('CADCHF',0.20),('CADJPY',0.20),('CHFJPY',0.23626),('EURAUD',0.14858),
--   ('EURCAD',0.20),('EURGBP',0.37985),('EURJPY',0.10283),('EURUSD',0.20367),
--   ('GBPCAD',0.25417),('GBPCHF',0.20),('GBPJPY',0.1234),('GBPUSD',0.11317),
--   ('NZDJPY',0.20),('NZDUSD',0.22249),('USDCAD',0.32686),('USDCHF',0.2253),
--   ('USDJPY',0.15356),('XAGUSD',0.04033),('XAUUSD',0.00082)
-- ),
-- target AS (
--   SELECT d.deployment_id, d.enabled, d.v5_cell_evidence AS ev,
--          COALESCE((d.v5_cell_evidence->>'minTradesPerCell')::int, 8) AS min_trades,
--          CASE WHEN d.venue='bitget' THEN 0.30::numeric
--               ELSE COALESCE(f.fee_r, 0.20)::numeric END AS fee_r
--   FROM scalp_v2_deployments d
--   LEFT JOIN feer f ON f.symbol = d.symbol
--   WHERE d.candidate_id IS NOT NULL AND d.retired_at IS NULL
--     AND d.v5_cell_evidence ? 'cells'
--     AND (d.v5_cell_evidence->'feeAdjusted') IS NULL
--     AND (d.v5_evaluated_at IS NULL OR d.v5_evaluated_at < :'cutoff')
-- ),
-- cell AS (
--   SELECT t.deployment_id, t.min_trades, t.ev, t.fee_r, t.enabled,
--          c.key AS cell_id, c.value AS cv,
--          (c.value->>'trades')::numeric AS trades,
--          (c.value->>'netR')::numeric   AS net_r
--   FROM target t CROSS JOIN LATERAL jsonb_each(t.ev->'cells') AS c(key, value)
-- ),
-- cell_adj AS (
--   SELECT c.*,
--     c.net_r - c.fee_r*c.trades AS adj_net_r,
--     CASE WHEN c.trades>0 THEN (c.net_r - c.fee_r*c.trades)/c.trades ELSE 0 END AS adj_exp,
--     (SELECT jsonb_agg(round(wn.v::numeric - c.fee_r*COALESCE(wt.v::numeric,0), 8) ORDER BY wn.idx)
--        FROM jsonb_array_elements_text(c.cv->'weeklyNetR') WITH ORDINALITY AS wn(v, idx)
--        LEFT JOIN jsonb_array_elements_text(c.cv->'weeklyTrades') WITH ORDINALITY AS wt(v, idx)
--          ON wt.idx = wn.idx) AS adj_weekly
--   FROM cell c
-- ),
-- per_cell AS (
--   SELECT deployment_id, enabled, ev, fee_r, cell_id,
--          (cv || jsonb_build_object('netR', adj_net_r)
--             || jsonb_build_object('expectancyR', adj_exp)
--             || COALESCE(jsonb_build_object('weeklyNetR', adj_weekly), '{}'::jsonb)) AS new_cell,
--          (trades >= min_trades AND adj_exp > 0) AS adj_eligible
--   FROM cell_adj
-- ),
-- rebuilt AS (
--   SELECT deployment_id, enabled, ev, fee_r,
--          jsonb_object_agg(cell_id, new_cell) AS new_cells,
--          COALESCE(jsonb_agg(cell_id) FILTER (WHERE adj_eligible), '[]'::jsonb) AS new_eligible,
--          COUNT(*) FILTER (WHERE adj_eligible) AS n_eligible
--   FROM per_cell GROUP BY deployment_id, enabled, ev, fee_r
-- )
-- UPDATE scalp_v2_deployments d
-- SET v5_cell_evidence = r.ev
--       || jsonb_build_object('cells', r.new_cells)
--       || jsonb_build_object('eligibleCells', r.new_eligible)
--       || jsonb_build_object('feeAdjusted',
--            jsonb_build_object('feeRPerTrade', r.fee_r, 'model', 'bulk_subtract_per_symbol_R')),
--     v5_enabled = (r.n_eligible > 0),
--     enabled = (d.enabled AND r.n_eligible > 0),
--     updated_at = now()
-- FROM rebuilt r
-- WHERE d.deployment_id = r.deployment_id;
-- COMMIT;
