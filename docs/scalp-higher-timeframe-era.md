# Scalp M30/H1 higher-timeframe era

Set up 2026-06-13. Moves composer discovery off the fee-dominated M15/M5 scalp
band onto M30/H1, where a wider 1R makes the fixed venue fee a small fraction of
edge instead of the dominant cost.

## Why

A timeframe probe ([`scripts/scalp-tf-probe.ts`](../scripts/scalp-tf-probe.ts))
over 26–52 weeks of 1m candles showed the M15 round-trip fee is ~0.35R/trade
(255% of a near-zero gross edge), falling to ~0.14R at H1. Net R/trade improves
monotonically with timeframe and the weekly bleed drops ~10×. See the
`project_higher_timeframe_fee_finding` memory for the full numbers.

## What changed in code

| Change | Where |
|---|---|
| Discovery emits ONLY `m30_m5` / `h1_m15` | `COMPOSER_DISCOVERY_TIMEFRAME_VARIANTS` in [`composerExecution.ts`](../lib/scalp/composer/composerExecution.ts); grid loop in [`research.ts`](../lib/scalp/composer/research.ts) |
| Stage windows scale by TF (×4 for H1 → stage C 12w→48w) | `COMPOSER_DISCOVERY_STAGE_WEEK_MULTIPLIER` + `resolveWorkerPolicy` in [`pipeline.ts`](../lib/scalp/composer/pipeline.ts) |
| Cross-symbol pooled significance gate | [`pooledSignificance.ts`](../lib/scalp/composer/pooledSignificance.ts) + promote gate in `pipeline.ts` |
| Higher TFs added to type system / registry | `ScalpBaseTimeframe`/`ScalpConfirmTimeframe`, `timeframeVariants.ts`, `marketData.ts`, `config.ts` |

Legacy M15/M5 deployments are untouched — they keep running and retire
naturally. Only NEW discovery is on the higher band.

## How the funnel works now

- **Stage A/B/C** stay PER-SYMBOL but are *quality/direction* gates over
  TF-scaled windows (A 16w, B 24w, C 48w at H1). Their trade-count minimums are
  lowered (B 14→6, C 24→10) because they no longer carry the significance load.
- **Promotion significance** is now POOLED across each strategy's sibling
  symbols (same venue+session+arm+modelVersion). The `40`-trade floor and
  `lowerBoundR ≥ −0.05` bar are evaluated on the pooled per-trade population
  (exact mean/variance reconstructed from each symbol's persisted `n/meanR/stdR`).
  A consistent cross-symbol edge gets a tighter pooled lower bound and promotes;
  a one-symbol fluke is diluted by weak siblings and is rejected.

## Config (set in `.env.local`; mirror to Vercel env for production)

```
SCALP_COMPOSER_POOLED_SIGNIFICANCE_ENABLED=true
SCALP_EVIDENCE_HOLDOUT_WEEKS=24            # was 6 (M15 era)
SCALP_COMPOSER_WORKER_STAGE_B_MIN_TRADES=6 # was 14
SCALP_COMPOSER_WORKER_STAGE_C_MIN_TRADES=10# was 24
# Optional explicit override of the auto-derived TF window multiplier:
# SCALP_COMPOSER_STAGE_WEEK_MULT=4
```

The discovery cutover, ×4 window scaling, and pooling-on are code defaults —
the env block only right-sizes the per-symbol trade gates and holdout split.

## Launching the bulk locally (not auto-run by this setup)

The local runner is [`scripts/research-local-bulk.ts`](../scripts/research-local-bulk.ts)
(loads `.env.local` itself, loops discovery + stage A/B/C until the pool drains,
caches candles per symbol, auto-backs-off). Run in order:

```bash
# 1. Composer discovery + stage A/B/C — the heavy bulk (M30/H1 grid, 48-week
#    stage-C windows). Long-running; safe to Ctrl-C and resume (cache-deduped).
BULK_RESEARCH_VERSION=composer \
  node scripts/with-db-env.mjs node --import tsx scripts/research-local-bulk.ts

# 2. Regime-tagged evidence refresh (v5) over the freshly-staged deployments.
BULK_RESEARCH_VERSION=research \
  node scripts/with-db-env.mjs node --import tsx scripts/research-local-bulk.ts

# 3. Promote — applies the cross-symbol POOLED significance gate. Promotion is
#    OFF by default; enable it for this run. Has live side effects (flips
#    deployments enabled), so run it deliberately.
SCALP_COMPOSER_PROMOTE_ENABLED=true \
  node scripts/with-db-env.mjs node --import tsx -e \
  "import('./lib/scalp/composer/pipeline.ts').then(m=>m.runScalpComposerPromoteJob()).then(r=>{console.log(JSON.stringify(r.details,null,2));process.exit(0)})"
```

Production: set the env block in Vercel and the existing `*/5` cycle +
hourly evaluate + `*/15` promote crons take over (promote needs
`SCALP_COMPOSER_PROMOTE_ENABLED=true` in Vercel env too). Watch what survives
via the dashboard summary / [`scalp-gate-roster.ts`](../scripts/scalp-gate-roster.ts).

## Candle coverage

Stage C reaches back `holdout(24w) + stageC(48w) = 72 weeks`, so each discovery
symbol needs ~72 weeks of 1m candles fresh to the last complete week. As of
2026-06-13 most seed symbols have 109 weeks; these 7 are too shallow (~20-24w)
and will be skipped / fail stage C until backfilled:
`PEPEUSDT, SHIBUSDT, UNIUSDT, XLMUSDT, EURJPY, GBPJPY, XAGUSD`.
Backfill via the load-candles job before the bulk if you want them included.

## Validate before trusting promotions

Re-run the probe at 52w to confirm per-symbol stage viability and pooled trade
counts for the cohort you're discovering on:
```
node scripts/with-db-env.mjs node --import tsx scripts/scalp-tf-probe.ts \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT --tfs m30_m5,h1_m15 --weeks 52
```

## Rollback

Delete the env block above (reverts gate tuning) and/or set
`SCALP_COMPOSER_POOLED_SIGNIFICANCE_ENABLED=false`. To restore M15/M5 discovery,
revert `COMPOSER_DISCOVERY_TIMEFRAME_VARIANTS` to include the full variant set.
