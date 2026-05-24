# Scalp pipeline architecture

This doc describes how the scalp trading system is organized. The directory
prefixes `scalp-v2/`, `scalp-v3/`, `scalp-v4/`, `scalp-v5/` look like parallel
versions but are not — they are **stacked layers**, all simultaneously active.
The label `v` refers to when each layer was added, not to which is "current".

## Layer roles

| Codebase prefix | Role-based name | Lives in | What it does |
|---|---|---|---|
| **v2** | `core` | [`lib/scalp-v2/`](../lib/scalp-v2/) | Candidate generation, discovery, promotion, live execution. The pipeline backbone. Strategy ID `model_guided_composer_v2` is the production composer. |
| **v3** | `temporal-filter` | [`lib/scalp-v3/`](../lib/scalp-v3/) | 30-min session-slot, weekday, and UTC-hour **variant filters** applied to v2 candidates. `:v3sl3`, `:v3wd2`, `:v3uh14` tune-id suffixes come from here. |
| **v4** | `regime-classifier` | [`lib/scalp-v4/`](../lib/scalp-v4/) | Weekly regime classification — vol / trend / risk axes per (venue, symbol, week). Writes `scalp_regime_snapshots`. The cells (`vol=mid\|tr=up\|risk=on`) are consumed by v5. |
| **v5** | `regime-evidence` | [`lib/scalp-v5/`](../lib/scalp-v5/) | 12-week regime-tagged evaluation. Replays each deployment, buckets trades by the regime cell active at entry, gates live entries on positive-expectancy cells. Replaces v4's 104-week walk-forward sweep. |

Each layer adds variants OR gating; none replaces the predecessor. v2 still
runs the trades. v5 just decides which entries to take.

## Candidate lifecycle

```
discover               (v2 cycle cron, every 5 min)
   → DSL grid expansion + composer + temporal variants
   → write deployment row (scalp_v2_deployments, enabled=FALSE)

v5 evaluate            (hourly cron + local bulk + Sunday rollover)
   → load last 12 weeks of 1m candles
   → resume from checkpoint if available (incremental), else full replay
   → bucket trades by regime cell at entry timestamp
   → write v5_cell_evidence + v5_replay_checkpoint
   → set v5_enabled = TRUE iff any cell meets minTrades + expectancyR > 0

v5 promote             (every 15 min)
   → flips enabled = TRUE on rows with v5_enabled = TRUE

live execute           (every 1 min)
   → composed gates (see below)
   → place / manage / close trades

trim-tail              (Sunday cron, weekly)
   → retires rows v5 has failed to qualify for 28+ days
```

## Live entry gates (composed in order)

Each entry attempt is journaled with the reason codes from every gate that
blocked it. The strategy receives the combined `entryBlockReasonCodes` list
via `runtime.strategy.entryBlockReasonCodes` and refuses the entry if any
match.

| # | Gate | Source | Reason codes |
|---|---|---|---|
| 1 | Promotion gate | v2 promote, in `deployment.promotion_gate.entryBlockReasonCodes` | varied — e.g. drift, scope, freshness |
| 2 | News blackout | v3 — `evaluateScalpV2V3NewsBlackout` | `V3_NEWS_BLACKOUT*` |
| 3 | Regime envelope | v4 — `resolveScalpV4EnvelopeBlock` | `V4_*` |
| 4 | Cell evidence | v5 — `resolveScalpV5EntryBlock` | `V5_CELL_NEGATIVE_EXPECTANCY`, `V5_CELL_NOT_IN_EVIDENCE`, `V5_CELL_DATA_STALE`, `V5_CELL_EVIDENCE_MISSING`, `V5_CELL_INSUFFICIENT_TRADES` |
| 5 | Weekly trading window | v5 — `resolveScalpV5SundayBlock` | `SUNDAY_EVALUATION_WINDOW` |

Existing positions are NOT affected by any of these gates — they reconcile
and close naturally via the execute cron. The gates only block NEW entries.

## Cron schedule (vercel.json)

| Schedule | Path | Layer | Purpose |
|---|---|---|---|
| `*/1 * * * *` | `/api/scalp/v2/cron/execute` | v2 | Live execution loop |
| `*/2 * * * *` | `/api/scalp/v2/cron/reconcile` | v2 | Position reconciliation |
| `*/5 * * * *` | `/api/scalp/v2/cron/cycle` | v2 | Discovery + research + promote |
| `10 */2 * * *` | `/api/scalp/v2/cron/load-candles` | v2 | 1m candle ingestion (both venues) |
| `0 * * * *` | `/api/scalp/v5/cron/evaluate?limit=50` | v5 | Hourly 12-week evidence refresh |
| `*/15 * * * *` | `/api/scalp/v5/cron/promote` | v5 | Auto-promote v5-passing rows |
| `0 6 * * 0` | `/api/scalp/v5/cron/trim-tail` | v5 | Weekly: retire consistently-failing rows |

The weekly Sunday rollover (evidence advancement + cull + refill + promote) runs
from the terminal via [`scripts/scalp-v5-sunday.ts`](../scripts/scalp-v5-sunday.ts)
instead of a vercel cron — too long-running for the serverless function timeout
and easier to monitor / re-run locally.

## Candidate pool steady-state

After dedup + trim-tail, the pool stabilizes at ~2-5k composer deployments
(varies with symbol coverage). The mechanics:

- **Growth**: discovery + composer + temporal-variant expansion adds new rows continuously.
- **Behavior dedup**: [`buildScalpV2ModelGuidedComposerGrid`](../lib/scalp-v2/research.ts) collapses variant tuples that the composer executes identically (exit-rule and regime-gate axes — the composer's internal exit logic ignores those overrides). Without this, the pool grew ~9-12× larger than necessary.
- **Trim-tail**: retires rows that have been v5-evaluated, have never had `v5_enabled = TRUE`, were never live-promoted, and have been in the pool >28 days. Self-cleaning mechanism for proven non-eligible candidates.

Manually retired rows survive as DB rows with `candidate_id = NULL` (out of the
v5 eval queue) and `enabled = FALSE` (no new live entries). The history is
preserved — nothing is hard-deleted.

## Storage layout

| Column / table | Owned by | Notes |
|---|---|---|
| `scalp_v2_deployments` | v2 | Authoritative deployment registry. ALL versions write here. |
| `scalp_v2_deployments.v5_cell_evidence` JSONB | v5 | Per-cell evidence stats (~10-50 KB) |
| `scalp_v2_deployments.v5_replay_checkpoint` JSONB | v5 | Strategy state + closed-candle tails for incremental replay (~100-200 KB; TOAST'd) |
| `scalp_v2_deployments.v5_lease_until` TIMESTAMPTZ | v5 | Work-lease TTL for concurrent evaluators |
| `scalp_regime_snapshots` | v4 | Weekly regime cell per (venue, symbol, week) |
| `scalp_candle_history_weeks` | v2 | 1m candles, per-week blob |
| `scalp_v2_journal` | v2 | Execution event log; gates' decisions land here in `rawPayload` |
| `scalp_regime_walkforward_results` | v4 | v4's walkforward sweep results (dormant) |
| `scalp_v2_runtime_config` | v2 | Singleton runtime config (kill switch, scope, risk profile) |

## Why no "v5 everything"

v5 was designed as the evaluation / gating layer specifically. Replacing
v2's candidate generation with a "v5-native" generator would mean rebuilding
discovery + primitive blocks + DSL grid + composer + temporal variants from
scratch — months of work for unclear gain. The v2 path produces good
candidates; the bottleneck v5 actually solves is evaluation speed and
regime-awareness, which it does cleanly.

## Why no rename

Renaming the four directory trees to role-based names (`scalp-core`,
`scalp-temporal-filter`, etc.) is a multi-day churn that would touch ~hundreds
of files and break any external references. The naming-by-version-number is
historical baggage but doesn't cost runtime correctness — it just makes the
codebase a bit harder to onboard onto. This doc is the lower-cost fix.
