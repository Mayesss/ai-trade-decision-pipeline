# Cleanup plan: retire forex & scalp, keep lib/swing as the only actor

Status: SKETCH — reviewed dependency map, no code changed yet.
Date: 2026-08-28

## Dependency map (verified by import analysis)

### Forex: fully severable, already half-dead
- **No kept code imports `lib/forex` at all.**
- All 8 `pages/api/forex/*` routes are already 410 stubs via `lib/forex/deprecation.ts`.
- `lib/swing/forexEvents.ts` is swing-internal (economic-events context) despite the
  name — it does NOT depend on `lib/forex`. Keep it.
- `data/forexUniverse.json` is used by `lib/swing/category.ts`. Keep it.

### Scalp: kept code depends on exactly 7 modules (the "shared kernel")
| Consumer (kept) | Scalp module | What it uses |
|---|---|---|
| `lib/swing/pg.ts`, `lib/swing/weeklyDigest.ts`, `scripts/scalp-pg-healthcheck.ts` | `lib/scalp/pg/client.ts`, `lib/scalp/pg/sql.ts` | shared Neon Postgres client (`scalpPrisma`, `isScalpPgConfigured`, `sql`) |
| `lib/capital.ts` (Capital.com platform client — live trading gates) | `lib/scalp/symbolInfo.ts`, `lib/scalp/marketHours.ts` (→ transitively `symbolMarketMetadata.ts`) | `isPreciousMetalFamilySymbol`, `resolveOpeningHoursState`, `ScalpAssetCategory` |
| `pages/api/swing/wake-watch.ts` | `lib/scalp/cronChaining.ts` | `invokeCronEndpoint` (generic util; only invokes `/api/swing/analyze`) |
| `lib/symbolRegistry.ts` | `lib/scalp/deployments.ts`, `lib/scalp/venue.ts` | **only by the `getScalpCron*` half, which NOTHING imports → dead code, delete instead of extract** |

All 7 modules are self-contained (imports: only each other + `pg`/`@neondatabase`/next types).
`lib/scalp/deployments.ts` drags in `strategies/registry` — another reason to delete its
consumer half of symbolRegistry rather than move it.

### UI
- `pages/index.tsx` → re-exports `pages/legacy.tsx` (9,379 lines): the combined
  swing+scalp dashboard. It imports **no scalp lib code** but contains a full scalp mode:
  ~1,000 scalp references, fetches ~27 `/api/scalp/*` endpoints. Must be stripped surgically.
- Scalp-only, delete: `pages/scalp-backtest.tsx`, `components/ScalpBacktestChart.tsx`,
  `components/scalp/` (already imported by nobody — dead).
- Keep: `components/WeeklyDigestPanel.tsx`, `ChartPanel`, `ChartSkeleton`.

### Routes, crons, scripts, data
- `vercel.json` crons: **all swing** — nothing to change there.
- Delete: `pages/api/scalp/` (24 routes), `pages/api/forex/` (8 stub routes).
- Scripts: ~31 of 36 scripts are forex/scalp research/backtest tooling → delete, plus their
  `package.json` entries (`test:forex`, `test:scalp`, `backtest:scalp:*`, `scalp:*`,
  `bulk:research`). Exceptions to keep (rename): `scalp-pg-healthcheck.ts` (generic health
  check on the shared DB client), `with-db-env.mjs`.
- Data: delete `data/scalp-*.json`, `data/scalp-replay/`; check `data/replay/` (forex replay
  fixtures → delete). Keep `capitalTickerMap.json`, `forexUniverse.json`.

### Database (shared Neon instance!)
- Swing exclusively uses the **`swing.` schema** (decisions, positions, ai_threads,
  ai_cooldowns, break_triggers, tick_log, postmortems, lessons, account_snapshots,
  weekly_digests) — clean separation.
- Scalp tables live in the same DB, public schema (`scalp_execution_runs`, `scalp_v2_*`, …).
  Inventory live via `information_schema` at execution time — DDL is not all in the repo.
- ⚠️ The env vars are scalp-named but power SWING's DB: `SCALP_PG_CONNECTION_STRING`
  (then `NEON__DATABASE_URL`, `DATABASE_URL`, …). **Never delete these in Vercel** until the
  client's resolution order is migrated.
- KV likely holds `scalp:*` candle keys (scalp-sync-candles-kv) — list before deleting; low
  priority, cheap to leave.

## Execution phases (one green commit each; branch `cleanup/forex-scalp`)

**Phase 0 — baseline & safety net.**
Branch; record green baseline: `npx tsc --noEmit`, `node --import tsx --test` over kept
tests (12 swing test files + root lib tests), `next build`. Add characterization tests at
the two behavior-bearing seams before touching them:
`capital.ts` market-hours gating (via `resolveOpeningHoursState`/`isPreciousMetalFamilySymbol`)
and `getCronSymbolConfigs()` parsing of vercel.json. (`invokeCronEndpoint` is covered by
`wakeWatch.test.ts` — verify.)

**Phase 1 — extract the shared kernel (no deletions, no renames).**
- `lib/scalp/pg/{client,sql}.ts` → `lib/db/{client,sql}.ts` (env resolution byte-identical).
- `lib/scalp/{symbolInfo,symbolMarketMetadata,marketHours}.ts` → `lib/market/`.
- `lib/scalp/cronChaining.ts` → `lib/cronChaining.ts`.
- Leave re-export shims at the old paths so untouched scalp internals still compile;
  shims die in Phase 3.
- `lib/symbolRegistry.ts`: delete the dead scalp half (`ScalpCronSymbolConfig`,
  `parseScalpCronPath`, `getScalpCron*`) and its scalp imports.
- Verify: tsc + tests + build.

**Phase 2 — strip scalp mode from `pages/legacy.tsx`.**
Purely subtractive edit of the landing page: remove scalp types, state, fetches, panels,
mode toggle; swing mode stays pixel-identical. Own commit. Verify with build + loading the
dashboard. (Before Phase 3 so the deployed UI never fetches deleted routes.)

**Phase 3 — delete forex & scalp.**
`git rm`: `lib/forex/`, `lib/scalp/` (+ Phase-1 shims), `pages/api/forex/`,
`pages/api/scalp/`, `pages/scalp-backtest.tsx`, `components/ScalpBacktestChart.tsx`,
`components/scalp/`, the ~29 dead scripts + package.json script entries, scalp/forex data
files, `docs/scalp-higher-timeframe-era.md`. Keep+rename `scalp-pg-healthcheck.ts` →
`pg-healthcheck.ts` (repoint import to `lib/db/client`). Git history is the attic — no
archive folder. Optional: one catch-all 410 stub at `/api/scalp/[...path].ts` +
`/api/forex/[...path].ts` for a grace period.
Verify: tsc + tests + build + run the app.

**Phase 4 — dead-code sweep & cosmetic renames.**
Run `npx knip` (or ts-prune) to find root-lib files/exports orphaned by the deletion
(candidates: parts of `analytics.ts`, `history.ts`, `indicators.ts`, unused deps like
`ag-grid` if only the scalp UI used it). Then one pure-rename commit:
`scalpPrisma`→`pgClient`, `isScalpPgConfigured`→`isPgConfigured`,
`ScalpAssetCategory`→`AssetCategory`, etc., plus `db:pg:health` script name. Update
`AGENTS.md`/`readme.md`/`docs/architecture.md` references.

**Phase 5 — database deprecation (deferred, destructive — separate approval).**
1. Now: inventory tables outside `swing.` schema via `information_schema`; record counts/sizes.
2. After ~2–4 weeks of the trimmed code running clean: create a Neon branch as a snapshot
   backup, then `DROP` the scalp/forex tables. Optionally clear `scalp:*` KV keys.
3. Optionally add a neutral `PG_CONNECTION_STRING` to the front of the client's env
   resolution and migrate the Vercel env var name; only then retire `SCALP_PG_*` names.

## Risk register
- `lib/capital.ts` market-hours logic gates **live trades** on the capital platform — it gets
  a test before its imports move (Phase 0).
- `legacy.tsx` edit is the riskiest diff (landing page, 9.4k lines) — isolated commit,
  subtractive only, manual verification.
- Vercel env vars with `SCALP_` names are load-bearing for swing — do not touch until Phase 5.3.
- DB drops are irreversible — Neon branch snapshot first, weeks of soak time, explicit approval.
