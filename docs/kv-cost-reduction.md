# Upstash KV cost reduction

Measured 2026-09-02 against the live pay-as-you-go database. Keyspace figures
come from a read-only `SCAN`/`MEMORY USAGE` sweep, traffic figures from
`swing.tick_log` / `swing.decisions` in Neon, per-path command counts from the
code cited.

## The bill

Invoice `77ZGFMMU-0017` (Aug 2026), via the Vercel marketplace:

| Line | Amount | Implies |
| --- | --- | --- |
| Pay-as-you-go **Request** cost | **$7.85** | **~3.9M commands/month** at $0.20/100K |
| Pay-as-you-go **Storage** cost | $0.17 | ~0.7 GB — matches the measured ~510 MB keyspace |
| VAT (19%, Germany) | $1.52 | not reducible |
| **Total** | **$9.54** | |

So this is **98% a command-count problem**. Storage is already noise, and
pay-as-you-go bills per command executed — a pipeline of N commands is still N
commands, so pipelining saves nothing. Only *fewer commands* do: multi-key
`MGET`/`DEL`, deduplicated reads, amortized housekeeping.

## Where the ~3.9M commands go

| Source | Runs/month | Commands each | Total/month | Share |
| --- | --- | --- | --- | --- |
| **Dashboard summary warm** (all 4 ranges) | 2,880 (1 per 15-min cycle) | **~500** | **~1.4M** | **37%** |
| analyze tick path | 62,303 | ~11–13 | ~0.8M | 20% |
| `warm-status` client poll | 129,600 per open tab | 2 | ~0.26M/tab | 7%/tab |
| Cached summary fetch (last-scan overlay) | ~96/day per open tab | **26** | ~0.07M/tab | 2%/tab |
| wake-watch | 43,200 (1/min) | ~2–4 | ~0.13M | 3% |
| `/api/chart`, `/api/timeline`, `/api/evaluate`, extra tabs/devices | — | — | remainder | ~25% |

Real traffic, for reference: 62,303 ticks in 30 days (~2,077/day), of which
**83% exit at one gate** (`primary_close_gate`, 12,050 of 14,482 in 7d) and only
**131 in 7 days reached an AI call** (0.9%). 6,839 decision rows / 30d.

Note `live-price` polls every 3 seconds per open tab and costs **zero** KV
commands (broker direct) — that path is already right; leave it alone.

### The dominant cost: the summary warm re-reads everything 4×

`warmAllSwingSummaries` ([summary.ts:696](../pages/api/dashboard/summary.ts#L696))
calls `buildAndCacheSwingSummary(range)` once per range, and each range runs the
**identical** per-symbol reads ([summary.ts:401](../pages/api/dashboard/summary.ts#L401)):

```
loadDecisionHistory(symbol, 120, platform)   // ZREVRANGE + 2-3 chunked MGETs
readSwingLastScan(platform, symbol)          // GET
```

The `120` is a constant — it does not vary by range. So per warm:
**4 ranges × 25 symbols × ~5 commands ≈ 500 commands, ~375 of them byte-identical
repeats**, plus ~4.5 MB of payload re-downloaded 4×.

### The runner-up: the freshness marker is written twice per tick

A `primary_close_gate` skip spends 11 commands, 8 of them the same metadata twice:

| # | Commands | What | Where |
| --- | --- | --- | --- |
| 4 | `SETEX` + `LPUSH` + `LTRIM` + `EXPIRE` | freshness marker at tick start | [analyze.ts:474](../pages/api/analyze.ts#L474) → [lastScan.ts:52](../lib/swing/lastScan.ts#L52) |
| 1 | `GET` | cron kill-switch state | [analyze.ts:583](../pages/api/analyze.ts#L583) |
| 4 | `SETEX` + `LPUSH` + `LTRIM` + `EXPIRE` | **the same marker again**, with the skip stage | [analyze.ts:495](../pages/api/analyze.ts#L495) |
| 2 | `INCR` + `EXPIRE` | warm latch | [analyze.ts:3914](../pages/api/analyze.ts#L3914) → [warmLatch.ts:37](../lib/swing/warmLatch.ts#L37) |

All of it is also written durably to `swing.tick_log` on the same tick; the KV
copy exists only for dashboard freshness.

## As built (shipped 2026-09-02)

All seven items are implemented and behaviour-neutral. Measured against the
contract snapshots, which record every KV command, so the diff *is* the proof:
18 snapshots lost exactly the commands below and nothing else.

| # | Change | Saves/month | Where |
| --- | --- | --- | --- |
| 1 | One set of per-symbol reads shared by all 4 range builds | **~930k (~$1.86)** | [summary.ts](../pages/api/dashboard/summary.ts) |
| 2+3 | Tick stamp: housekeeping once per tick, on the hourly tick | ~212k (~$0.42) | [lastScan.ts](../lib/swing/lastScan.ts), [analyze.ts:474](../pages/api/analyze.ts#L474) |
| 4 | `warm-status`: 2 `GET` → 1 `MGET` | ~130k/tab (~$0.26) | [warm-status.ts](../pages/api/dashboard/warm-status.ts) |
| 5 | Cached-summary overlay: 25 `GET` → 1 `MGET` | ~70k/tab (~$0.14) | [summary.ts](../pages/api/dashboard/summary.ts) |
| 6 | Warm latch: `EXPIRE` on the first increment and the finisher only | ~57k (~$0.11) | [warmLatch.ts](../lib/swing/warmLatch.ts) |
| 7 | Summary invalidation: 4 `DEL` → 1 multi-key `DEL` | ~20k (~$0.04) | [summaryCache.ts](../lib/swing/summaryCache.ts) |
| | **Total** | **~1.42M (~$2.85 of $7.85)** | 7 files |

Expect the Request line to land near **$5.00** (invoice ≈ **$5.95** with VAT),
assuming dashboard tab-hours stay as they were.

### How each one keeps behaviour identical

1. **Shared per-symbol reads.** `warmAllSwingSummaries` calls
   `prefetchSummarySymbolReads()` once and passes the map into each range build;
   a range with no prefetch (the on-demand HTTP path) still reads for itself, so
   the two paths cannot drift. Sound because `HISTORY_ROWS_PER_SYMBOL` is
   range-independent — every range asked for the same 120 rows. The shared
   arrays are never mutated downstream: `capitalHistoryClosedWindows`,
   `extractCapturedLeverages` and `enrichCapitalWindowFromHistory` all build new
   arrays (the `.sort()` in the last one runs on a `.filter()` result), so one
   range cannot disturb another. Per warm: ~400 commands → ~76.
2. **Tick stamp.** `stampSwingScanStarted` (tick start) writes `SETEX` + `LPUSH`
   and never trims; `recordSwingLastScan` (tick outcome) trims and refreshes the
   TTL only when the tick's own minute-of-hour is < 15 — i.e. the `:00` firing,
   once an hour per symbol. Readers take `LRANGE 0..399`, so a list that briefly
   holds ~404 entries hands back the same newest 400, and a 7-day TTL refreshed
   hourly never expires. The rule is the clock, not a random draw, because the
   contract snapshots record every command.
   *Deliberately conservative:* the start stamp keeps its `LPUSH`. Dropping it
   would save another ~62k/month, but a tick hard-killed by the platform (no
   error path, no `tick_log` row) would then leave no timeline dot at all —
   the KV list is its only trace. Not worth $0.12/month.
3. **Warm latch.** The TTL is stamped on the first increment *and* on the
   finisher, rather than once, so a failed first `EXPIRE` cannot leave a
   TTL-less key. Cycle ids never repeat, so the worst case is one stale latch
   key, never a collision.
4. **Batched reads.** `MGET` is one billed command whatever the key count, and
   `kvMGetJson` already returned per-key nulls on failure — the same fail-open
   the individual `.catch(() => null)` calls had. `parseSwingWarmLast` /
   `parseSwingAiHealth` apply the identical normalization the two loaders did.
5. **Multi-key `DEL`.** `kvDel` is now variadic and filters empty keys; the four
   summary keys drop in one command, atomically instead of concurrently.

New coverage: [test/unit/swing/lastScan.test.ts](../test/unit/swing/lastScan.test.ts)
pins the command sequences (start stamp, hourly housekeeping, quarter-tick
skip, batched `MGET`, empty universe). Every captured fixture is frozen past
:15, so without this test the housekeeping branch would be untested.

Item 1 has no automated test: the warm only fires when the 15-minute latch
completes, which never happens under the contract harness (it expects all 25
crons). It is verified by code path and by the mutation audit above — worth a
look at the first live warm.

## What the chart fixes cost (measured 2026-09-03)

Two later changes add load, so here is the meter on them. One `/api/chart`
request, measured in-process against production data:

| | KV commands | KV bytes in | Neon queries | Neon bytes |
| --- | --- | --- | --- | --- |
| 1D preset (15m x96), caches warm | **4** | 24.3 KB | **3** (15 rows) | 10.4 KB |
| 4H preset (5m x48) | 5 (one cache fill) | 13.7 KB | 3 | 10.4 KB |
| 1D preset, cold lambda | 4 | 24.3 KB | 50 (schema ensure) | 10.5 KB |

**Warm-triggered chart refresh** (one fetch per completed analyze cycle per open
dashboard tab, 96/day):

- KV: 96 x ~4.5 = ~430/day = **~13k commands/month ≈ $0.03** — 0.9% of the
  ~1.42M/month the work above removed.
- Neon: 96 x ~10 KB = **~30 MB/month**, against the 50 GB alert / 90 GB
  free-tier thresholds in [neon-egress.md](./neon-egress.md). 0.03% of it.
- Cold starts add ~47 schema-ensure queries but almost no bytes; Neon bills
  transfer and compute, not query count.

**Widened resting-entry history read** (reads from `windowStartMs - 48h` so an
order placed before a short window still draws): **zero extra commands.**
`loadSymbolMarkerHistory` issues one `ZREVRANGEBYSCORE` plus a single unchunked
`MGET` ([history.ts:457](../lib/history.ts#L457)) regardless of row count, and
the marker index is 7-day-trimmed and small (~23 rows for BTCUSDT), so the wider
score range adds a few KB at most. The Upstash invoice has exactly two lines —
Request and Storage — so extra bytes at the same command count are free, and no
new keys means storage is untouched.

Net effect of everything on this page: **~-1.4M commands/month (~-$2.82)**.

## Storage — worth doing, but it is $0.17

`DBSIZE` = 11,927 keys, ~510 MB, of which **97% is orphaned data from the retired
scalp subsystem** (`docs/cleanup-forex-scalp.md`) — no TTL, and zero references in
`lib/`, `pages/` or `scripts/`:

| Key group | Keys | Avg size | Total | TTL |
| --- | --- | --- | --- | --- |
| `scalp:candles:v1:<sym>:1m:<n>` | 749 | 438 KB | ~313 MB | none |
| `scalp:candles-history:v1:<sym>:1m` | 30 | 6.07 MB | ~174 MB | none |
| `scalp:research:task:v1:…` | 8,510 | 1.1 KB | ~9.5 MB | none |
| `decision:<ts>:<venue>:<sym>` (live) | 1,622 | 1.1–1.8 KB | ~2 MB | 7d |
| `swing:chart:candles:*` (live) | ~150 | 8–18.5 KB | ~2 MB | minutes |
| `swing:scanTicks:v1:*` (live) | 25 lists | 34 KB (400 entries) | ~0.9 MB | 7d |

Deleting the 9,289 `scalp:*` keys saves ~$0.16/month — do it for hygiene, not for
the bill, and only on an explicit go-ahead: KV has no equivalent of a Neon
recovery branch, so it is irreversible. Batched `SCAN` + multi-key `DEL`, with a
counting dry-run first.

## Later, and only with measurement

- **Trim what the timeline downloads.** `readSwingScanTicks` `LRANGE`s all 400
  entries (~35 KB) then filters by `sinceMs` in JS
  ([lastScan.ts:88](../lib/swing/lastScan.ts#L88)); deriving a count from the
  window returns the same rows for far less bandwidth. Bandwidth is not billed
  separately here, so this is a latency win, not a cost one.
- **One summary key instead of four.** The builder produces all ranges together,
  so a single blob makes a load 1 `GET` and an invalidation 1 `DEL`.
- **Consolidate the three KV clients** — `lib/kv.ts` plus private copies in
  [history.ts:114](../lib/history.ts#L114) and [utils.ts:25](../lib/utils.ts#L25)
  — so multi-key primitives are available everywhere. Delete `kvCollectKeys`
  (no callers).
- **Question the KV decision index.** Neon is already the source of truth for
  decisions; serving the summary rebuild from Neon would cut the remaining
  per-symbol reads entirely, but it changes read paths — do it behind a
  before/after measurement.

## Verification

Re-run the read-only audit sweep (`DBSIZE`, grouped `SCAN`, sampled
`MEMORY USAGE`/`TTL`) and compare the Upstash console's commands/day across each
change. The next invoice's Request line is the acceptance test: it should land
near **$4.50** if items 1–7 ship and one dashboard tab stays open as before.
