# N3 scout: listings, pre-launch perps, announcements and token unlocks

2026-10-12. This scout checks whether N3 can be tested; it does not ask whether N3 pays.

**No outcome was computed.** That means no post-event return, no funding value after a listing, no "did it dump" count, and no mean, median or hit rate of any post-event move, on any window. Event counts are never split by a future quantity.

The only market values read are levels at listing, all allowed by the brief:
- first-hour quote volume;
- day-0 quote volume;
- open-interest value at the end of the first hour.

There is also one dispersion of returns, taken from **non-event** coin-weeks on the 009 discovery panel (§E).

Scripts are in `research/copytrade/scouts/`:
- `n3_binance_events.py`: the Binance calendar, built from archive key listings.
- `n3_binance_probe.py`: per-event prefix listings, the listing-day pull, and the first-hour parse and summaries.
- `n3_hl_events.py`: the HL calendar and funding coverage.
- `n3_bitget.py` and `n3_crossvenue.py`: Bitget data, day-1 availability, spot before perp.
- `n3_unlocks.py`: point-in-time quality of the unlock calendar.
- `n3_power.py`: non-event dispersion and minimum detectable effect (MDE).

Data is under `research/copytrade/data/scout_n3/` (gitignored). Every pull sits in its own new directory with a `SHA256SUMS` file, and printed outputs are in `derived/*.txt`. Two web desk-research helpers (subagents, text only) supplied §C's docs facts and §D's literature. Their reads are listed in §H.

## Summary (key numbers)

1. **The Binance USDⓈ-M listing calendar is clean, point-in-time and free.**
   - 671 crypto-perp listings from 2020-01-02 to 2026-06-30, plus 13 relistings.
   - Per year: 2020: 77 · 2021: 58 · 2022: 26 · 2023: 99 · 2024: 131 · 2025: 239 · 2026-H1: 41.
   - The archive's first-file day equals `onboardDate` for 547 of 640 symbols, and is one day later for 64.
   - Listings cluster. Distinct listing weeks per year: 29 / 32 / 20 / 40 / 39 / 51 / 21. Up to 11 listings in one week (2025).
2. **First-hour data is complete on Binance.**
   - All 671 listings have every 1-minute kline of the first hour (60 of 60 rows) and 1-minute klines for 14 of 14 days, except 5 near archive edges.
   - Each listing's month(s) has 1h klines, premium index and monthly funding files.
   - 5-minute metrics (OI and long/short ratios) start within 5 minutes of the first trade for every listing from 2021-12 on. They are on 14 of 14 days for 24, 96, 122 and 227 listings in 2022–2025.
   - bookDepth exists from 2023: day 0 is covered for 99, 131, 215 and 41 listings in 2023–2026.
   - The full 14-day 1m history costs about 1.0 GB; the listing-day pull was 63 MB.
3. **Hyperliquid is thin and cannot be reconstructed intraday.**
   - The meta snapshot holds 230 perps with a daily candle before the cutoff. The first-candle calendar runs:
     - 54 left-censored at or before 2022-11-01;
     - 2023: 63 · 2024: 59 · 2025: 51 · 2026-H1: 3.
   - The candle API serves only the latest 5,000 candles. 1h/15m/1m probes for 2024-01, 2025-06, 2025-12 and 2026-06 returned **0 rows**, so first-hour HL bars exist only in our local every-account fills.
     - That archive covers 23 HL listings (2025-07-28 → 2026-06-16), which is the HL discovery year at 16 trials.
   - Hourly funding from listing exists for 136 HL coins (from 2023-05).
   - A pre-launch proxy (HL lists at least 2 days before Binance's perp) catches 30 listings. Another 15 HL coins never got a Binance perp. Hyperps cannot be flagged from the API.
4. **The owner's venues on Binance day 1** (by first-candle date):
   - HL had the coin by Binance day 1 for 43 / 56 / 45 / 4 of the 99 / 131 / 239 / 41 Binance listings in 2023 / 2024 / 2025 / 2026-H1.
   - Bitget had it for at least 27 / 66 / 143 / 31.
   - Either venue: 51 / 89 / 156 / 32.
   - Bitget does not serve history for symbols it has removed (codes 40309 and 40034), so 72 / 65 / 96 / 10 cases are **unknown**.
5. **Spot before perp on Binance is the regime that has disappeared.**
   - Spot was trading the day before the perp for 76/77 listings in 2020 and 77/99 in 2023.
   - That fell to 46/131 in 2024, 15/239 in 2025 and 0/41 in 2026-H1.
   - In 2025, 133 of 239 perps had **no Binance spot pair at all**, and 44 got spot later.
   - The event population therefore changes in kind after 2024: perp-first, Alpha and pre-market tokens.
6. **Costs and shortability.**
   - Taker fees: Binance 5 bp, Bitget 6 bp, HL 4.5 bp. Maker fees: Binance 2 bp, Bitget 2 bp, HL 1.5 bp.
   - New Binance perps settle funding **every 4 h with a ±2% cap**. From the archive, the first settlement was 4 h for 109/130 listings in 2024, 214/239 in 2025 and 37/37 in 2026, against 8 h before 2023-H2.
   - When a rate hits the cap, Binance can move a contract to 1 h settlement. The worst case for one position is therefore ±2% per hour.
   - HL funding is hourly with a 4%/h cap. Hyperps sample the premium at 1% weight.
   - Capacity at listing (median, p10–p90):
     - first-hour quote volume: $10 M (2023), $11 M (2024), $33 M (2025); p10–p90 is $7–160 M in 2025;
     - day-0 volume: $74 M, $112 M, $177 M;
     - OI after one hour: $1.6 M, $1.8 M, $3.1 M.
   - Binance pre-market perps are capped at about $200k notional at 5x.
7. **Power is not the binding constraint.**
   - Non-event small-cap coin-weeks have a 7-day log-return SD of 13.3% raw and 8.3% market-excess. Over 14 days: 18.8% and 11.1%.
   - Holdout A (2024-01 → 2025-07-27) has 260 events in 67 weeks. **MDE per event at 80% power, 7-day excess:**
     - 1.4% if events are iid;
     - 2.8% if clustered by week;
     - 5.6% if listing windows are twice as volatile.
8. **Announcements.**
   - Keyless, timestamped first-party archives exist: Binance CMS catalog 48 from 2017-07 and the Upbit announcements API from 2017-10.
   - **Neither accepts an end date**, so they were not queried here; the Upbit API also returns an edit history field (`first_listed_at`).
   - Wayback snapshots (end-dated) are dense only for Upbit notice pages.
   - Coinbase has no free archive.
9. **Unlocks have no free point-in-time calendar.**
   - DefiLlama emissions is Pro-only; Tokenomist, CryptoRank and Messari are paid.
   - The open-source `DefiLlama/emissions-adapters` repo is now 404. A public fork, with history ending 2026-03-21, holds 310 adapters.
   - Those schedules are **backfilled**. The median adapter was first committed 308 days after its earliest schedule date, and 46% more than a year after.
   - Only 19 / 43 / 44 / 21 schedule anchor dates in 2023 / 24 / 25 / 26-H1 were in the calendar before they happened.
10. **Literature has already read the 2023–2026 listing calendars.** Published 2024–2026 studies report steep post-listing declines for new tokens, Binance included (§D). No study of new-perp funding was found. An effect is likely; the open question is whether it can be captured net of funding, fees and squeezes.

---

## A. Event sets

### A1. Binance USDⓈ-M perp listings

**Source.** The listings are built from the L4 scout's S3 key listings (`data/scout_l4/listings/`), reused, plus today's exchangeInfo snapshot. Keys dated on or after 2026-07-01 were dropped at load.

**Definitions** (`n3_binance_events.py`):

| term | rule |
|---|---|
| universe | names matching `^[A-Z0-9]+USDT$`, minus TradFi and non-COIN contracts |
| listing | the first daily 1d-kline file |
| delisting | the last full (≥ 4 KB) metrics day, if it falls before 2026-06-01 (L4 rule) |
| relisting | a full-metrics segment that starts after a hole of more than 7 days |

| year | listings | distinct ISO weeks | max per week | relistings | delistings (rule; coverage fact only) |
|---|---|---|---|---|---|
| 2020 | 77 | 29 | 9 | 0 | 0 |
| 2021 | 58 | 32 | 5 | 0 | 1 |
| 2022 | 26 | 20 | 3 | 2 | 14 |
| 2023 | 99 | 40 | 7 | 2 | 3 |
| 2024 | 131 | 39 | 9 | 0 | 30 |
| 2025 | 239 | 51 | 11 | 8 | 46 |
| 2026-H1 | 41 | 21 | 5 | 1 | 47 |

- 674 universe symbols. Three (BTC, ETH, BCH) are left-censored at the archive's start, 2019-12-31.
- Delistings before 2021-12 cannot be read from the listing (L4 §B).

**Point-in-time quality**

- Archive first day minus `onboardDate`:

  | first day vs onboardDate | listings |
  |---|---|
  | same day | 547 |
  | +1 day | 64 |
  | +2 to +7 days | 13 |
  | more than 7 days later | 5 |
  | earlier | 11 |
  | symbol absent from exchangeInfo | 31 |

- The "earlier" cases are relisted or reused tickers, where `onboardDate` was reset (CTK, CVC, LIT, TLM, SLP, CVX, MAVIA, BNX, AIA, ICP, PUMP).
- **Ticker reuse is real.** PUMPUSDT 2025-04 and the pump.fun PUMP of 2025-07 are different tokens. A registration must key events on the segment, not the ticker.
- The listing *time* comes from the first 1m kline. `onboardDate` time-of-day clusters at 07:00 UTC (122 cases; probably a placeholder) and at 11:30–14:00. The first trade falls mostly at 12–14 UTC in 2023–24 and at 09–12 UTC in 2025.
- **Binance pre-market perps** (from 2025-04: BABY, INIT, SXT, NEWT, PUMP, XPL, WLFI, LINEA, 0G, MON …, per the docs helper) enter this calendar on their pre-market start day. The later conversion to a normal perp is a second event that is **not** visible in the archive. It is announced only in CMS articles.

### A2. Hyperliquid (`n3_hl_events.py`; meta snapshot of 2026-10-12, 234 main-dex perps, 56 flagged delisted)

| first 1d candle | listings | distinct weeks |
|---|---|---|
| ≤ 2022-11-01 (censored at query start) | 54 | — |
| 2023 | 63 | 34 |
| 2024 | 59 | 34 |
| 2025 | 51 | 31 |
| 2026-H1 | 3 | 3 |

- 4 meta coins have no candle before 2026-07-01. They were not named or used.
- HL's own calendar is small, about 50–60 a year. 2026-H1 is almost empty on the main dex; HIP-3 builder dexes are excluded.
- **Order against Binance**, for the 152 HL listings after 2023-06:

  | HL listing vs Binance perp | listings |
  |---|---|
  | within ±1 day | 103 |
  | HL at least 2 days *before* (pre-launch proxy) | 30 |
  | HL at least 2 days after | 4 |
  | no Binance USDT perp by 2026-06-30 | 15 |

- **Hyperps cannot be identified from the API.** There is no flag in `meta`, and the oracle method is not exposed.
  - The 30 "HL first" coins are the proxy.
  - The docs name ZRO (the first hyperp, around 2023-09), TIA, PYTH, JUP, W, STRK, PUMP-USD and XPL.
  - A conversion date ("shortly after spot lists on Binance/OKX/Bybit") is not in any API.
- The meta snapshot is today's. Coins removed from `meta` entirely, if any, are missing.

### A3. Bitget (`n3_bitget.py`)

- **The `bg-first-day` cache** has 168 files, counted before anything was printed. Values ≥ 2026-07-01 were dropped at load: 1 dropped, 167 kept. By year: 2019: 7 · 2020: 8 · 2021: 14 · 2022: 15 · 2023: 28 · 2024: 50 · 2025: 39 · 2026: 6.
  - It holds only the symbols earlier copy-trade work touched. It is **not a calendar**.
- **Today's `/api/v2/mix/market/contracts`** has 827 USDT-M perps. `launchTime` is empty for all. `openTime` gives 538 values before the cutoff, by year 2023: 12 · 2024: 120 · 2025: 161 · 2026: 245.
  - 169 values at or after the cutoff were dropped and only counted.
  - The 2026 bulge shows `openTime` is a re-open time, not the listing date: **unusable**.
- **Bitget has no free point-in-time listing calendar.**
  - Per-symbol date-bounded first-candle probes work, but only for live symbols. Removed symbols return 40309 or 40034, which is survivorship.
  - The day-1 check (§A5) uses these probes.

### A4. Listing announcements (timestamps)

| source | keyless? | history | end-date param? | notes |
|---|---|---|---|---|
| Binance CMS `bapi/composite/v1/public/cms/article/list/query?catalogId=48` | yes | 2017-07 → today, `releaseDate` in ms (docs helper) | **no**; newest-first pages | Not queried here (cannot be end-dated). Needs an owner-approved drop-at-load fetcher, or a ledger exposure. Titles carry effective dates, and releaseDate can differ from the title |
| Upbit `api-manager.upbit.com/api/v1/announcements?category=trade` | yes | 2017-10 → today, `listed_at` + `first_listed_at` (edits detectable) | **no** | same issue |
| Wayback CDX, end-dated `to=20260630` | yes | Upbit notice pages: 9,418 snapshots (2022: 2,689; 2025: 2,495); Binance listing-category page: 38 (2020–22); Binance listing-article paths: 794 (mostly 2022); CMS API URL: 29; CoinbaseAssets X: 6,353 (mostly 2022–23) | yes | Snapshot time is not announcement time; a page's own posted time must be parsed. Too sparse for Binance |
| Coinbase | — | X posts (@CoinbaseAssets / @CoinbaseMarkets) and the blog | — | no free, keyless, timestamped archive found |

**The point-in-time quality is good once fetched,** because these are first-party publish stamps. The obstacles are the end-date rule and latency: announcement reactions run in seconds to minutes (§D, items 10 and 13: insider run-ups before announcements).

### A5. Day-1 availability on the owner's venues (`n3_crossvenue.py`)

| Binance listing year | n | HL by day 1 | Bitget by day 1 | Bitget listed before Binance day 0 | either by day 1 | Bitget unknown (symbol removed) |
|---|---|---|---|---|---|---|
| 2023 | 99 | 43 | 27 | 5 | 51 | 72 |
| 2024 | 131 | 56 | 66 | 19 | 89 | 65 |
| 2025 | 239 | 45 | 143 | 35 | 156 | 96 |
| 2026-H1 | 41 | 4 | 31 | 5 | 32 | 10 |

Binance itself works for the owner (legality is a warning line only), so every Binance event is tradable on its own venue.

### A6. Token unlocks (`n3_unlocks.py`)

**Free sources.**

| source | status |
|---|---|
| DefiLlama `/api/emissions`, `/api/emission/{protocol}` | Pro list only (verified in `llms-pro.txt` vs `llms-free.txt`) |
| Tokenomist | API from $69/mo; free view shows 3 past and 1 future event |
| CryptoRank | Pro |
| Messari | Enterprise |
| **`DefiLlama/emissions-adapters`** | GitHub 404 (web and API) |

Under the rules here, none of the paid or keyed sources is usable.

**The fork.** A public fork, `Omni-Chain-Protocols/emissions-adapters`, was cloned with `--no-checkout` and hooks off. Facts:
- 1,116 commits, 2023-01-27 → 2026-03-21, **0 commits on or after 2026-07-01**;
- authors are DefiLlama maintainers (waynebruce0x 583, 0xngmi 54 …);
- HEAD is `fa7d0a0a`.

**What the fork contains.**
- 310 adapter files at HEAD. 317 were ever added and 12 were deleted at some point. That deletion is itself survivorship.
- Adapters added per quarter: 2023-Q1 45, Q2 41, Q3 27, Q4 17; 2024-Q1 9, Q2 37, Q3 10, Q4 7; 2025-Q1 10, Q2 75, Q3 3, Q4 3; 2026-Q1 33.
- Encoding: `manualCliff` in 269 files, `manualLinear` in 229, `manualStep` in 97, on-chain balance adapters in 50. **Events exist only after executing the TypeScript.** Untrusted downloaded code was not run. A registration would need either a pinned, reviewed run in a sandbox, or a hand-parse.

**Point-in-time quality** (literal schedule dates before the cutoff, compared with the adapter's first commit):
- 277 of 288 adapters were committed **after** their earliest schedule date.
- Lag p10/p25/p50/p75/p90 = 11 / 81 / 308 / 865 / 1,471 days. 46% were committed more than a year after.
- Only 1.7% were recorded at least 30 days before vesting began.
- Distinct anchor dates that were already in the calendar before they occurred: 2023: 19 · 2024: 43 · 2025: 44 · 2026-H1: 21. Against those, anchor dates backfilled after the fact: 96 / 62 / 43 / 1.

**Verdict.**
- Schedules are usually public in token docs at the token generation event (TGE), so they are "known in advance" in principle.
- But the only free machine-readable calendar was built after the fact, for tokens that stayed relevant, with retro-edits.
- A clean test would need a calendar snapshotted before each event. For free, that exists only for about 20–45 anchor dates a year, across about 300 tokens.

## B. Data coverage from the first hour (counts only)

| Binance listing year | n | 1m rows in first hour = 60 | 1m 14/14 days | metrics on day 0 (within 5 min of first trade) | metrics 14/14 | bookDepth day 0 / 14/14 | funding month file | 1h klines / premium 1h month |
|---|---|---|---|---|---|---|---|---|
| 2020 | 77 | 77 | 76 | 0 | 0 | 0 / 0 | 76 | 77 / 77 |
| 2021 | 58 | 58 | 58 | 3 | 3 | 0 / 0 | 56 | 58 / 58 |
| 2022 | 26 | 26 | 26 | 24 | 24 | 0 / 0 | 25 | 26 / 26 |
| 2023 | 99 | 99 | 99 | 96 | 96 | 99 / 90 | 99 | 99 / 99 |
| 2024 | 131 | 131 | 131 | 122 | 122 | 131 / 129 | 130 | 131 / 130 |
| 2025 | 239 | 239 | 238 | 227 | 227 | 215 / 215 | 239 | 239 / 239 |
| 2026-H1 | 41 | 41 | 37* | 40 | 36* | 41 / 36* | 37* | 37 / 36* |

\* Listings in the last 13 days of June 2026 run into the cutoff.

**Notes.**
- Relistings: metrics 14/14 for 12 of 13.
- Monthly funding files hold every settlement (`calc_time`, `funding_interval_hours`, rate). The first funding row is 4 h or less after the first trade for most 2024–26 listings. But 238 of 671 files have a row stamped **before** the first 1m kline, which may come from contract onboarding before trading opens. The loader must cut at the first trade.
- **Sizes for a registration pull:**

  | pull | size |
  |---|---|
  | 1m klines for days 0–13, all events (listed keys) | 1.03 GB |
  | bookDepth for the listing months | 5.9 GB (only day 0 to day 13 would be needed; about 1/2 of that) |
  | listing-day pull done here | 63 MB, 3,187 zips, all checksums verified |

  The 1m pull (≈1.03 GB) is just over the ~1 GB line in the brief; it needs the owner's approval or a split by period.

**Hyperliquid.**
- Daily candles back to listing exist. 1h, 15m and 1m candles exist only for the latest 5,000 bars, so **no free first-hour bars for any HL listing before about 2026-03**.
- Hourly funding (`fundingHistory`) covers at least 95% of hours from the first funding row for 136 coins: 2023: 33, 2024: 52, 2025: 48, 2026: 3. There is none before 2023-05-12.
- 102 coins have a first funding row within 24 h of the first candle. 22 take 1–7 days and 14 take more than 7 days; these are probably early-2023 coins and candle-before-funding artefacts.
- First-hour HL tape exists only in our local fills: 23 listings, 2025-07-28 → 2026-06-16, which is the **HL discovery year (16 trials)**.

**Bitget.** No bounded free funding history (L2 §A). Klines exist only for live symbols.

## C. Costs, shortability and capacity

| item | Binance USDⓈ-M | Hyperliquid | Bitget |
|---|---|---|---|
| fees, base tier (maker/taker) | 2 / 5 bp (BNB −10%) | 1.5 / 4.5 bp | 2 / 6 bp (contracts snapshot: 0.0002/0.0006 on all 827) |
| funding interval at listing | **4 h** for new perps: archive first settlement 4 h for 22/99 (2023), 109/130 (2024), 214/239 (2025), 37/37 (2026); 8 h earlier | 1 h | 4 h for new perps (TGTUSDT example); the snapshot shows 378 at 4 h, 448 at 8 h |
| funding cap | ±2% per settlement for non-major perps; moves to 1 h settlement when capped, so the worst case is ±2%/h; ±3% for ≤ 25x contracts from 2023-10 (secondary); pre-market perps: fixed +0.005%/4h from about 2025-10 | 4%/h; hyperps sample the premium at 1% weight | not stated in notices |
| initial max leverage | 20x–75x at listing; pre-market 5x with notional tiers to **about $200k** | new listings 3x–5x, often isolated-only; hyperps launched with about a $1M OI cap (secondary); 3–40x tiers | 20x typical (25–75x reported) |
| ADL / backstop | insurance-fund pools, then ADL ranked by PnL% × leverage | ADL when an account goes negative; HLP is the liquidation backstop | — |

**Shortability.** Every perp is shortable from its first minute. No borrow is needed; funding is the cost of carry. Day-1 availability on the owner's venues is in §A5. Binance itself carries 100% of its own events.

**Capacity (contemporaneous levels at listing; no outcome).**

| year | first-hour quote vol, $M (p10 / p50 / p90) | day-0 quote vol, $M | OI value at end of first hour, $M |
|---|---|---|---|
| 2020 | 0.19 / 1.0 / 7.2 | 3.0 / 11.6 / 112 | — |
| 2021 | 0.59 / 1.9 / 8.6 | 6.7 / 40 / 310 | (n=3) |
| 2022 | 0.40 / 8.8 / 100 | 6.6 / 157 / 929 | 0.05 / 1.2 / 3.7 |
| 2023 | 1.8 / 10.0 / 49 | 9.5 / 74 / 666 | 0.31 / 1.6 / 7.7 |
| 2024 | 3.9 / 11.2 / 62 | 24 / 112 / 699 | 0.67 / 1.8 / 8.3 |
| 2025 | 6.7 / 32.7 / 160 | 47 / 177 / 874 | 0.96 / 3.1 / 13.0 |
| 2026-H1 | 5.2 / 15.7 / 38.7 | 22 / 85 / 260 | 0.72 / 1.9 / 3.4 |

**Per-event capacity sketch.**
- At 1% of day-0 volume: about $0.7–1.8 M (medians 2023–25).
- At 5% of first-hour OI: about $80–160k.
- Hard caps:
  - Binance pre-market: about $200k notional;
  - HL new listings: 3–5x and OI caps.
- **A realistic size is $50–250k per event on Binance, and less on HL.** It is fine for the owner's size.
- Costs:
  - round trip on Binance: 10 bp taker / 4 bp maker in fees;
  - spread and impact at listing: not measured, but measurable without outcomes from bookDepth (2023 → 486 listings) and 1m taker volume;
  - **funding at ±2% per 4 h (or per hour) dominates everything else** in a squeeze.

## D. Literature (helper-verified; "V" = page opened, "P" = partly)

| study | sample period | events / venues | finding (authors' terms) |
|---|---|---|---|
| Momtaz, *Eur. J. Finance* 2021 (V) | 2013 → 2018-04 | about 1,400 ICO tokens | first-day pop 15% mean; median ICO −30% over 1–24 months |
| Animoca Brands Research (V via CryptoSlate), 2024-10 | 2024-01 → 2024-09 | 773 CEX listings (Binance 44, Bitget 339, Bybit 155 …) | median −40% to −70%; Binance median about −50% |
| CryptoNinjas × Storible (V), 2025-03 | 2024 listings, priced to 2025-02 | 389 tokens, 6 CEXs | +54% average pump, then −52%; Binance 98% below peak |
| Memento Research (V), 2025-12 | 2025 TGEs | 118 | about 85% below launch; median about −70% |
| Four Pillars (P), 2026 | 2025 listings, to 2026-03-11 | Upbit 59, Bithumb, Binance | Upbit median −81%; Binance 2025 about −72% |
| FMZ Quant blog (V), 2023-11 | 2023 Binance new **perps**, first 150 days | 86 | steady decline, no rebound; no funding analysis |
| flow, X post (V via ForkLog), 2024-05 | about 2023-11 → 2024-05 | Binance listings | more than 80% below the first-day price |
| Benedetti & Nikbakht, *JCF* 2021 (V) | before 2019 | 3,625 tokens, 108 venues | +16% around the first cross-listing |
| Ante 2019 working paper (V) | before 2019-09 | 327 listings, 22 exchanges | CAR +9.2% on days −3..+3; pre-run on days −3..−2 |
| Ante & Meyer, *DEF* 2021 (V) | before 2020 | 250 cross-listings | +6.5% on day 0 |
| Li, Luo, Wang & Wei, AFT 2025 (V, abstract) | not stated | Coinbase vs Binance plus about 80 venues | positive short-run; larger on Coinbase |
| Félez-Viñas, Johnson & Putniņš 2022 (V) | 2018-09 → 2022-05 | Coinbase announcements | pre-announcement run-ups; insider trading in 10–25% |
| Coin Metrics 2020 (V) | 2018 → 2020-Q2 | 16 Coinbase listings | small, −1% to +14% |
| The Tie, 2026-07-13 (V, **published after the cutoff**) | 2023-01 → **2026-06** | 1,844 listing events, 7 venues | pre-day-0 run-up that reverses within about 2 weeks; shrinking since 2023 |
| RockawayX (V) | 2024 | Binance, Bybit, MEXC | +7d positive, 30/60d not significant |
| Keyrock 2024-12 (V) | about 2022–2023 | 16,000+ unlocks, 40 tokens | about 90% negative pressure, from −30d to +14d; team unlocks worst |
| The Tie 2023 (V) | 2016 → 2023 | 350k unlock events | declines before unlocks |
| 6th Man Ventures 2023 (V) | to 2023-04 | 5,000 unlocks, 20 protocols | median −5% after unlocks above 1% of supply |
| Kim, SSRN 6632838 (P) | 2023 → 2025 | 52 Binance unlocks | 72 h mean −17% |
| Streamflow 2026-09-02 (V, aggregator, **published after the cutoff**) | Tokenomist data plus The Tie data **through 2026-08-19** | 236 Tokenomist events | median −16% vs BTC over one month; no effect under 2% of supply |
| new-perp funding / pre-launch perps | — | — | **no study found**; this looks like an open gap |

Dropped (unverified): Jump Crypto 2022, a Messari simulation of 92 Binance 2025 listings, CryptoRank on 113 TGEs, Messari's paywalled cliff-unlock study.

**Which calendars are already "read" by the literature (for priors):**
- Binance and CEX listings for 2023-H2 → 2026-06 are covered in returns terms: The Tie to 2026-06; Animoca, CryptoNinjas and RockawayX for 2024; Memento and Four Pillars for 2025.
- Unlocks: 2016 → 2025, and through 2026-08 via Streamflow.

**N3's direction is public knowledge.** The decisive questions are (a) funding cost, (b) squeeze tails, (c) fees and impact. None of those is published for perps.

## E. Sample and power (`n3_power.py`)

**Baseline.** The baseline uses non-event coin-weeks from the 009 discovery panel, 2021-12-06 → 2024-01-07, Monday starts. The rules:
- the start must be at least 30 days after the symbol's first kline;
- the end must be at least 7 days before the end of its alive segment;
- BTC and ETH are out;
- the top quartile by trailing 30-day volume is out.

Only dispersion was computed.

| horizon | coin-weeks | SD raw | 1.4826·MAD raw | SD excess (minus weekly cross-sectional median) | 1.4826·MAD excess | excess kurtosis (raw / excess) |
|---|---|---|---|---|---|---|
| 7 d | 12,334 (108 starts) | 0.133 | 0.099 | 0.083 | 0.048 | 3.4 / 16.2 |
| 14 d | 6,114 (54 starts) | 0.188 | 0.151 | 0.111 | 0.070 | 2.3 / 13.5 |

**Minimum detectable mean per event.** Two-sided 5%, 80% power: MDE = 2.80·σ/√N. N is either the events (iid) or the distinct listing weeks (fully clustered). The "×2" column assumes listing windows are twice as volatile as arbitrary coin-weeks, which is likely; it is not measured, because that would read event windows.

| calendar | events / weeks | 7 d excess: iid / clustered / ×2 clustered | 14 d excess | 7 d raw clustered / ×2 |
|---|---|---|---|---|
| 2020-01 → 2023-12 | 260 / 121 | 1.4% / 2.1% / 4.2% | 1.9% / 2.8% / 5.7% | 3.4% / 6.8% |
| 2021-12 → 2023-12 (009 discovery) | 128 / 62 | 2.0% / 2.9% / 5.9% | 2.8% / 4.0% / 7.9% | 4.7% / 9.4% |
| **2024-01 → 2025-07-27 (holdout A)** | **260 / 67** | **1.4% / 2.8% / 5.6%** | 1.9% / 3.8% / 7.6% | 4.5% / 9.1% |
| 2025 alone | 239 / 51 | 1.5% / 3.2% / 6.5% | 2.0% / 4.4% / 8.7% | 5.2% / 10.4% |
| 2025-07-28 → 2026-06-30 | 151 / 43 | (about 1.9% / 3.5% / 7.0%) | — | — |

**Reading the MDE.**
- A per-event edge of 4–8% (excess, 7–14 d) is detectable on holdout A even under clustering and doubled volatility.
- Costs are about 0.1–0.3% in fees and impact, plus funding.
- Fat tails (excess kurtosis 13–16) argue for **trimmed or robust tests (t on winsorized returns or on per-week means) and a cap on single-event weight**. They also argue for a tail statistic that is pre-registered, not inferred.
- HL alone (about 50 events a year, 23 in the archive year) has roughly 2.5× the MDE and cannot carry a test.

## F. Proposed split (decided now, before any outcome)

| segment | Binance events / weeks | role | sharing |
|---|---|---|---|
| 2020-01-02 → 2023-12-31 | 260 / 121 (metrics only for 123 events from 2021-12) | **discovery** | Overlaps 009's discovery calendar (2021-12 → 2023-12). But 009's universe required ≥ 30 days since listing, so **listing windows (days 0–29) never entered 009's book.** The bars sit in `data/binance_years` and must be whitelisted. 010's discovery (2023-06 → 2024-12) read cross-venue funding spreads with no listing-age conditioning (000f). Funding-based N3 designs share that read in aggregate |
| 2024-01-01 → 2025-07-27 | 260 / 67 (253 with 14-d room before 2025-07-28) | **decision holdout** | 009 holdout A: closed, released, unread by any outcome (009 closed in discovery; D17 is satisfied). 012 read HL–CEX funding on coins listed ≥ 7 days in 2025-01 → 2026-06. Funding-extreme designs on days 7+ share that, in aggregate. The literature has read this calendar in return terms |
| 2025-07-28 → 2026-06-30 | 151 / 43 Binance; 23 HL | **confirmatory / second holdout (Binance only)** | Binance data is L4's holdout B, unread. HL data is the HL discovery year (16 trials, family counted). Use for HL execution checks only |
| 2026-07-01 → | — | sealed | untouched |

**Recommendation.**
- Discover on 2020–2023 (260 events).
- Decide on holdout A (260 events, 67 weeks), using t_{66} on weekly event means, then the economic bar.
- Keep 2025-07-28 → 2026-06 as a Binance-only confirmatory block.
- Embargo: drop events whose horizon crosses a split boundary.
- Because the regime changed (spot before perp in 77% of 2020–23 listings vs 6% in 2025), a registration should pre-declare **spot-first vs perp-first** as a stratifier. Spot-first status is known at listing time.
- Pre-market perps (2025-04 →) should be either excluded or their own stratum.

## G. Tail statement (public examples only, not our windows)

**A short-the-listing book has an unbounded squeeze tail.** Public cases:

| case | what happened | venue response |
|---|---|---|
| **XPL hyperp on HL**, 2025-08-26/27 | about 2.5× in minutes against Binance pre-market's about $0.55 peak; more than $17 M of mostly short liquidations, which went from the book to ADL | caps on hyperp marks (10× EMA, later 3×/1.5× external) |
| **MYX**, Binance/Bitget, 2025-09 | about +1,400% over about 5 days; $89.5 M of shorts liquidated; OI above $400 M | none reported |
| **ALPACA**, Binance delisting squeeze, 2025-04 | +600% to 2,300%; funding moved to 1 h settlement | delisting already announced |
| **JELLY**, HL, 2025-03 | HLP short about $12 M underwater | delisting by validator vote, settlement at a chosen price |
| **POPCAT**, HL, 2025-11 | $4.9 M of HLP bad debt | — |

**How a test must cost the tail.**

1. **Gap-through stops.** A stop at +X% fills at the worst 1m high after the trigger, not at the stop. Pre-register a gap multiplier, and test sensitivity at the observed 1m high plus impact from bookDepth.
2. **Funding at the cap.** Charge every settlement at its archived value. Stress at ±2% per hour for the squeeze's duration (Binance switches capped contracts to 1 h).
3. **Isolated margin, 1–2x, a hard notional cap per event**, and a portfolio cap per listing-week. Listings cluster at up to 11 a week, and a market-wide alt squeeze hits all of them at once.
4. **Venue actions.**
   - Delisting settlement at a venue-chosen price (JELLY), which can be adverse.
   - ADL truncates winners on the profitable side, so the upside is capped while the squeeze downside is not.
   - Pre-market caps (about $200k).
5. **Tail statistics to pre-register:**
   - worst event (multiple of the stake);
   - worst listing-week and worst month of the book at the planned size;
   - a fractional-Kelly size that keeps the worst observed and the worst stressed month above −30%.
6. **A long-the-listing design** (convex) has a bounded loss per event but pays funding when crowded long, at the same caps.

## H. Reads list for the ledger

**Date bounds.**
- **Binance archive.**
  - Reused the L4 key listings; keys ≥ 2026-07-01 were dropped at load.
  - New prefix listings (`binance_vision_listing/`, 143,660 keys) were filtered in memory to dates < 2026-07-01 before counting. Post-cutoff keys were not even counted.
  - Downloads (`binance_vision_pull1/`): 3,187 zips, 63 MB, all dated ≤ 2026-06-30, every one checksum-verified. They hold listing-day 1m and 1d klines, listing-day metrics and listing-month fundingRate.
  - **Fields parsed:**

    | file | fields parsed |
    |---|---|
    | 1m klines | open time and quote volume; the day-1 file only to bound the first hour |
    | 1d kline | day-0 quote volume |
    | metrics | timestamps and OI value; all of day 0 was loaded into memory, and only the last stamp inside the first hour was used |
    | fundingRate | the first row's `calc_time` and `funding_interval_hours` |

  - **No price, return or funding-rate value was parsed.**
- **009 panel (`data/binance_years/panel`, 2021-11 → 2024-01-07).** Non-event coin-week dispersion only. Listing windows (first 30 days) and the 7 days before a delisting were excluded.
- **Hyperliquid.**
  - `meta`: a current snapshot. 4 coins listed at or after the cutoff were not named or used.
  - `candleSnapshot` 1d with endTime 2026-06-30 23:59:59.999: only open times were kept.
  - `fundingHistory`: up to 14 days from listing, end ≤ 2026-06-30. Only the row count and first time were kept.
  - 6 intraday probes, all before the cutoff: row counts only.
- **Bitget.**
  - Contracts: a current snapshot. 169 `openTime` values ≥ cutoff were dropped and counted.
  - `bg-first-day` cache: 1 value ≥ cutoff dropped and counted, never printed.
  - `history-candles` with endTime ≤ the Binance listing day + 2 d (< cutoff): only the existence and open time of one candle per symbol were kept.
- **Wayback CDX:** `to=20260630`, timestamps only.
- **GitHub.**
  - The DefiLlama org repo list is current metadata.
  - The fork's history ends 2026-03-21.
  - Literal schedule dates ≥ 2026-07-01 inside adapter texts were dropped uncounted.
  - TypeScript was never executed.
- **exchangeInfo:** the L4 snapshot (2026-10-11). `onboardDate` ≥ 2026-07-01 was dropped.

**Flags, for the orchestrator to ledger** (all from the two web desk-research helpers, not from scripts):
1. **Announcement calendar after the cutoff seen.** The docs helper scanned the Binance CMS catalog 48 newest-first and read article titles and dates after 2026-07-01:
   - a CTUSDT listing notice dated 2026-10-01;
   - "Pre-IPO" perp titles (OURA, MOONSHOT, Sep 2026);
   - the catalog total of 2,280 articles as of today.

   It also read the Upbit announcements API total as of today (788 trade notices). These are listing-calendar facts inside the sealed window, with no market outcome. **Any N3 announcement test must treat the 2026-07 → Binance and Upbit announcement calendar as partly seen (titles).**
2. **Literature published after the cutoff:**
   - The Tie, 2026-07-13: listing study with a sample to 2026-06, qualitative.
   - The Tie, 2026-07-30: 14 tokens, median about +7% BTC-adjusted.
   - Streamflow, 2026-09-02: unlock aggregates that include **The Tie data through 2026-08-19**, so some post-cutoff unlock outcomes are seen **in aggregate and qualitatively**: "effect concentrated in thin-float tokens; under 2% of supply shows none". That is an unlock-outcome statement partly covering the sealed window. Ledger it.
3. **Published outcome statistics** (not computed by us) exist for the 2024–2026 listing calendar (§D). The 2024-01 → 2025-07 holdout is fresh to the programme's computations, not to public priors.
4. Scripts saw **no** price, return, funding or OI observation after a listing's first hour, other than the allowed day-0 volume level and coverage row counts.

**Forbidden items (each respected):**
- No post-event return path, funding value or OI after the first hour; no means, medians or hit rates of post-event moves.
- No event count split by a future quantity. Delisting counts are by delisting year only and are never crossed with listings. The Bitget "unknown" column reflects API survivorship, not any outcome.
- No script read data dated ≥ 2026-07-01; every API query was end-dated.
  - Two first-party announcement APIs that cannot be end-dated were therefore not queried by the scripts.
  - The helpers' reads are flagged above.
- No paid, keyed or requester-pays data. No single pull over 1 GB.
- Nothing touched in `lib/`, `pages/`, `vercel.json`, registrations, `results.jsonl` or other scouts' files (L4 listings were read only).
- No commits. Nothing published.

## I. Honest odds and capacity

| light | effect exists | deployable at our size, net | why |
|---|---|---|---|
| **(i) new perps: short or long by funding and price path** | **about 1 in 2** | **about 1 in 4** | The post-listing decline is widely published (§D). Free first-hour data is complete on Binance for 671 events. Power is ample. The doubts are that funding at ±2%/4h (1 h when capped) may price the decline, that squeezes (MYX, XPL) dominate the left tail of a short book, and that the regime changed after 2024 (perp-first, no spot) |
| **(ii) listing announcements** | about 1 in 2 (documented; pre-runs) | **about 1 in 10** | A seconds-to-minutes latency race; insider pre-runs; Coinbase has no archive; Binance and Upbit APIs cannot be end-dated, and post-cutoff titles have already been seen |
| **(iii) token unlocks** | about 1 in 3 (documented, mostly small or thin-float) | **about 1 in 8** | No free point-in-time calendar. The open adapter calendar is backfilled (median lag 308 days), and events need executing untrusted code. Unlock outcomes through 2026-08 are partly seen via Streamflow |

**Capacity per event** (Binance, 2023–25 medians):
- first-hour volume: $10–33 M;
- day-0 volume: $74–177 M;
- OI after one hour: $1.6–3.1 M.

**Plausible size per event:** about $50–250k; pre-market perps are capped at about $200k. At 100–240 events a year, that is a book of $2–10 M of turnover a year at our scale, far above the owner's need.

**Next step if (i) is drafted:**
- pull the 1m klines for days 0–13 (1.03 GB; owner approval at the ~1 GB line), plus bookDepth for days 0–13 of 2023 → listings (about 3 GB), discovery calendar first;
- pre-register spot-first vs perp-first strata, gap-through stops, funding at archived values with a cap stress, and per-week caps.
