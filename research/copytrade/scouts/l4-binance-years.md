# L4 scout: free multi-year exchange data for cross-sectional factors

2026-10-11. Feasibility only. This scout used S3 listings (keys, sizes, dates), exchangeInfo,
REST probes that print only row counts and timestamps, and 8 sample files with 121 KB
zipped in total, all dated before 2026-07-01. **No return, correlation, factor statistic or
outcome of any kind was computed.** The only market value read was one month of BTCUSDT
quote volume, used to calibrate a liquidity proxy. The proxy was then rejected (see C).

Scripts: `research/copytrade/scouts/l4_binance_lister.py` (S3 lister), `l4_listing_stats.py`
(all tables below), `l4_sample_schema.py` (sample schemas), `l4_venue_probe.py` (REST depth
probes). Data: `research/copytrade/data/scout_l4/` (gitignored), with `listings/`, `samples/`,
`exchangeinfo/` and `stats_output.md`.

## Summary

1. **The cross-section starts on 2021-12-01, not in 2020.** Binance `metrics` (5-minute OI, top-trader
   and global long/short ratios, taker vol ratio) exists for BTCUSDT from 2020-09-01, but
   for the other 134 symbols only from 2021-12-01. Funding and klines start in 2020-01 for about 80 coins.
2. **The archive is survivorship-free, but the last file date is not the delisting date.** Dead
   contracts keep producing daily klines through 2026 and sparse metrics stubs. A listing-only
   rule, "last metrics file ≥ 4 KB", matches exchangeInfo `deliveryDate` for 117/120 delisted
   symbols (the other 3 are one day early). It identifies 141 delistings before 2026-06.
3. **Proposed split (decided now):** discovery 2021-12-06 → 2023-12-31, with 108 weeks, 17,374 coin-weeks
   and 161 coins/week on average. Holdout A is 2024-01-01 → 2025-07-27 (82 wk, 25,892 coin-wk). Holdout B is
   the HL year, 2025-07-28 → 2026-06-30 (47 wk, 24,325 coin-wk). The iid IC MDE at 80% power
   is ≈ 0.021, 0.017 and 0.018 respectively; under realistic noise (σ_IC 0.10–0.15) it is ≈ 0.03–0.04.
4. **Pull size:** metrics + funding + 1h klines is 1.54 GB (132k zips) for discovery and
   5.86 GB (516k zips) for all years. Both exceed the 1 GB approval line. Metrics is 91% of the bytes.
   Monday-only metrics files would cut discovery to 0.20 GB.
5. **There is no free multi-year liquidation record on any venue.** Bybit REST is the free second
   venue: OI and long/short at 5 min from 2020-08 (BTC), including delisted symbols. "Spirals
   across years" must be defined from OI collapses plus taker imbalance, not from liquidations.

---

## A. Binance USDⓈ-M datasets (data.binance.vision, listing only)

`univ` means crypto USDT perpetuals: the name matches `^[A-Z0-9]+USDT$`, minus the 217 `TRADIFI_PERPETUAL`
contracts and the INDEX contracts in today's exchangeInfo. Bytes are zipped, and each zip also has a
`.CHECKSUM`. "post" counts files dated ≥ 2026-07-01 from the listing. None were fetched. Monthly files are
dated by month, so 2026-06 is the last month before the cutoff.

| dataset | cadence (sample) | syms all / univ | first (univ) | last pre-cutoff | files univ pre | GB univ pre | GB all pre | files post | GB post |
|---|---|---|---|---|---|---|---|---|---|
| daily `metrics` | 5 min, 288 rows/day | 1034 / 679 | 2020-09-01 (BTC only); 134 syms 2021-12-01 | 2026-06-30 | 494,268 | 5.34 | 5.95 | 79,724 | 0.78 |
| monthly `fundingRate` | per settlement (8 h for BTC) | 988 / 681 | 2020-01 | 2026-06 | 18,562 | 0.019 | 0.021 | 2,272 | 0.002 |
| monthly `klines` 1h | 1 h | 1056 / 680 | 2020-01 | 2026-06 | 19,350 | 0.574 | 0.640 | 2,654 | 0.072 |
| monthly `klines` 1d | 1 d | 1056 / 680 | 2020-01 | 2026-06 | 19,350 | 0.033 | 0.037 | 2,654 | 0.004 |
| daily `klines` 1d | 1 d | 1061 / 681 | 2019-12-31 | 2026-06-30 | 577,714 | 0.193 | 0.216 | 87,922 | 0.030 |
| monthly `premiumIndexKlines` 1h / 1d | 1 h / 1 d | 982 / 680 | 2020-01 | 2026-06 | 18,590 each | 0.258 / 0.018 | 0.285 / 0.020 | 2,260 each | 0.029 / 0.002 |
| monthly `markPriceKlines` 1h / 1d | 1 h / 1 d | 1039 / 681 | 2020-01 | 2026-06 | 19,633 each | 0.305 / 0.020 | 0.347 / 0.023 | 2,652 each | 0.039 / 0.003 |
| monthly `indexPriceKlines` 1h / 1d | 1 h / 1 d | 979 / 681 | 2020-01 | 2026-06 | 19,210 each | 0.325 / 0.021 | 0.366 / 0.023 | 2,354 each | 0.039 / 0.003 |
| monthly `aggTrades` (reference) | tick | 1044 / 680 | 2020-01 | 2026-06 | 17,949 | 995 | 1,050 | 2,278 | 77.1 |

**The 1h/1d-only version, all univ symbols to 2026-06-30:**

- klines 1h + 1d: 0.61 GB, of which 1d alone is 0.033 GB.
- Adding premium, mark and index at 1h: 1.49 GB.
- Adding funding: +0.019 GB.
- Metrics comes only as 5-minute daily files, and those are the bulk: **5.34 GB univ**.

**Univ symbols with ≥ 1 file in the year:**

| dataset | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 (H1) |
|---|---|---|---|---|---|---|---|---|
| metrics | 0 | 1 | 136 | 161 | 257 | 384 | 598 | 612 |
| fundingRate | 0 | 80 | 136 | 159 | 251 | 382 | 610 | 582 |
| klines 1h/1d (monthly) | 0 | 80 | 137 | 161 | 251 | 382 | 607 | 647 |
| premiumIndexKlines | 0 | 80 | 137 | 162 | 249 | 372 | 612 | 577 |
| markPriceKlines | 0 | 80 | 137 | 163 | 251 | 382 | 622 | 651 |
| indexPriceKlines | 0 | 80 | 137 | 163 | 257 | 381 | 619 | 607 |

These counts include dead contracts whose files continue after delisting (see B), so they overstate
the live universe. That is especially true for the klines.

**Gaps** are counted inside each symbol's first..last span, before the cutoff:

| dataset | symbols | with a gap | missing units | max for one symbol |
|---|---|---|---|---|
| metrics, all files | 672 | 99 | 14,436 days | 873 days |
| metrics, full files only (≥ 4 KB, from 2021-12-01) | 671 | 53 | 3,304 days | 899 days (CVCUSDT: delisted and relisted) |
| fundingRate | 675 | 3 | 33 months | 27 |
| klines 1h / 1d, mark 1h / 1d | 674–675 | 0 | 0 | 0 |
| premiumIndexKlines | 674 | 4 | 33 months | 21 |
| indexPriceKlines | 675 | 2 | 6 months | 5 |

**Archive-wide holes:**

- No calendar day from 2021-12-01 to 2026-06-30 is without metrics.
- No day's univ file count drops more than 20% from the previous day's.
- The large single-symbol gaps are delist/relist cases: CVC, CVX, SLP, CTK, LIT, TLM and ICP.

**Sample schemas.** All samples are BTCUSDT, and every SHA-256 matched its `.CHECKSUM`.

- `metrics`: `create_time, symbol, sum_open_interest, sum_open_interest_value,
  count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio, count_long_short_ratio,
  sum_taker_long_short_vol_ratio`.
  - The column names are misleading. Per Binance's REST equivalents, they are: top-trader account
    ratio, top-trader position ratio, global account ratio, and taker buy/sell volume ratio.
  - The schema is identical in 2020-09-15 and 2026-06-15, and there are no empty cells.
  - **The 2020 file has every row written twice:** 576 rows over 288 stamps, all exact
    duplicates. Deduplicate on load.
  - The zip entries were regenerated (mtime 2026-03-18 for the 2020 file, 2026-08-11 for the
    2026-06 file), so checksums must be pinned at pull time.
- `fundingRate`: `calc_time, funding_interval_hours, last_funding_rate`. 2024-06 has 90 rows at 8 h.
- `klines` / `premiumIndexKlines` / `markPriceKlines` / `indexPriceKlines` share one 12-column schema: `open_time,
  open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume,
  taker_buy_quote_volume, ignore`. A 1h month has 720 rows with no gaps. `klines` carries the taker-buy
  volume, so **hourly taker imbalance is free from 2020-01**.
- There is no `fundingRate` in `daily/` (monthly only). Binance REST `/fapi/v1/fundingRate` returns
  BTCUSDT from 2019-09-10.

## B. Survivorship and point-in-time listing

| check | result |
|---|---|
| Delisted symbols kept in the archive? | **Yes.** 141 univ symbols have their last full metrics file before 2026-06-01. 27 of the symbols with no metrics after 2026-05 are **absent from today's exchangeInfo** |
| Delistings by year (rule below) | 2021: 1 · 2022: 14 · 2023: 3 · 2024: 30 · 2025: 46 · 2026-H1: 47 |
| Is the last file date the delisting date? | **No.** Daily 1d klines for dead contracts continue to 2026-06-30, with normal file sizes (FTTUSDT, delivered 2022-11-14, has 1d klines daily through 2026-10-09). Metrics leaves sparse stubs under 2.1 KB until batch stops (2024-07-15, 2025-03-10, 2025-06-05). Last-file date matches `deliveryDate` for 0/120 SETTLING symbols |
| Listing-only delisting rule: last metrics file ≥ 4,000 bytes | Equals `deliveryDate` for **117/120** SETTLING symbols and is the day before for 3/120. False alarms: 4 of 380,460 files of TRADING symbols are below 4 KB |
| Monthly last-file vs delivery month | fundingRate: same month 76/114, later 38. premiumIndex: 21 same, 32 next, 61 later. klines and mark: 0 same. Only funding is a rough marker |
| Listing date: first file vs exchangeInfo `onboardDate` | metrics (538 symbols onboarded after 2021-12-01): same day 479, +1 d 40, +2–7 d 8, earlier 11. 1d klines (647 after 2020-01-01): same day 554, +1 d 64, +2–7 d 13, >7 d 5, earlier 11 |
| exchangeInfo (fetched 2026-10-11, today's snapshot) | 924 symbols: 525 USDT perps TRADING, 133 SETTLING (delisted, still carrying `onboardDate`/`deliveryDate`), 217 TradFi perps (onboarded from 2025-12-11; they have metrics files and must be excluded), plus a few index contracts. Symbols delisted earlier have dropped out |

**Point-in-time reconstruction:**

- **Listing dates** come from the first file date, matching `onboardDate` within a day for 96% of symbols.
- **Delisting dates from 2021-12 onward** come from the metrics size rule, which agrees exactly with
  exchangeInfo where both exist.
- **Before 2021-12** (funding/klines-only years), delistings cannot be read from the listing: klines
  continue for dead contracts. They would need the kline contents (zero volume or count). That costs one
  monthly 1d file per symbol (0.033 GB), which was not pulled.
- **Any loader must cut each symbol at its delisting date.** Otherwise dead contracts enter the
  panel as zero-return, zero-volume coin-weeks.

## C. Universe per year (univ symbols with full metrics days, ≥ 4 KB files)

| period | min metrics days | any metrics | metrics ≥ min | … and aggTrades ≥ 10 MB/month | ≥ 50 MB/month | ≥ 200 MB/month | funding + 1d klines only (≥ 10 of 12 months) |
|---|---|---|---|---|---|---|---|
| 2020 | 300 | 1 | 0 | 0 | 0 | 0 | 24 |
| 2021 | 300 | 135 | 1 | 1 | 1 | 1 | 105 |
| 2022 | 300 | 160 | **127** | 126 | 31 | 2 | 132 |
| 2023 | 300 | 244 | **162** | 155 | 22 | 2 | 179 |
| 2024 | 300 | 372 | **237** | 226 | 70 | 9 | 262 |
| 2025 | 300 | 582 | **342** | 297 | 62 | 7 | 372 |
| 2026-H1 | 149 of 181 | 577 | **502** | 369 | 59 | 7 | 512 (≥ 5 of 6) |

**Liquidity, $10 M median daily volume: not determined.**

- A dollar proxy was tried. It used aggTrades monthly bytes (listing), calibrated on BTCUSDT 2024-06 at
  $366 B over 340 MB, which gives $1,078 per byte.
- It puts every qualifying symbol above $100 M/day. It is an upper bound, because alt trades are
  smaller than BTC's, so it cannot discriminate. The columns above are therefore activity tiers
  (trade count), not dollars.
- The true count needs `quote_volume` from monthly 1d klines for all univ symbols: 19,350 files,
  0.033 GB. That is well within the rules except for "a handful of samples", so it was not pulled. **It
  is the first cheap thing to pull** (pre-cutoff only, 2020-01 → 2026-06).

## D. Other venues: free multi-year OI, funding, long/short and liquidations

All REST probes carried an end time before 2026-07-01 and printed only counts and timestamps.

| venue / source | dataset | free history (probed) | granularity | delisted symbols? | notes |
|---|---|---|---|---|---|
| Binance REST `/futures/data/openInterestHist`, `topLongShortPositionRatio`, `globalLongShortAccountRatio`, `takerlongshortRatio` | OI, L/S, taker ratio | **~30 days only.** `startTime` 2026-06-01 and 2026-06-25 are both rejected (`-1130 startTime invalid`) | 5 m–1 d | — | The archive `metrics` is the only history |
| Binance REST `/fapi/v1/fundingRate` | funding | BTCUSDT from **2019-09-10** | per settlement | — | 1,000 rows per call |
| Bybit `public.bybit.com/trading` | tick trades | BTCUSDT from 2020-03-25 (H2); 2,064 symbol dirs | tick | yes (dirs persist) | no OI, funding or liquidation flag |
| Bybit `public.bybit.com/premium_index`, `spot_index` | premium/index | 11 **inverse** contracts only, 2019-10-01 → 2020-03-10 | — | — | useless for L4 |
| Bybit `public.bybit.com/kline_for_metatrader4` | klines | 23 symbols, 2020–2024 dirs | — | — | not useful |
| **Bybit REST v5 `/market/open-interest`** | OI | BTCUSDT from **2020-08-05**, ETHUSDT 2020-10-22; continuous in every June probe 2021–2026 | 5 min, 15 min, 30 min, 1 h, 4 h, 1 d | **yes**: FTT, LUNA2, MATIC, OMG, RNDR all return 2022-06 | 200 rows/page, cursor |
| **Bybit REST `/market/account-ratio`** | global long/short (accounts) | same start as OI (2020-08-05 BTC) | 5 min–1 d | yes | 500 rows/page |
| Bybit REST `/market/funding/history` | funding | BTCUSDT data in 2020-06 (linear BTCUSDT listed 2020-03) | per settlement | yes | 200/page |
| Bybit REST `/market/instruments-info?status=Closed` | delisted list | 311 closed USDT perps (launched 2021: 25, 2022: 40, 2023: 62, 2024: 117, 2025: 65, 2026: 2) | — | — | point-in-time universe for Bybit |
| OKX REST `/public/funding-rate-history` | funding | **~3 months only**: a query ending 2026-06-30 returned 2 rows, and nothing earlier | 8 h | — | |
| OKX REST rubik `contracts/open-interest-history` | OI | 1D from **2024-01-01** (BTC-USDT-SWAP); 5m/1H return nothing for pre-cutoff windows | 1D only (historical) | not probed | |
| OKX REST rubik `long-short-account-ratio-contract`, `taker-volume-contract`, top-trader L/S | L/S, taker | 1D back to ≥ 2024-03-22 (L/S), ≥ 2025-03-22 (taker, top-trader); older windows not fully probed | 1D | not probed | |
| OKX `public/market-data-history` (static.okx.com downloads) | module 1 trades, 2 1-min candles, 3 funding, 4–6 order books | BTC-USDT-SWAP: trades from 2021-10, candles 2021-09, **funding 2021-09** (then 2022-01 onward) | monthly zips | not probed | no OI module, no liquidation module |
| Coin-M Binance `liquidationSnapshot` | liquidations | 2023-06-25 → 2024-10-14 only, throttled to 1/s (H2) | event | — | inverse contracts only |

## E. Liquidation history: is there a free multi-year proxy?

| source | liquidation field? | span | verdict |
|---|---|---|---|
| Binance `metrics` (both samples) | **No.** The 8 columns are OI, three ratios and taker vol | 2021-12 → | proxy inputs only |
| Binance USDⓈ-M `liquidationSnapshot` | does not exist (H2) | — | none |
| Binance COIN-M `liquidationSnapshot` | yes, sampled at 1/s | 2023-06-25 → 2024-10-14 | 16 months, inverse only, counts and notional undercounted |
| Bybit public files / REST | no (the `allLiquidation` WS is real-time only) | — | none |
| OKX `liquidation-orders` | yes, last ~day only | — | none |
| Hyperliquid tape (local) | yes, complete | 2025-07-28 → 2026-06-30 | one year, and it is the HL discovery year (14 trials) |
| Paid: Tardis.dev | liquidations recorded from exchange streams (Binance forceOrder, throttled to 1/s since 2021-04-27) | 4–5 years on lower plans | $350–6,000/month (public page; the lowest tier needs quarterly or yearly billing) |
| Paid: CoinGlass API | "Liquidation Orders" endpoints; history depth per plan unclear (1d candles "all-time") | — | $29 / $79 / $299 / $699 per month |
| Paid: Kaiko | — | — | no public price |

**Answer: there is no free multi-year liquidation record.** The free proxy is a **deleveraging
event**: a 5-minute fall in OI (`metrics`, 2021-12 →; Bybit REST 5-min OI 2020-08 →) together with a
large hourly price move and a one-sided taker imbalance (`klines` taker-buy volume, 2020-01 →).
That proxy cannot separate liquidations from voluntary de-risking (the H2 caveat). Its label noise can
be measured against the complete HL tape in the overlap year. That measurement is behaviour, not
outcome, but it touches the HL discovery year, and any registration must say so. With these inputs,
"spirals across years" is about 4.5 years on Binance (2021-12 →) and about 5.9 years on
Bybit for BTC/ETH.

## F. Design sketch (no outcomes)

**The split, decided now, before any outcome is read:**

| segment | dates (weeks start Monday 00:00 UTC) | regime content (public knowledge) | role |
|---|---|---|---|
| Pre-metrics extension | 2020-01-06 → 2021-11-29 | 2020 crash, 2021 bull | optional discovery for funding/klines-only factors |
| **Discovery** | **2021-12-06 → 2023-12-31** | 2022 bear (LUNA, 3AC, FTX), 2023 recovery | discovery |
| **Holdout A** | **2024-01-01 → 2025-07-27** | 2024 ETF bull, 2025-H1 | **decision holdout** |
| Holdout B | 2025-07-28 → 2026-06-30 | = HL discovery year (2025-10-10 crash) | confirmatory only. H5/H6 were *motivated* by this year's spirals, so it is not independent for them |
| Programme holdout | 2026-07-01 → | — | sealed and untouched by this scout |

A **full-feature coin-week** has three requirements:

- full metrics files on all 7 days of week w;
- funding and 1h-kline monthly files covering w;
- 1h klines covering w + 1, the outcome week.

The outcome week must end before 2026-07-01. The last discovery week's outcome week falls
in 2024-01, so the registration needs a **1-week embargo** at each boundary.

| split | weeks | coin-weeks | mean coins/week | min–max | IC MDE (iid) |
|---|---|---|---|---|---|
| Discovery | 108 | 17,374 | 161 | 129–239 | **0.021** |
| Holdout A | 82 | 25,892 | 316 | 241–451 | **0.017** |
| Holdout B | 47 | 24,325 | 518 | 455–540 | 0.018 |
| Holdout A + B | 129 | 50,217 | 389 | 241–540 | 0.013 |
| Pre-metrics (funding + klines only) | 100 | 7,593 | 76 | 11–131 | 0.032 |

**The iid formula:**

- IC_mde ≈ (z₀.₉₇₅ + z₀.₈₀) / √(N_weeks × coins) = 2.80 / √(coin-weeks).
- This holds when each week's Spearman IC has s.e. 1/√coins and weeks are independent.
- **Effective independent weeks** = N_weeks (108 / 82 / 47) only for non-overlapping weekly
  rebalances. Daily-rebalanced 7-day horizons add overlap, not information.

**What the formula ignores:**

1. Cross-sectional correlation. One market factor dominates crypto, so the true per-week IC noise is
   far above 1/√coins and the effective breadth is much smaller than the coin count.
2. Serial correlation of the IC within regimes, which means fewer effective weeks.
3. Fat tails and listing/delisting churn.
4. The liquidity filter, which will shrink the coin count. C's ≥ 10 MB/month tier keeps
   74–99% of symbols depending on the year, and a $ filter is still undetermined.
5. Multiple testing across factors.
6. Costs, which turn an IC into PnL.

**A more honest version** uses the time-series s.d. of the weekly IC: MDE = 2.80 σ_IC / √N_weeks.
σ_IC is assumed here, not measured.

| σ_IC (assumed) | Discovery (N = 108) | Holdout A + B (N = 129) | Holdout A alone (N = 82) |
|---|---|---|---|
| 0.05 | 0.013 | 0.012 | 0.015 |
| 0.10 | 0.027 | 0.025 | 0.031 |
| 0.15 | 0.040 | 0.037 | 0.046 |
| 0.20 | 0.054 | 0.049 | 0.062 |

For comparison, the HL year gives ~47 weeks × ~100 coins: an iid MDE of ≈ 0.041, and ≈ 0.061 at σ_IC 0.15.
Binance discovery alone gives ~2.3× the weeks.

## G. The full pull

"Qualifying symbols" are univ symbols with ≥ 1 full-feature coin-week in the period. Files are
restricted to the period: metrics to full files (≥ 4 KB) on period dates, monthly files to the
months overlapping it. File counts are zips; there is one `.CHECKSUM` per zip on top.

| pull | symbols | zips (metrics + funding + 1h) | **GB** (metrics + funding + 1h) | metrics | funding | klines 1h | alt: klines 1d | + premium 1h | + mark 1h |
|---|---|---|---|---|---|---|---|---|---|
| **(i) discovery** (2021-12-01 → 2023-12-31) | 258 | **132,083** | **1.54** | 1.40 | 0.004 | 0.13 | 0.008 | 0.06 | 0.07 |
| **(ii) discovery + holdout** (→ 2026-06-30) | 667 | **516,020** | **5.86** | 5.33 | 0.017 | 0.52 | 0.030 | 0.23 | 0.27 |
| holdout alone (2024-01-01 → 2026-06-30) | 649 | 383,715 | 4.32 | 3.92 | 0.014 | 0.39 | 0.023 | 0.17 | 0.20 |
| variant: Monday-only metrics, discovery | 258 | 17,500 metrics zips | 0.20 metrics | | | | | | |
| variant: Monday-only metrics, all years | 667 | 68,895 metrics zips | 0.76 metrics | | | | | | |

**Notes on the pull:**

- Both (i) and (ii) exceed the ~1 GB line, so they **need owner approval**.
- About 132k or 516k small GETs, plus checksums, is a few hours of polite downloading.
- Unzipped, metrics is about 6× larger, so convert it to parquet per day on arrival.
- **Holdout hygiene for the pull:** download discovery first. Fetch the holdout segments into a
  separate directory only after a registration names them, because a holdout on disk can be read by accident.
- Optional extras:
  - the pre-metrics years' funding + 1h klines (2020-01 → 2021-11): about 0.1 GB, not counted above;
  - Bybit REST OI and long/short at 1 h for ~400 symbols × 5 years: about 90k paged requests, free.

## Reproduction

```bash
cd research/copytrade/scouts
PY=../.venv/bin/python
# listings (keys/sizes only; ~20 min total; writes ../data/scout_l4/listings/*.csv)
$PY -I l4_binance_lister.py daily metrics
$PY -I l4_binance_lister.py monthly fundingRate
for d in klines premiumIndexKlines markPriceKlines indexPriceKlines; do
  for i in 1h 1d; do $PY -I l4_binance_lister.py monthly $d $i; done; done
$PY -I l4_binance_lister.py monthly aggTrades
$PY -I l4_binance_lister.py daily klines 1d
# exchangeInfo (today's snapshot; classification only)
mkdir -p ../data/scout_l4/exchangeinfo && curl -s -o ../data/scout_l4/exchangeinfo/fapi_exchangeInfo.json https://fapi.binance.com/fapi/v1/exchangeInfo
# samples (8 zips, 121 KB, all pre-2026-07; each dir new; verify .CHECKSUM)
#   metrics BTCUSDT 2020-09-15 and 2026-06-15; monthly 2024-06 BTCUSDT fundingRate, klines 1h/1d,
#   premiumIndexKlines/markPriceKlines/indexPriceKlines 1h -> ../data/scout_l4/samples/<dataset>/
$PY -I l4_sample_schema.py $(find ../data/scout_l4/samples -name '*.zip' | sort)
# all tables in A, B, C, F, G
$PY -I l4_listing_stats.py > ../data/scout_l4/stats_output.md
# REST depth probes (end-dated < 2026-07-01, prints counts/timestamps only)
$PY -I l4_venue_probe.py binance; $PY -I l4_venue_probe.py bybit; $PY -I l4_venue_probe.py okx
```

Some extra probes were run inline with the helpers in `l4_venue_probe.py` and were not saved as
subcommands:

- the Bybit start-date windows for 2020-07 → 2021-06;
- Bybit OI for the delisted symbols FTT, LUNA2, MATIC, OMG and RNDR in 2022-06;
- Bybit `instruments-info?status=Closed` with pagination;
- OKX rubik start-date windows, and `market-data-history` modules 1–3 with ≤ 10-month windows.

The same `get()` guard asserts that every end parameter is before 2026-07-01.

## What I could not determine

- **The $-volume liquidity count** (≥ $10 M median daily). It needs `quote_volume` from 1d klines for
  all symbols (0.033 GB, pre-cutoff). That is not a handful of samples, so it was not pulled.
- **Delisting dates before 2021-12** (the funding/klines-only years). Kline files continue after
  delisting, so the dates need kline contents. Funding's last month matches 76/114 times.
- **What dead-contract kline rows contain** after delisting (presumably flat or zero volume). Not
  inspected.
- **Why metrics stubs stop in batches** (2024-07-15, 2025-03-10, 2025-06-05), and whether the
  archive will regenerate old days again. The zip mtimes show it has done so.
- **Bybit REST pacing for a full pull**, and whether every closed Bybit symbol has OI/LS
  history. Only 5 were probed.
- **OKX rubik long/short start dates** (only bounded between 2023-07 and 2024-03), and OKX
  coverage for alts and delisted instruments.
- **Whether CoinGlass plans include multi-year liquidation history.** The page does not say.
- **The real σ_IC.** It cannot be known without reading outcomes, so F's sensitivity table is assumption-based.

## Holdout hygiene statement

- **data.binance.vision S3 listing** (`s3-ap-northeast-1.amazonaws.com/data.binance.vision`, public, not
  requester-pays):
  - Listed every key, size and date for `um/daily/metrics`, `um/daily/klines/*/1d`,
    `um/monthly/{fundingRate, klines, premiumIndexKlines, markPriceKlines, indexPriceKlines}` at 1h/1d,
    and `um/monthly/aggTrades`, including keys dated 2026-07-01 → 2026-10-09.
  - From the post-cutoff keys, only counts and sizes were used. **No object dated ≥ 2026-07-01 was fetched.**
  - The listing does reveal which symbols still have files after the cutoff. That is survivorship information
    about the holdout, and it was used only in aggregate ("present in June 2026").
- **Downloaded:** 8 zips plus checksums, 121 KB, all BTCUSDT:
  - metrics for 2020-09-15 and 2026-06-15;
  - 2024-06 monthly fundingRate, klines 1h/1d, premiumIndexKlines 1h, markPriceKlines 1h and indexPriceKlines 1h.
  - All are before 2026-07-01. Values read: only BTCUSDT 2024-06 total quote volume (calibration).
- **Binance REST:**
  - `fapi/v1/exchangeInfo` was fetched once, 2026-10-11. It is a current snapshot used only to classify
    symbols (contract type, status, onboard and delivery dates). Its `deliveryDate` values ≥ 2026-07-01 were not used.
  - `futures/data/*` windows ending 2026-06-30 were all rejected, so no data came back.
  - `fapi/v1/fundingRate` ran for 2019-09 → 2019-12.
- **Bybit:**
  - The `public.bybit.com` directory index was read (names only).
  - REST `open-interest`, `account-ratio` and `funding/history` were queried in windows from 2019-06 to
    2026-06-30, each end-dated before the cutoff; counts and timestamps only were printed.
  - `instruments-info` (current metadata, statuses) was read.
- **OKX:** `funding-rate-history`, rubik OI, long/short and taker endpoints, and `market-data-history`
  modules 1–6 were all queried with end or `after` before 2026-07-01. They returned only file names and
  sizes, or counts and timestamps.
- **CoinGlass and Tardis:** public pricing pages only.
- **Hyperliquid:** nothing read.
- No outcome, return or feature–future relation was computed. Nothing was published or committed.
