# L2 scout: delta-neutral funding and basis carry

2026-10-11. Feasibility only, behaviour only (programme doc §3 point 4). This scout measures
coverage, costs, margin, and the **unconditional** size of funding spreads and basis against
costs at entry. It computes no autocorrelation, persistence, half-life, conditional statistic,
regression of future on current funding, or P&L of any rule. The one series-shape number
reported, sign changes per month, is a count over the level series (§F).

Scripts: `research/copytrade/scouts/l2_fetch.py` (bounded fetchers), `l2_analyze.py`
(coverage and distributions), `l2_spreads.py` (spread proxy from local tapes),
`l2_benchmarks.py` (sUSDe, on-chain). Data: `research/copytrade/data/scout_l2/` (gitignored).
Full analysis output: `data/scout_l2/analysis.txt`.

## Summary

1. **Data:** free, date-bounded funding history exists for HL (220 coins; 8-hourly from 2023-05-12, hourly from 2023-06-08), Binance, Bybit and OKX (8/4/2/1 h). The overlap with HL grows from about 55 coins in 2023 to about 190–200 in 2025–26. A full registration pull is about 8,000 HL requests (about 5–6 h) plus about 12 MB of CEX files, well below 1 GB.
2. **Costs:** the base-tier taker round trip for HL × CEX is 19–20 bp in fees plus ≤ 1 bp of spread on core coins (impact at size not measured). Amortised over 7 days the hurdle is ≈ 11 %/yr; over 30 days ≈ 2.6 %/yr; over 1 day ≈ 77 %/yr.
3. **Spread HL − CEX, core (BTC/ETH/SOL):** the median is +0.4 to +7.7 %/yr in every venue-year. p10/p90 is about −25/+47 %/yr in 2023, narrowing to about −12/+12 %/yr in 2026 H1. |spread| exceeds the 7-day hurdle in 22–32 % of hours in 2025–26 and 40–62 % in 2023–24, and the 1-day hurdle in ≤ 5 %. The spread is exactly 0 for long stretches, when both venues sit at the 10.95 %/yr baseline. **Others (pooled)** have median 0 and wider tails, and in 2023 HL sat far below the CEXs (median −15 %/yr).
4. **Perp–spot on Binance (core):** the median funding (the carry) fell from 9–11 %/yr (2023–24) to about 5 % (2025) and 1–2 % (2026 H1), against a 7-day hurdle of 16.7 %/yr. Basis at the hour close is −3 to −6 bp. The packaged equivalent, sUSDe, realised 15.7 % / 6.2 % / 4.0 % (2024 / 2025 / 2026 H1).
5. **Access and hygiene:** for a German resident, Binance, Bybit (global and EU today), OKX global and Bitget are **research data only**. HL is an unlicensed grey zone. OKX Europe X-Perps (launched 2026-04-15) is the only licensed perp leg, and it has no usable history before the holdout. A pre-existing Bitget cache exposed 15 holdout rows (below).

## Holdout incident (for the orchestrator to ledger; proposed id 000d)

**What happened.** While surveying caches, I printed the first 400 bytes of a pre-existing
cache file, `research/copytrade/data/cache/bg-funding/113661ad35e25a2321da967e.json`
(command `head -c 400`). I identified the file by hashing candidate keys: it is Bitget **ARBUSDT** funding history, written
by `ct/bitget.py:funding_history` under the key `ARBUSDT:2026-10-07`.

**What was seen.** Fifteen complete 8-hour Bitget ARBUSDT funding settlements, from
2026-07-09 16:00 to 2026-07-14 08:00 UTC (ms 1783612800000 to 1784016000000), plus the timestamp of a sixteenth
(2026-07-14 16:00) whose value was cut off. All are inside the holdout. In order the values were 2.7e-05, -6e-06,
8.2e-05, 0.0001 (5×), -2.2e-05, 1.6e-05, 0.0001 (3×), -8.7e-05 and 0.0001. They were printed to the terminal only. They were not saved, and
nothing was computed from them.

**Scope of the cache.** A later count, which dropped rows at load and printed counts only, found that
all 21 `bg-funding` files together hold **0 rows before 2026-07-01 and 7,783 rows
on or after it**. The cache is entirely holdout-dated, because Bitget serves about 90 days and the cache was
filled in October 2026. It was not used for anything else. The
`hl-funding` cache (`p4_prefetch.py`, end 2026-07-10) also extends into the holdout. It was not read beyond the first 600 bytes of one
file, which held only 2025-10-20 rows (pre-cutoff). I fetched fresh Hyperliquid data with explicit bounds instead.

**What must not follow.** No Bitget-based or ARB-based threshold may be chosen
because of these rows. Any registration using Bitget funding must not treat
2026-07-09/14 as discovery.

## A. Coverage

**Universe rule (fixed, outcome-free).**
- The candidates are every coin in the local HL discovery archive (202 coins, timestamps < 2026-07-01) together with every Binance USDT-M symbol
  that has a `fundingRate` directory (names only, from the S3 listing, mapped as `1000X` → `kX`). That gives 907 candidates.
- Each candidate was probed with HL `fundingHistory` from 2023-05-01 to 2026-06-30. 220 coins have funding before the cutoff.
- Overlap means a coin has HL and CEX data in the same sampled window.
- **Core = BTC, ETH, SOL**, with the full hourly span. **Others** are a fixed calendar sample: 500 hours from 00:00 UTC on the 1st of
  January, April, July and October, 2023-07 → 2026-04, which is 12 windows.
- **Delisted HL coins keep printing exact zeros** (rate = premium = 0). 61,473 such hours over 29 coins were excluded.

| venue | free source used (bounded) | history used | interval | caps / clamps | gaps | coins overlapping HL per year: 2023 / 2024 / 2025 / 2026 H1 |
|---|---|---|---|---|---|---|
| **Hyperliquid** | `api.hyperliquid.xyz/info` `fundingHistory` (start/end) | 2023-05-12 → 2026-06-30 23:00 | **8 h until 2023-06-08, then 1 h** | 4 %/h; no print at the cap; 51 of 966k loaded prints ≥ 1 %/h | BTC/ETH/SOL: 27,508 hours, three 2-hour gaps (2023 ×2, 2024 ×1) | 58 / 138 / 202 / 192 (HL coins live in the windows) |
| **Binance USDⓈ-M** | `data.binance.vision` monthly `fundingRate` (calc_time, interval_h, rate) | pulled from 2023-01 (older files exist) → 2026-06-30 16:00 | core always 8 h. Sampled others, by settlement count: 4 h 164k, 8 h 79k, 1 h 5k, 2 h 74 | ±0.75 × MMR for listed majors, ±2 % for others; observed ±3 % (2023) | core: none > 8 h. 737 of 2,646 symbol-months had no file (not listed) | 53 / 133 / 195 / 186 |
| **Bybit linear** | `api.bybit.com/v5/market/funding/history` (startTime/endTime) | BTC from 2020-03; core pulled from 2023-01 → 2026-06-30 16:00 | inferred from gaps. Others: 8 h 70k, 4 h 69k, 1 h 17k, 2 h 6.8k | tied to margin rates; observed −4 % (DOODUSDT 2025-10-07) | core: none > 8 h | 54 / 136 / 193 / 184 |
| **OKX swap** | `static.okx.com/…/swaprates/monthly/{YYYYMM}/{X}-USDT-SWAP-fundingrates-{YYYY-MM}.zip`. Months are UTC+8, so the **last pre-cutoff row is 2026-06-30 08:00 UTC** (the 2026-07 file was not fetched) | core from 2022-12-31 16:00 | inferred. Others: 8 h 82k, 4 h 73k, 1 h 4.9k, 2 h 2k | per contract; observed ±3 %, ±2 % | core: none > 8 h. **Cross-period collection before 2024-01** (§D) | 43 / 100 / 156 / 145 |
| any CEX | | | | | | 55 / 138 / 201 / 190 |
| **Bitget** (scope addition) | none: the REST endpoint is page-numbered from the newest row and cannot take a date bound | — | 8 h (cache) | — | local cache is 100 % holdout-dated | **no bounded free history** |
| **OKX Europe X-Perps** (§H) | none found | launched 2026-04-15 | 8 h | not published | — | — |

Notes:
- 2023 has only two sampled windows (July, October) and 2026 two (January, April), so per-year counts are "≥ 1 window that year".
- In 24 Binance, 60 Bybit and 5 OKX paired non-core windows the CEX rate is constant for the whole window, which may mean an inactive
  symbol. They are kept, and they affect the pooled "others" cells marginally.
- HL coins delisted before 2025-07-28 that never had a Binance USDT-M perp are missing from the candidate set. The miss is small and
  inside "others" only.

## B. Costs

**B1. Fees at the base tier.** Fees are in % of notional per fill. "Verified" means I read the number on the venue's
own page. "Secondary" means it was quoted from a search snippet or a third-party copy because the venue page would not render.

| venue | perp maker | perp taker | spot maker / taker | source |
|---|---|---|---|---|
| Hyperliquid (tier 0) | 0.015 | 0.045 | 0.040 / 0.070 | verified: hyperliquid.gitbook.io/hyperliquid-docs/trading/fees (staking discount 5–40% not applied) |
| Binance USDⓈ-M (VIP 0) | 0.020 | 0.050 (0.045 with BNB) | 0.100 / 0.100 | secondary: binance.com/en/support/articles/360033544231; the fee page did not render |
| Bybit (non-VIP) | 0.020 | 0.055 | 0.100 / 0.100 | secondary: bybit.com/en-US/help-center/article/Trading-Fee-Structure |
| OKX global (regular) | 0.020 | 0.050 | 0.080 / 0.100 | secondary: okx.com/fees did not render |
| OKX Europe X-Perps | 0.020 | 0.050 | — | verified: okx.com/en-eu/x-perps ("2 bps maker and 5 bps taker") |
| Bitget | not verified | not verified | — | not checked; see §H |

The round trip is open plus close of both legs, taker, at base tier. Fees alone come to: HL + Binance **19 bp**, HL + Bybit
**20 bp**, HL + OKX **19 bp**, Binance perp + Binance spot **30 bp**. Maker on both legs would be 7 bp
for HL + Binance (1.5 + 2.0, × 2), but that carries fill risk, which this scout does not measure. Funding is paid on notional, so the
amortised hurdle in §C is **round trip ÷ hold**: for example 21 bp over 7 days is **10.95 %/yr**, over 1 day 76.7 %/yr, and over
30 days 2.6 %/yr.

**B2. Spread proxy.** Contemporaneous prices only. The measure is the median gap between consecutive opposite-side
taker prints within 1 s (`l2_spreads.py`). A zero means the next opposite print hit the same price.

| coin | HL median (bp), 11 days: 15th of each month 2025-08 → 2026-06 | Binance USDT-M median (bp), local HL-cascade days only (stress-biased) |
|---|---|---|
| BTC | 0.00 | 0.01 |
| ETH | 0.00 | 0.03 |
| SOL | 0.00 | 0.70 |
| XRP | 0.00 | 0.43 |
| DOGE | 0.00 | 0.46 |
| HYPE | 0.00 | 0.22 |
| HL others, 171 coins with ≥ 200 pairs: p25 / p50 / p75 of the per-coin median | −0.11 / 0.00 / 0.11 | — |

On HL the proxy collapses to one tick. Prices carry at most 5 significant figures, so the tick is between
0.1 and 1 bp of price. At small size the quoted spread is therefore ≤ 1 bp per fill for core coins on both venues. **Not
measured: market impact at size.** It is the real constraint for mid-caps, and Binance `bookDepth` (±0.2–5 % depth,
2023 →, free) could measure it. §C uses **0.5 bp per fill for core and 2 bp for others**, which is deliberately above the
proxy, so the hurdle is 21–22 bp round trip for core and 27–28 bp for others.

**B3. Transfers between venues.**

| leg | cost | time | source |
|---|---|---|---|
| deposit to HL (USDC via Arbitrum bridge) | Arbitrum gas only; minimum 5 USDC (smaller deposits are lost) | < 1 min | Bridge2 docs, hyperliquid.gitbook.io/…/bridge2 (via search) |
| withdraw from HL | flat 1 USDC | 3–4 min | same; secondary guides agree |
| CEX USDT/USDC withdrawal (Arbitrum / Tron) | about 0.1–1 USD flat (Binance USDT TRC20 1 USDT; Arbitrum about 0.1) | minutes | secondary (eco.com fee comparisons); not verified per venue |
| USDC ↔ USDT | HL margins in USDC, Binance/Bybit/OKX USDT-M margin in USDT. The conversion is one spot fill (about 0–10 bp). Binance USDC-M perps avoid it | — | — |

At a $10k leg, one rebalancing transfer costs about 1–2 bp, which is small next to fees. What matters is **latency in a spiral**: a top-up takes
minutes, and liquidation can happen in seconds.

**B4. Margin and liquidation distance.** These are analytic, isolated legs, using maintenance-margin rates `m` from the venue docs. A
delta-neutral pair across venues **cannot cross-margin**, so each leg stands alone. Capital for notional N per leg is 2N / L:
2N at 1×, N at 2×, 0.67N at 3×. Long liquidates after a fall of (1/L − m)/(1 − m), and short after a rise of (1/L − m)/(1 + m).

| leg (m) | 1× long / short | 2× long / short | 3× long / short |
|---|---|---|---|
| HL BTC (1.25 %, 40× max) | −100 % / +97.5 % | −49.4 % / +48.1 % | −32.5 % / +31.7 % |
| HL ETH (2 %, 25× max) | −100 % / +96.1 % | −49.0 % / +47.1 % | −32.0 % / +30.7 % |
| HL SOL (2.5 %, 20× max) | −100 % / +95.1 % | −48.7 % / +46.3 % | −31.6 % / +30.1 % |
| HL mid-cap (5 %, 10× max) | −100 % / +90.5 % | −47.4 % / +42.9 % | −29.8 % / +27.0 % |
| HL small (10–16.7 %, 5×–3× max) | −100 % / +71–82 % | −40–44 % / +29–36 % | −20–26 % / +14–21 % |
| CEX BTC (about 0.4–0.5 %) | −100 % / +99.2 % | −49.8 % / +49.4 % | −33.1 % / +32.8 % |
| CEX mid/small (about 1.25–2.5 %) | −100 % / +95–98 % | −48.7–49.4 % / +46–48 % | −31.6–32.5 % / +30–32 % |

Sources: HL margining ("maintenance margin is … half of the initial margin at max leverage") and HL liquidations
(1.25 %–16.7 %) at hyperliquid.gitbook.io. The Bybit BTCUSDT MMR of 0.5 % up to 2 M USDT is from the Bybit help centre. Binance's tier
table did not render, and its leverage-bracket endpoint needs a key, so the CEX rows are approximate. The figures ignore fees and the
difference between mark and last price across venues. They also ignore that HL pays funding on the **oracle** price. **Regulatory cap:**
if a German retail client trades on an EU-licensed venue, ESMA's 2026-02-24 statement treats perps as likely CFDs, and national CFD
measures cap crypto CFDs at 2:1 for retail (BaFin general CFD measure; the figure comes from a search snippet). So 2× may be the maximum
on the lawful leg (§H).

## C. Unconditional spread distribution

**Definition.** For each HL hour h:

spread_h = (HL rate for hour h − CEX rate ÷ interval hours for the CEX interval containing h) × 8760, in %/yr.

- Positive means HL pays more, so the trade would be short HL and long the CEX.
- Pooled over hours. Core uses every hour; others use only the sampled windows.
- **Share** is the fraction of hours with |spread| above the round-trip cost amortised over 1, 7 or 30 days.
- The round trip is fees plus 4 × half-spread (§B): 21 bp (Binance, OKX) and 22 bp (Bybit) for core, 27–28 bp for others. The 7-day
  hurdle is therefore 10.95 / 11.5 %/yr for core and 14.1 / 14.6 %/yr for others.
- These are levels at a point in time. **No statement about how long a spread lasts is made or implied.** That is the registration's trial.

**C1. HL − Binance**

| group | year | hours | p10 | p25 | p50 | p75 | p90 | share > 1d / 7d / 30d hurdle |
|---|---|---|---|---|---|---|---|---|
| core | 2023 (from 05-12) | 16,863 | −23.1 % | −8.4 % | +2.8 % | +16.8 % | +45.9 % | 0.048 / 0.540 / 0.822 |
| core | 2024 | 26,349 | −5.3 % | 0.0 % | +5.2 % | +16.7 % | +41.8 % | 0.027 / 0.401 / 0.700 |
| core | 2025 | 26,280 | −7.6 % | 0.0 % | +4.5 % | +9.4 % | +15.5 % | 0.003 / 0.263 / 0.749 |
| core | 2026 H1 | 13,011 | −11.0 % | −3.5 % | +2.9 % | +8.0 % | +12.3 % | 0.000 / 0.239 / 0.787 |
| others | 2023 | 35,446 | −64.8 % | −36.4 % | −15.3 % | 0.0 % | +11.0 % | 0.058 / 0.597 / 0.817 |
| others | 2024 | 218,992 | −11.1 % | 0.0 % | 0.0 % | +6.6 % | +31.7 % | 0.019 / 0.268 / 0.441 |
| others | 2025 | 318,373 | −17.5 % | 0.0 % | 0.0 % | +7.3 % | +28.1 % | 0.021 / 0.293 / 0.481 |
| others | 2026 H1 | 178,377 | −22.7 % | 0.0 % | 0.0 % | +8.9 % | +24.5 % | 0.025 / 0.315 / 0.559 |

**C2. HL − Bybit**

| group | year | hours | p10 | p25 | p50 | p75 | p90 | share > 1d / 7d / 30d |
|---|---|---|---|---|---|---|---|---|
| core | 2023 | 16,863 | −26.0 % | −9.7 % | +0.4 % | +16.3 % | +43.8 % | 0.040 / 0.536 / 0.791 |
| core | 2024 | 26,349 | −3.6 % | 0.0 % | +5.5 % | +17.3 % | +40.1 % | 0.023 / 0.406 / 0.678 |
| core | 2025 | 26,280 | −8.3 % | 0.0 % | +3.2 % | +9.5 % | +15.5 % | 0.003 / 0.260 / 0.684 |
| core | 2026 H1 | 13,011 | −12.1 % | −4.1 % | +1.7 % | +7.5 % | +11.8 % | 0.001 / 0.215 / 0.754 |
| others | 2023 | 35,496 | −69.5 % | −39.1 % | −16.2 % | 0.0 % | +11.9 % | 0.066 / 0.613 / 0.819 |
| others | 2024 | 220,249 | −14.2 % | 0.0 % | 0.0 % | +4.8 % | +28.4 % | 0.017 / 0.260 / 0.427 |
| others | 2025 | 315,496 | −23.9 % | 0.0 % | 0.0 % | +2.1 % | +24.7 % | 0.027 / 0.285 / 0.442 |
| others | 2026 H1 | 175,769 | −29.1 % | −1.1 % | 0.0 % | +5.4 % | +23.8 % | 0.035 / 0.316 / 0.505 |

**C3. HL − OKX**

| group | year | hours | p10 | p25 | p50 | p75 | p90 | share > 1d / 7d / 30d |
|---|---|---|---|---|---|---|---|---|
| core | 2023 | 16,863 | −24.8 % | −9.3 % | +5.4 % | +19.6 % | +46.9 % | 0.049 / 0.622 / 0.907 |
| core | 2024 | 26,349 | −9.5 % | −0.1 % | +7.7 % | +17.3 % | +40.5 % | 0.027 / 0.484 / 0.870 |
| core | 2025 | 26,280 | −7.7 % | 0.0 % | +5.2 % | +10.6 % | +16.4 % | 0.003 / 0.316 / 0.774 |
| core | 2026 H1 | 12,987 | −13.1 % | −5.3 % | +1.6 % | +7.3 % | +11.7 % | 0.000 / 0.250 / 0.777 |
| others | 2023 | 29,633 | −71.9 % | −38.8 % | −15.6 % | +2.8 % | +18.7 % | 0.066 / 0.647 / 0.912 |
| others | 2024 | 159,357 | −17.9 % | −4.9 % | +6.1 % | +18.2 % | +36.6 % | 0.017 / 0.447 / 0.853 |
| others | 2025 | 250,663 | −22.6 % | −3.7 % | 0.0 % | +11.3 % | +28.4 % | 0.022 / 0.360 / 0.649 |
| others | 2026 H1 | 138,668 | −25.2 % | −0.9 % | 0.0 % | +7.5 % | +24.0 % | 0.025 / 0.325 / 0.537 |

**C4. Core per coin: p10 / p50 / p90, %/yr**

| coin | year | HL − Binance | HL − Bybit | HL − OKX |
|---|---|---|---|---|
| BTC | 2023 | −19.8 / +2.8 / +34.3 | −21.2 / 0.0 / +32.8 | −19.8 / +5.0 / +35.5 |
| BTC | 2024 | −1.7 / +5.6 / +39.7 | 0.0 / +5.9 / +38.1 | −6.9 / +8.4 / +38.6 |
| BTC | 2025 | −3.0 / +5.1 / +12.9 | −2.4 / +4.6 / +13.8 | −3.2 / +5.2 / +14.0 |
| BTC | 2026 H1 | −9.6 / +3.1 / +11.4 | −8.7 / +2.8 / +11.0 | −11.0 / +1.9 / +11.1 |
| ETH | 2023 | −11.9 / +6.2 / +46.5 | −12.7 / +5.7 / +45.5 | −15.0 / +8.4 / +48.5 |
| ETH | 2024 | −7.9 / +3.6 / +37.0 | −6.5 / +4.2 / +35.8 | −10.0 / +6.6 / +35.9 |
| ETH | 2025 | −8.7 / +3.8 / +12.5 | −8.5 / +2.6 / +14.5 | −7.7 / +5.3 / +15.3 |
| ETH | 2026 H1 | −7.4 / +4.5 / +12.7 | −8.0 / +3.4 / +12.0 | −9.2 / +3.0 / +11.9 |
| SOL | 2023 | −32.0 / 0.0 / +59.2 | −36.6 / 0.0 / +56.9 | −36.2 / +0.6 / +60.5 |
| SOL | 2024 | −5.7 / +6.6 / +50.1 | −4.2 / +6.2 / +48.4 | −12.1 / +8.4 / +47.7 |
| SOL | 2025 | −11.5 / +4.6 / +19.2 | −13.3 / +1.8 / +18.7 | −12.5 / +5.2 / +18.8 |
| SOL | 2026 H1 | −14.7 / 0.0 / +12.7 | −18.0 / −0.1 / +13.6 | −17.3 / −0.2 / +12.3 |

How to read it:
- The core spread distribution has narrowed every year since 2023. In 2025–26 its p90 (about +12 to +19 %/yr) sits just above the
  7-day hurdle (about 11 %/yr), and its p10 (about −8 to −18 %/yr) sits just below the negative hurdle.
- Most hours that clear the 7-day hurdle are in the tails, and the 1-day hurdle is almost never cleared after 2024.
- The 30-day hurdle (2.6 %/yr) is cleared in 68–91 % of hours. That only matters if a spread lasted 30 days, which is the
  persistence question this scout does not answer.

**C5. Binance perp vs spot, core.** Basis = (perp close ÷ spot close − 1) at the same 1 h close, in bp. Carry = Binance funding, annualised.
This is what short perp / long spot collects. Round trip: spot taker 10 bp + perp taker 5 bp, × 2, + spread = 32 bp, so the
hurdle over 1 / 7 / 30 days is 116.8 / 16.7 / 3.9 %/yr. "f < −thr" would need a spot short, i.e. borrowing.

| coin | year | basis bp p10 / p50 / p90 | funding %/yr p10 / p25 / p50 / p75 / p90 | share f > +hurdle 1d / 7d / 30d | share f < −hurdle 7d / 30d |
|---|---|---|---|---|---|
| BTC | 2023 | −5.5 / −3.8 / +4.3 | −0.0 / 3.2 / 9.1 / 11.0 / 11.0 | 0.000 / 0.047 / 0.717 | 0.000 / 0.022 |
| BTC | 2024 | −5.4 / −2.8 / +7.5 | 0.9 / 5.7 / 11.0 / 11.0 / 24.1 | 0.000 / 0.147 / 0.804 | 0.000 / 0.026 |
| BTC | 2025 | −5.6 / −4.4 / −3.1 | −0.7 / 2.0 / 5.3 / 9.1 / 11.0 | 0.000 / 0.000 / 0.616 | 0.000 / 0.028 |
| BTC | 2026 H1 | −6.1 / −4.9 / −3.7 | −5.9 / −2.2 / 1.7 / 4.7 / 7.2 | 0.000 / 0.000 / 0.312 | 0.000 / 0.168 |
| ETH | 2023 | −5.6 / −3.8 / +4.9 | 0.3 / 3.4 / 9.0 / 11.0 / 11.0 | 0.000 / 0.053 / 0.727 | 0.001 / 0.025 |
| ETH | 2024 | −5.2 / −3.0 / +7.9 | 2.7 / 7.0 / 11.0 / 11.0 / 27.2 | 0.000 / 0.172 / 0.865 | 0.000 / 0.011 |
| ETH | 2025 | −5.7 / −4.4 / −3.0 | −1.3 / 1.6 / 5.3 / 9.2 / 11.0 | 0.000 / 0.000 / 0.597 | 0.002 / 0.037 |
| ETH | 2026 H1 | −6.2 / −4.9 / −3.8 | −8.0 / −3.3 / 1.3 / 5.0 / 7.9 | 0.000 / 0.000 / 0.318 | 0.018 / 0.227 |
| SOL | 2023 | −9.1 / −3.2 / +6.3 | −14.3 / −2.1 / 11.0 / 11.0 / 11.0 | 0.000 / 0.073 / 0.625 | 0.083 / 0.223 |
| SOL | 2024 | −6.0 / −2.9 / +8.8 | −0.4 / 5.0 / 11.0 / 11.0 / 34.4 | 0.002 / 0.197 / 0.789 | 0.001 / 0.043 |
| SOL | 2025 | −7.1 / −5.0 / −2.9 | −9.6 / −3.9 / 2.3 / 9.2 / 11.0 | 0.000 / 0.001 / 0.426 | 0.037 / 0.255 |
| SOL | 2026 H1 | −8.2 / −5.9 / −3.6 | −17.4 / −8.7 / −1.6 / 4.6 / 10.4 | 0.000 / 0.000 / 0.271 | 0.109 / 0.419 |

Binance funding piles up at exactly 10.95 %/yr (the interest leg) in 2023–24, and drifts below it in 2025–26. Perp–spot carry on
Binance at the base tier needs funding above 16.7 %/yr to clear a 7-day hurdle. That happened in 0–20 % of hours in 2023–24 and in
**none** in 2025–26. Basis levels (−3 to −6 bp in 2025–26) are small next to the 32 bp round trip.

## D. Funding mechanics

| | Hyperliquid | Binance USDⓈ-M | Bybit linear | OKX swap |
|---|---|---|---|---|
| settlement | **every hour**. Rate = 8 h formula ÷ 8. **8-hourly until 2023-06-08**, measured on BTC/ETH/SOL | 00/08/16 UTC by default. Many alts settle every 4 h. From 2025-05-02 a settlement at the cap switches the symbol to 1 h; from 2026-01-02 it reverts to 4 h after 16 calm cycles | 8 h by default; 4 h, 2 h and 1 h symbols are present in the data (§A) | 8 h by default. Some contracts moved to 4 h (2024-07-18/25). Since then, 4/2/1 h automatically at the cap; it reverts only if 12 h stay within ±0.20 % |
| formula | F = P̄ + clamp(I − P̄, ±0.05 %) | F = [P̄ + clamp(I − P̄, ±0.05 %)] ÷ (8/N), then capped | F = clamp[P̄ + clamp(I − P̄, ±0.05 %), cap, floor] | F = clamp[(P̄ + clamp(I − P̄, ±0.05 %)) ÷ (8/N), cap, floor]. The ÷(8/N) factor and fixed I apply **from 2026-06-01** |
| interest I | 0.01 % / 8 h, i.e. 0.00125 %/h, **10.95 %/yr to shorts** | 0.01 % / 8 h (0.03 %/day) | 0.01 % / 8 h | 0.01 % fixed (before 2026-06-01: 0.03 % ÷ (24/N)) |
| premium P | impact price vs **oracle**, sampled every 5 s, averaged over the hour | impact bid/ask vs index, every 5 s, time-weighted (later samples weigh more) | weighted toward settlement | impact price vs index, per minute, weighted moving average |
| cap | **4 %/h**, the same for all assets | ±0.75 × MMR at max leverage for listed majors; ±2 % for others ("may be adjusted"). Observed **±3 %** prints in 2023 | tied to margin rates; observed **−4 %** (DOODUSDT 2025-10-07) | per contract, adjusted in real time; observed ±3 % and ±2 % |
| payment basis | size × **oracle** price × rate | mark × size | mark × size | mark × size |
| predicted rate before settlement | **yes**: `info {type: predictedFundings}` gives HL, Binance and Bybit next rates with `nextFundingTime`; `metaAndAssetCtxs.funding` is the live estimate | yes: `premiumIndex.lastFundingRate` is the live estimate, plus `nextFundingTime` (the UI shows "estimate of the last 8 h of premium") | yes: the ticker `fundingRate` is the live estimate (from memory, not re-verified) | yes since current-period collection. **Before 2024-01** OKX used *cross-period* collection, so the rate charged was fixed one period ahead (§F caveat) |
| source | hyperliquid.gitbook.io …/trading/funding; …/api/info-endpoint/perpetuals | binance.com FAQ 360033525031 | bybit.com help "Funding fee calculation" (via search) | okx.com/en-us/help/perps-funding-fee-mechanism; …/important-update-revision-of-the-funding-rate-formula…; …/okx-to-adjust-funding-rate-interval… |

**What matters for the trade:**
1. Every venue has the same 0.01 %/8 h interest leg and ±0.05 % clamp. Whenever the premium sits inside the dead zone, both legs print
   the baseline 10.95 %/yr and the **spread is exactly 0**. §C shows 0.00 % at the p25 or p50 in many cells. The carry exists only
   when a premium leaves the dead zone on one venue and not on the other.
2. Timing: HL accrues every hour, a CEX at its timestamp (00/08/16 UTC, or every 4 h). A pair must straddle the CEX timestamp to
   collect that leg. Entering and exiting between CEX timestamps captures only the HL side.
3. HL settles on the oracle (spot) price, CEXs on mark. In a dislocation the two legs' funding cash flows are computed on different
   prices.

## E. Benchmarks (not our hypothesis; returns may be read, end < 2026-07-01)

**Ethena sUSDe**, a packaged funding/basis product. DefiLlama's yields chart (`yields.llama.fi/chart/{pool}`) has **no
date bound**, so I did **not** call it. Instead, `l2_benchmarks.py` reads `sUSDe.convertToAssets(1e18)` via `eth_call`
pinned to the block at each month start. The block comes from `coins.llama.fi/block/ethereum/{ts}`, which is bounded by its timestamp. The last
point is 2026-06-30T23:00Z (block 25,433,640). These are realised exchange-rate returns, not advertised APYs.

| year | span read | sUSDe realised, annualised |
|---|---|---|
| 2023 | 2023-12-01 → 2024-01-01 | 0 % (rate stayed at 1.0000 until 2024-02-01; no accrual on-chain) |
| 2024 | 2024-02-01 → 2025-01-01 | **15.7 %** (14.3 % over the calendar year) |
| 2025 | 2025-01-01 → 2026-01-01 | **6.2 %** |
| 2026 H1 | 2026-01-01 → 2026-06-30 | **4.0 %** |

For comparison: HLP was about 2–4 %/yr outside spiral fortnights, and its lending leg about 3 %. Binance BTC funding median was **9.1 %
(2023), 10.95 % (2024), 5.3 % (2025), 1.7 % (2026 H1)** (§C2). The packaged product's yield fell with the funding level.

**Funding-rate markets.** Pendle **Boros** (Arbitrum) launched in 2025 with Binance BTC/ETH funding markets. Press reports
say it opened **Hyperliquid BTC and ETH funding markets on 2025-09-12** ([ChainCatcher](https://www.chaincatcher.com/en/article/2205150)),
and later added OKX and an HL HIP-3 SK Hynix funding market ([Blockworks](https://blockworks.co/news/boros-sleeper-perps)). I found no
HYPE funding market. I did not verify this on the Boros app. Its history is too short for discovery before 2026-07, and I read no
Boros returns.

## F. Tail-risk ingredients (behaviour; no P&L)

**F1. Largest |funding| prints in the loaded sample.** This is core over the full span and others in the windows, plus each coin's first
500 HL hours. Rates are per settlement, with per-hour in brackets.

| venue | largest prints (any coin) | largest core prints |
|---|---|---|
| HL | CYBER −2.94 %/h (2023-09-02 09:00); HYPER −2.49 %/h (2025-07-10 15:00); AZTEC −2.26 %/h (2026-02-20 09:00); LOOM −2.24 %/h (2023-10-15 14:00) | SOL −0.205 %/h (**2025-10-10 22:00**), −0.132 %/h (23:00), −0.069 %/h (2025-10-11 00:00) |
| Binance | LOOMUSDT −3.00 % (2023-10-15/16, four settlements, at the ±3 % cap); MINAUSDT −3.00 % (2023-10-24) | SOLUSDT −0.93 % (2023-01-04 08:00, before HL's start), −0.82 % (2023-01-03 16:00) |
| Bybit | DOODUSDT −4.00 % (2025-10-07 08:00); LOOMUSDT −3.00 % (2023-10-14/16, five settlements) | SOLUSDT −0.86 % (2023-01-04), **−0.50 % (2025-10-11 00:00)** |
| OKX | UMA −3.00 % (2024-01-21), TRB −2.99 % (2024-01-01), MINA −2.99 % (2023-10-25), GAS −2.00 % (2025-04-10) | SOL −1.08 % (2023-01-04 16:00), **−0.42 % (2025-10-11 00:00)** |

**F2. HL cap events.** There were **0 prints at the 4 %/h cap** in the loaded sample (966k HL coin-hours). 51 prints were ≥ 1 %/h.
The cap is not where HL's tail lives; it lives in single listings (CYBER, LOOM, HYPER, AZTEC). The extreme prints on every venue are negative
(shorts paying), and they cluster on alt squeezes and on 2025-10-10/11.

**F3. Sign changes per month** of the interval-level spread (HL accrued over each 8 h CEX interval minus the CEX rate; about 93
intervals per month). The cells give the median count per month, with the max in brackets. This is a count over the level series, not a persistence measure.

| coin | venue | 2023 (8 months) | 2024 | 2025 | 2026 H1 |
|---|---|---|---|---|---|
| BTC | Binance / Bybit / OKX | 10 (13) / 9 (19) / 19 (34) | 8 (21) / 12 (17) / 23 (27) | 12 (27) / 10 (26) / 13 (28) | 23 (28) / 26 (32) / 28 (31) |
| ETH | Binance / Bybit / OKX | 13 (15) / 12 (21) / 19 (40) | 13 (25) / 14 (23) / 23 (30) | 14 (28) / 16 (28) / 12 (35) | 19 (28) / 21 (34) / 26 (42) |
| SOL | Binance / Bybit / OKX | 11 (14) / 9 (24) / 29 (37) | 14 (27) / 12 (17) / 25 (36) | 18 (30) / 20 (30) / 24 (33) | 34 (36) / 28 (36) / 35 (38) |

OKX's counts are higher. That is partly the alignment caveat: before 2024-01 OKX charged the rate set in the previous period, and the
file's timestamp convention for that era was not verified. Sign changes rose in 2026 H1, when spreads compressed toward 0.

**F4. Venue incidents (public sources, all before 2026-07-01).**

| date | venue | event | source |
|---|---|---|---|
| 2025-02-21 | Bybit | about $1.5 B ETH stolen from a cold wallet (attributed to Lazarus); Bybit stated it was solvent | [Cointelegraph](https://www.cointelegraph.com/news/bybit-exchange-hacked) |
| 2025-03-26 | HL | JELLY manipulation put HLP at risk ($12–20 M est.). Validators delisted JELLY and settled the market; the Foundation refunded longs at 0.037555 | [The Defiant](https://thedefiant.io/news/defi/hyperliquid-to-compensate-jellyjelly-traders-and-strengthen-risk-protocols) |
| 2025-07-29 14:10–14:47 UTC | HL | API servers overloaded and orders delayed; about $2 M refunded | [Cointelegraph](https://cointelegraph.com/news/hyperliquid-reimburses-2m-crypto-traders-outage), [The Block](https://www.theblock.co/post/364861/hyperliquid-outage-refund) |
| 2025-10-10/11 | HL | **first cross-margin ADL in over two years**. Our data: SOL funding −0.205 %/h at 22:00 | [Wublock](https://wublock.substack.com/p/hyperliquid-activates-cross-margin) |
| 2025-10-10 21:36–22:16 UTC | Binance | USDe, wBETH and BNSOL depegged on Binance only (USDe < $0.66) and collateral was liquidated; $283 M compensation. A cross-venue pair margined in such collateral would have lost a leg | [The Block](https://theblock.co/post/374295/binance-pays-283-million-in-compensation-following-fridays-depegs-covering-user-losses) |
| 2025-11-12 | HL | POPCAT manipulation; HLP lost about $4.9 M; deposits and withdrawals were paused | [Cointelegraph](https://cointelegraph.com/news/hyperliquid-hlp-popcat-attack-3m-wipeout), [The Block](https://www.theblock.co/post/378606/hyperliquid-pauses-deposits-withdrawals-popcat-trading-scheme-speculation) |
| 2021-07-30 | Binance | derivatives wound down for Germany, Italy and the Netherlands (access, §H) | [The Block](https://www.theblock.co/post/113068/binance-crypto-derivatives-trading-europe-shut-down) |

CEX ADL event counts are not published, so I could not establish them. The 2025-10-10 cluster is the template for the "leg liquidated in a spiral" risk:
- the short leg faces a squeeze at the same time as funding goes deeply negative;
- withdrawals (HL, 2025-11-12) and collateral (Binance, 2025-10-10) can fail at the moment a top-up is needed.

## G. Data sizes for a full registration pull (all overlapping coins, 2023 → 2026-06)

| source | scope | requests / files | bytes | time / note |
|---|---|---|---|---|
| HL `fundingHistory` | 220 coins, from each coin's first print to the cutoff: ≤ 3.99 M coin-hours | ≈ 8,000 POSTs (500 rows each) | ≈ 170 MB JSON (about 42 B a row) | ≈ 5–6 h at 1,100 weight/min. Stop at a coin's delisting: rows of exact zeros continue after it |
| Binance `fundingRate` monthly | 213 coins | ≈ 5,300 GETs | ≈ 5.6 MB (about 1 KB per file) | minutes |
| Bybit `funding/history` | 214 coins | ≈ 2,550 requests if all 8 h; up to about 2–4× that with 4 h / 1 h symbols | ≈ 20–40 MB JSON | ≈ 15–40 min at 4 req/s |
| OKX monthly funding files | 174 coins | ≈ 4,300 GETs | ≈ 6.1 MB | minutes; months in UTC+8 |
| Binance 1h klines, spot + perp | core: 3 coins × 42 months × 2 | 252 | 9.7 MB | (pulled) |
| same, all overlapping coins | 213 × about 38 months × 2 | about 16,000 | **≈ 0.6 GB** | approaching the 1 GB stop rule |
| Binance `bookDepth` (impact at size) | 6 coins, 1 year (h2 scout) | daily | ≈ 1.0 GB | only if a registration needs impact |

This scout downloaded **93 MB** in total under `data/scout_l2/` (HL 50 MB, Binance 19, OKX 12, Bybit 11).

## H. Lawful access from Germany (scope addition, 2026-10-11)

This section is not legal advice. The verdicts come from public reporting and venue pages as of 2026-10-11. The background is:

- **ESMA, 2026-02-24:** derivatives marketed as "perpetual futures" are "likely" in scope of the CFD product-intervention measures (leverage caps, negative-balance protection, margin close-out). A funding-rate feature does not change that ([Finance Magnates](https://www.financemagnates.com/forex/regulation/esma-tells-firms-perpetual-futures-fall-under-eu-cfd-rules)).
- **MiCA:** offshore spot providers had to wind down for EU clients by 2026-07-01. Perps sit outside MiCA, under MiFID ([CoinDesk opinion, 2026-07-01](https://www.coindesk.com/opinion/2026/07/01/europe-is-closing-the-door-on-offshore-crypto-but-it-s-leaving-the-riskiest-window-open)).
- **Who is bound:** German licensing law binds the provider. I found nothing that prohibits the client as such. But a venue whose own terms exclude German residents can only be used by breaching those terms or misdeclaring residence. I treat that as **not usable**.

| venue | verdict for a German resident | why | citation |
|---|---|---|---|
| **Binance** (USDⓈ-M) | **Research data only** | Binance stopped new derivatives accounts for Germany, Italy and the Netherlands on 2021-07-30, with a 90-day close-out for existing users | [The Block 2021-07-30](https://www.theblock.co/post/113068/binance-crypto-derivatives-trading-europe-shut-down) |
| **Bybit global** | **Research data only** | Bybit announced it would progressively limit Global services for EEA residents, Germany included, and move them to Bybit EU | [AMBCrypto](https://ambcrypto.com/bybit-to-phase-out-global-services-for-eea-users-as-mica-migration-begins/) |
| **Bybit EU** | **Not available yet** | Its MiCA licence (Austria FMA, 2025-05-28) covers spot only. The MiFID licence for Bybit X GmbH was still pending in 2026-09 (CEO estimate: about November 2026). No perps | [Finance Magnates](https://www.financemagnates.com/cryptocurrency/bybit-pursues-mifid-license-for-eu-derivatives-phases-out-mt5-for-in-house-trafi/), [CASP tracker](https://casptracker.eu/exchange/bybit/) |
| **OKX global** | **Research data only** | EEA clients are served by OKX's European entities, and global OKX perps are not offered to them | inferred from OKX EU pages; not verified in a ToS |
| **OKX Europe X-Perps** | **Lawful candidate; check Germany is a "supported European country" in-app** | OKX Europe Markets Ltd is MiFID-authorised by the MFSA (Malta) and passported across the EEA. It launched 2026-04-15 with BTC, ETH, SOL, XRP, ADA, DOGE, PEPE, LTC, PUMP and SUI, then added TAO, BNB, HYPE, LINK and TRX in 2026-05. Up to 10× leverage (a retail CFD cap of 2:1 may apply), negative-balance protection, a 60-month expiry with cash settlement. **Funding every 8 h** with multi-asset margin. The formula is **not published** on the X-Perps pages ("identical to standard perp UX"). Fees 2 / 5 bp | [okx.com/en-eu/x-perps](https://www.okx.com/en-eu/x-perps), [OKX: X-Perps vs perps](https://www.okx.com/en-eu/learn/x-perps-vs-standard-perpetual-futures), [BlockEden 2026-04-17](https://blockeden.xyz/blog/2026/04/17/okx-x-perps-europe-mifid-5-year-expiry-perpetual-futures/) |
| **Bitget** | **Not usable (per reporting); the owner should check their own account** | Reports say Bitget's Terms of Use (dated 2026-08-14) name Germany, France and Austria as prohibited, and a German IP gets a "Restricted IP" popup. The Bitget EU MiCA application with the Austrian FMA (2026-06-17) is pending | [Datawallet](https://www.datawallet.com/crypto/bitget-restricted-countries), [Finance Magnates MiCA day one](https://www.financemagnates.com/cryptocurrency/regulation/mica-day-one-we-tested-how-europes-unlicensed-exchanges-treat-new-users/), [CASP tracker](https://casptracker.eu/exchange/bitget/) |
| **Hyperliquid** | **Accessible; unlicensed in the EU; legal grey zone, needs the owner's decision or counsel** | No KYC. Secondary summaries of its terms restrict US persons, Ontario and sanctioned territories, and **do not list Germany**. `app.hyperliquid.xyz/terms` returned no text (403/JS), so this is **unverified**. No EU authorisation; ESMA's statement applies to firms offering perps | [CoinDesk op-ed](https://www.coindesk.com/opinion/2026/07/01/europe-is-closing-the-door-on-offshore-crypto-but-it-s-leaving-the-riskiest-window-open), [fintelegram](https://fintelegram.com/defi-bingo-hyperliquids-billion-dollar-machine-defis-most-profitable-trading-startup-faces-the-license-question/) |

**What this does to L2.** As of today, the only cross-venue pair a German resident can plausibly run is **Hyperliquid
(grey zone) × OKX Europe X-Perps**. That pair has **no pre-2026-07 history worth a discovery set**: X-Perps launched on
2026-04-15, and I found no free, date-bounded funding history for it. Binance, Bybit and OKX global funding stay in this scout as
**reference series**: the market-wide spread an X-Perps leg would face, not a tradable leg. The same applies to the perp–spot basis
trade on Binance (§C2). A perp–spot pair on lawful venues would be X-Perps short against spot held at a MiCA-licensed venue.

**Bitget as a CEX leg in A–C (scope addition).** It cannot be added with data:
- Bitget's public funding history (`/api/v2/mix/market/history-fund-rate`, as used by `ct/bitget.py`) takes only
  `pageSize`/`pageNo`, pages back from the newest settlement, and keeps about 90 days. It **cannot take a date bound**, and every page
  it would serve today is holdout-dated, so it was **not called**.
- The local cache `data/cache/bg-funding` holds **0 pre-2026-07 rows** (see the incident above).
- Tardis.dev has free first-of-month Bitget files from 2024-11, but it is a new vendor and needs owner approval.

Bitget therefore appears in §A as "no bounded free history" and is absent from §C.

## Reproduction

```bash
cd research/copytrade
# local HL coin list (archive, discovery year, timestamps < 2026-07-01) -> data/scout_l2/hl_archive_coins.json
#   (one-off DuckDB query; see "A" for the rule)
.venv/bin/python -I scouts/l2_fetch.py universe   # 907 candidates -> HL probes, ~45 min
.venv/bin/python -I scouts/l2_fetch.py hl         # BTC/ETH/SOL full span + 12 windows x 220 coins
.venv/bin/python -I scouts/l2_fetch.py binance    # data.binance.vision monthly fundingRate
.venv/bin/python -I scouts/l2_fetch.py okx        # static.okx.com monthly funding files
.venv/bin/python -I scouts/l2_fetch.py bybit      # /v5/market/funding/history, bounded
.venv/bin/python -I scouts/l2_fetch.py klines     # Binance spot + USDT-M 1h klines, core
.venv/bin/python -I scouts/l2_spreads.py          # spread proxy, local tapes (~5 min)
.venv/bin/python -I scouts/l2_benchmarks.py       # sUSDe exchange rate at month starts
.venv/bin/python -I scouts/l2_analyze.py > data/scout_l2/analysis.txt
```

## What I could not determine

- **Market impact at size** and quoted CEX spreads on ordinary days. The free Binance `bookTicker` ends 2024-03, and local
  aggTrades cover only HL-cascade days. Binance `bookDepth` could measure impact (§G).
- **Binance and OKX maintenance-margin tiers** per symbol. The fee and leverage pages did not render, and Binance's bracket endpoint needs a key.
  §B4 uses approximate CEX MMRs.
- **Bitget funding before 2026-07.** No bounded free source exists (§H). Tardis.dev's free first-of-month files would mean a new vendor.
- **OKX Europe X-Perps:** its funding formula and cap, its history, and whether Germany is on its supported-country list.
- **Hyperliquid's terms text.** The page did not render, so the restricted-jurisdiction list is from secondary sources.
- **OKX timestamp convention before 2024-01** (cross-period collection). This affects which interval a rate belongs to, which matters for F3
  counts but hardly for quantiles.
- **Bybit's live-estimate field** (`tickers.fundingRate`) was not re-verified in its docs (the page timed out).
- **HL coins delisted before 2025-07-28 that never had a Binance USDT-M perp** are not in the candidate set.
- **CEX ADL event counts.**
- **Whether an inactive CEX symbol** prints a constant baseline rate. 89 paired windows were flagged and kept (§A).
- **Lawfulness for the owner** is a legal question. §H gives reported facts, not advice.

## Rules I was unsure about

- **Listing and enumeration endpoints without a date bound.** I listed the names under the S3 prefix
  `data/futures/um/monthly/fundingRate/`, which is names only and reveals symbols listed after 2026-07 (they were then
  probed with bounded calls, which returned nothing). The h2 scout set this precedent. I did **not** call `meta`, `exchangeInfo`,
  Bybit `instruments-info` or `risk-limit`, DefiLlama `yields`, Bitget `history-fund-rate` or OKX `funding-rate-history`, because
  none takes an end bound.
- **The sign-change count (F3)** is a count over the level series, as the brief allows. It still describes how often the series
  crosses zero. The orchestrator may prefer to drop it from anything that sets a registration threshold.
- **Web pages** (venue docs, news) describe the present, including events after 2026-07-01: X-Perps pairs added in 2026-05, Bitget ToS of
  2026-08-14, Bybit's MiFID status in 2026-09. They are regulatory and mechanical facts, not market data, and no threshold here uses them.

## Holdout hygiene

Every script drops rows with timestamp ≥ 1782864000000 (2026-07-01T00:00Z) at load, before any print or save. In this
scout's own data the dropped-row counts were **0 for every source** (`analysis.txt`, last line). The one exposure was the
pre-existing Bitget cache above.

| endpoint | calls | end bound |
|---|---|---|
| `POST api.hyperliquid.xyz/info` `fundingHistory` | 907 probes, 3 core full-span pulls, about 2,000 windows, plus 6 manual curl probes | `endTime` ≤ 1782863999999 for scripted calls. Manual probes ended 2023-05-02 (FOOBAR, 1000000BOB, 0G, ZZZ, ETH) or at the cutoff − 1 ms (BTC) |
| `data.binance.vision` monthly `fundingRate`, spot and USDT-M `klines/1h` | 2,646 + 252 GETs | file month ≤ 2026-06 (asserted in code) |
| S3 listing `data/futures/um/monthly/fundingRate/` (delimiter `/`) | 1 paginated listing | none: **names only**, no file listing and no contents |
| `api.bybit.com/v5/market/funding/history` | about 2,600 + 4 manual | `endTime` ≤ 1782863999999 (manual probes: 2020-04-02 and 2023-06-04) |
| `www.okx.com/api/v5/public/market-data-history` (module 3) | 1 | `end` = 1711929600000 (2024-04-01) |
| `static.okx.com/…/swaprates/monthly/…` | 2,730 GETs | file month ≤ 2026-06 (UTC+8); last row 2026-06-30 08:00 UTC |
| `coins.llama.fi/block/ethereum/{ts}` | 33 | ts ≤ 1782860400 (2026-06-30T23:00Z) |
| `eth_call` sUSDe `convertToAssets` (drpc, plus one probe each on publicnode, llamarpc, ankr, cloudflare, 1rpc, merkle, tenderly, blastapi) | 33 + 9 | block ≤ 25,433,640 (2026-06-30T23:00Z) |
| local: HL pruned archive (coin list, spread proxy) | — | `timestamp < 2026-07-01` / days ≤ 2026-06-15 |
| local: Binance aggTrades parquet (spread proxy) | — | files `date < 2026-07-01` |
| local: `data/cache/bg-funding` | 1 accidental print, then 1 count-only read | **exposure: 15 holdout rows printed (ARBUSDT, 2026-07-09/14)**, see above |
| not called (no end bound) | — | HL `meta` / `predictedFundings` / `l2Book`, Binance `exchangeInfo` / `premiumIndex` / `leverageBracket`, Bybit `instruments-info` / `risk-limit` / `tickers`, OKX `funding-rate(-history)`, Bitget `history-fund-rate`, DefiLlama `yields` |

Web pages read (docs and news, not market data): hyperliquid.gitbook.io (fees, funding, margining, liquidations, ADL, info
endpoint, onboarding), binance.com FAQ, okx.com help and X-Perps pages, and the news and secondary sources cited in §B, §D, §E, §F and §H.
