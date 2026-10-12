# N2 scout: options against the liquidation map

2026-10-12. This is a behaviour-only feasibility scout for light N2
(`docs/edge-discovery-next-prompt-2026-10-12.md`). It covers:

- data coverage and size;
- costs, taken as trade price minus the contemporaneous mark;
- unconditional signal frequencies;
- the **coincident** association between the map/stress state and the implied-vol level and skew;
- unconditional distributions of DVOL changes and underlying moves, used for power and tail;
- verified literature.

**No option payoff, no P&L of any rule, no forward realised vol, no VRP on our windows and no
statistic conditioned on a future quantity was computed.** One possible indirect exposure is
disclosed in §9 (two unconditional numbers printed side by side).

**Scripts:**

- `research/copytrade/scouts/n2_net.py`: guarded HTTP. Every request needs an end bound before
  2026-07-01, and any row at or after the cutoff is a hard error. Errors raise and are never cached.
  Each pull goes to a new directory, with SHA256SUMS.
- `n2_deribit.py`: DVOL, perpetual candles, option trades, and the option trades in the hour
  before each map snapshot.
- `n2_bybit.py`: Bybit hourly OI.
- `n2_binance_options_list.py`: S3 listing only.
- `n2_analyze.py`: every number below.

Output: `research/copytrade/data/scout_n2/analysis_all.txt`.

**Data:** `research/copytrade/data/scout_n2/` (gitignored), 114 MB in total. Every directory's
SHA256SUMS was re-verified OK.

**Holdout A was skipped on purpose.** No Deribit, underlying or proxy row dated 2024-01-01 →
2025-07-27 was pulled or read. This keeps 009's released-and-unread holdout A clean as the
candidate N2 holdout.

---

## Summary (key numbers)

1. **Data: free and keyless enough for a full test.**
   - Deribit `history.deribit.com` returns every option trade back to at least 2019-07. Each trade
     carries price, mark, IV and index, and the request takes start/end bounds.
   - DVOL (hourly) runs from **2021-03-24**.
   - The full BTC+ETH option tape for 2021-03 → 2023-12 plus the HL year is about **7–12 GB raw
     JSON (≈1–2 GB as trimmed parquet), 30–55k requests, 3–5 h**. That is above the 1 GB scout
     line and above 5 GB raw, so it needs the owner's approval, or the alternative below.
   - A design-sized pull (one hour of trades at each entry time) is **≈55 MB a year**.
   - Tardis free first-of-month files returned **HTTP 403 (Cloudflare)** from this host.
   - Binance and OKX option history is short (2023 →) and thin. Bybit has none.
2. **Costs decide (a).**
   - Deribit's flat fee is 0.0003 BTC/ETH per contract, capped at 12.5% of the premium. For
     1–7-day options at 5–15 delta that cap binds: the **median fee is 8–12.5% of premium**.
   - The median effective half-spread against the mark is **2–7% of premium (0.3–2 IV pts)** on
     those wings, and **0.4–2.5% (0.2–2.3 IV pts)** at 30–45 delta.
   - A 5–15Δ wing costs **≈10–20% of premium per fill** (one fill if held to expiry, 15–25% for a
     round trip). A near-ATM 3–7-day straddle sold and held costs about **1.5–4% of premium**.
3. **Signal frequencies (HL year, 286 daily snapshots).**
   - BTC long-side map density within 2% (+0.5%) of price has a median of 2.0% of gross HL
     notional and p90 of 5.3%. It is ≥ 5% on **33/286 days** and ≥ 10% on 6.
   - ETH is much sparser: p90 2.8%, ≥ 5% on 6 days.
   - Market stress: the $77 M figure is p99.9 of **non-zero** 5-min buckets. Over all clock
     buckets p99.9 is $47.6 M.
   - Buckets of **≥ $77 M occur on 29/338 days and in 18/49 weeks. ≥ $15 M occurs on 165/338
     days and in 44/49 weeks.** A "low-stress week" filter would therefore leave very few weeks.
4. **Coincident pricing: the options do not "see" the map's side, and a dense BTC map coincides
   with LOW implied vol.**
   - Spearman(BTC L+S density, DVOL at the snapshot) is **−0.46**. Relative to its own trailing
     30-day median it is −0.32.
   - The median DVOL is 47.6 when the map is sparse and 39.0 when it is dense.
   - For ETH the figures are −0.05 and −0.11.
   - A skew proxy built from Deribit marks (10–40Δ call IV minus put IV, about 6.5-day tenor,
     from trades in the hour before the snapshot) has a median of −4.3 pts (BTC). It does not move
     with the map's long-minus-short imbalance (ρ −0.01 BTC, −0.10 ETH).
   - DVOL in the same hour as HL stress hardly moves. In ≥ $77 M hours the median same-hour change
     is +0.4 pts (BTC) and +0.7 pts (ETH).
   - A dense map is partly a low-vol regime marker. Whether the options are cheap *relative to
     what follows* cannot be read here; reading it is the test.
5. **Power is poor for (a) and marginal for (b).**
   - The unconditional payoff of a 1-sd OTM weekly strangle has sd/mean 2.7 (BTC) and 2.8 (ETH).
     For a 1-sd put it is 4.9.
   - **The top 3 weeks out of about 300 carry 16–20% of all strangle payoff and 38–40% of all put
     payoff.**
   - A weekly ATM straddle's payoff has sd/mean of about 1.0.
   - Minimum detectable effects (MDE) at 80% power:
     - (a) on HL-year weekly trades: ≈ 1.5 premiums per trade;
     - (a) on HL-year daily trades: ≈ 0.7–1.1;
     - (b) weekly over three years: ≈ 0.22 of premium. That is twice the published *unconditional*
       VRP (about 0.10 of premium).
6. **Tail for (b), short weekly ATM straddle.** This uses underlying moves only, with a constant
   premium of 7.0% (BTC) and 9.0% (ETH) of spot.
   - Worst week: **−35% (BTC, 2020-03) and −55% (ETH, 2021-01 rally)** of notional.
   - Worst 4 weeks: **−61% and −73%** (the 2020-12 → 2021-01 rally).
   - At ¼ Kelly (≈0.3× notional per unit of equity): worst week −11% / −16%, worst month −18% /
     −22%.
   - The HL year's worst week was 2026-01-30: BTC −21.7%, ETH −30.7%.
7. **Odds** (§10):
   - (a): effect exists **~10%**, deployable **~4%**.
   - (b): a map/stress increment over plain VRP exists **~15%**, deployable as a filtered
     short-vol book **~20%**, at about 8–15%/yr with −20 to −45% worst months.
   - Most of (b)'s return would be the already-known VRP, not the map.

---

## 1. Data sources

All requests were end-bounded before 2026-07-01 by the `n2_net` guard. "Keyless" means no account
and no key.

| source | endpoint / URL | keyless? | coverage (probed) | size / effort | pulled here |
|---|---|---|---|---|---|
| **Deribit option trades** | `https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time` (`currency`, `kind=option`, `start_timestamp`, `end_timestamp`, `count≤1000`, `sorting=asc`, `include_old=true`) | yes | BTC rows from **2019-07-01**. Earlier dates not probed; a third-party README says "since the start of the platform". Fields: `trade_id, trade_seq, timestamp, instrument_name, price, mark_price, iv, index_price, direction, amount, tick_direction`. **There is no liquidation flag** (a 10-min BTC-PERPETUAL probe on 2022-06-13 showed none) | Per day, BTC 5.5k–41k trades and ETH 2.9k–41k trades. About 230 B per trade raw, about 1 request per 1,000 trades. ~6 MB/day raw on the sample-day mean | 7 full days × 2 coins (`deribit_trades_sample/`, 69 MB, 290 requests): 2021-06-15, 2022-06-15, 2022-11-09, 2023-06-15, 2025-10-10, 2026-03-16, 2026-06-15. Plus the hour before each of the 286 HL map snapshots (`deribit_snapwin_1h/`, 35 MB trimmed, 601 requests, BTC 116,813 and ETH 57,887 trades) |
| **Deribit DVOL** | `www.deribit.com/api/v2/public/get_volatility_index_data` (resolution 3600) | yes | **2021-03-24 →** for BTC and ETH, 32,424 hourly rows each, with one gap > 1 h. The docs say the `continuation` pages backwards, so the fetcher uses fixed 30-day forward windows and errors if `continuation` is set | 1.3 MB per coin, 47 requests | `deribit_dvol_1h/`: 2021-03-24 → 2023-12-31 and 2025-07-28 → 2026-06-30 |
| **Deribit perpetual candles** | `www.deribit.com/api/v2/public/get_tradingview_chart_data` (resolution 60) | yes | BTC-PERPETUAL from 2019-01-01, ETH from 2019-03-14 | 6.3 MB | `deribit_perp_1h/`: 2019 → 2023-12-31 and 2025-07-28 → 2026-06-30. Hourly, because **1D candles are stamped 08:00 UTC**, so the 2026-06-30 candle would run into the holdout (see §9) |
| Deribit instrument list | `public/get_instruments?expired=true` | yes | **not called**: it takes no date bound. Instrument coverage was taken from the trades instead (229–711 instruments/day in the samples) | — | — |
| Deribit settlement prices | `public/get_delivery_prices` | yes | **not called**: it has only offset/count and returns newest first, which would read the holdout. A test must settle from the trades' `index_price` (or perp/index candles) over 07:30–08:00 UTC, or call it with a guard that drops post-cutoff rows unread | — | — |
| Tardis.dev | `https://datasets.tardis.dev/v1/deribit/options_chain/YYYY/MM/01/OPTIONS.csv.gz` (first of the month free per its docs) | docs say yes | **HTTP 403 from Cloudflare** for every probe from this host (plain GET, range GET, browser UA). The `api.tardis.dev/v1/exchanges/deribit` metadata (200) lists `options_chain`, `quotes`, `trades` and `book_snapshot_25` for OPTIONS since 2019-03-30 | Paid plans for options are $350–3,000/month: skip | nothing |
| Binance options | `data.binance.vision/data/option/daily/{BVOLIndex, EOHSummary}` | yes | BTCBVOLUSDT 2023-06-20 → **2025-02-18** (604 files, 263 MB). EOHSummary BTC/ETH/BNB/XRP/DOGE 2023-05-18 → **2023-10-23** (147 files for BTC). Zero files on or after the cutoff | — | listing only. Too short to use |
| OKX options | `www.okx.com/api/v5/public/market-data-history?module=1&instType=OPTION&instFamilyList=BTC-USD` → `static.okx.com/.../BTC-USD-optionchain-trades-YYYY-MM-DD.zip` | yes | daily files from **≈2023-01** (none for 2022-10-03 or earlier). 0.01–0.11 MB/day, which is thin | — | listing only |
| Bybit options | `public.bybit.com` | — | no options directory (futures/spot only) | — | — |
| **Bybit OI** (signal proxy) | `api.bybit.com/v5/market/open-interest` (1h; 5 min also exists per L4) | yes | BTCUSDT from 2020-08-04, ETHUSDT from 2020-10-21. No gaps | 295 requests, 1.5 MB | `bybit_oi_1h/`: → 2023-12-31 |
| Binance USDⓈ-M metrics/klines/funding (signal proxy) | local `binance_years/panel` | — | 2021-11 → 2023-12-31. The panel's bars run to 2024-01-01 23:00 and were **cut at 2024-01-01 00:00** in every query | local | — |
| Other free multi-year liquidation record | — | — | **none** (L4 §E holds). Deribit's history trades carry no liquidation flag. The only free multi-year pressure proxy is OI drops + same-hour return + taker share | — | — |

**Size of a full Deribit pull.** BTC+ETH option trades for 2021-03-24 → 2023-12-31 plus
2025-07-28 → 2026-06-30 cover 3.7 years.

- At the calm-day rate (BTC ~12–14k and ETH ~9–11k trades a day) that is ≈7 GB raw.
- At the sample mean (BTC 22k, ETH 18k; the sample is stress-biased) it is ≈12 GB raw.
- It takes 30–55k requests. The scout saw no HTTP 429 at about 3–8 requests a second, so 3–5 h.
- Trimmed to 8 fields as parquet it is ≈1–2 GB.

**A design-sized alternative.** Pull one hour of trades before each entry time, as
`deribit_snapwin_1h` did, plus a 07:30–08:00 window on each expiry day for settlement. That is
≈55 MB a year, or ≈0.2 GB in total, inside the free-pull rules.

## 2. Costs

**Public docs, verified by the helper:**

- Deribit options fee: 0.03% of underlying (0.0003 BTC/ETH per contract) for maker and taker
  since 2020-08-24, **capped at 12.5% of the option price** (insights.deribit.com/?p=2277; cap per
  CoinGecko; Deribit's own formula page returned 403).
- Delivery fee: 0.015%, also capped at 12.5%, and 0 for daily options (Coincentral 2022).
- A new fee schedule takes effect 2026-08-01. Its tier table is an image and was not read.
- Settlement: 08:00 UTC, with dailies since 2020-02-03.

**Measured (contemporaneous):** trade price against Deribit's own mark at the same trade, in
1–7-day options, on 7 sample days. "Effective half-spread" is (price − mark) signed by the taker's
direction. Mark IV was inverted from the mark price with Black-76 on the index.

| tenor | \|Δ\| | BTC half-spread % of premium (p50, p75) | BTC in IV pts (p50) | BTC fee % of premium (p50) | ETH half-spread % (p50, p75) | ETH fee % (p50) |
|---|---|---|---|---|---|---|
| 0–1 d | 0.05–0.15 | 7.4, 18.4 | 1.4 | **12.5** | 6.1, 17.1 | 12.0 |
| 0–1 d | 0.15–0.30 | 2.9, 7.9 | 1.1 | 8.1 | 3.5, 8.2 | 4.6 |
| 0–1 d | 0.30–0.45 | 1.5, 5.2 | 0.9 | 4.3 | 2.5, 5.3 | 2.1 |
| 1–3 d | 0.05–0.15 | 4.5, 12.2 | 1.4 | 8.6 | 3.4, 9.2 | 6.0 |
| 1–3 d | 0.15–0.30 | 0.9, 4.4 | 0.3 | 4.0 | 1.4, 4.1 | 2.1 |
| 1–3 d | 0.30–0.45 | 0.4, 2.4 | 0.2 | 1.5 | 1.0, 2.5 | 0.9 |
| 3–7 d | 0.05–0.15 | 2.4, 8.3 | 0.3 | **12.0** | 1.8, 5.8 | 7.9 |
| 3–7 d | 0.15–0.30 | 0.7, 4.6 | 0.2 | 4.3 | 1.0, 3.6 | 3.0 |
| 3–7 d | 0.30–0.45 | 0.4, 2.6 | 0.2 | 2.2 | 0.8, 2.3 | 1.5 |

**Notes on the cost table:**

- The **stress sample days** (2022-11-09 and 2025-10-10) gave a median half-spread of 1.6% (BTC)
  and 2.0% (ETH), the same as the calm days. Trade counts double on those days.
- The mark is Deribit's model price, not the mid, so the p25 is negative. Treat the p50–p75 range
  as the realistic taker cost.
- **Round trip, taker:**
  - a 25Δ 3–7-day wing ≈ (fee 4% + half-spread 0.7–4.6%) × 2 legs ≈ **10–17%** of premium, or
    about **5–9%** if held to expiry (no exit fill, delivery 0.015% of spot);
  - a 5–15Δ wing ≈ **15–25%**;
  - a 3–7-day ATM straddle sold and held to expiry ≈ **1.5–4%** of premium.
- **Maker:** maker fees are the same as taker on Deribit options (0.03%), so only the spread is
  saved.
- **Volume** in the 1–7-day, 5–45Δ band: 1.5k–12k BTC trades a day (2.7k–25k contracts) and
  0.8k–12k ETH trades (14k–173k contracts). A $10k–$1 M book is far below 1% of daily volume.
- **Access (warning line only):**
  - Coinbase completed its acquisition of Deribit on 2025-08-14 (The Block).
  - Retail is served via the Panama entity. VARA approval covers institutional/qualified investors.
  - Deribit is **not MiCA-authorised**, and EEA access is a grey zone (perpfinder, citing the
    ESMA register).
  - No source confirms that it is lawful for a German retail client. A venue geoblock is a
    practical fact to check at onboarding.

## 3. Signal frequencies (unconditional)

**HL map.** Data: 286 daily snapshots at about 00:00 UTC, 2025-07-29 → 2026-06-30, from
`map_levels.parquet` (snapshot_ms < cutoff asserted). Density is the share of the coin's gross HL
notional on that side whose liquidation price lies between the price and ±d (+0.5%).

| | BTC long-side | BTC short-side | ETH long-side | ETH short-side |
|---|---|---|---|---|
| 2% density p10 / p50 / p90 / max | 0.95 / 1.99 / 5.3 / 12.8% | 0.61 / 1.63 / 4.3 / 10.7% | 0.13 / 0.81 / 2.8 / 9.2% | 0.04 / 0.23 / 1.6 / 10.4% |
| days ≥ 2% / ≥ 5% / ≥ 10% (of 286) | 142 / 33 / 6 | 113 / 17 / 1 | 50 / 6 / 0 | 21 / 2 / 1 |
| 1% density p50 / p90 | 0.84 / 2.4% | 0.70 / 2.5% | 0.04 / 0.92% | 0.03 / 0.45% |

- Combined (L+S) 2% density terciles are BTC 3.43% / 5.23% and ETH 0.88% / 2.00%. "Dense" (top
  third) and "sparse" (bottom third) are about 95 days each by construction.
- HL gross notional per snapshot (p50): BTC $2.1 B, ETH $1.4 B.

**HL market-wide stress.** Data: liquidations.parquet, t < cutoff, clock 5-min buckets.

- The quantiles of **non-zero** buckets reproduce 004: p99 $15.2 M, p99.9 $76.0 M. Over all
  buckets (zeros included) the quantiles are p99 $6.3 M, p99.9 $47.6 M, and the maximum is $4.3 B
  (2025-10-10).
- **Days with any bucket ≥ $5 M / $15 M / $77 M: 253 / 165 / 29 of 338.** Weeks: 44 of 49 have a
  bucket ≥ $15 M, and 18 of 49 have one ≥ $77 M.
- The ≥ $77 M days are clustered: 2025-07-30 → 09-22 has 13, 10-10 → 11-21 has 9, 2026-01-13 →
  01-31 has 6, and 06-25 has 1.
- Excluding HLP's own auto-deleveraged rows changes nothing at these thresholds. Only the maximum
  moves, to $2.36 B.

**Multi-year proxies.** Discovery calendars only: Binance 2021-11 → 2023-12 and Bybit 2020-08 →
2023-12.

- **Market-wide hourly OI change, Binance** (price-neutral: the contract-count change weighted by
  the prior hour's OI value, about 143 symbols):
  - p0.1 −3.9%, p1 −1.6%.
  - Hours ≤ −3%: 2022 had 9 h on 9 d; 2023 had 21 h on 17 d.
  - **Data artifacts:** single-hour OI dips that revert, and partial-coverage hours (e.g.
    2022-03-07 16:00 at −99.7%, 2023-06-06 11:00 with 1 symbol). Some were removed by a revert
    rule and a coverage rule. **Some residue remains:** 2023 counts more deep drops than 2022,
    which is implausible. A registration must fix the cleaning rule in advance.
- **BTCUSDT / ETHUSDT "deleveraging hours"** (OI ≤ −3% and same-hour |return| ≥ 2%), Binance
  2021-11 → 2023: 22 h on 17 d, and 28 h on 23 d.
- **Bybit + Deribit perp, by year 2020 (partial) / 2021 / 2022 / 2023:**
  - BTC: 3 / 34 / 17 / 3 hours on 2 / 25 / 14 / 3 days;
  - ETH: 11 / 77 / 30 / 4 hours on 7 / 52 / 26 / 4 days.
  - Bybit's ≤ −5% hourly OI drops are frequent in 2020–21 (thin early market), so that threshold
    is not portable across years.
- **OI build-up** (OI / trailing-30-day mean, past-only), BTC: p90 1.11, p99 1.35. Hours ≥ 1.25:
  453 of 18,248 (BTC) and 693 of 17,533 (ETH).
- **Global long/short account ratio** p1/p50/p99: BTC 0.68 / 1.54 / 4.23, ETH 0.65 / 1.50 / 2.81.
- **Hourly taker-buy share of quote volume** p0.1/p99.9: 0.36 / 0.64.
- **Funding settlements beyond ±0.05%** (2,373 each): BTC 9 above / 2 below, ETH 6 / 9. These are
  rare.
- **There is no multi-year proxy for the map itself** (where liquidation prices sit). The proxies
  measure stress and aggregate leverage, not the nearby density.

## 4. Coincident pricing of the map (allowed: same snapshot or same hour, no later data)

**DVOL at the snapshot.** This is the last hourly DVOL close at or before the snapshot.

| | Spearman(density, DVOL) | Spearman(density, DVOL / own trailing-30d median) | DVOL p50, sparse / middle / dense third |
|---|---|---|---|
| BTC long-side 2% | −0.35 | −0.23 | |
| BTC short-side 2% | −0.12 | −0.15 | |
| **BTC L+S 2%** | **−0.46** | **−0.32** | **47.6 / 40.5 / 39.0** |
| ETH L+S 2% | −0.05 | −0.11 | 65.7 / 64.3 / 66.4 |

**Skew at the snapshot.** This uses Deribit marks of trades in the hour *before* each snapshot. The
expiry nearest to 7 days (2–14 days) is used. RR proxy = median mark IV of 10–40Δ calls minus
10–40Δ puts.

- It is usable on 247 of 286 snapshots for BTC and 206 for ETH.
- RR p10/p50/p90: BTC −11.6 / −4.3 / −0.1 pts; ETH −12.0 / −3.3 / +1.1 pts.
- Spearman with the map:

  | | long-side density | short-side density | long − short | L+S density vs ATM mark IV |
  |---|---|---|---|---|
  | BTC | 0.15 | 0.15 | −0.01 | −0.45 |
  | ETH | 0.10 | 0.36 | −0.10 | −0.07 |

**Stress against DVOL in the same clock hour.** HL year, 8,112 hours.

- Spearman(hour's max 5-min stress, DVOL close): BTC 0.03, ETH 0.12.
- Same-hour DVOL change (close − open), median (p90):

  | | < $5 M | $5–15 M | $15–77 M | ≥ $77 M (35 h) |
  |---|---|---|---|---|
  | BTC (pts) | 0.0 | +0.10 | +0.16 | +0.38 (2.3) |
  | ETH (pts) | 0.0 | +0.08 | +0.18 | +0.68 (2.1) |

**Reading.**

- Implied vol does not rise with a dense BTC map. It is *lower*, and so is ATM IV.
- The skew does not lean toward the side where the liquidations sit.
- That is what N2's premise needs: the market does not price the map's content into level or
  skew. It is equally what a regime artefact looks like. Leverage piles up close to price when
  vol is low, and low-vol regimes have cheap options for a reason.
- DVOL is a smoothed 30-day index, so its same-hour reaction to spirals is small (≤ 1 pt median).
  Short-dated IV reacts more but was not measured hourly.
- **None of this says whether the options are mispriced relative to what follows.**

## 5. Sample and power

**Trades per year (independent).**

- (a) on the HL map:
  - daily 1–3-day options on dense days: about 95 per coin, or about 120–150 effective for
    BTC+ETH, because the coins are correlated;
  - weekly 7-day options in dense weeks: about 16 per coin, or about 25 effective.
- (b): weekly sells, 52 per coin per year, minus filtered weeks. The stress filter at ≥ $15 M
  removes 44 of 49 weeks, so it must be set much higher, or by the map alone.

**Payoff dispersion (unconditional underlying moves, 307 BTC and 297 ETH non-overlapping Friday
08:00 weeks, 2019 → 2023 plus the HL year, holdout A excluded).**

- Weekly sd: 9.0% (BTC), 11.3% (ETH). Kurtosis 6.1 / 7.4.
- ATM straddle: sd(|r|)/mean(|r|) = 1.03 / 1.01. A fairly priced straddle's return on premium has
  an sd of about 1.0.
- 1-sd OTM strangle: payoff sd/mean 2.7 / 2.8. It pays in 67 of 307 weeks (BTC).
- 1-sd OTM put: sd/mean 4.9. It pays in 25 of 307 weeks.
- **Convexity: the top 1 / 3 / 5 weeks carry 7 / 16 / 25% (BTC strangle) and 19 / 38% (BTC put,
  top 1 / 3) of all payoff over 6 years.** One week decides an HL-year mean, as one hour did in
  002–004.
- DVOL unconditional, daily closes: BTC p10/p50/p90 38.5 / 59.4 / 91.9. The 1-day change has sd
  3.8 pts (p1 −9.5, p99 +13.3) and the 7-day change has sd 8.4. ETH has sd 4.3 and 9.9.

**MDE at 80% power, two-sided 5%:** MDE = 2.8 × sd / √n, in units of premium per trade.

| design | n | sd / premium | MDE |
|---|---|---|---|
| (a) HL year, weekly OTM strangles, dense weeks | ~25 | 2.7 | **1.5** |
| (a) HL year, daily options, dense days | ~150 | 3–5 (fatter at 1 d) | **0.7–1.1** |
| (a) on a multi-year proxy, weekly, 2021-03 → 2023 (top third) | ~50 per coin, ~75 eff. | 2.7 | 0.87 |
| (b) weekly ATM straddle, all weeks, 3 years | 156 | 1.0 | 0.22 |
| (b) daily 1-day straddles, 3 years | ~1,000 | ~1.2 | 0.11 |
| (b) increment, signal-on vs signal-off, 3 years weekly, thirds | 52 vs 104 | 1.0 | ~0.48 |

**What the MDEs mean for each idea:**

- **(a)** must beat VRP plus costs. Those are about −10% (ATM) to −30%+ (wings) of premium a
  trade, plus 5–25% in costs. So the conditional edge must be about +0.3–0.5 premium even to
  break even.
- With an MDE ≥ 0.7, a test can detect only a spectacular effect.
- **(b)'s increment over the plain VRP (~0.10 of premium) is below detection on any weekly design.**
  Only daily designs over three or more years reach about 0.1.

## 6. Literature (verified by a web-only helper; URL opened and quoted)

**Crypto VRP: positive and large.**

- Almeida, Grith, Miftachov, Wang, *Risk Premia in the Bitcoin Market*, arXiv:2410.15195 (v2
  2025), Deribit data 2017-07 → 2022-12.
  - The BTC variance risk premium is **0.14 in annualised variance** (BVIX² 0.72 vs RV² 0.58),
    against about 0.02 for the S&P 500.
  - It is higher in low-vol clusters (0.17) than in high-vol clusters (0.12).
  - It turned **negative from 2020-03-15 to 2020-04-08**.
- Amberdata on Deribit Insights (2023-01-24):
  - "30-day IV > 30-day RV nearly 70% of the time" since 2019-04;
  - VRP mean about +15 pts in contango;
  - "naively selling volatility … disastrous outcomes" when unmanaged.
- Winkel & Härdle, *Pricing Kernels and Risk Premia implied in Bitcoin Options*, Risks 11(5):85
  (2023): investors pay "a substantial risk premium to insure … against short-term price
  movements", and it is smaller for longer tenors.
- **Published short-vol backtest:** Deribit Insights (2024-09-19). Selling a 0.35Δ weekend
  strangle (Friday 16:00 → Sunday 08:00) from 2020 to 2024-08 returned **27.4% APR with 23.8%
  maximum drawdown** (212 trades, including fees and 5% slippage). The author says hourly data
  likely understates the drawdowns. See §9: this is an outcome overlapping holdout A for a (b)-type
  design.
- DVOL methodology (Deribit Insights, 2021-03-31): 30-day, VIX-style variance-swap method,
  EMA-smoothed. Launch was reported by The Block on 2021-03-31. The data starts 2021-03-24.

**Leverage and liquidations against vol and crash risk.**

- Schmeling, Schrimpf, Todorov, *Crypto carry*, BIS WP 1087 (2023): "high crypto carry predicts
  future price crashes", together with "a rise in the price of crash risk insurance". So the
  options market already prices *some* leverage information.
- Garcia Seuma, arXiv:2607.27070 (posted 2026-07-29; literature about 2022–2025, not market data):
  across 7 BTC liquidation cascades, "no variable is event-invariant". Only taker-flow variance
  compression survives, as a population-level precursor and not as a per-event alarm.

**Dropped (not verifiable or not relevant):**

- Alexander & Imeraj, *The Bitcoin VIX and its Variance Risk Premium* (JAI 2021). It exists per
  search snippets, but every host returned 403, so no number is used.
- Alexander & Imeraj, *Delta hedging bitcoin options with a smile* (QF 2023): snippet only.
- Zulfiqar & Gulzar (Financial Innovation 2021) exists and was verified, but it is a numerical-IV
  paper with no VRP content.
- Atanasova et al., on illiquidity premium in crypto options: 403.
- Cheng et al., on BitMEX liquidations: snippet only.

## 7. Design space and split (no parameter set from any outcome)

**Calendars and who has read them.**

| calendar | signal | options data | earlier looks sharing it | role for N2 |
|---|---|---|---|---|
| 2021-03-24 → 2023-12-31 | Binance proxy (2021-12 →), Bybit OI (2020-08 →); **no map** | Deribit (new family, 0 trials) | Binance 2021–23 family: 1 (009 discovery). This scout: unconditional proxy frequencies, DVOL, perp moves, 4 cost-sample days | **discovery for proxy-based (a)/(b)** |
| 2024-01-01 → 2025-07-27 (009 holdout A) | Binance/Bybit proxies (not on disk; part of L4's 4.3 GB holdout pull, or Bybit REST) | Deribit | 009 released it unread. **This scout read nothing in it.** Literature exposure: the Deribit weekend-strangle backtest (§9) | **decision holdout for proxy-based designs**: existence test with t_{G−1}, economic bar after |
| 2025-07-28 → 2026-06-30 (HL year) | **the only map**, stress rate | Deribit | **HL family: 16 trials.** Signal data is not new; options data is. This scout: coincident DVOL/skew, 3 cost days, perp moves | discovery for map-based designs only, counted as HL trial 17+ |
| 2026-07-01 → | map (HL holdout snapshots on disk, unread) | Deribit | sealed; D18 gives the first claim to L3 | a map-based decision needs a registration that amends D18 openly, **or a forward paper-test** from new snapshots |

**Design space:**

- **(a) Long convexity on a dense map.**
  - Buy a 1–7-day near-ATM straddle, or a 25Δ put/strangle (the fee cap makes 5–15Δ wings
    uneconomic), at the daily 00:00 UTC snapshot when the signal is in its top third.
  - The tercile is fixed on the trailing distribution, point-in-time.
  - Hold to expiry; settle on the 07:30–08:00 index TWAP.
  - Primary statistic: mean return on premium, signal-on minus signal-off, clustered by week.
  - Report the result without the largest positive week.
  - Placebo: the signal lagged by a random 7–30 days.
- **(b) Short vol unless dense or stressed.**
  - Sell the 7-day ATM straddle at Friday 08:00 (weekly expiry), with fixed delta-hedge or none.
  - Skip when the signal is in its top third.
  - Test the *difference* against the unconditional short. The VRP itself is known and is not
    the hypothesis.
  - Sizing is fixed in advance at ¼ Kelly from the literature μ (below).
- **Signals.**
  - Map designs: L+S density at 2%, or long-side only for puts.
  - Proxy designs: market-wide OI build-up (OI / 30-day mean), deleveraging-hour count over the
    trailing 7 days, and funding.
  - The proxy cleaning rule (artifact and coverage filters, §3) must be frozen in the draft.
- **Family accounting.**
  - Each map-based test adds to the HL family (16 → 17+).
  - Proxy-based tests add to the Binance 2021–23 family (1 → 2+) *and* start the Deribit-options
    count.

## 8. Risk and tail per idea

**(a): bounded loss.** The maximum loss is the premium.

- At a fixed 1–2% of equity in premium per week, the worst plausible losing streak is the ordinary
  case: the 1-sd put pays in only 25 of 307 weeks, so 20 or more losing weeks in a row are
  routine.
- That costs about −20 to −40% of equity over a half-year at 2% a week, before any edge.
- Kelly for (a) is undefined without an edge estimate. At a zero edge it is 0.

**(b): unbounded tail. Stylised short weekly ATM straddle, 1 unit of spot notional.**

Assumptions:

- The premium is held constant at 1.114 × mean|r_w| (√(0.72/0.58) from Almeida et al.): 7.0% of
  spot for BTC and 9.0% for ETH.
- Loss = |weekly move| − premium.
- Gaussian Kelly f* ≈ μ/σ², with μ ≈ 0.11 × premium − costs ≈ 0.5% of spot a week and σ ≈ 6.5%
  of spot. That gives f* ≈ 1.2× notional per unit of equity. This is optimistic: the tails argue
  lower.

| event (underlying moves only) | BTC loss, % notional | ETH loss, % notional | full Kelly (1.2×) | ½ Kelly | ¼ Kelly |
|---|---|---|---|---|---|
| worst week: BTC 2020-03-06 (−42%, intra-week −60%) | −35 (−53 at the low) | — | −42% (−64%) | −21% (−32%) | −11% (−16%) |
| worst week: ETH 2021-01-01 (+64%) | — | −55 | −66% | −33% | −16% |
| 2022-06 3AC week (BTC −30%, ETH −39%) | −23 | −30 | −27 / −36% | −14 / −18% | −7 / −9% |
| 2022-11 FTX week (−16% / −20%, lows −25 / −33%) | −9 | −11 | −11 / −13% | −6 / −7% | −3% |
| 2025-10-10 week (−12.7% / −13.2%; day lows −16.4 / −22.7%) | −6 | −4 | −7 / −5% | −4 / −3% | −2% |
| HL-year worst week, 2026-01-30 (−21.7% / −30.7%) | −15 | −22 | −18 / −26% | −9 / −13% | −4 / −7% |
| **worst 4 consecutive weeks** (2020-12-11 → rally) | **−61** | **−73** | −73 / −88% | −37 / −44% | **−18 / −22%** |

**What the tail table implies:**

- The tail of a weekly short straddle is the **rally** as much as the crash: 2020-12 → 2021-01.
- Mark-to-market inside the week adds the intra-week excursion and the vega jump. A 7-day ATM
  straddle has vega ≈ 0.11% of spot per vol point, so a +13-pt 1-day DVOL p99 costs about −1.4%
  of spot.
- **Expected return of (b) without any map edge:** about 0.5% of spot a week × 52 × the sizing.
  That is ≈ **+8%/yr at ¼ Kelly and +15%/yr at ½ Kelly**, gross of funding and margin. It is in
  line with the published 27% APR / 24% maximum drawdown weekend strangle.
- The map filter can add at most a fraction of this, and it is below detection (§5).

## 9. Reads list for the ledger

**All reads end before 2026-07-01. None fall inside 2024-01-01 → 2025-07-27.**

| read | bounds | what was seen |
|---|---|---|
| Deribit DVOL 1h, BTC+ETH | 2021-03-24 → 2023-12-31 23:00; 2025-07-28 → 2026-06-30 23:00 | levels, unconditional changes, coincident with map/stress. Schema probe: BTC daily DVOL values 2021-03-24 → 2021-04 printed |
| Deribit BTC/ETH-PERPETUAL 1h | 2019-01-01 → 2023-12-31 23:00; 2025-07-28 → 2026-06-30 23:00 | unconditional weekly/30-day moves, worst weeks, stylised straddle worst week/month (no mean printed) |
| **Deleted pull** | perp **1D** candles 2019 → 2026-06-30 | Deribit stamps 1D candles 08:00 UTC, so the candle stamped 2026-06-30 08:00 covers about 8 h of 2026-07-01, and the 2023-12-31 candle covers about 8 h of holdout A. **Only the stamps' hour-of-day was printed. No value was read.** The directory was deleted and replaced by 1h candles. Disclosed as a fetched-not-read holdout touch |
| Deribit option trades, full days | 2021-06-15, 2022-06-15, 2022-11-09, 2023-06-15, 2025-10-10, 2026-03-16, 2026-06-15 | cost table, counts. Schema probe: 3 BTC trades 2021-06-15 and 2 on 2019-07-01 printed |
| Deribit option trades, the hour before each map snapshot | 286 windows 2025-07-28 → 2026-06-30 | skew proxy and ATM IV at the snapshot (coincident) |
| Deribit BTC-PERPETUAL trades | 2022-06-13 12:00–12:10 | field list only (no liquidation flag) |
| Bybit OI 1h | BTC 2020-08-04 → 2023-12-31, ETH 2020-10-21 → 2023-12-31 | unconditional counts. A few OI ratios around 2020-08 drops printed while diagnosing artifacts |
| Binance panel (local) | 2021-11-01 → 2023-12-31 23:00 (cut) | unconditional proxy frequencies. BTC OI ratios around 4 artifact hours in 2023 printed |
| HL map_levels, liquidations (local) | snapshot_ms / t < 1782864000000 | frequencies, coincident association. `snapshot_accounts.parquet` was not opened |
| OKX market-data-history | listing for 2020-06 → 2023-05 and 2026-06-15 (window end 2026-06-15 23:59). **The response listed the file name `...2026-06-16.zip` (UTC+8 labelling)** | names and sizes only. Nothing fetched |
| Binance options S3 | listing | names and sizes; 0 post-cutoff files exist |
| Tardis | `api.tardis.dev/v1/exchanges/deribit` (current metadata) | dataset since-dates. The dataset downloads returned 403 |
| Literature (helper) | — | **The Deribit Insights weekend-strangle backtest (2020 → 2024-08: 27.4% APR, 23.8% maximum drawdown)**: a published outcome of a (b)-type unconditional short-vol design overlapping holdout A. Almeida et al. VRP 2017–2022: overlaps the discovery calendar |

**Possible indirect exposure (flagged).** §5 prints the unconditional DVOL median (59.4, 2021-03 →)
and the unconditional weekly underlying sd (9.0%, 2019 → and the HL year). A reader could form a
crude unconditional implied-versus-realised comparison from them. The windows differ: the weekly sd
includes 2019–2020, before DVOL existed. No such comparison was computed, and **no
map/stress-conditional version exists anywhere**.

**Forbidden list, each respected:**

- No pairing of IV, option price, map or stress with any later realised vol, price move, option
  payoff or P&L. There is no VRP on our windows, no straddle return and no hit rate. The stylised
  straddle prints only the worst week and month of underlying moves against a constant premium,
  with no mean.
- No count split by a future quantity.
- Nothing dated ≥ 2026-07-01 read. The perp-1D touch was deleted unread; the OKX file name was
  disclosed.
- No paid, keyed or requester-pays data. Total downloads were 114 MB.
- Untouched: `lib/`, `pages/`, `vercel.json`, registrations, `results.jsonl` and other scouts'
  files. No commits and no uploads.
- Fetchers raise on errors and never cache them. They page forward in fixed windows, and the Bybit
  window errors on a full page. Tail coverage was checked: the last rows are 2026-06-30 23:00 or
  2023-12-31 23:00.

## 10. Honest odds, returns, capacity

**(a) Long gamma on a dense map.**

- **Effect exists (options underprice the dense-map tail enough to show): ~10%. Deployable: ~4%.**
- For:
  - the options do not price the map's side;
  - a dense BTC map coincides with lower IV.
- Against:
  - fees of 8–12.5% of premium on wings;
  - an unconditional −VRP drag;
  - payoff sd of 3–5 premiums;
  - an MDE ≥ 0.7 premium;
  - a one-year map in a family already at 16 trials;
  - a dense map that is mostly a low-vol regime marker;
  - the spiral weeks that drive convex payoffs come in clusters (13 of the ≥ $77 M days fall in
    2025-07/09).
- Expected return: most likely −20 to −40% of the premium budget a year. The upside, if real, is
  +50–100% of the premium budget in a spiral year.
- Capacity: ample, at $100k+ of premium a week.

**(b) Short vol when the map is sparse and stress is low.**

- **A map/stress increment over the plain VRP exists: ~15%. A filtered short-vol book is
  deployable: ~20%.**
- The deployable part is mostly the known, priced VRP.
- Expected return: **+8 to +15%/yr at ¼–½ Kelly**, with a worst week of −11 to −33% and a worst
  month of −18 to −44% (BTC/ETH).
- It is fragile to rallies as well as crashes. The stress filter as defined (≥ $15 M) would idle
  44 of 49 weeks.
- Capacity: a few $M of notional a week in 7-day ATM, far above our size.
- **It does not reach the mandate's 30–100% landing zone without leverage that the tail table
  forbids.**

**Recommended next step, if N2 is pursued:**

1. Draft (b) as a **proxy-based, daily or weekly design** on 2021-03 → 2023-12 discovery, with
   holdout A as the decision holdout.
2. Use the design-sized Deribit pull: entry-hour windows plus settlement windows, ≈0.2 GB, free.
3. Keep map-based (a) only as a forward paper test from new snapshots. Spending HL-year trial 17
   on an MDE of 0.7+ is not worth it.

## Reproduction

```bash
cd research/copytrade/scouts; PY=../.venv/bin/python
$PY -I n2_deribit.py dvol    ../data/scout_n2/deribit_dvol_1h
$PY -I n2_deribit.py perp    ../data/scout_n2/deribit_perp_1h
$PY -I n2_deribit.py trades  ../data/scout_n2/deribit_trades_sample 2021-06-15 2022-06-15 2022-11-09 2023-06-15 2025-10-10 2026-03-16 2026-06-15
# SNAPLIST = distinct snapshot_ms from map_levels.parquet (< 1782864000000), one per line
$PY -I n2_deribit.py snapwin ../data/scout_n2/deribit_snapwin_1h SNAPLIST 1
$PY -I n2_bybit.py ../data/scout_n2/bybit_oi_1h
$PY -I n2_binance_options_list.py
$PY -I n2_analyze.py all > ../data/scout_n2/analysis_all.txt
```
