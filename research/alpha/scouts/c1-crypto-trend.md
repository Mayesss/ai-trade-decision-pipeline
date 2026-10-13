# C1 scout: crypto trend following and convex crypto exposures — a test design with real power

2026-10-13, round 4, behaviour-only. Literature, arithmetic, and behaviour facts from
free keyless Binance archive files dated before 2026-07-01. **No return, P&L or price
change after any signal, breakout or state was computed, on any calendar. No strategy
equity curve exists anywhere in this scout.** No post-2025-03 performance report of the
paper's strategy was opened (that would read calendar (a) by proxy).

---

## 1. Summary

1. **The paper's rule, verbatim (§2):**
   - nine Donchian lookbacks on closes, with a midpoint trailing stop;
   - min(25%/σ90, 200%) per lookback **per coin**, monthly top-20 by median volume;
   - 10 bp costs, no funding.
   - Its top-20 book ran at **9% vol** (Sharpe 1.57, α 10.8%/yr).
   - The sample is **2015-01-01 → 2025-03-19, ranks 1–100 from 2020**, so ranks 21–100 are not independent:
     they correlate 0.89–0.97 with the top 20 as baskets.
2. **Power:**
   - 80% power needs about 6.2/SR² years out of sample: 2.7 y at SR 1.5, 6.2 y at SR 1.0, **11 y at SR 0.75**.
   - On (a) 2025-04 → 2026-06, power is 0.28 at SR 1. Pooled with the forward span to 2027-12-31, it is 0.49.
   - **"Alive at half strength" cannot be decided with power before about 2036.**
   - "Dead vs as published" (SR 1.57) can be decided by about 2027-09 to 2028-06 (power 0.76–0.88 if the truth is 0).
3. **Behaviour (Binance perps, 2021-03 → 2026-06):**
   - 11.6 sub-signal flips per coin-month;
   - turnover 4.4×/yr, so **fees are only 0.2–0.26%/yr**;
   - gross mean 0.08, p99 0.27 at paper sizing;
   - unconditional universe funding +5.4%/yr;
   - HL lists 19–20 of the top 20 from 2023-12.
4. **Return and risk:**
   - At 25% vol (2.8× the paper's sizing), alive at half the Sharpe: ≈ +16–18%/yr over cash.
   - Worst plausible week −20 to −35%, worst month −30 to −40%.
   - Bayes-Kelly at 1-in-3 odds is ≈ 16% vol, so start at the paper's ≈ 9%.
5. **Odds and next step:**
   - The effect exists: 1 in 3. Beats HLP on return: 1 in 5; risk-adjusted: 1 in 15.
   - Register one trial on 2025-04 → 2027-12-31: α vs {BTC, EW alts, vol-managed EW}, never with trend-defined
     regime cells.
   - All three convex alternatives are rejected.

---

## 2. The paper's rule, verbatim, and what is in and out of its sample

Source: Zarattini, Pagani & Barbon, *Catching Crypto Trends: A Tactical Approach for
Bitcoin and Altcoins*, first version 2025-04-04, this version 2025-04-09; Swiss Finance
Institute RP 25-80. Read in full from the authors' open PDF
(<https://concretumgroup.com/wp-content/uploads/2026/02/Catching-Crypto-Trends.pdf>,
linked from <https://concretumgroup.com/catching-crypto-trends-a-tactical-approach-for-bitcoin-and-altcoins/>;
SHA-256 `9fc2811b…945d`, copy in `research/alpha/data/scout_c1/paper/`). RePEc:
<https://ideas.repec.org/p/chf/rpseri/rp2580.html>. SSRN 5209907 returned HTTP 403 to a
plain request and was not retried. CXO's summary (2025-05-29,
<https://www.cxoadvisory.com/technical-trading/crypto-asset-trend-following-strategies/>)
agrees on every parameter below.

### 2.1 Signal (§4.1, §4.3; long-only is the paper's primary)

- Data: **daily closes only** (channels are on closes, not highs/lows).
- `DonchianUp_n(t) = max(Close_t … Close_{t−n+1})`, `DonchianDown_n(t) = min(…)`,
  `DonchianMid_n(t) = 0.5·(Up + Down)`.
- Entry: `Pos_n(t) = 1 if Close_t = DonchianUp_n(t)` (today's close is the n-day highest close,
  today included).
- Exit: `Pos_n(t) = 0 if Close_t ≤ TrailingStop_n(t)`; otherwise `Pos_n(t) = Pos_n(t−1)`.
- Trailing stop: `TrailingStop_n(t+1) = max(TrailingStop_n(t), DonchianMid_n(t))`, never lowered;
  on a new entry the initial stop is the channel midpoint at entry (footnote 3).
- Lookbacks: **n ∈ {5, 10, 20, 30, 60, 90, 150, 250, 360} days.**
- Long/short (Appendix): `−1 if Close_t = DonchianDown_n(t)`; a short exits when
  `Close_t ≥ TrailingStop_n(t)`, with `TrailingStop_n(t+1) = min(TrailingStop_n(t), DonchianMid_n(t))`.

### 2.2 Sizing (§4.2, §4.3)

- `w_n(t) = min(0.25 / σ_t, 200%) × Pos_n(t)`, σ_t = annualised volatility of the asset's
  daily returns over **90 days** ("3-month").
- Ensemble ("Combo"): `w_Combo(t) = (1/9) Σ_n w_n(t)`; return `r(t) = w_Combo(t−1) × ret_asset(t)`
  (positions decided at close t, earning from close t; no execution lag).
- **The 25% is a per-asset sleeve target, not a portfolio target.** The top-20 portfolio
  realised **9% volatility** (Table 4). The leverage cap is 200% **per lookback sleeve per asset**.

### 2.3 Costs and rebalancing (§5.1)

- Costs tested 0/10/25/50 bp; **all headline numbers are net of 10 bp per trade** with a
  **20% rebalance threshold** on volatility-driven changes only (signal changes trade at
  once). "Difference between current and target allocation exceeds 20%" — the paper does not
  say absolute or relative; both are measured below.
- **No funding, no borrow, no slippage beyond the 10 bp.** The data are CoinMarketCap spot
  aggregates, not perps.

### 2.4 Universe and portfolio (§7.1, §7.2)

- Monthly, at month end: listed ≥ 365 days; not a wrapped token, stablecoin or NFT
  collectible; **median daily volume ≥ $2 M over the preceding 30 days**; rank by median
  daily volume in the month; keep the top B (B = 20 headline).
- Capital split equally, 1/B per asset; each sleeve runs the Combo independently; sleeves
  are reset to 1/B at each month end.
- Intra-month removal if the 30-day median volume < $1 M or the 30-day median daily price
  change < 0.5%.

### 2.5 Headline results (net of 10 bp, no funding)

| object | span | CAGR | vol | Sharpe | MDD | α vs BTC | β |
|---|---|---|---|---|---|---|---|
| BTC Combo (Table 1, gross) | 2015-01-01 → 2025-03-19 | 30% | 17% | 1.58 | 19% | 14% | 0.17 |
| top-20 long-only (Table 4) | 2015-01-01 → 2025-03-19 | 18% | 9% | 1.57 | 11% | 10.8% | 0.08 |
| top-20 long/short (Table 7) | same | 16% | 10% | 1.24 | 13% | 15.5% | 0.00 |
| deciles D1…D10 of ranks 1–100 (Fig. 6) | **2020-01-01 → 2025-03-19** | — | — | "no meaningful size effect" | — | — | — |

### 2.6 Sample: dates and coins

- **Dates:** data 2010-01 → 2025-03 (21,616 coins, CoinMarketCap, survivorship-free);
  backtests **2015-01-01 → 2025-03-19**. The decile study (ranks 1–100) covers
  **2020-01-01 → 2025-03-19**.
- **Coins:** the top-20 is point-in-time by CoinMarketCap median volume; **the paper does
  not list its monthly members.** Table 3 lists the top 40 by cumulative volume (BTC, ETH,
  XRP, BCH, LTC, EOS, SOL, BNB, WETH, ADA, TRX, ETC, DOGE, LINK, DOT, BSV, AVAX, SHIB, MATIC,
  XLM, NEO, ATOM, DASH, QTUM, ZEC, FIL, LUNC, UNI, PEPE, NEAR, AAVE, VET, XMR, FTM, ALGO, OMG,
  CRV, YFI, OKB, RUNE). Fig. 5 shows fewer than 20 eligible coins before mid-2017.
- **What is out of sample:** only time after **2025-03-19**. Every crypto coin with
  meaningful volume before that date is inside the paper's universe (ranks 1–100 for
  2020 → 2025-03 via Fig. 6; ranks 1–50 for 2015 → via Table 4). BTC 2010–2014 is in the
  data but outside the backtest, and is untradeable history of no use for a decay question.

---

## 3. Calendars and universes: independence and power

### 3.1 Power arithmetic (`c1_power.py`, output `data/scout_c1/derived/power.txt`)

The model uses monthly net returns (or monthly α residuals) at annual Sharpe/IR S over G month clusters.
- Noncentrality: d = S·√(G/12).
- Test: one-sided 5% existence test against t_{G−1−k}.
- k is the number of extra betas: k = 0 for raw returns; k = 2 for a three-factor α with
  constant betas. Per-cell betas add k.
- Power is simulated (40k draws).

| span | G | crit (k=0 / k=2) | power S=0.5 | S=0.75 | S=1.0 | S=1.5 | E[t] at S=1 |
|---|---|---|---|---|---|---|---|
| (a) 2025-04 → 2026-06 (out-of-paper, pre-cut) | 15 | 1.761 / 1.782 | 0.13 | 0.20 | 0.28 | 0.48 | 1.12 |
| (c) 2026-07 → 2027-06 (forward, unread) | 12 | 1.796 / 1.833 | 0.12 | 0.17 | 0.24 | 0.41 | 1.00 |
| (c) 2026-07 → 2027-12 | 18 | 1.740 / 1.753 | 0.15 | 0.22 | 0.32 | 0.55 | 1.22 |
| (a)+(c) → 2027-06-30 | 27 | 1.706 / 1.711 | 0.18 | 0.29 | 0.43 | 0.71 | 1.50 |
| **(a)+(c) → 2027-12-31** | **33** | 1.694 / 1.697 | 0.21 | 0.34 | **0.49** | **0.79** | 1.66 |
| (a)+(c) → 2028-06-30 | 39 | 1.686 / 1.688 | 0.23 | 0.38 | 0.55 | 0.85 | 1.80 |
| (a)+(c) → 2029-06-30 | 51 | 1.676 / 1.677 | 0.27 | 0.46 | 0.66 | 0.92 | 2.06 |
| (b) in-paper 2020-01 → 2025-03 (replication only) | 63 | 1.670 | 0.31 | 0.53 | 0.74 | 0.96 | 2.29 |

**Years of out-of-sample data for 80% power** (normal approximation): S = 0.5 needs 24.7 y, 0.75 needs 11.0 y, 1.0 needs 6.2 y,
1.25 needs 4.0 y, and 1.5 needs 2.7 y. Counted from 2025-04 that means **~2028-01 if S = 1.5, ~2031-06 if S = 1.0, and
~2036 at the desk's "half the paper" S ≈ 0.75.** Counted from the unread forward start 2026-07 alone, add 1.25 y.

**The decay test is the one with power.** Its null is "the paper's SR 1.57 still holds", rejected one-sided at 5%:

| truth | G = 15 | G = 27 | G = 39 |
|---|---|---|---|
| S = 0 (dead) | 0.54 | 0.76 | 0.88 |
| S = 0.5 | 0.33 | 0.48 | 0.61 |
| S = 0.78 (half) | 0.22 | 0.32 | 0.41 |

So by mid-2027 to mid-2028 the programme can tell "dead" from "as published". It cannot tell "half-alive" from
either.

### 3.2 The options

**(a) 2025-03-20 → 2026-06-30, top-20 (15 full months, 2025-04 → 2026-06).**
- This is the only out-of-paper span before the cut, and the desk's t ≈ 1.1 is confirmed (E[t] = 1.12 at S = 1).
- Status: no trend statistic has been computed on it. It overlaps:
  - the HL year (2025-07-28 → 2026-06-30, 16 trials, all other signals);
  - the tail of L4 holdout A (2025-03-20 → 2025-07-27, released and unread; N2 named holdout A as its
    candidate holdout). Using (a) consumes about 4 months of holdout A.
- **Designer-knowledge caveat.** The model drafting this design was trained on data into 2026. It
  knows the broad BTC path of 2025 to early 2026: a rally into October 2025, then a large decline.
  - Any choice the designer makes on (a) is contaminated: long-only vs long/short, the universe, the costs.
  - Copying the paper verbatim, including its own long-only primary, removes most of those
    degrees of freedom. It cannot remove the decision to register.
  - (a) should therefore be a stratum reported separately, never the sole decider.

**(b) Ranks 21–100 over 2020 → 2026: not independent.**
- *In sample:* the paper's Fig. 6 runs the same rule on volume deciles D1…D10 of ranks 1–100 over
  2020-01 → 2025-03-19, and Table 4 covers top-50 from 2015.
- *Correlated:*
  - The average pairwise daily-return correlation between top-20 members and ranks 21–100 is
    0.52–0.70 by year, the same as within the top 20 (0.54–0.73).
  - The EW top-20 and EW 21–100 baskets correlate **0.89–0.97** (table in §4.5).
  - At pairwise ρ ≈ 0.6, the effective number of independent underlyings caps near 1/ρ ≈ 2. The paper's
    flat Sharpe from 20 to 50 assets (1.57 → 1.54) shows its own breadth saturating.
- Post-2025-03, ranks 21–100 share (a)'s month clusters. They add breadth, not time, and power is
  set by time.
- Use: an implementation check on perps with funding (replication power 0.74 at S = 1.0), not
  evidence about decay.

**(c) Forward span 2026-07-01 → (unread, accumulating), with a fixed end.**
- This is the only span free of both the paper's sample and the designer's knowledge.
- Alone it is weak: power 0.32 at S = 1.0 by 2027-12-31.
- Its value is as the second stratum of a pooled test.

**(d) Pooled across many coins, coin-clustered or time-clustered.**
- Coin clustering is anti-conservative here. The dominant shock is the common crypto-market month,
  with pairwise ρ 0.5–0.7, so coin clusters are not independent.
- Time (month) clusters are the honest unit, and pooling coins inside a month adds almost
  nothing (Neff ≈ 1/ρ).
- Two-way clustering gives the same G as month clustering. Pooled designs therefore inherit
  §3.1's table.

**(e) Cross-asset on HL `xyz` (2025-10-13 →; equity indices, single names, commodities, FX).**
- This adds independent observations of a **different hypothesis**: classic TSMOM, documented
  over a century (AQR summary of Hurst–Ooi–Pedersen, <https://www.aqr.com/Insights/Research/Journal-Article/A-Century-of-Evidence-on-Trend-Following-Investing>).
  It says nothing about whether *crypto* trend has decayed.
- The paper itself measures a 7.4% average rolling correlation between crypto trend and the SG
  Trend Index.
- The underlyings have decades of off-venue history, so the venue adds only costs (0.9 bp taker,
  N1) and about 8.5 pre-cut months.
- It could be a separate light ("managed futures on xyz at 1 bp"); it does not rescue C1's power.

### 3.3 Recommendation

1. **Do not read (a) alone.** At power 0.28 (S = 1), a pass or a fail carries little information, and a
   read spends the only pre-cut out-of-paper span.
2. **Register one decisive trial now,** before any read: the pooled span **2025-04-01 → 2027-12-31**
   (G = 33), fixed end, two pre-declared hypotheses (§6):
   - H1, existence: power 0.49 at S = 1.0, 0.79 at S = 1.5;
   - H2, decay against the published 1.57: power ≈ 0.80 if dead.
   - Optional second look at 2029-06-30 (G = 51) with alpha spending.
3. **Plainly: "alive at half the paper's strength" cannot be decided with 80% power before about
   2036, and S = 1.0 not before about 2031.** "Dead vs as published" can be decided by about 2027-09
   to 2028-06.
   - The deployment decision therefore has to be a **decision-theoretic sizing problem**, not a
     significance gate: a small live allocation at the Bayes-Kelly size of §5, with H2 as the kill switch.

---

## 4. Behaviour facts (free keyless data before 2026-07-01; past prices only)

Source: Binance USDⓈ-M monthly 1d klines for every USDT-quoted symbol prefix (895 prefixes, 787
with pre-cut rows; 19,730 files, 34 MB, all `.CHECKSUM`-verified, 2,491 post-cut keys counted only).
Monthly funding for every top-20 member-month (1,293 files; one 404: LUNAUSDT 2022-06, after its
delisting). Script `c1_behaviour.py`; full output in `data/scout_c1/derived/run2/behaviour_report.txt`.

### 4.1 Point-in-time universe (the paper's §7.1 rule on Binance perps)

The rule:
- perp age ≥ 365 d from its first kline;
- median daily quote volume ≥ $2 M in the prior month;
- rank by that median;
- stablecoins, gold and TradFi perps excluded.

| month | eligible (age rule) | eligible (no age rule) | top-20 on HL at month start | rank-20 median $M/day | rank-100 median $M/day |
|---|---|---|---|---|---|
| 2021-03 | 24 | 93 | 0/20 | 70 | — |
| 2021-12 | 77 | 134 | 0/20 | 249 | — |
| 2022-06 | 110 | 145 | 0/20 | 215 | 18 |
| 2023-06 | 134 | 184 | 11/20 | 96 | 10 |
| 2023-12 | 144 | 234 | 19/20 | 166 | 15 |
| 2024-06 | 173 | 262 | 20/20 | 179 | 15 |
| 2025-04 | 225 | 378 | 19/20 | 117 | 11 |
| 2025-10 | 235 | 444 | 20/20 | 150 | 12 |
| 2026-03 | 204 | 348 | 19/20 | 86 | 7 |
| 2026-06 | 242 | 383 | 19/20 | 95 | 8 |

(Every month is in `derived/run2/behaviour_report.txt`; the members are in `derived/run1_nofunding/universe_top100.csv`.)

- **The perp-age rule bites until 2021-03.** Only 3 coins were eligible in 2021-01 and 11 in 2021-02, so a
  perp-only top-20 exists from **2021-03**. The paper's rule ages the *asset*, not the perp.
- Distinct symbols ever in the top 20: **116**; ever in the top 100: 313.
- **Hyperliquid:** HL first-funding dates come from L2's bounded probe. They show 0 listings before 2023-06
  (HL perps only start in the probe window), then **19–20 of the top 20 from 2023-12 on**.
  HL delistings are not captured.
- **Bitget:** existence probes (open time only) at each coin's first top-20 month. Of 116, 73 were
  confirmed listed and 22 are unknown (symbol since removed, code 40034).
  - 21 returned no candle. All but one are 2021–22 first-months, which is likely Bitget history
    retention rather than non-listing, so treat them as unknown.
  - **From 2024 on, 33 of 36 were confirmed and 3 are unknown (removed).**
- **The point-in-time loader needs the L4 delisting cut.** LUNAUSDT stays in the 2022-06 top 20 on May
  volume although it was delisted (no June funding file).
  - The paper's intra-month exit rule (30-d median volume < $1 M or median |daily change| < 0.5%)
    fires on only 92 of 39,239 top-20 member-days. It would not reliably catch this.

### 4.2 Signal turnover (Combo, top-20 members, 2021-01 → 2026-06, about 1,284 coin-months)

| lookback | entries + exits per coin-month | share of member-days long |
|---|---|---|
| 5 | 5.43 | 0.39 |
| 10 | 2.68 | 0.34 |
| 20 | 1.27 | 0.29 |
| 30 | 0.87 | 0.27 |
| 60 | 0.50 | 0.28 |
| 90 | 0.35 | 0.26 |
| 150 | 0.23 | 0.25 |
| 250 | 0.16 | 0.22 |
| 360 | 0.13 | 0.22 |
| **all 9** | **11.6 sub-signal flips per coin-month** | — |

The long fractions are signal-state counts. None is linked to any later price, and the lookbacks are not compared on
any outcome.

### 4.3 Turnover and fee drag (target weights; signal changes trade at once, vol changes behind the 20% threshold)

| book | one-way turnover, × capital/yr (paper sizing) | fee drag at 4.5 bp | at 6 bp | at 2.8× scale, 6 bp |
|---|---|---|---|---|
| long-only, 20% absolute threshold | 4.39 | 0.20%/yr | 0.26%/yr | 0.74%/yr |
| long-only, 20% relative threshold | 4.40 | 0.20% | 0.26% | 0.74% |
| long-only, no threshold | 4.53 | 0.20% | 0.27% | 0.76% |
| long/short, 20% absolute | 8.71 | 0.39% | 0.52% | 1.46% |

The table leaves out two flows: drift-induced rebalancing, and the month-end reset of sleeves to 1/B. Both depend on
post-entry price changes, so they are omitted by design (forbidden conditioning). They add a little.
**Costs are not the binding constraint here,** unlike rounds 1–3. The paper's 10 bp/trade assumption is above our 4.5–6 bp.

### 4.4 Exposure and margin

| quantity (paper sizing, B = 20) | mean | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| gross, long-only | 0.08 | 0.06 | 0.16 | 0.27 | 0.31 |
| gross, long/short | 0.12 | — | 0.20 | — | 0.33 |
| net, long/short | −0.02 | — | p10 −0.17 / p90 0.13 | — | — |
| Combo weight per member-day (sleeve units) | 0.078 | — | 0.20 | — | 0.85 |

**Unconditional 90-day vol of top-20 members** (median by year): 2021 1.41, 2022 1.09, 2023 0.76,
2024 0.92, 2025 1.01, 2026-H1 0.83. A fully-on sleeve therefore holds only 0.18–0.33 of its capital.

**Margin at a 25% portfolio-vol target.** The paper's top-20 book realised 9% vol, so 25% needs a scale of
**k ≈ 2.8**: mean gross 0.22, p99 0.76, max 0.87.
- Under 1× gross, cross margin cannot be liquidated by any move seen in 2020–2026.
- At HL/Bitget initial margins (≤ 10–20%), it uses < 10–20% of equity.
- **Isolated margin at ≥ 3× per coin would be liquidated by a 2021-05-19-type day (−34% EW).** Use cross
  margin, or ≤ 2× isolated.

### 4.5 Unconditional funding level of the universe (EW over top-20 members, all member-days; not by signal state)

| year | EW members, %/yr | BTC, %/yr | share of member-days with positive funding |
|---|---|---|---|
| 2021 | +30.8 | +30.6 | 0.91 |
| 2022 | −7.9 | +4.2 | 0.54 |
| 2023 | +0.9 | +7.9 | 0.82 |
| 2024 | +12.0 | +11.9 | 0.87 |
| 2025 | +1.7 | +5.1 | 0.71 |
| 2026-H1 | −10.1 | +1.1 | 0.58 |
| **2021-01 → 2026-06** | **+5.4** | **+11.0** | 0.75 |
| 2025-01 → 2026-06 | −2.2 | — | — |

- The mean is skewed by a few coins with extreme negative funding.
- **Unconditional drag on a long-only book:** gross × level ≈ 0.08 × 5.4% ≈ 0.4%/yr at paper sizing, or
  1.2%/yr at k = 2.8.
- The paper models **no funding at all**. The drag conditional on being long in uptrends is plausibly
  larger. It is **not measured here (forbidden)** and belongs inside the registered run.

**Unconditional return correlations** (daily, membership by month):

| year | within top-20 (pairwise) | top-20 × ranks 21–100 | within 21–100 | corr(EW top-20, EW 21–100) | corr(EW 21–100, BTC) |
|---|---|---|---|---|---|
| 2021 | 0.63 | 0.61 | 0.62 | 0.95 | 0.80 |
| 2022 | 0.73 | 0.69 | 0.67 | 0.96 | 0.85 |
| 2023 | 0.55 | 0.52 | 0.51 | 0.94 | 0.79 |
| 2024 | 0.54 | 0.52 | 0.54 | 0.95 | 0.74 |
| 2025 | 0.71 | 0.70 | 0.72 | 0.97 | 0.78 |
| 2026-H1 | 0.63 | 0.58 | 0.56 | 0.89 | 0.81 |

---

## 5. Expected return, tail, Kelly, capacity, odds

### 5.1 Underlying moves in the pre-named stress episodes (prices only; Binance perps)

| episode | asset | day-of close→close | worst 7 d in ±10 d | worst 30 d in ±30 d | day-of low vs prior close |
|---|---|---|---|---|---|
| 2020-03-12 | BTC / ETH / EW top-20 | −40.0% / −45.2% / −42.7% | −47.4% / −53.3% / −53.8% | −53.4% / −58.1% / −61.4% | −45.2% / −51.2% |
| 2021-05-19 | BTC / ETH / EW | −14.4% / −27.6% / −34.0% | −26.0% / −41.5% / −48.1% | −43.1% / −40.8% / −45.2% | −33.1% / −58.5% |
| 2022-05 LUNA (05-09) | BTC / ETH / EW | −11.7% / −11.6% / −20.3% (worst day −28.4%) | −26.7% / −29.1% / −52.6% | −29.7% / −40.2% / −60.2% | −11.8% / −11.8% |
| 2022-11 FTX (11-08) | BTC / ETH / EW | −9.9% / −14.9% / −15.7% (worst day −21.0%) | −21.9% / −27.4% / −28.1% | −22.1% / −27.9% / −28.3% | −18.2% / −23.8% |
| 2025-10-10 | BTC / ETH / EW | −7.3% / −12.5% / −23.0% | −11.1% / −16.5% / −32.5% | −17.8% / −27.2% / −42.0% | −16.5% / −22.1% |

The EW top-20 basket uses the no-age-rule top 20, so it covers 2020. It includes the crashing coin itself where that
coin was a member (LUNA).

Unconditional worst windows, 2020-01 → 2026-06:

| | worst 1 d | worst 2 d | worst 7 d | worst 30 d |
|---|---|---|---|---|
| BTC | −40.0% | −39.7% | −47.4% | −53.4% |
| EW top-20 | −42.7% | −44.2% | −53.8% | −61.4% |

### 5.2 Tail of the book (unconditional exposure × named move; no signal state at the episode is used)

- A daily-close trailing stop cannot act inside a one-day gap. So the worst week is mostly the
  first one or two days times the gross held going in.
- The gross on the eve of each episode is **not** looked up (forbidden). The unconditional p90 and p99
  of gross stand in for it.

| sizing | gross p90 / p99 | 2020-03-12 day (−43%) | 2021-05-19 day (−34%) | 2025-10-10 day (−23%) |
|---|---|---|---|---|
| paper (≈ 9% vol) | 0.16 / 0.27 | −7% / −12% | −5% / −9% | −4% / −6% |
| 25% vol (k = 2.8) | 0.45 / 0.76 | −19% / −33% | −15% / −26% | −10% / −17% |

**Worst plausible week at 25% vol: −20 to −35%.** **Worst plausible month: −30 to −40%.** The month figure is
the crash day plus whipsaw re-entries. For scale, the paper's 11% in-sample MDD at 9% vol becomes about 31% at 25%,
before decay. At paper sizing: worst week about −12%, worst month about −15%.

### 5.3 Expected return (arithmetic on the paper's numbers, not a backtest)

| state | SR | at paper sizing (≈ 9% vol) | at 25% vol (k = 2.8) |
|---|---|---|---|
| alive at half the paper (SR 0.78, net of ≥ 10 bp) | 0.78 | +7.1% − 0.4% funding ≈ **+6.7%/yr** over cash | +19.6% − 1.2 to 3% funding ≈ **+16 to 18%/yr** |
| dead (costs + funding only) | ≈ −0.1 to −0.2 | ≈ −1 to −2%/yr | ≈ −4 to −6%/yr |
| expectation at odds 1 in 3 | E[SR] ≈ 0.16 | ≈ **+1.2%/yr** over cash | ≈ **+2.3%/yr** over cash, with a −30% tail |

Idle collateral earns the lending rate (about 3%) on top in every state.

### 5.4 Kelly

- **If alive (SR 0.78):** full Kelly runs at a vol of SR ≈ 78% (Gaussian). 25% vol is **0.32 Kelly**. The
  jump tail (a −43% day) argues for no more than that.
- **At the programme's odds** (1 in 3 alive), E[SR] ≈ 0.16, so the **Bayes-Kelly vol is ≈ 16%** and half
  Bayes-Kelly is ≈ 8% vol. **That is the paper's own sizing (≈ 9% vol),** which is the recommended start.
- Scale to 25% only after H1 passes, or once H2 has had the power to reject decay and has not.

### 5.5 Capacity

- The rank-20 coin traded ≥ $71 M/day (median, 2026-05, the low).
- At $1 M of capital, k = 2.8 and the max sleeve weight 0.85, the largest single-coin notional is about $120 k:
  **≈ 0.17% of a day's volume.** Capacity is non-binding to tens of $M.

### 5.6 Odds

- **The effect exists** (post-paper α > 0 net, SR ≥ 0.5): **1 in 3**. This keeps the desk's figure:
  - Against: McLean & Pontiff find 58% lower returns post-publication across 97 predictors
    (<https://counterpointfunds.com/wp-content/uploads/2017/07/PredictabilityMcleanPontiff.pdf>,
    abstract read). 1.57 × 0.42 ≈ 0.66.
  - Against: the desk's Arefev note (TSMOM ≈ 0 after 2020-07).
  - For: the paper's own 2020–2025 deciles.
- **Deployable and beating HLP:**
  - On return at 25% vol, if alive: 2 in 3. **Overall ≈ 1 in 5.**
  - Risk-adjusted, against HLP's Sharpe 1.4 and −2.5% drawdown: **≈ 1 in 15.** Trend's Sharpe 0.78 and 30–45%
    drawdown lose on that metric even when alive.

---

## 6. Pre-registrable rule sketch and criteria

**Family.** A new family, "crypto TSMOM (C1)", with **1 decisive trial** (long-only, verbatim). Long/short is
reported but not decisive.
- Calendars shared:
  - the HL year (16 trials, all other signals; no trend statistic ever computed);
  - the tail of L4 holdout A, 2025-03-20 → 2025-07-27 (unread; N2's candidate holdout);
  - 2026-07-01 →, which this registration would **claim openly** for this family.
- This scout's behaviour exposure should be ledgered (proposed `000p`; §8).

**Rule (copied from §2; deviations named):**
1. **Universe.**
   - Binance USDⓈ-M USDT perps, rebuilt each month: perp age ≥ 365 d; median daily quote volume ≥ $2 M in
     the prior month; top 20 by that median.
   - Exclude stablecoins, gold and TradFi perps.
   - Point-in-time delisting cut by L4's rule (last metrics file ≥ 4 KB), plus the paper's intra-month exit rule.
   - *Deviation:* the age is the perp's, not the asset's (from 2021-03).
2. **Signal.** The nine lookbacks, the entry at the n-day closing high, the midpoint trailing stop, closes at
   00:00 UTC; all verbatim.
   - *Deviation:* fills at the next bar's open, seconds after the close.
3. **Sizing.** min(0.25/σ90, 2) per lookback, Combo = mean of nine, 1/20 of capital per sleeve with a
   month-end reset, and a 20% **absolute** threshold on vol-driven changes. The paper's wording is ambiguous;
   absolute is the declared reading, and turnover is the same either way (§4.3).
   - Test at k = 1. The t-statistic does not depend on k.
4. **Costs.** 6 bp per fill on |Δnotional| plus **real funding at every settlement held** (Binance archive),
   plus 2 bp slippage per fill.
5. **Span.**
   - S1 = 2025-04-01 → 2026-06-30 (15 months; designer-knowledge caveat recorded).
   - S2 = 2026-07-01 → 2027-12-31 (18 months, sealed until then).
   - Decision on the pooled G = 33 month clusters at 2027-12-31. An optional second look at 2029-06-30 (G = 51)
     uses Lan–DeMets O'Brien–Fleming spending, with boundaries fixed at registration.
   - S1 and S2 are reported separately as strata.

**Statistic and market adjustment.**
- Regress monthly net returns on three factors, with constant betas (primary):
  - BTC;
  - the EW top-20 basket (the alt factor);
  - a **vol-managed EW basket**: the same universe and sizing with Pos ≡ 1, which isolates trend timing
    from the Moreira–Muir vol-timing component.
- The decision is on α with t_{G−1−3}.
- Robustness: betas per **trailing BTC 90-d realised-vol tercile** (k = 9).
- **No regime cell may be defined by the trailing trend of BTC or of the book.**
  - The strategy's return is β_t × market with β_t set by the trailing trend. Per-cell betas on trend cells
    would credit the whole timing gain, E[r_m | up-trend] − E[r_m | down-trend], to β, and leave α ≈ 0 by
    construction.
  - Round 3's "β per regime cell" lesson must be applied with **non-trend** cells here.
- A short-horizon reversal factor is **reported, not decisive.** The book is mechanically anti-reversal
  through its 5- and 10-day legs, so that factor would absorb part of the tested effect.

**Hypotheses (both one-sided 5%, each with its own role):**
- **H1, existence:** α > 0, with t ≥ crit(t_{G−4}) **and** above the 95th percentile of the null
  calibration below.
- **H2, decay:** reject "SR = 1.57" if (ŜR − 1.57)·√(G/12) < −1.645. H2 is the kill switch for any
  live allocation.
- **Decisions:**

  | H1 | H2 | decision |
  |---|---|---|
  | pass | — | scale toward 25% vol, after the economic bar |
  | fail | rejects | close |
  | fail | does not reject | extend to the second look; stay at the paper's sizing |

- **Economic bar, applied after existence and not in series:** net α at 25% vol ≥ HLP-calm + 4%/yr.

**Null calibration and placebo.**
- Stationary block bootstrap of **dates**, jointly across coins (mean block 5 d), over the test span, with the
  rule re-run on each draw. This keeps vol clustering and cross-correlation and destroys multi-week
  autocorrelation, and it gives the null distribution of the α t.
- Placebo: the rule with each coin's Pos path circularly shifted by a random offset. This keeps the flip
  counts and turnover and breaks the timing.

**Integrity (ported from round 3):**
- a guarded runner that refuses before 2027-12-31 for S2;
- a gate token on the S2 read;
- the result row committed by the runner;
- shadow-module scans and refusals instead of `assert`;
- Binance `.CHECKSUM` pinned at pull time.

---

## 7. Convex alternatives

| alternative | verdict | reason |
|---|---|---|
| **Vol-managed BTC / EW basket** (Moreira & Muir, *Volatility Managed Portfolios*, JF 2017; NBER w22208, <https://www.nber.org/papers/w22208>: managed portfolios that take less risk when vol is high earn large alphas on equity factors and FX carry) | **Rejected as a light; kept as the third factor in §6** | It is long crypto beta with vol timing. Nobody structurally pays it, and its expected return is the crypto premium itself, which is unpriced and not an edge. As a regressor it separates trend timing from vol timing. Grobys et al. (FMPM 2025, via the N7 desk; their page refused connections here, so not re-opened) report that vol management mitigates crypto *momentum* crashes |
| **Long-dated OTM calls / call or put spreads on Deribit as convex trend** | **Rejected** | Fees are small for long-dated options: 0.03% of underlying per contract (Deribit Insights, 2020-08, <https://insights.deribit.com/?p=2277>), capped at 12.5% of premium (N2). The killer is the **variance risk premium**. Fact 6, with N2's literature (BTC VRP 0.14 in annualised variance), puts implied about 10 vol pts above realised. A 6-month ATM call (vega ≈ 0.28% of spot per vol pt) bleeds about 2.8% of spot, **≈ 16% of its premium**, per 6 months. That is ≈ 11%/yr per unit of delta, more than the ≈ 5%/yr per-unit α that half the paper's edge implies. Spreads halve the vega and the convexity together. The only open door is a separate vol-timing light (buy only when IV < trailing RV), and that is not trend |
| **Cross-asset lead-lag, HL `xyz` equity perps ↔ crypto** | **Rejected** | Fact 7: in US hours xyz takes an *outside* price, so it cannot lead. Fact 2: cross-venue at 1 s is not capturable at our latency. BTC venue lead-lag lives at about 500 ms horizons (Albers, Cucuringu, Howison & Shestopaloff 2021, <https://arxiv.org/abs/2108.09750>). No published day-scale equity→crypto lead-lag was found. At weekends crypto is the liquid leader, which is N1's index-weekend light, already powerless |

---

## 8. Exposure list, files created, constraints honoured

### 8.1 Exposure (proposed ledger `000p`)

- **Read:**
  - Binance USDⓈ-M monthly 1d klines (OHLC, quote volume, count), 2019-12 → 2026-06-30, 787 symbols;
  - Binance funding for the 1,293 top-20 member-months, 2021-01 → 2026-06;
  - L2's bounded HL first-funding file (`research/copytrade/data/carry/inputs/hl_first.json`; values ≥ the cut were dropped at load; none used);
  - Bitget first-candle **open times** for 116 symbols at endTimes ≤ 2026-06-01.
- **Computed, all unconditional or signal-state-only:**
  - monthly universe counts and liquidity;
  - per-lookback flip counts and long-fractions, pooled 2021-01 → 2026-06;
  - target-weight turnover and the gross/net distribution (pooled);
  - σ90 levels by year;
  - EW funding level by year;
  - return correlations by year;
  - BTC/ETH/EW underlying moves in the five named episodes, and unconditional worst windows.
- **Calendars touched:** 2020-01 → 2026-06-30. This includes span (a) and the HL year (2025 and 2026-H1
  rows appear in the funding, correlation and universe tables, plus the 2025-10-10 episode). **No outcome
  of any signal was computed on any calendar.**
- **Post-cut:** 2,491 kline keys dated ≥ 2026-07 were counted in the listing and never fetched. Nothing else.
- **Literature:** no post-2025-03 performance of the paper's strategy was opened. On CXO's page only the 2025-05-29
  summary was used; it reports no out-of-sample results.
- **Designer knowledge:** the drafting model's training data cover market prices into 2026, which
  includes span (a). This is disclosed in §3.2.

### 8.2 Files created

- `research/alpha/scouts/c1-crypto-trend.md` (this file)
- `research/alpha/scouts/c1_fetch.py`: Binance archive fetcher (listing; month < 2026-07; `.CHECKSUM` verify; SHA256SUMS)
- `research/alpha/scouts/c1_behaviour.py`: every number in §4–§5.1
- `research/alpha/scouts/c1_bitget.py`: Bitget existence probe
- `research/alpha/scouts/c1_power.py`: §3.1 tables (reads no data)
- `research/alpha/data/scout_c1/paper/`: the paper PDF plus SHA256SUMS
- `research/alpha/data/scout_c1/bn_um_klines_1d_monthly/`: 19,730 zips, SHA256SUMS, FETCH_LOG.txt
- `research/alpha/data/scout_c1/bn_um_funding_top20_months/`: 1,293 zips, SHA256SUMS, FETCH_LOG.txt
- `research/alpha/data/scout_c1/bitget_probe/`: `bitget_by_first_top20.json`, SHA256SUMS
- `research/alpha/data/scout_c1/derived/`:
  - `power.txt`;
  - `run1_nofunding/` and `run2/` (behaviour reports, `universe_top100.csv`, `first_top20_month.csv`, `gross_daily.csv`);
  - `top20_member_months.csv`;
  - SHA256SUMS.

Total 86 MB. The first kline pull (8 threads) was stopped for slowness and its partial directory deleted
before the complete pull.

### 8.3 Constraints honoured

**Forbidden conditionings, all honoured:**
- no return, P&L or price change after any signal, breakout or state, on any calendar;
- no funding by signal state;
- no "already in position" split;
- no comparison of lookbacks, universes, thresholds or variants by outcome (the only comparisons are
  turnover and counts);
- no strategy equity curve;
- nothing from 2026-07-01 → except a key count.

**Calendar cut:** every fetched file and probe is dated before 2026-07-01, and rows ≥ 2026-07-01 are dropped at load.

**Access:**
- SSRN (paper page; McLean–Pontiff) and iris.unito.it returned 403 and were not retried by other means.
- The osuva.uwasa.fi server refused the connection and was not retried.
- No logins, keys or paid or requester-pays data were used.

**Data handling:**
- Each pull is in its own new directory, with SHA256SUMS.
- Binance `.CHECKSUM` verified on every file: 0 mismatches, one 404.
- Scripts live outside the data directories, and data paths are passed as arguments.
- Analysis ran with `python -I`.
- The paper PDF's text was extracted with macOS PDFKit via `osascript`, from the scratchpad. No Python
  PDF library was available.

**Untouched:**
- nothing created or modified under `research/copytrade/` (read-only reads only), `lib/`, `pages/`,
  `components/`, `vercel.json` or `package.json`;
- no `results.jsonl` written, no git commit, no Neon query, nothing published or uploaded.

