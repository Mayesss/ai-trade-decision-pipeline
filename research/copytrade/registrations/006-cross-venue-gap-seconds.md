# 006 — Cross-venue capture of the forced-flow gap at second granularity: a taker entering 1 s after a Hyperliquid forced print that sweeps away from Binance (H3)

**Status: WITHDRAWN 2026-10-11** — closed before registration (owner decision), on the
behaviour finding in §3: at forced prints the executable cross-venue gap is a median −8.5 to
+0.9 bp and reaches the cost threshold in 36 of 1,213 causal runs. **Not a trial; no outcome
was read; the family count stays at fourteen.** The runner can never run: this file will not
say REGISTERED. Text below is the draft as it stood (decision table PROPOSED, never approved). Mechanics are built and checked on synthetic data. The events, filters, gaps at forced prints (contemporaneous), quote-proxy staleness, signals and clock alignment were measured in §3, behaviour only. **No Hyperliquid or Binance price after any signal or forced-print time has been read for any real event.** Only the *times* of the next prints after t + L were read, to measure fill delays.

The runner (`l8_run.py`) refuses to compute an outcome unless all of the following hold:
- this file says REGISTERED;
- it is committed and unmodified since;
- every research module the runner imports is clean and matches §8;
- the data manifests match §8;
- `results.jsonl` holds no row of 006.

During the run, every Binance and HL file is checked against the data manifest as it is read.

**Revision 3 (2026-10-10, before registration; orchestrator decision).** No outcome existed for any version.

(A) **Staleness of the executable proxy was measured** (§3.3b). The executable signal's near-zero gap is **not a staleness artefact**:
- The HL quote proxy is a median **0 ms** old: the same block as the forced print.
- It sits a median **1.7–7.2 bp** beyond the forced print.
- The upper bound on its bias is **1.3–6.5 bp**, against a G_MIN of 37 bp.

(B) **The primary signal is now the SWEEP gap**: the forced book print against Binance's last trade at or before it, at the per-coin G_MIN, with revision 2's honest fills.
- The rationale: the sweep gap brackets the true executable gap from above, and the quote proxy brackets it from below. Side-aware honest fills on the sweep trigger measure what a taker actually gets.
- The executable-proxy signal is a reported variant ("exec signal", 10 positions). Its behaviour finding stays in §3.
- The run's reported **half-life of the sweep gap** (+0.25 → +60 s, and which venue closes it) is **the main input for deciding H1**. A resting provider is filled *at* the sweep price. If HL's book reverts within a second, only a maker can earn that reversion, and a taker 1 s late cannot.

D15 and D17 are rewritten. The holdout, power and fill delays are recomputed for the sweep set, and the data manifest and hashes are rebuilt.

**Revision 2 (2026-10-10).**
- The honest fill rule: the max/min over [t + L, the first print of our side]; forced rows are never fill candidates; exits start after both entry fills; missing fills are counted, not dropped.
- The executable-quote signal (now a variant).
- Per-coin slippage and G_MIN.
- Criterion 8 (without the best day).
- Observability (F15) and the L = 3 s condition.
- The full-import guard and the data manifest.

**Revision 1 (2026-10-10).**
- The causal trigger (004) replaces the not-point-in-time 002 cascades.
- Book prints only.
- L = 1 s primary.
- Per-coin blocks.
- The 10-bp holdout gate.

Plan: `docs/copy-trade-plan-2026-10-07.md`. Horizon H3 (H2 folded in): `docs/forced-flow-research-horizons-2026-10-10.md` §4, §8. Prior ledger entries `001`–`005`: **fourteen trials on one discovery year, no pass.**

**Where this stands after 005.**
- 005 tested whether post-cascade drift could be captured by entering after the flow ended and holding for two hours. It **failed on 2026-10-10** (−10 bp, t −0.80).
- Every slower design has now failed: minute-level hedges, resting ladders, entry at detection, entry after the flow.
- Whatever is left of the forced-flow edge lives inside the cascade's seconds, where the forced print and the other venue part company. That is the only place 006 looks, and it is why 006 matters.

**The question.**
- **Primary:** when a Hyperliquid forced print shows a sweep gap to Binance of at least the coin's G_MIN, does a taker who sends both legs 1 s later, filled honestly at the prices a taker gets, earn net of four fees and slippage? The exit is when the unwind quotes show the gap closed, or at 60 s.
- **Behind it (§3):** does an *executable* cross-venue gap beyond costs ever open? Rarely: in 36 of 1,213 runs.
- **And for H1 (§6, reported):** how fast does the sweep gap close, and on which venue?

**Disclosure: the fifteenth look.**
- This is **trial 15 on the discovery year's events** (2025-07-28 → 2026-06-30); fourteen prior trials read them.
- The Binance outcome data is new. The selection is not.
- The hypothesis came from 002 §11's gap. §3.3 shows that gap is mostly HL's spread being swept.

## 1. Trials registered here

| id | hypothesis | role |
|---|---|---|
| T1 | **Taker convergence on the sweep signal.** At 004's causal trigger, scan HL's forced *book* prints from the trigger print while the run is alive. At the first print whose **sweep gap** — s (P_bn − P_forced)/P_forced, with P_bn = Binance's last trade at or before it — reaches the coin's G_MIN, send market orders 1 s later: buy HL / sell Binance, mirrored for shorts. They fill by the honest D8 rule. Close the same way when the unwind executable gap is ≤ 0 (orders 1 s later), or 60 s after the later entry fill. The trade earns net of four fees and slippage: **all six coins pooled**, positive in majors and in alts, robust to the best hour and the best day | primary, the only trial |

One trial. **Hour-clustered t ≥ 2.33 and day-clustered t ≥ 1.96** (D11), plus six more criteria (§5). **Trial 15 on the discovery year's events.**

**Written expectation.** About 1 in 6 that T1 passes. Under 1 in 10 that it clears the H7 bar in majors. The reasons:
- The executable gap at the signal is about zero (§3.3).
- A taker must pay HL's refilled ask 1 s after the sweep.
- HL lags Binance by 0.6–1.0 s, and the gap is smallest in 2026.

## 2. Data and periods

- **Discovery: 2025-07-28 → 2026-06-30. The holdout (2026-07-01 →) is unread.**
  - No Binance or HL file dated 2026-07-01 or later is on disk.
  - `ct/bn.files` asserts this; `l8_run.hl_window` / `hl_quotes` assert every window ends before it.
  - The plan drops any signal whose longest possible trade plus the 300-s fixed exit would reach it. None does.
- **Events (primary, D1): 004's causal trigger.**
  - Code: `ct/trigger.triggers` with `l6_run`'s D1 constants, imported unchanged; `l8_run.causal_events`.
  - Rule: the coin's trailing-60-s liquidation notional reaches max($100 k, 0.5 × the coin's trailing 30-day median daily notional of strictly earlier days). 15-minute cooldown. Six coins.
  - Events whose threshold came from the cross-coin fallback median are dropped.
  - Fields: `t_start` = the first forced print in the trailing window; `scan_from` = the trigger print; `scan_to` = the end of the run that contains it (the first 60-s gap).
  - The 002 cascades are a reported variant, "not point-in-time".
- **Scan points:** the distinct times of the event's forced **book** prints (`Close Long/Short`, the event's side) in [scan_from, scan_to]. The backstop rows' times are a reported variant.
- **HL tape for quotes and fills (`l8_run.BOOK`):**
  - Only the taker's row of each match (`crossed`).
  - **Never a forced fill** (`is_liquidation`), a backstop (`Liquidated …`) or an `Auto-Deleveraging` row.
  - Ordered by (timestamp, file, row).
  - `side` = the aggressor's side: `buy` is a print at the ask, `sell` a print at the bid.
- **Binance:** USDⓈ-M aggTrades / bookDepth for the six symbols, from `h3_fetch.py` (23.45 GB of zips, checksums pinned).
  - `is_buyer_maker = false`: the buyer took (a print at the ask). `true`: the seller took (a print at the bid).
  - Ordered by (transact_time, agg_trade_id).
  - Coverage: 1,213 of 1,214 primary triggers.
- **Clock:** OFFSET_MS = 0 (D7, §3.1); +150 ms is reported.
- **Stress:** the 005 D5 switch (all coins minus the event's own, $77 M per 5 min) over [min(t_start, t − 1 s), t − 1 s].
- **Fees:** HL taker 0.045 %, Binance USDⓈ-M taker 0.05 % (base tier, no BNB discount). Four fills: 19 bp.

**Causality.**
- The plan reads `coin`, `side`, `t_start`, `fallback_median` and the scan up to the signal. Each scan point carries quotes at or before its own time only.
- Tests poison every other event field, and every point after the signal: nothing changes.
- The causal trigger is tested on synthetic fills: threshold, window start, scan end at the first 60-s gap, cooldown, fallback flag.

## 3. What was measured before writing this (behaviour only, no return)

From `l8_describe.py` (log `data/scratch/l8.log`, written by one run of every section; numbers in
`data/scratch/l8_{clock,events,stale,power}.json`) and `l8_run.py --events`. §3.1–3.2 do not depend on the
event set.

### 3.1 Clock alignment (non-event hours) — read this first

Calm hours: whole UTC hours on downloaded days, ≥ 30 min from any HL
liquidation fill ≥ $100 k in the coin and from any 002 cascade of the coin;
4 seeded random hours per coin per month (44–48 per coin). Statistic:
correlation of HL 100-ms log returns with Binance 100-ms log returns shifted
by a lag (lag > 0: Binance, as timestamped, leads HL), lags −3 s … +3 s by
50 ms, then 10 ms around the peak; bootstrap over hours.

| coin | pooled peak lag | bootstrap 95 % | per-hour peak IQR | Jul–Oct 2025 (monthly peaks) | Nov 2025 → Jun 2026 |
|---|---|---|---|---|---|
| BTC | **+620 ms** (corr 0.16) | 620 – 690 | 630 – 950 | 950 – 1,000 | 580 – 720 |
| ETH | **+720 ms** (0.12) | 670 – 1,050 | 660 – 950 | 1,000 – 1,150 | 550 – 1,050 |
| SOL | **+720 ms** (0.06) | 690 – 950 | 660 – 950 | 900 – 1,000 | 250 – 770 |
| XRP | **+700 ms** (0.05) | 690 – 810 | 630 – 950 | 950 – 1,000 | −100 (2 months) – 720 |
| DOGE | **+640 ms** (0.04) | 600 – 1,050 | 640 – 1,000 | 1,000 – 1,050 | 570 – 760 |
| HYPE | **−150 ms** (0.10) | −160 – −130 | −140 – +650 | −150 – −130 | −200 – −120 (3 months +550 – +650) |

- **HL as archived lags Binance by 0.6–1.0 s in five of six coins; in HYPE,
  HL leads Binance by ~0.14 s.** The correlation is near zero at lag 0 in
  every coin (|corr| ≤ 0.014).
- **The lag moved within the sample.** A day-by-day scan (`l8_describe.py
  clockdays`: BTC and ETH, 3 calm hours a day, 2025-10-15 → 11-20) shows a step on **2025-11-01**
  (BTC 950–1,050 → 750–800 ms) and a second around 11-09 (→ 600–700 ms).
- **Is it a clock offset or a reaction delay? Not identifiable from prices
  alone, but bracketed.** If the step were a change in HL's timestamp
  convention, HYPE's HL-leads lag would have moved by the same −300 ms on
  2025-11-01; it stayed at −120 … −160 ms through Jul–Dec. With HL archive
  stamps late by c and HL-led moves seen at c − r (r = Binance's reaction
  to HL, including HL's block-to-feed delay), −140 ms = c − r gives
  **c ≈ r − 140 ms**, so **|c| ≲ 0.15 s** for any plausible r of 0–300 ms,
  and the 0.6–1.0 s in the other coins is HL's quote *reaction* to Binance,
  which shortened in November 2025. The secondary bump at −100/−200 ms in
  BTC, SOL and XRP (HL-led moves) fits the same reading.
- **What this means for the design — stated loudly.**
  1. The *timestamp* uncertainty (~±150 ms) is **15 % of L = 1 s** and
     **60 % of L = 0.25 s**. The 0.25 s block is uninterpretable as a
     latency statement and is reported only; L = 1 s is the primary; 3 s is
     robust. A +150 ms offset variant is reported (Binance treated as
     stamped 150 ms early on HL's clock: the signal reads only older Binance
     prices, D7).
  2. HL's *economic* lag of 0.6–1.0 s is **the same size as L**. At a forced
     print, part of g(t) is HL not yet having caught up with Binance; by
     t + 1 s much of that may be gone. This does not invalidate the test —
     it is exactly what the trade must survive — but it means the outcome is
     sensitive to L, and the lag regime changed mid-sample (a criterion-4
     halves split falls on 2025-12-07, after the step).
  3. The signal reads Binance at or before t. If c > 0 (HL stamped late),
     that is up to c ms of Binance *after* the true forced print. The trader
     sees the HL print only after HL publishes the block, so for c below
     that publication delay the signal is still causal in real time; the
     +150 ms variant bounds the rest.

### 3.2 HL timestamp semantics (§6 of the log)

HL `timestamp` is the **block time**: shared by every fill in a block, and
across coins (on a typical day 580–680 k distinct ms timestamps carry 3.6–7.7
M fills; 40–55 % of timestamps carry fills of more than one coin). Gaps
between consecutive timestamps: median 104–130 ms; modes 53–57 ms and
200–202 ms until early 2026, **66–68 ms (and 134, 200)** by April 2026.
Forced fills: 4.56 M on 447 k distinct (coin, ms) times — **10 forced fills
per block**. **Resolution: one block, 55–70 ms.** Same-block prints are
ordered by (timestamp, file, row) (P3 gate G3) or (t, trade_id) for the
liquidation rows; the trigger time is the block time either way.

### 3.3 Gaps at the scan points (prices at or before each forced book print)

**1,214 primary triggers** (BTC 163, ETH 214, SOL 184, XRP 195, DOGE 134, HYPE 324) give **7,532 distinct scan times**. Both executable quote proxies exist within 1 s at 6,777 of them (90 %).

| coin side | triggers | **sweep gap**: median / p90 | per-trigger max sweep gap: median / p90 | **sweep reaching G_MIN** | exec gap: median / p90 | per-trigger max exec gap: median / p90 | exec reaching G_MIN |
|---|---|---|---|---|---|---|---|
| BTC L | 104 | 6.1 / 95 | 8.6 / 39 | 12 | −1.7 / 9.0 | 2.3 / 12.0 | 3 |
| BTC S | 59 | 6.1 / 20 | 10.7 / 30 | 5 | 0.9 / 8.9 | 3.8 / 14.0 | 2 |
| ETH L | 154 | 11.1 / 222 | 10.1 / 41 | 18 | −1.6 / 61 | 3.1 / 19.4 | 7 |
| ETH S | 60 | 6.7 / 25 | 10.7 / 51 | 7 | −0.6 / 8.9 | 2.2 / 12.9 | 1 |
| SOL L | 127 | 8.1 / 174 | 12.7 / 55 | 19 | −4.6 / 26 | 1.6 / 21.0 | 5 |
| SOL S | 57 | 7.6 / 21 | 14.6 / 36 | 5 | −0.7 / 7.7 | 1.9 / 12.3 | 1 |
| XRP L | 156 | 3.2 / 78 | 6.5 / 30 | 10 | −4.6 / 5.2 | −0.4 / 6.2 | 4 |
| XRP S | 39 | 10.1 / 45 | 10.3 / 47 | 4 | 0.7 / 7.9 | 1.9 / 11.8 | 0 |
| DOGE L | 122 | 5.4 / 162 | 8.8 / 38 | 9 | −8.5 / 14.6 | 0.0 / 14.8 | 5 |
| DOGE S | 12 | 8.0 / 21 | 11.2 / 22 | 1 | −1.3 / 23 | 9.0 / 23 | 0 |
| HYPE L | 222 | 29.7 / 417 | 19.8 / 63 | 53 | −0.6 / 12.2 | 4.3 / 18.0 | 8 |
| HYPE S | 102 | 17.7 / 46 | 19.0 / 56 | 19 | −0.3 / 9.6 | 3.0 / 11.9 | 0 |

- The **sweep** gap reaches G_MIN in **162** of 1,213 runs; the **executable** gap in **36**.
- The executable gap at a forced print is about zero: medians −8.5 to +0.9 bp.
- The sweep gap is mostly **HL's own spread being swept**. The forced print hits the bid, while the price a taker would pay (the HL ask) stays near Binance. This reframes 002 §11.

**Per-month trend.** Each cell gives triggers / sweep-set positions / median per-trigger max exec gap / median per-trigger max sweep gap (bp):

| Aug | Sep | Oct | Nov | Dec | Jan | Feb | Mar | Apr | May | Jun |
|---|---|---|---|---|---|---|---|---|---|---|
| 96 / 11 / 2.8 / 14.5 | 140 / 9 / 1.5 / 11.3 | 164 / 25 / 2.2 / 10.8 | 90 / 16 / 5.1 / 17.7 | 47 / 12 / 5.0 / 14.8 | 200 / 29 / 2.4 / 13.4 | 60 / 7 / 5.3 / 17.0 | 78 / 5 / 2.0 / 9.6 | 82 / 2 / 0.7 / 9.2 | 141 / 7 / 1.2 / 8.5 | 116 / 6 / 1.9 / 8.6 |

Both gaps are smallest from March 2026 on: the sweep gap at 8.5–9.6 bp, against 11–18 bp before. That fits the shortening of HL's lag to Binance in November 2025 (F16).

### 3.3b Staleness of the executable proxy (behaviour only)

The executable proxies are measured at the scan points, with prices at or before t. The calm-hour spread proxy is HL's p50 bounce in 24 calm hours per coin (2 × the p50 half-bounce): BTC 0.11, ETH 0.43, SOL 0.42, XRP 0.71, DOGE 1.4, HYPE 0.25 bp.

| class side | scan points | HL proxy age (ms): p50 / p75 / p90 | Binance proxy age: p50 / p90 | HL proxy beyond the forced print, s (HL_q − P_forced)/P_forced: p25 / **p50** / p75 / p90 | calm HL spread | HL proxy older than the run's previous forced print | no HL proxy within 1 s (no signal there) | **upper bound on the bias** (p50 distance − calm spread) |
|---|---|---|---|---|---|---|---|---|
| majors L | 2,905 | 0 / 52 / 257 | 11 / 69 | 1.0 / **4.2** / 16.2 / 57 bp | 0.42 bp | 0 % | 3.0 % | **3.8 bp** |
| majors S | 983 | 0 / 0 / 152 | 23 / 101 | 0.4 / **1.7** / 5.8 / 13.8 | 0.42 | 0 % | 4.2 % | **1.3 bp** |
| alts L | 3,054 | 0 / 220 / 539 | 48 / 298 | 2.4 / **7.2** / 22.1 / 100 | 0.71 | 0 % | 16.7 % | **6.5 bp** |
| alts S | 590 | 0 / 0 / 259 | 64 / 352 | 2.3 / **6.1** / 11.0 / 20.4 | 0.71 | 0 % | 12.2 % | **5.4 bp** |

**Verdict: the executable signal's near-zero gap is not a staleness artefact.**
- **The proxy is fresh.** The median HL quote is in the same block as the forced print. None predates the run's previous forced print; quotes older than 1 s are not used (3–17 % of points have none, so those points have no signal).
- **It sits a few bp beyond the forced print.** The median is 1.7–7.2 bp. That is more than a calm spread, which is what a fresh ask just above a sweeping sale looks like.
- **The bias is small against the threshold.** At most 1.3–6.5 bp, against G_MIN 37 bp.
- **The two signals bracket the truth.** The executable gap underestimates the true one by at most a few bp. The sweep gap overestimates it by HL's swept spread, which is 4–7 bp at the median and 57–100 bp at p90 on the longs side.

### 3.4 Slippage per coin and venue (D6) and G_MIN

Per fill: max(2 bp, the coin's p75 half-bounce in the 10 s before (a) every primary trigger print and (b) every sweep-signal print at the fixed 37-bp G_MIN). Fixing (b) at 37 bp keeps the rule from depending on the signal it prices.

| coin | HL p75 bounce (a) / (b) | Binance p75 bounce (a) / (b) | **slippage per fill HL / Binance** | round trip | **G_MIN** |
|---|---|---|---|---|---|
| BTC | 0.4 / 4.8 bp | 0.2 / 1.8 bp | **2.4 / 2.0 bp** | 27.8 bp | **37.8 bp** |
| ETH | 0.7 / 3.1 | 0.3 / 1.6 | **2.0 / 2.0** | 27.0 | **37.0** |
| SOL | 1.6 / 5.0 | 1.3 / 4.6 | **2.5 / 2.3** | 28.6 | **38.6** |
| XRP | 2.2 / 4.1 | 0.8 / 0.9 | **2.1 / 2.0** | 27.2 | **37.2** |
| DOGE | 3.7 / 14.4 | 1.0 / 3.5 | **7.2 / 2.0** | 37.4 | **47.4** |
| HYPE | 1.7 / 2.8 | 0.5 / 0.9 | **2.0 / 2.0** | 27.0 | **37.0** |

The 4-bp floor is reported.

### 3.5 Filters, positions, fill delays

| set | fallback | no Binance | no signal | stress | **positions** |
|---|---|---|---|---|---|
| **primary (causal trigger, sweep signal)** | 193 | 1 | 1,051 | 33 | **129** (majors **54**: BTC 13, ETH 20, SOL 21; alts **75**: XRP 8, DOGE 5, **HYPE 62**) |
| exec signal (variant) | 193 | 1 | 1,177 | 26 | 10 (6 on 2025-10-10) |
| inclusive stress | 193 | 1 | 1,051 | 41 | 121 |
| fallback events in | — | 1 | 1,239 | 35 | 132 |
| backstop print times in the scan | 193 | 1 | 1,050 | 33 | 130 |
| 002 cascades (not point-in-time) | 229 | 1 | 558 | 40 | 133 |

- **Sides:** 88 L / 41 S (majors 37 / 17).
- **Halves:** 64 / 65, split at 2025-12-07 14:25 UTC.
- **Clusters:** **103 UTC-hour and 75 UTC-day**. At most 4 positions per hour and 11 per day (2025-10-10).
- **Signal gap:** median 43 bp (p10 38, p90 70, p99 176).
- **Signal timing:** the signal fires at the trigger print itself in the median case (p90 45 s later).
- **Entry fill delay** (times only), from t + L to the first print of our aggressor side, for the sweep set:

  | L | HL p50 / p90 / p99 / max | HL fills > 1 s | Binance p50 / p90 / p99 / max |
  |---|---|---|---|
  | **1 s** | **155 / 704 / 1,967 / 2,428 ms** | **7** | **10 / 80 / 175 / 203 ms** |
  | 3 s | 120 / 470 / 1,444 / 1,740 | 4 | 19 / 135 / 385 / 538 |
  | 0.25 s | 150 / 925 / 2,150 / 2,717 | 11 | 7 / 50 / 182 / 247 |

  None is missing within 5 s. The D8 rule charges the highest (or lowest) print of either side over each delay.
- **Holdout rate:** 20 positions in 2026-03 → 06 (122 days), i.e. 0.164 a day → **option A expects n ≈ 17** at the recent rate. The full-year rate (0.382 a day) gives 39.

### 3.6 Power and the holdout (sweep set)

The null P&L is the change of the cross-venue gap over the hold. The simulation draws at the 129 actual signal times: 1,000 discovery draws, then holdout blocks of n = 17 under the D13 CONFIRM rules. "End-to-end" = P(pass all eight, pooled estimate ≥ 10 bp) × P(CONFIRM). The D13 three-second condition is not simulated, so end-to-end is an upper bound. **Plan on the 25–50 bp rows** (reviewer).

| noise per trade | null SE | MDE at t 2.33 (50 % / 80 %) | P(pass all eight): μ = 0 / 10 / 25 / 50 bp | end-to-end: μ = 10 / 25 / 50 bp |
|---|---|---|---|---|
| calm (60-s gap change in calm hours) | 0.2 bp | 0.5 / 0.7 bp | 0.006 / 1.00 / 1.00 / 1.00 | 0.47 / 1.00 / 1.00 |
| pre-signal, **mostly pre-cascade** (g(t−60) − g(t−120): majors 11.1, alts 7.2 bp) | 0.8 | 1.9 / 2.6 | 0.007 / 1.00 / 1.00 / 1.00 | 0.49 / 1.00 / 1.00 |
| in-cascade g_exec(t) − g_exec(t−60), t − 60 ≥ window start (majors n 13, 21.8 bp; alts n 9, 13.1 bp; selection-inflated) | 1.6 | 3.6 / 4.9 | 0.007 / 1.00 / 1.00 / 1.00 | 0.37 / 1.00 / 1.00 |
| in-run executable 60-s change (majors 13.5, alts 40.5 bp) | 2.9 | 6.7 / 9.1 | 0.007 / 0.86 / 1.00 / 1.00 | 0.18 / 0.90 / 1.00 |
| in-run **sweep** 60-s change (majors 37.4, alts 78.0 bp) | 5.8 | 13.4 / 18.2 | 0.006 / 0.25 / 0.98 / 1.00 | 0.04 / 0.47 / 0.91 |
| **25 bp per trade** | 2.2 | 5.1 / 7.0 | 0.007 / 0.98 / 1.00 / 1.00 | 0.25 / **0.99** / 1.00 |
| **50 bp per trade** | 4.4 | 10.3 / 14.0 | 0.007 / 0.45 / 1.00 / 1.00 | 0.10 / **0.64** / 0.99 |
| 100 bp per trade | 8.8 | 20.5 / 27.9 | 0.007 / 0.11 / 0.67 / 1.00 | 0.01 / 0.21 / 0.64 |

Under no effect, end-to-end is 0.000 in every row; P(CONFIRM) on one null holdout draw is about 0.07.

**Holdout options** (P(CONFIRM) at a true 25 bp):
- **A) end 2026-10-09:** n ≈ 17 at the recent rate (39 at the full-year rate). P(CONFIRM) is **0.99 under 25-bp noise** and **0.67 under 50-bp noise**.
- **B) a fixed later end date.**
  - Under 25-bp noise, P ≥ 0.8 needs n = 7, which A already exceeds.
  - Under **50-bp noise** it needs **n = 24**: at the recent rate that is **147 days, ending 2026-11-24**; at the full-year rate, 63 days, ending 2026-09-01.
  - B is therefore stated as **2026-11-24**. The Binance pull would grow to about 7 GB and the HL fills to about 18 GiB (estimates scaled from A).

## 4. T1 — the trade

For each primary trigger (fallback dropped), in time order per coin:

1. **Scan** the forced book prints from the trigger print to the run's end, in execution order. At each print: g_sweep = s (P_bn − P_forced) / P_forced, where P_bn is Binance's last trade at or before the print (HL clock).
2. **Signal** at the FIRST print with g_sweep ≥ **G_MIN(coin)** (§3.4). One trade per trigger. The executable-proxy signal (`xv.quotes`) is a reported variant.
3. **Stress** (005 D5): skip if the market-minus-own-coin trailing-5-min liquidation notional reached $77 M at any point in [min(t_start, t − 1 s), t − 1 s].
4. **One position per coin:** skip if t < the previous t + `xv.busy_ms` (L + 5 s + 60 s + L + 30 s).
5. **Entry:** market orders sent at t0 = t + **L (1 s)**. Each fills by the **D8 rule** within 5 s:
   - **BUY** = max(the first buy-aggressor book print at or after t0, the highest book print of either side in [t0, that print's time]);
   - **SELL** = min(the first sell-aggressor print, the lowest print in between);
   - forced, backstop and ADL rows are never candidates;
   - plus adverse slippage per fill (§3.4).
   - **If a leg has no qualifying print, the position is never opened.** It is counted.
   - **t_open** = the later entry fill.
6. **Exit decision:** the first τ ≥ t_open at which the **unwind executable gap** is ≤ 0, else t_open + 60 s. For a long-HL position the unwind gap is the HL bid (last sell-aggressor print within 1 s) against the Binance ask (last buyer-taker trade within 1 s). It is evaluated at t_open and at every print of either venue.
7. **Exit fills** by D8: at τ + L, or at t_open + 60 s for the scheduled cap.
   - With no qualifying print within 30 s, the fill is the window's **worst print**, counted.
   - Fixed exits at t_open + 5 / 15 / 30 / 60 / 300 s are reported.
8. **Outcome** on the HL notional: four fills, each with fees and slippage (`xv.net_return`). The gross return is kept for the placebo.

## 5. Pass — all six coins pooled, all eight criteria

1. mean > 0 with **hour-clustered t ≥ 2.33**;
2. **day-clustered t ≥ 1.96**;
3. mean and median of the same sign;
4. both count halves positive;
5. both liquidation sides positive;
6. **majors and alts each positive** (alts ≈ HYPE, F7);
7. positive without the largest-positive hour;
8. **positive without the largest-positive DAY** (2025-10-10 holds 11 of the 129 positions).

**Placebo** (`drift.placebo`):
- 200 seeds; gross returns are sign-flipped per UTC-hour cluster, with costs kept. Per-day flips are reported.
- Under the D8 fills, the **gross return already contains both venues' spread** and the delay drift: entry at the ask or worse, exit at the bid or worse. "Costs kept" therefore covers fees and slippage only, and the flip symmetrises a spread-paying gross, which makes the placebo slightly conservative.

## 6. Reported, decides nothing

- **Universes:** majors, alts, per coin (hour and day).
- **Exits and legs:** fixed exits; exit reasons; hold times; each leg separately; the entry gap at the fills against the signal gap.
- **Fill accounting:** entry skips (never opened) and the missing leg; exit fallbacks (worst print); missing exits; fill delays per venue, at entry and exit.
- **Latency:** 3 s, and **0.25 s, which cannot be read as a latency result** (§3.1).
- **Fill and slippage variants:** side-blind fills; slippage 0; the 4-bp floor.
- **Signal and event variants:**
  - the **exec signal** (10 positions);
  - inclusive stress;
  - fallback events in;
  - backstop print times;
  - the **002 cascades (not point-in-time)**;
  - a Binance offset of +150 ms.
- **The half-life — the main input for H1.** For each signal and for every primary trigger (notional-weighted over all its forced book prints), the **sweep gap** is tracked at +0, +0.25, +0.5, +1, +2, +5, +10, +30 and +60 s. That is HL's last non-forced book print against Binance's last trade, after the forced print and Binance at t0. The **executable gap** is tracked the same way.
  - Each point splits into **hl_close** (HL reverting) and **bn_close** (Binance moving).
  - A resting provider (H1) is filled *at* the sweep price. **If hl_close at +0.25 to +1 s carries most of the sweep gap, the reversion is a maker's, not a taker's**: only a resting order earns it, and this taker trial cannot.
  - If instead the gap closes on Binance, or persists beyond L, a taker can earn it, and T1 measures that.
- **The 002 1-minute consistency check** on the cascades.

No reported block becomes a primary on discovery.

## 7. Known weaknesses and design flaws, stated now

- **F1 — Clock and lag.**
  - HL lags Binance by 0.6–1.0 s; the lag stepped down in November 2025.
  - The stamp offset is bracketed (|c| ≲ 150 ms).
  - 0.25 s is not a latency result.
- **F2 — Backstop and ADL rows** are excluded. Forced rows are signal points, never fills or quotes.
- **F3 — The fill rule (D8)** uses prints as a proxy for the touch. There is no queue and no depth; slippage is the only impact model.
- **F4 — The exit** uses the executable unwind gap, which is fresh (§3.3b) and biased by at most a few bp.
- **F5 — Slippage.** The p75-bounce rule is thin for DOGE on HL (14 bp p75 at the deepest prints).
- **F6 — Event selection: fixed** by the causal trigger. The cooldown does not re-arm inside a long run.
- **F7 — Alts ≈ HYPE.** 62 of the 75 alt positions are HYPE, where HL *leads* Binance (§3.1). "Positive in alts" mostly tests HYPE; per-coin blocks are reported.
- **F8 — Samples.**
  - 54 majors (BTC 13), 41 on the shorts side.
  - The holdout has about 17 positions at the recent rate.
- **F9 — Cross-coin overlap:** up to 4 positions in an hour.
- **F10 — The crash day:** 11 positions on 2025-10-10. Criteria 7 and 8 and the day t are the guard.
- **F11 — Bear year:** 68 % of positions are longs liquidated.
- **F12 — No HL book depth.**
- **F13 — The signal gap is a sweep gap.** §3.3 and §3.3b show it is mostly HL's swept spread. The trial's fills charge that spread; the signal does not net it out.
- **F14 — The executable gap beyond costs is rare.**
  - 36 of 1,213 runs; the exec-signal variant has 10 positions, 6 of them on 2025-10-10.
  - Under the sweep signal, most entries will see an executable gap near zero at t0. T1 therefore mostly measures whether HL's refilled book or Binance moves in the trade's favour within 60 s, net of about 28–37 bp.
- **F15 — Observability.**
  - HL's public `trades` websocket carries no liquidation flag, as far as I know offline (**to be verified**), and no public liquidation stream exists.
  - Live use needs a non-validator node's fills.
  - **L = 1 s from block time is a best-case floor.** D13 adds the 3-s condition.
- **F16 — Decay.**
  - Both gaps are smallest from March 2026 (sweep 8.5–9.6 bp, exec 0.7–2.0 bp, as the median per-trigger maximum).
  - HL's lag shortened from 1,000 to 650 ms in November 2025.
  - Any edge may be disappearing, and the holdout rate is about 0.16 a day against 0.38 over the year.
- **F17 — Proxy staleness: measured, small** (§3.3b). The upper bound on the bias is 1.3–6.5 bp. Points with no proxy within 1 s carry no signal.

## 8. Built before any outcome is computed

- [x] **`ct/xvenue.py`.** The quotes, the D8 fill rule, the plan, the simulation (entry skips, t_open, the unwind exit, exit fallbacks, fill delays), eight criteria, the decision with the 10-bp and 3-s conditions, CONFIRM, the two-mode half-life (`gap_profile`, sweep / exec), and power with end-to-end.
- [x] **`ct/bn.py`.** Binance lookups (taker filter, verifier hook).
- [x] **`l8_run.py`.**
  - The primary is now the sweep signal (`VARIANTS`).
  - `hl_quotes` returns times and prices, and the quotes are kept per event.
  - The half-life is computed in both modes.
  - Also: `--manifest`, per-file verification, the full-import guard, `--events`.
- [x] **`l8_describe.py`.** §3, including the staleness section (`stale`). The log is written even if a section fails.
- [x] **`tests/test_xvenue.py`.** **100 checks pass.** They now include both half-life modes (sweep: last prints of either side; exec: quote proxies within 1 s).

**The guard.** The run refuses unless all of these hold:
- status REGISTERED;
- (a) the file is tracked;
- (b) it is unmodified;
- (c) it is committed;
- (d) the guarded code is clean;
- (e) every file below matches;
- (f) `results.jsonl` holds no 006 row.

Today it refuses on the status, (a), (c) and (d).

**What is guarded.**
- **Code:** every research module the runner imports, transitively.
- **Data:** the event file, `liquidations.parquet`, the Binance download manifest, and `data/scratch/l8_data_manifest.json`. That manifest gives the path and SHA-256 of all **1,555 files** the run can read: 245 HL fills days, and 655 aggTrades plus 655 bookDepth parquet files, 35.6 GB in total. It was rebuilt for revision 3.
- The run checks each file's SHA-256 against the manifest as it reads it.

| file | sha256 |
|---|---|
| `l8_run.py` | `9124c955dd6358e0eff6169424afc3ed1a5f0e07c8ce73f4712c523b61ee32df` |
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/xvenue.py` | `ff97574c9c0ae81cfb0d7b2c85d20825745e59968b5102df58bafae9eff632b3` |
| `ct/bn.py` | `e4e46858d43b839e3e85b51f4d249004ea74ce8fc608f7628ac4878a7cc7a6b2` |
| `ct/drift.py` | `4c15eae52c46b0374eff0f2531ca08d5602faf20fc49bebeaf375d7656601b85` |
| `ct/eventstudy.py` | `08a780b4d40bad811d5d826d42d29c44d673246048d85cf31ec1be2eaf6018b0` |
| `ct/provider.py` | `22185c5f1ccb839257efc6025db929aea5283c686fca284a5e05492065c171d0` |
| `ct/passive.py` | `3aa6f1da7bd63d2856479dd7a3e3f7ef9c84890e52093028c7b3e7bb55bd86e2` |
| `ct/tape.py` | `ab803d7cf740cd6d25f229d5d7e5e4cc33933f8ed1209ccc10f0981dbd56bf0e` |
| `ct/archive_fills.py` | `0727c03224e26ab13d70384fb1d974a47eaa51c00ef98f0ba61103a97715b418` |
| `ct/liq.py` | `d03159c1963097d73536f17c7282c37c159784d418c1b55783e09051226bb292` |
| `ct/trigger.py` | `9c15b529474beb3fd330615aa97a1154a22820a311ab23ba895c8c410009d61d` |
| `ct/leaders.py` | `378fbf403e8c372c2d2795b5b19c256199a1dc9360b8c6a4da55803f501ed329` |
| `ct/net.py` | `90092590941e805eb465e7e1f05467c05e376bf8b07fe6dbdacfdb6cf7e00006` |
| `ct/bitget.py` | `024ce24d11a4ec7366b8ea54dd719f30700f235b9f8396543f67b25b0684204d` |
| `l7_run.py` | `786410b77530e0778551939f3849c40d3af1b745b1630a57ca1b58dedd1f0473` |
| `l6_run.py` | `108f40f4678ac50e46c850d4a20d181334cfd491b8345ffa59a9f8dd9988f41a` |
| `l5_run.py` | `b8e29099e36378b625eb308303f1a4964255b8986d3437d29c675702bcbd8703` |
| `data/derived/hyperliquid/cascades.json` | `b21f2df0d4a87387a1b49f9636cc6a0b94306c4286accef1a976a9e7d6639819` |
| `data/derived/hyperliquid/liquidations.parquet` | `3ee8503d19269ce5175056928df2e500f9d98c002c9283f8265b84257c1c4ecf` |
| `data/binance/manifest.json` | `9ab867d6c84ad0bd48d96561cedc1d0ef56ddfe9648b6627f81a397c278549af` |
| `data/scratch/l8_data_manifest.json` | `cbf2b931d7bb1c628b5669814014186d6bcf6c88fa27433e9de47e340cf91814` |

## 9. Owner decisions — PROPOSED, not approved

| # | item | proposed |
|---|---|---|
| D1 | events | 004's causal trigger (trailing 60 s ≥ max($100 k, 0.5 × the trailing-30-day median day), 15-min cooldown); six coins; discovery only; fallback-median events dropped; scan to the run's end. The 002 cascades are reported, labelled not point-in-time |
| D2 | universe | six coins pooled; majors and alts each positive; per coin reported |
| D3 | prints | forced **book** prints are the scan points. HL quotes and fills use non-forced book prints only. Backstop and ADL rows are excluded |
| D4 | signal | **the sweep gap** (the forced book print vs Binance's last trade at or before it). The first print with g_sweep ≥ G_MIN(coin) fires. **The executable-proxy signal is reported** (§3.3: 36 runs reach G_MIN, 10 positions) |
| D5 | G_MIN | per coin: round trip + 10 bp. BTC 37.8, ETH 37.0, SOL 38.6, XRP 37.2, DOGE 47.4, HYPE 37.0 bp |
| D6 | slippage | per coin and venue, max(2 bp, the p75 half-bounce) (§3.4). The 4-bp floor and 0 are reported; 0 is not a rescue path |
| D7 | latency and observability | **L = 1 s** from block time, **a best-case floor** (F15: no public liquidation flag on HL's trade stream, to be verified). 3 s is reported and part of D13. 0.25 s is reported **with the warning that it cannot be read as a latency result**. The +150 ms offset is reported |
| D8 | fills | BUY = max(the first buy-aggressor print at or after the send, the highest print in between); SELL mirrored. Book prints only. Entry window 5 s: a missing leg means the position was never opened (counted). Exit window 30 s: otherwise the worst print (counted). Side-blind fills are reported |
| D9 | exit | the unwind executable gap ≤ 0, scanned from t_open, then D8 fills at τ + L. Otherwise the cap at t_open + 60 s. Fixed exits are reported |
| D10 | stress / overlap | the 005 D5 switch up to t − 1 s; one position per coin (`busy_ms`) |
| D11 | inference | hour-clustered t ≥ 2.33 and day-clustered t ≥ 1.96 |
| D12 | pass | the eight criteria of §5; the hour-flip placebo with costs kept, 200 seeds |
| D13 | decision on a pass | **pass (all eight) AND a pooled estimate ≥ 10 bp AND a positive mean at L = 3 s → ask the owner** for both holdout pulls: Binance aggTrades and bookDepth (~4.9 GB under A, free) and HL fills (~12 GiB under A, requester-pays, for 006 only — 005 failed). Exact byte counts come from a LIST first. Read once. CONFIRM if mean > 0 with one-sided hour t ≥ 1.64, median > 0, and mean ≥ 0 without the largest-positive hour. **Holdout end, owner's option: A) 2026-10-09** — n ≈ 17 at the recent rate (39 at the full-year rate); P(CONFIRM) at a true 25 bp is 0.99 under 25-bp noise and 0.67 under 50-bp noise. **B) 2026-11-24** — n ≈ 24 at the recent rate, which is the n for P(CONFIRM) ≥ 0.8 under 50-bp noise (under 25-bp noise A already suffices); a discovery pass waits for that date; pulls ~7 GB Binance + ~18 GiB HL (estimates) |
| D14 | the H7 bar | ≥ 25 bp net per major fill, realisable: the yardstick, reported on discovery and on the holdout |
| D15 | decision on a fail | "**A taker entering 1 s after a forced print that shows a sweep gap ≥ G_MIN does not earn net of costs in the discovery year; the executable gap beyond costs is rare (behaviour, §3) — the cross-venue line closes for takers; H1's prospects are read from the reported half-life.**" |
| D16 | family | trial 15 on the discovery year's events. Fourteen prior trials read them; the Binance outcome data is new, the selection is not |
| D17 | **register or close** | **(i) Register the sweep-primary version** (the orchestrator's recommendation), because its reported half-life decides H1: is the sweep's reversion a maker's within the first second, or a taker's? **(ii) Close H3 now** on the behaviour finding: the executable gap beyond costs opened in 36 of 1,213 runs, and the sweep gap is HL's own spread. No outcome is read, and H1 is then decided without the half-life. **(iii) Redesign** under a new trial count |

The runner reads its parameters from `ct/xvenue.py`, `l8_run.py` and `l6_run.py`'s D1 constants. Any change to D1–D12 is a change there and here, with a new hash table and a new data manifest.
