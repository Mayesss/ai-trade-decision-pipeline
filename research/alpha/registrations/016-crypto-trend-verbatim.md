# 016 — Crypto time-series trend following: the Zarattini–Pagani–Barbon (2025) rule verbatim, registered before its forward span exists

**Status: REGISTERED 2026-10-14** (orchestrator, under the owner's research mandate; review A: register (Revision 1
recheck, after the end-date change made in Revision 2); review B: register (Revision 2 recheck)). Every row of §13 is
adopted as recommended. Registration procedure: this commit is made in the main worktree and tagged
`r016-registered`; the tag must be kept (the guard does not require it; it keeps this commit reachable from `--all`
against the worktree-squash attack). Residuals stated at registration (deliberate tampering only, review B Revision 2
recheck): a concurrent same-user process racing the runner's fresh bytecode prefix in the temporary directory (forged
`ct16` code ran in 1 of 20 fetcher runs in the reviewer's probe, 0 of 20 runner runs); an in-process `-c` program
posing as the fetcher. Known cosmetic defect: some code comments and labels still say "G = 33" / "2027-12" (the
registered values are G = 45 and 2028-12-31, enforced in code and tests). Nothing runs before 2029-01-08.

Previously DRAFT — Revision 2 (2026-10-13, drafter), the final revision before registration. Revision 1 answered two
independent reviews (`reviews/016-review-a.md`: 2 blockers, 3 majors, 10 minors; `reviews/016-review-b.md`: 1
blocker, 4 majors, 11 minors); both reviewers' rechecks of Revision 1 (the appendices of those files) say register
after one change each, and Revision 2 makes them: **the end date moves to 2028-12-31** (review A R1-M1, accepted by the
orchestrator) and **`-X` options are refused** (review B R1-M1). Every finding and its fix is in §R1 and §R2. Every
row of §13 is PROPOSED.

**What changed, in one paragraph.** The decision statistic is now a **holdings-based timing statistic** (Lo 2008),
not a factor α: constant-slope α manufactures α out of a constant drift when volatility has a leverage effect
(review A B1). Its critical value is calibrated at 3,000 draws per world on 19 regime worlds, the reviewer's
leverage-effect bull worlds included, with the Monte Carlo error stated and a 2-SE margin. Span (c) must pass on its
own at 10% (B2). Decay is tested on the same statistic against the paper's Table 4, under any forward drift (M1),
and the four outcomes became a three-step ladder. The guard is rebuilt on 015's fixes: bound to the real repository,
read from HEAD, bound to the one REGISTERED commit for the 27 months to the read, `python3 -I -S` with a pinned
interpreter, no mocks or `now` near the real tree, and a mutation harness that shows each protection's test failing
when the protection is removed.

**Nothing in this draft was computed on real data beyond hashes, file counts and first/last months** (§15). Every
size, power and false-pass number below comes from synthetic worlds (`r016_run.py --power`, which reads no data).
The runner refuses any fetch or read before **2029-01-08** (the registered end 2028-12-31 + 8 days), and refuses
everything while this file says DRAFT.

Programme: `docs/alpha-discovery-2026-10-13.md` (round 4, light C1). Scout: `scouts/c1-crypto-trend.md`.
Ledger entries read: `000t` (C1 scout), `000q` (D1 seasoned-path look), `copytrade/registrations/000l` (the
2026-07 → 2026-09 market seen by other studies). Templates ported: `r008_run.py`, `r008_fetch.py`, `r013_run.py`,
014 Revision 2 (`ed8f06b`, `<rev>:./<path>`), and 015 Revision 1's guard (`r015_run.py`, review B's fixes).

---

## 0. Hypothesis, mechanism, sign, and what has been seen

**Hypothesis.** The paper's top-20 long-only crypto trend book, run verbatim on Binance USDⓈ-M perpetual closes
from 2025-03-20 (the day after the paper's sample ends) to 2028-12-31, earns a positive **timing return** net of the paper's 10 bp
costs: the part of its return that comes from *when* it holds each coin, beyond what holding each coin's average
weight throughout would have earned (Lo 2008's active component; §4). The same statistic, read against the paper's own
strength, decides whether the rule has **decayed** from what Table 4 published.

**Mechanism (who pays).**
1. **Under-reaction, then late crowds.** Information about adoption, listings, unlocks and flows diffuses slowly
   through a retail-dominated market. Prices trend first; a levered crowd joins late and pays the trend at its top.
   The rule enters on a new n-day closing high, so it buys after the move has started and before the crowd.
2. **Forced flow in the trend's direction.** Rallies squeeze short perp positions and liquidate them (fact 2 of
   the programme: forced flow overshoots); declines liquidate levered longs. Both push price further in the trend
   direction, for days. The midpoint trailing stop exits when the move gives back half its channel.
3. **Why it could survive.** Trend books are long volatility-of-drift and short whipsaw; they lose steadily in
   chop. Most capital cannot hold a strategy whose return arrives in a few months a year (the paper's
   "stair-step", §7.2 p. 21). The edge is a risk-bearing and patience premium, not an arbitrage.
4. **Why it may have decayed.** The paper is public (2025-04), CTAs now trade crypto futures, and the
   post-publication decay literature (McLean–Pontiff: −58% on average across 97 predictors) applies. The ladder's
   `decayed` step asks this directly.

**Sign.** Long-only (the paper's primary, §1 p. 3: "we restrict attention to a long-only implementation"). Timing
return > 0.

**What has been seen (exposure).**

| look | calendars | what it saw | exposure to 016 |
|---|---|---|---|
| **The paper** (Zarattini, Pagani & Barbon, SFI RP 25-80, v. 2025-04-09) | 2015-01-01 → 2025-03-19 (top 20/50), 2020-01 → 2025-03-19 (ranks 1–100) | the rule's in-sample results: SR 1.57, α 10.8%/yr, β 0.08, vol 9%, MDD 11% (Table 4) | the rule and its published Sharpe are public priors; **nothing out of its sample was opened** |
| **Post-publication decay literature** (public priors) | — | McLean–Pontiff; the desk's Arefev note (TSMOM ≈ 0 after 2020-07) | direction of the prior only |
| **Designer knowledge** (disclosed) | span (a), 2025-03-20 → 2026-06-30 | the drafting and orchestrating model's training data cover the crypto market path through about mid-2026: a rally into early October 2025, the 2025-10-10 liquidation cascade, and a large decline into 2026 | **the choice to test this rule now is not independent of (a).** The rule is copied verbatim and cannot be tuned; §7 decides how (a) enters |
| **`000t`** (C1 scout) | Binance perps 2020-01 → 2026-06 | universe counts, Donchian flip counts and long fractions, turnover, gross exposure, σ90 levels, unconditional funding by year, correlations, BTC/ETH/EW moves in five named stress episodes | behaviour only; no return after any signal; no strategy P&L. No parameter here is set from it (the rule is the paper's) |
| **`000q`** (D1 scout) | Binance seasoned coin-weeks 2021-11 → 2024-01 | continuation after +50% / +90% touches | outside 016's test span; no threshold here is near those levels |
| **`000l`** | 2026-07 → 2026-09 | **the 2026-09-14 alpha-lab sweep computed momentum and reversal ICs on 83 Bitget perps through 2026-09**; the live AI trader's inverted study (2026-06-27 → 2026-10-10: fading the AI paid) | **inside span (c).** Months 2026-07 → 09 of the forward span are not unseen by this programme: a cross-sectional momentum IC on 4H bars is a different statistic from this time-series book, but its sign is outcome-adjacent |
| **008** (registered) | HL fills 2026-07-01 → 2026-09-28 | will read inverted-positioning outcomes in those months (its fetch is in progress) | 008 reads HL wallet data; when it reports, its result is about 016's span-(c) months. Stated, not avoidable |
| **The HL discovery year** (16 trials) | 2025-07-28 → 2026-06-30 ⊂ (a) | other signals (copying, forced flow, carry, listings) | no trend statistic was ever computed on it |
| L4 holdout A (released, unread) | 2025-03-20 → 2025-07-27 ⊂ (a) | nothing | 016 consumes about four months of it |

## 1. Trial, family, calendars

- **New family: "crypto time-series trend". This is trial 1.** One trial, long-only, verbatim. The long/short
  version (paper Appendix) is reported, never decisive.
- **Carried-over programme counts** (programme doc §2): HL discovery year 16 trials, no pass; HL holdout read only by
  008 (2026-07-01 → 2026-09-28); Binance new-perp listing windows 2 trials (013), no pass; Deribit options 0 (014
  parked); Binance 2021–23 1 trial (009); L2 carry 3, closed; HL `xyz` 0; US equity calendar flows 1 (015, drafted
  alongside). 016 shares no family with any of them.

**Calendars (fixed; D17).**

| span | dates | status | months (clusters) |
|---|---|---|---|
| warm-up | each symbol's first archived Binance perp close (from 2019-12) → 2025-03-19 | signal states, σ90 and universe only; **no return is booked** | — |
| **(a)** | **2025-03-20 → 2026-06-30** | out of the paper's sample; before the programme's cut; **designer-known** | 15 (the 12-day March 2025 stub joins April) |
| **(c)** | **2026-07-01 → 2028-12-31** (Revision 2: one year longer; review A R1-M1) | **forward, unread, claimed openly for this family.** It does not exist yet in full. 008 reads HL fills in 2026-07 → 09, and `000l` looked at momentum through 2026-09 (stated in §0) | 18 |
| **pooled (a)+(c)** | 2025-03-20 → 2028-12-31 | **the primary**, with (c) required to pass on its own at 10% (§7) | **G = 45** |

**One read only**, on or after 2029-01-08 (2028-12-31 + 8 days). There is no interim look and no second look inside 016. A later
extension (2029-01 →) would be a new registration with its own forward span.

## 2. The rule, verbatim (paper sections and printed pages)

| element | the paper | here |
|---|---|---|
| data | daily OHLCV closes (§3, §4.1 p. 6–7: "DonchianUp = max{Close_t, …, Close_{t−n+1}}") | Binance USDⓈ-M 1d kline closes (00:00 UTC) — D11 |
| channel | Up, Down on **closes**, today included; Mid = ½(Up + Down) (p. 7) | same |
| entry | "as soon as the closing price hits the upper boundary": Pos = 1 if Close_t = Up_t(n) (§4.1 p. 7; §4.3 p. 10) | same; ties enter (D2) |
| exit | "the first closure below a trailing stop"; Pos = 0 if Close_t ≤ TrailingStop_t (p. 7, p. 10) | same; **≤** (the formula's sign, p. 10) |
| stop | TrailingStop_{t+1} = max(TrailingStop_t, DonchianMid_t), "never … updated downward"; the stop used on day t+1 is set at the end of day t (p. 7); a new position starts at the entry day's Mid (footnote 3) | same; a long that prints a new high keeps its stop (not a new position) — D2 |
| case order | Pos = 1 if Close = Up; 0 if Close ≤ stop; else Pos_{t−1} (p. 10) | same order: an n-day high at or below the stop stays long (D2) |
| lookbacks | n ∈ {5, 10, 20, 30, 60, 90, 150, 250, 360} (§4.3 p. 10) | same; flat until n closes exist (D2) |
| sizing | w_n = min(0.25/σ_t, 200%) × Pos_n; σ_t the 90-day ("3-month") annualised vol of the asset's daily returns (§4.2 p. 8–9; §4.3 p. 10) | same; σ = sample SD of 90 simple returns × √365 (D3) |
| Combo | w = (1/9) Σ_n w_n; r_t = w_{t−1} × ret_t (§4.3 p. 10) | same; positions set at close t earn from close t (no lag); a one-close lag is reported |
| costs | "net of a transaction cost of 10 basis points" (§5.1 p. 15) | 10 bp × |Δ notional| per coin, every trade incl. resets and cuts (D7) |
| threshold | "only rebalanced if the difference between the current and target allocation exceeds 20%", applied **only** to volatility-driven changes; signal changes trade at once (§5.1 p. 14–15) | per sub-model, **absolute** 0.20 of sleeve weight against the drifted held weight (D5); relative 20% reported |
| universe | month end: listed ≥ 365 days; not wrapped / stablecoin / NFT; median daily volume ≥ $2 M over the preceding 30 days; rank by median volume; top B (§7.1 p. 18) | Binance perps, D8–D9 |
| portfolio | 1/B of capital per asset; each runs the Combo independently; rebalanced (reset to 1/B) at month end (§7.2 p. 18, the AUM formula) | B = 20 (Table 4's headline); continuing members keep their held weights, new members take their current target (D10, D12) |
| removal | intra-month if the 30-day median volume < $1 M or the 30-day median daily price change < 0.5% (§7.2 p. 18) | checked at every close (D9) |
| funding, borrow | none (spot aggregates, CoinMarketCap) | primary: none (D11); deployable variant: real Binance funding |
| direction | long-only primary (p. 3) | same |

**Where the paper is silent, decided from mechanism (all in §13):** the annualisation (√365: crypto trades every
day), σ on simple returns with n − 1, the rule's behaviour before n closes (flat), absolute vs relative threshold
(absolute: allocations are quoted in % of capital, "200%"), netting of the nine sub-model trades inside one coin,
dead and missing rows, the March 2025 initialisation, and how a new member enters (at its current target: the signal
is a property of the price path, not of membership).

## 3. Data

**Source and venue (D8, D11).** Binance USDⓈ-M, the free public archive (`data.binance.vision`):
- **Months ≤ 2026-06:** the C1 scout's pull, `data/scout_c1/bn_um_klines_1d_monthly/` (19,730 zips, 787 symbols,
  each verified against Binance's `.CHECKSUM` at pull time). It is pinned here by the SHA-256 of its `SHA256SUMS`
  (§16), and every file is re-hashed against that list as the read opens it. The scout's funding pull is **not**
  used (it covers the scout's own top-20 months only).
- **Months 2026-07 → 2028-12 (klines) and 2025-03 → 2028-12 (funding, every USDT symbol):** fetched by
  `r016_fetch.py` after 2029-01-08 (§11), each zip verified against `.CHECKSUM` and pinned by a manifest digest in
  the `fetched` row.
- **Contract types:** the `fapi/v1/exchangeInfo` snapshot fetched for 013 on 2026-10-11 (its `serverTime` is
  2026-10-09T07:16Z; copied into `data/r016/static/`, pinned) plus the snapshot the fetch takes in 2028. A symbol is
  excluded if the 2026 snapshot marks it as not (`PERPETUAL`, `COIN`) — TradFi equity/commodity/FX perps (onboarded
  from 2025-12), index perps, deliveries — or, **only for symbols the 2026 snapshot does not list**, if the 2028
  snapshot does (review B m6: a later reclassification is never applied retroactively). Plus fixed name lists:
  stablecoins, wrapped tokens, gold tokens, and the scout's TradFi names (D8).

**Point in time.**
- **Listing age** = days since the perp's first traded daily close (D8, decided now, before any read; review A m3,
  review B m6). The paper ages the *asset* (CoinMarketCap IDs). The perp-age rule **does bind in span (a)**: about 220
  live symbols were first archived between 2024-04 and 2025-06, and token migrations restart both the age and the
  Donchian history at the new ticker: MATIC→POL (2024-09), FTM→S (2025-01), RNDR→RENDER (2024-07), EOS→A (2025-05),
  MKR→SKY (2025-09). It is kept because it is point in time and unambiguous; chaining the five pairs would be a
  hand-made list. The members that only the age rule excludes are reported month by month (`age_excluded_by_month`).
- **Ranking window** (review A m4): the paper filters on the "preceding 30 days" and ranks by the median "during the
  same month" (p. 18); here both use the trailing 30 days (Fig. 5's caption: "trailing 30-day trading volume"). They
  differ only in 31-day months and February. A decision, not a quote.
- **Volume** = Binance perp quote volume (USDT), not CoinMarketCap's cross-exchange spot aggregate; it tilts the top
  20 toward perp-speculative names (review A m5). The $2 M floor never binds near the top 20 (rank-20 median ≥ $71
  M/day at its 2026-05 low, scout §5.5). A spot-volume overlap is **not** added (§R1, A-m5: it needs a second archive
  pull with its own fetch path and tests, for a number that decides nothing).
- **The delisting cut (D9).** Binance's archive keeps writing flat, zero-trade 1d rows for dead contracts for years
  (L4 scout: FTTUSDT through 2026-10), and the scout found **LUNAUSDT in June 2022's top 20 on May volume after its
  delisting**. So: (i) a symbol is eligible at a month end only if it traded (count > 0) on **each of the last 7
  days**; (ii) a member whose row goes dead during the month settles at its last traded close (Binance settles a
  delisted perp near its index), cost charged, and its sleeve is cash until the month end; (iii) the paper's §7.2
  liquidity exit runs at every close. All three are counted before the decision.
- **Tradability elsewhere (reported).** The fetch probes Hyperliquid (`meta`; per coin, the first funding time and
  the last month start with funding). A reported sub-book keeps only members listed on HL at the month start
  (Binance `1000X` ↔ HL `kX`). The scout found HL lists 19–20 of the top 20 from 2023-12. Bitget is not probed: its
  history retention made the scout's listing probe incomplete, and the deployable venue here is Binance itself.

## 4. The decision

### 4.1 The statistic: the book's timing return (Lo 2008)

On each day t of 2025-03-20 → 2028-12-31, from the book as run (10 bp, no funding):

    A_t = Σ_i (w_i,t−1 − w̄_i) · r_i,t − cost_t

- w_i,t−1 is coin i's book weight held into day t (its notional over book value at the previous close); r_i,t its
  close-to-close return; cost_t the day's fees over book value. Σ_i w_i,t−1 r_i,t − cost_t is the book's return
  exactly (tested to 1e-12).
- w̄_i is coin i's mean weight over its member-days in the span evaluated. Σ_i w̄_i r_i,t is the **passive** book that
  holds each coin at its average weight throughout; A_t is what timing added on top.
- **Why this statistic** (review A B1). Under any constant drift, whatever the volatility dynamics, E[A_t] ≈ −cost:
  holding more of a coin at some times and less at others earns nothing when the drift does not change. Drift that
  changes over time — what a trend rule harvests — gives A a positive mean. A constant-slope factor α does not have
  that property: OLS weights days by variance, and with a leverage effect the book is out exactly when volatility is
  high, so β̂ falls below the book's average exposure and α̂ ≈ (w̄ − β̂)·μ > 0 in any bull (review A's GJR bull worlds:
  false pass 0.05–0.12). Calendar-cell slopes do not cure it and cost 0.2–0.4 of power (Revision 0 §5).
- **t_A** = mean(A) / its CR1 standard error clustered by calendar month (G = 45; the 12-day March 2025 stub joins
  April). The span-(c) statistic t_A,c uses (c)'s days only and **its own w̄** over (c).
- The factor α (BTC, EW alt, vol-managed EW; constant, half-year and year slopes) and the raw Sharpe are reported.

### 4.2 The ladder (review A M1; one read, three outcomes)

| step | condition | outcome | what follows |
|---|---|---|---|
| 1 | **t_A ≥ CAL_H1 = 2.00** (= the largest 95th percentile over the 19 calibration worlds + 2 Monte Carlo SE, rounded up; never below t_44(0.95) = 1.680) **and t_A,c ≥ 1.282** (span (c) alone, one-sided 10%; review A B2) | **`alive`** | the economic bar (§4.3): clears → **`paper_then_scale`** (a forward paper/live period at the paper's sizing, scaling toward **min(the Bayes-Kelly size, 2.8 × the paper's sizing)** — review A R1-m2: at any `alive` the Bayes-Kelly vol of the timing leg is ≈ 50% or more, beyond §9's tail budget); below → `exists_below_bar` |
| 2 | otherwise, **t_A < C2 = −0.94** (the smallest 5th percentile of t_A over nine "as published" worlds − 2 MC SE, rounded down; §6) | **`decayed`** | **`close`**: the family closes; any owner allocation ends |
| 3 | otherwise | **`not_demonstrated`** | "not demonstrated at this power", never "no effect"; any owner allocation ends (review A M2) |

- **"As published"** (review A M1) is the timing strength at which a synthetic decade 2015-01 → 2025-03-19 (a shared
  +75%/yr drift, GJR leverage, t(3) tails) reproduces Table 4's α/vol = 10.8% / 9% = 1.2 against BTC (§6). C2 is the
  t_A that such a rule would beat 95% of the time over 2025-03 → 2028-12 with a forward drift of 0, +40% or −40%/yr.
  So `decayed` has size ≤ 0.05 under any of those drifts.
- **No two point bars in series.** Step 1 is an existence test with a calibrated small-sample critical value; its
  only conjunction is (c)'s 10% significance, whose power cost is stated (§6). The economic bar routes only.

### 4.3 The economic bar (after `alive` only; never a pass condition; review A M3)

- The **deployable variant**: the same book at 6 bp per fill (Binance taker 5 bp, 4.5 with BNB, + 1 bp slippage)
  with real Binance funding on every settlement held.
- Its **timing leg** A^dep (the same statistic, funding included in cost_t; tradable as the book with a static short
  of w̄_i per coin) has an annualised Sharpe SR_A. Shrink it by the positive-part James–Stein factor (1 − 1/t_A²) of
  the H1 t: SR_s.
- Size at the **Bayes-Kelly vol** SR_s / (1 + 1/T), T = 3.78 years (the variance of a Sharpe estimate over T). The
  return there is **SR_s² / (1 + 1/T)**; it must reach **4%/yr** (HLP calm), i.e. SR_s ≥ 0.22.
- Stated plainly: a sanity floor at a like-for-like risk budget, not a hurdle with power. The Revision-0 bar
  (2.8 × min(raw, α) × shrink) compared a 25%-vol book with HLP's calm return and cleared on 67–90% of false passes.

## 5. Calibration (synthetic only; `data/r016/logs/power.json`; Revision 2: end 2028-12-31, G = 45)

**The calibration set (D19): 19 trend-free worlds, 3,000 draws each** (24 coins, rotating volumes, the real
calendar to 2028-12-31; GARCH(1,1) vol and t(4) innovations unless stated; "GJR" = GARCH(0.05, 0.88) with a leverage
term 0.10).

| world | mean t_A | q95 of t_A (± MC SE) | `alive` |
|---|---|---|---|
| flat | −0.82 | 1.02 ± 0.04 | 0.001 |
| bull +60%/yr | −0.66 | 0.99 ± 0.04 | 0.002 |
| bear −60%/yr | −0.78 | 0.93 ± 0.04 | 0.003 |
| drift aligned with the half-year cells (a six-month regime) | −0.03 | 1.60 ± 0.03 | 0.015 |
| two −35% crash days | −0.67 | 1.05 ± 0.03 | 0.003 |
| chop, vol × 1.4 | −0.60 | 1.10 ± 0.03 | 0.004 |
| mean-reverting chop (AR −0.1) | −1.64 | 0.34 ± 0.05 | 0.000 |
| **vol timing pays** (drift +100%/yr in low vol, −100% in high) | +0.37 | **1.93 ± 0.03** (binding) | 0.029 |
| alt β switching by cell, cell drifts | −0.19 | 1.48 ± 0.04 | 0.010 |
| Student-t(3), GJR, cell drifts | +0.18 | 1.78 ± 0.03 | 0.020 |
| bull +60% + a −30% crash | −0.55 | 1.11 ± 0.04 | 0.002 |
| GJR bull +40%, t3 | −0.69 | 0.97 ± 0.04 | 0.001 |
| GJR bull +80%, t3 | −0.61 | 1.01 ± 0.03 | 0.001 |
| GJR bull +80%, t4 | −0.67 | 0.98 ± 0.04 | 0.002 |
| GJR bull +80%, t3, −35% day in (a) | −0.55 | 1.05 ± 0.03 | 0.001 |
| GJR bull +80%, t3, −35% day in (c) | −0.52 | 1.11 ± 0.04 | 0.004 |
| GJR bull +120%, t3 | −0.47 | 1.03 ± 0.03 | 0.001 |
| asymmetric vol timing (+100 / −60), GJR t3 | +0.17 | 1.70 ± 0.04 | 0.018 |
| GJR bear −60%, t3 | −0.80 | 0.93 ± 0.03 | 0.001 |

- **CAL_H1 = 2.00**: the largest q95 + 2 SE is the vol-timing world's 1.930 + 2 × 0.033 = 1.995, rounded up (the floor
  t_44(0.95) = 1.680 does not bind). Revision 1 at G = 33 had 1.75. **Review A's estimate was ≈ 1.84**: its
  trend-free worlds at 1,000–1,500 draws did not include this symmetric vol-timing world at G = 45 (its q95 rises from
  1.69 at G = 33 to 1.93: with more months the inverse-vol timing that pays in that world becomes more visible). The
  next worlds are 1.78 (t3 GJR cells, review A's 1.71 at 2027) and 1.70 (asymmetric vol timing).
- The Monte Carlo SE of each q95 is the half-width of the order-statistic 95% interval / 1.96 (0.03–0.05 at 3,000
  draws). Adding worlds can only raise CAL_H1.
- **The leverage-bull worlds, where the factor α failed** (review A: q95 1.83–2.27, false pass 0.05–0.12), stay flat:
  mean t_A −0.69 to −0.47, q95 0.97–1.11, `alive` 0.001–0.004. What lifts t_A is something predictable from the past:
  vol timing (the inverse-vol sizing is timing too) and the aligned six-month regimes (a six-month trend).

## 6. Power, false-pass and decay rates (synthetic; Revision 2, end 2028-12-31)

**Provenance.** `python3 -I -S r016_run.py --power` wrote `power.json` (per world) and `power_records.json` (per draw):
19 calibration worlds and 9 as-published worlds at 3,000 draws, 15 trend worlds and 4 drift-power worlds at 600, 4
hindsight / regime worlds at 1,000; fixed seeds. CAL_H1 and C2 were then written into `ct16/stats.py`; the records hold
t_A, t_A,c and the raw statistics, never a decision. The run is **resumable**: each finished world is stored in
`data/r016/logs/worlds/` under a key of (spec, draws, seeds, **and the calendar**: end date and cell count, so a
Revision-1 world is never reused at the new end) and is read back, not re-run; this run was paused by the owner and
resumed from the cache. Verified in Revision 1: a second `--power` over the full cache wrote byte-identical outputs;
a finished world re-run from scratch equals its stored records; a draw is bit-identical across processes (a test; a
`set`-order float nondeterminism in a fee sum was found and fixed then). Revision 1's logs are in `data/r016/logs_rev1/`.

**Trend worlds.** Each coin's drift is κ × its daily vol × (0.7 u_t + 0.71 v_i,t), u and v AR(1) with half-life h ∈
{20, 60, 120} days, mean zero, no market drift. The axis is the **mean per-draw 45-month Sharpe**, with the pooled
population Sharpe beside it (review A m1).

| mean per-draw ŜR (pooled SR), h 20 / 60 / 120 | **`alive`** h 20 / 60 / 120 | t_A ≥ CAL only | `decayed` h 20 / 60 / 120 |
|---|---|---|---|
| 0 → 0.02 / 0.09 / 0.09 (pooled 0.17 / 0.23 / 0.25) | 0.008 / 0.008 / 0.017 | 0.012 / 0.018 / 0.022 | **0.23 / 0.15 / 0.17** |
| 0.5 → 0.56 / 0.53 / 0.51 (0.63 / 0.63 / 0.69) | 0.24 / 0.23 / 0.18 | 0.30 / 0.29 / 0.25 | **0.013 / 0.015 / 0.038** |
| **0.75** → 0.83 / 0.75 / 0.78 (0.89 / 0.86 / 0.94) | **0.59 / 0.38 / 0.32** | 0.66 / 0.47 / 0.42 | 0.000 / 0.003 / 0.012 |
| **1.0** → 1.10 / 0.98 / 1.02 (1.13 / 1.08 / 1.15) | **0.85 / 0.67 / 0.53** | 0.90 / 0.75 / 0.66 | 0 / 0 / 0.007 |
| **1.5** → 1.49 / 1.48 / 1.49 (1.48 / 1.51 / 1.58) | **0.96 / 0.93 / 0.88** | 0.98 / 0.96 / 0.93 | 0 / 0 / 0 |

- **`alive`:** at a true SR of 1, **0.85 / 0.67 / 0.53** (Revision 1 at 2027-12-31: 0.65 / 0.39 / 0.23; review A
  estimated 0.87 / 0.67 / 0.60 at 2028-12-31); at 0.75, **0.59 / 0.38 / 0.32** (review A: 0.43 at h 60); at 1.5,
  0.96 / 0.93 / 0.88. The extra year more than doubles (c) (18 → 30 months), which makes t_A,c ≥ 1.282 attainable and
  weakens w̄'s absorption of the trend; the higher CAL (2.00) gives back a little.
- **`decayed`:** a dead rule ends `decayed` with probability **0.23 / 0.15 / 0.17**, and **0.21 / 0.18** under a +40% /
  −40%/yr forward drift (h 60); at SR 0.5, 0.013–0.038. Its size against as-published is **0.004–0.044** under every
  drift (below). Review A estimated 0.24 at h 60 with C2 ≈ −0.72 (drift 0 only); the drift worlds set C2 lower here.
  Most dead books still end `not_demonstrated`, which ends any allocation all the same (§14).
- **The cost of (c)'s own 10% test** (review A B2): at SR 1 the t-only rate 0.90 / 0.75 / 0.66 becomes `alive`
  0.85 / 0.67 / 0.53 (−0.05 / −0.08 / −0.12); at SR 0.75, −0.07 / −0.09 / −0.11 (Revision 1: −0.18 to −0.29).
- **Why H1 power exceeds the scout's arithmetic** (0.49 at SR 1 over 33 months): the timing statistic is estimated on
  daily data and carries none of the market's variance.

**"As published" (C2).** Table-4 pilot (40 synthetic decades per κ ∈ {0, 0.03, 0.06, 0.09, 0.12}; every coin shares
the +75%/yr drift; GJR; t(3)): the α/vol ratio against BTC is 0.40–0.45 at κ = 0 — **under this calibration's GJR t(3)
assumption, about a third of Table 4's 1.2 is the leverage-effect artefact alone**; this is an assumption, not a
finding (review A R1-m1): it holds only if crypto has an equity-style leverage effect, and under symmetric GARCH t(4)
the artefact is ≈ 12% (0.15 at κ = 0; review A `p8_log.txt`), the h-60 κ that reproduces Table 4 rises to ≈ 0.10, and
a C2 calibrated there would be higher (review A: ≈ −0.45 at G = 33). The GJR mapping is kept because it is the
conservative one for a kill switch (`decayed` fires less). It reaches 1.2 at κ = 0.090 / 0.065 / 0.061 (h 20 / 60 /
120). The simulated decade's β is 0.16–0.17 and its vol 12.5% (Table 4: 0.08 and 9%): only the scale-free ratio is
matched. Under the GJR assumption the paper's own timing strength, net of the artefact, is modest (a timing Sharpe of
roughly 0.25–0.4 with no drift).

| as-published world (h; forward drift 0 / +40% / −40%) | mean t_A | q05 of t_A (± MC SE) | `decayed` rate at C2 |
|---|---|---|---|
| h 20 | +1.78 / +1.53 / +1.99 | −0.00 ± 0.05 / −0.21 ± 0.03 / +0.12 ± 0.05 | 0.007 / 0.010 / 0.004 |
| h 60 | +1.27 / +1.09 / +1.32 | −0.57 ± 0.05 / −0.68 ± 0.05 / −0.54 ± 0.05 | 0.027 / 0.029 / 0.024 |
| **h 120** | +1.10 / +0.96 / +1.21 | −0.81 ± 0.05 / **−0.86 ± 0.04** / −0.62 ± 0.04 | 0.040 / 0.044 / 0.029 |

- **C2 = −0.94**: the smallest q05 − 2 SE is the h-120, +40%-drift world's −0.857 − 2 × 0.041 = −0.939, rounded down.
  Revision 1 at G = 33 had −1.26. **Review A's estimate was ≈ −0.72**: from drift-0 worlds only; the +40% forward drift
  and the 2-SE margin set it lower here.

**False-pass rates of `alive`** in the 19 calibration worlds at CAL_H1 = 2.00: **0.000–0.029** (largest: vol timing
0.029, the t(3) GJR cell world 0.020, asymmetric vol timing 0.018, the aligned six-month regime 0.015; the six GJR
bull worlds 0.001–0.004, where Revision 0's rule gave 0.05–0.12); the trend-free trend worlds (κ at SR 0) 0.008–0.017.
Forward drift ±40% at a dead and a half-strength book (h 60): dead `alive` 0.008 / 0.017, `decayed` 0.207 / 0.177;
half strength `alive` 0.165 / 0.202, `decayed` 0.013 / 0.022.

**Hindsight (review A B2).** The designer-shaped index path in (a), (c) trend-free: `alive` **0.006** (t-only 0.016).
Trend in the warm-up and (a) (ŜR ≈ 1.5 / ≈ 2.5 there), none in (c): `alive` **0.017 / 0.022**, against 0.24 / 0.70 for
the pooled t alone and 0.13 / 0.23 with Revision 0's sign condition (mean t_A on (a) 1.88 / 3.55, on (c) −0.87 /
−0.84). Quarterly regimes switching mid-cell (a trend world, reported): `alive` 0.018.

## 7. Span (a): in the primary, and (c) must stand on its own (D18; review A B2)

- **Why pool.** (a) adds 15 of 45 month clusters. A (c)-only test at its own calibrated value (max(t_29(0.95), the
  largest q95 + 2 SE of t_A,c) = 1.79) has power 0.76 / 0.59 / 0.48 at SR 1, against **0.85 / 0.67 / 0.53** for the
  pooled ladder: pooling buys 0.05–0.09.
- **Why (c) must pass alone.** The drafter knows the shape of (a): a rally, a cascade, a decline. The risk is not that
  the known index path manufactures timing return (it does not: 0.006 above) but that the rule was chosen *now
  because* (a) looked like a good trend year. With trend in (a) and none in (c), Revision 0's sign condition passed
  0.20–0.24 (review A). Requiring **t_A,c ≥ 1.282** caps that at **0.017–0.022**: `000t`'s "never its sole decider"
  holds by construction.
- **The decay step pools (a) and (c)** with no condition: (a)'s known decline is a drift, and the timing statistic does
  not carry drift (C2 is calibrated under ±40%/yr forward drift).

## 8. Costs and execution

| item | primary | deployable (reported, and the economic bar) |
|---|---|---|
| prices | Binance perp 1d closes | same |
| fee | 10 bp × |Δ notional| (the paper's) | 6 bp (Binance taker 5 bp; 4.5 with BNB; + 1 bp slippage) |
| funding | none (the paper's) | every Binance settlement, on the notional held into the day |
| execution | at the close that sets the signal (the paper's r_t = w_{t−1} ret_t) | the same; next-open is seconds later; a one-close lag is reported |

**Why perp closes in the primary, not spot (D11).** (1) One venue gives the universe, the delisting cut, the prices
and the funding consistently; the paper's CoinMarketCap aggregates are not reproducible point in time. (2) The
deployable instrument is the perp; a spot pass would leave a basis question open. (3) Daily close-to-close perp and
spot returns differ by the day's change in basis (expected a few bp; not measured here). (4) **Funding is left out
of the primary** so that the decay step compares the paper's object (no funding) with the paper's object; funding
enters the deployable variant and the economic bar (scout: the universe's unconditional funding is +5.4%/yr, −2.2%/yr in 2025-01 →
2026-06; × mean gross 0.08 ≈ 0.4%/yr at paper sizing, more if longs coincide with high funding — measured only
inside the read).

Turnover ≈ 4.4× capital a year (scout), so fees are ≈ 0.4%/yr at 10 bp and ≈ 0.26%/yr at 6 bp. 0, 25 and 50 bp are
reported.

## 9. Tail, sizing, Kelly, capacity, expected return

Arithmetic from the scout's stress episodes (Binance, 2020-03 → 2025-10; unconditional gross p90/p99, never the gross
held on the eve of an episode), re-derived here:

| sizing | gross p90 / p99 | 2020-03-12 day (EW −42.7%) | 2021-05-19 (−34.0%) | 2025-10-10 (−23.0%) | worst plausible week | worst plausible month |
|---|---|---|---|---|---|---|
| **paper (≈ 9% vol)** | 0.16 / 0.27 | −6.8% / −11.5% | −5.4% / −9.2% | −3.7% / −6.2% | **≈ −12%** | **≈ −15%** |
| **gross × 2.8 (≈ 25% vol on average)** | 0.45 / 0.76 | −19% / −32% | −15% / −26% | −10% / −17% | **−30 to −40%** | **−30 to −45%** |

- A daily-close stop cannot act inside a one-day gap, so the worst week is mostly the first day × the gross held.
  The month adds whipsaw re-entries.
- **The p99 column is the central case, not the extreme** (review A m8). A crash after a calm rally finds the book
  at its *most* invested: in synthetic worlds the gross held into a −35% day that follows a calm rally has p50 2.5×
  and p90 4× the unconditional mean, and the book loses a median 18% of equity that day (p10 −28%) at 20% book vol.
  With the scout's mean gross 0.08 and max 0.31, a 2025-10-10-type day after a calm rally sits at the p99 gross. So
  the worst plausible week at ≈ 25% vol is written **−30 to −40%**, and the synthetic MDD p05 in a two-crash world is
  −40% at 20% vol (the Revision-0 "11% → 31%" scaling of the paper's in-sample MDD understated it).
- **k = 2.8 is a gross multiplier, not a vol target** (review A m9): realised vol at k = 2.8 moves with the gross
  (mean 0.08, p99 0.27), so "25% book vol" is an average.
- **Margin:** cross margin, or ≤ 2× isolated per coin (a 2021-05-19 day liquidates 3× isolated); at k = 2.8 the
  p99 gross is 0.76 of equity.
- **Kelly.** If alive at half strength (SR 0.78), full Kelly runs at ≈ 78% vol, so 25% is ≈ 0.32 Kelly. At the
  programme's odds (1 in 3 alive) the Sharpe is a two-point prior (0.78 w.p. ⅓, −0.15 w.p. ⅔): E[SR] ≈ 0.16 and
  Var(SR) ≈ 0.19, so the Bayes-Kelly vol is E[SR]/(1 + Var) ≈ **13.5%** (review A m7; not 16%). The paper's ≈ 9% is
  ≈ 0.67 of it. Start there; scale only after `alive`, and never beyond min(Bayes-Kelly, 2.8 × the paper's sizing).
- **Capacity.** At $1 M, k = 2.8, the largest single-coin notional is ≈ $120 k, ≈ 0.17% of the rank-20 coin's daily
  volume: non-binding to tens of $M.

**Expected return over cash (idle collateral earns ≈ 3% on top in every state):**

| state | paper sizing (≈ 9% vol) | 25% vol (k = 2.8) |
|---|---|---|
| alive at full strength (SR 1.57) | 14.1% − 0.4% funding ≈ **+13.7%/yr** | 39% − 1.2 to 3% ≈ **+36 to 38%/yr** |
| alive at half strength (SR 0.78) | 7.0% − 0.4% ≈ **+6.6%/yr** | 19.5% − 1.2 to 3% ≈ **+16 to 18%/yr** |
| dead (costs and funding only) | ≈ −1 to −2%/yr | ≈ −4 to −6%/yr |
| expectation at 1-in-3 odds | ≈ +1.2%/yr | ≈ +2.3%/yr, with a −30% tail |

**Against the goal.** Only the "alive at full strength, 25% vol" cell reaches the mandate's 30–100% landing zone, with
a −30 to −40% plausible month. Half strength at 25% vol is a 16–18%/yr book with HLP-beating return and a far worse
risk-adjusted profile than HLP. Odds (scout, unchanged): exists 1 in 3; beats HLP on return 1 in 5; risk-adjusted
1 in 15.

## 10. Reported, decides nothing

Strata (a) and (c): t_A, the factor α and the Sharpe of each. The factor α with constant, half-year and year slopes;
the BTC-only constant-β α (the paper's own α object); the raw Sharpe with the noncentral-t quantile at the paper's
1.57 (it carries the forward market drift, review A M1). The deployable variant (timing leg, α, Sharpe, mean) and
its funding drag; costs 0 / 25 / 50 bp; the relative threshold; a one-close lag; long/short (paper Appendix); the
HL-tradable sub-book; tails at paper sizing and at gross × 0.25/realised vol (worst day, 7-day, 30-day, MDD); gross
exposure; turnover; the members excluded by the perp-age rule alone, by month; a **placebo**: every symbol's
Donchian states circularly shifted by an offset drawn from [400, n − 400] traded days (review A m6: never a near-lag),
200 seeds, the share with t_A at or above the actual and the critical value; and the counts before the decision.
The members by month are stored. No reported block may become a primary.

## 11. Guard, integrity, the fetcher, procedure

**Files.** `ct16/rule.py` (the rule, universe, book, factors, the per-day weights; pure), `ct16/stats.py` (the timing
statistic, the ladder, the bar, the recheck; the reported estimators; pure), `ct16/data.py` (archive parsers on
bytes), `ct16/sim.py` (synthetic worlds; reads nothing), `r016_run.py` (guard, gated panel, the read, `--power`,
`--fingerprint`), `r016_fetch.py` (the guarded fetch), `tests/test_r016.py`, `tests/test_r016_mutants.py`.

**Commands.** `python3 -I -S research/alpha/r016_fetch.py`, then `python3 -I -S research/alpha/r016_run.py --run`,
with python3 the pinned interpreter (CPython 3.14.8, binary SHA-256 `7ebde552…4063`, stdlib fingerprint
`c99bbd75…071e`; the pins are 015's, measured by the same function). If the interpreter changes before 2028 (a
`brew upgrade`), the recovery is to install CPython 3.14.8 until `--fingerprint` equals the pins; **the pins are never
edited after REGISTERED** (review B m7).

**The guard** (`r016_run.guard_failures`; review B's fixes, ported from 015 Revision 1):
- (s) status, hash table, approval / restart / repo-path lines read from **HEAD:./**, never the working tree;
  (a) tracked; (b) working-tree bytes (`hash-object --no-filters`) equal the HEAD blob; (c) committed (M3);
- **(r) reg0** (M4): exactly one REGISTERED content of the registration across `--all --reflog` (no first-parent
  restriction: a `--no-ff` merge cannot re-register), approval, restart and repo-path lines removed; reg0 = the
  earliest such commit by ancestry; HEAD's registration and every guarded code blob equal reg0's; both fetch rows
  carry reg0. A revision committed at any time in the 27 months before the read refuses, even if later reset;
- (d) guarded code bytes equal HEAD; no assume-unchanged / skip-worktree flag or filter attribute on the
  registration, the code or `results.jsonl`; no untracked, ignored, modified or **stdlib-named** importable file at
  research/alpha's top level or in ct16/ (research/alpha is *appended* to `sys.path`) — remedy named in the refusal:
  commit every top-level `.py` and package, of any study, before the fetch and before the read (m10);
- (e) the table; **(t)** the wall clock ≥ 2029-01-08 UTC, and a `now` argument is refused whenever the real
  repository is involved (M1); (k) `results.jsonl` clean and ending with a newline (m3);
- **(w)** (B1): this file's repository is the pinned `REAL_REPO`
  (`/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline`), its main worktree (`--absolute-git-dir` =
  `--git-common-dir` = `REAL_REPO/.git`), a named branch (no detached HEAD), the anchor `9c131e2` an ancestor. If the
  repository is moved, a committed line `016 repo-path: APPROVED <date> <abs path>` re-pins it without a code edit —
  accepted **only when the pinned path no longer exists**, so a clone beside the real repository can never use it;
- **(v)** (M2): `python3 -I -S` (isolated, no site; `sys.orig_argv` carries both), no `.pth` in site-packages, no
  `-O`, the interpreter version, binary hash and stdlib fingerprint equal the pins, every loaded module comes from the
  pinned stdlib or a hashed 016 file, and every cached bytecode from the fresh `sys.pycache_prefix` (set before any
  016 import, in the fetcher too, so the fetcher can no longer load the guard from `__pycache__`);
- (f)/(i): rows found in the working tree and in `git log --all --reflog -m --root -p --text --no-textconv
  --no-ext-diff` (log.showRoot, no replace objects); an unparseable added line that mentions a 016 registration
  refuses (m3); keyed on the `016-` prefix;
- read: (g1)–(g3), (h') as Revision 0; **restart** only when no commit anywhere that adds a `read_started`, `read` or
  `read_failed` line was made by the runner identity (`r016_run`): the data open only after the runner's own started
  commit is in HEAD of the real ledger, so "no runner commit" is the verifiable fact that no data were opened (M4).
- git runs from `/usr/bin/git` with `GIT_*` stripped, `GIT_CONFIG_NOSYSTEM=1`, `GIT_CONFIG_GLOBAL=/dev/null`,
  `GIT_NO_REPLACE_OBJECTS=1`, `--no-replace-objects`, `core.hooksPath=/dev/null`, `core.fsmonitor=false`; every
  `<rev>:<path>` is `<rev>:./<path>`.

**Gate tokens and the real tree.** `guard()` no longer opens anything: `run()` commits `read_started` (as the runner,
alone), verifies the line in HEAD of the real ledger, and only then issues the data gate. `Panel` refuses the real
directories, **or any byte copy of a registered real data file** (by hash; m2), without that gate, and re-hashes the
`SHA256SUMS` and exchangeInfo bytes it parses against the HEAD table (m8). `run()` takes no argument. The real
results file and the real data are used only together, only with the default commit hook, only with the real
`guard` **and** the real `guard_failures` (both captured at import; m2). `append_row` refuses to glue a row onto a
line without a newline (m3). The runner never pushes. A failed automatic commit leaves the row — for the read row,
the decision — in plain text in the working tree: commit it by hand, as it is (m11).

**Bytecode (Revision 2, review B R1-M1).** Any `-X` option is refused (`sys._xoptions` or `-X` in `sys.orig_argv`):
a command-line `-X pycache_prefix=<dir>` used to bring back forged decision bytecode under `-I -S`. A pre-existing
prefix is kept only when the running program is `r016_fetch.py` and the prefix is the one it just created itself
(`_R016_PREFIX`); otherwise the runner creates a fresh one, and (v) refuses if `sys.pycache_prefix` is not the prefix
this program created.

**Accepted residuals: each needs deliberate tampering, as in 015** (review B r1-m2, r1-m3, r1-m4).
- **A symlink at the pinned path, or a self-issued repo-path line** (r1-m2): both need the real repository moved away
  (`mv`) and replaced by a link or a clone; `resolve()` follows the link, and a repo-path line is accepted only when
  the pinned path no longer exists. Not closed in code; it is an act on the owner's file system, visible in history.
- **Swapping module globals in-process** (r1-m3, r1-m4): replacing the fetcher's `REAL_HOOKS` or
  `_real_witness_hooks`, or assigning both `_GATE` and `_GUARD_PASSED`, after the guard and inside the running
  process. `guard`, `_GUARD_FAILURES` and `HOOKS['commit']` are identity-checked; the rest needs a harness that
  imports the runner and patches it, which is not the registered command.
- **Machine-level tampering**: the stdlib's own `__pycache__` (the fingerprint hashes `.py` and `.so`), the system
  clock (the witness's server date and the archive's 2028-12 key still gate the fetch), site-packages (unreachable
  under `-S`).

**The fetcher** (`r016_fetch.py`):
- `fetch()` takes no argument and runs only from the real repository; `_fetch_in()` (tests only) refuses when the
  tree, the store or the ledger lies in the real repository (M1), before anything that could write;
- the guard (clock gate) first, then the **witness** (m1): the archive must list BTCUSDT 2028-12 and the listing's
  HTTP `Date` must be ≥ 2029-01-08 — before any row is committed or any file written, so an early run (a forged
  clock) leaves nothing behind; attempts committed before 2029-01-08 never count toward persistence;
- pulls Binance klines 2026-07 → 2028-12 and funding 2025-03 → 2028-12 (every USDT symbol), the exchangeInfo, the HL
  probe; later months counted, never fetched; months ≤ 2026-06 never re-fetched;
- every zip verified against `.CHECKSUM` and parsed before an atomic write; **on resume every stored file is
  re-checked against `.CHECKSUM`** and re-fetched if it differs (m5d); never cached: an error, an empty answer, a
  mismatch;
- coverage, keyed (m5): BTCUSDT every forward day (never waivable); the 2026-06/07 and **2028-11/12 seams**; contiguous
  forward months; **a funding file for every (symbol, month) with a traded kline row**; **every live COIN perp of the
  2029 exchangeInfo onboarded before 2028-12 has forward klines** (a truncated listing cannot drop a symbol silently);
- the same key on 3 committed attempts ≥ 24 h apart (all after the gate) becomes unavailable, ≤ 10 names; BTCUSDT
  keys never;
- S3 pages forward (exclusive `marker`); HL windows [a, b − 1 ms] (inclusive `endTime`).

**Tests.** `tests/test_r016.py`: **208** checks, all synthetic, with **every socket connection disabled** (m4). Both
layouts (runner at the top level and in `research/alpha/`) for the guard and the end-to-end fetch → approval → read.
A **sandbox** stands in for the real repository (a fresh git repository with 016's code, REAL_REPO and ANCHOR
rewritten to it, synthetic data; never a clone of the real repository, never real data): there the registered
command refuses only on (t) and (g1), and a clone with its data copied, the fetcher run in that clone, a linked
worktree and a detached HEAD each refuse with (w). `tests/test_r016_mutants.py`: **27 of 27 mutants killed** (8 of 8 unmutated baselines pass; Revision 2 adds the `-X` check) — each of the
protections review B listed (and the new ones) is deleted in its own sandbox, and the targeted test fails on the named
check. **No test goes stale**: the real-file checks accept DRAFT or REGISTERED, the date checks are conditional on
today, and "no approval line" is required only while no `fetched` row exists (m9). Commit-state simulation (`scratchpad/scripts/r016_commit_sim.sh`: a `git clone --local` with its own `.git` and **no data**, real or copied): 205 of 205 pass at DRAFT and at REGISTERED (in a clone the three real-tree M1 checks become one check that the clone is refused with (w)), and the real guard in the REGISTERED clone refuses with **(w)** (four lines) and (t) — the Revision-0 simulation's "only (t) and (g1)" is now impossible.

**Procedure.**
1. REGISTERED (the orchestrator), committed with the code **in the main worktree of the real repository** and
   **tagged `r016-registered`** (an annotated tag on reg0; review B r1-m1: a REGISTERED commit made in a linked
   worktree and squashed away could otherwise leave no REGISTERED content in `--all --reflog`; the tag keeps reg0
   reachable for the 27 months, and anyone can check `git rev-parse r016-registered` against the guard's reg0). Every
   `.py` at the top of `research/alpha` and in `ct16/` committed (015's and any later study's included). The
   orchestrator writes reg0 into the programme document.
2. From 2029-01-08, in the real repository, on a named branch: `python3 -I -S research/alpha/r016_fetch.py`,
   resumable until its `fetched` row.
3. The orchestrator reads the fetch report (counts only), then commits `016 read: APPROVED <date> row <sha256>`.
4. `python3 -I -S research/alpha/r016_run.py --run`, once. The result is written into this file in its own terms.
5. **Clean-up rule for an early run** (review B): an early fetch is refused by the clock gate before anything is
   written; if one ever leaves a `fetch_started` or attempt row (a forged clock and a forged server date), the rows
   stay, they are ledgered, and the fetch resumes in 2029 (their attempts never count toward persistence, and every
   stored file is re-checked). Nothing is deleted from `results.jsonl`, ever.

## 12. Known weaknesses, stated now

- **Designer knowledge of (a)** (§0, §7): mitigated by the verbatim rule, the timing statistic, and (c)'s own 10% test.
- **`000l` and 008 inside (c):** 2026-07 → 09 were looked at as cross-sectional momentum ICs and will be as HL
  inverted positioning. Neither is this book's statistic; both are outcome-adjacent.
- **"As published" depends on the Table-4 mapping** (§6): the synthetic decade matches the α/vol ratio, not β or vol.
  If the paper's α owes less to the leverage artefact than the GJR t(3) sim assumes (≈ ⅓; ≈ 12% under symmetric
  GARCH, review A R1-m1), C2 is too low and `decayed` too rare
  (conservative for a kill switch only in the sense that it rarely fires; the ladder then ends in `not_demonstrated`,
  which ends any allocation too).
- **The timing statistic counts vol timing as timing** (inverse-vol sizing moves weights with predictable vol); the
  calibration's vol-timing world prices it in CAL_H1.
- **Power at half strength stays modest** (§6), and (c)'s own test costs power; that is the price of not letting the
  designer-known year decide.
- **Binance perps, not the paper's spot aggregates;** perp age, not asset age; Binance perp volume.
- **Funding is modelled on the notional held into the day** at the previous close, not at the settlement mark.
- **Not modelled:** jumps correlated with the book's exposure beyond the crash worlds; exchange failure.
- **Archive risk:** if Binance changes the archive before 2028, the fetch refuses and an amendment (a new
  registration) is needed. **Interpreter risk:** see §11 (the pins are never edited).

## 13. Decision table (every row PROPOSED)

| # | item | PROPOSED | reason |
|---|---|---|---|
| D1 | the rule | §2, verbatim | the trial is the paper's rule |
| D2 | signal details | as Revision 0 (closes, today included; ties enter; exit on ≤ stop; stop for t+1 set at close t; fn 3; the paper's case order; flat until n closes) | p. 7, p. 10, fn 3 |
| D3 | σ | sample SD (n − 1) of 90 simple returns × √365; no σ → no position | p. 8–9; crypto trades 365 days |
| D4 | rows | missing → carried; zero-trade or zero-price → dead | Binance archive behaviour (L4) |
| D5 | threshold | per sub-model, absolute 0.20; relative reported | p. 14–15 |
| D6 | funding (deployable only) | (d 00:00, d+1 00:00] attribution; on the notional held into the day | Binance mechanics |
| D7 | costs | 10 bp × \|Δ notional\| per coin; deployable 6 bp + funding; 0/25/50 bp reported | p. 15 |
| D8 | universe | Binance USDT perps; **perp age** (deviation stated, decided before any read); trailing-30-day median for filter and rank; exclusions: the 2026 exchangeInfo, the 2028 one only for symbols the 2026 one lacks, name lists | §3, §7.1; review A m3–m5, review B m6 |
| D9 | delisting cut | traded on each of the last 7 days; dead row → settle, sleeve to cash; §7.2 exits at every close | the LUNA case |
| D10 | book | initialised at the close of 2025-03-19; month-end reset; continuing members keep held weights | §7.2 |
| D11 | instrument | perp closes; no funding in the primary | §8 |
| D12 | new members | enter at their current Combo target | the signal is a property of the price path |
| D13 | direction | long-only; long/short reported | p. 3 |
| **D14** | **statistic** | **the timing return t_A (Lo 2008), CR1 by month, G = 45**; the factor α (constant, half-year, year slopes) reported | review A B1: the factor α fails in leverage-effect bulls; t_A has mean ≈ −cost under any constant drift |
| **D15** | **`alive`** | t_A ≥ CAL_H1 = 2.00 and t_A,c ≥ 1.282 | §4.2, §5; review A B1, B2 |
| **D16** | **`decayed`** | t_A < C2 = −0.94 (Table-4 worlds × forward drift 0 / ±40%) | §4.2, §6; review A M1 |
| D17 | calendars | (a) 2025-03-20 → 2026-06-30; (c) 2026-07-01 → **2028-12-31**; one read on or after **2029-01-08** | §1; review A R1-M1 |
| **D18** | span (a) | pooled, with (c) required at 10% on its own | §7; review A B2 |
| **D19** | calibration | 19 worlds × 3,000 draws (incl. GJR bulls +40/+80/+120%, ±crash days, asymmetric vol timing, GJR bear); CAL = max(q95 + 2 SE), rounded up; C2 = min(q05 − 2 SE) over 9 as-published worlds, rounded down | review A B1, M1 |
| **D20** | outcomes and bar | the three-step ladder; the bar at the Bayes-Kelly size of the shrunk timing Sharpe vs 4% | review A M1, M3 |
| D21 | fetch | §11; witness first; funding coverage, second seam, listing cross-check, resume re-verify | review B m1, m5 |
| **D22** | guard | §11; REAL_REPO, reg0, HEAD reads, -I -S pins, no mocks or `now` near the real tree, restart only without a runner read row | review B B1, M1–M4 |
| D23 | reported | §10 | — |
| D24 | family | crypto time-series trend: trial 1 | new family |
| D25 | owner option | §14 | review A M2 |

## 14. Owner option (not part of the trial)

**PROPOSED:** the owner **may** paper-trade or live-trade the verbatim rule at the paper's sizing (≈ 9% book vol,
≈ 0.67 of the Bayes-Kelly size at the programme's odds) during the forward span, on Binance or HL, from any date.
- **It does not change the test.** The read uses Binance archive closes, not the owner's fills; the rule, the
  statistic, the critical values and the end date are fixed here and bound to reg0 by the guard.
- **The allocation ends at the read unless the read says `alive`** (review A M2): `decayed` and
  `not_demonstrated` both end it. `not_demonstrated` carries no evidence for the rule, and a dead book ends there
  about half the time.
- **What it does change:** the owner will see interim span-(c) P&L. That is an informal look at the forward span; it
  may not be used to amend, stop early or re-time 016 (any registration that uses it ledgers it).
- A live drawdown beyond the plausible month at the chosen size (−15% at 9% vol) is a reason to stop trading, never a
  reason to touch the test.

## 15. Disclosures and real-data reads (for the ledger)

**Revision 0 (drafter):** the paper's text (PDFKit into the scratchpad); the first row of `BTCUSDT-1d-2020-01.zip`
(2020-01-01, in the paper's sample) and the header lines of one 2022-01 kline and one 2022-01 funding file, printed
while checking the CSV layout; the SHA-256 of the scout's kline `SHA256SUMS` and the paper's `SHA256SUMS`; the
listing's line count and last month; the 2026 exchangeInfo's field names and type counts.

**Revision 1 (drafter):** the two reviews and their probe summaries (`scratch_review016a/p1c_lo_timing.py`,
`p1c_summary.txt` and the review text's tables; synthetic only); `r015_run.py` (code, read-only, to port its guard).
**No real data value was read.** The commit-state simulation copies no real data (Revision 0 copied the pull's
`SHA256SUMS` and the exchangeInfo — metadata — into its clone; review B noted that as the B1 pattern; it no longer
does). Synthetic only: the Table-4 pilot (scratchpad), the calibration and power run (`data/r016/logs/`; Revision
0's logs kept in `data/r016/logs_rev0/`).

**Revision 2 (drafter):** the two recheck appendices and their tables; `000u`. Synthetic only: the re-run
calibration and power at the 2028-12-31 end (`data/r016/logs/`; Revision 1's kept in `data/r016/logs_rev1/`).

**`000u` (ledgered by the orchestrator):** during review B's recheck, one probe without the network sandbox sent **one
real listing request** to Binance's archive for BTCUSDT's monthly 1d kline file names. The answer carried file names
for months in span (c) (2026-07 →) and the server date: **existence of files, no values**; nothing was downloaded,
parsed beyond key names, or written. Span (c) stays unread as data.

**015 has run** (`results.jsonl` lines 1–3: its started, result and reported rows, committed; outcome
`not_demonstrated`). They are another registration's rows in the shared ledger: 016's scan matches only the `016-`
prefix, does not block on them, and keeps refusing on an unparseable line that mentions 016 (tested: a committed
015 row leaves the fetch gate open; review B's recheck confirmed the same against the real ledger, read-only).

**Designer knowledge** (§0) is disclosed, not a read.

## 16. SHA-256 of the code and data the read depends on

| file | sha256 |
|---|---|
| `ct16/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct16/rule.py` | `5138cc0e34954eda46fe2800bc36df963e57010f73fed31780b9051acc0a5552` |
| `ct16/stats.py` | `0f25cd45a1a314c7f186857196d2ffa8572ac564c88eac35338185f315ccd4c2` |
| `ct16/data.py` | `bffd4a78bec08fcdf3d5cc258f3eaa7bfeefea031f8ae64727ed02ff27b224e7` |
| `ct16/sim.py` | `e626e49b30fa863dbe2382111ff0def0c6f5cf39aee940e4a6aee1822579e6bc` |
| `r016_run.py` | `718bf42f52aa8f4128986bd41795db6369ff4ba46a8abb659caf53b3eed9b70c` |
| `r016_fetch.py` | `dfb1f188aa615e5b4678b11df62f628e4b292e2eabc37b2f15d3f9870538f33b` |
| `data/scout_c1/bn_um_klines_1d_monthly/SHA256SUMS` | `ff50da311181f092581bf61b953fe51750872d33cc9e8a7c7021ffe2a7f91b31` |
| `data/r016/static/fapi_exchangeInfo_20261011.json` | `a7f720f5ece63fe9ab087cbdd9fecc66d18645f113896b81ec171c89636ed39f` |

## R1. Revision 1: every finding and its fix (2026-10-13)

File keys: R = `r016_run.py`, F = `r016_fetch.py`, S = `ct16/stats.py`, U = `ct16/rule.py`, M = `ct16/sim.py`,
T = `tests/test_r016.py`, X = `tests/test_r016_mutants.py`.

| finding | fix | where |
|---|---|---|
| **A-B1** constant-slope α manufactures α in leverage-effect bulls; CAL under-sized | primary → the timing return t_A (Lo 2008) as the reviewer specified; recalibrated on 19 worlds incl. the reviewer's GJR bulls (+40/+80/+120%, ±−35% days, t3/t4), asymmetric vol timing and a GJR bear, 3,000 draws each, MC SE reported, CAL = max(q95 + 2 SE) rounded up = 1.75; the factor α t reported | S `timing_series`, `mean_t`, `timing`, `h1`; U `run_book` (per-day weights, cost); M `CALIBRATION`, `quantile_se`; §4.1, §5 |
| **A-B2** (a) can carry the pass alone | `alive` also needs t_A,c ≥ 1.282 on (c) alone with its own w̄; cost stated | S `h1`, `T_FWD`; §7, §6 |
| **A-M1** H2 on raw Sharpe measures the forward drift | decay on t_A, C2 calibrated to Table 4 (κ_pub by α/vol = 1.2 on synthetic decades) × forward drift 0/±40%; size ≤ 0.05 there; the ladder alive / decayed / not_demonstrated; raw Sharpe reported | S `ladder`, `C2`, `raw_sharpe_vs_paper`; M `table4_draw`, `KAPPA_PUB`, `table4_worlds`; §4.2, §6 |
| **A-M2** a dead book keeps trading after `undecided` | the owner allocation ends at the read unless `alive` | §14 |
| **A-M3** the bar clears almost always | return at the Bayes-Kelly vol of the shrunk timing Sharpe vs 4%; stated as a sanity floor | S `economic`; §4.3 |
| A-m1 "true SR" axis | labelled mean per-draw ŜR, pooled SR beside it | M `tables`; §6 |
| A-m2 the paper's Sharpe convention | moot: decay no longer on raw Sharpe; noted (Table 4's raw mean/vol ≈ 1.8–2.0 vs the published 1.57) | §6 |
| A-m3 perp age binds in (a) | §3 corrected; kept and stated; age-only exclusions reported | §3, U `age_excluded`, R `compute` |
| A-m4 ranking window | recorded as a decision | §3, D8 |
| A-m5 spot-volume overlap | **not added**: a second archive pull, fetch path and tests for a number that decides nothing; the tilt is stated | §3 |
| A-m6 placebo offsets near a lag | offsets in [400, n − 400] | R `placebo_syms` |
| A-m7 Bayes-Kelly ignores parameter variance | 13.5%, the paper's 9% ≈ 0.67 BK | §9, §14 |
| A-m8 the tail's p99 is the central case | worst plausible week at ≈ 25% vol −30 to −40%; synthetic MDD p05 −40% | §9 |
| A-m9 k = 2.8 is a multiplier | stated | §9 |
| A-m10 H2's crit depends on h | superseded by M1 | — |
| **B-B1** the guard binds to any clone or worktree | REAL_REPO pinned; main worktree; named branch; repo-path recovery line only when REAL_REPO no longer exists; tested in a sandbox (clone with data, fetcher in a clone, linked worktree, detached HEAD) | R `REAL_REPO`, `binding_failures`, `bound_repo`, `guard`; F `fetch`; T `binding_checks` |
| **B-M1** mocks and `now` on the real repository | `fetch()` takes no argument; `_fetch_in` refuses any real tree, store or ledger; `now` refused on the real repository; one containment test | F `_fetch_in`; R `guard_failures`, `in_real`; T `gate_checks` |
| **B-M2** isolation | -I -S required (flags and `orig_argv`), no .pth, interpreter binary + stdlib pins, module and bytecode origins, path appended, fresh prefix before any 016 import (fetcher included), git by absolute path with neutral config | R (v), F header; T `env_checks` |
| **B-M3** index flags hide edits | registration from HEAD:./, `hash-object --no-filters` vs HEAD blobs, flags and filter attributes refused | R `head_text`, `same_as_head`, `flag_failures`; T `history_checks` |
| **B-M4** nothing binds the read to the REGISTERED commit | reg0 across `--all --reflog`; one REGISTERED content; HEAD code = reg0; rows carry reg0; data gate after the started row; restart only without a runner read row | R `registered_versions`, `reg0_failures`, `runner_added_lines`, `run`; T `history_checks`, `restart_checks` |
| B-m1 an early run leaves rows | clock gate first, then the witness (2027-12 key, server Date) before any write; pre-gate attempts never count; clean-up rule | F `witness_failures`, `apply_persistence`; §11 |
| B-m2 path gate on a mutable global | copies refused by hash; `guard_failures` captured | R `Panel`, `_guard_ok`, `real_tree_failures` |
| B-m3 newline, showRoot, replace | newline refusals; `--root`, showRoot, no replace objects; unparseable 016 lines refuse | R `append_row`, `result_rows`, `_git` |
| B-m4 vacuous tests | one targeted test per protection; the mutation harness shows each failing when its protection is removed; socket guard | T, X |
| B-m5 fetch coverage | funding coverage, second seam, listing cross-check, resume re-verify | F `coverage`, `pull` |
| B-m6 universe metadata | perp age decided; 2028 snapshot only for symbols absent from 2026; serverTime stated | §3; R `Panel.load` |
| B-m7 environment over 15 months | pins and recovery stated; git version recorded | R `env_fingerprint`; §11 |
| B-m8 hash the bytes used | `Panel.load` re-hashes SHA256SUMS and exchangeInfo against the HEAD table | R `Panel.load` |
| B-m9 a check stale at approval | conditional on no fetched row | T `real_file_checks` |
| B-m10 sequencing | the (d) refusal names the remedy; procedure step 1 | R; §11 |
| B-m11 nits | END + 8 days; `guard_failures` captured; the failed-commit note | §1, §11 |
| Orch.: no real data in clones | the commit simulation and every sandbox use synthetic trees only | §11, §15 |
| Orch.: the power run was killed mid-way | the run is resumable per world (keyed by spec, draws, seeds); a `set`-order float nondeterminism found by the resume check fixed (sorted iteration) and the run redone; byte-identical re-run and a fresh world re-run verified | M `run_set`, `power_run`; U `run_book.build`; R `power`; T `sim_checks`; §6 |


## R2. Revision 2: the Revision 1 rechecks and their fixes (2026-10-13)

| finding | fix | where |
|---|---|---|
| **A R1-M1** the end date (accepted by the orchestrator) | span (c) 2026-07-01 → **2028-12-31**; G = 45; the fetch and read gate **2029-01-08**; fetch months 2026-07 → 2028-12 (klines) and 2025-03 → 2028-12 (funding); the second seam, the witness key and the listing cross-check move with the end; eight half-year cells (reported); CAL_H1 and C2 recalibrated at 3,000 draws per world on the 19 null worlds and the nine as-published worlds (0 / ±40%/yr), MC SE and the +2 SE rule; the power table re-run | U `SPAN_C`, `READ_NOT_BEFORE`, `CELLS`; R `FWD_MONTHS`, `FUNDING_MONTHS`; F `coverage`, `hl_presence`; M `CELL_SEQ`, the world-cache key now carries the calendar; S `CAL_H1`, `C2`; §1, §4–§7, §11 |
| **B R1-M1** `-X pycache_prefix` restores forged bytecode | any `-X` option refused (`sys._xoptions`, `orig_argv`); a pre-existing prefix kept only when the running program is `r016_fetch.py` and the prefix is its own `_R016_PREFIX`; (v) refuses a prefix this program did not create; test plus mutant (the test fails with the check removed) | R header, `environment_failures`; F `_R016_PREFIX`; T `env_checks`; X `B-R1-M1` |
| B r1-m1 reg0 lost through a worktree squash | procedure: REGISTERED committed in the main worktree and tagged `r016-registered` (no code change) | §11 procedure 1 |
| B r1-m2 symlink at the pinned path; self-issued repo-path line | stated as a residual needing deliberate tampering (the real repository moved away) | §11 |
| B r1-m3, r1-m4 in-process hook / gate swaps; machine-level tampering | stated as residuals, as in 015 | §11 |
| A R1-m1 the leverage-artefact share | stated as conditional on the GJR t(3) calibration (≈ 12% under symmetric GARCH); kept as the conservative mapping | §6, §12 |
| A R1-m2 scale-to size | `paper_then_scale` scales toward min(Bayes-Kelly, 2.8 × the paper's sizing) | §4.2 |
| A (recheck) the bar clears on every `alive` | accepted as a sanity floor, as §4.3 says | §4.3 |
| Disclosures | `000u` (one real listing request returned span-(c) file names, no data); 015's committed rows in the shared ledger, handled and tested | §15; T `guard_checks` |
