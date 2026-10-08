# 001 — Slow skilled traders: can a follower capture them, and does their positioning beat the crowd's?

**Status: DRAFT — not registered.** It becomes the registration in the commit
that changes this line to `REGISTERED <date>` after the owner has approved
every item in §10. Until then no ranking score and no outcome is computed,
and nothing is launched even after (owner, 2026-10-08: prepare, don't launch).

Plan: `docs/copy-trade-plan-2026-10-07.md`. Prior ledger entry: `000` (P0
looked inside the holdout). Drafting inputs were behaviour-only:
`p4_features.py`, `p4_describe.py` (no returns, `realized_pnl` never read) and
BTC price history for regime labels (`ct/regimes.py`).

**Revision history (all before registration):**
1. First draft: book tests (consensus, top-K) as pass/fail — too little power.
2. Second: a cross-sectional copy test of all eligible wallets.
3. **This one (owner, 2026-10-08):** built around the two ideas with a
   mechanism. **A** — copy only *slow* traders: a position held for days is
   not front-run away in minutes, and few trades mean low costs — the two
   killers measured so far (crowding, costs). **B** — use the dataset nobody
   else has: full-population positions joined to point-in-time skill, and
   trade where skilled slow money disagrees with the crowd.

## 1. Trials registered here

| id | hypothesis | role |
|---|---|---|
| T1 | **H0-leader**: a leader's own skill persists (all eligible traders, crypto) | diagnostic — tells whether a failure of A/B is "no skill" or "skill not capturable" |
| T2 | **A — slow-trader copy**: among slow traders, the selection score predicts the follower's net hedged return on Bitget; the top quintile earns; the edge survives a 24-hour delay | primary |
| T3 | **B — skilled vs crowd**: the gap between skilled slow traders' positioning and everyone else's predicts coin returns, market-neutral, after costs | primary |

Three trials. Family-wise one-sided α = 0.05 / 3 = **0.0167** (z ≥ 2.13).
Crypto (main dex) only: `xyz` has 15–122 slow traders per window (too few)
and no Capital venue yet. Everything in §8 is reported and decides nothing.

## 2. Data and periods

- Discovery only: 2025-07-28 → 2026-06-30 (`p3_fills.py`, P3 gates passed).
  **Holdout (2026-07-01 →) is not read** — a later registration at P6. The
  holdout is mostly a rally (Aug +24%, Sep +8%), unlike most of discovery: a
  pass there is also a regime test.
- Windows (`ct/schedule.py`): lookback 91 d, hold 56 d, step 56 d; selections
  2025-10-27, 2025-12-22, 2026-02-16, 2026-04-13.
- Leader equity and positions: snapshot table, as-of the latest snapshot
  strictly before the moment needed (P2). No snapshots 2025-10-25 → 12-14:
  B has no signal on those days; A uses the last snapshot before.
- Fills: archive, file order = execution order (P3 G3).
- Prices: Bitget public API — 1H candles for B and the hedge, 1m candles for
  A's executions (200-minute blocks containing an order), cached.
- Regimes: BTC 1H since 2019-07 (`ct/regimes.py`).

## 3. Populations — behaviour only, on the lookback [S − 91 d, S)

**Eligible (T1):** ≥ 20 completed round trips; account value ≥ $10,000 and
≤ 7 days old as-of S; maker share ≤ 0.5; median hold ≥ 2 h; ≤ 500 fills per
active day; ≥ 80% of notional in Bitget-mapped coins; vaults / protocol
accounts excluded (address list fixed before any score is computed).

**Slow (T2, T3):** same, but **median hold ≥ 24 h** and **≥ 10** completed
round trips (slow traders trade less). Sizes per window before the age and
vault filters: 1,647 / 1,167 / 865 / 833.

Why these values: §3 of the feature report (`data/derived/p4_describe.txt`) —
distributions only.

## 4. Scores (computed only after registration)

Per completed round trip *i* in the lookback (flat → flat per coin, flips
split, exact cash flow — not `realized_pnl`, which drifts on sub-cent coins):

    RoN_i = (cash flow_i − est. Hyperliquid fees_i) / max notional_i

Fees per fill at Hyperliquid's base tier — 0.045% taker, 0.015% maker —
**confirmed against Hyperliquid's fee schedule before the first score; a
correction then is an amendment, allowed only before any outcome exists.**

- **Plain score** (T1): `t = mean(RoN) / sd(RoN) × √n`.
- **Cross-regime score** (T2, T3): trips split by the BTC regime when they
  opened — *rising* if BTC's trailing 30-day return was ≥ 0, else *falling*
  (`Regimes.is_rising`, point-in-time). `score = min(t_rising, t_falling)`,
  each over ≥ 4 trips; wallets without 4 trips in both regimes have no score
  and are excluded (count reported). A leader must be good in **both**
  regimes — this removes "was short while everything fell".
- **Long-bias** (control): long-opening notional / all opening notional.

## 5. T1 — H0-leader (diagnostic)

Outcome: the plain score over the hold window, eligible wallets with ≥ 5
completed trips there. Wallets split into terciles of lookback long-bias;
per window, Spearman IC between lookback and hold score within each tercile,
count-weighted. Fisher z_w = atanh(IC_w)·√(n_w − 3); Z = Σ z_w / √W.
**Pass:** Z ≥ 2.13 and IC_w > 0 in ≥ 3 of 4 windows. Reported: wallets
dropped for < 5 hold trips, and the variant scoring them 0.

## 6. T2 — A: slow-trader copy

For **every** scored slow wallet, a single-leader copy over the hold window
(`ct/replay.follow` + `ct/targets.Copy([leader])`, configuration §7) at two
lags: the follower acting on the leader's positions **1 h** old (poll every
60 min) and **24 h** old (`LeaderBook` lag).

Outcome: the follower's **mean daily net return on capital, BTC-hedged**. A
wallet that stops trading leaves a flat follower — nobody is dropped.

**Pass, all four:**
1. tercile-weighted IC between cross-regime score and 1 h-lag outcome:
   Z ≥ 2.13;
2. IC_w > 0 in ≥ 3 of 4 windows;
3. **the top quintile earns**: pooled mean hedged daily net return at 1 h
   lag > 0 (rank correlation alone is never enough — the spec's reversion
   study ranked well and lost money);
4. **the mechanism holds**: the top quintile's pooled mean at **24 h** lag is
   ≥ 50% of its 1 h value. If the edge collapses with a day's delay it was
   speed, not insight — and not deployable at this scale.

Reported with it: top-quintile median; equal- and risk-weighted per-trip
ATR-R; the decay curve at lags 10 min / 1 h / 6 h / 24 h / 72 h.

## 7. Simulator configuration (T2)

| parameter | value | justification (registration 000: pilot-equal values need one) |
|---|---|---|
| poll interval | **60 min** | slow leaders need no faster; cheapest to run live (no socket) |
| lag | 1 h primary, 24 h mechanism check | §6 |
| capital | $10,000 nominal | "does the signal exist"; real ~$100 reported (§8) |
| target leverage | 1.0× | one unit of gross exposure at the leader's typical risk |
| leader typical leverage | median gross leverage over the lookback snapshots | point-in-time |
| leader equity | as-of the latest snapshot before each poll | point-in-time |
| max leverage | 3× | account-safety cap in line with the live trader's posture, not tuned |
| rebalance band | 25% | trade only on a quarter drift — avoids paying 0.06% on small adjustments; a design rule |
| taker fee | 0.06% | Bitget, all contracts |
| slippage | 10% of the minute's high–low | about the half-spread of liquid perps at small size; 0% / 25% robustness |
| maintenance margin | 1% of gross | conservative vs Bitget's small-size tiers |
| funding | Hyperliquid funding as the proxy before 2026-07-09 | Bitget serves ~90 days; "no funding" robustness |
| Bitget listing | coins copied only after their first Bitget daily candle | P0 |
| hedge | BTC perp sized to −Σ (holding notional × coin beta), coin beta from 30 days of hourly returns vs BTC before each UTC day (point-in-time); rebalanced daily and right after any poll that trades; same fees and slippage; separate ledger, so hedged and unhedged are both reported | a holdings beta works from day one (a beta from the follower's own returns would leave the first month unhedged). Checked on a synthetic pure-BTC book: $341 unhedged → −$25 hedged, the rest being the hedge's own costs |
| window end | close everything at the last minute, paying costs | plan §5.5 |

## 8. T3 — B: skilled vs crowd

**Cohort** at each selection S: the top quintile of slow wallets by
cross-regime score, fixed for the hold window. **Crowd**: every other account
in the snapshots.

**Daily signal** per coin *c* (Bitget-mapped and listed), from each snapshot
(as-of rule): `tilt_X(c)` = net signed notional of group X in *c* ÷ gross
notional of group X across all coins. `signal(c) = tilt_cohort(c) −
tilt_crowd(c)`. A coin enters only if ≥ 3 cohort wallets hold it.

**Outcome:** coin return from snapshot time + 1 h to + 73 h (Bitget 1H
closes) — a 3-day horizon fitting slow money.

**Pass, all four:**
1. daily cross-sectional Spearman IC: mean > 0 with Newey–West t (lags 3,
   the horizon overlap) ≥ 2.13;
2. **costed portfolio**: long the top / short the bottom quintile of
   signal, equal risk (inverse 30-day volatility), one third of the book
   rebalanced each day (3-day hold), Bitget taker 0.06% + **flat 5 bp
   slippage** per side + funding proxy; mean daily net return > 0,
   Newey–West one-sided p < 0.0167. (Measured on synthetic data: costs are
   ~5 bp/day of the book, so the gross edge must be roughly 10 bp/day to
   pass — a high bar, stated in advance.)
3. mean and median of the portfolio's daily return have the same sign (the
   reversion lesson);
4. positive in both halves of the discovery period.

Reported: IC by monthly regime label; turnover; gross vs net.

## 9. Reported, decides nothing

All results split by monthly BTC regime (UP / DOWN / FLAT, `month_label` —
descriptive, uses hindsight). Robustness: poll 10 min; slippage 0% / 25%; no
funding; unhedged; real capital ~$100 with every skipped order counted;
quartiles instead of quintiles; T3 horizons 1 d and 7 d; plain score instead
of cross-regime score.

## 10. Built before any outcome is computed (prepare phase)

Each piece gets a mechanics check; none computes a score or an outcome.
**Status 2026-10-08** — built and checked (`tests/test_prepare.py`, all
synthetic): round trips / flips / fees / long-bias / cross-regime score;
statistics false-positive rate (0/200 one-sided at random) and power
(planted IC 0.1 detected 100%); Newey–West on autocorrelated noise; lag;
hedge; B's book (noise loses its costs, a planted persistent edge is found);
tilts on one real snapshot with an arbitrary address set. Populations built
(`p5_population.py`): eligible 1,524–2,245, slow with ≥ 4 trips in both
regimes 403–769 per window; 1,200–1,600 vault addresses removed per window.
The runner `p5_run.py` refuses to start unless this file is committed as
REGISTERED and unmodified, aborts if its mechanics gates fail, and appends
results citing this file's commit. Remaining before launch: Bitget 1-minute
blocks for T2 (~196k requests), Hyperliquid funding history (running).
Items:
- archive fills → `leader_events`, point-in-time Bitget listing dates;
- `equity_at(t)`, typical leverage from the snapshot table;
- `LeaderBook` lag; BTC hedge leg in `follow`;
- Hyperliquid funding history; Bitget 1H candles for mapped coins over
  discovery; 1m blocks for A's executions (positions only — no PnL needed
  to know where orders fall);
- vault address list;
- regime labels (done: BTC 1H 2019-07 → 2026-10, no gaps);
- score, tercile IC, Fisher combination, the B signal and portfolio — tested
  on **synthetic** data only: random signals must give |Z| < 2.13 in ≥ 95%
  of 200 seeds.

## 11. Owner decisions before this becomes REGISTERED

| # | item | proposed |
|---|---|---|
| D1 | A poll / lags | 60 min; 1 h primary, 24 h mechanism check |
| D2 | slippage | T2: 10% of minute range; T3: flat 5 bp per side |
| D3 | family-wise α | 0.05 / 3, one-sided |
| D4 | funding proxy | Hyperliquid funding |
| D5 | equity freshness | ≤ 7 days |
| D6 | populations | §3 (slow: median hold ≥ 24 h, ≥ 10 trips) |
| D7 | cross-regime score | min(t_rising, t_falling), ≥ 4 trips each, BTC 30-day trend |
| D8 | A mechanism criterion | 24 h lag keeps ≥ 50% of the 1 h top-quintile mean |
| D9 | B definition | cohort = top-quintile slow wallets; tilt difference; 3-day horizon; ≥ 3 cohort holders |
| D10 | B pass | §8, all four |
