# 001 — Slow skilled traders: can a follower capture them, and does their positioning beat the losers'?

**Status: REGISTERED 2026-10-09** (owner, 2026-10-09: "all of them it is,
register, commit and start"). This commit is the registration: every item
in §11 approved, every §10 item built and checked on synthetic data, T2b on
all scored day traders. No ranking score or outcome existed before it. Any
later change is an amendment, allowed only before an outcome exists.

Plan: `docs/copy-trade-plan-2026-10-07.md`. Prior ledger entry: `000` (P0
looked inside the holdout). Drafting inputs were behaviour-only:
`p4_features.py`, `p4_describe.py` (no returns, `realized_pnl` never read),
BTC price history for regime labels (`ct/regimes.py`), and one real snapshot
read for a mechanics fact (§8, revision 4).

**Revision history (all before registration):**
1. First draft: book tests (consensus, top-K) as pass/fail — too little power.
2. Second: a cross-sectional copy test of all eligible wallets.
3. Third (owner, 2026-10-08): built around the two ideas with a mechanism.
   **A** — copy only *slow* traders: a position held for days is not
   front-run away in minutes, and few trades mean low costs. **B** — use the
   full-population positions joined to point-in-time skill, and trade where
   skilled slow money disagrees with the crowd.
4. **This one (2026-10-08, after an independent review of revision 3; owner
   goal restated: find leaders whose edge is NOT crowded and replicate them;
   capital is no longer a constraint — a paper account comes first).** What
   changed and why:
   - **B's "crowd" was empty.** In a perp market every account's signed
     notional sums to zero per coin. Verified on the 2026-03-02 snapshot:
     |net / gross| < 1e-15 for all 190 markets. So `tilt_crowd(c)` was
     mechanically `−cohort_net(c) / crowd_gross`, and the old signal was the
     cohort's own tilt times a per-day constant — identical ranking, IC and
     book. **The crowd is now a named group: the bottom quintile of scored
     slow wallets** (§8). "Skilled minus losers" carries information; "skilled
     minus everyone" did not.
   - **The holdings-beta hedge deleted timing skill.** A leader long BTC gave
     a follower long BTC plus short BTC: flat. Its own synthetic check showed
     it ($341 unhedged → −$25 hedged). Majors are > 70% of gross open
     interest, so A's outcome had measured "alt selection minus two legs of
     costs". **The skill outcome is now regression alpha; the tradeable
     outcome is a static hedge from the lookback** (§6, §7). The holdings
     hedge stays as a reported variant.
   - **Loser persistence.** In retail-trader studies the robust part of
     persistence is that bad traders stay bad. A t-stat score punishes
     churn, so IC criteria can pass on the bottom tail while the top earns
     nothing. **An IC pass with a flat top quintile is a null** (§5, §6), and
     IC within the top half plus decile means are reported.
   - **Flat follower = 0** can encode activity, not skill. Active-only IC and
     the share of zero outcomes by score quintile are reported (§6).
   - **The 24 h mechanism check was a ratio of two noisy means.** It is now
     a paired difference per wallet with its standard error (§6).
   - **Windows are not independent** (the same wallets recur in all four).
     Wallet overlap and a wallet-clustered bootstrap are reported; the Fisher
     Z is read as an upper bound (§5, §6).
   - **Crowding is measured, not only inferred from lag** (§4b, T4).
   - **Day traders get their own trial (T2b)** at a 10-minute poll; scalpers
     are scored in T1 and reported, never copied (§6b, §9).
   - **Lag grid gains a 1-minute point and a 1-minute poll** (robustness).
     A one-minute delay is deployable (Vercel per-minute cron, or a local
     poller for the paper account); sub-minute is not. An edge concentrated
     in the first minutes is a *speed* edge — the crowded kind — and is read
     as a warning, not a pass.
   - **The "$100 real size" robustness is replaced by the paper account's
     size** (D15). Live following at the old account size is no longer the
     question.
   - **Regime caveat written in** (§2): all four hold windows are bear or
     flat; the holdout is one rally. A discovery pass means "persisted inside
     a bear"; a holdout pass adds one regime transition, nothing more.

## 1. Trials registered here

| id | hypothesis | role |
|---|---|---|
| T1 | **H0-leader**: a leader's own skill persists (all eligible traders, crypto), by hold-time bucket | diagnostic — "no skill" vs "skill not capturable"; expected to pass on the bottom tail |
| T2 | **A — slow-trader copy**: among slow traders (median hold ≥ 24 h), the selection score predicts the follower's alpha; the top quintile earns on a static-hedged Bitget book; the edge survives a 24-hour delay | primary |
| T2b | **A' — day-trader copy**: same among day traders (median hold 1–24 h) at a 10-minute poll; the edge survives a 1-hour delay | secondary trial |
| T3 | **B — skilled vs losers**: the gap between top- and bottom-quintile slow traders' positioning predicts coin returns, market-neutral, after costs | primary |
| T4 | **C — uncrowded skill**: among high-score slow wallets, the 24 h-lag outcome is higher for wallets with few shadowers than for wallets with many | secondary trial |

Five trials. Family-wise one-sided α = 0.05 / 5 = **0.01 (z ≥ 2.33)**.
**D11:** the owner may instead take T1 out of the family as a pure
diagnostic (then 4 trials, α = 0.0125, z ≥ 2.24); either way the count is
fixed before any outcome exists. Crypto (main dex) only: `xyz` has 15–122
slow traders per window (too few) and no Capital venue yet. Everything in §9
is reported and decides nothing.

**Written expectations** (so a contradiction is the interesting result):
T1 passes, carried by loser persistence. Scalper copies lose net at every
deployable poll. Day traders are marginal at 10 minutes. Slow traders are the
only bucket that can clear costs at 60 minutes. If the edge of any bucket is
concentrated before the 10-minute lag, it is speed, and we cannot win that
race against co-located copiers.

## 2. Data and periods

- Discovery only: 2025-07-28 → 2026-06-30 (`p3_fills.py`, P3 gates passed).
  **Holdout (2026-07-01 →) is not read** — a later registration at P6. The
  holdout is mostly a rally (Aug +24%, Sep +8%).
- Windows (`ct/schedule.py`): lookback 91 d, hold 56 d, step 56 d; selections
  2025-10-27, 2025-12-22, 2026-02-16, 2026-04-13.
- **Regime limit, stated in advance.** The four hold windows are: Nov −23%,
  Dec–Feb grind down, Feb–Apr down then a bounce, Apr–Jun down. Discovery
  therefore answers "did skill persist *inside the 2025-26 bear*". The
  holdout supplies one regime transition; the paper period (P7) a second.
  No result from this registration may be described as regime-robust.
- Leader equity and positions: snapshot table, as-of the latest snapshot
  strictly before the moment needed (P2). No snapshots 2025-10-25 → 12-14:
  B has no signal on those days; A uses the last snapshot before.
- Fills: archive, file order = execution order (P3 G3).
- Prices: Bitget public API — 1H candles for B, betas and the hedge, 1m
  candles for A's executions (200-minute blocks containing an order), cached.
- Regimes: BTC 1H since 2019-07 (`ct/regimes.py`).

## 3. Populations — behaviour only, on the lookback [S − 91 d, S)

**Eligible (T1):** ≥ 20 completed round trips; account value ≥ $10,000 and
≤ 7 days old as-of S; maker share ≤ 0.5; median hold ≥ 2 h; ≤ 500 fills per
active day; ≥ 80% of notional in Bitget-mapped coins; vaults / protocol
accounts excluded (address list fixed before any score is computed). The
equity floor removes lookback losers who fell under $10k — a range
restriction on the score, stated here, not a bias in sign.

**Hold-time buckets (T1 reporting, T2 / T2b populations):** *scalper* median
hold < 1 h (T1 only — the eligible floor of 2 h is lifted to 0.2 h for this
bucket, fills/day ≤ 2,000, so that scalpers exist in T1 at all); *day trader*
1 h ≤ median hold < 24 h; *slow* median hold ≥ 24 h.

**Slow (T2, T3, T4):** eligible, but **median hold ≥ 24 h** and **≥ 10**
completed round trips (slow traders trade less). Sizes per window before the
age and vault filters: 1,647 / 1,167 / 865 / 833; after all filters: 888 /
708 / 600 / 641; with ≥ 4 trips in both regimes 769 / 419 / 403 / 516; with
≥ 8: 489 / 248 / 218 / 319 (`p5_population.py`, 2026-10-08).

**D7 outcome (rule applied, counts only):** the smallest window at a floor
of 8 has 218 scored slow wallets, under the 250 the rule requires, so the
**per-regime trip floor is 4** for every window and every trial. Stated
before any score exists; the floor-8 variant is not run.

**Day traders (T2b):** eligible, **1 h ≤ median hold < 24 h**, ≥ 20 trips:
2,049 / 1,818 / 1,506 / 1,603 per window; with ≥ 4 trips in both regimes
1,904 / 1,289 / 1,228 / 1,529. **Scalpers (T1 only):** 520 / 524 / 445 /
342. T1 by bucket (scalper / day / slow): 520 / 2,049 / 533; 524 / 1,818 /
428; 445 / 1,506 / 345; 342 / 1,603 / 371.

**Hedge legs (plan §5.2), not filtered.** Cross-venue basis traders are
slow, consistent and good in both regimes — exactly what the score favours.
No reliable behaviour-only rule was found; the limitation is stated and the
top quintile's funding income share is reported (§9).

Why these values: §3 of the feature report (`data/derived/p4_describe.txt`) —
distributions only.

## 4. Scores (computed only after registration)

Per completed round trip *i* in the lookback (flat → flat per coin, flips
split, exact cash flow — not `realized_pnl`, which drifts on sub-cent coins):

    RoN_i = (cash flow_i − est. Hyperliquid fees_i) / max notional_i

Fees per fill at Hyperliquid's base tier — 0.045% taker, 0.015% maker —
**confirmed 2026-10-09 against the fee schedule at
hyperliquid.gitbook.io/hyperliquid-docs/trading/fees** (tier 0 perps:
taker 0.045%, maker 0.015%). Referral discounts on a wallet's first $25M
of volume and HYPE-staking discounts of 5–40% exist, so the base tier
overstates fees for large or staked wallets: RoN is a conservative estimate
of net skill, by a near-constant per-trip amount that cannot flip a ranking
among slow traders. Not modelled.

- **Plain score** (T1): `t = mean(RoN) / sd(RoN) × √n`.
- **Cross-regime score** (T2, T2b, T3, T4): trips split by the BTC regime
  when they opened — *rising* if BTC's trailing 30-day return was ≥ 0, else
  *falling* (`Regimes.is_rising`, point-in-time). `score = min(t_rising,
  t_falling)`, each over ≥ 4 trips; wallets without 4 trips in both regimes
  have no score and are excluded (count reported). A t over 4 trips is
  noisy and the min of two regresses hard; **D7 asks the owner whether the
  floor becomes 8** (fewer wallets, less noise — counts for both reported by
  `p5_population.py` before the decision).
- **Long-bias** (control): long-opening notional / all opening notional.

### 4b. Crowding features (new, revision 4)

Per leader on the lookback. The first two use other wallets' *behaviour*
only and are built and checked on synthetic data in the prepare phase; the
third uses the leader's PnL rank and waits for the REGISTERED commit.

- **Shadower excess.** For each leader *open* (flat → position, or flip) in
  coin *c* at time *t*: the number of distinct other wallets that open *c*
  in the same direction from flat within [t, t + 60 min), minus the expected
  number from the coin's base rate of such opens per hour over [t − 24 h,
  t + 24 h) excluding that hour. Per leader: mean excess over its opens, and
  **repeat-shadower share** — the share of all shadow-opens (wallet ×
  window appearances) that come from wallets seen on ≥ 3 of the leader's
  opens. Count-weighted: a share of distinct wallets is diluted by one-off
  random openers in busy coins (synthetic check: 5 persistent copiers among
  36 observed wallets read 0.14 by wallet, 0.65 by appearance). A
  persistent set is the signature of being copied.
- **Post-trade path.** For each leader open: the coin's Bitget return from
  the open to +10 min, +1 h, +6 h, +24 h, +72 h, signed by the leader's
  direction, in units of the coin's daily ATR%. Per leader: the mean path.
  A spike that reverts says impact or front-running; drift says
  information. (Uses prices, not the leader's cash flow, but it is a return
  measure of the leader's trades: computed only after registration.)
- **Watchability.** The leader's trailing 30-day realized-PnL rank among all
  wallets as-of S, from fills (the public leaderboard ranks by dollar PnL, so
  this is what commercial copiers surface), and its account-value rank.
  After registration only.

## 5. T1 — H0-leader (diagnostic)

Outcome: the plain score over the hold window, eligible wallets with ≥ 5
completed trips there. Wallets split into terciles of lookback long-bias;
per window, Spearman IC between lookback and hold score within each tercile,
count-weighted. Fisher z_w = atanh(IC_w)·√(n_w − 3); Z = Σ z_w / √W.
**Pass:** Z ≥ z_crit and IC_w > 0 in ≥ 3 of 4 windows.

Reported: the same by hold-time bucket; **IC within the top half of the
lookback score**; decile means of the hold score; wallets dropped for < 5
hold trips, and the variant scoring them 0; wallet overlap between windows
and a wallet-clustered bootstrap of Z. **Reading rule:** a pass with a flat
or negative top half is loser persistence, and is reported as such.

## 6. T2 — A: slow-trader copy

For **every** scored slow wallet, a single-leader copy over the hold window
(`ct/replay.follow` + `ct/targets.Copy([leader])`, configuration §7) at a
60-minute poll, lags **1 h** (primary) and **24 h** (mechanism), plus the
decay grid of §9.

**Outcomes per wallet-window** (a wallet that stops trading leaves a flat
follower — nobody is dropped):
- **alpha** — the intercept of the follower's unhedged daily net return
  regressed on BTCUSDT's daily return over the hold window. Skill with
  market exposure removed *on average*, so a leader whose skill is when to
  hold BTC keeps it. Used for the ranking criteria.
- **static-hedged net return** — mean daily net return of the follower book
  plus a BTCUSDT hedge sized from the lookback (§7). A tradeable number, all
  costs in. Used for the earning criteria.

**Pass, all four:**
1. tercile-weighted IC between cross-regime score and 1 h-lag **alpha**:
   Z ≥ z_crit;
2. IC_w > 0 in ≥ 3 of 4 windows;
3. **the top quintile earns**: pooled mean **static-hedged** daily net
   return at 1 h lag > 0 (rank correlation alone is never enough — the
   spec's reversion study ranked well and lost money);
4. **the mechanism holds**: (a) the top quintile's pooled static-hedged
   mean at **24 h** lag > 0, and (b) the **paired** per-wallet difference
   (1 h − 24 h), with its standard error, is less than half the 1 h mean.
   If the edge collapses with a day's delay it was speed, not insight.
   A 24 h mean *above* the 1 h mean is flagged as reversion suspicion.

**Reading rules:** an IC pass (1–2) with criterion 3 failing is a null —
loser persistence, not copyable skill. Reported with it: IC within the top
half; **active-only IC** and the share of zero outcomes by score quintile
(if the zero share trends with score, the IC carries activity and the
active-only IC is the one to read); top-quintile median; equal- and
risk-weighted per-trip ATR-R; holdings-hedged and unhedged variants; the
decay curve at every lag in §9; wallet overlap and the clustered bootstrap.

### 6b. T2b — A': day-trader copy

Identical to T2 on the day-trader population (§3) with: poll **10 min**,
primary lag **10 min**, mechanism lag **1 h**, same four criteria and
reading rules. Reported: the decay curve at 1 min / 10 min / 1 h / 6 h /
24 h, and the same at a 1-minute poll. Needs its own 1-minute Bitget blocks
(`p5_blocks.py` extended to this population before registration — counts
only, no PnL).

### 6c. T4 — C: uncrowded skill

Among the **top half** of slow wallets by cross-regime score, split at the
median of **shadower excess** (§4b) within each window. Outcome: the
static-hedged daily net return at **24 h** lag. **Pass:** pooled mean of
the low-shadower half minus the high-shadower half > 0 with z ≥ z_crit,
standard error from a wallet-level bootstrap pooled over windows. Power is
limited (roughly 40–75 wallets per half per window) and is reported with
the result. Reported: the same split by repeat-shadower share and by
watchability; the post-trade path by split.

## 7. Simulator configuration (T2; T2b differs only where §6b says)

| parameter | value | justification (registration 000: pilot-equal values need one) |
|---|---|---|
| poll interval | **60 min** (T2b: 10 min) | slow leaders need no faster; cheapest to run live |
| lag | 1 h primary, 24 h mechanism (T2b: 10 min / 1 h) | §6 |
| capital | $10,000 nominal | "does the signal exist"; the paper size reported (§9, D15) |
| target leverage | 1.0× | one unit of gross exposure at the leader's typical risk |
| leader typical leverage | median gross leverage over the lookback snapshots | point-in-time |
| leader equity | as-of the latest snapshot before each poll | point-in-time |
| max leverage | 3× | account-safety cap, not tuned |
| rebalance band | 25% | trade only on a quarter drift — a design rule |
| taker fee | 0.06% | Bitget, all contracts |
| slippage | 10% of the minute's high–low | about the half-spread of liquid perps at small size; 0% / 25% robustness |
| maintenance margin | 1% of gross | conservative vs Bitget's small-size tiers |
| funding | Hyperliquid funding as the proxy before 2026-07-09 | Bitget serves ~90 days; "no funding" robustness |
| Bitget listing | coins copied only after their first Bitget daily candle | P0 |
| **hedge (static, revision 4)** | one BTCUSDT position for the whole window, notional = −β̄ × target leverage / leader typical leverage × capital, where **β̄ is the leader's lookback average net beta**: the time-average over lookback days of Σ_c position_c × price_c × beta_c / equity, positions from the fill path, equity as-of, beta_c the point-in-time 30-day hourly beta to BTC (BTC = 1; no beta → 1); re-trued daily when off by > 25%; same fees and slippage; separate ledger | removes the leader's *average* market exposure and keeps timing; a holdings hedge (rebalanced after every trade) removes both — reported as a variant. Exact handling of flat days and of positions open at lookback start is settled in the prepare phase with a synthetic test, before registration |
| window end | close everything at the last minute, paying costs | plan §5.5 |

## 8. T3 — B: skilled vs losers

**Cohort** at each selection S: the top quintile of slow wallets by
cross-regime score, fixed for the hold window. **Crowd (revision 4): the
bottom quintile by the same score**, fixed likewise. The old "everyone else"
crowd was the cohort's mechanical negative — net signed notional per coin
sums to zero across all accounts (verified, 2026-03-02 snapshot, 190/190
markets) — so it carried no information. **D13:** the owner may choose the
alternative crowd, accounts with value < $1,000 ("retail"); the other is
reported.

**Daily signal** per coin *c* (Bitget-mapped and listed ≥ 31 days), from
each snapshot (as-of rule): `tilt_X(c)` = net signed notional of group X in
*c* ÷ gross notional of group X across all coins. `signal(c) = tilt_top(c) −
tilt_bottom(c)`. A coin enters only if ≥ 3 cohort wallets hold it; a bottom
tilt with no holders is 0.

**Outcome:** coin return from snapshot time + 1 h to + 73 h (Bitget 1H
closes) — a 3-day horizon fitting slow money. The daily IC uses the **same
3-day return** (the runner's 1-day IC is corrected before registration).

**Pass, all four:**
1. daily cross-sectional Spearman IC: mean > 0 with Newey–West t (lags 3,
   the horizon overlap) ≥ z_crit;
2. **costed portfolio**: long the top / short the bottom quintile of
   signal, equal risk (inverse 30-day volatility), one third of the book
   rebalanced each day (3-day hold), Bitget taker 0.06% + **flat 5 bp
   slippage** per side + funding proxy; mean daily net return > 0,
   Newey–West one-sided p < α. (Measured on synthetic data: costs are
   ~5 bp/day of the book, so the gross edge must be roughly 10 bp/day to
   pass — a high bar, stated in advance.)
3. mean and median of the portfolio's daily return have the same sign (the
   reversion lesson);
4. positive in both halves of the discovery period.

Reported: IC by monthly regime label; turnover; gross vs net; **breadth** —
coins per day with a signal and names per side (expect 2–6 on many days).

## 9. Reported, decides nothing

All results split by monthly BTC regime (UP / DOWN / FLAT, `month_label` —
descriptive, uses hindsight). Robustness: poll 10 min and **1 min**;
lags **1 min** / 10 min / 1 h / 6 h / 24 h / 72 h (the decay curve, per
hold-time bucket, scalpers included — a scalper copy is reported here and
never tested); slippage 0% / 25%; no funding; unhedged; holdings-hedged;
**the paper account's size (D15) with every skipped order counted**;
quartiles instead of quintiles; T3 horizons 1 d and 7 d; plain score
instead of cross-regime score; T3 with the retail crowd; the top quintile's
funding income as a share of gross (the hedge-leg limitation).

## 10. Built before any outcome is computed (prepare phase)

Each piece gets a mechanics check; none computes a score or an outcome.
**Status 2026-10-08** — built and checked (`tests/test_prepare.py`, all
synthetic): round trips / flips / fees / long-bias / cross-regime score;
statistics false-positive rate (0/200 one-sided at random) and power
(planted IC 0.1 detected 100%); Newey–West on autocorrelated noise; lag;
holdings hedge; B's book (noise loses its costs, a planted persistent edge
is found); tilts on one real snapshot with an arbitrary address set.
Populations built (`p5_population.py`): eligible 1,524–2,245, slow with
≥ 4 trips in both regimes 403–769 per window; 1,200–1,600 vault addresses
removed per window. The runner `p5_run.py` refuses to start unless this
file is committed as REGISTERED and unmodified, aborts if its mechanics
gates fail, and appends results citing this file's commit. Bitget 1H
candles and Hyperliquid funding: fetched. 1-minute blocks for the slow
population: fetching (~196k requests, resumable).

**Revision 4 items, in build order, each with a synthetic test
(2026-10-08; test files named):**
- [x] T3 crowd = a named set (`ct/crowd.tilts(..., crowd=...)`;
      `tests/test_crowd_sets.py`: on a synthetic snapshot the old
      "everyone else" signal has Spearman 1.000 with the cohort's own tilt
      and equals it times one per-day constant; a bottom-quintile crowd
      recovers a planted disagreement on 24/25 coins; breadth reported);
- [x] regression-alpha outcome and the static hedge (`replay.Params
      hedge_mode='static'`, `leaders.average_beta`, `stats.alpha`,
      `replay.benchmark_daily_returns`; `tests/test_prepare.py`: a
      constant-beta BTC book is neutralised by the static hedge
      ($262 → −$18); a 5-of-10-day BTC timer keeps its timing under the
      static hedge ($586 → $446, expected $459) and loses it under the
      holdings hedge (→ −$29); alpha recovers a planted intercept; average
      beta = days held / days × price / equity, and a position open at the
      window start is read from `startPosition`);
- [x] lag grid with 1 min; 1-minute poll in the robustness set
      (`p5_blocks.SPECS`, `p5_run.T2 / T2B`);
- [x] day-trader population and scalper bucket (`p5_population.py`, counts
      at both D7 floors, `population_summary.json` carries the chosen
      floor); `p5_blocks.py` extended to the day-trader population at
      10-minute and 1-minute polls and to the daily BTC marks the static
      hedge re-trues on — fetch queued behind the slow-population fetch;
- [x] shadower excess and repeat-shadower share (`ct/crowding.py`;
      `tests/test_crowding.py`: a planted set of 5 copiers among 300 random
      openers gives excess 5.01 and repeat share 0.64, an uncopied leader
      −0.37 and 0.00; opens from fills count an open and a flip, not a
      reduce);
- [x] post-trade path and watchability (`ct/crowding.py`, code only — no
      real leader until REGISTERED; the 10-minute horizon fetches the
      leader's open minute from Bitget at run time, cached);
- [x] statistics: top-half IC, decile means, active-only IC and zero share,
      paired lag difference with SE, wallet overlap and clustered bootstrap
      (`ct/stats.py`; `tests/test_stats_r4.py`: 0/200 null seeds reach
      z 2.33; bootstrap sd 0.92 for independent wallets and 1.67 when one
      wallet effect repeats across the 4 windows; a planted crowding
      penalty found at z 2.7, null at −1.2);
- [x] T4 split and bootstrap (`stats.split_test`, same test);
- [x] T3 IC on the 3-day return (`p5_run.crowd_trial`);
- [x] `p5_run.py` writes T1 / T2 / T2b / T3 / T4 with trial count 5 and
      z 2.33 on every block; paper size $1,000 for the top quintile
      (`tests/test_runner_assembly.py`: synthetic accumulators — a planted
      delay-robust edge passes all four T2 criteria, a null does not, a
      planted crowding penalty passes T4, and an IC pass with a flat top
      quintile is labelled NULL by the reading rule). The guard still
      refuses to start: "not REGISTERED".

**Still open before the REGISTERED commit:** the owner's REGISTERED
commit. The revision-4 block fetch finished 2026-10-09 06:13 (186,008
blocks, cache 2.6 GB); fees confirmed (§4).

## 11. Owner decisions — approved 2026-10-08

All fifteen approved by the owner on 2026-10-08 with the values below. The
file stays DRAFT until the §10 items are built and checked; the REGISTERED
commit then freezes exactly this table.

| # | item | approved |
|---|---|---|
| D1 | A poll / lags | 60 min; 1 h primary, 24 h mechanism check. T2b: 10 min; 10 min / 1 h |
| D2 | slippage | T2: 10% of minute range; T3: flat 5 bp per side |
| D3 | family-wise α | 0.05 / 5 = 0.01, one-sided, z ≥ 2.33 |
| D4 | funding proxy | Hyperliquid funding |
| D5 | equity freshness | ≤ 7 days |
| D6 | populations | §3: slow median hold ≥ 24 h, ≥ 10 trips; day traders 1–24 h, ≥ 20 trips; scalpers 0.2–1 h in T1 only |
| D7 | cross-regime score | min(t_rising, t_falling), BTC 30-day trend; **≥ 8 trips per regime, with the rule fixed now: if any window has fewer than 250 scored slow wallets at 8, the floor is 4 for all windows** (counts from `p5_population.py`, behaviour only). **Applied 2026-10-08: 218 in window 3 → floor 4** (§3) |
| D8 | A mechanism criterion | 24 h top-quintile static-hedged mean > 0 and paired (1 h − 24 h) < half the 1 h mean; 24 h above 1 h flagged as reversion suspicion |
| D9 | B definition | cohort = top-quintile slow wallets; crowd = bottom quintile; tilt difference; 3-day horizon; ≥ 3 cohort holders |
| D10 | B pass | §8, all four |
| D11 | family | 5 trials, T1 included |
| D12 | A outcomes | ranking on regression alpha; earning on the static-hedged book; holdings hedge and unhedged reported |
| D13 | B crowd | bottom quintile primary; retail < $1k reported |
| D14 | T2b and T4 | included, counted in the family |
| D15 | paper account size | $1,000 — the smallest size at which BTC's lot step does not dominate (about $200 a name at one unit of gross over five coins); reported with every skipped order counted |
