# 005 — Post-cascade drift: enter against the forced flow after it has ended, hold two hours (H4)

**Status: REGISTERED 2026-10-10** — the owner approved the decision table
(D1–D16, holdout option A; the D11/D12 gate left to the orchestrator,
Revision 3). The status line is changed to REGISTERED by the owner. Mechanics are built and checked on
synthetic data; the event set, filters and entry prices were counted
(§3, behaviour only); no exit price and no return after any entry has been
computed for any real event. The runner (`l7_run.py`) refuses to compute an
outcome until this file says REGISTERED, is committed unmodified, and the
code and event file match the hashes in §8.

**Revision 1 (2026-10-10, before registration, orchestrator's decisions on
the first draft's flagged items).** Primary event set without the warm-up
events qualified on the cross-coin fallback median (D1); stress switch on
the market series *excluding the cascade coin's own liquidations* (D5);
criterion 6 removes the largest *positive* hour (D9); holdout criteria fixed
(D14); the hedged variant stated (§4). The first draft's versions of D1 and
D5 are reported blocks.

**Revision 2 (2026-10-10, before registration, independent review).** The
guard checks tracking, commit, clean code, file hashes and an existing
result row (§8); power is stated jointly over all six criteria, by
simulation, at unconditional and pre-cascade volatility (§3); the holdout
end date is fixed now as an owner choice A / B (D14); the placebo flips
the gross return per hour cluster with costs kept (§5); the stress switch
reads only buckets complete before the entry; exits use a new
deterministic tape lookup; disclosures below; D12 and D13 reworded. No
outcome existed for any version.

**Revision 3 (2026-10-10, after the owner's approval).** Owner chose
holdout option A (end 2026-10-09). The holdout gate (D11/D12) is the
discovery pass alone, not "pass and point estimate ≥ 25 bp": under option A
the point-estimate gate cut the end-to-end chance at a true 25 bp effect
from about 0.46 (0.81 × 0.57) to 0.29, and a pass already requires an
estimate of about 18 bp or more (2.33 × SE). The 25 bp bar moves to where
the data is larger: the forward paper period a holdout CONFIRM leads to.
`ct/drift.decide` changed accordingly; nothing else did. No outcome existed.

Plan: `docs/copy-trade-plan-2026-10-07.md`; horizon H4 in
`docs/forced-flow-research-horizons-2026-10-10.md` §4. Prior ledger entries
`001`–`004`: **thirteen trials on one discovery year, no pass.**

**The hypothesis.** After a local (non-spiral) liquidation cascade ends,
majors drift back against the forced flow over the next hours by enough to
clear taker costs, when the trade is entered *after* the flow has ended —
not at its onset, not inside it, not with a resting order.

**Why this, and what is already known about it.** 003 T1 (fill at the
cascade's median forced print) showed the reversion building with the
horizon: +0.13% at 15 min, **+0.75% at 60 min, +1.18% at 240 min** (all
coins, medians 0.46–0.48%), majors +0.22% at 15 min, t 3.3. 004's
**fixed 60-minute exit in majors was +0.16%, t 1.46** — the only positive
block in that registration. 002 measured a 60-minute hold on Bitget after a
cascade at **0.00% in majors** — but 002 entered at *detection*, inside the
flow, on Bitget, with a one-minute poller. This is entry at *flow end*, on
Hyperliquid's own tape, with 004's stress switch, at a longer horizon.

**What the earlier numbers do and do not say (disclosure).**

- 003's majors +22 bp (t 3.3) was computed on all 002 cascades,
  **including the 152 majors warm-up events this registration drops**.
- 003 T1's **first half was negative (−0.39%)**; the second carried it
  (+0.64%).
- 003 T1 was **asymmetric by side**: shorts liquidated +0.42% (t 2.1),
  longs liquidated +0.06%. Two thirds of this registration's majors
  positions are on the longs-liquidated side, so **criterion 5 (positive
  on both sides) is a binding constraint**, not a formality.
- **Majors became the primary partly because 004's only positive block
  was its fixed 60-minute exit in majors** (`fixed_60m_majors`, +0.16%,
  t 1.46) — the universe was chosen after seeing that block.

**The fourteenth look, stated plainly.**

- This is **trial 14** on the discovery year 2025-07-28 → 2026-06-30.
- The horizon (120 min), the hold-for-hours idea and the majors universe
  were chosen *after* 003 and 004 reported on these same cascades. Those
  numbers were from a different entry (the median forced print, or a
  ladder fill inside the flow), so this test is not a re-read of them —
  but it is not independent of them either. **A pass on discovery is
  partly in-sample; only the holdout decides.** That is why the threshold
  is t ≥ 2.33 and not 1.96 for a single trial, why a day-clustered t is
  required as well, and why a pass leads to the holdout and not to a
  conclusion.

## 1. Trials registered here

| id | hypothesis | role |
|---|---|---|
| T1 | **Post-cascade drift**: at the end of a qualifying cascade (002 primary, without warm-up events), after a full minute with no liquidation in the coin, enter as a taker against the forced flow (longs liquidated → buy; shorts liquidated → sell) and exit as a taker 120 min later — earns net of fees and slippage in **majors** (BTC, ETH, SOL) | primary, the only trial |

One trial. One-sided threshold **hour-clustered t ≥ 2.33** (α ≈ 0.01,
stricter than 1.96 for the reasons above) **and day-clustered t ≥ 1.96**.
**Cumulative across 001–005: fourteen trials on one discovery year.**
Everything in §6 is reported and decides nothing.

**Written expectation.** About 1 in 3 that a significant, costed effect
exists in majors (the H4 estimate). If it exists at the 25 bp bar, the
joint probability that this registration ends in a holdout confirmation
is about **0.3–0.4 at unconditional volatility and 0.2–0.3 at pre-cascade
volatility** (§3).

## 2. Data and periods

- **Discovery: 2025-07-28 → 2026-06-30.** **Holdout (2026-07-01 →)
  unread** and its fills are not on disk; the runner drops any event whose
  entry window or longest reported exit would reach 2026-07-01 (none do:
  the last cascade ends 2026-06-30 04:09 UTC) and asserts every exit lookup
  is before it.
- **Events:** `data/derived/hyperliquid/cascades.json`, key `primary` —
  the 2,838 cascades registered in 002 (G 60 s, purity ≥ 0.8, notional
  ≥ K 1.0 × the coin's trailing 30-day median daily liquidation notional
  and ≥ $250 k; `l2_events.py` over `liquidations.parquet`), 463 in
  majors. **Primary: without the 426 events (152 majors) qualified on the
  cross-coin fallback median** (`fallback_median` — the coin had fewer than
  five days of its own liquidation history, 003's rule): 2,412 events, 311
  in majors. The inclusive 002 set is a reported block.
- **Tape:** every Hyperliquid archive fill (`ct/tape.py`); entry = first
  print at or after the entry time (`l7_run.first_prints`), exit = last
  print at or before the exit time (`tape.prices_at_ordered`, new in this
  registration). Both break same-millisecond ties by (timestamp, file,
  row) — file order is execution order (P3 gate G3). 002–004's
  `prices_at` is unchanged.
- **Market stress:** `liquidations.parquet`, one-second buckets — 004's
  series, all coins **minus the cascade coin's own** (primary); all coins
  (reported).
- **Hedge betas (reported variant):** Bitget 1H closes of BTC/ETH/SOL from
  the local cache, candles closed before each entry, trailing 30 days.
- **Fees:** Hyperliquid base tier taker 0.045% each side (confirmed
  2026-10-09).

**Causality of the event fields (checked, `l7_describe.py`).** The trade
reads five fields: `coin`, `side`, `t_start`, `t_end`, `fallback_median`
(`ct/drift.ENTRY_FIELDS`; a test poisons every other field and the
positions do not change). Qualification used the run's notional and purity
(known at `t_end`), the coin's trailing median of *strictly earlier* days
(known at `t_start`; the fallback flag is known at `t_start` too), and the
$250 k floor. The run is known to be over at `t_end + 60 s`: **0 of 2,838
events** have a liquidation print in the coin within (t_end, t_end + 60 s],
and **0** within the minute before `t_start` (runs are maximal). The entry
is at `t_end + 62 s`. Not point-in-time, and not read by the trade:

1. **Coin universe (survivorship).** `l2_events.py` keeps coins mapped in
   `data/p0/2026-10-07/symbols.json` and listed on Bitget per
   `bitget_listing.json`, both built 2026-10-07; a coin delisted from
   Bitget before then is absent. No effect on BTC/ETH/SOL (the primary);
   it affects which alts are in the reported blocks.
2. **HLP's own ADL rows** (erratum in 002–004) are in the liquidation
   series for 2025-10-10; that day is stressed either way.

## 3. What was measured before writing this (behaviour only, no return)

From `l7_describe.py` (log `data/scratch/l7.log`) and `l7_run.py --events`.

- **Events through the filters** (in order: warm-up → holdout edge →
  stress switch → no entry print within 60 s → one position per coin).
  Holdout edge removes nothing anywhere.

  | variant | universe | cascades | warm-up | stress | no print | overlap | **positions** |
  |---|---|---|---|---|---|---|---|
  | **primary** | **majors** | 463 | 152 | 50 | 0 | 49 | **212** (BTC 57, ETH 87, SOL 68) |
  | primary | alts | 2,375 | 274 | 476 | 2 | 518 | 1,105 |
  | primary | all | 2,838 | 426 | 526 | 2 | 567 | 1,317 |
  | inclusive events (first draft's D1) | majors | 463 | — | 56 | 0 | 146 | 261 |
  | inclusive stress (first draft's D5) | majors | 463 | 152 | 78 | 0 | 43 | 190 |

- **Sides (primary majors):** 142 longs-liquidated (buy) / 70
  shorts-liquidated (sell); all coins 976 / 341.
- **Halves (primary majors).** Calendar halves (split 2026-01-13):
  80 / 132. The criterion uses count halves in time order (as 003/004):
  106 / 106, split at 2026-01-26 15:12 UTC; sides per half 61 L / 45 S and
  81 L / 25 S.
- **Stress switch.** Excluding the cascade coin's own flow, it removes
  **526 events, 50 in majors** (inclusive series: 559 / 78), on 22 days
  (August–September spiral days, 2025-10-10, late October to November,
  2026-01-19 → 01-31, 2026-06-02 and 06-25). The own-coin exclusion
  **re-admits 33 events, 28 of them majors** — the largest own-coin
  cascades: their median notional is $53 M against $12 M for all
  primary-set majors cascades, 10 of the 28 are in the top decile of majors
  cascade notional, the five largest $91–135 M (D5).
- **One position per coin** skips 49 primary majors events; 19 primary
  majors positions have their 240-min window overlapping the next
  position in the same coin (the 240-min reported horizon is not
  independent across those).
- **Entry timing.** Entry fill − t_end in primary majors: median
  62.19 s, p90 62.8 s, p99 63.6 s. First print after the entry time:
  majors median 188 ms, p90 0.79 s, max 4.6 s — none missing within 5 s;
  alts median 0.57 s, p90 4.0 s, 135 without a print within 5 s, 2 within
  60 s (skipped). 43 events (all coins, all sets) see a new run of
  liquidations start during the 2 s latency; they are traded (the closed
  run qualified) — known, not changed.
- **Clusters among primary majors entries.** **161 distinct UTC-hour
  clusters, 114 UTC-day clusters**; at most 3 positions per hour, 7 per
  day; per day: 61 days with 1, 29 with 2, 12 with 3, 7 with 4, 5 days
  with 5–7. All coins: 802 hour / 261 day clusters.
- **Costs, round trip:** taker 4.5 bp × 2 = 9 bp; with 5 bp adverse
  slippage each side **19 bp** (primary); with 10 bp, 29 bp.
- **Hedge betas** (ETH/SOL on BTC, trailing 30 days of 1H closes before
  each primary entry): ETH median 1.20 (p10–p90 1.13–1.42), SOL 1.31
  (1.07–1.67); none missing.

**Power — minimum detectable mean (MDE).**

- *Unconditional volatility.* Bitget 1H closes at every hour of the
  discovery year (cached blocks, none reaching July), not at or
  conditioned on any event: SD of 120-min log returns **BTC 65 bp, ETH
  97 bp, SOL 108 bp** (robust SD 41 / 59 / 71 bp; kurtosis 8–11); 60 min
  47 / 68 / 76 bp; 240 min 92 / 140 / 155 bp. Correlation of 120-min
  returns 0.82–0.87. Under a driftless random walk with these volatilities
  and the 212 primary majors entry times (overlapping holds and cross-coin
  correlation included), the null SE of the mean is **7.8 bp**
  (hour-clustered 7.7, day-clustered 7.8). **MDE at t 2.33: 18 bp at 50%
  power, 25 bp at 80%** (day t 1.96: 15 / 22 bp).
- *Pre-cascade volatility.* Realised 2-hour volatility from 5-minute
  Hyperliquid tape returns over the 120 minutes **before each primary
  majors cascade's first print** (no price at or after `t_start`),
  against the same estimator on every non-overlapping 2-hour window of the
  year: BTC 129 vs 70 bp (**ratio 1.84**; median-based 1.39), ETH 132 vs
  101 bp (**1.30**), SOL 140 vs 125 bp (**1.12**). Scaling each coin's
  120-min SD by its ratio (BTC 120, ETH 127, SOL 122 bp): null SE
  **10.4 bp**, **MDE at t 2.33: 24 bp at 50% power, 33 bp at 80%** (day
  t 1.96: 20 / 29 bp). Volatility *after* a cascade is plausibly higher
  still; at 1.5× unconditional the 80% MDE is 37 bp, at 2× 49 bp.

**Joint power of all six criteria, by simulation** (`drift.simulate_power`:
2,000 draws; each primary majors position's net return = the planted true
mean + Gaussian random-walk noise at its actual entry time, with the
measured cross-coin correlation, overlapping holds, and its actual side;
the actual half split; all six criteria applied; holdout = contiguous
blocks of the same entry pattern under the D14 criteria):

| volatility | true mean | P(pass all six) | P(pass and estimate ≥ 25 bp) | P(holdout confirm) A / B | **end-to-end** A / B |
|---|---|---|---|---|---|
| unconditional | 18 bp | 0.51 | 0.20 | 0.38 / 0.58 | 0.08 / 0.12 |
| unconditional | **25 bp** | **0.81** | **0.52** | 0.57 / 0.80 | **0.29 / 0.42** |
| unconditional | 37 bp | 0.99 | 0.94 | 0.85 / 0.97 | 0.80 / 0.92 |
| pre-cascade | 18 bp | 0.31 | 0.26 | 0.26 / 0.41 | 0.07 / 0.10 |
| pre-cascade | **25 bp** | **0.55** | **0.50** | 0.40 / 0.61 | **0.20 / 0.30** |
| pre-cascade | 37 bp | 0.90 | 0.87 | 0.65 / 0.87 | 0.57 / 0.76 |
| unconditional | 0 (no effect) | 0.011 | 0.0005 | 0.05 / 0.05 | ≈ 0.00003 |

End-to-end = P(discovery pass with estimate ≥ 25 bp, the first draft's D11 gate) ×
P(holdout confirm). (Under the approved gate — the pass alone, Revision 3 — option A gives
about 0.81 × 0.57 ≈ 0.46 at a true 25 bp, unconditional volatility; 0.55 × 0.40 ≈ 0.22 at
pre-cascade volatility.) **At a true effect exactly at the bar, the
point-estimate gate alone halves the chance of reaching the holdout**
(about half of passing estimates fall below the true mean). Gaussian
noise: the real tails are fatter (kurtosis 8–11), so these are upper
bounds on power.

**Holdout size and pull.**

- At the discovery rate (0.635 primary majors positions per day), option
  A (holdout ends 2026-10-09, 101 days) gives **~64 positions**: null SE
  14 bp, CONFIRM needs a mean of about 23 bp; P(confirm) at a true 25 bp
  is 0.55–0.57 (unconditional volatility), 0.40 at pre-cascade
  volatility.
- Option B: the smallest holdout giving **P(confirm) ≥ 0.80 at a true
  25 bp and unconditional volatility** in the simulation is **126
  positions → 199 days → fixed end date 2027-01-15** (inclusive; the
  analytic t-only estimate also gives 126).
- **The holdout can refute a large effect; under A it has only about a
  coin-flip chance of confirming one at the bar.**
- **Pull size (estimate, not a quote).** The 338 discovery days of
  main-dex pruned fills are 34.0 GiB on disk, 0.094 GiB/day over the last
  90 days; P3's transfer ran ~1.23× its on-disk size (53 vs 43 GiB). For
  option A (101 days): **~9.5 GiB on disk, ~12 GiB requester-pays
  transfer**; for option B (199 days): ~19 GiB on disk, **~23 GiB
  transfer**. No listing or footer of a holdout day was read (the footers
  on disk for 2026-09-15 were not opened); the exact byte count needs a
  LIST and footer read — **to be quoted** to the owner before the pull.

## 4. T1 — the trade

For each `primary` cascade not qualified on the fallback median, in time
order per coin:

1. **Entry time** `t_e = t_end + 60 s + 2 s` — the run is closed once a
   minute passes without a liquidation print in the coin (the cascade gap
   G = 60 s), plus 2 s of latency. Never before.
2. **Stress switch (004 D8, own coin excluded):** skip if the
   exchange-wide trailing-5-minute liquidation notional **of every coin
   except the cascade's own** was ≥ **$77 M** at any second in
   [`t_start`, `t_e − 1 s`] (one-second buckets as in 004; a bucket holds
   [s, s + 1 s), so evaluating up to `t_e − 1 s` reads only prints before
   the entry).
3. **Entry price:** the first tape print at or after `t_e`, within 60 s
   (none → skip), **taker 0.045%, 5 bp adverse**.
4. **Direction:** against the forced flow — longs liquidated → buy;
   shorts liquidated → sell.
5. **One position per coin:** skip if `t_e` falls before an earlier
   position in the same coin has reached fill + 120 min. Skipped events
   hold nothing.
6. **Exit:** the tape's last trade at or before **fill + 120 min**
   (`tape.prices_at_ordered`), taker 0.045%, 5 bp adverse. Reported
   horizons 60 and 240 min (same entries, same overlap rule).
7. **Outcome** per position: net return
   `s · ((x·(1 − s·slip)) / (e·(1 + s·slip)) − 1) − 2 × 0.045%`,
   s = +1 buy / −1 sell (`ct/drift.net_return`); the gross return (no fee,
   no slippage) is kept for the placebo.

**Why "local" means the market outside the coin (D5).** The hypothesis is
about forced flow that the rest of the market can absorb: a single-coin
run while every other coin's liquidations sum to under $77 M in five
minutes is a coin-level dislocation with the market's counterparty
capacity intact, whereas a market-wide spiral is not "local" whatever the
coin's own share. Counting the coin's own flow made the switch remove a
coin's cascade for being large; excluding it re-admits the 28 largest
own-coin majors cascades (§3).

**Unhedged.** The hedged variant in H4's description is **dropped for the
primary**: BTC is the hedge instrument and cannot hedge itself, and
hedging only two of three coins would make the primary a mixture. Hedging
ETH/SOL with BTC is **reported only**: an opposite BTC position of β × the
notional, β the OLS beta of the coin's 1H log returns on BTC's over the
trailing 30 days of Bitget closes *before* the entry; BTC entered at its
first print at or after the coin's fill (60 s window), exited at its last
print at or before the coin's exit, taker with 5 bp adverse on both legs
(`ct/drift.beta`, `hedged_return`).

## 5. Pass — majors, all six

On the 120-min net returns of primary majors positions:

1. mean > 0 with **t ≥ 2.33, clustered by the UTC hour of the entry fill**;
2. **day-clustered t ≥ 1.96** (UTC day of the entry fill) — a two-hour
   hold spans hour boundaries, so neighbouring hour clusters are not
   independent;
3. mean and median the same sign;
4. positive in both halves (majors positions in time order, split by
   count);
5. positive in both liquidation sides (longs liquidated / shorts
   liquidated) separately — binding (see the disclosure above);
6. **mean > 0 without the hour cluster with the largest positive summed
   net return** — the hour most favourable to the hypothesis. Removing a
   large *loss* could only help a pass, so the loss hour is never the one
   removed; if no hour sums above zero nothing is removed. The blocks
   without the largest-by-count and the largest-absolute hour are
   reported.

**Placebo (calibration):** 200 seeds; in each, every **UTC-hour cluster's
gross returns get one random sign together, costs kept** (net' = ±gross −
cost); the statistic is criterion 1's hour-clustered t. Reported: the
share of seeds that would pass criterion 1 (calibrated if ≤ 5%; with costs
kept it should be near 0), and the share with |t| ≥ 2.33 on the flipped
gross alone (a symmetric check, ≈ 2% expected). The same with whole
**UTC days** flipped together is reported — it shows how far the
hour-clustered t understates day-level dependence.

## 6. Reported, decides nothing

Alts and all coins under the same six criteria; per coin (BTC, ETH, SOL);
horizons 60 / 240 min (hour- and day-clustered); exit slippage 0 / 10 bp;
the largest-by-count and largest-absolute hour removed; **the inclusive
002 event set** (warm-up events in) and **the inclusive stress series**
(own coin in), each under the six criteria in majors; **ETH/SOL hedged
with BTC** next to ETH/SOL unhedged; the day-flip placebo; counts skipped
by each filter.

**No reported block may become a primary on the discovery year.** Any
follow-up that one of them suggests — a horizon, a universe, a variant,
the 0-bp slippage block — is a **new trial**: it is added to the family
count and decided on the holdout only.

## 7. Known weaknesses, stated now

- **The fourteenth look** and the disclosures above: the horizon, the hold
  and the majors universe were chosen after 003 and 004 reported on the
  same cascades.
- **Power.** 212 positions, 161 hour clusters; the 80% MDE is ~25 bp at
  unconditional volatility and ~33 bp at pre-cascade volatility. At a true
  25 bp the six criteria pass in 55–81% of simulations and the whole path
  to a holdout confirmation succeeds in 20–42% (§3).
- **Fat tails.** 120-min returns have kurtosis 8–11 unconditionally; the
  clustered t and the power simulation are normal approximations; the
  2025-10-10 lesson is handled by the stress switch, criterion 6 and the
  day-clustered t, not by the t-statistic.
- **The stress threshold** is a percentile of this year (004 D8, not
  re-tuned); excluding the own coin changes which series it is applied
  to, not the number.
- **Exit and entry at the tape's print** with 5 bp adverse as the
  allowance; no book depth. At majors' depth a taker order of research
  size is small; 0 and 10 bp are reported. **The 0-bp slippage block is
  not a rescue path**: a fail at 5 bp is a fail.
- **Unhedged primary** (§4).
- **Bear year, 67% of primary majors positions on the longs-liquidated
  side** (81 L / 25 S in the second count half). Criterion 5 is the
  regime guard; the holdout (a rally) is the regime test.
- **Survivorship of the alt universe** (§2) — reported blocks only.
- **240-min overlap** (19 majors positions) and **43 events with a new
  run during the latency** — reported, unchanged.
- **Holdout rate.** Option B's date assumes the discovery rate of
  positions; a calmer or busier holdout gives fewer or more. The date is
  fixed, the n is not.

## 8. Built before any outcome is computed

- [x] `ct/drift.py` — entry time, warm-up filter, stress switch over
      [first print, entry − 1 s] with the own coin excluded, entry-print
      rule, one position per coin, holdout edge, net and gross returns,
      the six criteria (hour and day clusters, largest-positive-hour
      removal), the gross-flip placebo, decision table, the holdout read,
      hedge beta and hedged return, null-SE / MDE / power arithmetic and
      the joint-power simulation;
- [x] `ct/tape.prices_at_ordered` — new, deterministic last-print lookup
      for 005's exits; existing functions unchanged;
- [x] `tests/test_drift.py` (`.venv/bin/python -m tests.test_drift`, 70
      checks pass): costs exact; entry at flow end and not before; the
      stress switch evaluates only up to t_e − 1 s (the bucket
      [t_e − 1 s, t_e) counts, [t_e, t_e + 1 s) does not); poisoned
      non-entry fields change nothing; exit lookups at the holdout are
      refused; warm-up and own-coin rules; overlap; null fails and the
      gross-flip placebo is calibrated (pass share 0, |t| share 2%); the
      placebo flips an hour cluster jointly and keeps costs; planted
      +0.3% passes all six; planted +0.15% is "below the bar"; a planted
      crash hour does not pass; a planted loss hour is never removed; a
      one-sided drift fails criterion 5; day-correlated noise lowers the
      day t; holdout read; beta and hedged arithmetic; Cholesky and
      correlated draws reproduce the covariance; the simulation passes a
      large planted mean and rarely passes none; option B's date
      arithmetic; **the guard in a throwaway git repo**: untracked
      registration (a), staged but uncommitted (c), committed and clean
      (allowed), registration edited (b), guarded code edited (d)+(e),
      event file changed (e), result row present (f), status DRAFT;
- [x] `l7_run.py` — the run; `--events` prints counts only (primary and
      both inclusive variants). **The guard refuses** unless all of: status
      REGISTERED; (a) the registration is tracked by git; (b) unmodified vs
      HEAD; (c) it has a commit; (d) `git status --porcelain` is clean for
      the six code files below; (e) the files below match this table;
      (f) `results.jsonl` holds no row of this registration. Today it
      refuses on the status, (a), (c) and (d); the (e) hashes match;
- [x] `l7_describe.py` — the §3 numbers (behaviour only).

**SHA-256 of the code and data the outcome depends on** (read by the
guard; recomputed by the orchestrator at commit):

| file | sha256 |
|---|---|
| `ct/drift.py` | `4c15eae52c46b0374eff0f2531ca08d5602faf20fc49bebeaf375d7656601b85` |
| `l7_run.py` | `786410b77530e0778551939f3849c40d3af1b745b1630a57ca1b58dedd1f0473` |
| `ct/tape.py` | `ab803d7cf740cd6d25f229d5d7e5e4cc33933f8ed1209ccc10f0981dbd56bf0e` |
| `ct/eventstudy.py` | `08a780b4d40bad811d5d826d42d29c44d673246048d85cf31ec1be2eaf6018b0` |
| `ct/provider.py` | `22185c5f1ccb839257efc6025db929aea5283c686fca284a5e05492065c171d0` |
| `ct/liq.py` | `d03159c1963097d73536f17c7282c37c159784d418c1b55783e09051226bb292` |
| `data/derived/hyperliquid/cascades.json` | `b21f2df0d4a87387a1b49f9636cc6a0b94306c4286accef1a976a9e7d6639819` |

## 9. Owner decisions — approved 2026-10-10 (D14 option A; D11/D12 per Revision 3)

| # | item | proposed |
|---|---|---|
| D1 | events | `cascades.json` primary (002) **without the 426 events (152 majors) qualified on the cross-coin fallback median** (coin with < 5 days of own history, 003's rule): 212 majors positions. The inclusive 002 set is reported |
| D2 | entry | first tape print at or after t_end + 60 s + 2 s, within 60 s (else skip); taker 0.045%, 5 bp adverse |
| D3 | direction | against the forced flow: longs liquidated → buy, shorts liquidated → sell |
| D4 | hold / exit | 120 min primary, last print at or before fill + 120 min (deterministic ties), taker 0.045%, 5 bp adverse; 60 / 240 min and slippage 0 / 10 bp reported — the 0-bp block is not a rescue path |
| D5 | stress switch | trailing-5-min liquidation notional of all coins **except the cascade's own** ≥ $77 M (004 D8, not re-tuned) at any second in [first print, entry − 1 s]; re-admits 28 majors cascades, the largest own-coin ones; the inclusive series is reported |
| D6 | overlap | one position per coin; an event entering while the coin's earlier position is held (< fill + 120 min) is skipped |
| D7 | universe | majors (BTC, ETH, SOL) primary; alts and all coins reported |
| D8 | inference | hour-clustered (entry fill's UTC hour) t ≥ 2.33 and day-clustered t ≥ 1.96; placebo: gross sign flipped per hour cluster, costs kept, 200 seeds (per-day flips reported) |
| D9 | pass | the six criteria of §5; halves by count; criterion 6 removes the hour with the largest positive summed net return (by-count and by-absolute reported) |
| D10 | family | one trial here; cumulative fourteen on the discovery year, stated; no reported block becomes a primary on discovery (§6) |
| D11 | decision — pass (all six criteria) | ask the owner to approve the holdout pull (requester-pays, byte count quoted first, D15) and read it **once** under D14 |
| D12 | the H7 bar (25 bp net per major event) | not a gate on the holdout (Revision 3); a discovery pass is unconfirmed until the holdout; after a CONFIRM the bar is the pass criterion of the paper-period registration |
| D13 | decision — fail | **not detected at an MDE of 25 bp (unconditional volatility) / 33 bp (pre-cascade volatility), 80% power; H4 closes as a strategy candidate; H10 dropped** |
| D14 | holdout read (only after D11) | holdout = 2026-07-01 → a **fixed end date**, **chosen: A) 2026-10-09** (≈ 64 positions; P(confirm) at a true 25 bp 0.57, end-to-end 0.29) (not chosen: B) 2027-01-15 (≈ 126 positions, P(confirm) 0.80, end-to-end 0.42; a discovery pass waits for that date)). Cascades rebuilt by `l2_events.py`'s rule **with the trailing 30-day median seeded from the discovery liquidations up to 2026-06-30, so no holdout event qualifies on the fallback median**; same five filters, same trade, majors; read **once**. **CONFIRM if all of:** mean net > 0 with one-sided hour-clustered t ≥ 1.64; median > 0; mean ≥ 0 without the largest-positive hour (`ct/drift.holdout_confirm`). CONFIRM → H4 earns a registered forward **paper** period (no live capital; a new registration). Otherwise → H4 closes |
| D15 | holdout pull | owner approval with a byte count; estimate ~12 GiB transfer under A, ~23 GiB under B (§3); exact count to be quoted from a LIST + footers before any byte of a holdout day is read |
| D16 | hedged variant | dropped for the primary; ETH/SOL hedged with BTC reported (β from 30 days of 1H closes before each entry) |

The runner reads its parameters from `ct/drift.py`; any change to D1–D9
before registration is a change there and in this file together, and a
new hash table. The holdout read (D14) needs the holdout cascades and a
holdout mode in the runner, added by amendment before the pull, with the
criteria and end date above unchanged.

## 10. Result — 2026-10-10 (run of commit 56c2901, `results.jsonl` line 5)

One trial, hour-clustered z_crit 2.33, day-clustered 1.96. **Cumulative
across 001–005: fourteen trials on one discovery year, no pass.** Placebos
(gross sign flipped per hour / per day, costs kept): 0.0 / 0.0 pass share,
|t| ≥ 2.33 in 1.5% / 2.0% of seeds (calibrated). 212 primary majors
positions, 161 hour clusters, 114 day clusters, as counted in §3.

| trial | verdict | the numbers |
|---|---|---|
| T1 post-cascade drift, majors, 120 min | **FAIL** (criteria 1, 2, 4, 5, 6; criterion 3 holds only because mean and median are both negative) | mean **−0.10%** net per position, se 0.13%, **t −0.80** (day-clustered −0.73); median −0.09%; hit 45%. Longs liquidated (buy) −0.17% (t −0.94, n 142); shorts liquidated (sell) +0.04% (t 0.41, n 70). Without the largest positive hour −0.15% (t −1.19). |

**Decision (D13):** not detected at an MDE of 25 bp (unconditional
volatility) / 33 bp (pre-cascade volatility), 80% power. **H4 closes as a
strategy candidate; H10 is dropped. The holdout is not spent.**

**Diagnostics (reported, decide nothing; no block becomes a primary, §6).**

- **Gross of slippage the drift is about zero.** At 0 bp slippage majors
  are −0.4 bp net of the 9 bp fees, so roughly +9 bp gross over two hours,
  t ≈ 0. At 10 bp slippage, −20 bp. There is no cost level at which this
  would reach the 25 bp bar.
- **Horizons, majors:** 60 min −12.5 bp (t −1.30, hit 40%); 120 min
  −10.3 bp; 240 min −17.2 bp (t −1.00, median −2.7 bp). Nothing builds
  with the horizon once the entry is after the flow.
- **Per coin:** BTC −8.7 bp (n 57), ETH −16.8 bp (n 87), SOL −3.4 bp
  (n 68); none distinguishable from zero.
- **Alts and all coins** (reported): alts −12.9 bp (t −0.56, n 1,105,
  median −3.0 bp); all coins −12.5 bp (t −0.61). Alts at 60 min +2.1 bp.
- **Variants, majors:** the inclusive 002 event set −13.1 bp (t −1.22,
  n 261); the inclusive stress series −13.6 bp (t −0.98, n 190). Neither
  orchestrator revision changed the reading.
- **ETH/SOL hedged with BTC:** −47.1 bp (t −7.3, hit 24%) against −10.9 bp
  unhedged. The BTC leg costs about β (≈ 1.25) × 19 bp ≈ 24 bp. The rest,
  about −12 bp, is ETH/SOL underperforming BTC over the two hours after
  their own cascade: a mild relative continuation, not a reversion.

**Reading.** 003 T1 earned +22 bp in majors from a fill *at the median
forced print*, with the reversion growing to 240 min. Entered one minute
after the last forced print, the same cascades give nothing. The rebound
003 measured is the move from the forced print back to where the market
was trading within the cascade's own minute. It is paid to whoever was
filled during the flow, and it is gone by the time the flow is visibly
over. What remains after flow end is a two-hour directional bet with zero
mean in majors and a slight continuation on the longs-liquidated side in a
bear year. Combined with 004 (a realisable ladder inside the flow: −2 bp),
the forced-flow reversion on Hyperliquid has now been tested before,
during and after the flow, and only the ex-post fill at the median print
earns. The light is inside the cascade's seconds, where 006 (H3) looks
next.
