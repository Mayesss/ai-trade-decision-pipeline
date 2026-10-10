# 004 — Forced-flow liquidity provider on Hyperliquid: ladder at depth, hold only forced fills, exit when the flow ends; and the cross-venue capture

**Status: REGISTERED 2026-10-10** (owner: "approved", D1–D10 as proposed
in §9). This commit is the registration: architecture from 001–003's
results, thresholds from behaviour-only distributions, mechanics checked on
synthetic data, no forward return computed for any order before it. Any
later change is an amendment, allowed only before an outcome exists.

Plan: `docs/copy-trade-plan-2026-10-07.md`. Prior ledger entries `001`–`003`
(eleven trials on this discovery year). Owner, 2026-10-09/10: no knob
tuning; change the logic and the architecture.

**What 001–003 established** (their §11/§12/§10): a fill by forced flow at
the depth of the cascade earns — in majors +22 bp per fill at 15 min, t 3.3,
growing for hours; a resting order filled by any other flow 1–3% away loses
14 bp, t −8; an order posted at the cascade's onset fills too early and
loses to the continuation; one exchange-wide hour (2025-10-10 21:00 UTC)
dominates every mean; Hyperliquid's forced print sits 19–58 bp beyond
Bitget at the same moment. **This registration tests the architecture
those facts imply**, not a new parameter value.

**Run note (2026-10-10, no change to any rule).** The first start (after
the REGISTERED commit) built 3,805 triggers, filled 2,100+ rungs over 312
days and crashed in the shared statistics function before any statistic
was computed, printed or written: `summarise_trial` reads every horizon's
return and 004's rows carry the primary one only (`KeyError: '5m'`). Fixed
by reading missing horizons as None, which the clustered statistic already
drops; the synthetic assembly checks pass unchanged. `results.jsonl` has no
004 line. Restarted.

## 1. Trials registered here

| id | hypothesis | role |
|---|---|---|
| T1 | **The provider**: at the trigger, a ladder of resting rungs at 0.5 / 1.0 / 1.5% beyond the cascade's first print, sizes 1 : 2 : 3, alive until the forced flow has ended; a fill is held only if a liquidation printed within a second of it, otherwise scratched at the next print; held fills exit when the flow has ended for 10 min, or at the reference price, or after 4 h; no new orders while the exchange-wide liquidation rate says spiral. **Majors primary**, alts reported | primary, the strategy |
| T2 | **Cross-venue capture**: each forced fill on Hyperliquid is hedged on Bitget at the next minute's open and both legs are unwound 15 min later — the dislocation is captured, not the reversion | primary, the second mechanism |

Two trials, family-wise one-sided α = 0.025 (z ≥ 1.96). **Cumulative
across 001–004: thirteen trials on one discovery year**; a pass here is a
reason to read the holdout, not a result.

**Written expectations.** T1 majors about 1 in 2; alts lower (tails). T2
about 1 in 3 in majors (the gap was 19 bp against ~13 bp of costs) and
better in alts (58 bp), where Bitget's own book is the risk.

## 2. Data and periods

Discovery 2025-07-28 → 2026-06-30; holdout unread. Tape and liquidation
fills as in 003; Bitget 1-minute candles for T2 prefetched for every
trigger window (`l6_blocks.py`: 2,427 blocks, 27 fetched 2026-10-10).
Fees: Hyperliquid maker 0.015%, taker 0.045%; Bitget taker 0.06%, slippage
10% of the minute's range.

## 3. What was measured before writing this (behaviour only, no return)

- **Who fills a resting order.** For always-on orders 1–3% away, the trade
  that crosses the level is a liquidation print in under 1% of fills (2.9%
  in majors). For orders posted at the 003 trigger, a liquidation prints
  within 1 s of the fill in 54–68% of fills and within 5 s in 81–92%
  (majors 74–84% / 98–99%). Forced flow is separable only during
  cascades — always-on provision is not revived here; the counterparty
  check is a filter on triggered fills, and 003's T2 (fills ~85% forced
  within 5 s, mean −0.71%) says it is not the main lever: **depth is**.
- **Ladder reach.** Beyond the first print, 72% of qualifying cascades
  reach 0.5%, 51% reach 1.0%, 39% reach 1.5% (majors 54 / 30 / 20%),
  a median 21 / 30 / 35 s after the first print; the trigger fires a median
  16 s in, so the deep rungs are posted before they are reached.
- **Market stress.** Exchange-wide liquidation notional per 5-min bucket:
  p99 $15 M, p99.9 $77 M. At p99 the switch would stand aside for 30% of
  triggers and 41% of majors' — most of the market's forced flow happens
  when the whole market is liquidating; at **p99.9 it stands aside for 8%**
  (341 triggers), the spiral days (2025-10-10, 2025-08-01, 2025-08-18,
  2025-08-25). p99.9 is the primary; p99 is reported.
- **Scale.** 3,805 triggers survive (660 in majors), 10,032 rungs, over
  312 trading days.

## 4. T1 — the provider

Trigger as in 003 (trailing-60 s liquidations ≥ max($100 k, 0.5 × the
coin's median day), 15-min cooldown, gross ≥ $5 M). Reference = the first
liquidation print of the trailing window. Rungs at 0.5 / 1.0 / 1.5% beyond
the reference against the flow, sizes 1 : 2 : 3, rungs already passed by
the trigger print skipped (they would cross the book). Posted 2 s after the
trigger; **alive until 60 s after the last liquidation print of the run**
(the live system knows the run ended when a minute passes without a
print), at most 300 s.

Fill = first tape print at or beyond the rung. **Counterparty check**: a
liquidation print in the coin within ±1 s of the fill → held; else
**scratched** at the print 2 s later (taker, 5 bp adverse). Held fills
**exit** at the earliest of: 10 min after the run's last liquidation
print; the first tape print back at the reference; 4 h after the fill —
taker, 5 bp adverse. Outcome per rung, size-weighted.

**Pass (majors), all four:** (1) size-weighted mean > 0, clustered by the
fill's UTC hour, t ≥ 1.96; (2) mean and median the same sign; (3) positive
in both halves; (4) positive in both liquidation sides (longs liquidated /
shorts liquidated — the regime guard: a bear year favours one side).
Placebo: direction shuffle. **Reported:** all coins and alts under the same
four; EV per unit of posted size; fill and held shares by rung; the
no-check variant (hold everything, fixed 15-min exit); fixed 15 and 60-min
exits; the largest hour cluster removed; the p99 stress variant.

## 5. T2 — cross-venue capture

For every **held** fill in T1: sell (buy) the same notional on Bitget at
the open of the minute after the fill, adverse by 10% of that minute's
range, taker 0.06%; 15 min later unwind both legs — Hyperliquid at the
tape's last trade (taker, 5 bp), Bitget at that minute's open (same
slippage, taker). Outcome = both legs on the Hyperliquid notional.

**Pass (majors), the same four criteria.** Reported: all coins and alts;
the two legs separately (does the gap close toward Bitget, or does Bitget
catch down?).

## 6. Known weaknesses, stated now

Queue position and partial fills are not modelled; at gross ≥ $5 M the
order is small against the book. "A liquidation print within a second" is
a proxy for "my counterparty was the liquidation engine"; live, the trades
feed gives the counterparty address directly. Exit prices are the tape's
last trade. The reference-price exit is path-dependent and is what a
provider would do. The stress threshold is a percentile of this year.
One bear year; the holdout decides.

## 7. Built before any outcome is computed

- [x] `ct/provider.py` — ladder, exit rule, counterparty outcome, stress
      switch, hedged capture (`tests/test_provider.py`);
- [x] `ct/tape.first_touch_details` — the crossing print with its flag;
- [x] `l6_run.py` — the run (guard as before; `--triggers` prints counts
      only); `l6_blocks.py` — Bitget blocks for T2. Statistics reuse
      `l5_run.summarise_trial` (synthetic checks in
      `tests/test_l5_assembly.py`). The guard refuses: not REGISTERED.

## 8. Reported, decides nothing

Everything listed under "Reported" in §4 and §5; by rung; by side; by
monthly regime label; triggers skipped by the stress switch and by gross.

## 9. Owner decisions — approved 2026-10-10 as proposed

| # | item | approved |
|---|---|---|
| D1 | rungs and sizes | 0.5 / 1.0 / 1.5% beyond the first print, sizes 1 : 2 : 3 |
| D2 | order life | until 60 s after the run's last liquidation print, at most 300 s; posted 2 s after the trigger |
| D3 | counterparty check | hold if a liquidation printed within ±1 s of the fill, else scratch at the print 2 s later |
| D4 | exit | earliest of flow end + 10 min, reference touch, 4 h cap |
| D5 | universe | majors primary; all coins and alts reported |
| D6 | costs | HL maker 0.015% / taker 0.045%, exit 5 bp adverse; Bitget taker 0.06%, 10% of range |
| D7 | clustering | fill's UTC hour |
| D8 | stress switch | exchange-wide 5-min liquidation notional ≥ $77 M (p99.9); p99 ($15 M) reported |
| D9 | family | 2 trials here, α 0.025, z 1.96; cumulative 13 stated |
| D10 | T2 hold | 15 min, both legs |

## 10. Result — 2026-10-10 (run of commit 05c820e, `results.jsonl` line 4)

Two trials, z_crit 1.96; cumulative thirteen across 001–004. Placebos
0.035 / 0.045 (calibrated). 3,805 triggers, 10,036 rungs posted, 2,138
filled (21%), 63% of fills forced by the ±1 s rule and held.

| trial | verdict | the numbers |
|---|---|---|
| T1 provider, majors | **FAIL** (all four) | size-weighted mean **−0.02%** per fill, se 0.10%, t −0.24; median +0.03%; hit 52%; halves −0.03% / −0.02%; longs liquidated −0.04%, shorts +0.05%. **EV per unit of posted size −0.003%** (majors), −0.038% (all coins). All coins −0.21% (t −1.0); alts −0.23%. |
| T2 cross-venue capture, majors | **FAIL**, strongly negative | mean **−0.43%**, se 0.07%, **t −5.9**; hit 18%; both halves negative; all coins −0.47% (t −5.3). Legs: Hyperliquid −0.25%, Bitget −0.19%. |

**Reading.**

1. **Depth at fixed distances does not reproduce the 003 edge.** 003's T1
   filled at the cascade's notional-weighted median print and earned +22 bp
   in majors; that price is deep in proportion to the cascade's eventual
   size, which is unknown when the ladder is posted. Fixed rungs fill early
   in big cascades and the deeper the rung, the worse: −0.12% at 0.5%,
   −0.19% at 1.0%, −0.28% at 1.5% (all coins) — filling the 1.5% rung
   selects the cascades that keep going. The edge measured in 003 is an
   ex-post construct; the realisable ladder is zero in majors after costs.
2. **The counterparty check helps but is not the lever.** Majors with the
   check and the flow-end exit: −0.02%; without it, fixed 15 min: −0.21%.
   It removes the worst third of fills; it does not create an edge.
3. **The exit is too early.** A fixed 60-minute exit in majors gives
   +0.16% (t 1.46, median +0.18%, hit 58%) against −0.02% for flow end +
   10 min — consistent with 003, where the reversion built for hours. Not
   significant, and a longer hold is a bigger directional bet.
4. **The cross-venue gap is not capturable at minute granularity.** 002
   measured Hyperliquid's *last* print against Bitget's *next-minute* open.
   Hedging at the rung fill, which happens during the cascade, sells Bitget
   while Bitget is still falling and holds Hyperliquid while it continues
   down: both legs lose, t −5.9. Whatever gap exists lives in the seconds
   around the forced print and needs sub-second hedging that this data
   cannot model and this stack does not have.

**State of the question.** Four registrations, thirteen trials, one
discovery year, no pass. What survived every test: forced flow overshoots
on the venue where it lands, for seconds to minutes; a resting order
filled by anything else loses; and no placement rule computable in advance
on this data captured the overshoot after costs. The one untested design
is a *trailing* ladder that follows the print down while liquidations
keep printing and fills where the median forced notional actually lands —
which is what a live market maker does with queue position and counterparty
identity that the archive does not record. That is a live-data question.
The holdout stays unread.

**Reproducibility note.** Same-millisecond liquidation prints were not
ordered deterministically in the per-coin queries; the second start posted
10,036 rungs against 10,033 on the first. Ordering is pinned in the
following commit; it changes no result.
