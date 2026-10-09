# 003 — Passive liquidity into forced flow: resting orders on Hyperliquid where the map says liquidations will land

**Status: DRAFT — not registered.** It becomes the registration in the commit
that changes this line to `REGISTERED <date>` after the owner has approved
every item in §10. Until then no forward return is computed for any event
or order.

Plan: `docs/copy-trade-plan-2026-10-07.md`. Prior ledger entries: `001`
(skill persists, no follower earns), `002` (no reversal on Bitget after a
cascade; the one kept fact: Hyperliquid's last liquidation print sits a
median 58 bp below Bitget's next-minute open after long cascades, 50 bp
above after short ones, ~19 bp for majors — the overshoot lives on the
venue of the forced fill, in the seconds of the fill). **Owner, 2026-10-09:
the goal outranks the venue rule** — execution on Hyperliquid, a different
stack, even a new repo, if that is where a consistent edge is.

**The idea.** A liquidation is a market order that sweeps the book. Whoever
has a resting order where it lands is paid the overshoot. The archive gives
every Hyperliquid fill (the tape), every liquidation fill, and once a day
every position's liquidation price (where the next forced orders sit). So
three questions can be asked point-in-time: does a resting order filled by
a cascade earn after the price settles (T1, the upper bound); does a resting
order placed each day at a fixed distance, only where the map shows forced
flow within reach, earn once *everything* that reaches it is counted —
cascades and ordinary declines alike (T2, the strategy); and is the map
what makes the difference (T3).

## 1. Trials registered here

| id | hypothesis | role |
|---|---|---|
| T1 | **Passive fill at the forced print**: a resting order filled at a cascade's median liquidation price, closed on the tape 15 min later as a taker, earns net of Hyperliquid fees | upper bound — if this fails, nothing below can work |
| T2 | **Map-placed resting orders**: a bid (ask) 2% below (above) the price at each daily snapshot, placed only where the cumulative liquidation notional between the price and the level is ≥ DENSE_MIN of the coin's book, alive until the next snapshot, filled by whatever reaches it, closed 15 min after the fill — earns conditional on a fill, with clustered inference | primary, the strategy |
| T3 | **The map matters**: among all orders at the same distance, the top density tercile earns more per fill than the bottom tercile | secondary |

Three trials. Family-wise one-sided α = 0.05 / 3 = **0.0167 (z ≥ 2.13)**.
Everything in §6 is reported and decides nothing.

**Written expectations.** T1: likely to pass — it conditions on a cascade
having happened and prices the fill at the median forced print; it measures
the overshoot 002 saw cross-venue, not a deployable edge. T2: about 1 in 3.
A resting order is filled by every seller who reaches it, and most declines
through a level are not forced; adverse selection is the whole question.
T3: 1 in 3; depends on T2 having fills on both sides of the density split.

## 2. Data and periods

- **Discovery: 2025-07-28 → 2026-06-30.** Holdout (2026-07-01 →) unread.
- Tape: every archive fill of the coin (`ct/tape.py`): last trade before a
  time, first trade at or beyond a level inside a window, last trade at or
  before a time. Point-in-time by construction.
- Cascades (T1): `cascades.json` primary set from 002 (G 60 s, K 1.0,
  F $250 k; 2,838 events; 82% longs liquidated). Their liquidation fills
  from `liquidations.parquet`.
- Map levels (T2, T3): `map_levels.parquet` (`l4_levels.py`): per snapshot,
  Bitget-mapped coin, side and distance d ∈ {1%, 2%, 3%}: P0 = last HL
  trade before the snapshot, level = P0 × (1 ∓ d), density = liquidation
  notional of positions on that side between P0 and the level plus 0.5%
  beyond, over the coin's gross notional. 45,155 coin-snapshots, 286
  snapshots (the 2025-10-25 → 12-14 gap has none: no orders there).
- Fees: Hyperliquid base tier, confirmed 2026-10-09 — maker 0.015%, taker
  0.045%.

## 3. Thresholds — from distributions only (`data/scratch/l4.log`)

- Within 2% of price the map is **empty for most coins**: the 90th
  percentile of cumulative forced flow is 0.02% of the book; BTC/ETH/SOL
  median 0.95% (longs) / 0.41% (shorts); at 3% the majors reach 1.8%.
  Liquidation prices sit far from price because leverage is low on average.
  So the strategy places **only where the map shows forced flow within
  reach**: **DENSE_MIN = 0.5%** of the book within the 2% level (about the
  majors' typical value); the **sparse control** is density < 0.05%.
- Gross notional per coin-snapshot: deciles $0.3 M … $35.6 M.
  **GROSS_MIN = $5,000,000** (about the 70th percentile) keeps dust coins
  out — a $10,000 resting order in a $300 k book is not a passive fill, it
  is the book.
- Distance **d = 2% primary**; 1% and 3% reported.
- Exit **15 min after the fill**, primary; 5, 60, 240 min reported. A
  passive fill is paid for the overshoot, which 002 measured as closing
  within the first minutes; 15 min is long enough for the tape to settle
  and short enough to be the fill's own effect.

## 4. T1 — passive fill at the cascade print

For each cascade: fill price = the **notional-weighted median** of the
cascade's liquidation fill prices (half the forced notional traded at or
beyond it — a resting order there is filled with certainty and not at the
bottom tick). Entry as maker (0.015%). Exit at the tape's last trade at
*t_end + 15 min*, as taker (0.045%) adverse by **5 bp** (0 and 10 bp
reported). Outcome: net return, signed so that earning is positive.
Standard errors clustered by **UTC hour** of the cascade end.

**Pass, all four:** (1) mean > 0 with clustered t ≥ 2.13; (2) mean and
median the same sign; (3) positive in both halves of the period; (4)
positive in majors and in the rest separately. **Placebo:** direction
shuffle, |t| ≥ 2.13 in ≤ 5% of 200 seeds. **Reported with it:** the mean
without the largest hour cluster (the 2025-10-10 lesson), horizons, sides.

## 5. T2 — map-placed resting orders; T3 — the map's value

**Orders (T2):** one per (snapshot, coin, side) at d = 2% for coins with
gross ≥ GROSS_MIN and density ≥ DENSE_MIN. Alive from the snapshot until
the next snapshot (at most 24 h). **Filled** at the first tape print at or
beyond the level inside the window (a bid fills when a trade prints at or
below it). Entry as maker at the level; exit at the tape's last trade 15
min after the fill, taker, 5 bp adverse. Unfilled orders return 0 and
count in the EV, not in the conditional test.

**Pass (T2), the same four criteria as T1** on the **conditional** returns
of filled orders, clustered by the fill's UTC hour. Reported: fill rate,
**EV per order** (the deployable number), by side, by distance, majors.

**T3:** all orders at d = 2% with gross ≥ GROSS_MIN, no density filter,
split into density terciles **within snapshot**; the clustered difference
top − bottom in conditional return ≥ 2.13 with terciles ordered, and T2
passing. Reported: fill rate and EV per tercile — the map's value is in
both: who gets filled, and what the fill is worth.

## 6. Reported, decides nothing

Horizons 5 / 15 / 60 / 240 min; exit slippage 0 / 5 / 10 bp; distances
1 / 2 / 3%; sides; majors vs rest; by monthly regime label; the largest
cluster removed; EV per order by density tercile; fill time of day; T1's
fill price relative to the cascade's last print (how much of the bottom
tick the median fill gives up).

## 7. Known weaknesses, stated now

- A resting order's fill is modelled as "a trade printed at or beyond the
  level". Queue position, partial fills and the order's own impact are not
  modelled; at GROSS_MIN this is a small order in a large book.
- Exit at the tape's last trade is a proxy for a taker fill; 5 bp adverse is
  the allowance, with 0 and 10 reported.
- The map is up to a day old. T2 tests the stale map, as a live system
  would have it at the snapshot cadence; a live map would be fresher.
- Orders are capital at rest for a day for a fill rate that may be low; EV
  per order is the number that carries that cost.
- One year, bear-heavy, with 82% of cascades on the long side.

## 8. Built before any outcome is computed

- [x] `ct/tape.py` — point-in-time tape lookups;
- [x] `l4_levels.py` — map levels at fixed distances (`map_levels.parquet`)
      and the §3 distributions;
- [x] `ct/passive.py` — fill price, net return, EV (`tests/test_passive.py`);
- [x] `l5_run.py` — the run with the 001/002 guard
      (`tests/test_l5_assembly.py`: null fails, planted +0.2% passes all
      four, a planted crash hour does not pass and the without-largest-
      cluster block is ~0). Guard refuses: not REGISTERED.

## 9. Owner decisions before this becomes REGISTERED

| # | item | proposed |
|---|---|---|
| D1 | T1 fill price | notional-weighted median of the cascade's liquidation prints |
| D2 | distance | 2% primary; 1% and 3% reported |
| D3 | GROSS_MIN | $5,000,000 gross notional per coin-snapshot |
| D4 | DENSE_MIN / sparse control | 0.5% of the book within the level / < 0.05% |
| D5 | horizon | 15 min after the fill primary; 5, 60, 240 reported |
| D6 | costs | maker 0.015%, taker 0.045%, exit 5 bp adverse (0 / 10 reported) |
| D7 | order life | snapshot to next snapshot, at most 24 h |
| D8 | clustering | UTC hour of the cascade end (T1) / of the fill (T2, T3) |
| D9 | family | 3 trials, α 0.0167, z 2.13 |

`GROSS_MIN = $5,000,000` and `DENSE_MIN = 0.005` are read by the runner from
this file.
