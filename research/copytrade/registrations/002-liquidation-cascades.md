# 002 — Liquidation cascades: does forced flow overshoot, and does the liquidation map say where?

**Status: REGISTERED 2026-10-09** (owner: "approved", all of D1–D9 as
proposed in §10). This commit is the registration: thresholds set from
distributions only, every builder checked on synthetic data, prices for the
event windows prefetched, no forward return computed for any event before
it. Any later change is an amendment, allowed only before an outcome exists.

Plan: `docs/copy-trade-plan-2026-10-07.md`. Prior ledger entries: `000`,
`001` (result in its §12: skill persists on Hyperliquid, but no follower
earns; the one measured positive was multi-day drift, and the one strong
mechanism in the data is forced flow). Owner, 2026-10-09: "let us see if
there is a light at the end of that tunnel first."

**Why this and not another copy test.** A liquidation is a market order
nobody chose to send at that price. When many hit one coin inside minutes,
price moves past where willing traders would take it, and the literature on
forced selling (fire sales, margin spirals, index-reconstitution flow) says
it tends to come back. The archive gives two things the public estimates do
not: the exact liquidation fills of every account (`is_liquidation`), and,
once a day, every open position's liquidation price — the map of where the
next forced orders sit. Execution needs a one-minute poller, not a copier.

**Erratum (2026-10-10, from H7, `docs/hlp-benchmark-2026-10-10.md` §2).**
$2.03 B of the `Liquidated Cross` rows in `liquidations.parquet` are HLP's
own liquidator positions being auto-deleveraged on 2025-10-10 (counterparty
row `Auto-Deleveraging`), not users being liquidated. That hour's cascade
notional and the market-stress series carry about $2 B of HLP's own ADL.
No verdict changes: the hour is far above every threshold either way, and
it was already treated as one cluster.

## 1. Trials registered here

| id | hypothesis | role |
|---|---|---|
| T1 | **Reversal**: after a liquidation cascade in a coin, Bitget's price moves back against the cascade's direction over the next hour, net of taker fees and cascade-minute slippage | primary |
| T2 | **Map**: cascades that run into a dense region of the latest liquidation map overshoot and revert more than cascades into sparse regions — the T1 trade earns more in the top density tercile than in the bottom | secondary |

Two trials. Family-wise one-sided α = 0.05 / 2 = **0.025 (z ≥ 1.96)**.
Everything in §9 is reported and decides nothing.

**Written expectations.** T1: the owner's and the reviewer's prior is about
1 in 2 that a significant net reversal exists at 60 minutes in discovery,
lower that it survives the holdout. Continuation (not reversal) is the
plausible alternative and is reported as such, never flipped into a claim.
T2: 1 in 3. If T1 fails, T2 is reported but cannot pass.

## 2. Data and periods

- **Discovery: 2025-07-28 → 2026-06-30** (archive fills and snapshots
  already local; this hypothesis family has never been tested on it).
  **Holdout (2026-07-01 →) stays unread**; a pass here leads to a holdout
  registration, read once (spec invariant 4).
- Liquidation fills: `data/derived/hyperliquid/liquidations.parquet`
  (`l1_liquidations.py`): every archive fill flagged `is_liquidation` — the
  liquidated account's fill (direction `Close Long` sold / `Close Short`
  bought), with its own time, size and HL price. No forward price.
- Open interest proxy: `oi_by_coin_snapshot.parquet` — per snapshot and
  coin, gross notional (≈ 2 × open interest), long and short notional,
  position count. As-of the latest snapshot strictly before the event. No
  snapshots 2025-10-25 → 12-14: events there have no OI and no map (kept in
  T1 with the fallback in §4, excluded from T2; counts reported).
- Liquidation map: the raw snapshot's `liquidation_price`, `size`,
  `notional` per position, read per event day.
- Prices: Bitget 1-minute candles (`ct/bitget.py`), prefetched for every
  event's window before registration (`l2_blocks.py`, counts only); 1H
  candles for the BTC beta; coins only after their first Bitget daily candle.
- Regimes: `ct/regimes.month_label` (UP / DOWN / FLAT, hindsight) for
  reporting splits.

## 3. Event definition — thresholds from distributions only

A **cascade** in coin *c* is a maximal run of liquidation fills in *c* where
consecutive fills are ≤ **G** apart, the dominant direction carries ≥ **P**
of the run's notional, and the run's total liquidation notional is ≥ **K ×
the coin's trailing median** daily liquidation notional over the 30 days
before the run (coins with no history use the cross-coin median) **and** ≥
a floor of **F** dollars. The run's **direction** is that of the liquidated
positions (`Close Long` = longs liquidated = the price fell). Its **start**
is the first fill, its **end** *e* the last fill. A live poller only knows
the run ended when **G** passes without a liquidation, so the **detection
time** is *e + G* and the **entry** is the open of the Bitget minute after
the first poll at or after *e + G* (poll grid 1 min — the same convention as
the follower in 001).

G, P, K, F are set from the distributions in `data/derived/l1_describe.txt`
(inter-fill gaps, run sizes, direction purity, notional per run — **no
return anywhere**). What they show (2026-10-09): 4.56 M liquidation fills,
$40.2 B, 200 coins; liquidations arrive in bursts — 90% of fills within a
second of the previous one in the same coin, the 95th percentile gap 21 s,
BTC's 99th 80 s; runs are one-directional by nature (purity ≥ 0.8 for 98% of
runs at any G); a run's notional relative to the coin's trailing median
*daily* total has p90 1.7 and p99 57 — a few cascades make a day.
**Proposed: G = 60 s, P = 0.8, K = 1.0 (the run liquidates at least a
typical whole day of the coin), F = $250,000.** Built by `l2_events.py`
(event times, sides and sizes only): **2,838 events** over 338 days (8.4 a
day) in 112 Bitget-listed coins, 463 in BTC/ETH/SOL; 426 use the cross-coin
fallback median (coins with under 5 days of history); 123 events in coins
not yet listed on Bitget were dropped. **82% are longs being liquidated**
(2,329 vs 509) — the discovery year was a bear — so the by-side split in §6
matters and T1's pooled result is dominated by down-cascades. Robustness
grid, reported: K 0.5 (3,794 events) / 2.0 (2,169), F $50 k (7,116) / $1 M
(1,232).

**Direction labels.** Longs liquidated = `Close Long`, `Liquidated Cross
Long`, `Liquidated Isolated Long` (the price fell); shorts liquidated = the
`Short` forms. The `Liquidated Cross` rows are few (46 k) but large
($4.5 B): backstop liquidations, kept.

## 4. T1 — reversal

For every cascade in a Bitget-listed coin, one trade: **against** the
cascade (long after longs were liquidated, short after shorts), entered at
the entry minute's open, exited at the open of the minute **h** after entry,
h = **60 min primary**; 5, 15, 240 min and 24 h reported. Equal notional
per event.

**Costs:** Bitget taker 0.06% per side; slippage **10% of the execution
minute's high–low range** per side — cascade minutes have wide ranges, so
this is the honest penalty; 0% and 25% reported. No funding (an hour).

**Outcome:** net return per event, signed so that reversal is positive.

**Dependence:** cascades cluster (a BTC cascade fires alt cascades in the
same minutes). The unit is the event; standard errors cluster by **UTC
hour**; clustering by day is reported.

**Pass, all four:**
1. mean net return at 60 min > 0 with hour-clustered t ≥ 1.96;
2. mean and median have the same sign (the magnitude lesson from the spec);
3. positive in both halves of the discovery period;
4. positive in **majors (BTC, ETH, SOL) and in the rest separately** —
   a reversal only in illiquid alts is slippage we cannot capture, one only
   in BTC is a market effect; both are reported, the pass needs both.

**Placebo (synthetic null, §9):** a **direction shuffle** — the same
events and returns with each event's sign randomised — must reach |t| ≥
1.96 in ≤ 5% of 200 seeds (it asks whether the cascade's direction carries
the information; random *minutes* would need prices outside the prefetched
windows, and a shifted window inside them would be contaminated by the
event itself). A planted reversal of 0.1% must be found on synthetic rows.

**Fallback for the snapshot gap:** events in the gap use the cross-coin
median daily liquidation notional of the surrounding 30 days for K.

## 5. T2 — the map

For each cascade with a snapshot before it: **density** = liquidation
notional of positions on the liquidated side whose `liquidation_price` lies
within **±1%** of the cascade's start price (longs with liquidation below
the price for a down cascade; shorts above for an up cascade), divided by
the coin's gross notional in that snapshot. Events are split into terciles
of density within each calendar month (density scales differ by coin and
period).

**Pass:** mean T1 net return (60 min) of the top tercile minus the bottom
tercile > 0 with hour-clustered z ≥ 1.96, and the ordering top > middle >
bottom.

Reported: density as a *predictor of cascade occurrence* (does a dense
region within 2% of the day's open price raise the probability of a cascade
that day?) — descriptive, decides nothing; the snapshot's age at each event.

## 6. Reported, decides nothing

Returns by horizon (1, 5, 15, 60, 240 min, 24 h); by coin tier; by cascade
size quintile; by direction (longs vs shorts liquidated); by monthly regime
label; BTC-hedged variant (holdings beta from `ct/venue.beta`); slippage
0% / 25%; the HL-vs-Bitget gap at the event (Bitget entry price vs the
cascade's last HL fill price, in bp — the cross-venue risk); detection
latency sensitivity (entry at e + 2G and at e + 5 min); event counts per
day and the share of events inside the snapshot gap; continuation statistics
if the sign is negative, stated as "continuation", never as a tradeable
claim.

## 7. Known weaknesses, stated now

- The map is up to a day old (longer across the snapshot gap); margin adds
  and closes move liquidation prices intraday. T2 is a test of the stale
  map, which is also what a live system would have.
- Bitget is not Hyperliquid: Bitget runs its own cascades and its price can
  gap from HL's during one. The HL-vs-Bitget gap is reported; the trade is
  evaluated at Bitget prices only.
- Live detection needs the liquidation stream in real time (Hyperliquid
  trades feed or node data); the one-minute poll plus G is the modelled
  latency. Faster infrastructure is a later owner decision, not a parameter
  here.
- Events are not independent across coins in the same minutes; hour
  clustering is the primary correction and may still understate.
- One year, bear-heavy; the holdout adds one rally.

## 8. Simulation conventions

Same as 001 where they apply: `ct/bitget.candle_at` for execution minutes,
fills at the next minute's open, adverse by the slippage fraction of that
minute's range, Bitget lot step and $5 minimum at a **$10,000** nominal
notional per event (the paper size reported at **D8**), coins only after
listing. No leverage cap needed (one position at a time per coin).

## 9. Built before any outcome is computed (prepare phase)

Each with a synthetic test, none computing a forward return on real events:
- [x] `l1_liquidations.py` — extraction (4.56 M fills, 40 s); `l1_describe.py`
      — distributions for §3 (`data/derived/l1_describe.txt`);
- [x] `ct/liq.py` — `cascades`, `trailing_median_fn`, `density`
      (`tests/test_liq.py`: planted cascades found with side, size, bounds;
      a higher K removes the smaller; random background gives none; a
      planted liquidation cluster is measured by `density`);
- [x] `ct/eventstudy.py` — per-event net return with costs, hour-clustered
      t (≈5% false positives on independent noise and on 50 clusters of
      duplicates; 57% if duplicates are treated as independent), direction
      placebo, planted 0.1% reversal found, tercile split (planted density
      effect z 3.5, null z 0.2) — `tests/test_eventstudy.py`;
- [x] `l2_events.py` — events at the grid (`cascades.json`); `l2_blocks.py` —
      19,571 blocks needed for the event windows, 16,215 already cached,
      3,356 fetched 2026-10-09 (from event times only).
- [x] `l3_run.py` — the run, same guard as `p5_run.py` on this file
      (`tests/test_l3_assembly.py`: null fails, planted reversal passes all
      four, planted density effect passes T2, continuation is labelled and
      fails, T2 cannot pass without T1). The guard refuses: not REGISTERED.

## 10. Owner decisions — approved 2026-10-09 as proposed

| # | item | approved |
|---|---|---|
| D1 | G (run gap) | 60 s (§3: 95th-percentile gap 21 s, BTC 99th 80 s) |
| D2 | P (direction purity) | 0.8 (non-binding guard: 98% of runs) |
| D3 | K and F (size thresholds) | K = 1.0 × trailing 30-day median daily notional, F = $250 k → 2,838 events; robustness K 0.5 / 2.0, F $50 k / $1 M |
| D4 | primary horizon | 60 min |
| D5 | costs | taker 0.06% + 10% of minute range per side |
| D6 | clustering | UTC hour primary, day reported |
| D7 | density band | ±1% of start price, terciles within month |
| D8 | paper notional | $10,000 nominal; paper size reported |
| D9 | family | 2 trials, α 0.025, z 1.96 |

## 11. Result — 2026-10-09 (run of commit 7611ace, `results.jsonl` line 2)

Two trials, z_crit 1.96; 2,836 events with an entry price; 977 hour
clusters. Placebo share 0.045 (calibrated). The pre-registered reading:

| trial | verdict | the numbers |
|---|---|---|
| T1 reversal at 60 min | **FAIL** (criterion 1) | mean +1.87% per event, se 1.81%, **t 1.04** (day-clustered 1.19); median +0.10%; halves +3.65% / +0.10%; majors +0.001% (t 0.02, n 463), rest +2.24% (t 1.05). Three of four criteria passed on sign alone; the one with inference did not. |
| T2 map density | **FAIL** | terciles ordered the wrong way: dense 0.72%, middle 2.17%, sparse 2.79%; z −1.09. Snapshot age median 16 h. |

**What the numbers are made of (diagnostics, no criterion changed).**
The mean is one hour of history. Every one of the eight largest returns is
dated **2025-10-10 21:23 UTC**, the crash in which Bitget alts printed
wicks (RENDER entry $0.56, exit $2.08 an hour later: +264%; DYDX +229%,
FLOKI +219%, TIA +199%). Hour clustering treated those 155 events as one
observation, which is why the t is 1.04 against a mean of 1.9%. **Without
that hour the 60-minute mean is −0.02% (t −0.10, n 2,681)**; the
winsorised (±20%) mean is +0.39% with t 0.91; the hit rate is 53.4% with a
clustered z of 1.65 against 50%. In majors there is nothing at any horizon
(60 min mean 0.00%, median −0.08%, hit 47%; 24 h −0.8%, continuation in a
bear). Shorts-liquidated cascades show mild continuation (−0.22%, t −1.3).
Latency (2G, 5 min) and the robustness grid change nothing. The typical
path net of costs: −0.17% at 1 min, −0.08% at 5, −0.05% at 15, +0.10% at
60, +0.08% at 240, −0.09% at 24 h — a bump inside the cost band.

**The one fact worth keeping.** The gap between Hyperliquid's last
liquidation fill and Bitget's next-minute open has a median of **+58 bp**
after long-liquidation cascades and **−50 bp** after short ones (majors
±19 bp). The overshoot is real, but it lives on Hyperliquid in the seconds
of the forced fill, and half a percent of it is gone before a Bitget order
can exist. Capturing it means being the liquidity the liquidator hits —
resting orders on Hyperliquid at the levels the map marks — which is a
different product, a different venue, and an owner decision against the
standing rule that execution stays on Bitget / Capital.

**What follows.** No pass, so the holdout stays sealed. The crash hour is
a reminder for every later test: cluster, and report the mean without the
largest cluster.
