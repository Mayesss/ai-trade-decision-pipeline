# Forced-flow research: results of registrations 001–004 and the horizons they open

**Written 2026-10-10, to be picked up cold.** Companion to
`docs/copy-trade-plan-2026-10-07.md` (the plan) and the four registrations in
`research/copytrade/registrations/` (the ledger: every rule written before
its run, every result in its own §). Results live in
`research/copytrade/results.jsonl`, one line per run citing the registration
commit. This document does two things: it states what the year of data
established, and it lays out the hypotheses that the results make worth
testing next — with what each needs, what it would cost, and an honest
expectation, not a hopeful or a defeatist one.

The goal was never achieved and is not abandoned: a consistent, capturable
edge. What changed is where to look and with what instrument.

---

## 1. What was asked, what was run, what came out

| reg. | question | trials | verdict | the number that decided it |
|---|---|---|---|---|
| 001 | Does a Hyperliquid wallet's past skill predict a follower's return on Bitget? Slow traders, day traders, skilled-vs-losers positioning, crowding | 5 | T1 pass, four fail/null | skill persists (IC 0.40, Z 41) but the best slow quintile copies at −0.04%/day hedged; day traders' edge gone within the hour; positioning book −0.15%/day |
| 002 | After a liquidation cascade, does Bitget's price revert within the hour? Does the liquidation map say where? | 2 | fail, fail | mean +1.9% but t 1.04 — one crash hour (2025-10-10 21:23 UTC) is the whole mean; without it −0.02%; majors zero |
| 003 | On Hyperliquid's own tape: does a fill at the forced print earn? A triggered order? An always-on map order? | 3 | fail, fail, fail | fill at the median forced print: **majors +22 bp, t 3.3** (its own block; the pooled test fell to the crash hour); triggered order fills too early (−0.71%, median +0.21%); always-on orders **−14 bp per fill, t −8** |
| 004 | A ladder at depth, hold only forced fills, exit when the flow ends; and hedge each fill on Bitget | 2 | fail, fail | ladder ≈ 0 in majors (−0.02%); deeper rungs lose more; cross-venue capture −0.43%, t −5.9 |

Thirteen trials on one discovery year (2025-07-28 → 2026-06-30, bear-heavy,
one crash). **The holdout (2026-07-01 →) was never read.** Every run passed
its mechanics gates and its placebo; every stop and amendment is in the
registrations.

## 2. What is established (keep these)

1. **Trader skill persists on Hyperliquid, strongly.** Rank correlation of a
   per-trip t-stat across 91-day lookback and 56-day hold: 0.40–0.44 in all
   four windows, in every hold-time bucket. The bottom tenth stays firmly
   negative. This is the one high-powered positive fact in the whole
   programme (001 §12). It predicts a follower's alpha — and no follower
   design earned after costs.
2. **Forced flow overshoots on the venue where it lands, briefly.**
   Hyperliquid's last liquidation print sits a median 58 bp beyond Bitget's
   next-minute open for alts, 19 bp for majors (002 §11). A fill at the
   cascade's median forced print, closed 15 minutes later, earns +22 bp in
   majors, t 3.3, and the reversion keeps building for about four hours
   (003 §10). This is the light; it is narrow and it is fast.
3. **A resting order filled by anything other than forced flow loses.**
   14 bp net per fill at 15 minutes over 14,356 fills, t −8, at 1–3% from
   the price, and the liquidation map's density changes nothing (003 §10).
   Adverse selection on Hyperliquid is real and large.
4. **Forced flow is separable only during cascades.** For an order posted at
   a cascade trigger, a liquidation prints within a second of the fill in
   54–84% of fills; for an always-on order, under 1% (004 §3). Liquidations
   are also only a minority of what crosses any level, even in cascades.
5. **Depth that earns is defined ex post.** The median forced print is deep
   in proportion to how large the cascade becomes, which is unknown at
   posting. Fixed rungs fill early in big cascades, and the deeper the rung
   the worse (004 §10). Any realisable version must *follow* the flow.
6. **One exchange-wide hour dominates every mean.** 2025-10-10 21:00 UTC.
   Market-wide liquidation rate separates local cascades (which revert) from
   spirals (which do not); the switch at the 99.9th percentile ($77 M per
   five minutes) stands aside for 8% of triggers (004 §3).
7. **Minute granularity cannot test the capture.** The cross-venue gap and
   the overshoot live in seconds; Bitget 1-minute candles and a 1-minute
   poller see the aftermath (002, 004 §10).
8. **Positioning-derived signals at 3 days were negative or null** (001 T3,
   003 T3). Where skilled slow money sits relative to losers anti-predicted
   3-day returns in this period, t −3.4 — a bear-year fact, not a strategy.

## 3. Data on hand (all local, gitignored under `research/copytrade/data/`)

| asset | coverage | derived tables |
|---|---|---|
| Hyperliquid fills, every account, with `is_liquidation`, `crossed`, direction | 2025-07-28 → 2026-06-30, 338 days, 43 GiB pruned | `liquidations.parquet` (4.56 M forced fills, $40 B), `cascades.json` (2,838 qualifying cascades + robustness grid), per-coin daily liquidation totals |
| Daily account snapshots: every position with liquidation price, leverage, value | 286 discovery snapshots (gap 2025-10-25 → 12-14) | `snapshot_accounts.parquet`, `oi_by_coin_snapshot.parquet`, `map_levels.parquet` (cumulative forced flow within 1/2/3% per coin-day) |
| Bitget 1-minute candles | event windows only: 252 k blocks for 001, ~22 k for 002/004 | — |
| Bitget 1H candles, mapped coins | 2025-06 → 2026-07 | betas, ATR |
| Hyperliquid funding, mapped coins | 2025-10-20 → 2026-07 | — |
| BTC 1H since 2019-07 | regimes | `regimes/btc_1h.parquet` |
| Code | `ct/` (replay, tape, liq, trigger, provider, eventstudy, stats, crowding) with synthetic tests; runners `p5_run`, `l3_run`, `l5_run`, `l6_run` behind the registration guard | — |

**Not on hand, and decisive for the next steps:** order-book depth and queue
position; sub-second cross-venue prices; the counterparty identity of a
fill at the moment of the fill (live only); liquidation streams of other
venues; the holdout months (sealed by design).

---

## 4. Horizons

Each horizon states the hypothesis, the mechanism, what it needs, how to test
it, and two honest numbers: the odds that a *significant, costed effect
exists* in a registered test, and the odds it is *deployable* at a size that
matters. Ordered by how directly the results point at it.

### H1. The trailing ladder: a market maker that follows the forced flow

**Hypothesis.** A provider that keeps bids a fixed fraction below the
*current* print while liquidations keep printing — and cancels when they
stop — fills at the depth where the median forced notional lands, and earns
the overshoot that 003 measured ex post.

**Why the results point here.** Fact 2 says the edge exists at the median
forced print; fact 5 says fixed depth misses it; fact 4 says the fills are
mostly forced only during cascades. A trailing order is the only placement
that is deep in proportion to the cascade without knowing its size.

**What it needs.** Queue position and partial fills — the archive has
neither. Two routes: (a) a live paper market maker on Hyperliquid's
websocket (`trades`, `l2Book`, `userFills`), posting and cancelling in
paper, logging where the forced prints land relative to the order; (b)
historical L2 book data for Hyperliquid (Tardis.dev lists Hyperliquid
since 2025 — paid; the official node data `hl-mainnet-node-data` carries
order statuses by block, requester-pays, unverified for book
reconstruction).

**Test.** Paper for 6–8 weeks of live cascades (at the 2025–26 rate, roughly
8 qualifying a day across coins, 1–2 in majors), registered in advance:
fill rate at each trailing distance, forced share by counterparty address,
net return at the flow-end and 60-minute exits, majors primary. Hour
clustering; the crash-hour rule.

**Expectation.** Effect exists: about 1 in 2 — the 003 upper bound is
positive and the trailing design is the faithful version of it. Deployable:
about 1 in 4 — queue competition from HLP and market makers at exactly
those prints, and the capacity is 5–10% of a cascade's forced notional, so
a few hundred fills a year at tens of bp each in majors. Steer: run it on
alts too, where the overshoot is 58 bp, with a size cap; the tails are the
price.

### H2. The same mechanism on the deepest venues, with tick data that exists

**Hypothesis.** Forced-flow overshoot and its reversion exist on Binance,
Bybit and OKX futures, where forced flow is several times Hyperliquid's,
and can be measured at the right granularity from public historical data.

**Why.** Binance publishes historical futures `aggTrades` (tick), `bookTicker`
and **`liquidationSnapshot`** daily files for USDⓈ-M contracts, free;
Bybit publishes public trade history; OKX has liquidation and trade
history via API. That is the tick-level, venue-of-the-fill data this
programme lacked, at zero cost, for the venues with the most forced flow.

**Test.** Rebuild `l1` → `l2` → `l3` on Binance: cascades from the
liquidation snapshot, the event study on the venue's own ticks at 1 s to
60 min, the forced-print fill as the upper bound, then the trailing
provider in simulation with a conservative queue model (fill only if the
print trades *through* the level by a tick). Discovery/holdout split by
calendar from day one.

**Expectation.** Effect exists: about 1 in 2 — the mechanism is the same and
the literature on liquidation cascades in Binance futures is substantial.
Deployable: about 1 in 5 — these books are the most competitive in crypto
and the overshoot per fill will be smaller than Hyperliquid's; the gain is
frequency and depth. Steer: start with the upper bound (fill at the median
forced print) on three majors and three alts; if it is under 10 bp, stop.

### H3. Cross-venue capture at second granularity

**Hypothesis.** The 19–58 bp gap between Hyperliquid's forced print and the
other venues' price can be captured if the hedge is placed within seconds,
not at the next minute.

**Why.** 004's capture lost at minute granularity because Bitget was still
falling at the next minute's open (fact 7). Whether the gap is capturable
is a sub-second question.

**What it needs.** Second-level or tick prices on both venues at the same
clock: Hyperliquid 1-second candles exist in the Hydromancer archive
(requester-pays, ~1 s bars) and the tape is already local; Binance
`aggTrades` ticks (free) or Bitget trade ticks via websocket recording
(live only).

**Test.** For every forced print in a cascade, the other venue's mid at +1,
+2, +5, +10, +30 s and the Hyperliquid tape at the same offsets: the gap's
half-life, and the hedged P&L with 1–3 s latency. Register before reading
the Hyperliquid 1-second candles.

**Expectation.** Effect exists: about 1 in 2 for alts (58 bp median gap
against ~13 bp of round-trip costs), 1 in 5 for majors (19 bp). Deployable:
1 in 6 — this is latency arbitrage against firms that do it in microseconds;
if the gap's half-life is under two seconds, it is not ours. Steer: measure
the half-life first; it decides everything and costs one data pull.

### H4. Post-cascade drift at one to four hours, entered after the flow ends

**Hypothesis.** After a local (non-spiral) cascade ends, majors drift back
over 60–240 minutes by enough to clear taker costs when entered *after* the
last forced print, not during.

**Why.** 003 T1's reversion built from +0.13% at 15 min to +1.18% at 240
(medians 0.29 → 0.48%); 004's fixed 60-minute exit was the only positive
block in majors (+0.16%, t 1.5). 002 measured a 60-minute entry on Bitget
at zero for majors — but 002 entered at detection, inside the flow, with
a Bitget poller; this is entry at flow end, on Hyperliquid, with the stress
switch, longer horizon.

**What it needs.** Nothing new: tape, cascades, stress switch are local.

**Test.** One registration, one primary: majors, entry at the first print
after 60 s without a liquidation, taker, hold 120 min, exit taker; hedged
and unhedged; the crash-hour rule; the family count stated (would be the
fourteenth trial on this year). If it passes, the holdout.

**Expectation.** Effect exists: 1 in 3 — the signs all point the same way
but no block was significant, and a 2-hour hold in majors is a directional
bet with 12 bp of costs against medians of 20–50 bp. Deployable: 1 in 3 if
it exists — a taker strategy at minute cadence needs no infrastructure
beyond what the repo has. Steer: this is the cheapest test on the list and
the one most exposed to the "fourteenth look" problem; register it with the
holdout as the decision.

### H5. Spirals as a regime signal

**Hypothesis.** When the exchange-wide liquidation rate exceeds its
99.9th percentile, prices continue in the direction of the forced flow for
hours; the stress switch is a tradable short-term trend signal, not only a
stand-aside rule.

**Why.** The spiral hours (2025-10-10, 2025-08-01, -18, -25, 2026-02-05)
broke every reversal mean because the flow continued. That is a statement
about continuation after extreme market-wide forced selling.

**What it needs.** Local data only.

**Test.** Event study on the 5-minute buckets at or above the 99.9th
percentile: BTC and ETH returns at 1, 4, 12, 24 hours, in the direction of
the net forced flow; costed as a taker trade; day-clustered (these buckets
cluster into a handful of days — the effective sample is the number of
spiral *days*, about 10–15).

**Expectation.** Effect exists: 1 in 3 — margin spirals are documented to
continue, but ten to fifteen independent days is barely a test. Deployable:
1 in 3 if it exists, and it fires a few times a year. Steer: treat it as a
risk overlay for any other strategy first; as a standalone it needs more
years, which means other venues' liquidation histories (H2's data).

### H6. Leverage crowding as a weekly factor

**Hypothesis.** Coins whose open interest sits closest to liquidation — the
share of positions within a few percent of their liquidation price — earn
lower (or more volatile) returns over the following week, because they are
the ones the next cascade hits.

**Why.** The map (`map_levels.parquet`) is a direct measure of how fragile
each coin's positioning is, point-in-time, every day. 001 T3 and 003 T3
used positioning at 3 days and got nothing or negative; fragility over a
week is a different object, closer to the "crowded trade" literature.

**What it needs.** Local data plus Bitget or Hyperliquid daily prices
(1H local).

**Test.** Cross-sectional: daily rank of coins by fragility (forced flow
within 5% of price, long minus short), 7-day forward return, IC with
Newey–West, and the costed long/short book with the magnitude check that
killed the earlier reversal signal. Register as a factor test under the
spec's rules; discovery then holdout.

**Expectation.** Effect exists: 1 in 4 — crowding factors are real but
weak, and this universe is 100 coins over one year. Deployable: 1 in 3 if
it exists — a weekly rebalanced book is the easiest thing in this repo to
run. Steer: also test fragility as a *conditioner* for H4 (reversion is
larger where the map showed flow? 003 T3's tercile split said no at 15
minutes; a week is different).

### H7. Be the backstop: the liquidation profit the market already sells

**Hypothesis.** The forced-flow edge is already packaged as Hyperliquid's
HLP vault (and the backstop liquidator role on other venues): depositors
earn the liquidation and market-making spread without the latency race.

**Why.** $4.5 B of the year's liquidations were `Liquidated Cross` rows —
backstop takeovers at a discount — and HLP's returns, including 2025-10-10,
are public on-chain. If the edge we keep measuring is being captured by the
vault, the honest question is whether the vault's return, after its own
tail risk, beats building the capture ourselves.

**What it needs.** HLP's historical equity curve and its liquidation
component (public), the vault's capacity and lock-up terms.

**Test.** A measurement, not a trial: decompose HLP's monthly return into
market-making and liquidation components; compare its drawdown in spiral
hours with ours (004 §3). No holdout needed; it is not our hypothesis.

**Expectation.** The return is positive and documented; the risk is
concentrated in exactly the spirals; the capacity is large; the return is
shared with every other depositor. Odds that it beats a home-built provider
on risk-adjusted return at our size: better than even. Steer: this is the
benchmark any H1–H3 result has to beat, and it should be measured before
anything is built.

### H8. Skill persistence as a filter, not a product

**Hypothesis.** The one strong positive (fact 1) is worth something as a
*negative* filter on any flow-based signal: forced flow from wallets in the
bottom deciles is different from forced flow from the top.

**Why.** Liquidations are per account; the archive knows each liquidated
account's skill score. Cascades made of chronic losers being liquidated may
revert differently from cascades that liquidate skilled traders (who were
probably right and early).

**What it needs.** Local data: join `liquidations.parquet` to the 001
scores by address and window.

**Test.** Split cascades by the median lookback skill of the liquidated
accounts; compare the 003 T1 upper-bound return and the 004 ladder return
across the split. One trial, cheap.

**Expectation.** Effect exists: 1 in 4 — plausible mechanism, small
samples once split. Deployable only as a conditioner. Steer: do it inside
the H1 paper period as a reported split, not as a standalone registration.

### H9. Multi-venue forced flow as a live product

**Hypothesis.** Running the H1 provider on Hyperliquid, Binance, Bybit and
OKX at once multiplies the fill count by the ratio of their forced flow to
Hyperliquid's (several times) and diversifies the spiral risk across venues.

**Why.** Frequency was the binding constraint on every positive block:
a few hundred fills a year in majors on one venue.

**What it needs.** H1 proven on one venue first; then per-venue liquidation
streams (`forceOrder` on Binance, liquidation topics on Bybit and OKX, all
public websocket), a provider that handles each venue's fee and tick
structure, and capital on each. A separate stack: an always-on process
(not Vercel), one event loop per venue, a shared risk book.

**Expectation.** Conditional on H1 working on Hyperliquid: deployable at
2 in 3 — the engineering is ordinary; the odds that the edge generalises to
Binance's book are the H2 odds. Steer: this is the scale step, not a
research step; do not start it before H1 or H2 has a pass.

### H10. The copy-trading residual

**Hypothesis.** The 72-hour drift after skilled slow traders' opens (0.13 ATR
gross) and the day-traders' first-ten-minutes edge are real but were not
capturable at the tested lags and costs.

**Why.** 001's reported blocks. Both are small, the first needs a 3-day
hold and the second needs seconds.

**Expectation.** 1 in 5 for either to clear costs in a registered test;
listed for completeness. Steer: only the 72-hour version is cheap to test
(data local); do it only if H4 passes, since it shares the "entered after,
held for hours" shape.

---

## 5. Gray areas and open questions

- **Who is on the other side of the forced print?** The archive shows both
  rows of a match but not which was the aggressor at the micro level; the
  crossing fill is a liquidation row in under 5% of cases even in cascades,
  while a liquidation prints within a second in up to 84%. Live `trades`
  frames carry both addresses. This is the first thing a paper market maker
  should log.
- **Why does Bitget sit 50 bp away from the HL print a minute later, yet
  hedging there at the fill loses?** Either the gap is gone within seconds
  or Bitget is still falling when the fill happens. H3's half-life
  measurement answers it.
- **Is the 2025-10-10 hour an outlier or the distribution's true tail?**
  Thirteen trials treated it as a cluster; a longer history (H2's venues
  have years) says how often it recurs. Every forced-flow strategy's
  expected return is decided by this question.
- **Does skill persistence (fact 1) carry *any* tradable implication?** It is
  the strongest fact in the data and nothing built on it earned. The honest
  possibilities: the persistence is in losing (fees, churn), which no
  counterparty can collect; or copy latency at any poll loses it; or the
  edge is in sizing that normalisation removed (001's risk-weighted ATR-R
  was negative, so probably not).
- **How stale can the map be and still mean something?** The daily snapshot
  told nothing about fills at 15 minutes (003 T3, flat terciles). It may
  tell something at a week (H6) or as a within-cascade depth estimate (H1,
  "how much forced notional still sits below").
- **Majors versus alts.** Every positive block was in majors; every large
  mean was alts' tails. The edge is small and clean in majors, large and
  dangerous in alts. Sizing rules for alts are a research question of their
  own.
- **What does the holdout add?** One rally (Aug +24%, Sep +8%). Any pass on
  discovery still means "in a bear"; the holdout is the only regime
  transition available before the forward archive accrues.

## 6. Protocol for the next sessions

1. **The ledger continues.** New hypothesis, new registration file, status
   DRAFT until the owner approves the decision table, REGISTERED commit,
   guarded runner, one results line. The crash-hour rule (cluster by hour;
   report without the largest cluster) and the placebo are mandatory.
2. **Count the family.** Thirteen trials have read the discovery year. A
   registration that reads it again states that count and treats a pass as
   the reason to read the holdout, not as the result. New data (H2, H3) and
   live paper (H1) start their own counts.
3. **Holdout stays sealed** until a rule has passed discovery under this
   protocol. It is read once.
4. **Measure the benchmark first** (H7). A home-built provider has to beat
   depositing into the vault that already does this.
5. **Infrastructure for the live horizons** is a separate stack: an
   always-on process with websocket access to the venues, paper execution,
   a log of every print, order and fill with timestamps to the millisecond.
   Nothing in this repo's Vercel path is suited to it, and nothing in this
   repo needs to change for it.
6. **If data is missing, look; if evidence is missing, test.** The two data
   pulls that unlock the most are Binance's historical liquidation and tick
   files (free; H2, H5) and Hyperliquid's 1-second candles (requester-pays;
   H3).

## 7. Expectations in one table

| horizon | effect exists | deployable | cost to find out | first step |
|---|---|---|---|---|
| H1 trailing ladder, live paper | 1 in 2 | 1 in 4 | weeks of paper, a websocket stack | log where forced prints land vs a trailing order |
| H2 same mechanism on Binance etc. with tick data | 1 in 2 | 1 in 5 | a free data pull, a day of adaptation | upper bound at the median forced print, 6 coins |
| H3 cross-venue capture at seconds | 1 in 2 alts, 1 in 5 majors | 1 in 6 | HL 1-s candles (requester-pays) | the gap's half-life |
| H4 post-cascade drift, 1–4 h, majors | 1 in 3 | 1 in 3 | one registration, local data | register; holdout decides |
| H5 spirals as continuation | 1 in 3 | 1 in 3 | local data; more years from H2 | event study on the 99.9th-percentile buckets |
| H6 leverage crowding, weekly | 1 in 4 | 1 in 3 | local data | factor test under the spec's rules |
| H7 be the backstop (HLP) | documented | n/a | a measurement | decompose HLP's return |
| H8 skill as a filter on forced flow | 1 in 4 | conditioner | local data | split cascades by liquidated accounts' skill |
| H9 multi-venue product | conditional on H1/H2 | 2 in 3 if so | a new stack | not before a pass |
| H10 copy residuals | 1 in 5 | low | local data | only after H4 |
