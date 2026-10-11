# Edge discovery programme (after forced flow)

**Started 2026-10-11, to be picked up cold.** Brief:
`docs/edge-discovery-orchestrator-prompt-2026-10-11.md`. Predecessor:
`docs/forced-flow-research-horizons-2026-10-10.md` (§9 is what it left
behind). Benchmark: `docs/hlp-benchmark-2026-10-10.md`. Ledger:
`research/copytrade/registrations/`, results in
`research/copytrade/results.jsonl`. Branch `copytrade-research`; commits are
local, the owner pushes. Nothing is published (AGENTS.md rule 5).

**Goal.** A consistent, capturable edge at our size (well under $1 M), in any
lawful market. Generous with hypotheses, strict in testing.

**Benchmark for every light.** HLP: +19% in the archive year, 17 points from
two spiral fortnights, about 2–4% a year otherwise, daily Sharpe 1.4, max
drawdown −2.5%, long the spiral tail. Its lending leg: about 3% a year.

---

## 1. Lights and status

| id | light | status | effect exists | deployable | vs HLP |
|---|---|---|---|---|---|
| L1 | Vault allocation on Hyperliquid (skill persistence, no copy latency) | scout running | 1 in 3 | 1 in 4 | NAV-like; vaults can be leveraged single-regime bets; closure tail |
| L2 | Funding / basis carry across venues | scout running | likely | 1 in 2 | low single digits net, capacity-limited; leg liquidation in a spiral |
| L3 | Inverted 001 T3 positioning, holdout-only | **held** until the L1 scout reports (holdout allocation) | 1 in 5 | — | rally holdout is a hard test |
| L4 | Multi-year cross-sectional factors from free Binance data | scout done (§4); discovery pull 1.54 GB awaits owner | 1 in 4 | 1 in 3 if it exists | weekly book |
| L5a | Funding-settlement effects (price and flow around funding timestamps) | **parked** 2026-10-11 (§4): documented edge ~0.5 bp vs ≥ 9 bp round trip | 1 in 10 | 1 in 10 | cost-bound |
| L5b | Prediction-market favourite–longshot bias | **closed** 2026-10-11 (§4): no lawful venue for a German resident | — | — | — |

**Why L5a could be consistent:** funding is a scheduled, mechanical cash
flow, and traders act around it every settlement, every day. **Why L5b could
be consistent:** the favourite–longshot bias is a behavioural preference
documented across a century of betting markets.

## 2. Family counts and holdouts

- **Hyperliquid discovery year (2025-07-28 → 2026-06-30):** 14 trials, no
  pass. Any registration reading it again states the count and treats a pass
  as the reason to read the holdout.
- **Hyperliquid holdout (2026-07-01 →):** sealed, never read. L3 and an
  archive-year L1 would both want it. Its allocation is decided once, after
  the L1 scout, not by whichever light registers first.
- **New data starts its own count,** stating the selection it shares with
  earlier looks. L1 on pre-archive vault history (2023-05 → 2025-07) would
  share selection with 001 T1: the light was chosen because skill persisted
  on the archive year.
- **For every new light, 2026-07-01 → is held back as well.** All scout
  queries carry an end date before 2026-07-01, so that period stays usable
  as a holdout for any programme.

## 3. Corrections to the brief (2026-10-11, orchestrator)

1. **L1's premise is half right.** Latency killed the day-trader copy (001
   T2b). For slow traders it did not: the top quintile earned −0.04%/day at
   a 1 h lag and +0.02% ± 0.05 at 24 h (001 T2). The warning sign is the
   risk-weighted per-trip ATR-R of −0.05: the trades leaders sized up lost.
   T1's persistence is per trip, equal-weighted; a depositor's return is
   size-weighted. Odds lowered from 1 in 2 / 1 in 3 to 1 in 3 / 1 in 4.
2. **The obvious L1 source leaks the holdout.** The cached vault list
   (`data/cache/hl-vaults`, 9,477 vaults, 6,386 marked closed, cached
   2026-10-08) carries today's `apr`, `pnls`, `tvl` and `isClosed`, and
   `vaultDetails.portfolio` has no end-date parameter. **Owner-approved
   handling (2026-10-11):** scripts read only address, leader, name,
   creation time and relationship from the list, and cut every
   `vaultDetails` history at 2026-07-01 at load, before anything is printed
   or saved.
3. **L1 can get new years.** Vault histories go back to 2023-05 and no trial
   has read vault returns, so the pre-archive span is a candidate discovery
   set with a bull regime in it, if the history is dense enough.
4. **For L2, "behaviour only" has to be defined.** In carry the signal
   (today's spread) and the payoff (future funding) are the same series, so
   measuring persistence would be running the trial. The scout measures
   coverage, costs, margin, and the unconditional size of spreads against
   costs at entry (006's gap-against-cost test). Persistence goes into the
   registration.

## 4. Direction log

**2026-10-11 — programme opened; first moves.** Reading list done. The
owner approved: (1) four parallel scouts, free endpoints only, behaviour
only, no data spend: L1 vaults, L2 carry, L4 Binance multi-year coverage,
and a desk scout for L5a/L5b; (2) the L1 holdout handling in §3.2; (3) the
L5 picks; (4) holding L3 until the L1 scout reports, so the Hyperliquid
holdout is allocated once. Ranking after reading: L2 and L1 first (cheapest
clean tests of a structural premium and of the one strong fact), L4 for
power, L5 as independent sources, L3 last (post-hoc idea, lowest odds,
consumes the holdout).

**2026-10-11 — L5b closed; L5a parked; venue access becomes a standing
question.** The desk scout (`research/copytrade/scouts/l5-desk.md`) found
no lawful prediction-market venue for a German resident. The GGL (notice of
5 Sep 2025) classes Polymarket-style event bets as unlicensable, illegal
betting and treats taking part as illegal (§285 StGB). Betfair Exchange is
not licensable, and Kalshi contracts on financial underlyings fall under
BaFin's retail binary-option ban. The orchestrator checked the GGL position
against independent press reports of the notice. L5b stops on access, not on
evidence. L5a is parked, not registered. The only out-of-sample clock-time
predictability on Binance (Kim & Hansen 2026, 2021–2024) is about 0.5 bp a
boundary, and it is unchanged when the funding quarter-hours are excluded.
Settlement literature documents volume and spread shapes, not returns. A
taker round trip costs 9–11 bp. The local behaviour check (counts only) shows
nothing special at 08/16 UTC. With about 3,300 settlements a coin, a
costs-clearing effect would be easy to detect, so the literature's silence
counts against it. **Cross-cutting:** Binance wound down derivatives for
German users on 2021-07-30, so Binance is research data only. Every light now
states its lawful execution venue (Hyperliquid, Bitget, OKX Europe X-Perps
under MiFID since 2026-04-15, …). The L2 scout was asked to add this for each
carry leg.

**2026-10-11 — L4 scout: the data exists, from 2021-12, with the power the
HL year lacked.** (`research/copytrade/scouts/l4-binance-years.md`; the
start date was re-checked by the orchestrator on the public listing.)
- **Coverage.** Binance's 5-minute metrics (open interest, long/short
  ratios, taker volume ratio) have a cross-section from **2021-12-01**
  (134 coins), not 2020. Funding and klines go back to 2020-01.
- **Coins per year with metrics on ≥ 300 days:** 127 / 162 / 237 / 342 in
  2022–2025, and 502 in 2026-H1.
- **Survivorship.** Delisted symbols stay in the archive. A symbol's
  delisting date is its last metrics file of at least 4 KB: this matches
  exchangeInfo's `deliveryDate` for 117 of 120 delisted symbols, and the
  other 3 are one day early. A loader must cut each symbol there, because
  1d klines continue after death.
- **No free multi-year liquidation record exists on any venue.** Spirals
  across years have to be proxied from OI drops plus taker imbalance (Bybit
  REST has 5-minute OI from 2020-08 as a second venue).
- **Split, fixed now, before any outcome:**
  - discovery 2021-12-06 → 2023-12-31;
  - holdout A 2024-01-01 → 2025-07-27;
  - holdout B 2025-07-28 → 2026-06-30 (the HL year: confirmatory only, and
    not independent for ideas H5 and H6 that it motivated);
  - 2026-07 → stays sealed;
  - a 1-week embargo at each boundary.
- **Power.** Detectable weekly IC at 80% power is about 0.02 under iid
  (discovery: 108 weeks, 17,374 coin-weeks), and about 0.03–0.04 under a
  realistic weekly IC dispersion. The HL year could detect only far larger
  effects.
- **Bytes.** The discovery pull (metrics + funding + 1h klines, 258
  symbols) is **1.54 GB**, and all years 5.86 GB. A Monday-only metrics
  variant is 0.20 GB.
- **Venue caveat.** The book cannot trade on Binance from Germany (§4,
  L5). The registration must restrict the universe point-in-time to coins
  listed on a lawful venue (Bitget listing dates are cached in
  `data/cache/bg-first-day`; also Hyperliquid), and must cost the trade at
  that venue.

## 5. Data inventory

Everything from the forced-flow programme (its §3 and §9), plus whatever the
scouts add. Scout outputs: `research/copytrade/scouts/<id>.md` plus a
script; data under `research/copytrade/data/scout_<id>/` (gitignored).

| asset | where | coverage | notes |
|---|---|---|---|
| HL fills, every account | `data/archive`, `data/pruned` | 2025-07-28 → 2026-06-30 | 43 GiB; vault addresses included (001 excluded them by list) |
| HL daily account values | `data/derived/hyperliquid/snapshot_accounts.parquet` | 286 discovery snapshots | `user, account_value, gross/net_notional`; gives vault equity point-in-time |
| HL vault list | `data/cache/hl-vaults` | cached 2026-10-08 | holdout fields present: see §3.2 |
| HL funding | `data/cache/hl-funding` | 2025-10-20 → | |
| Binance aggTrades + bookDepth, 6 coins | `data/binance` | HL-cascade days only | not contiguous |

## 6. Scouts in flight

| scout | file | started | state |
|---|---|---|---|
| L1 vaults | `scouts/l1-vaults.md` | 2026-10-11 | running |
| L2 carry | `scouts/l2-carry.md` | 2026-10-11 | running |
| L4 Binance multi-year | `scouts/l4-binance-years.md` | 2026-10-11 | done |
| L5 desk | `scouts/l5-desk.md` | 2026-10-11 | done |
