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
| L1 | Vault allocation on Hyperliquid (skill persistence, no copy latency) | scout done (§4); archive-year design proposed | 1 in 3 | 1 in 4 | NAV-like; vaults can be leveraged single-regime bets; closure tail |
| L2 | Funding / basis carry across venues | scout done (§4); **blocked on venue access** | likely (2023–24), compressed since | 1 in 5 | ~1–2%/yr unconditional net in 2025–26, below HLP's lending leg |
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

**2026-10-11 — L1 scout: vaults are day traders, the archive year is the
only place the 001 score can be computed, and the pre-archive years cannot
carry it.** (`research/copytrade/scouts/l1-vaults.md`)
- **Selectable vaults** (≥ 20 trips in 91 d, equity ≥ $10k): the scout
  counts 158 / 131 / 114 / 121 at the four 001 selection dates with a
  snapshot ≤ 7 d in the lookback. The orchestrator's recount with 001's own
  eligibility rule (`equity_age_h ≤ 168` in `features_<S>.parquet`) gives
  258 at 2025-10-27 and 158 at 2026-04-13. The registration fixes one
  definition. At ≥ $100k: about 40–80.
- **Vaults hold for a median of 8–14 h**, so they belong to 001's *day*
  bucket, not its slow one. That makes the light *more* pointed, not less:
  001 T2b found day-trader skill persists (IC Z 7.5) but decays within the
  hour for a follower. A depositor has zero lag. Depositing is the one design
  that keeps the part 001 showed is lost to latency. The size-weighting risk
  (§3.1) still stands.
- **Capacity at < 10% of vault equity:** 41–51 vaults per window for $10k,
  12–17 for $50k, 2–5 for $250k. Deployable size is tens of thousands, not
  hundreds.
- **Pre-archive years cannot carry the 001 score.** `allTime` has one point
  every 7–14 days, and the grid continues after closure. API fills are cut
  off by retention for active vaults. A pre-archive design could rank only
  on coarse NAV, which is not the persistent fact, so it is dropped as
  primary.
- **NAV is reconstructable in the archive year.** Depositor flows appear on
  the vault's own ledger (`vaultDeposit`/`vaultWithdraw`, endTime-bounded),
  and equity is in the snapshots (for vaults with an open position). The
  `allTime` grid truncated at the cutoff is the cross-check. Closure proxy:
  the first `vaultDistribution`. The list looks complete: 0 of 1,064
  leader-created vaults are missing.
- **Terms (today's docs, "legacy" vaults):** 10% profit share, ≥ 5% leader
  ownership, 1-day lock-up, withdrawals force up to 20% position closes, no
  documented caps.
- **Design consequence.** L1 runs on the archive year: the 001 score from
  fills over the lookback, and depositor NAV over the hold. It would be
  **trial 15** on that year. A pass only earns the holdout read. **Holdout
  allocation (decided here):** L1 and L3 both need the same Hyperliquid
  holdout pull (fills and snapshots, requester-pays, ~12 GiB). It is pulled
  once, after the rules that use it are REGISTERED. L3 is holdout-only; L1
  reads it only on a discovery pass. The holdout family count is stated in
  both.
- **Exposure.** The local `snapshot_accounts.parquet` holds holdout-period
  rows (8.65 M). The scout saw counts only; recorded in `000c`. Every reader
  filters in the query.

**2026-10-11 — L2 scout: the spread exists but has compressed, and the
lawful pair has no history; venue access becomes the owner's gating
decision.** (`research/copytrade/scouts/l2-carry.md`; the orchestrator
re-read the core quantiles in `data/scout_l2/analysis.txt`.)
- **Data.** Free, date-bounded funding history exists for HL (hourly from
  2023-06, 220 coins), Binance, Bybit and OKX. About 190–200 coins have
  overlapping listings in 2025–26.
- **Costs.** Fees on both legs' round trip are 19–21 bp. Amortised hurdle:
  about 77% / 11% / 2.6% a year for 1-day / 7-day / 30-day holds. Separate
  venues mean separate margin: at 2×, a leg is liquidated on a 43–49% move.
- **The HL-minus-CEX spread, core coins.**
  - Median +3 to +5% a year in 2024–26.
  - p10–p90 range narrowed from about −23/+46% (2023) to about −11/+12%
    (2026-H1).
  - The spread is beyond the 7-day hurdle in 22–26% of hours in 2025–26,
    and almost never beyond the 1-day hurdle.
  - **Unconditionally, short HL / long CEX nets about 1–2% a year in
    2025–26 at a 30-day hold, below HLP's ~3% lending leg.** Only a
    conditional entry rule could beat it, and that rule is the trial.
- **Perp-spot on Binance.** Median funding fell from 9–11% a year (2023–24)
  to about 1–2% (2026-H1). No hour in 2025–26 cleared the 7-day hurdle.
  Ethena sUSDe realised 15.7 / 6.2 / 4.0% (2024 / 2025 / 2026-H1).
- **Access (decisive).** For a German resident, Binance, Bybit, OKX global
  and **Bitget** (its terms reportedly bar Germany; the owner should check
  their own account) are research data only. **Hyperliquid is unlicensed in
  the EU, a grey zone.** OKX Europe X-Perps (MiFID, since 2026-04-15) is
  the only licensed perp leg, and it has no history before the holdout. A
  lawful L2 can therefore not be tested on its own venue pair before the
  holdout.
- **Not to be used for thresholds.** F3's monthly sign-change counts of the
  spread (BTC median 8–26 a month) are counts over the level series, but
  they inform persistence. They may not set any L2 registration parameter.
- **Exposure.** A pre-existing Bitget funding cache, entirely holdout-dated,
  was printed (15 ARB rows); recorded in `000d`. The orchestrator's brief
  had pointed at that cache.

## 5. Data inventory

Everything from the forced-flow programme (its §3 and §9), plus whatever the
scouts add. Scout outputs: `research/copytrade/scouts/<id>.md` plus a
script; data under `research/copytrade/data/scout_<id>/` (gitignored).

| asset | where | coverage | notes |
|---|---|---|---|
| HL fills, every account | `data/archive`, `data/pruned` | 2025-07-28 → 2026-06-30 | 43 GiB; vault addresses included (001 excluded them by list) |
| HL daily account values | `data/derived/hyperliquid/snapshot_accounts.parquet` | 286 discovery snapshots **+ ~98 holdout snapshots (filter `snapshot_ms < 1782864000000`)** | `user, account_value, gross/net_notional`; gives vault equity point-in-time |
| HL vault list | `data/cache/hl-vaults` | cached 2026-10-08 | holdout fields present: see §3.2 |
| HL funding | `data/cache/hl-funding` | 2025-10-20 → 2026-07-10 | **filter `time < 1782864000000`** |
| Bitget funding | `data/cache/bg-funding` | holdout only | **holdout-only data, never read in discovery (`000d`)** |
| HL / Binance / Bybit / OKX funding, date-bounded | `data/scout_l2/` | 2023 → 2026-06-30 | fetched by the L2 scout |
| Binance aggTrades + bookDepth, 6 coins | `data/binance` | HL-cascade days only | not contiguous |

## 6. Scouts in flight

| scout | file | started | state |
|---|---|---|---|
| L1 vaults | `scouts/l1-vaults.md` | 2026-10-11 | done; exposure ledgered as `000c` (counts only) |
| L2 carry | `scouts/l2-carry.md` | 2026-10-11 | done; exposure ledgered as `000d` |
| L4 Binance multi-year | `scouts/l4-binance-years.md` | 2026-10-11 | done |
| L5 desk | `scouts/l5-desk.md` | 2026-10-11 | done |
