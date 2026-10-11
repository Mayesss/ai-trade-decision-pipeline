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
| L4 | Weekly cross-sectional funding carry (F1) on Binance perps; F2 crowding dropped on turnover | **009 review: 1 blocker + 5 major; Revision 3 in progress**; pull done (1.527 GB, checksums 0 fail) | 1 in 3 | 2 in 5 | weekly book, market-neutral; capacity ample at our size |
| L2 | Cross-venue funding carry (HL / Binance / Bybit pairs; OKX reported), conditional entry | **010 review: 3 blockers; Revision 1 in progress; L2 holdout demoted (000f), forward holdout 2026-07 → for the owner** | 2 in 3 (2023–24); compressed since | 1 in 3 | 2–6%/yr net if it holds; leg-liquidation tail at ≥ 2× |
| L1 | Deposit in top-scored Hyperliquid user vaults | **007 DRAFT Revision 2 committed; decision table with the owner** | 1 in 3 | 1 in 4 | vault NAV; single-regime leverage; capacity tens of $k |
| L5b | Prediction-market favourite–longshot bias (Kalshi, maker side) | scout done (§4); candidate 011, clean out-of-sample on post-paper data; data pull (≤ 15–20 GB) needs owner | 2 in 5 | 1 in 5 | ~3–8%/yr gross, −1 to +4% over cash: lending-like, tiny capacity, diversifying except crypto contracts |
| L3 | Inverted 001 T3 positioning, holdout-only | drafting 008 after 007 (shares its holdout pull) | 1 in 5 | — | rally holdout is a hard test |
| L5a | Funding-settlement effects | parked (economics: ~0.5 bp documented vs 9–11 bp costs) | 1 in 10 | 1 in 10 | cost-bound |

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
  snapshot ≤ 7 d in the lookback. *(Correction, 2026-10-11: the
  orchestrator's first recount of 258 / 158 was wrong. `equity_age_h` is
  negative (snapshot before S), so `≤ 168` filtered nothing. The 007
  drafter's count with the sign applied, plus 001's maker and
  fills-per-day caps and the distribution rule, is **92 / 73 / 59 / 68**,
  171 distinct vaults.)* At ≥ $100k: about 40–50.
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

**2026-10-11 — owner: legality is a warning, not a blocker; lights
re-ranked.** The owner reports that Bitget and Binance work for them, says
exchanges enforce their own feature bans, and notes they are not a German
citizen. Jurisdiction is now one warning line per light. A venue's *own*
geoblock is still a practical fact: Polymarket is close-only for Germany,
by the venue. Owner approved the L4 discovery pull (1.54 GB). Consequences:
- **L4 moves to first.** It can be executed on Binance itself (the deepest
  book, 5 bp taker), so deployability rises. Funding carry joins the family
  as a factor: it is a documented crypto premium, and it is the
  single-venue form of L2. Effect 1 in 3, deployable 2 in 5.
- **L2 reopens as 010.** Every pair of HL, Binance, Bybit and OKX is usable.
  Discovery is 2023-06 → 2024-12 (wide spreads), holdout 2025-01 →
  2026-06-30 (the compressed regime, a hard and realistic test), and
  2026-07 → stays sealed. The scout's unconditional quantiles and sign-flip
  counts covered all years: this is disclosed, and they set no parameter.
  Bitget has no date-bounded history, so it is excluded from testing.
- **L5b reopens on Kalshi.** Bürgi, Deng & Whelan, *Makers and Takers: The
  Economics of the Kalshi Prediction Market* (CEPR DP20631; 300k+
  contracts; checked by the orchestrator) document a favourite–longshot
  bias on Kalshi, larger for takers' prices, and shrinking over time. A
  scout checks access for a non-US resident, fees, history and capacity.
  Polymarket is venue-geoblocked for Germany and is kept as research data.
- **L1 and L3 are unchanged.** Hyperliquid was always treated as usable.
- **L5a stays parked,** for economics, not legality.

**2026-10-11 — L5b scout: the bias is real on Kalshi but halving, and it
pays only the maker side, at lending-like returns.**
(`research/copytrade/scouts/l5b-kalshi.md`; the paper was read in full.)
- **The evidence.** In Bürgi, Deng & Whelan's sample (2021 → 2025-04;
  46,282 contracts), favourites at 91–99¢ returned +0.5% per trade for
  takers (after fees) and +1.8% for makers (who paid no fee then). The bias
  is strongest in crypto contracts and not significant in politics or
  entertainment.
- **It is shrinking.** The slope was 0.048 in 2024 and 0.021 in
  Jan–Apr 2025.
- **Cost arithmetic.** At a 3-month lock-up the cash hurdle is about 1% of
  stake. The taker side fails (−2%/yr excess). The maker side clears it
  (+3.4%/yr) only if the edge has not halved. At ≤ 10-day horizons the
  hurdle vanishes, and market size binds instead: the median contract
  traded $9k in total, and the median trade was $35.
- **Access.** Kalshi's help pages accept non-US users with ID KYC. Two third
  parties read Germany as not restricted; sign-up is unverified. Collateral
  earns no interest outside the US. Polymarket is close-only for Germany
  (venue geoblock).
- **Data.** The archive endpoints need no key (one date-bounded probe
  returned 200). Full history is about 15–20 GB.
- **Reading.** The attraction is a clean test: the rule is fixed by a
  published paper, and every contract after 2025-04 is out of its sample
  and unread by us. The drawback is economics. The best case is about
  lending plus a few points, on tens of thousands of dollars. It is ranked
  after the three drafts. Registration 011 would test the paper's maker
  rule (favourites ≥ 90¢, ≤ 10-day horizon), discovery 2025-05 →
  2026-06, 2026-07 → sealed. The pull (post-paper period only, size to be
  listed first) goes to the owner with the next decision bundle.

**2026-10-11 — 007 review: not ready. A distribution is not a closure;
the holdout moves to two clean windows.** The independent review
(`registrations/reviews/007-review.md`) found one blocker, which the
orchestrator re-counted from event types and times: 61 of 196 vault
ledgers show a distribution, 15 show two or more, and 16 took deposits
after their first. Treating a first distribution as closure would have cut
returns at a leader-chosen time and leaked lookback NAV into one outcome.
Orchestrator decisions:
- **Closure** is defined by behaviour, and distributions are outflows.
- **No grid point before S, ever.** Vault-windows that cannot be measured
  are imputed neutrally and counted, and the run stops if they exceed 5%.
- **Market factor.** It enters the null and power simulations, and
  criterion 1 is weighted within long-bias terciles, as in 001 T1.
- **HLP** is compared over the same unscaled interval.
- **Holdout** is S = 2026-07-06 and 2026-08-31: no overlap with discovery,
  read after 2026-10-26.
- **Deposits open at S:** the eligible set requires evidence of outside
  depositors, as recommended to the owner.
- **Criterion 2:** two versions go to the owner with their power.
- All minor findings are fixed. Count reads are recorded in `000e`.

**2026-10-11 — 007 Revision 1: fixable design, weak holdout.** All
first-review findings were addressed. The tests pass (89/89). Eligible
vault counts per window, full set / outside-depositor set:
99/79/63/72 and 74/64/50/54. No vault-window is unmeasurable.
- **Discovery joint power** (outside set, NAV attenuation and the market
  factor included): 0.55 at IC 0.2 with criterion 2 as D > 0, and 0.36
  with D/SE ≥ 1.64.
- **The bear-year null** (score correlated −0.4 with exposure) passes
  criterion 1 in 30% of runs uncontrolled. Exposure terciles bring it to
  12%, and the clustered Z to 5%. All five criteria together pass 1% of
  the time.
- **The holdout is weak.** Confirm power is 0.18–0.27 at IC 0.2, and end
  to end 0.13 at IC 0.2 or 0.42 at IC 0.3. As designed, the holdout can
  confirm only a large effect.
- **Second review** checks the drafter's random-effects clustered Z (τ²
  over 4 windows risks being anti-conservative), the behavioural closure,
  the entry rule, and a holdout extended with forward windows as an owner
  option.

**2026-10-11 — 007 second review: two fixes, and the holdout becomes the
owner's choice.** (`registrations/reviews/007-review-r1.md`)
- **M-a.** The drafter's random-effects clustered Z was a 3-df t-test
  compared with 2.33. Its null passed up to about 5% at a nominal 1%.
  Criterion 1 now also requires a t-test on the four window ICs at 3.18.
  This costs about 0.12 of joint power.
- **M-b.** Closure depended on activity after the hold. The exit is now
  decided only by in-hold behaviour, and a capital base under $1k ends the
  measurement.
- **M-c.** Under an exposure-tilted null, the two-window holdout in one
  rally confirmed 3–12% of the time. CONFIRM now uses a market-adjusted
  outcome with a sign rule and a common 14-day grid entry. Two holdout
  windows (read after 2026-10-26) versus four (end fixed now at
  2027-02-15) goes to the owner with power.
- **Also.** A hung 010 fetch (a sleep stall, 6 h at 0% CPU) was killed by
  the orchestrator, and the drafter was resumed with timeouts.

**2026-10-11 — 009 drafted: the costs decide, not the IC.**
- **Pull.** 257 symbols, 131,965 zips, 1.527 GB, 0 checksum failures,
  nothing on or after 2024-01-08.
- **Family.** F3 dropped (no citable weekly sign, and it overlapped
  001 T3), so the family is 2.
- **Behaviour facts, verified by the orchestrator from `describe.json`.**
  - F1 (funding carry): weekly rank autocorrelation 0.58, 2.05 turnover a
    week, **10.6%/yr cost**; 6.6%/yr with a 40% buffer.
  - F2 (7-day OI growth): autocorrelation **−0.20**, 3.48 turnover a week,
    **17.8%/yr cost**. The rank reverses week to week.
- **Power.** The weekly book's standard error of about 25 bp binds. End to
  end, F1 passes 0.09 at IC 0.03 and 0.51 at IC 0.05.
- **Orchestrator decisions, before review, from behaviour only.**
  - The buffer is the primary rebalance rule.
  - The 7-day F2 is rejected on turnover. A 28-day OI growth replaces it
    only if its buffered cost is ≤ 15 bp a week and its rank
    autocorrelation is ≥ 0.5; otherwise the family is F1 alone at z 1.645.
  - Every citation is verified online or dropped.

**2026-10-11 — 007 ready for the owner.** Revision 2 fixed the second
review's M-a (window-IC t ≥ 3.182) and M-b (exit by in-hold behaviour only;
a test perturbs all post-hold data across 300 vaults and no outcome
changes). The orchestrator re-ran the 101 tests and checked the power
tables against `power_r2.md`.
- **Discovery joint power,** recommended version (a): 0.10 / 0.51 / 0.86
  at IC 0.1 / 0.2 / 0.3. The null passes 0.4%, and the bear-tilted null
  1.7%.
- **Holdout CONFIRM at IC 0.2:** A 0.26, B 0.43. The rally-tilted null
  confirms ≤ 0.4%.
- **End to end at IC 0.2:** A 0.13, B 0.22.
- **Recommended:** B, (a), outside depositors, and the market-adjusted
  outcome reported (not required) in discovery.
- **Overall odds that 007 yields a confirmed deployable edge:** about 1 in
  10, at a cost of compute plus a few dollars of holdout data.

**2026-10-11 — 009 Revision 1: F2 survives the turnover gate narrowly,
and half of it is reversal.**
- **Buffered costs.** F1 12.7 bp a week; the 28-day F2 (OI value) 14.6 bp
  (gate 15), with rank autocorrelation 0.524 (gate 0.5).
- **F2 is half reversal.** Its rank correlates 0.53 with minus the past
  28-day return, because OI value contains price.
- **Orchestrator decision.** F2 primary moves to OI in *contracts* under
  the same gates. Otherwise the value version stays, with a residualised
  variant reported.
- **Citations.** F1's sign rests on verified analogues (Koijen et al. 2018;
  Schmeling, Schrimpf & Todorov 2023) plus mechanism. F2's is mechanism
  only. Basu & Miffre was removed (the opposite sign).
- **The economic bar.** About 60–110 bp a week gross for the book
  criterion at 50–80% power, so discovery detects only a large factor.
  End to end at weekly IC 0.03: 0.09 for each factor. A fail will read
  "not detected at that size".
- **Power-simulation bug.** The tie-unaware Spearman inflated the null; it
  was fixed and tested, and no real-data IC was affected.

**2026-10-11 — 010 drafted.**
- **The rule.** At 00/08/16 UTC, enter when the trailing-24 h annualised
  spread is at least θ ≈ 12%/yr. Short the high-funding venue, long the
  low, 1× per leg, taker on all four fills, fixed hold H = 13–18 days.
  H and θ come from cost arithmetic (C = 21–29 bp; margin m = 6%/yr on
  notional, which is HLP's lending leg on gross capital), not from data.
- **Discovery counts.** T1 has 4,867 positions (323 coins, 81 weeks). T2
  (core coins) has 121.
- **Power.** The binding criterion is annualised ≥ 3%: power 0.49 at a
  true 3%, 1.00 at 4%. That is optimistic, given Gaussian noise.
- **Gap-model error.** 1.3–1.9 bp for core HL pairs; about 7–11 bp for alts,
  by analogy.
- **ρ = 0.071** is the cross-coin correlation of spreads, not persistence;
  checked by the orchestrator.
- **For the reviewer.** In the L2 holdout window the drafter read levels:
  spread SDs and quantiles, ρ, HL price-gap levels from 2025–26, and
  entry counts.
- **The HL fetch.** It stalled for 6 h after the machine slept, and was
  killed and resumed with timeouts. 0 holdout rows were received.

**2026-10-11 — 009 Revision 2: F2 in contracts clears the gates
narrowly.** Verified by the orchestrator in `describe.json`:
- buffered cost 14.93 bp a week (gate 15);
- rank autocorrelation 0.516 (gate 0.5);
- Spearman with minus the past 28-day return 0.20 (the value version had
  0.53);
- contracts vs value 0.90.

The variants tried on behaviour data only are all recorded: 7-day value
(rejected on turnover), 28-day value (reversal-laden, now reported only)
and 28-day contracts (primary). Power: end to end at weekly IC 0.03 is 0.09
(F1) / 0.08 (F2), and at IC 0.05 0.52 / 0.48. Under the null, P(pass)
≤ 0.013. 88 of 88 tests pass. Independent review started.

**2026-10-11 — 010 review: three blockers, and the L2 holdout is
spent.** (`registrations/reviews/010-review.md`; B1 confirmed by the
orchestrator.)
- **B1.** The draft's decision funnel split qualifying decisions by "already
  held". That is a forward persistence statistic of the payoff: about 51% vs
  1.9% in discovery, 36% vs 0.6% in the L2 holdout. The orchestrator's brief
  invited it by allowing counts without naming the forbidden conditioning.
  Ledgered as `000f`. **2025-01 → 2026-06 can no longer decide 010.** It
  becomes a reported, exposed block. The decisive holdout is unread
  funding from 2026-07-01, with an end date for the owner to choose (A:
  2026-09-30, data exists; B: 2026-12-31).
- **B2.** OKX delisted instruments are missing, so OKX legs are dropped
  from the primary.
- **B3.** Liquidation loss was understated by 2–30×; it is now modelled as
  the whole short-leg margin.
- **M1–M4.** Criterion 5 stays a 3%/yr point bar, with a month-clustered t
  at t₁₈ = 2.10. HL candles for 2023–24 are checked for the price gap.
  Slippage is tiered by volume. The holdout mode is built and hashed now.
- **Odds.** The exposure tells us the funding leg persists half the time
  after entry. Whether the carry *nets* after costs, gap moves and
  liquidations is still open. Effect-exists odds rise (2 in 3 → 3 in 4).
  Deployable stays 1 in 3: the compressed 2025–26 regime and capacity
  decide that.

**2026-10-11 — 009 review: F2 dies on its own cost gate, and 009 must run
before 010.** (`registrations/reviews/009-review.md`)
- **B1, the overlap.** 010's `data/carry/` holds Binance funding and prices
  over 009's holdout A, outside 009's guard. Ledgered as `000g`.
  **Ordering rule:** no 010 outcome on Binance data from 2024-01-01 on until
  009's holdout A is read or closed. 009 runs first, and neither reads the
  other's data directory.
- **M1, drift costs.** Re-sizing drifted holdings adds about 1.8 bp a week,
  so the 28-day contracts F2 costs about 16.8 bp and fails the 15 bp gate,
  whose intent was the full cost. **F2 is dropped**, with no more variants:
  that would be gate-chasing. Four variants are on record, all rejected on
  behaviour. **The family is F1 alone, z = 1.645.**
- **M2.** F1's book has about −0.1 beta. A market factor goes into the
  simulations, and a beta-adjusted net > 0 becomes a condition of the
  holdout-A pull.
- **M3.** The holdout-A path is built and guarded now (it needs a pass row
  plus an owner approval line).
- **M4.** Two outcome-adjacent statistics are disclosed (`000g`).
- **M5.** A positive net at 2× slippage becomes a condition of the
  holdout-A pull.
- **ICP.** It was wrongly excluded by a metrics gap. F1 needs no metrics.
- **Checked sound:** point-in-time, the funding sign, delisting exits, the
  tie fix, the split, and no gate-chasing up to Revision 2.

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
| Binance USDⓈ-M metrics + funding + 1h klines, all USDT perps | `data/binance_years/` (`MANIFEST.tsv`) | 2021-11-01 → 2024-01-01 (discovery of 009) | 257 symbols, 1.527 GB, checksummed |
| HL / Binance / Bybit / OKX funding, premium, klines for carry | `data/carry/` | 2023 → 2026-06-30 | 010's data; 0 holdout rows dropped |
| Vault ledgers + truncated `allTime` | `data/vaults/` (`manifest_r1.json`) | → 2026-06-30 | 007's data; values unread |

## 6. Scouts in flight

| scout | file | started | state |
|---|---|---|---|
| L1 vaults | `scouts/l1-vaults.md` | 2026-10-11 | done; exposure ledgered as `000c` (counts only) |
| L2 carry | `scouts/l2-carry.md` | 2026-10-11 | done; exposure ledgered as `000d` |
| L4 Binance multi-year | `scouts/l4-binance-years.md` | 2026-10-11 | done |
| L5 desk | `scouts/l5-desk.md` | 2026-10-11 | done |
