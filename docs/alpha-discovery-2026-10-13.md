# Alpha-discovery programme, round 4 (any market, fresh perspective)

**Started 2026-10-13, to be picked up cold.** Brief:
`docs/alpha-discovery-orchestrator-prompt-2026-10-13.md` (holds the owner's
mandate). Predecessors: `docs/edge-discovery-2026-10-12.md` (round 3),
`docs/edge-discovery-2026-10-11.md` (round 2),
`docs/forced-flow-research-horizons-2026-10-10.md` (round 1). Benchmark:
`docs/hlp-benchmark-2026-10-10.md`. New research root `research/alpha/`
(registrations from 015, ledger from `000p`); the old root
`research/copytrade/` keeps 001–014 and `000`–`000o`. One trial ledger spans
both. Branch `alpha-discovery`; commits are local, the owner pushes. Nothing
is published (AGENTS.md rule 5).

**Goal (owner).** "Aim for 100% a week, without overfitting." Read as a
search direction: convex payoffs, high-turnover edges with real per-trade
expectancy, levered premia with a measured tail, small-capacity high-Sharpe
edges. Plausible landing zones: 30–100%+/yr with 30–50% drawdowns, or a
small-capacity edge with a high Sharpe.

**Benchmark.** HLP: +19% in the spiral year, 2–4%/yr calm, Sharpe 1.4, max
drawdown −2.5%. Its lending leg: about 3%/yr.

---

## 1. Lights and status

| id | light | status | effect exists | deployable | vs HLP |
|---|---|---|---|---|---|
| 008 (L3) | Inverted 001 T3 positioning, HL holdout only | REGISTERED; fetch attempt 3 ≥ 2026-10-14 06:40, attempt 4 ≥ 24 h later, then the read | 1 in 5 (gross) | 1 in 20 (taker book never traded) | a confirm only earns a cost-reduced follow-up |
| A1 | Be the house: perp-DEX LP pools and protocol vaults (counterparty to persistent losers, fact 1) | **scout done; no trial** — an allocation with a venue-exploit tail (5–10%/yr est.), NAV test unpowered (~9 yr for t 2) | LLP 0.6 | LLP 1 in 6 | HLP-like; owner option: HLP + LLP sleeve ≤ 25% per venue |
| B1 | US equities/ETFs/rates: scheduled and structural flows, executed on `xyz` perps / CFDs / futures | **scout done; 015 (month-end liquidity cycle, settlement-shifted) to be drafted**; auctions and FOMC weeks held; overnight drift, PEAD, `xyz` carry dead or lending | 1 in 3 | 1 in 5 | ≈ 5%/yr at half effect, 1.2×; 11–25%/yr at full effect, 1.2–2.6× |
| C1 | Crypto trend following (N7-1) and convex crypto exposures: a test with power | **scout done; 016 (paper rule verbatim, forward-registered to 2027-12-31) to be drafted**; existence power 0.49 at SR 1, decay test 0.76–0.88 | 1 in 3 | 1 in 5 (return), 1 in 15 (risk-adj.) | ≈ +16–18%/yr at 25% vol if alive at half strength; month tail −30 to −40% |
| D1 | New-perp listings: squeeze tail by mechanism vs 013 T2 frozen, for the unread confirmation calendar | **scout done; parked, no trial** — best trial is T2 frozen, P(confirm) ≈ 0.09, ≈ 0–4%/yr at the robust size after the peso charge; calendar stays unread | 1 in 3 | 1 in 20 | below HLP |
| E1 | Structural payers beyond trading (BNB rewards, dollar yields, points, event contracts, futures/FX premia, pre-IPO perps) | **scout done; no trial** — all lending-like today; BNB stack ≈ 2.5%/yr hedged at the 2026-H1 pace | 9 in 10 (paid) | 1 in 4 | below HLP calm |

## 2. Family counts and holdouts (carried over)

- **HL discovery year (2025-07-28 → 2026-06-30):** 16 trials, no pass.
- **HL holdout:** 008 is the first and only reader of 2026-07-01 →
  2026-09-28 (D13 amending D18). 2026-09-28 → 2027-06-30 stays unread.
- **Binance new-perp listing windows:** 2 trials (013), no pass. The
  confirmation calendar (listed 2025-07-28 → 2026-06-21) is unread; lock
  lifted.
- **Deribit options:** 0 trials (014 parked); both calendars unread.
- **Binance 2021–23:** 1 trial (009). **L2 carry:** 3, closed.
- **HL `xyz` dex:** 0 trials; shares account-scoring selection with 001.
- **2026-07-01 → is held back for every new light,** unless a registration
  claims it openly. The 2026-07 → 2026-09 crypto market was looked at by
  other studies (`000l`); any registration on those months states it.
- **New data starts its own count,** stating the selection it shares.

## 3. Direction log

**2026-10-13 — round 4 opened; reading list done; branch and research root
created; five scouts launched in parallel; 008's timer armed.**
Read in the brief's order: round 3 end to end plus its next prompt, round 2
end to end, round 1 §1–2 and §9, the HLP benchmark §1, every registration's
Result (001–005, 007, 009, 010, 012, 013), 014's PARKED note, 006, the
ledger `000`–`000o`, the N7 desk scout in full (N1–N3 through round 3's
log), spec §2, AGENTS.md.

*What I take from three rounds.* Every premium we found was real and
visible, and in every case costs or a jump tail took it at a size we could
survive (fact 8). So this round ranks lights by **who pays, how much after
costs, and what the tail does at a robust size**, before any trial is
spent. The one high-powered positive fact (loser persistence on HL) was
never monetised because every design tried to *follow* someone. The
structural way to monetise persistent losers is to be their counterparty:
HLP does exactly that and is our benchmark. So house-side products are
scouted first (A1).

*Ranking and why (before scouts report).*
1. **A1, be the house.** Directly monetises fact 1; HLP proves the shape
   (+19%, Sharpe 1.4); there may be pools with higher returns and a
   hedgeable beta. The risk is the tail (exploits, oracle attacks, trader-win
   spirals), which must be priced.
2. **C1, trend following.** The only light with a plausible path into the
   30–100% zone; the problem is power, not data. The scout designs a test
   that can actually see it, or says when one can.
3. **D1, listings.** The best economic shape of round 3 (median +9% per
   event); a mechanism fix for the squeeze tail could lift the mean. One
   trial on an unread calendar.
4. **B1, equities and rates.** Free data with the new latitude; cheap
   execution on `xyz` perps; month-end liquidity has 150 unread post-paper
   months. Probably 5–15%/yr at a sane size, with a real test.
5. **E1, other payers.** Mostly lending-plus; scouted so that nothing is
   missed and so that the BNB reward stack gets its behaviour step.

Rejected at the start, by established facts: anything that follows or
copies traders (1), forced-flow capture by placement or after the flow (2),
taker-executed funding carry (3), resting liquidity at a distance on HL (4),
and short-dated short vol at an unpriced size (6).

*Job 1 (008).* Attempts 1–2 ran on 2026-10-13 (ends 05:24 and 06:36 +0200).
Attempt 3 is allowed from 2026-10-14 06:40 +0200; a background timer fires
at 06:45. The 008 guard refuses on any untracked or modified `.py` at the
`research/copytrade/` top level or in `ct/`; all new code lives under
`research/alpha/`, and every scout brief forbids writing under
`research/copytrade/`.

**2026-10-13 — A1 scout: being the house is an allocation with an exploit
tail, not a testable alpha; no trial.** (`research/alpha/scouts/a1-house-side.md`;
the orchestrator re-checked every stored pull: checksums pass, 15 series,
all end 2026-06-30, 0 rows on or after 2026-07-01; LLP 529 daily rows from
2025-01-18, GMX GM 1,092 from 2023-07-05, JLP 259 days × 3 series.)
- **Two kinds of house.** Backstop vaults (HLP, Lighter LLP) are long the
  spiral tail. GLP-style basket pools (JLP, GMX GM/GLV, Aster ALP, Avantis)
  carry basket beta; hedged, they earn fees plus traders' net losses but are
  short traders' winning trends (the opposite skew to HLP).
- **The tail is the venue, not the market:** in about 16 months, GMX v1 GLP
  drained (2025-07, ~$40 M), Lighter outage losses (2025-10), Paradex
  rollback (2026-01), Drift admin-key drain (~$285 M, 2026-04), Ostium OLP
  −72% (2026-07, post-cutoff, ledgered). Impairment rate estimated (not
  measured) at 5–10%/yr per venue → ≤ 20–25% of capital per venue.
- **Power.** A NAV-level LLP-vs-HLP test needs about 9 years for t = 2 on a
  10%/yr gap (P&L sits in ~2 spiral events a year). Only the calm-carry
  component (ex-spiral days) is testable, at about t 2.5 on 11 months.
  Hedged GLP pools are powered only for "fees exceed hedge costs", a
  lending-plus claim.
- **gTrade rejected on mechanism:** traders' losses go to a buffer and GNS
  burns, not to depositors (the loser leg of fact 1 is not ours).
- **Decision.** A1 spends no trial. What it offers the owner is an
  *allocation*: an HLP + Lighter LLP "house sleeve", each capped at ~20–25%
  of capital, with the exploit tail stated, as a better-diversified
  benchmark-like holding. Expected return is HLP-like (2–4%/yr calm, spiral
  upside), so it does not move us toward the goal; it is recorded as an
  owner option, not pursued. Odds LLP beats HLP risk-adjusted: about 1 in 6.

**2026-10-13 — D1 scout: the best listings trial is T2 frozen, and it is
not worth the calendar; parked, no trial.** (`research/alpha/scouts/d1-listings-squeeze.md`;
the orchestrator re-read §3 and `data/scout_d1/derived/power_calib.json`,
checksums pass.)
- **Mechanism options.** 0.5× margin rejected: on seasoned coins (proxy),
  after a +90% touch the median 7-day exit is still ×1.6–1.8 and 11% reach
  ×2.86, so it saves ~0.2 of notional per squeeze while doubling the loss
  bound. A +50% close-triggered stop is the best construction from
  mechanism, but its effect on the mean has no sign (Kaminski & Lo); it cuts
  the plausible worst month from −29.7% to −20.2%, so it is a sizing
  overlay. Float/FDV has no free point-in-time source; spot-first has no
  variation (0 of 148 confirmation events); a liquidity filter is
  contaminated by 013's printed tiers.
- **Power is set by event count and body noise, not by the tail:** ≈ 58 T2
  events, α SE 3.7–4.7%, P(pass) 0.07–0.14 at the discovery α (3.4%),
  ≈ 0.09 averaged over a shrunk posterior. Critical value ≈ 2.2 (to be
  re-derived).
- **Return at the robust size** (2.5% per event, ~55 a year): +3 to +7%/yr,
  about **0–4%/yr after 013's peso charge**. Odds ≈ 1 in 10 to confirm,
  1 in 20 to reach paper.
- **Decision: parked under the rule that parked 014** ("a confirm cannot
  beat HLP at a tail-respecting size → no trial"). The confirmation calendar
  (Binance new perps listed 2025-07-28 → 2026-06-21) stays unread. If a
  later light needs listing windows with real upside, the T2 frozen +
  stop-overlay sketch (§4 of the scout) is ready, and the sealed 2026-H2
  listings could be claimed openly to add ~15–20 events.
- **Exposure, ledgered in `000q`:** the scout computed post-entry path
  quantities (touch rates, exits after a +90% touch, stop and margin rule
  differences) on **seasoned** coin-weeks of 009's Binance panel
  (2021-11 → 2024-01; no listing windows). That is a continuation-after-large-move
  look: any trend or momentum registration on Binance 2021–23 states it.

**2026-10-13 — E1 scout: every structural payer found is real, and every
one is lending-like at our size today; no trial.**
(`research/alpha/scouts/e1-structural-payers.md`; the orchestrator recounted
the reward table: 97 events to 2026-06-30, none edited after the cutoff,
checksums pass; small count discrepancies recorded in `000r`.)
- **BNB reward stack (N7-2), behaviour step done.** HODLer airdrops 5 / 54 /
  7 projects (2024-H2 / 2025 / 2026-H1), Launchpool 21 / 8 / 1 (2024 / 2025 /
  2026-H1), Megadrop ended 2025-04. Reward size per airdrop halved (median 2%
  → 1% of max supply). Launchpool pool size is public (16.6–19.1 M BNB); the
  HODLer pool is not, but shares the same Simple Earn base. Hedge feasible
  (HL BNB perp, ~$105–143k within 10 bp). **Hedged return: ~5%/yr at the
  2025 pace, ~2.5%/yr at the 2026-H1 pace, over half of it ordinary funding
  carry.** The payer's decay is measured, not assumed. Venue tail
  (Binance freeze) unpriced; size as money one accepts losing.
- **Crypto dollar yields** (sUSDe, Pendle PT, Aave/Morpho/Sky): lending,
  3–6%/yr, with documented depeg/insolvency tails (USDe $0.65 on Binance
  2025-10-10; xUSD −77% 2025-11-04).
- **Event contracts:** no access change since `l5b`. New reachable venue:
  HL HIP-4 outcome markets (live 2026-05-02, 7/4 bp); no bias study, two
  months of pre-cutoff history.
- **Commodity carry/momentum:** CFD financing (~4%/yr each side) exceeds the
  premium; futures net ~5.5% at 15% vol, test power 0.30–0.55, and full
  per-contract history is not keyless (Nasdaq Data Link needs a free key →
  owner's call; not requested, since even a pass is lending-plus).
- **Rejected:** points/airdrop farming (one-off bets, rules set after the
  fact: not testable), HL pre-IPO perps/hyperps (funding ×0.005 so nobody
  pays carry; 3 pre-cutoff conversions), maker rebates (unreachable),
  CFD carry, insurance underwriting.
- **Decision.** No trial. The BNB stack is recorded as an owner option at
  ~2.5%/yr hedged today, rising only if Binance's listing cadence returns.

**2026-10-13 — B1 scout: scheduled institutional flows are testable on
149 unread months; 015 (month-end liquidity cycle) to be drafted.**
(`research/alpha/scouts/b1-equities-flows.md`; the orchestrator recounted the
month-end calendar (150 rows, 149 complete, settlement lags 44 / 80 / 25)
and checked the power arithmetic: t 2.67 at 85 bp over 149 months implies a
window SD ≈ 3.9%, consistent with 1.12% daily vol over ~13 days.)
- **Month-end liquidity cycle (Etula et al. RFS 2020), shifted by the
  settlement lag** (T+3 → T+2 on 2017-09-05, SEC 34-80295; → T+1 on
  2024-05-28, SEC 34-96930): 2014-01 → 2026-05 is after the paper's sample
  and unread by us. Long/short power 0.85 at the published 85 bp, 0.38 at
  half. Cost ≈ 1–2 bp per round trip on MES or the `xyz` SP500 perp.
  **Return:** ≈ 5%/yr at half effect and 1.2×; ≈ 11%/yr at full effect and
  1.2×, 25%/yr at 2.6×. Tail at 1.2×: worst week −13 to −17%, month −15 to
  −20% (documented two-day S&P falls −10.5% 2025-04, −13.9% 2020-03).
- **Pension-rebalancing front-run (Harvey, Mazzoleni & Melone 2025):** SR
  1.11, skew +5.2 over 1997–2023, the one convex payoff here. But the
  authors' sample (extended to 2025) covers our whole calendar: a pass is a
  replication; the post-publication span (~16 months) has t ≈ 1.15 at SR 1.
- **Treasury auction cycle (Lou, Yan & Zhang):** the best-powered test here
  (2y/5y t 3.5–4 at the full effect), but futures-only and ≈ 5%/yr at a
  robust size. Held.
- **Dead or lending:** overnight drift (≈ 0 in 2021–25 per its own authors,
  and below `xyz` cost even at full effect), `xyz` funding carry (≈ +1%/yr
  over T-bills, with weekend-gap and venue tails), PEAD (large caps, gone),
  pre-holiday, an earnings-week premium on `xyz` names (data and
  survivorship block it). FOMC even weeks: one paper, no post-2016
  replication; held.
- **Decision.** Draft **015: the settlement-shifted month-end liquidity
  cycle**, one trial in a new family (US equity calendar flows, count 1).
  Why spend a trial here when its likely landing is 5–11%/yr: it is the only
  light of the round that is (i) testable with power on a calendar nobody
  in this programme has read, (ii) almost free to execute, (iii) leverable
  with a documented tail, and (iv) carries a **mechanism prediction** (the
  window moves with the settlement lag; *correction after review A: the
  primary cannot separate it from the unshifted windows, and the paired
  contrast has power 0.15–0.22, so the mechanism is reported, not decided*).
  A confirm at near-full effect is the round's first credible 20%+ path at
  2–2.6×. HMM rebalancing goes in as a pre-stated **reported replication**
  (decides nothing); the 2026-07 → calendar stays held back for a forward
  confirmation of either. Exposure ledgered as `000s`.

**2026-10-13 — owner: fix blockers instead of parking around them; 014
review B's B1 fixed (`ed8f06b`).** The owner, on 014-review-b B1: "fix it
instead and ask me if such a blocker comes along the way and why it was
blocked." The fault: the holdout gate used `git show HEAD:results.jsonl`
and `git show <c>:<rel>`, which git resolves from the repository root, so
with the runner in `research/copytrade/` the gate could never open. Fix:
`<rev>:./<path>`. A new test runs the guard checks in both layouts; the
real-layout run failed before the fix exactly as reviewed and passes after
(144/144). **Blocked on the way:** updating `r014_run.py`'s SHA-256 pin in
014 §9 was refused by the session's safety classifier ("modify shared
resources": it protects registration pins from silent code swaps); I asked
the owner, who approved the pin update. 014 stays PARKED on economics; its
other review findings remain open. 008's guard re-checked after the commit:
only "fetch first". **Carried forward:** the 015 drafter was told to use
`<rev>:./<path>` and test both layouts, since its runner lives in
`research/alpha/`.

**2026-10-13 — C1 scout: crypto trend following cannot be shown "alive at
half strength" with power this decade, but its decay can be decided by
2027–28; 016 (verbatim, forward-registered) to be drafted.**
(`research/alpha/scouts/c1-crypto-trend.md`; the orchestrator verified the
checksums, that every kline and funding file ends 2026-06, and the power
table by hand: SR 1 → monthly 0.289 × √33 = 1.66; 6.2 years for 80%.)
- **The rule, verbatim** (Zarattini, Pagani & Barbon 2025, read in full from
  the authors' PDF): nine Donchian lookbacks {5…360} on daily closes,
  midpoint trailing stop, size min(25%/σ90, 200%) per lookback per coin,
  averaged; universe rebuilt monthly (≥ 365 days listed, median volume
  ≥ $2 M, top 20, 1/20 each); 10 bp costs, 20% rebalance threshold; spot,
  no funding. **The 25% is per coin: the book ran at ≈ 9% vol** (SR 1.57,
  α 10.8%/yr, β 0.08). Sample 2015-01-01 → 2025-03-19; the paper also
  tested ranks 1–100 over 2020 → 2025-03, so a wider universe is not
  independent (baskets correlate 0.89–0.97).
- **Power.** Out-of-paper 2025-04 → 2026-06 alone: 0.28 at SR 1. Pooled
  with a forward span to 2027-12-31 (33 months): 0.49 at SR 1, 0.79 at SR
  1.5. **Decay against the paper's 1.57:** 0.76 at 27 months and 0.88 at 39
  if the truth is SR 0. The decidable question is "has it decayed".
- **Behaviour.** ≈ 11.6 sub-signal flips per coin-month; turnover ≈ 4.4×
  capital a year → fees 0.20–0.26%/yr; universe funding +5.4%/yr overall,
  −2.2%/yr in 2025-01 → 2026-06; HL lists 19–20 of the top 20 since 2023-12.
  The loader needs a delisting cut (LUNA stays in June 2022's top 20).
- **Return and tail.** At 25% book vol (2.8× the paper's sizing) and half
  the paper's Sharpe: ≈ +16–18%/yr over cash; worst week −20 to −35% (the
  2020-03-12 day was −43% for the equal-weight top 20), worst month −30 to
  −40%. Kelly at 1-in-3 odds ≈ 16% vol; start at the paper's ≈ 9%.
- **Convex alternatives rejected:** vol-managed BTC (beta, nobody pays; kept
  as a factor), long-dated Deribit calls (the VRP costs ≈ 16% of premium per
  6 months, more than the trend edge per unit), equity-perp ↔ crypto
  lead-lag (US names take an outside price; latency-bound).
- **Decision.** Draft **016: crypto trend following, the paper's rule
  verbatim, long-only primary**, a new family (crypto time-series trend,
  count 1). Primary: existence of α against BTC, an equal-weight alt basket
  and a vol-managed basket (β per **calendar** cell, never per trailing-trend
  cell, which would absorb the trend into β), on 2025-04 → 2027-12-31;
  the pre-declared decay test against SR 1.57 alongside. Span (a) is a
  stratum, never the sole decider (designer knowledge; `000t`). The forward
  span 2026-07-01 → 2027-12-31 is claimed openly (`000l`'s momentum sweep
  through 2026-09 stated). Why now: registering before the forward data
  exists is free, it is the only light with a 15–20%/yr path, and a
  decay verdict is itself worth having. Exposure ledgered as `000t`.

**2026-10-13 — 015 DRAFT committed (`012eff7`); two independent reviews
launched.** The orchestrator re-ran the suite: 137/137. Primary: the L/S
month statistic y_m on Mkt-RF with settlement-era means, 149 months; confirm
iff mean > 0, t ≥ max(1.655, **1.91**) (calibrated over 13 regime worlds,
10,000 draws each; the binding world is a 2020-style crash ending on a
pressure-window end), and mean > 0 without the largest month. False pass
0.024–0.049 across the worlds; a two-crash stress world gives 0.086 at 1.91
and would need 2.13 (−0.07 power; D8 for the reviewers). Power 0.69–0.88 at
the full effect, 0.22–0.45 at half. **Weakness stated by the drafter:** the
primary cannot tell the settlement mechanism apart (an effect left on the
unshifted T+3 windows still passes 0.65); the paired contrast has power
0.15–0.22, so it is reported only. Size 1.0× on MES (≈ ¼–⅓ Kelly): +10%/yr at
full effect, +4.9% at half; worst week −20%, month −23 to −25%. Reviews: A
(design, statistics, economics, tail) and B (guard, integrity, data,
calendar; including the sequencing with 016's untracked files, which 015's
shadow-module check refuses on).

**2026-10-13 — 015 review A: register after fixes (no blocker, four
majors); decisions taken, Revision 1 waits for review B.**
(`research/alpha/registrations/reviews/015-review-a.md`; independent
simulator reproduces size and power: 0.772 / 0.290 iid, 0.878 / 0.444 GARCH
t4.)
- **M1 — a pass means "existed after 2013", not "alive now"** (a dead-at-
  publication or decaying effect still passes 0.13–0.29). *Decision:* after
  a pass, paper only; real money needs the Europe confirmation (M4) and a
  forward span of ≥ 31 months with forward t ≥ 1; recent-window and per-era
  blocks are reported.
- **M2 — scale each month by trailing vol** (RMS of the 20 days ending T−9,
  known at entry; floor 0.4%/day). Power at half effect under GARCH t4
  0.44 → 0.65, never lower elsewhere, and less crash-driven. *Decision:*
  accept as the primary (a variance normalisation fixed by convention, not
  by data); recalibrate (reviewer's estimate ≈ 1.82–1.85).
- **M3 — D8:** under the honest null (crash timing independent of
  month-ends) 1.91 has size 0.028 even with two crashes; the "aligned"
  world is not the worst case, and 2.13 still fails the worst pair while
  costing 0.07 power. *Decision:* no extra margin for double alignment;
  calibrate as before; report the Monte Carlo SE.
- **M4 — reported blocks spend fresh samples.** Europe 2014-01 → 2026-05 is
  the only independent replication sample (power ≈ 0.9 at full effect).
  *Decision:* Europe is **held back unread as 015's pre-stated
  confirmation** (same statistic, its own settlement lag, null calibrated
  with the US–Europe correlation), read only after a US pass. The HMM block
  is **dropped** from 015 (its post-publication months stay unread for a
  registration of its own).
- Minors accepted: soften the "fluke cannot fake" wording; `xyz` trails MES
  by ≈ 3.9%/yr (unpaid collateral); the bar uses the T+1-regime volatility;
  the worst 5 days at 1× is −22.0%; rename "016-forward".

**2026-10-13 — 015 review B: register after fixes (1 blocker, 3 majors,
all in the guard); Revision 1 ordered with both reviews' fixes.**
(`research/alpha/registrations/reviews/015-review-b.md`; probes in
`research/alpha/scratch_review015b/`. Sound: 23 hashes, the loader drops
rows ≥ 2026-06-10 before parsing, calendar 149 months / 64 Friday-T / SEC
dates and boundary months, suite 137/137 in a clone at DRAFT and at
REGISTERED.)
- **B1 — the guard is not bound to the real repo.** In a clone with the
  real data symlinked, the real `--check` printed "guard open"; a full run
  in a clone or a linked worktree leaves the origin open (rows vanish with
  the clone or `git worktree remove`). The drafter's own scratch script had
  copied real data into a REGISTERED clone and run `--check`: one flag from
  reading the outcome off the books. **No `--run` touched real data**
  (orchestrator check: `research/alpha/results.jsonl` is empty; the
  scratchpad's `explore_*` files are the 016 drafter's synthetic worlds).
  Fix: pin the absolute repo path, require the main worktree, refuse a
  detached HEAD; clone tests use synthetic trees only.
- **M1** `run(root)` mixed trees (recorded `exists_paper` from another
  tree's data); **M2** `-I` not enforced (a forged `statistics` on
  `PYTHONPATH` turned a null t −1.61 into t 7.75); **M3** the guard trusted
  git's view of the working tree (`--skip-worktree` hid edits). Fixes:
  real tree only; require isolation, check module origins, pin the stdlib
  fingerprint, absolute git with neutral config and no replace objects;
  read the registration from `HEAD:./` and compare `hash-object
  --no-filters` with HEAD blobs.
- Minors all accepted (trailing newline, `showRoot`/`replace` row hiding,
  vacuous loader-gate tests, a narrow restart clause, `DATA_END`, the
  interpreter pin). **016 coupling:** each draft's shadow scan refuses on the
  other's untracked files (correct); 016's draft is committed before 015
  runs.
- **Revision 1** goes to the 015 drafter with every A and B fix; then the
  reviewers re-run their probes only.

**2026-10-13 — 016 DRAFT committed (`66df71a`); two reviews launched.**
The orchestrator re-ran the suite: 186/186 (both guard layouts, clones at
DRAFT and REGISTERED). H1: α of the daily net book (10 bp, no funding)
against BTC, an equal-weight alt basket and a vol-managed basket,
month-clustered (G = 33), t ≥ **1.731** (largest q95 over 11 regime worlds)
**and** α > 0 on the forward span alone, so the designer-known span (a)
cannot carry a pass. Power 0.68–0.83 at SR 1 (0.41–0.54 forward-only);
false pass 0.000–0.048; a world shaped like the known 2025–26 path passes
0.017. H2 (decay vs SR 1.57) has power only ≈ 0.5 at true SR 0 (the
scout's 0.76–0.88 was optimistic: slow trends make a 33-month Sharpe noisy).
**Departure from the brief, for the reviewers:** one slope per factor, not β
per calendar cell (cell slopes absorb the timing gain: −1.1 to −1.6 in t).
Expected over cash, alive at full strength: +13.7%/yr at ≈ 9% vol, +36–38%
at 25%; half strength +6.6% / +16–18%. Real data seen beyond hashes and
dates: one in-paper row (BTCUSDT 2020-01-01) and CSV headers (016 §15).
Reviews: A (β choice, H2, span (a), fidelity to the paper, economics) and B
(every 015-review-b fault, the date gate, the fetcher, the current-state
exchangeInfo snapshot as a survivorship risk).

**2026-10-13 — 016 review B: register after fixes (1 blocker, 4 majors);
the same guard faults as 015, plus one of its own.**
(`research/alpha/registrations/reviews/016-review-b.md`; probes in
`research/alpha/scratch_review016b/`, every attack on throwaway stand-ins
with synthetic data; orchestrator check: no new commit or row in the real
repository, `research/alpha/results.jsonl` empty.) Sound: the fetcher, the
pinned pull (19,730 hashes), the point-in-time universe (no survivorship);
186/186 at DRAFT, at REGISTERED, and with 015's revision in progress; the
date gate cannot be cheated with real data (no `fetched` row can exist
before Binance's archive holds BTC through 2027-12-31).
- **B1** the guard opens in a clone or worktree (the 015 B1 pattern; a
  stand-in ran fetch → approval → read to an outcome). **M1** `fetch()`
  with the real root plus test mocks and `now=2028` burns the trial (it
  commits `fetched` rows). **M2** isolation: no `-I` check; a `.pth` file
  runs even under `-I`; a forged `__pycache__` guard module; a committed
  `research/alpha/csv.py` shadows the parser. **M3** index flags hide
  edits. **M4 (016-specific)** nothing ties the 2028 read to the REGISTERED
  commit: a committed 2027 revision lowering `CAL_H1` to 1.20 passes.
- **Decisions.** All accepted; 016 Revision 1 waits for review A. The new
  faults (`.pth`, committed shadow modules, mocks with the real root in any
  role, binding the read to the REGISTERED commit) were sent to the 015
  drafter now, mid-revision. Minor of note: 7 of 11 deliberately broken
  protections left 016's suite green — the revision must make each test
  fail when its protection is removed. One accidental network request in a
  broken-guard test (`GET data.binance.vision/x`, 404, no data).
- **Programme lesson.** Two guards written in parallel from the same
  templates had the same holes. After 015 Revision 1, 016 ports 015's
  revised guard rather than re-deriving it.

**2026-10-13 — 015 Revision 1 done (every A and B finding plus 016-B's
shared faults); probe-only rechecks launched.** The orchestrator re-ran the
suite: 200/200 (199/199 in a synthetic clone at DRAFT and REGISTERED, where
the real guard refuses only with (w), the repo-binding check).
- **Primary z = y / (√12·σ̂)**, σ̂ = RMS of x over T−28…T−9, floor
  0.4%/day. **Critical value 1.92** (binding world: one 2008-style crash
  aligned to a pressure-window end, q95 1.862 ± 0.026 MC SE, + 2 SE, rounded
  up). False pass 0.024–0.045 over the 13 worlds; placement-averaged crashes
  0.029–0.033; double alignment 0.070 (reported). **Power** full 0.769 iid /
  0.986 GARCH t4; half 0.278 / 0.620. Decayed worlds still pass 0.12–0.27.
- **What a pass buys:** paper only. Real money needs the held-back Europe
  confirmation and **015F** (forward 2026-06 → 2028-12, forward t ≥ 1).
- **Guard:** real repo path pinned, main worktree only, no detached HEAD;
  `run()` takes no root; `python3 -I -S` required, module/bytecode origins
  checked, interpreter and stdlib pinned, unpinned `.pth` refused, committed
  stdlib-shadowing modules refused; `/usr/bin/git` with neutral config, no
  replace objects, no hooks; registration read from HEAD and file bytes
  compared with HEAD blobs; index flags and filters refused; the row binds
  the REGISTERED commit. The clone test itself caught one more hole (the
  suite's real-ledger test wrote a row into a clone's ledger), now refused.
- HMM dropped (`r015_hmm.py` and the Treasury copies removed). The
  overclaim in this document's B1 entry is corrected in place.

**2026-10-13 — 015 Revision 1 rechecks: A says register; B says register
after two majors; Revision 2 ordered.** A: every major and minor fixed;
an independent estimator agrees to 1e-15; size at 1.92 is 0.026–0.049 (the
binding world's q95 1.913: no spare margin); power within MC error of the
registration. B: every round-1 attack and the three 016-B faults refused; a
stand-in of the real layout opens and runs once. **R1-M1:** the REGISTERED
version was searched on first-parent history only, so a post-REGISTERED
revision merged `--no-ff` from a side branch opened the guard (null t −1.80
→ +1.78). **R1-M2:** the restart clause checked authorship in HEAD's history
only: after a runner-committed started row and a real read, `reset --hard`
plus a hand re-commit reopened it. Revision 2 fixes both, opens the data
gate only after the started row is in HEAD, takes μ from all eras for the
routing bar (A), pins the Europe file when it is pulled (A), and records a
`.pth` that swaps `sys.flags` as a stated limitation needing deliberate
interpreter tampering (as 008's forged-`.pyc` residual). Then B rechecks
R1-M1/M2 only, and 015 is registered.

## 4. Distance to goal

**At the start of round 4 (2026-10-13):** unchanged from round 3. Nothing
tested beats HLP. Best point estimate: 013 T2's discovery book, about
+6%/yr at 2.5% per event (α t 1.24, not significant). Best statistically
supported result: 010 T2, +2.96%/yr (the lending rate).

**After round 4's scouting (2026-10-13, no trial spent yet):**
- **Closed or parked without a trial:** house-side pools (an allocation
  with an exploit tail, unpowered as alpha), listings T2 (≈ 0–4%/yr at a
  robust size), every structural payer (lending-like today), overnight
  drift, PEAD, `xyz` carry.
- **Two registrations being drafted:**
  - **015, month-end liquidity cycle** (149 unread months): a confirm at
    the published effect ≈ 11%/yr at 1.2×, 25%/yr at 2.6× (month tail −15
    to −20% at 1.2×); at half the effect ≈ 5%/yr. Odds of a confirm ≈ 1 in
    3. Decidable now.
  - **016, crypto trend verbatim**: if alive at half strength ≈ +16–18%/yr
    at 25% vol, month tail −30 to −40%. Decided 2028-01; decay verdict is
    the likely answer.
- **The honest picture.** The 30–100% zone needs leverage on an edge that
  survives out of sample. Only 015 (at near-full effect, 2–2.6×) and 016
  (alive, 25%+ vol) reach 20%+, each with a 30–40% drawdown budget; the two
  would diversify each other (equity calendar vs crypto trend). HLP-like
  allocations (A1, E1) remain owner options for the lending leg.

## 5. Data inventory

Everything in round 2 §5 and round 3 §5 (all under
`research/copytrade/data/`, gitignored). Round 4 scout pulls go under
`research/alpha/data/scout_<id>/`, each with `SHA256SUMS`.

| asset | where | coverage | notes |
|---|---|---|---|

## 6. Workers in flight

| worker | deliverable | started | state |
|---|---|---|---|
| A1 scout (house side) | `research/alpha/scouts/a1-house-side.md` | 2026-10-13 | done; exposures to ledger `000p` |
| B1 scout (equities, rates) | `research/alpha/scouts/b1-equities-flows.md` | 2026-10-13 | done; `000s` |
| C1 scout (crypto trend) | `research/alpha/scouts/c1-crypto-trend.md` | 2026-10-13 | done; `000t` |
| D1 scout (listings squeeze) | `research/alpha/scouts/d1-listings-squeeze.md` | 2026-10-13 | done; `000q` |
| E1 scout (structural payers) | `research/alpha/scouts/e1-structural-payers.md` | 2026-10-13 | done; `000r` |
| 015 drafter | `research/alpha/registrations/015-month-end-liquidity-cycle.md` | 2026-10-13 | DRAFT committed `012eff7` (137/137) |
| 015 review A (design) | `research/alpha/registrations/reviews/015-review-a.md` | 2026-10-13 | done: 0 blockers, 4 majors (all accepted) |
| 015 review B (guard/data) | `research/alpha/registrations/reviews/015-review-b.md` | 2026-10-13 | done: 1 blocker, 3 majors (all accepted) |
| 015 Revision 1 | drafter | 2026-10-13 | done (200/200); recheck A register, B two majors |
| 015 Revision 2 | drafter | 2026-10-13 | running |
| 016 drafter | `research/alpha/registrations/016-crypto-trend-verbatim.md` | 2026-10-13 | DRAFT committed `66df71a` (186/186) |
| 016 review A (design) | `research/alpha/registrations/reviews/016-review-a.md` | 2026-10-13 | running |
| 016 review B (guard/fetch) | `research/alpha/registrations/reviews/016-review-b.md` | 2026-10-13 | done: 1 blocker, 4 majors (accepted) |
| 008 attempt-3 timer | — | 2026-10-13 | fires 2026-10-14 06:45 +0200 |
