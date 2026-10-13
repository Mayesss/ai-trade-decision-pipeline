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
| B1 | US equities/ETFs/rates: scheduled and structural flows, executed on `xyz` perps / CFDs / futures | scout running | — | — | — |
| C1 | Crypto trend following (N7-1) and convex crypto exposures: a test with power | scout running | — | — | — |
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

## 4. Distance to goal

**At the start of round 4 (2026-10-13):** unchanged from round 3. Nothing
tested beats HLP. Best point estimate: 013 T2's discovery book, about
+6%/yr at 2.5% per event (α t 1.24, not significant). Best statistically
supported result: 010 T2, +2.96%/yr (the lending rate).

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
| B1 scout (equities, rates) | `research/alpha/scouts/b1-equities-flows.md` | 2026-10-13 | running |
| C1 scout (crypto trend) | `research/alpha/scouts/c1-crypto-trend.md` | 2026-10-13 | running |
| D1 scout (listings squeeze) | `research/alpha/scouts/d1-listings-squeeze.md` | 2026-10-13 | done; `000q` |
| E1 scout (structural payers) | `research/alpha/scouts/e1-structural-payers.md` | 2026-10-13 | done; `000r` |
| 008 attempt-3 timer | — | 2026-10-13 | fires 2026-10-14 06:45 +0200 |
