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
| A1 | Be the house: perp-DEX LP pools and protocol vaults (counterparty to persistent losers, fact 1) | scout running | — | — | — |
| B1 | US equities/ETFs/rates: scheduled and structural flows, executed on `xyz` perps / CFDs / futures | scout running | — | — | — |
| C1 | Crypto trend following (N7-1) and convex crypto exposures: a test with power | scout running | — | — | — |
| D1 | New-perp listings: squeeze tail by mechanism vs 013 T2 frozen, for the unread confirmation calendar | scout running | — | — | — |
| E1 | Structural payers beyond trading (BNB rewards, dollar yields, points, event contracts, futures/FX premia, pre-IPO perps) | scout running | — | — | — |

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
| A1 scout (house side) | `research/alpha/scouts/a1-house-side.md` | 2026-10-13 | running |
| B1 scout (equities, rates) | `research/alpha/scouts/b1-equities-flows.md` | 2026-10-13 | running |
| C1 scout (crypto trend) | `research/alpha/scouts/c1-crypto-trend.md` | 2026-10-13 | running |
| D1 scout (listings squeeze) | `research/alpha/scouts/d1-listings-squeeze.md` | 2026-10-13 | running |
| E1 scout (structural payers) | `research/alpha/scouts/e1-structural-payers.md` | 2026-10-13 | running |
| 008 attempt-3 timer | — | 2026-10-13 | fires 2026-10-14 06:45 +0200 |
