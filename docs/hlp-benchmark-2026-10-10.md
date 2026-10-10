# H7 — the benchmark: Hyperliquid's HLP vault as packaged forced flow

**Written 2026-10-10.** A measurement, not a trial: no registration, no
holdout, nothing in `results.jsonl`, no change to `lib/`, `pages/` or
`vercel.json`. It answers the question in
`docs/forced-flow-research-horizons-2026-10-10.md` §4 H7 and §6.4: does
depositing into HLP already deliver the forced-flow edge that registrations
003–004 kept measuring, and at what risk? Anything built later (trailing
ladder, Binance replication, cross-venue capture) has to beat this.

Code: `research/copytrade/h7_extract.py` (archive extract + public-API
fetches, all cached) and `research/copytrade/h7_analyze.py` (every number
below). Outputs: `research/copytrade/data/derived/hyperliquid/h7/`
(gitignored: `summary.json`, `hourly_pnl.parquet`).

---

## 1. Short answer

- **HLP earned +19.4% over the archive year** (API, 2025-08-06 → 2026-07-08).
  **+17.4% of it came in two fortnights**, the ones holding 2025-10-10 and
  2026-01-31. The other 23 fortnights together made +1.7% (+1.9% a year).
  Daily, after reconciling against the API: Sharpe 1.40, worst day −1.10%,
  max drawdown −2.5%, Sortino 7.6. Almost no downside, all upside in a
  handful of hours.
- **HLP does not deliver the edge we measured. It is long the opposite tail.**
  Its money is the **backstop**: the liquidator child vaults take over
  positions the book cannot absorb (+$58.8 M in the year by the API's own
  child breakdown, +$59.5 M of that in the two event fortnights). Its
  **market making made −$1.8 M**.
- **The 003 edge does exist inside HLP's book, at the same size, and is
  economically nothing there.** HLP's market-making fills against a forced
  print in majors earned **+22.9 bp at 15 min** (gross), next to 003 T1's
  +22 bp. But they total $20 M of notional in a year, about $46 k.
- **In the hour that broke every provider we simulated, HLP made its year.**
  2025-10-10 21:00 UTC: HLP **+$48.5 M (about +10% of TVL in one hour)**.
  003 T1's fills in that same hour averaged about −7.5% each (130 fills).
- **Verdict (§8): at our size HLP is the better risk-adjusted use of capital
  by a wide margin.** A home-built provider has to show realisable net edge
  of ≥ 25 bp per fill in majors at ~500 fills a year, without losing in the
  spiral hours, before it is worth building.

## 2. Sources, and what was checked against what

| item | source | verified against |
|---|---|---|
| HLP address `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | Hyperliquid docs, info endpoint: the `vaultDetails` example response uses this address | stats-data vault list (cached 2026-10-08): "Hyperliquidity Provider (HLP)", relationship `parent`, 7 children; `vaultDetails` today: same name, description "This community-owned vault provides liquidity … multiple market making strategies, performs liquidations, and accrues platform fees" |
| Child vaults | the parent's own `relationship.childAddresses` | each child's `vaultDetails`: Strategy A / B ("component market making strategy"), Strategy X ("component strategy"), Liquidator 1–4 ("liquidates positions on all / liquid / medium / low-liquidity coins as soon as they become liquidatable") |
| Lock-up | docs, Protocol vaults: "withdrawals open 4 days after your most recent deposit" | — |
| Terms | `vaultDetails`: `leaderCommission` 0, `leaderFraction` 0.2%, `allowDeposits` true | — |
| Return series | `vaultDetails.portfolio` (`allTime`: 100 points since 2023-05, median spacing 14 days; `month`: 48 points) | the archive reconstruction below (correlation 0.967 over 23 fortnights) |
| Daily TVL | DefiLlama `protocol/hyperliquid-hlp` (daily from 2024-12) | API account value: 2025-10-01 $425.0 M vs $427.1 M |
| Funding | `userFunding` per child (daily aggregates; ~1,350 cached requests) | — |
| Fills | the local pruned archive, 2025-07-28 → 2026-06-30: every fill of the 8 HLP addresses, with the counterparty row (same `trade_id`) | per-child API pnl (below) |

**Reconstruction.** For each child, coin and UTC hour:
`pnl = position_{h−1} × Δmark + Σ fills q × (mark_h − px)`, with the mark
taken as the hour's last tape trade. Fees and non-fill transfers are not in
the archive.

| leg, archive span | API (`pnlHistory` per child) | reconstructed |
|---|---|---|
| Strategy A | +$2.27 M | +$1.94 M |
| Strategy B | −$4.25 M | −$4.18 M |
| Liquidators 1–4 | +$58.3 M | +$47.5 M |
| parent, not in any child | +$9.7 M | — |
| **parent total** | **+$64.8 M** | **+$50.7 M** (incl. +$0.3 M funding) |

The market-making legs match. The $14 M residual is mostly positive (13 of
23 fortnights, up to +$5 M) and largest in calm stretches. That is consistent with income that is not a
fill: fee accrual booked at the parent, and the margin that moves with a
backstop takeover. The liquidators also hold thin coins, where last-trade
marks are noisy. **Consequence:** the reconstructed daily series reads calm
periods about 3.5% a year too low. Return statements below use the API.
Daily risk statements use the reconstruction with each fortnight's residual
spread evenly over its days ("adjusted").

**A correction to the brief.** `liquidations.parquet` holds $4.5 B of
`Liquidated Cross` rows. **$2.03 B of those are HLP's own liquidator
positions being auto-deleveraged** (the HLP row reads `Liquidated Cross
Long`, the counterparty row `Auto-Deleveraging`): Liquidator 2 $1.30 B and
Liquidator 1 $0.70 B on 2025-10-10, plus small amounts on 2025-11-12 and
2026-04-09. Backstop takeovers of users' positions were **$2.70 B** ($2.41 B
cross, $0.29 B isolated; 20,832 fills). For 002–004 this means the
2025-10-10 cascade notional and the stress-rate series carry about $2 B of
HLP's own ADL in that hour. No verdict changes: the hour is far above the
$77 M switch either way.

## 3. Return series

**By calendar year (API):**

| year | return | P&L | mean TVL |
|---|---|---|---|
| 2023 (from May) | +100.6% | +$1.1 M | $5 M |
| 2024 | +79.1% | +$48.8 M | $154 M |
| 2025 | +19.0% | +$68.3 M | $403 M |
| 2026 to 10-10 | +8.1% (+7.0% of it on 2026-01-31) | +$20.4 M | $301 M |

**Archive year, fortnight by fortnight (API).** The P&L is flat except for
two jumps: 2025-10-01 → 10-15 **+$41.4 M (+9.70%)** and 2026-01-21 → 02-04
**+$18.8 M (+7.00%)**. Losing fortnights: 2025-10-29 → 11-12 −$4.7 M
(−0.80%) and 2026-04-01 → 04-15 −$2.3 M (−0.51%). From 2026-02-04 to
2026-10-10 the vault added **+$0.6 M on $180–450 M**. The last 30 days made
+$0.63 M on $188 M (4.1% a year); the API's `apr` field reads 3.7%.

**Daily (archive, 338 days):**

| | reconstructed | adjusted |
|---|---|---|
| compounded | +14.6% | **+19.6%** |
| annualised vol | 14.6% | 14.6% |
| Sharpe | 1.07 | **1.40** |
| max drawdown | −4.9% (10-14 → 01-30) | **−2.5%** (09-24 → 10-09) |
| worst day | −1.13% (2025-11-12) | −1.10% |
| best day | +11.9% (2025-10-10) | |
| without the 32 days that had a spiral hour | −3.3% | **−0.1%** |

Concentration: the best single day carries 91% of the year's reconstructed
P&L, and the top three carry 138%. 165 of 338 days were negative. The
volatility is upside: downside deviation is 2.7% a year, Sortino 7.6.

**Monthly ($M, reconstructed; ret compounded daily on DefiLlama TVL):**

| month | MM | backstop | total | TVL | ret |
|---|---|---|---|---|---|
| 2025-08 | +0.08 | +1.71 | +1.82 | 475 | +0.37% |
| 2025-09 | +0.32 | +10.12 | +10.48 | 525 | +1.97% |
| 2025-10 | −0.51 | +44.42 | +44.09 | 505 | +11.55% |
| 2025-11 | −0.49 | −8.01 | −8.56 | 515 | −1.58% |
| 2025-12 | −0.57 | −6.69 | −7.26 | 390 | −1.82% |
| 2026-01 | −0.20 | +15.73 | +15.56 | 286 | +5.53% |
| 2026-02 | +0.01 | −0.99 | −0.98 | 353 | −0.28% |
| 2026-03 | −0.03 | −0.03 | −0.05 | 428 | +0.01% |
| 2026-04 | −0.35 | −0.67 | −1.01 | 403 | −0.24% |
| 2026-05 | −0.30 | −1.22 | −1.50 | 382 | −0.41% |
| 2026-06 | −0.34 | −2.03 | −2.36 | 306 | −0.67% |

(The calm-month negatives are mostly the residual: by the API, those same
fortnights were +$0.0–0.3 M each.)

## 4. Decomposition: backstop versus market making

**By the API's child breakdown, archive span:**

| leg | event fortnights | rest | total |
|---|---|---|---|
| market making (A + B) | −$0.2 M | −$1.6 M | **−$1.8 M** |
| backstop (Liquidators 1–4) | +$59.5 M | −$0.8 M | **+$58.8 M** |
| Strategy X | 0 | 0 | 0 (funded after the archive) |
| parent only (fees, transfers) | +$1.0 M | +$8.7 M | **+$9.7 M** |

HLP's carry outside the events, about 2% a year, is the parent's own
accrual. Neither strategy leg makes money in calm periods.

**Per fill, from HLP's own fills (gross; mark = last trade of the minute
+15 / +60):**

| HLP fills | fills | notional | 15 min | 60 min | 15 min, spiral days removed |
|---|---|---|---|---|---|
| MM vs non-forced flow, all coins | 153 M | $38.0 B | −0.05 bp | −0.3 bp | +0.4 bp (alts) / −0.2 bp (majors) |
| **MM vs a forced print, majors** | 16,404 | $20 M | **+22.9 bp** | +32.6 bp | +7.0 bp |
| MM vs a forced print, alts | 211,028 | $120 M | −74.2 bp | +78.0 bp | +43.8 bp |
| **backstop takeover, majors** | 5,775 | $1.76 B | **+576 bp** | +859 bp | 5 fills only |
| **backstop takeover, alts** | 15,057 | $0.95 B | **+976 bp** | +1,906 bp | 574 fills, −262 bp |
| liquidators unwinding (taker) | 288,043 | $0.63 B | −36 to −126 bp | | −10 to −21 bp |

Reading:

1. **Market making is a wash.** 153 M fills, $38 B, about zero markout at 15
   and 60 minutes, before fees and rebates we cannot see. It provides the
   venue's liquidity; it is not where depositors' return comes from.
2. **The 003 effect replicates on HLP's book.** Forced prints that hit HLP's
   quotes in majors earned +22.9 bp at 15 min (003 T1: +22 bp net). Without
   the spiral days it is +7 bp. In alts it is a loss at 15 min and a gain at
   60 min, which is 003's "reversion builds for hours" again. HLP's MM
   barely trades majors ($5 B of $38 B), so this fill type is
   $20 M a year inside a $400 M vault: real, and immaterial.
3. **The backstop is a privileged role, not a placement.** The protocol hands
   positions that the book cannot absorb to the liquidator vaults, at a
   price that leaves 6–10% of room within 15 minutes on the days it matters.
   Outside spiral days takeovers are rare (579 fills, $110 M) and lose. No
   outside order can sit in that queue; the closest outside analogue is
   providing liquidity in the same hours, which is exactly what 003–004 lost
   on.
4. **ADL capped the tail.** On 2025-10-10 the venue auto-deleveraged $2.0 B
   of the liquidators' takeovers against profitable traders. That is part of
   why HLP's worst hours are small: the protocol protects its own vault.

## 5. Spiral hours

Definition as 004 D8: market-wide trailing-5-minute liquidation notional
≥ $77 M at any second in the hour. That gives 39 hours on 32 days. Over
those 39 hours HLP made **+$61.5 M** (backstop +$62.6 M, MM −$1.2 M). Two
hours carry it: without 2025-10-10 21:00 and 2026-01-31 18:00 the other 37
hours sum to **−$2.1 M**, and 22 of the 39 were negative (−$7.8 M).

**The brief's days and their neighbours (whole UTC day, reconstructed):**

| day | peak 5-min liq. | liquidated | HLP | backstop | MM | worst / best hour |
|---|---|---|---|---|---|---|
| 2025-08-01 | $175 M | $629 M | −$0.06 M | −0.16 | +0.10 | 13:00 −0.13 / 14:00 +0.18 |
| 2025-08-18 | $98 M | $471 M | −$0.21 M | −0.25 | +0.04 | 01:00 −0.10 / 12:00 +0.07 |
| 2025-08-25 | $82 M | $650 M | −$0.66 M | −0.74 | +0.08 | 20:00 −0.19 / 16:00 +0.18 |
| **2025-10-10** | **$4,585 M** | **$7,165 M** | **+$45.89 M** | +45.53 | +0.36 | 20:00 −2.00 / **21:00 +48.45** |
| 2026-01-31 | $674 M | $1,073 M | **+$17.15 M** | +17.24 | −0.08 | 17:00 −0.58 / 18:00 +15.14 |
| 2026-02-05 | $70 M (below the switch) | $484 M | −$1.28 M | −1.35 | +0.08 | 20:00 −0.25 / 21:00 +0.08 |
| 2026-02-06 | $72 M | $346 M | +$0.81 M | +0.84 | −0.03 | 00:00 −0.20 / 05:00 +0.31 |

The 2025-10-10 21:00 hour split into MM −$1.53 M and backstop +$49.98 M.
2025-09-22 06:00 (peak $1,053 M) made +$2.62 M. The largest days by coin
were BTC (Liquidator 2, +$8.4 M realised on 2025-10-10) plus a long list of
alts, and ETH (Liquidator 2, +$10.9 M) on 2026-01-31.

**Against our providers.** 003 T1, filling at the median forced print, lost
about −7.5% per fill across the 130 fills in 2025-10-10 21:00. The cluster
alone moved the all-coin mean from +0.49% to +0.13%. 004's ladder posted
nothing in that hour (stress switch) and still could not earn elsewhere.
HLP's sign in that hour is the opposite of ours. On moderate spiral days
(the brief's August days, 2026-02-05), HLP loses small amounts, −$0.1 to
−$1.3 M (≤ 0.4% of TVL). It wins big only when the book genuinely cannot
absorb the flow. **HLP is long the extreme-spiral tail; a resting provider
is short it.**

**HLP's own tail** sits in thin-coin takeovers, not in spirals. Worst day
2025-11-12: −$6.3 M, almost all POPCAT on Liquidator 1 (−$5.5 M realised,
with 336 ADL fills that day). Next, 2025-09-25: −$4.3 M, also backstop.
Each was under 1.2% of TVL.

## 6. Capacity

- **Dollar P&L does not scale with TVL; return does the dividing.** 2024:
  $48.8 M on a mean $154 M (+79%). 2025: $68.3 M on $403 M (+19%). The
  backstop earns what the liquidation events pay, whoever is deposited.
- **TVL has been leaving.** $448 M (2026-04-01) → $181 M (2026-10-10), as
  calm-period returns went to about zero. Between 2026-08-12 and 08-19 **$100 M moved into
  Strategy X**, which earns about 2.9% a year with no perp P&L (USDC lending,
  per the vault description). Today 55% of HLP is that lending leg. The
  spiral exposure per deposited dollar is roughly half what it was in the
  archive year.
- **Terms.** 4-day lock-up from the latest deposit, no leader commission;
  `maxDistributable` $41 M today. Our size (well under $1 M) is irrelevant to
  the vault's capacity and to its return.
- **Risks that do not show in the series.** Everything sits on Hyperliquid
  (chain, bridge, governance), the same venue risk any capture there would
  carry. The protocol can change liquidation routing and ADL rules. Manipulated
  thin-coin takeovers (POPCAT) are the realised tail. And the return is a
  wager on how often a 2025-10-10 happens.

## 7. The comparison that matters

Per-fill evidence from 003/004, as return on capital. Each fill is sized at
a fraction *f* of capital at 1×, and `annual return = f × fills × edge`.
Majors, about 500 fills a year (463 in 338 days):

| | edge per fill (net) | annual at f = 0.15 | spiral hour |
|---|---|---|---|
| 003 T1 majors, fill at the median forced print (ex-post upper bound, not placeable) | +22 bp (t 3.3) | **+15–16%** | included in the mean; all-coin version lost −7.5%/fill × 130 fills |
| 004 T1 majors, realisable ladder | −2 bp (t −0.2) | **−1.5%** | stood aside (switch) |
| 004 majors, fixed 60-min exit (reported, not tested) | +16 bp (t 1.5) | +11% | — |
| HLP's own MM vs forced prints, majors | +22.9 bp gross (+7 ex-spiral) | — | $20 M of fills a year |
| **HLP deposit** | — | **+19.4%** archive year; **+1.9%/yr** without the two events; ~4%/yr recently | **+$48.5 M (+10%) in the hour** |

Drawdown: HLP's adjusted daily max drawdown was −2.5%, its worst day −1.1%.
The upper-bound capture at f = 0.15 would have taken the 2025-10-10 hour at
whatever its majors share was. The all-coin version, at the same sizing,
would have lost more than the account (130 fills × −7.5% × 0.15).

## 8. Verdict

**HLP is the better risk-adjusted use of capital, and it is not a packaged
version of our capture. It is its mirror image.** Over the archive year it
returned +19% at a daily Sharpe of 1.4, a −2.5% drawdown and a −1.1% worst
day, for a 4-day lock-up and no fee. The return came from the protocol's
privileged backstop role, collected in the two hours when the book could not
absorb the forced flow: the same hours that destroyed every resting provider
we simulated. Its market making, the part an outsider could imitate, made
nothing, and the per-fill forced-print edge we measured is visible in its
book at +23 bp but worth $46 k a year there. Outside the events HLP earns
about 2% a year, about what its new lending leg pays, and it is now half
lending.

So a home-built provider has a double bar. It must return more than HLP in
a spiral year (about 20%). It must also not give that back in the spiral
hour, where HLP gains and resting liquidity loses. At ~500 major fills a
year and 15% of capital per fill, that means **≥ 25 bp net per fill,
realisable, with the spiral hour contained to a few percent of capital**.
At 30% of capital per fill (concentration risk doubles) it means ≥ 13 bp.
That is 003's ex-post upper bound delivered in practice; 004 measured the
realisable version at −2 bp. Until H1 (live paper) or H2 (Binance ticks)
shows a realisable ≥ 25 bp in majors, the capture is not worth building for
return. If it is built, it is worth building as a *complement* to an HLP
deposit, since the two have opposite signs in the hour that decides each one.

## 9. Not done, and why

- No AWS / requester-pays data was read; nothing was downloaded beyond the
  public Hyperliquid API (~1,400 rate-limited requests) and one DefiLlama
  request.
- HLP's fee tier and rebates are not public; HLP markouts are gross.
- The parent-level +$9.7 M is attributed by elimination (fees / transfers),
  not by a ledger entry.
- The API's history has 14-day resolution; daily and hourly figures are the
  archive reconstruction, residual-adjusted where stated.
