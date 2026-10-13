# 016 — Crypto time-series trend following: the Zarattini–Pagani–Barbon (2025) rule verbatim, registered before its forward span exists

**Status: DRAFT** (2026-10-13, drafter; Revision 0). Not committed. Two independent reviews and a second-round
recheck are required before REGISTERED (programme ledger protocol, steps 4–5). Every row of §13 is PROPOSED.

**Nothing in this draft was computed on real data beyond hashes, file counts and first/last months** (§15). Every
size, power and false-pass number below comes from synthetic worlds (`r016_run.py --power`, which reads no data).
The runner refuses any fetch or read before **2028-01-08** (the registered end 2027-12-31 + 7 days), and refuses
everything while this file says DRAFT.

Programme: `docs/alpha-discovery-2026-10-13.md` (round 4, light C1). Scout: `scouts/c1-crypto-trend.md`.
Ledger entries read: `000t` (C1 scout), `000q` (D1 seasoned-path look), `copytrade/registrations/000l` (the
2026-07 → 2026-09 market seen by other studies). Templates ported: `research/copytrade/r008_run.py`,
`r008_fetch.py`, `r013_run.py`, their reviews, and 014 Revision 2 (`ed8f06b`, the `<rev>:./<path>` fix).

---

## 0. Hypothesis, mechanism, sign, and what has been seen

**Hypothesis.** The paper's top-20 long-only crypto trend book, run verbatim on Binance USDⓈ-M perpetual closes
from 2025-03-20 (the day after the paper's sample ends) to 2027-12-31, earns a positive α net of the paper's 10 bp
costs: a return that BTC, an equal-weight alt basket and a volatility-managed equal-weight basket do not explain.
A second, pre-declared hypothesis asks whether its Sharpe ratio is **below the published 1.57**.

**Mechanism (who pays).**
1. **Under-reaction, then late crowds.** Information about adoption, listings, unlocks and flows diffuses slowly
   through a retail-dominated market. Prices trend first; a levered crowd joins late and pays the trend at its top.
   The rule enters on a new n-day closing high, so it buys after the move has started and before the crowd.
2. **Forced flow in the trend's direction.** Rallies squeeze short perp positions and liquidate them (fact 2 of
   the programme: forced flow overshoots); declines liquidate levered longs. Both push price further in the trend
   direction, for days. The midpoint trailing stop exits when the move gives back half its channel.
3. **Why it could survive.** Trend books are long volatility-of-drift and short whipsaw; they lose steadily in
   chop. Most capital cannot hold a strategy whose return arrives in a few months a year (the paper's
   "stair-step", §7.2 p. 21). The edge is a risk-bearing and patience premium, not an arbitrage.
4. **Why it may have decayed.** The paper is public (2025-04), CTAs now trade crypto futures, and the
   post-publication decay literature (McLean–Pontiff: −58% on average across 97 predictors) applies. H2 asks this
   directly.

**Sign.** Long-only (the paper's primary, §1 p. 3: "we restrict attention to a long-only implementation"). α > 0.

**What has been seen (exposure).**

| look | calendars | what it saw | exposure to 016 |
|---|---|---|---|
| **The paper** (Zarattini, Pagani & Barbon, SFI RP 25-80, v. 2025-04-09) | 2015-01-01 → 2025-03-19 (top 20/50), 2020-01 → 2025-03-19 (ranks 1–100) | the rule's in-sample results: SR 1.57, α 10.8%/yr, β 0.08, vol 9%, MDD 11% (Table 4) | the rule and its published Sharpe are public priors; **nothing out of its sample was opened** |
| **Post-publication decay literature** (public priors) | — | McLean–Pontiff; the desk's Arefev note (TSMOM ≈ 0 after 2020-07) | direction of the prior only |
| **Designer knowledge** (disclosed) | span (a), 2025-03-20 → 2026-06-30 | the drafting and orchestrating model's training data cover the crypto market path through about mid-2026: a rally into early October 2025, the 2025-10-10 liquidation cascade, and a large decline into 2026 | **the choice to test this rule now is not independent of (a).** The rule is copied verbatim and cannot be tuned; §7 decides how (a) enters |
| **`000t`** (C1 scout) | Binance perps 2020-01 → 2026-06 | universe counts, Donchian flip counts and long fractions, turnover, gross exposure, σ90 levels, unconditional funding by year, correlations, BTC/ETH/EW moves in five named stress episodes | behaviour only; no return after any signal; no strategy P&L. No parameter here is set from it (the rule is the paper's) |
| **`000q`** (D1 scout) | Binance seasoned coin-weeks 2021-11 → 2024-01 | continuation after +50% / +90% touches | outside 016's test span; no threshold here is near those levels |
| **`000l`** | 2026-07 → 2026-09 | **the 2026-09-14 alpha-lab sweep computed momentum and reversal ICs on 83 Bitget perps through 2026-09**; the live AI trader's inverted study (2026-06-27 → 2026-10-10: fading the AI paid) | **inside span (c).** Months 2026-07 → 09 of the forward span are not unseen by this programme: a cross-sectional momentum IC on 4H bars is a different statistic from this time-series book, but its sign is outcome-adjacent |
| **008** (registered) | HL fills 2026-07-01 → 2026-09-28 | will read inverted-positioning outcomes in those months (its fetch is in progress) | 008 reads HL wallet data; when it reports, its result is about 016's span-(c) months. Stated, not avoidable |
| **The HL discovery year** (16 trials) | 2025-07-28 → 2026-06-30 ⊂ (a) | other signals (copying, forced flow, carry, listings) | no trend statistic was ever computed on it |
| L4 holdout A (released, unread) | 2025-03-20 → 2025-07-27 ⊂ (a) | nothing | 016 consumes about four months of it |

## 1. Trial, family, calendars

- **New family: "crypto time-series trend". This is trial 1.** One trial, long-only, verbatim. The long/short
  version (paper Appendix) is reported, never decisive.
- **Carried-over programme counts** (programme doc §2): HL discovery year 16 trials, no pass; HL holdout read only by
  008 (2026-07-01 → 2026-09-28); Binance new-perp listing windows 2 trials (013), no pass; Deribit options 0 (014
  parked); Binance 2021–23 1 trial (009); L2 carry 3, closed; HL `xyz` 0; US equity calendar flows 1 (015, drafted
  alongside). 016 shares no family with any of them.

**Calendars (fixed; D17).**

| span | dates | status | months (clusters) |
|---|---|---|---|
| warm-up | each symbol's first archived Binance perp close (from 2019-12) → 2025-03-19 | signal states, σ90 and universe only; **no return is booked** | — |
| **(a)** | **2025-03-20 → 2026-06-30** | out of the paper's sample; before the programme's cut; **designer-known** | 15 (the 12-day March 2025 stub joins April) |
| **(c)** | **2026-07-01 → 2027-12-31** | **forward, unread, claimed openly for this family.** It does not exist yet in full. 008 reads HL fills in 2026-07 → 09, and `000l` looked at momentum through 2026-09 (stated in §0) | 18 |
| **pooled (a)+(c)** | 2025-03-20 → 2027-12-31 | **the primary** (§7) | **G = 33** |

**One read only**, after 2027-12-31 + 7 days. There is no interim look and no second look inside 016. A later
extension (2028-01 →) would be a new registration with its own forward span.

## 2. The rule, verbatim (paper sections and printed pages)

| element | the paper | here |
|---|---|---|
| data | daily OHLCV closes (§3, §4.1 p. 6–7: "DonchianUp = max{Close_t, …, Close_{t−n+1}}") | Binance USDⓈ-M 1d kline closes (00:00 UTC) — D11 |
| channel | Up, Down on **closes**, today included; Mid = ½(Up + Down) (p. 7) | same |
| entry | "as soon as the closing price hits the upper boundary": Pos = 1 if Close_t = Up_t(n) (§4.1 p. 7; §4.3 p. 10) | same; ties enter (D2) |
| exit | "the first closure below a trailing stop"; Pos = 0 if Close_t ≤ TrailingStop_t (p. 7, p. 10) | same; **≤** (the formula's sign, p. 10) |
| stop | TrailingStop_{t+1} = max(TrailingStop_t, DonchianMid_t), "never … updated downward"; the stop used on day t+1 is set at the end of day t (p. 7); a new position starts at the entry day's Mid (footnote 3) | same; a long that prints a new high keeps its stop (not a new position) — D2 |
| case order | Pos = 1 if Close = Up; 0 if Close ≤ stop; else Pos_{t−1} (p. 10) | same order: an n-day high at or below the stop stays long (D2) |
| lookbacks | n ∈ {5, 10, 20, 30, 60, 90, 150, 250, 360} (§4.3 p. 10) | same; flat until n closes exist (D2) |
| sizing | w_n = min(0.25/σ_t, 200%) × Pos_n; σ_t the 90-day ("3-month") annualised vol of the asset's daily returns (§4.2 p. 8–9; §4.3 p. 10) | same; σ = sample SD of 90 simple returns × √365 (D3) |
| Combo | w = (1/9) Σ_n w_n; r_t = w_{t−1} × ret_t (§4.3 p. 10) | same; positions set at close t earn from close t (no lag); a one-close lag is reported |
| costs | "net of a transaction cost of 10 basis points" (§5.1 p. 15) | 10 bp × |Δ notional| per coin, every trade incl. resets and cuts (D7) |
| threshold | "only rebalanced if the difference between the current and target allocation exceeds 20%", applied **only** to volatility-driven changes; signal changes trade at once (§5.1 p. 14–15) | per sub-model, **absolute** 0.20 of sleeve weight against the drifted held weight (D5); relative 20% reported |
| universe | month end: listed ≥ 365 days; not wrapped / stablecoin / NFT; median daily volume ≥ $2 M over the preceding 30 days; rank by median volume; top B (§7.1 p. 18) | Binance perps, D8–D9 |
| portfolio | 1/B of capital per asset; each runs the Combo independently; rebalanced (reset to 1/B) at month end (§7.2 p. 18, the AUM formula) | B = 20 (Table 4's headline); continuing members keep their held weights, new members take their current target (D10, D12) |
| removal | intra-month if the 30-day median volume < $1 M or the 30-day median daily price change < 0.5% (§7.2 p. 18) | checked at every close (D9) |
| funding, borrow | none (spot aggregates, CoinMarketCap) | primary: none (D11); deployable variant: real Binance funding |
| direction | long-only primary (p. 3) | same |

**Where the paper is silent, decided from mechanism (all in §13):** the annualisation (√365: crypto trades every
day), σ on simple returns with n − 1, the rule's behaviour before n closes (flat), absolute vs relative threshold
(absolute: allocations are quoted in % of capital, "200%"), netting of the nine sub-model trades inside one coin,
dead and missing rows, the March 2025 initialisation, and how a new member enters (at its current target: the signal
is a property of the price path, not of membership).

## 3. Data

**Source and venue (D8, D11).** Binance USDⓈ-M, the free public archive (`data.binance.vision`):
- **Months ≤ 2026-06:** the C1 scout's pull, `data/scout_c1/bn_um_klines_1d_monthly/` (19,730 zips, 787 symbols,
  each verified against Binance's `.CHECKSUM` at pull time). It is pinned here by the SHA-256 of its `SHA256SUMS`
  (§16), and every file is re-hashed against that list as the read opens it. The scout's funding pull is **not**
  used (it covers the scout's own top-20 months only).
- **Months 2026-07 → 2027-12 (klines) and 2025-03 → 2027-12 (funding, every USDT symbol):** fetched by
  `r016_fetch.py` after 2028-01-08 (§11), each zip verified against `.CHECKSUM` and pinned by a manifest digest in
  the `fetched` row.
- **Contract types:** the 2026-10-11 `fapi/v1/exchangeInfo` snapshot (copied from 013's inputs into
  `data/r016/static/`, pinned) plus the snapshot the fetch takes in 2028. A symbol is excluded if either marks it as
  not (`PERPETUAL`, `COIN`): TradFi equity/commodity/FX perps (onboarded from 2025-12), index perps, deliveries.
  Plus fixed name lists: stablecoins, wrapped tokens, gold tokens, and the scout's TradFi names (D8).

**Point in time.**
- **Listing age** = days since the perp's first traded daily close. The paper ages the *asset*; a perp-age rule is
  stricter (it bites until 2021-03, irrelevant from 2025).
- **Volume** = Binance quote volume (USDT), not CoinMarketCap's cross-exchange aggregate. The $2 M floor never binds
  near the top 20 (rank-20 median ≥ $71 M/day at its 2026-05 low, scout §5.5).
- **The delisting cut (D9).** Binance's archive keeps writing flat, zero-trade 1d rows for dead contracts for years
  (L4 scout: FTTUSDT through 2026-10), and the scout found **LUNAUSDT in June 2022's top 20 on May volume after its
  delisting**. So: (i) a symbol is eligible at a month end only if it traded (count > 0) on **each of the last 7
  days**; (ii) a member whose row goes dead during the month settles at its last traded close (Binance settles a
  delisted perp near its index), cost charged, and its sleeve is cash until the month end; (iii) the paper's §7.2
  liquidity exit runs at every close. All three are counted before the decision.
- **Tradability elsewhere (reported).** The fetch probes Hyperliquid (`meta`; per coin, the first funding time and
  the last month start with funding). A reported sub-book keeps only members listed on HL at the month start
  (Binance `1000X` ↔ HL `kX`). The scout found HL lists 19–20 of the top 20 from 2023-12. Bitget is not probed: its
  history retention made the scout's listing probe incomplete, and the deployable venue here is Binance itself.

## 4. The decision

### 4.1 H1 — existence (the primary)

On the **daily net book return** r_t (10 bp, no funding), 2025-03-20 → 2027-12-31:

    r_t = α + β1·BTC_t + β2·ALT_t + β3·VM_t + e_t

- **BTC_t:** BTCUSDT's close-to-close return.
- **ALT_t:** the equal-weight return of the day's members other than BTC (as the book holds them; cut members leave).
- **VM_t:** the **vol-managed basket**: the same book, same universe, same sizing, same threshold, with Pos ≡ 1 for
  every lookback, gross of costs. It carries every return the book could earn from *vol timing* (Moreira–Muir) and
  static exposure, so α isolates the trend signal's timing.
- **One slope per factor over the whole span (k = 3)** — D14. Calendar-cell slopes (half-year, k = 18; year,
  k = 9) are **reported**. Trailing-trend cells are never used: they would absorb the trend into β by construction.
- **CR1 standard errors clustered by calendar month**, G = 33.

**H1 passes iff both:**
1. α̂ > 0 and **t ≥ CAL_H1 = 1.731**, which is max(t_{G−1−k}(0.95) = t_29(0.95) = 1.699, the largest 95th
   percentile of the α t over the eleven calibration worlds of §5);
2. **the same model fitted on span (c) alone gives α̂_c > 0** (the forward-sign condition, D18; §7).

### 4.2 H2 — decay against the paper (pre-declared, same read)

On the **monthly** net book returns (compounded within the month; the stub joins April 2025), G = 33:
T = √G · mean / sd, ŜR = mean / sd · √12. **H2 rejects "SR = 1.57" iff T < CAL_H2 = 0.117**, which is the
smaller of the noncentral-t 5% quantile (df 32, δ = 1.57·√(33/12): 0.954, i.e. ŜR below ≈ 0.58) and the smallest
5th percentile of T in the three synthetic worlds whose book truly runs at SR 1.57 (§6: 0.1177). **So H2 rejects iff
ŜR < ≈ 0.07.** It is the paper's own object — raw
Sharpe, net of 10 bp, no funding — so it carries the market component (β 0.08 in-sample) as the paper's did.

### 4.3 Four outcomes, and what each leads to

| H1 | H2 | outcome | what follows |
|---|---|---|---|
| pass | not rejected | `alive_as_published` | the economic bar (§4.4); clears → **`paper_then_scale`**: a forward paper/live period at the paper's sizing, scaling toward 25% book vol over it; fails → `exists_below_bar` |
| pass | rejected | `alive_decayed` | the economic bar on the shrunk estimate; clears → **`paper_reduced_size`** (half-Kelly at the shrunk Sharpe); fails → `exists_below_bar` |
| fail | rejected | `decayed_not_shown` | **`close`**: the family closes; this is the kill switch for any owner allocation (§14) |
| fail | not rejected | `undecided` | **`not_demonstrated`**: "not demonstrated at this power", never "no effect". No claim; an extension needs a new registration |

### 4.4 The economic bar (after H1 only; never a pass condition)

- The **deployable variant**: the same book at **6 bp per fill** (Binance taker 5 bp, 4.5 with BNB, + 1 bp
  slippage) **with real Binance funding** on every settlement held (long pays a positive rate).
- Its annual net return, taken as **min(raw mean, factor-adjusted α)** (conservative in bull and bear calendars,
  as 013), × the proposed size **k = 2.8** (25% book vol over the paper's ≈ 9%), × the positive-part James–Stein
  shrink **(1 − 1/t²)** of the H1 t.
- **Clears iff ≥ 4%/yr** (HLP calm). It routes only to a paper/live step.

### 4.5 No two point bars in series

H1 is an existence test with a calibrated small-sample critical value; the only conjunction is the forward sign
(power cost in §6). H2 is a separate, pre-declared question from the same read. The economic bar is applied after,
on a shrunk number, and decides only the route.

## 5. Market adjustment and calibration (synthetic only)

**Why not β per calendar cell (D14), with the evidence.** A trend book's exposure follows realised drift. Inside any
cell, the cell's own slope estimate rises with the cell's realised return, so the intercept loses the in-cell
timing gain by construction (an in-sample covariance bias of order C/n_cell, where C is the book's exposure-to-past-
return covariance). Measured on the registered path, 600 draws per world:

| world (trend-free unless stated) | mean α t: constant / year cells / half-year cells | 95th pct: constant / year / half-year |
|---|---|---|
| flat | −0.81 / −1.42 / −1.88 | 1.00 / 0.48 / 0.13 |
| bull +60%/yr | −0.75 / −1.30 / −1.82 | 1.03 / 0.64 / 0.23 |
| bear −60%/yr | −0.71 / −1.30 / −1.71 | 1.09 / 0.60 / 0.18 |
| drift aligned with the half-year cells (a six-month regime) | −0.12 / −0.86 / −1.72 | 1.55 / 0.96 / 0.24 |
| two −35% crash days | −0.57 / −1.10 / −1.61 | 1.05 / 0.66 / 0.07 |
| chop, vol × 1.4 | −0.66 / −1.07 / −1.26 | 1.16 / 0.90 / 0.67 |
| mean-reverting chop (AR −0.1) | −1.51 / −2.03 / −2.42 | 0.52 / 0.13 / −0.45 |
| vol timing pays (drift +100%/yr in low vol, −100% in high) | −0.17 / −0.65 / −1.17 | 1.54 / 1.28 / 0.81 |
| alt β switching by cell, cell drifts | −0.35 / −1.18 / −1.85 | 1.36 / 0.74 / 0.26 |
| Student-t(3), leverage GARCH, cell drifts | +0.16 / −0.62 / −1.99 | **1.73** / 1.21 / 0.05 |
| bull + a −30% crash | −0.42 / −1.06 / −1.53 | 1.36 / 0.81 / 0.49 |
| *for contrast, trend at a true SR ≈ 1 (h 20 / 60 / 120; §6)* | *constant 2.73 / 2.41 / 2.39; year 2.13 / 1.65 / 1.57; half-year 1.56 / 0.88 / 0.84* | — |

- **In every trend-free world the constant-slope model is size-correct at its own floor or the calibrated value**,
  and the half-year model's t is shifted down by about **1.1–1.6** — it is conservative under the null, and it
  costs the same shift in every trend world (§6): at a true SR of 1, H1 power falls from 0.68–0.83 (constant) to
  0.21–0.44 (half-year cells).
- **What cells were meant to stop** (rounds 2–3: a book whose β moves with the regime for reasons other than its
  signal, while the market's drift moves too) is covered here by the factors and the calibration set:
  vol-driven exposure is in VM (the "vol-timing pays" world), composition-driven alt β is in ALT (the "alt-β
  cells" world), and the **aligned half-year drift regimes** of round 3 (which a trend book legitimately harvests,
  since a six-month drift is a six-month trend) are **in the calibration set anyway**: they set CAL_H1.
- **Calibration set (D19):** flat; bull +60%/yr; bear −60%/yr; drift aligned with the six half-year cells
  (+90, −70, +30, −50, +80, −30%/yr); two −35% crash days; chop at 1.4× vol; mean-reverting chop (AR −0.1);
  vol-timing pays (drift +100%/yr in low vol, −100% in high); alt β switching by cell with cell drifts; Student-t(3)
  with leverage GARCH and cell drifts; bull + a −30% crash. GARCH(1,1) vol, Student-t(4) innovations, 24 coins,
  rotating volumes, the real calendar. **CAL_H1 = 1.731** (the largest 95th percentile, 1.7308 in the Student-t(3) leverage world with cell drifts; the floor
  t_29 = 1.699).

## 6. Power and false-pass rates (synthetic; `data/r016/logs/power.json`)

**Provenance.** `r016_run.py --power` wrote `power.json` and `power_records.json` (31 worlds; 600 draws for the
calibration, hindsight and misaligned worlds, 400 for the 18 trend worlds; fixed seeds, so a reviewer can re-run it).
CAL_H1 and CAL_H2 were then written into `ct16/stats.py` from those records; that is the only difference between the
code that produced the records and the hashed code (the records store t and T, never a decision).

**Trend worlds.** Each coin's drift is κ × its daily vol × (0.7 u_t + 0.71 v_i,t), u and v AR(1) with half-life
h ∈ {20, 60, 120} days, mean zero (no market drift), so the book's Sharpe is pure timing. κ was set by a pilot
(200 draws per κ ∈ {0.05, …, 0.3}) to hit target Sharpes; the achieved mean Sharpe is reported. 400 draws per world.

| true book SR (achieved) | H1 power (pass rule), h 20 / 60 / 120 | t-only | H2 rejects 1.57, h 20 / 60 / 120 |
|---|---|---|---|
| 0 (−0.03 / 0.05 / 0.01) | 0.022 / 0.048 / 0.040 | 0.022 / 0.052 / 0.045 | **0.51 / 0.51 / 0.50** |
| 0.5 (0.53 / 0.54 / 0.51) | 0.29 / 0.23 / 0.25 | 0.30 / 0.25 / 0.27 | **0.20 / 0.22 / 0.29** |
| **0.75** (0.79 / 0.62 / 0.74) | **0.60 / 0.41 / 0.50** | 0.63 / 0.43 / 0.54 | 0.12 / 0.24 / 0.20 |
| **1.0** (1.09 / 0.93 / 0.97) | **0.83 / 0.72 / 0.68** | 0.86 / 0.76 / 0.70 | 0.06 / 0.12 / 0.16 |
| **1.5** (1.48 / 1.45 / 1.39) | **0.97 / 0.96 / 0.94** | 0.98 / 0.97 / 0.95 | 0.02 / 0.03 / 0.10 |
| 1.57 (1.53 / 1.58 / 1.65) | 0.98 / 0.97 / 0.96 | 0.99 / 0.98 / 0.98 | 0.005 / 0.028 / 0.050 (H2's size) |
| *1.0, half-year cells (k 18)* | *0.44 / 0.22 / 0.21* | | |
| *1.0, year cells (k 9)* | *0.66 / 0.45 / 0.44* | | |

- **H1:** at a true SR of 1 the pass rule has power **0.68–0.83** (0.72 at h 60); at 0.75, 0.41–0.60; at 1.5, 0.94–0.97. The scout's "alive at half strength" (SR ≈ 0.78) is thus about a coin flip, not hopeless. With half-year cells it would be 0.21–0.44 at SR 1.
- **H2:** in the SR-1.57 worlds the 5th percentile of T is 1.22 / 0.54 / 0.12 at h 20 / 60 / 120 (it falls as the trend slows: slow trends make a 33-month Sharpe noisy), far below the normal-theory 0.954. Taking the smallest (0.117) keeps H2's size ≤ 0.05 in all three, and **costs power: H2 rejects at a true SR of 0 with 0.50–0.51, at 0.5 with 0.20–0.29.** The scout's 0.76–0.88 assumed i.i.d. normal months; it is revised here. H2 now rejects only when the pooled Sharpe is about zero or below (ŜR < 0.117/√(33/12) ≈ 0.07).
- **Why H1 power exceeds the scout's arithmetic** (0.49 at SR 1): the scout's model was monthly raw returns with
  E[t] = SR·√(G/12) = 1.66. Here α is estimated on daily data with the market factors removing most of the book's
  market variance, so α's information ratio exceeds the book's raw Sharpe.

**False-pass rates of the full H1 rule** (t ≥ CAL_H1 and α̂_c > 0) in the calibration worlds: 0.000–0.048 (largest: the Student-t(3) leverage world 0.048 and vol timing 0.042; the aligned six-month regime 0.033; every other world ≤ 0.018). The trend-free trend worlds (κ at SR 0) give 0.022 / 0.048 / 0.040.
**The hindsight world** (span (a) shaped like the designer-known path — +120%/yr drift 2025-04 → 2025-10-05, a
−20% day on 2025-10-10, −100%/yr to 2026-02 — and span (c) trend-free): H1 passes 0.017 (t-only 0.023; mean α t −0.36).
**Quarterly regimes switching mid-cell** (a trend world by definition; reported): H1 passes 0.048 (mean t +0.06): the rule barely harvests three-month regimes at this vol.

## 7. Span (a): in the primary, with a forward-sign condition (D18)

- **Power argument.** The forward span alone (G = 18) has E[t] ≈ √(18/33) ≈ 0.74 of the pooled one. In the scout's
  arithmetic that is 0.32 vs 0.49 at SR 1; on the registered path the forward span alone (constant slopes, floor t_14(0.95) = 1.761) has power 0.41–0.54 at SR 1 against **0.68–0.83 pooled**.
- **Hindsight argument.** The drafter knows the shape of (a): a rally, a cascade, a decline. A long-only trend book
  plausibly does well against a long benchmark in such a year, and a designer who knows that might choose to test
  it now. But (i) the rule is verbatim (no lookback, size, universe or cost was chosen), (ii) the market model is
  fixed by the evidence of §5, not by (a), and (iii) on the registered path the designer-shaped year produces
  **no α**: the hindsight world gives mean α t −0.36 and a full-rule pass rate of 0.017. The predictable part
  of a known index path is small next to the book's daily noise.
- **Decision (PROPOSED):** (a) **is** in the primary, pooled with (c), **and** H1 requires α̂_c > 0 on (c) alone,
  so a pass can never be carried by the designer-known months. Its power cost is 0.02–0.04 (0.855 → 0.830, 0.76 → 0.72, 0.70 → 0.675 at h 20 / 60 / 120) at SR 1. (a) and (c)
  are reported as strata with their own α t and Sharpe.
- **H2** pools (a) and (c) without a condition: the known 2025-26 decline can only push a long-only Sharpe down, which
  makes H2 *more* likely to reject — the conservative direction for a kill switch.

## 8. Costs and execution

| item | primary | deployable (reported, and the economic bar) |
|---|---|---|
| prices | Binance perp 1d closes | same |
| fee | 10 bp × |Δ notional| (the paper's) | 6 bp (Binance taker 5 bp; 4.5 with BNB; + 1 bp slippage) |
| funding | none (the paper's) | every Binance settlement, on the notional held into the day |
| execution | at the close that sets the signal (the paper's r_t = w_{t−1} ret_t) | the same; next-open is seconds later; a one-close lag is reported |

**Why perp closes in the primary, not spot (D11).** (1) One venue gives the universe, the delisting cut, the prices
and the funding consistently; the paper's CoinMarketCap aggregates are not reproducible point in time. (2) The
deployable instrument is the perp; a spot pass would leave a basis question open. (3) Daily close-to-close perp and
spot returns differ by the day's change in basis (expected a few bp; not measured here). (4) **Funding is left out
of the primary** so that H2 compares the paper's object with the paper's object; funding enters the deployable
variant and the economic bar (scout: the universe's unconditional funding is +5.4%/yr, −2.2%/yr in 2025-01 →
2026-06; × mean gross 0.08 ≈ 0.4%/yr at paper sizing, more if longs coincide with high funding — measured only
inside the read).

Turnover ≈ 4.4× capital a year (scout), so fees are ≈ 0.4%/yr at 10 bp and ≈ 0.26%/yr at 6 bp. 0, 25 and 50 bp are
reported.

## 9. Tail, sizing, Kelly, capacity, expected return

Arithmetic from the scout's stress episodes (Binance, 2020-03 → 2025-10; unconditional gross p90/p99, never the gross
held on the eve of an episode), re-derived here:

| sizing | gross p90 / p99 | 2020-03-12 day (EW −42.7%) | 2021-05-19 (−34.0%) | 2025-10-10 (−23.0%) | worst plausible week | worst plausible month |
|---|---|---|---|---|---|---|
| **paper (≈ 9% vol)** | 0.16 / 0.27 | −6.8% / −11.5% | −5.4% / −9.2% | −3.7% / −6.2% | **≈ −12%** | **≈ −15%** |
| **25% vol (k = 2.8)** | 0.45 / 0.76 | −19% / −32% | −15% / −26% | −10% / −17% | **−20 to −35%** | **−30 to −40%** |

- A daily-close stop cannot act inside a one-day gap, so the worst week is mostly the first day × the gross held.
  The month adds whipsaw re-entries. The paper's in-sample MDD 11% at 9% vol scales to ≈ 31% at 25%.
- **Margin:** cross margin, or ≤ 2× isolated per coin (a 2021-05-19 day liquidates 3× isolated); at k = 2.8 the
  p99 gross is 0.76 of equity.
- **Kelly.** If alive at half strength (SR 0.78), full Kelly runs at ≈ 78% vol, so 25% is ≈ 0.32 Kelly. At the
  programme's odds (1 in 3 alive), E[SR] ≈ ⅓·0.78 + ⅔·(−0.15) ≈ 0.16, so the Bayes-Kelly vol is ≈ 16% and half of it
  ≈ 8% — **the paper's own ≈ 9%**. Start there; scale only after H1 passes.
- **Capacity.** At $1 M, k = 2.8, the largest single-coin notional is ≈ $120 k, ≈ 0.17% of the rank-20 coin's daily
  volume: non-binding to tens of $M.

**Expected return over cash (idle collateral earns ≈ 3% on top in every state):**

| state | paper sizing (≈ 9% vol) | 25% vol (k = 2.8) |
|---|---|---|
| alive at full strength (SR 1.57) | 14.1% − 0.4% funding ≈ **+13.7%/yr** | 39% − 1.2 to 3% ≈ **+36 to 38%/yr** |
| alive at half strength (SR 0.78) | 7.0% − 0.4% ≈ **+6.6%/yr** | 19.5% − 1.2 to 3% ≈ **+16 to 18%/yr** |
| dead (costs and funding only) | ≈ −1 to −2%/yr | ≈ −4 to −6%/yr |
| expectation at 1-in-3 odds | ≈ +1.2%/yr | ≈ +2.3%/yr, with a −30% tail |

**Against the goal.** Only the "alive at full strength, 25% vol" cell reaches the mandate's 30–100% landing zone, with
a −30 to −40% plausible month. Half strength at 25% vol is a 16–18%/yr book with HLP-beating return and a far worse
risk-adjusted profile than HLP. Odds (scout, unchanged): exists 1 in 3; beats HLP on return 1 in 5; risk-adjusted
1 in 15.

## 10. Reported, decides nothing

Strata (a) and (c) (α, t, Sharpe); half-year- and year-cell α; BTC-only constant-β α (the paper's own α
definition); the deployable variant (α, Sharpe, mean) and the funding drag; costs 0 / 25 / 50 bp; the relative
threshold; the one-close lag; long/short (paper Appendix); the HL-tradable sub-book; tails at paper sizing and
scaled to 25% vol (worst day, 7-day, 30-day, MDD); gross exposure; turnover; a **placebo**: every symbol's
Donchian states circularly shifted by a random offset (flips and turnover kept, timing broken), 200 seeds, the
share with α t at or above the actual and the critical; and the counts before the decision (symbols, files, missing
rows, delisting cuts, liquidity exits, months with fewer than 20 members). The members by month are stored.
No reported block may become a primary.

## 11. Guard, integrity, the fetcher, procedure

**Files.** `ct16/rule.py` (the rule, universe, book, factors; pure), `ct16/stats.py` (estimators, H1, H2, the
decision, the recheck; pure), `ct16/data.py` (archive parsers on bytes only: no path reads), `ct16/sim.py`
(synthetic worlds; reads nothing), `r016_run.py` (guard, gated panel, the read, `--power`), `r016_fetch.py` (the
guarded fetch), `tests/test_r016.py`.

**The guard** (`r016_run.guard_failures`; ported from r008 / r013 and their reviews):
- (s) REGISTERED; (a) tracked; (b) unmodified against HEAD; (c) committed;
- (d) the guarded code clean, and `shadow_modules`: no untracked, ignored or modified `*.py`, sourceless `*.pyc`,
  `*.so`/`*.pyd` or package at the top level of `research/alpha` or in `ct16/`;
- (e) code and data equal the §16 table, each file listed once;
- **(t) today (UTC) ≥ 2028-01-08**;
- (k) `results.jsonl` tracked and clean;
- (w) git's top level is this repository; the anchor `9c131e2` (000t) is an ancestor of HEAD; every git call runs
  with `GIT_*` removed;
- (v) `-O` refused; Python pinned (3.14.8; the decision path is standard-library only); `ct16` loaded from its
  hashed sources; a fresh `sys.pycache_prefix` outside the repository, set before `ct16` is imported;
- (f) fetch: no `fetched`, `read_started`, `read`, `read_failed` row anywhere — working tree, every branch, the
  reflog, merge and stash diffs (`git log --all --reflog -m -p --text --no-textconv --no-ext-diff`), keyed on the
  `016-` prefix;
- read: (g1) exactly one `fetch_started` and one `fetched` row, committed in `HEAD:./results.jsonl`, same
  registration commit, an ancestor of HEAD; (g2) code unchanged since, the registration changed only by approval or
  restart lines, the fetched row's code hashes and environment equal today's; (g3) the fetch manifest hashes to the
  fetched row's digest; (h') `016 read: APPROVED <date> row <sha256 of the fetched row>` committed strictly after
  the fetched row; (i) no `read`/`read_failed` row anywhere, and a `read_started` row blocks unless a later
  committed `016 read restart: APPROVED <date> started-row <sha256>` names it.
- **Every `git show` uses `<rev>:./<path>`**, so the gate resolves from `research/alpha/` (014 review B, B1). The
  suite runs the guard and the end-to-end fetch → approval → read in **both layouts** (runner at the top level, and
  in `research/alpha/` under a parent repository).

**Gate tokens.** `Panel` opens the real directories (the pinned pull and the fetch store) only after the real
`guard()` set `_GATE` for mode `run` in this process; a refusal, never an assert. The real results file and the real
data are used only together, only with the default commit hook and only with the real guard
(`real_tree_failures`). `record()` appends, commits `results.jsonl` alone, and refuses unless the line is in
`HEAD:./results.jsonl` (checked with the real git). **The runner never pushes.** A failure after `read_started`
writes `read_failed` (terminal). The decision is recomputed from the stored daily series (`stats.recheck`); a row
that does not recompute is stored with `recheck_failed` and not printed.

**The fetcher** (`r016_fetch.py`; its own pre-checks plus the guard in mode `fetch`):
- pulls Binance klines 2026-07 → 2027-12 and funding 2025-03 → 2027-12 for every USDT symbol, the exchangeInfo, and
  the HL probe; **later months are counted, never fetched; months ≤ 2026-06 are never re-fetched**;
- S3 `ListObjects` pages **forward** with the exclusive `marker`; HL `fundingHistory` takes **inclusive** start and
  end (008 §16), so its windows are [a, b − 1 ms];
- every zip verified against `.CHECKSUM` and parsed before an atomic write; **never cached: an error, an empty
  answer, a checksum mismatch**;
- **tail and seam coverage:** BTCUSDT must have every forward day to 2027-12-31 (never waivable); every symbol that
  traded in the last 7 days of 2026-06 must continue in 2026-07; forward months are contiguous;
- keyed refusals; the same key on 3 committed attempts ≥ 24 h apart becomes "unavailable (persistent)" (≤ 10 names,
  else stop for a decision); every attempt writes a committed row;
- the real hooks refuse unless reached from the real repository's guarded fetch; mock hooks must supply every hook
  and are never used on the real tree; **mocks test the page boundaries** (exclusive marker; inclusive HL end).

**Tests:** **186** checks pass, at DRAFT. **No test goes stale at commit or at REGISTERED:** the real-file checks
accept DRAFT or REGISTERED, never assert commit state, and the date checks are conditional on today's date
(commit-state simulation, `scratchpad/scripts/r016_commit_sim.sh`: a `git clone --local` with its own `.git`; 016's files copied in and committed at DRAFT, then the status line changed to REGISTERED and committed: **186 of 186 pass at DRAFT and 186 of 186 at REGISTERED**, and the real guard in the REGISTERED clone refuses only on (t), the date, and (g1), fetch first; the clone was discarded).

**Procedure.**
1. Two reviews, a recheck, then REGISTERED (the orchestrator), committed with the code; every `.py` at the top of
   `research/alpha` and in `ct16/` tracked and clean (015's files included).
2. From 2028-01-08: `research/copytrade/.venv/bin/python -I r016_fetch.py`, resumable until its `fetched` row.
3. The orchestrator reads the fetch report (counts only), then commits `016 read: APPROVED <date> row <sha256>`.
4. `… -I r016_run.py --run`, once. The result is written into this file in its own terms.

## 12. Known weaknesses, stated now

- **Designer knowledge of (a)** (§0, §7): mitigated by the verbatim rule, the evidence-fixed market model, the
  forward-sign condition and the strata; not removable.
- **`000l` and 008 inside (c):** 2026-07 → 09 were looked at as cross-sectional momentum ICs (Bitget, 4H) and will
  be as HL inverted positioning. Neither is this book's statistic; both are outcome-adjacent.
- **The market model is a choice** (D14). It was fixed from synthetic evidence; a reviewer may prefer year cells
  (power in §6).
- **H2's critical value depends on the trend's persistence** in the synthetic worlds (slower trends make monthly
  Sharpe noisier); the smallest 5th percentile is used, which costs H2 power.
- **Binance perps, not the paper's spot aggregates;** perp age, not asset age; Binance volume, not CMC volume.
- **Funding is modelled on the notional held into the day** at the previous close, not at the settlement mark.
- **The 2028 exchangeInfo may have dropped TradFi perps delisted before then;** the frozen names, the 2026-10-11
  snapshot and the stored member lists let a reader check.
- **Not modelled:** jumps correlated with the book's exposure beyond the crash worlds; intraday stops; exchange
  failure (the venue tail).
- **Archive risk:** if Binance changes or withdraws the archive before 2028, the fetch refuses and an amendment is
  needed.

## 13. Decision table (every row PROPOSED)

| # | item | PROPOSED | reason |
|---|---|---|---|
| D1 | the rule | §2, verbatim: nine lookbacks, close-based Donchian entry, midpoint trailing stop, min(25%/σ90, 200%) per lookback, mean of nine, top 20 by 30-day median volume, 1/20 each, monthly reset, §7.2 exits, 10 bp, 20% threshold, long-only | the trial is the paper's rule; any choice would be a degree of freedom chosen by a designer who knows (a) |
| D2 | signal details | channels on closes, today included; ties enter; exit on close ≤ stop; stop for t+1 set at close t; new entry starts at Mid (fn 3); a new high while long does not reset the stop; the paper's case order (entry first); flat until n closes | p. 7, p. 10, fn 3; where silent, the literal reading |
| D3 | σ | sample SD (n − 1) of the last 90 simple close-to-close returns × √365; no σ → no position | p. 8–9 "90-day annualized volatility of the returns"; crypto trades 365 days |
| D4 | rows | a missing row is carried (no return, no decision; counted); a zero-trade or zero-price row is dead | Binance archive behaviour (L4) |
| D5 | threshold | per sub-model, absolute 0.20 of sleeve weight, held (drifted) vs target; signal changes trade at once; relative 20% reported | p. 14–15; allocations quoted in % ("200%"); scout: turnover equal either way |
| D6 | funding (deployable only) | a settlement at τ belongs to the holding day (d 00:00, d+1 00:00]; charged on the notional held into the day; long pays a positive rate | Binance mechanics; deterministic attribution |
| D7 | costs | primary 10 bp × |Δ notional| per coin, net of the nine sub-models, on every trade incl. resets, cuts and exits; deployable 6 bp + funding; 0/25/50 bp reported | p. 15; Binance taker fee |
| D8 | universe | Binance USDⓈ-M USDT perps; age from the first traded close; 30-day median quote volume for both filter and rank; exclusions by exchangeInfo (2026-10-11 and 2028 snapshots: not PERPETUAL/COIN), stablecoin, wrapped and gold names, the scout's TradFi list | §3, §7.1; Fig. 5 caption "trailing 30-day trading volume" |
| D9 | delisting cut | eligible only if traded on each of the month's last 7 days; a dead row during the month settles at the last traded close, sleeve to cash; §7.2 exits at every close | the LUNA case (scout §4.1) |
| D10 | book | initialised at the close of 2025-03-19 with March's members at their current targets; returns from 2025-03-20; month-end reset to 1/20 each; continuing members keep held weights; idle cash earns 0 in the series | §7.2; cash interest is outside the paper's object |
| D11 | instrument | perp closes; no funding in the primary | §8 |
| D12 | new members | enter at their current Combo target | the signal is a property of the price path |
| D13 | direction | long-only primary; long/short reported | p. 3 |
| D14 | market model | α on BTC, EW alt, vol-managed EW, **one slope each (k = 3)**, CR1 by calendar month (G = 33); half-year (k 18) and year (k 9) cells reported; never trailing-trend cells | §5: cells shift the null t by ≈ 1.1–1.6 and cut power at SR 1 from 0.68–0.83 to 0.21–0.44; the constant model is size-correct in every calibration world at CAL_H1 |
| D15 | H1 | α̂ > 0, t ≥ CAL_H1 = 1.731 = max(t_29(0.95), largest q95 of 11 calibration worlds), and α̂_c > 0 | §4.1, §5 |
| D16 | H2 | T = √33·mean/sd of monthly net returns < CAL_H2 = 0.117 = min(nct q05 0.954, smallest q05 of T in the three SR-1.57 worlds) | §4.2, §6 |
| D17 | calendars | (a) 2025-03-20 → 2026-06-30; (c) 2026-07-01 → 2027-12-31, claimed openly; one read after 2028-01-08 | §1 |
| D18 | span (a) | in the pooled primary, with the forward-sign condition; strata reported | §7 |
| D19 | calibration set | the eleven worlds of §5, 600 draws each; CAL = the largest 95th percentile (never below the floor) | round-3 lesson; adding worlds can only raise it |
| D20 | outcomes | §4.3; economic bar §4.4 (6 bp + funding, min(raw, adjusted), k 2.8, shrink 1 − 1/t², ≥ 4%/yr) | separates existence from economics |
| D21 | fetch | §11; Binance klines and funding, exchangeInfo, HL probe; persistence 3 × 24 h; ≤ 10 unavailable names | ported from 008 |
| D22 | guard | §11; date gate 2028-01-08; approval line after the fetched row; restart line | ported from 008 / 013 / 014 R2 |
| D23 | reported | §10 | — |
| D24 | family | crypto time-series trend: trial 1 | new family |
| D25 | owner option | §14 | — |

## 14. Owner option (not part of the trial)

**PROPOSED:** the owner **may** paper-trade or live-trade the verbatim rule at the paper's sizing (≈ 9% book vol;
≈ half Bayes-Kelly at the programme's odds) during the forward span, on Binance or HL, from any date.
- **It does not change the test.** The read uses Binance archive closes, not the owner's fills; the rule, the
  statistics, the critical values and the end date are fixed here; the guard refuses any change to this file
  after REGISTERED except approval and restart lines.
- **What it does change:** the owner (and anyone who sees the account) will see interim P&L from span (c). That
  is an informal look at the forward span. It may not be used to amend, stop early or re-time 016 (the read runs
  on 2028-01-08 regardless), and any registration that uses that P&L must ledger it.
- The kill switch: an H2 rejection (`decayed_not_shown` or `alive_decayed` below the bar) ends the allocation; a
  live drawdown beyond the plausible month at the chosen size (−15% at 9% vol) is a reason to stop trading, never a
  reason to touch the test.

## 15. Disclosures and real-data reads by the drafter (for the ledger)

- **Read:** the paper's text (the PDF in `data/scout_c1/paper/`, extracted with macOS PDFKit into the session
  scratchpad); the scout report, `power.txt`, `000t`, `000q`, `000l`; the templates and reviews listed at the top.
- **Real data touched, values:** none of span (a) or later. While checking the archive CSV layout, the first row of
  `BTCUSDT-1d-2020-01.zip` (2020-01-01 OHLC, in the paper's sample) was printed, and the header lines of one 2022-01
  kline file and one 2022-01 funding file.
- **Real data touched, metadata only:** the SHA-256 of the scout's kline `SHA256SUMS` and of the paper's
  `SHA256SUMS`; the line count (19,730) and the last month (2026-06) of the kline listing; the exchangeInfo
  snapshot's field names and its (underlyingType, contractType, status) counts (current state, 2026-10-11).
- **Not opened:** `derived/*/gross_daily.csv`, any kline value of 2021 or later, any funding value, anything dated
  on or after 2026-07-01.
- **Synthetic only:** every simulation (`data/r016/logs/`); the pilot that set κ (scratchpad, 200 draws per κ).
- **Designer knowledge** (§0) is disclosed, not a read.

## 16. SHA-256 of the code and data the read depends on

| file | sha256 |
|---|---|
| `ct16/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct16/rule.py` | `eee567bfcd448a81e2e86b6a32724996ab91990b6b21b350559aa7c531861b41` |
| `ct16/stats.py` | `df33f70154dfa99fe45c1f6f2c3358d685f2e343acd4673bdc766429f1cfa8b8` |
| `ct16/data.py` | `bffd4a78bec08fcdf3d5cc258f3eaa7bfeefea031f8ae64727ed02ff27b224e7` |
| `ct16/sim.py` | `331de283297f9bba9c4382135558639a1a1e3c603f034c03cb5b7778990eaa21` |
| `r016_run.py` | `a94e451be43bdf2ff8347d7b6b68e46bcf5aea4f276584ec5c3cbb7cdca67a5f` |
| `r016_fetch.py` | `9cbcb31f1a9072ccf69e5804ff0d3185181eeaa7dbfbfea6f2399752cba26af1` |
| `data/scout_c1/bn_um_klines_1d_monthly/SHA256SUMS` | `ff50da311181f092581bf61b953fe51750872d33cc9e8a7c7021ffe2a7f91b31` |
| `data/r016/static/fapi_exchangeInfo_20261011.json` | `a7f720f5ece63fe9ab087cbdd9fecc66d18645f113896b81ec171c89636ed39f` |
