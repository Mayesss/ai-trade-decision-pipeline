# Independent review A of 016 DRAFT (crypto trend following, the ZPB 2025 rule verbatim), 2026-10-13

Reviewer brief: an adversarial review of `registrations/016-crypto-trend-verbatim.md` (DRAFT, `66df71a`),
`ct16/*.py`, `r016_run.py`, `r016_fetch.py`, `tests/test_r016.py` and `data/r016/logs/power*.json`. It covers
design, statistics, fidelity to the paper, economics and the tail. Guard and integrity are reviewer B's scope.
Every existing file was read-only. `r016_run.py` and `r016_fetch.py` were never run in any mode. No guard was called
against the real tree.

**What was run.** Every probe is synthetic. The world generator, the OLS/CR1 estimator, the span splits and every
derived statistic are my own code (`scratch_review016a/common.py`). The book itself is the registered object
(`ct16.rule.compute`). My estimator reproduces `ct16.stats.alpha_fit` and `sharpe` to 1e-12 on a test draw.

| probe | what it checks | output |
|---|---|---|
| `p0_records.py`, `p0b_power_at_crit.py` | re-reads of the drafter's synthetic `power_records.json`: selection on span (a), the economic bar on passes, the α t as a decay statistic, power at other critical values | `p0_log.txt`, `p0b_log.txt` |
| `p1_size.py` | 11 trend-free worlds × 1,000 draws, incl. a 3,000-draw re-run of the drafter's binding world; 3 regime worlds and 3 trend worlds × 600; constant / year / half-year slopes | `p1_summary.txt`, `p1_out.json` |
| `p1b_bull_leverage.py` | what drives the false passes: drift × GARCH asymmetry × tails, 7 worlds × 1,000 | `p1b_summary.txt` |
| `p1c_lo_timing.py` | a holdings-based timing statistic (Lo 2008) as the fix, 7 worlds × 400 | `p1c_summary.txt` |
| `p23_worlds.py`, `p23_analyse.py`, `p2b_alpha_h2.py` | span (a): trend on/off by span, 8 worlds; H2: SR 0 … 1.57 × forward market drift 0 / ±40%/yr, 18 worlds; 600 draws each | `p23_analysis.txt`, `p2b_log.txt` |
| `p6_tail.py` | gross held *into* a crash day that follows a calm rally, against the unconditional gross | `p6_log.txt` |

**Reviewer exposure (for the ledger).** I read the paper in full: the authors' PDF in `data/scout_c1/paper/`, SHA-256
checked against its `SHA256SUMS`, with text extracted by PDFKit into `scratch_review016a/paper.txt`. I also read the
draft, its code and logs, the scout, `power.txt`, `000t` and `000l`, the programme doc §2 and its C1 entries, and the
orchestrator prompt's "Process" and "Lessons". **Designer knowledge:** like the drafter, I know the broad
2025–26 market path (the rally into 2025-10, the 10-10 cascade, the decline into 2026). I used it only to shape
synthetic worlds. **Real data read: none.** No kline, funding or gross value, and nothing dated on or after 2025-03-20.

---

## Verdict: not registrable as drafted; register after the listed fixes (Revision 1, then a recheck)

The rule is the paper's, faithfully transcribed. The places where the paper is silent are decided by mechanism.
The universe and execution choices are defensible. The draft is right to reject calendar-cell slopes: they absorb
0.6–1.5 of the α t of a real trend. Two things break before the freeze:

| item | one line |
|---|---|
| **B1** | CAL_H1 = 1.731 is under-sized. In a bull market with a leverage effect, constant slopes manufacture α: false pass 0.05–0.12. The drafter's own binding world re-runs at q95 1.83, not 1.73 |
| **B2** | Span (a) can decide alone. With trend in the designer-known year and none after it, the registered rule passes 0.20–0.24. This breaks `000t` ("never its sole decider") and §7's own claim |
| M1 | H2 on raw Sharpe mostly measures the forward market drift. ±40%/yr moves mean T by ±1.2. The size at a true 1.57 reaches 0.16 in a bear, and the power at a dead book falls to 0.17 in a bull. An α-based decay statistic has power 0.96–0.98 at SR 0 under any drift |
| M2 | When the book is dead, `undecided` happens 45–79% of the time, and §14's kill switch never fires on `undecided`. The owner keeps trading a dead book |
| M3 | The economic bar is decorative. It clears on 85–99% of H1 passes, and on 67–90% of passes in SR-0 worlds |

---

## BLOCKERS

### B1. The calibration misses the leverage-bull worlds, where constant slopes create α out of a constant drift

**Evidence** (`p1_summary.txt`, `p1b_summary.txt`, `p1c_summary.txt`). Rates are the share with t ≥ 1.731; "H1" adds
α̂_c > 0. GJR means GARCH(0.05, 0.88) with leverage term γ = 0.10.

| trend-free world (constant drift unless stated) | const: mean t / q95 / H1 | year cells q95 / rate | half-year q95 / rate |
|---|---|---|---|
| drafter's binding world re-run (cells aligned, t3, GJR), **3,000 draws** | +0.24 / **1.83** / **0.057** | 1.33 / 0.023 | 0.02 / 0.000 |
| bull +40%/yr, GJR, t3 | −0.42 / 1.47 / 0.024 | 0.75 / 0.005 | 0.06 / 0 |
| bull +80%, **symmetric** GARCH, t3 | −0.58 / 1.25 / 0.012 | 0.84 / 0.008 | 0.23 / 0.001 |
| bull +80%, GJR, t3 / t4 | +0.05 / 1.83 / **0.056** · +0.05 / 1.79 / 0.051 | 1.37–1.40 / 0.02–0.03 | 0.64–0.80 / ≤ 0.008 |
| bull +80%, GJR, t3, one −35% day (in (a) / in (c), two seeds) | 1.94–2.07 / **0.072–0.091** | 1.59–1.72 / 0.04–0.05 | 0.76–0.96 / ≤ 0.010 |
| bull +120%, GJR, t3 | +0.54 / **2.27** / **0.116** | 1.90 / 0.068 | 1.18 / 0.012 |
| vol timing pays, asymmetric (+100% in low vol, −60% in high), GJR t3 | +0.54 / 2.19 / **0.116** | 1.84 / 0.065 | 1.24 / 0.015 |
| bear −60%, GJR / alt-minus-BTC drift / β rising in uptrends / 2022 bear with +12% squeezes / universe entering on rallies (flat, bull) / the 2025–26 path in (a) with alt bleed | q95 0.24–1.60 / H1 0.002–0.032 | — | — |

- **Mechanism.** OLS β is variance-weighted. Under a leverage effect, the book is out after drops, which is when
  vol is high, so β̂ sits below the book's average exposure w̄. The result is α̂ ≈ (w̄ − β̂)·μ > 0 in any bull.
  That α comes from the regression's weighting, not from timing returns: a static position of w̄ earns the same
  mean. The symmetric-GARCH control (0.012 against 0.056) isolates the asymmetry. The draft's calibration set has a
  symmetric-GARCH bull (q95 1.03), and a t3 leverage world whose cell drifts average only +8%/yr. It never has
  **leverage + sustained bull**, the configuration of 2020–21 and 2023–24.
- **The registered number is also a Monte Carlo low.** A q95 from 600 draws has SE ≈ 0.09. Re-run with my
  generator at 3,000 draws, the drafter's binding world gives q95 1.83 and a false pass of 0.057.
- **No fixed CAL is safe for every drift.** The excess grows with μ (+40: 0.024, +80: 0.056, +120: 0.116). Year
  cells halve the rate but do not cure it (0.068 at +120%), and they cost 0.17–0.27 of power at SR 1.
- **D14 itself is right.** With trend at SR ≈ 1, the mean t is 2.35–2.73 with constant slopes, 1.59–2.15 with year
  cells and 0.88–1.45 with half-year cells. H1 power is 0.71–0.83, 0.46–0.64 and 0.25–0.41 respectively
  (`p1_summary.txt` T1–T3). Even recalibrated to the worst world here, constant slopes beat year cells at matched
  size (below). The departure from the brief's "β per calendar cell" is justified. The calibration is the problem.

**Fix, preferred: make the primary statistic a holdings-based timing test.** Use
A_t = Σ_i (w_i,t−1 − w̄_i) r_i,t − cost_t, where w̄_i is coin i's mean book weight over its member-days (Lo 2008,
"active component"). The t of its mean uses CR1 by month. Its mean is ~0 in **every** constant-drift world,
whatever the vol dynamics. It keeps the gains from drift that changes over time, which is what trend following
harvests.

| world | α t (registered): q95 / P(≥ 1.731) | timing t_A: q95 / P(≥ 1.731) |
|---|---|---|
| flat GJR t3 | 0.79 / 0.007 | 1.04 / 0.005 |
| bull +80% GJR t3 | 2.03 / 0.085 | 1.11 / **0.013** |
| bull +120% GJR t3 | 1.97 / 0.105 | 1.08 / **0.005** |
| vol timing pays (asym.) | 2.27 / 0.168 | 1.65 / 0.035 |
| trend SR≈1, h 20 / 60 / 120 (**power**) | 0.85 / 0.74 / 0.72 | **0.87 / 0.71 / 0.61** |

- **Costs:** it needs the per-coin weights (`run_book(keep_paths=True)` already exposes them), a recalibration over
  the extended world set at ≥ 3,000 draws for the binding worlds, and tests. The power cost at SR 1 is 0–0.11
  (largest at h 120, where w̄_i absorbs a trend that persists all 33 months). The α t stays as a reported block.
  The forward-sign or B2 condition applies to t_A on (c).
- **Fallback:** keep the α t and add, at ≥ 3,000 draws, bull +80% and +120% with GJR t3, the same with a −35% day,
  and the asymmetric vol-timing world. CAL becomes ≈ 2.1–2.3. Power at SR 1 falls from 0.83/0.72/0.68 to
  0.71–0.74 / 0.58–0.61 / 0.51–0.56 (h 20/60/120; `p0b_log.txt` at crit 2.06–2.2), and the rule is still exposed
  to a bull beyond +120%/yr.
- Either way, state CAL to the precision the Monte Carlo supports (±0.04 at 3,000 draws), not as "1.731".

### B2. The designer-known year can carry the pass alone; the forward-sign condition does not stop it

`000t` (binding): "Span (a) may be a stratum of a trend registration, never its sole decider". The programme doc's
C1 decision says the same. §7 claims that "a pass can never be carried by the designer-known months". Requiring
only α̂_c > 0 does not deliver that, because a sign is a coin flip when (c) is dead.

**Evidence** (`p23_analysis.txt`, 600 draws per world; `p0_log.txt` on the drafter's records). S1 is trend in the
warm-up and (a) at SR ≈ 1.5, none in (c). S2 is the same at SR ≈ 2.5. "Selection" conditions the reviewer's 11
trend-free worlds on the (a) stratum having looked good.

| design (pooled CAL 1.731 unless stated) | S1 (c) dead | S2 (c) dead | selection t_a ≥ 1.5 | both SR 0.75 | **both SR 1** | both SR 1.5 | (a) dead, (c) SR 1 |
|---|---|---|---|---|---|---|---|
| **registered** (t ≥ CAL, α̂_c > 0) | **0.198** | **0.238** | **0.241** (drafter's records: 0.200) | 0.498 | 0.693 | 0.955 | 0.263 |
| t ≥ CAL and t_c ≥ 0.5 | 0.107 | 0.140 | 0.196 | 0.457 | 0.643 | 0.947 | 0.255 |
| t ≥ CAL and t_c ≥ 1.0 | 0.042 | 0.062 | 0.141 | 0.390 | 0.570 | 0.905 | 0.240 |
| **t ≥ CAL and t_c ≥ 1.282** | **0.023** | **0.035** | 0.099 | 0.328 | **0.512** | 0.852 | 0.230 |
| same, with CAL 2.1 (B1 fallback) | 0.020 | 0.035 | 0.066 | 0.270 | 0.455 | 0.837 | 0.140 |
| (c) alone, t_c ≥ 1.978 (calibrated on my nulls) | 0.000 | 0.003 | 0.025 | 0.182 | 0.318 | 0.675 | 0.317 |
| (c) alone, floor t_14 1.761 (under-sized: q95 of t_c reaches 1.98) | 0.003 | 0.010 | 0.037 | 0.242 | 0.412 | 0.752 | 0.380 |

- **This is the realistic hindsight model.** The drafter's hindsight world asks whether the designer-known *index
  path* manufactures α. It does not: 0.017 there, and 0.032 in my version with alt bleed and GJR. The risk is
  different: the designer may have chosen to test now *because* (a) looked like a good trend year. Given that, a
  dead forward span passes one time in four or five.
- **Dropping (a)** removes the problem but costs 0.28–0.38 of power at SR 1 (0.69 → 0.32–0.41).
- **Requiring (c) alone to be one-sided significant at 10% (t_c ≥ 1.282)** caps the hindsight false pass at 0.02–0.035
  in the S worlds. Under the stronger selection model (t_a ≥ 1.5) it caps it at 0.07–0.10, against 0.24 now. It
  costs 0.18 of power at SR 1 (0.69 → 0.51) and keeps most of what pooling buys. A softer t_c ≥ 1.0 costs 0.12
  but leaves 0.04–0.06 (S) and 0.14 (selection).

**Fix.** Replace D18's sign condition with **t_c ≥ 1.282** on the primary statistic fitted on (c) alone. Restate §7:
(a) adds precision, and (c) must stand on its own at 10%. Re-derive the §6 power table. **Cost:** −0.18 power at
SR 1 (−0.17 at SR 0.75, −0.10 at SR 1.5). Combined with B1's fallback, power at SR 1 is ≈ 0.46. That is
honest: the scout already said "alive at half strength" cannot be shown this decade.

---

## MAJORS

### M1. H2 on raw Sharpe mostly measures the forward market drift; an α-based decay statistic has twice the power

**Evidence** (`p23_analysis.txt`, `p2b_log.txt`; trend h 60 at the drafter's κ; a market drift of 0 or ±40%/yr
over 2025-03-20 → 2027-12-31; 600 draws).

| true book (κ) | fwd drift | mean ŜR | mean T | mean α t | registered H2 rejects (T < 0.117) | α-t H2 rejects (t < 1.856) |
|---|---|---|---|---|---|---|
| SR 0 | 0 / −40% / +40% | 0.05 / −0.76 / 0.72 | 0.08 / −1.26 / 1.20 | −0.10 / −0.11 / 0.00 | 0.46 / 0.83 / **0.17** | **0.977 / 0.978 / 0.957** |
| SR 0.5 | 0 / −40 / +40 | 0.53 / −0.18 / 1.06 | | 1.06–1.12 | 0.23 / 0.59 / 0.08 | 0.77 / 0.79 / 0.75 |
| SR 0.75 | 0 / −40 / +40 | 0.69 / 0.16 / 1.22 | | 1.64–1.77 | 0.18 / 0.43 / 0.05 | 0.52 / 0.57 / 0.52 |
| SR 1 | 0 / −40 / +40 | 1.02 / 0.52 / 1.46 | | 2.36–2.42 | 0.11 / 0.26 / 0.03 | 0.30 / 0.27 / 0.31 |
| **SR 1.57 (size)** | 0 / −40 / +40 (h 60) | 1.62 / 1.21 / 1.95 | | 3.85–4.00 | 0.015 / **0.077** / 0.003 | 0.028 / 0.035 / 0.037 |
| **SR 1.57 (size)** | 0 / −40 (h 120) | 1.54 / 1.10 | | 3.87 | 0.067 / **0.157** | 0.050 / 0.040 |

- **The market path is a third of the paper's own Sharpe.** Table 4's book has β 0.08 to BTC over 2015–2025, when
  BTC's arithmetic drift was of order +70–80%/yr. That β alone contributed ≈ 6%/yr of the ≈ 17%/yr mean. A forward
  span's drift of ±40% moves T by ±1.2 and ŜR by ±0.75, and the α t by ±0.05. The draft calls a bear market "the
  conservative direction for a kill switch". It is a **size violation**: a book truly at 1.57 is declared decayed
  8–16% of the time. In a bull, the kill switch fires on a dead book 17% of the time.
- Recalibrating T over the drift worlds (crit −1.72) restores the size, but power at SR 0 collapses to 0.10.
  Raw Sharpe cannot be rescued.
- **The α t is the better decay statistic.** Its critical value is the smallest q05 over six as-published worlds
  (h 20/60/120, drift 0/±40): 1.856. Size is 0.015–0.050 everywhere. Power is 0.96–0.98 at SR 0, 0.75–0.79 at
  SR 0.5, 0.52–0.57 at SR 0.75 and 0.27–0.31 at SR 1, and it does not move with the drift. The drafter's own records
  agree: 0.98–0.99 at SR 0 against 0.50 (`p0_log.txt`). The BTC-only α t (the paper's α object) is worse (0.35–0.71
  at SR 0) because alt drift leaks into it.
- **Caveat.** An α-based critical value depends on mapping "as published" to an α information ratio. In the sim's
  SR-1.57 worlds the book runs at 16% vol with α t ≈ 3.9. The paper's book ran at 9% vol, α 10.8%/yr, β 0.08,
  IR ≈ 1.5. A conservative version (crit 0.8, the noncentral-t q05 at IR 1.52 over 33 months) still gives power
  0.74–0.81 at SR 0, with size ≤ 0.005 in every as-published world here.

**Fix.** H2 tests the paper's α, not its raw Sharpe. Calibrate κ so the simulated 10-year in-sample book reproduces
Table 4 (vol ≈ 9%, β ≈ 0.08, α ≈ 10.8%/yr). Take c2 = the smallest q05 of the primary statistic (α t, or t_A if B1's
preferred fix is adopted) over those worlds × h ∈ {20, 60, 120} × forward drift ∈ {0, ±40%}. Keep the raw Sharpe
as a reported block. **The outcome table changes shape.** With c2 below CAL_H1, a pass implies "not rejected", so
the four outcomes become a ladder: `alive` (pass), `not_demonstrated` (c2 ≤ t < CAL, or the (c) condition fails)
and `decayed` (t < c2). Rewrite §4.3 accordingly. **Cost:** a calibration run and §4.2/§4.3/§6 rewritten. No data
cost.

### M2. A dead book ends `undecided` about half the time, and §14 keeps trading it

**Evidence** (`p23_analysis.txt`, registered rules). Among dead-book outcomes, `close` is 0.46 / 0.82 / 0.17 and
`undecided` is 0.52 / 0.15 / **0.79** at forward drift 0 / −40% / +40%. The drafter's zero-drift worlds give
0.45–0.47 `undecided` (`p0` re-read). §14's kill switch is "an H2 rejection" only. So if the strategy is dead,
the owner's allocation runs on past 2028 about half the time, and four times in five in a bull.

**Fix.** §14: the allocation ends at the read unless H1 passes. `not_demonstrated` ends it too, because it carries
no evidence. With M1's statistic, `decayed` also comes out at 0.96–0.98 when the book is dead. **Cost:** none to the
test.

### M3. The economic bar almost always clears, including on false passes

**Evidence** (`p0_log.txt`, the drafter's records, no funding). The bar is k = 2.8 × min(raw, α) × (1 − 1/t²) ≥ 4%.
P(clears | H1 pass) is 0.85–0.99 across the trend worlds and **0.67–0.90 on the passes in the SR-0 worlds**. At
t = 1.731 the shrink factor is 0.67, so the bar needs only ≈ 2.1%/yr at paper sizing. Every H1 pass at a plausible
SE (≈ 3%/yr) gives ≥ 5%. Multiplying by k = 2.8 compares a 25%-vol book's return with HLP's calm 4%, which is
earned at a fraction of that risk.

**Fix (choose one, state it now).**
1. Compare like risk: the shrunk Sharpe of the deployable variant against HLP's (≈ 1.4 in the programme doc).
2. Or the shrunk return at the **Bayes-Kelly size implied by the shrunk Sharpe**, against 4%.

Either way, state honestly that the bar is a sanity floor. Also, min(raw, α) charges the long-only book for the
realised market path, which says nothing about the forward market. Use the α leg alone, net of the deployable
variant's funding. **Cost:** a line of code and a sentence; it routes only.

---

## MINORS

1. **"True SR" in §6 is the mean of per-draw Sharpes, not the population Sharpe.** At low SR the ratio is biased low:
   the "SR 0" worlds have a population Sharpe of 0.16–0.24, and "SR 1" has 1.07–1.16 (`p0` re-read of the
   drafter's records). Label the axis "mean per-draw ŜR", or calibrate κ to the population ratio. Power at a stated
   true SR is slightly overstated.
2. **The paper's Sharpe convention.** Table 4's own numbers imply a raw mean/vol of ≈ 1.8–2.0: 443% over 10.21 years
   is CAGR 18.0%, arithmetic ≈ 17.0%, at 9% vol. The published Sharpe is 1.57. So the paper subtracts a cash rate of
   ≈ 3%, or annualises with √252. "Raw Sharpe = 1.57" is therefore ≈ 0.3 below the paper's raw object. This is
   conservative for H2, but §4.2's "the paper's own object" is not exact. State the convention. It is moot if M1
   moves H2 to α.
3. **Perp age is not "irrelevant from 2025".** Renamed or migrated assets get new perp symbols, which reset both the
   365-day age and the Donchian history: MATIC→POL (2024-09), FTM→S, EOS→A, MKR→SKY, RNDR→RENDER. The paper's
   CoinMarketCap IDs keep both. Keep the rule (it is point-in-time and unambiguous), but correct §3. Report the
   members excluded by age alone and the sleeves flat for lack of history.
4. **Ranking window.** The paper ranks by "median daily trading volume **during the same month**" (p. 18). D8 uses the
   trailing 30 days for both filter and rank. They differ only in 31-day months and in February. Record it as a
   decision, not a quote.
5. **Volume source.** Binance perp quote volume tilts the top 20 toward perp-speculative names, relative to
   CoinMarketCap's spot aggregate. The draft's reasons for it are good. But H2's "like with like" holds for the
   rule, not for the universe. Add a reported overlap with a Binance-*spot* volume ranking (same free archive).
6. **Placebo offsets.** A circular shift with an offset within a few hundred days of 0 or of the series length
   reproduces the actual book (a lag) or looks ahead. Draw offsets from [400, len − 400].
7. **Bayes-Kelly ignores parameter variance.** With a two-point prior (SR 0.78 at 1/3, −0.15 at 2/3), Var(SR) ≈ 0.19,
   so f ≈ E[SR]/(1 + Var) gives ≈ 13.5% vol, not 16%. The paper's 9% is ≈ 0.67 of Bayes-Kelly, not half. The
   conclusion (start at paper sizing) stands.
8. **Tail: the p99 column is the central case, not the extreme.** In synthetic worlds where a −35% day follows a calm
   rally, the gross held into the crash has p50 2.5× the unconditional mean and p90 4× (`p6_log.txt`). The book
   loses a median 18% of equity that day (p10 −28%) at the world's 20% book vol. With the scout's mean 0.08 and
   max 0.31, a 2025-10-10-type crash after a calm rally sits at the p99 gross. At k = 2.8 that is −17% for −23% EW
   and −32% for −43% EW. Write the worst plausible week at 25% vol as −30 to −40%. The synthetic MDD p05 in a
   two-crash world is −40% at 20% vol, against the draft's "11% → 31%".
9. **k = 2.8 is a multiplier, not a vol target.** Realised vol at k = 2.8 moves with gross (mean 0.08, p99 0.27), so
   "25% book vol" is an average. Say so in §4.4 and §9.
10. **H2's critical value depends on h** (T q05 1.22 / 0.54 / 0.12), and taking the minimum gives H2 a size of 0.005
    at h 20. This is superseded by M1.

---

## Checked and sound

- **Verbatim rule** (paper pp. 7–10, 14–19, Appendix p. 33, all read):
  - Donchian on closes with today included.
  - Entry on Close = Up; exit on Close ≤ stop, the formula's sign.
  - Stop_{t+1} = max(Stop_t, Mid_t), never lowered; footnote 3's initial stop at Mid; the case order.
  - The nine lookbacks; w_n = min(0.25/σ, 200%) × Pos_n; σ over 90 days ("3-month"); the Combo as the mean of nine;
    r_t = w_{t−1}·ret_t.
  - 10 bp; the 20% threshold applied only to vol-driven changes (pp. 14–15).
  - The universe filters (≥ 365 days; no stablecoins, wrapped tokens or NFTs; median ≥ $2 M; top B); 1/B of capital
    per asset with the monthly reset (the AUM formula); the §7.2 exits ($1 M, 0.5%); long-only; B = 20 (Table 4's
    headline).
  - The code (`signal_path`, `targets`, `rebalance`, `select_universe`, `liquidity_exit`, `run_book`) implements
    each item.
- **Where the paper is silent, decided by mechanism:** √365 (CMC daily data includes weekends); n − 1; flat until n
  closes; an absolute threshold (allocations are quoted in %, as in "200%"); new members at their current target;
  netting the nine sub-model trades within a coin; continuing members keep their held weights. Each is the natural
  reading, and none was chosen for an outcome.
- **Perp closes with funding excluded ≈ the paper's spot object.** The basis change nets out over months, and
  funding belongs to the deployable variant (D11). D6's attribution of funding to (d 00:00, d+1 00:00] matches a
  trade just after the 00:00 close, which pays the 00:00 settlement on the old position. 6 bp (5 bp taker + 1 bp) is
  realistic for top-20 perps at $1 M.
- **The delisting cut** (traded on each of the last 7 days; settle at the last traded close; the §7.2 exit every
  close) handles the LUNA case and the flat rows of dead contracts.
- **The factors.** BTC close-to-close; the EW alt basket of the book's own members; VM as the same book with Pos ≡ 1,
  gross of costs. VM correctly carries the vol-sizing component. Trailing-trend cells are correctly excluded.
- **D14's direction.** Constant slopes over calendar cells, with numbers in B1. Month clustering is valid under
  overlapping holdings: under a martingale null the scores are martingale differences. The size failures above come
  from mean misattribution, not from the SE.
- **Worlds where the registered H1 holds size:** flat (with or without GJR); bear; alt-minus-BTC drift; β rising in
  uptrends; a 2022-style bear with short squeezes; a momentum crash after a quiet rally; a universe entering on
  rallies; the 2025–26 path in (a) with alt bleed. All give 0.002–0.032 (`p1_summary.txt`). Year-aligned ±80%
  regimes pass 0.105, but a year-long regime is a trend the rule legitimately harvests.
- **Small items.** The forward-sign power cost (0.02–0.04) reproduces. Turnover and fee arithmetic, capacity (≈ 0.17%
  of the rank-20 coin's volume at $1 M) and the Kelly ratio (25% vol ≈ 0.32 Kelly at SR 0.78) are right. The
  economic bar is applied once, after H1, on a shrunk estimate. That structure is sound; M3 is about its scale.
- **Disclosure.** Designer knowledge is stated, and `000l` and 008 are disclosed inside (c).

---

## Files

All under `research/alpha/scratch_review016a/`:
- **Code:** `common.py`, `p0_records.py`, `p0b_power_at_crit.py`, `p1_size.py`, `p1b_bull_leverage.py`,
  `p1c_lo_timing.py`, `p23_worlds.py`, `p23_analyse.py`, `p2b_alpha_h2.py`, `p6_tail.py`, `pdftext.swift`.
- **Outputs:** the `*_log.txt`, `*_summary.txt`, `*_out.json` and `*_records.json` files, and `paper.txt`.

Seeds are fixed, so every table above can be re-run. Nothing else in the repository was modified, and no
`results.jsonl` was written.
