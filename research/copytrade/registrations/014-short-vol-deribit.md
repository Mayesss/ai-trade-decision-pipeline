# 014 — A sized short-volatility book on Deribit BTC/ETH options, with a stress filter (N2(b); Deribit options family, T1 + T2)

**Status: DRAFT — PARKED 2026-10-13 (orchestrator, under the owner's delegation). Not registered; no
outcome was read on either calendar; no trial is spent.**

**Why parked (orchestrator, after `reviews/014-review-a.md` and `014-review-b.md`).** The decision rule
fixed before the tail computation (`000m`; programme doc §3, 2026-10-12) was: review if the realistic
tail allows a materially larger size, otherwise park. Review A (M1) showed the larger size does not hold:
with daily hedging a one-day gap costs nearly the whole move (−46% of notional for a −50% gap on any
day), the 2020-03 week was mild because the fall spread over five hedges, and one extra single-gap week
cuts tail-aware Kelly from ~1.7 to ~0.55. A robust quarter-Kelly size is n ≈ 0.10–0.15, where the expected
return at the published VRP (0.10 of premium) is about +2–3%/yr after the tail charge — below HLP's
lending leg — and joint power is about 0.06. Both reviews also found the entry premium priced off the
index rather than the expiry's forward (an error of ~3% of premium, median, up to 16%, the size of the
edge under test; review A B1, review B M4), plus a holdout gate that could never open in the real layout
(review B B1) and the integrity faults fixed in 008. **For any later short-vol design:** price off the
forward recovered from each trade's `price`/`iv`; hedge more often than daily or price the single-gap
week explicitly; detach the size from the test; add the regime null (review A M2) to the floor
calibration. The pull and both panels stay on disk and pinned; the 2021-03-24 → 2026-06-30 Deribit
calendars remain unread by any outcome computation.

**Revision 1 (2026-10-13, orchestrator decision `000m`): the realistic hedged tail.** The registered
H24 mechanics, unchanged, were run on the 21 out-of-calendar weeks (entries 2019-03-29 → 2021-03-05, all
holds ending before 2021-03-24) whose BTC or ETH weekly excursion is ≥ 25 %, selected on the underlying
alone, plus the 2020-03 cluster for the 4-week path (§5). The realistic worst week is −14.1 % of notional
(adversarial bound −53.8 %), no week liquidates at n ≤ 0.45, and the tail charge falls from 0.264 % to
0.172 % of notional a week. The tail-aware Kelly rises to 1.65 (fat world, VRP 0.10) and the **proposed
size to n = 0.40** (D9), still inside the adversarial stress's tail budget. Entry, instrument, hedge,
criteria and calendars are unchanged. §5, §6 (criterion 5 and the deploy bar at the new n), §8, §9, §10,
§11 and §12 are updated; Revision 0's tail is kept as the reported stress (§5a).

Drafted 2026-10-12 for the orchestrator and the owner (round 3, light N2(b)). Every row of the decision
table (§10) is PROPOSED with a recommendation. The design-sized pull is on disk and verified (§2). The
mechanics are built and checked on synthetic data (§9). **On the two test calendars (2021-03-24 →
2026-06-30) no premium was compared with anything realised, no option was marked to market after entry,
and no payoff, P&L, hit rate, IV change after entry, realised vol after entry or stress-versus-outcome
statistic was computed.** The only real payoffs computed are the authorised out-of-calendar tail weeks
of Revision 1 (all before 2021-03-24).
On real data only coverage counts were computed (§2, §12). Nothing dated on or after 2026-07-01 was
requested or received.

The runner (`r014_run.py`) refuses to compute an outcome until this file says REGISTERED, is tracked,
committed and unmodified; the guarded code is clean; the code and both panels match §9's SHA-256 table;
and no 014 row of any kind exists in `results.jsonl` — in the working tree or in any branch's history.

Programme: `docs/edge-discovery-next-prompt-2026-10-12.md` (mandate), light N2. Scout:
`scouts/n2-options.md`, `data/scout_n2/analysis_all.txt`. Ledger entry read: `000k`. Patterns and
reviews read: 009, 012, `reviews/*`.

## 0. Hypothesis, why this design, and what has been seen

**Hypothesis.** A weekly book that sells 7-day at-the-money BTC and ETH straddles on Deribit,
delta-hedges them daily on the coin's perpetual and holds them to expiry earns a premium **net of
Deribit's real costs** (taker spread against the mark, the 12.5 %-capped option fee, the delivery fee,
perpetual hedge fees, funding), and that premium is large enough, at a size fixed by its measured tail,
to beat HLP's ~3 %/yr lending leg. This is the owner's "levered risk premium with a measured tail":
the premium is the crypto variance risk premium (VRP), whose **sign is a public prior** (below); its
**size net of costs on our calendars is unknown to our code**, and that is what 014 tests.

**Two trials (family of 2, Bonferroni):**
- **T1** the book every week;
- **T2** the same book standing aside (idle, zero return) in any week whose entry is in a **stress
  state**: for either coin, DVOL at the last hourly close ≥ 1 h before entry is ≥ 1.25 × its minimum over
  the trailing 7 days.

**Orchestrator decisions, with the reasons I accept them.**
- *Idea (a), long gamma on a dense liquidation map, is not drafted.* The scout's costs make 5–15 Δ
  wings cost 10–20 % of premium per fill (fee cap binding, half-spread 2–7 %), and a dense BTC map
  coincides with **lower** implied vol (Spearman −0.46): what (a) buys is mostly a low-vol regime marker,
  bought with a −VRP drag. MDE ≥ 0.7 premium per trade on the only map year.
- *The map is not used at all.* Its increment over plain VRP is below detection (MDE ≈ 0.48 of premium,
  scout §5), and it exists only in the HL discovery year, which already carries 16 trials. 014 uses no
  HL data and adds nothing to the HL family.
- *Power fix.* A weekly **naked** straddle has payoff SD ≈ 1 premium (MDE ≈ 0.22 vs a published VRP ≈
  0.10). §3 compares, on SYNTHETIC data only, four constructions — unhedged weekly, weekly delta-hedged
  every 24 h or 8 h, and seven 1-day straddles a week — at equal gross VRP, with each one's own costs and
  its own skew-calibrated critical values, and picks the 24 h-hedged weekly straddle (§3, D4).

**Why a DVOL stress rule for T2, not a Binance OI-drop rule** (chosen from mechanism and data, never
from outcomes):
- *Mechanism.* Short-vol losses cluster when a vol regime breaks: realised vol keeps rising after a
  spike (GARCH clustering and the leverage effect), forced deleveraging comes in clusters (the scout's
  ≥ $77 M days cluster in runs of 6–13), and the one published episode of **negative** crypto VRP
  (Almeida et al., 2020-03-15 → 04-08) began right after a spike. A 25 % jump of the 30-day implied
  index within a week is the option market's own record of such a break. The counter-mechanism is also
  real — IV overshoots after spikes and the premium for selling then can be largest — and that is
  exactly what T2 against T1 measures.
- *Data.* DVOL is free, keyless, hourly, point-in-time and on disk for both calendars from one source,
  with **0 missing hours** (§2). Binance OI is missing for 2021-03 → 2021-10 and holdout A, and the
  scout found artefacts its cleaning rule could not fully remove ("2023 counts more deep drops than
  2022"); its deleveraging hours are so rare (17–23 days in two years) that T2 would almost equal T1.
- *Threshold.* 1.25 is scale-free (the coins' 7-day DVOL-change SDs relative to level are alike: 8.4 /
  59.4 and 9.9 / 72.6, scout) and sits near 1.8 SD of the weekly relative change, so that the book idles
  roughly one week in ten to fifteen. The synthetic world idles 6 % (it has no IV spikes); the real share
  is printed before the decision and never chosen after it.

**What has been seen (and what has not).**
- **Public priors on the sign.** Almeida, Grith, Miftachov & Wang (2024/25), Deribit 2017-07 →
  2022-12: BTC VRP 0.14 in annualised variance, higher in low-vol clusters, negative 2020-03-15 → 04-08
  (overlaps discovery). Amberdata/Deribit Insights: 30-day IV above RV ~70 % of the time since 2019-04.
  **Deribit Insights (2024-09-19) published a 0.35 Δ weekend-strangle backtest, 2020 → 2024-08: 27.4 %
  APR, 23.8 % max drawdown — an outcome of a short-vol design that overlaps our holdout (2024-01 →
  2024-08).** Our holdout is fresh to our computations, not to published short-vol backtests (`000k`).
- **`000k` (scout reads).** Unconditional DVOL levels and changes, unconditional weekly underlying SD
  and kurtosis, the stylised unhedged straddle worst week/month, a cost table from 7 full trade days
  (three of them in our holdout: 2025-10-10 — a Friday entry day — 2026-03-16, 2026-06-15), and ATM mark
  IV at 286 HL snapshot hours (2025-07-29 → 2026-06-30, inside our holdout). A reader could form a crude
  unconditional implied-versus-realised comparison from the DVOL median and the weekly SD; **nobody
  computed one, on any window, for any construction.** No statistic pairs a premium with what followed.
- **This draft's own reads** (§12): coverage counts only.
- **Parameters set from those reads:** none beyond what §3–§5 label as power, cost and tail inputs (the
  scout's cost table, unconditional dispersion facts and printed worst-week excursions).

## 1. Trials, family and calendars

| id | book | role |
|---|---|---|
| **T1** | sell 7-day ATM straddles BTC + ETH every Friday 08:00 UTC, 24 h delta hedge, hold to expiry | trial 1 |
| **T2** | T1, idle in stress weeks (DVOL ≥ 1.25 × trailing 7-day minimum, either coin) | trial 2 |

- **Family.** Deribit options data are new to the ledger: **the Deribit-options family starts here, at
  2 trials** (T1, T2), Bonferroni one-sided 2.5 % each at discovery. No Binance, Bybit or HL data are
  used: the Binance 2021–23 family (1), the L2 carry family (3) and the HL discovery year (16) are
  unchanged.
- **Calendars and earlier looks sharing them.**

| calendar | dates | role | earlier looks at the same dates |
|---|---|---|---|
| discovery | entries Fridays 2021-04-02 → 2023-12-22 (143 weeks); every expiry before 2024-01-01 | decides whether the holdout is read | 009 discovery (Binance funding/returns, other instruments); the N2 scout's unconditional DVOL/perp reads and 2 cost-sample days (2021-06-15, 2022-06-15/11-09, 2023-06-15); Almeida et al. (published VRP to 2022-12) |
| embargo | the entry of 2023-12-29 (expiry 2024-01-05) | belongs to neither calendar | — |
| **decision holdout** | entries Fridays 2024-01-05 → 2026-06-19 (129 weeks); every expiry before 2026-07-01 | existence test, read once after a discovery pass and an approval line | **2024-01 → 2025-07-27 = 009's holdout A, closed, released, unread by our code** (009 read Binance only; never opened). **2025-07-28 → 2026-06-30 = the HL year**: 16 HL trials, none on Deribit options; the scout's unconditional DVOL, perp moves, ATM IV at 286 snapshot hours and 3 cost-sample days (`000k`). The Deribit Insights backtest covers 2024-01 → 2024-08 |
| sealed | 2026-07-01 → | never read by 014 | D18 of 012: L3 only |

- **Boundary rule (fixed now).** A position belongs to a calendar iff its entry is on or after the
  calendar's start and its expiry is strictly before the calendar's end; one crossing a boundary
  belongs to neither (the 2023-12-29 entry is the embargo week). Look-back data for the stress rule
  (8 days of DVOL before 2024-01-05) is discovery-period data and carries no outcome.
- **Written expectation.** Discovery pass (either trial): ~1 in 4 (power 0.2 at the prior VRP 0.10,
  0.6 at 0.15, §3; the prior is itself uncertain). Holdout existence given a pass: ~1 in 2. A deploy
  recommendation (existence and the tail-charged economic bar): ~1 in 12. **Even then, at the
  registered size (n = 0.40), the book earns ~5–6 %/yr after its tail charge at the prior VRP and
~15–17 %/yr at VRP 0.15 (§5): it cannot reach the mandate's 30–100 % zone.**

## 2. Data, pull and coverage

**Sources (all keyless, free, end-bounded < 2026-07-01; `r014_fetch.py`).**

| unit | endpoint | span | rows |
|---|---|---|---|
| DVOL 1h, BTC/ETH | `www.deribit.com/api/v2/public/get_volatility_index_data` (fixed 30-day forward windows; `continuation` = error) | 2021-03-24 00:00 → 2026-06-30 23:00 | 46,200 each |
| perpetual 1h candles | `get_tradingview_chart_data` (a candle kept only if it closes inside its window) | 2021-03-01 00:00 → 2026-06-30 23:00 | 46,752 each |
| perpetual funding 1h | `get_funding_rate_history` | 2021-03-01 01:00 → 2026-06-30 23:00 | 46,687 each |
| option trades | `history.deribit.com/.../get_last_trades_by_currency_and_time`, forward-paged, de-duplicated by `trade_id` | [Friday 04:00, Friday 08:00) for **274 Fridays 2021-04-02 → 2026-06-26**, per coin | 928,711 |

- **Not fetched:** the full option tape (not approved); `get_instruments` (no date bound — it returns
  today's state) and `get_delivery_prices` (no end bound, newest first). Both would read past the cutoff.
  Instrument existence comes from traded names; settlement from the trades' index price (§4).
- **The pull:** `data/r014/pull_20261012/` (new directory), **1,574 requests, 247.9 MB transferred,
  208.5 MB on disk, 555 files listed in SHA256SUMS** (sha256 of SHA256SUMS:
  `ca64f3444dd9ec2507f623a7390625eb958f63181fbb2c18b203de4f0eaf1bd7`). Errors were raised, never written;
  0 rows on or after 2026-07-01 were received (a hard error in the fetcher); tail coverage checked (every
  series ends 2026-06-30 23:00).
- **Panels** (`r014_build.py`, after re-verifying every SHA-256): `data/r014/panel_discovery/` (series
  stamped [2021-03-01, 2024-01-01); trade windows of Fridays 2021-04-02 → 2023-12-29) and
  `data/r014/panel_holdout/` (series [2023-12-24, 2026-07-01); Fridays 2024-01-05 → 2026-06-26). Each
  has a MANIFEST with per-file SHA-256; a rebuild is byte-identical (tested). **Both are hashed in §9 now:
  the holdout panel cannot change between registration and its read.**

**Coverage (counts only; `data/r014/logs/coverage.json`).**

| | discovery BTC | discovery ETH | holdout BTC | holdout ETH |
|---|---|---|---|---|
| entries | 143 | 143 | 129 | 129 |
| ATM straddle available (traded 7-day strike within 2.5 % of the index) | **143** | **142** (1 no strike within band) | **129** | **129** |
| entry index from a trade ≤ 30 min old | 143 | 143 | 129 | 129 |
| chosen strike's latest trade age, min (p50 / p90 / max) | 2.4 / 14.1 / 59 | 3.9 / 13.7 / 93 | 3.0 / 22.2 / 131 | 4.9 / 21.8 / 162 |
| settlements with ≥ 10 covered minutes in 07:30–08:00 | 143 | 143 | 129 | 129 |
| trade windows (with 0 trades) | 144 (0) | 144 (0) | 130 (0) | 130 (0) |
| option trades in the windows | 214,049 | 186,237 | 331,777 | 196,648 |
| windows with a next-day (daily) expiry traded | 144 | 144 | 130 | 130 |
| DVOL / perp hours missing in the calendar span | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| funding hours missing (treated as 0, counted at run) | 34 | 34 | 30 | 30 |

Dailies exist on every Friday of both calendars (relevant to construction D1 in §3).

**Read whitelist and lock.** The run opens only `data/r014/panel_discovery/` (discovery) or
`data/r014/panel_holdout/` (holdout, only after its gate), the registration and `results.jsonl`.
`Panel` refuses any other path, refuses the holdout panel without the gate's flag, refuses a discovery
run on a holdout directory, checks every file against its MANIFEST and asserts every row lies inside its
calendar's bounds (tested). No 014 source names another data directory (static test).

## 3. Power, construction choice and critical values (SYNTHETIC only)

`ct/shortvol_sim.py`; reproduce with `.venv/bin/python -I r014_run.py --power` (reads no data; writes
`data/r014/logs/power.json`).

**The synthetic world.** Daily log-vol AR(1) (φ 0.99), hourly Student-t(4) innovations, rare down-skewed
jumps, BTC–ETH correlation 0.8; the market prices options at its own vol forecast × m (plus 8 % pricing
noise), m calibrated per construction so that the **gross** mean P&L per unit premium equals the target
VRP. Costs are the run's own (`position_pnl`). Two worlds:

| fact (scout, real) | BTC real | BTC base | BTC fat | ETH real | ETH base | ETH fat |
|---|---|---|---|---|---|---|
| weekly SD | 0.09 | 0.09 | 0.085 | 0.113 | 0.12 | 0.104 |
| weekly kurtosis | 6.1 | 5.755 | 4.428 | 7.4 | 7.99 | 4.631 |
| \|r\| SD/mean | 1.03 | 0.969 | 0.873 | 1.01 | 0.985 | 0.911 |
| 1-SD strangle payoff SD/mean | 2.67 | 2.775 | 2.557 | 2.82 | 2.963 | 2.521 |
| 1-SD put payoff SD/mean | 4.95 | 3.53 | 3.467 | 4.87 | 4.002 | 3.688 |
| top-3 weeks share of strangle payoff | 0.16 | 0.183 | 0.152 | 0.2 | 0.215 | 0.15 |
| DVOL 7-day change SD (pts) | 8.37 | 9.12 | 9.3 | 9.89 | 12.92 | 12.68 |
| DVOL median | 59.4 | 61.8 | 63.1 | 72.6 | 78.3 | 78.1 |

(Synthetic values: medians over blocks of 307 weeks, as the scout measured 307 real weeks. Stress weeks: 5.9 % / 6.1 %.)

The base world fits everything but the put tail (synthetic put payoff SD/mean 3.5–4.0 against 4.9).
The fat world has rarer, larger, down-skewed jumps. Its medians over 307-week blocks look *tamer*,
because most blocks miss the big jump — which is the very sampling effect at issue — but its null
false-pass rates at Student critical values are the higher ones (H24: discovery 0.052 against 0.037,
holdout 0.090 against 0.066). **Critical values are calibrated in the fat world**; the real put tail is
fatter than either world's block medians, which is a stated weakness (§8).

**The skew problem, designed out.** A short-vol weekly series is negatively skewed and its worst weeks
are rare: in a finite sample the mean is usually above its expectation and the plain t's null
distribution sits to the right. At Student critical values the synthetic false-pass rates were 2–6× the
nominal (the 012 review's M4 lesson: the operative α is each stage's own). 014 therefore uses (i)
Hall's (1992) monotone skew-corrected t (`shortvol.skew_t`; Johnson's 1978 correction is not monotone
and flipped sign on a synthetic calm-world test) and (ii) **critical-value floors** equal to that
statistic's null quantiles in the fat world (net mean exactly 0; seeds 101 and 202, 20,000 weeks each,
6,000 block draws of 13 consecutive weeks; the max of the two, rounded up): **discovery 2.59 (q 0.975,
G 143); holdout 2.16 (q 0.95) / 2.52 (q 0.975)**. The critical value is max(t_{G−1}(q), floor).

**Construction comparison** (statistics only, criterion 5 off; each construction with its own floors;
3,000-week banks, 1,000 draws):

| construction | own floors disc / hold95 | world | VRP | net / premium | costs / premium | weekly SD / premium | worst synthetic week (notional) | P(disc T1) | P(hold alone) | P(joint any) |
|---|---|---|---|---|---|---|---|---|---|---|
| U | 2.09 / 1.78 | base | 0.1 | 0.067 | 0.034 | 0.75 | -80 % | 0.14 | 0.21 | 0.03 |
| U | 2.09 / 1.78 | base | 0.15 | 0.117 | 0.033 | 0.71 | -78 % | 0.43 | 0.45 | 0.19 |
| U | | base | net 0 (null) | 0 | | | | 0.018 (Student only 0.023) | 0.042 (Student 0.051) | 0.0010 |
| U | 2.09 / 1.78 | fat | 0.1 | 0.067 | 0.034 | 0.68 | -36 % | 0.20 | 0.21 | 0.04 |
| U | 2.09 / 1.78 | fat | 0.15 | 0.117 | 0.033 | 0.64 | -35 % | 0.48 | 0.47 | 0.24 |
| U | | fat | net 0 (null) | 0 | | | | 0.030 (Student only 0.034) | 0.059 (Student 0.075) | 0.0010 |
| H24 | 2.59 / 2.16 | base | 0.1 | 0.051 | 0.049 | 0.36 | -33 % | 0.19 | 0.26 | 0.04 |
| H24 | 2.59 / 2.16 | base | 0.15 | 0.103 | 0.048 | 0.35 | -32 % | 0.66 | 0.67 | 0.45 |
| H24 | | base | net 0 (null) | 0 | | | | 0.006 (Student only 0.037) | 0.036 (Student 0.066) | 0.0000 |
| H24 | 2.59 / 2.16 | fat | 0.1 | 0.050 | 0.050 | 0.36 | -42 % | 0.27 | 0.35 | 0.10 |
| H24 | 2.59 / 2.16 | fat | 0.15 | 0.102 | 0.048 | 0.34 | -41 % | 0.68 | 0.70 | 0.52 |
| H24 | | fat | net 0 (null) | 0 | | | | 0.015 (Student only 0.052) | 0.039 (Student 0.090) | 0.0000 |
| H8 | 2.76 / 2.4 | base | 0.1 | 0.042 | 0.059 | 0.29 | -28 % | 0.21 | 0.23 | 0.05 |
| H8 | 2.76 / 2.4 | base | 0.15 | 0.094 | 0.057 | 0.28 | -27 % | 0.61 | 0.61 | 0.37 |
| H8 | | base | net 0 (null) | 0 | | | | 0.013 (Student only 0.053) | 0.026 (Student 0.088) | 0.0010 |
| H8 | 2.76 / 2.4 | fat | 0.1 | 0.040 | 0.060 | 0.32 | -58 % | 0.30 | 0.33 | 0.10 |
| H8 | 2.76 / 2.4 | fat | 0.15 | 0.093 | 0.058 | 0.30 | -56 % | 0.66 | 0.64 | 0.47 |
| H8 | | fat | net 0 (null) | 0 | | | | 0.027 (Student only 0.072) | 0.046 (Student 0.126) | 0.0000 |
| D1 | 2.3 / 1.93 | base | 0.1 | 0.029 | 0.072 | 0.28 | -55 % | 0.17 | 0.20 | 0.03 |
| D1 | 2.3 / 1.93 | base | 0.15 | 0.080 | 0.071 | 0.26 | -49 % | 0.85 | 0.81 | 0.71 |
| D1 | | base | net 0 (null) | 0 | | | | 0.020 (Student only 0.034) | 0.042 (Student 0.064) | 0.0000 |
| D1 | 2.3 / 1.93 | fat | 0.1 | 0.028 | 0.073 | 0.27 | -32 % | 0.18 | 0.25 | 0.05 |
| D1 | 2.3 / 1.93 | fat | 0.15 | 0.079 | 0.071 | 0.25 | -29 % | 0.86 | 0.83 | 0.75 |
| D1 | | fat | net 0 (null) | 0 | | | | 0.032 (Student only 0.050) | 0.051 (Student 0.067) | 0.0020 |

**Choice.** The rule: the highest discovery power at the prior VRP 0.10, averaged over the base and
fat worlds; constructions within 0.03 of each other (about two Monte Carlo standard errors at 1,000
draws) are tied, and a tie goes to the smaller fat-world worst synthetic week (the brief's "power per
unit of tail"). Averages at 0.10: H8 0.255, **H24 0.23**, D1 0.175, U 0.17. H8 and H24 are tied; H24's
fat-world worst week is −42 % against H8's −58 %, so **the primary is H24, the weekly ATM straddle
delta-hedged every 24 h**. H24 also has the higher power at VRP 0.15 (0.66–0.68 against 0.61–0.66) and
lower costs (4.9 % of premium against 5.9 %), so it is less exposed to a cost-model error. D1 (seven
1-day straddles) pays ~7 % of premium in costs, is weak at 0.10 and strong only at 0.15; it would also
need a mark and settlement window every day, which the pull does not hold. The naked straddle has the
lowest power and a −80 % synthetic worst week.
*Disclosure:* H24 was first picked on an exploratory synthetic run (each construction's floors from a
4,000-week bank), where it led H8 at 0.10 by 0.01; in the final table above H8 leads by 0.025. The rule
and its tie band are stated after seeing both synthetic tables. No real outcome exists for any
construction, so the choice carries no real-data selection.

**The primary, registered floors, every criterion on (n = 0.10 shown; Revision 1's n = 0.40 gives the
same rates within 0.01, see the Reading), 6,000-week banks, 2,000 draws.**
Discovery pass = all five criteria; holdout = existence (q 0.95 for one trial carried forward, 0.975 for
two); joint = both. "Net zero" is the VRP at which the net mean is exactly 0 (≈ gross VRP 0.05: the
costs are ~5 % of premium).

| world | gross VRP | net / premium | mean net / week (notional) | annual at n 0.10 | P(disc T1) | P(disc T2) | P(disc any) | P(hold T1 alone) | P(joint T1) | P(joint any) |
|---|---|---|---|---|---|---|---|---|---|---|
| base | 0.0 | -0.052 | -0.386 % | -2.0 % | 0.00 | 0.00 | 0.00 | 0.00 | 0.000 | 0.000 |
| base | 0.05 | -0.000 | -0.001 % | -0.0 % | 0.01 | 0.01 | 0.01 | 0.04 | 0.001 | 0.001 |
| base | 0.1 | 0.051 | 0.427 % | 2.2 % | 0.19 | 0.19 | 0.21 | 0.27 | 0.043 | 0.058 |
| base | 0.15 | 0.103 | 0.903 % | 4.7 % | 0.60 | 0.60 | 0.63 | 0.62 | 0.348 | 0.385 |
| base | **net 0** | 0 | 0 | 0 | 0.006 | 0.006 | 0.009 | 0.038 | 0.0005 | 0.0010 |
| fat | 0.0 | -0.050 | -0.402 % | -2.1 % | 0.00 | 0.00 | 0.00 | 0.00 | 0.000 | 0.000 |
| fat | 0.05 | 0.002 | 0.015 % | 0.1 % | 0.02 | 0.02 | 0.02 | 0.06 | 0.001 | 0.001 |
| fat | 0.1 | 0.053 | 0.477 % | 2.5 % | 0.23 | 0.20 | 0.24 | 0.32 | 0.056 | 0.062 |
| fat | 0.15 | 0.105 | 0.995 % | 5.2 % | 0.54 | 0.50 | 0.54 | 0.57 | 0.290 | 0.295 |
| fat | **net 0** | 0 | 0 | 0 | 0.015 | 0.014 | 0.018 | 0.051 | 0.0000 | 0.0000 |

**Reading.**
- **Error control at the registered n = 0.40** (same draws): at a true net zero, discovery passes
  (either trial) 0.025 (base) / 0.028 (fat) of the time, the holdout alone confirms 0.039 / 0.052 (its own
  α, nominal 5 %), and the joint path ≤ 0.0015. (At n = 0.10, shown in the table, criterion 5 trims the
  discovery null to 0.009 / 0.018.) The fat world is the calibration world, so its rates sit at the
  nominal level by construction; the base world is below.
- **Power at n = 0.40:** discovery (either trial) 0.23 / 0.25 and joint 0.058 / 0.066 at VRP 0.10;
  0.63 / 0.54 and 0.38 / 0.30 at VRP 0.15 (base / fat).
- **Power is low at the prior.** At gross VRP 0.10 (net ≈ 0.05 of premium) discovery passes ~1 time in
  5 and the joint path ~1 in 17; at 0.15, ~3 in 5 and ~1 in 3. A non-pass means "not demonstrated at
  this power", never "no premium".
- **Criterion 5 barely costs power** (n 0.10 vs 0.25 vs 0.45 differ by ≤ 0.02): when the t clears 2.59
  the sample mean is large. **Size is therefore an economic choice, not a power choice (D9).**
- **Costs decide.** With spread and hedge costs doubled, discovery power at VRP 0.10 falls to 0.03–0.05 (net 0.012–0.014 of premium), and at 0.15 to 0.34–0.35.
- **T2** has the same power as T1 within 0.03 in the synthetic world, which has no conditional VRP:
  whatever T2 gains or loses on real data comes from the stress weeks themselves.

## 4. The trade (fixed now; `ct/shortvol.py`)

1. **Entry.** Every Friday 08:00 UTC of the calendar, per coin (BTC, ETH).
2. **Instrument.** The Friday expiry 7 days out (08:00). Strike = the **traded** strike of that expiry
   in [entry − 4 h, entry) nearest the entry index (ties to the lower), required within 2.5 % of it;
   both legs (call + put) at that strike, one contract each per unit (1 BTC / 1 ETH).
3. **Entry index.** The index price of the latest option trade in the window if ≤ 30 min old; else the
   perpetual's close at entry (counted).
4. **Mark.** Mark IV from the latest trade at the chosen strike (Deribit's `mark_price` × that trade's
   index, inverted with Black-76 on that index and that trade's time to expiry); both legs re-priced at
   the entry index with 7 days to expiry. Deribit marks one IV per strike; the call/put mark difference
   is basis, ignored (a forward equal to the index; a 7-day basis is a few bp).
5. **Fill.** Sell at mark × (1 − h): **h = the scout's contemporaneous trade-vs-mark p75 half-spread for
   3–7-day, 30–45 Δ options: BTC 2.60 %, ETH 2.31 % of premium** (the ATM-most bucket measured; p50 0.39 %
   / 0.83 % is a reported variant). Taker throughout.
6. **Fees.** Option: min(0.0003 × index, 0.125 × price) per contract per leg (Deribit since 2020-08-24).
   Delivery at expiry on the in-the-money leg: min(0.00015 × settlement, 0.125 × its value). Perpetual
   hedge: **0.05 % taker + 1 bp slippage** per unit traded (doubled in a reported block).
7. **Hedge.** At entry and every 24 h after (Saturday … Thursday 08:00), trade the perpetual to
   neutralise the straddle's Black-76 delta (2 N(d1) − 1 per unit), with the leg IV = entry IV +
   (DVOL now − DVOL at entry) (β_IV = 1; floor 5 %), at the perpetual's close at that hour; unwound at
   expiry. Hourly funding on the hedge from Deribit's own rate (a long pays a positive rate; a missing
   hour is 0, counted).
8. **Exit.** Hold to expiry. **Settlement** = the mean of per-minute medians of the trades' index price
   over 07:30–08:00 (Deribit settles on that window's index TWAP), with ≥ 10 covered minutes, else the
   perpetual's close at 08:00 (counted). Payoff |settlement − K| per straddle.
9. **Accounting.** USD, premium converted at the entry index (Deribit options are coin-settled; holding
   the coin premium unhedged is not part of the book). Return per unit notional = net USD / entry index.
   **Weekly book return r_w** = the mean over the coins traded that week (equal USD notional); a coin
   whose straddle or settlement is missing is left out that week (counted); a week with neither coin is
   absent from G (counted).
10. **Margin and liquidation (D6).** Deribit standard margin, one account per currency (BTC- and
    ETH-margined accounts are separate), each with leverage n (book notional / equity, §5). At every
    hourly mark: option value by Black-76 at the perpetual price with the DVOL-shifted leg IV;
    maintenance = 2 × 0.075 × S + the straddle's mark + 1 % of the hedge notional; **equity (1 + n × P&L
    per notional) below n × maintenance per notional → liquidated at that mark**: the straddle is bought
    back at mark × (1 + h) with fees, the hedge closed, the loss crystallised, the coin idle for the rest
    of the week. At n = 0.40 it is not triggered in any of the 21 realistic stress weeks or the 2020-03
    cluster (§5), and a −50 % gap liquidates a 3× book but not the registered one (tested); a reported
    block marks with β_IV = 2.
11. **No stop-loss, no per-week loss cap.** A stop on a short option in a gap fills at the gap and
    crystallises the vega mark-up; the hedge and the size are the risk controls.
12. **Missing hours** of perp or DVOL in a path are filled with the last value (counted; 0 in both
    calendars by §2).

## 5. Sizing and the tail (D9; Revision 1 under `000m`)

**Revision 1 (2026-10-13, orchestrator decision `000m`).** The tail is now **computed**, not bounded: the
registered H24 mechanics (§4, unchanged) were run on real out-of-calendar stress weeks, and the result
replaces the adversarial atom in the Kelly, the tail charge and the size. The adversarial bound is kept
as a reported **stress** and still caps the size. Nothing on or after 2021-03-24 was used for it.

**The weeks, selected on the underlying alone** (`r014_tail.py select`, before any book number): every
Friday-08:00 entry 2019-01-04 → 2021-03-12 (115 weeks; every hold ends before 2021-03-24, DVOL's start
and the discovery calendar's), selected iff the BTC or ETH weekly excursion max(|close move|, −low, high)
from Deribit perpetual hourly candles is ≥ 25 %, plus the two weeks of the adversarial bound (both
qualify anyway). **21 weeks:** 2019-03-29, 05-10, 06-21, 07-12, 09-13, 09-20, 10-25; 2020-01-10, 02-07,
03-06, 07-24, 08-28, 11-20, 12-11; 2021-01-01, 01-15, 01-22, 01-29, 02-05, 02-19, 03-05. (ETH has no
perpetual before 2019-03-14; those weeks were selected on BTC alone.) The 2020-03 cluster (entries
2020-02-14 → 2020-04-03) was also computed, for the 4-week path only; its other weeks are not atoms.

**Two substitutions before DVOL exists (stated):** the hedge-delta and liquidation-mark IV proxy is the
hourly ATM mark IV of the position's own expiry from the tape (latest trade in the preceding hour at the
traded strike nearest the perp price; carried forward when an hour has none), in place of DVOL; and
where no traded strike of the 7-day expiry lay within 2.5 % of the index at entry, the straddle is struck
at the index and marked with the IV of the nearest traded strike of that expiry (8 coin-weeks: ETH
2019-05-10, 2019-09-13, 2020-01-10, 2020-03-13, 2020-04-03, 2021-01-29; BTC 2020-03-20, 2021-01-22).
Thin 2019 ETH tapes settled on the perp close (fewer than 10 covered minutes) in 8 coin-weeks.

**Realistic hedged book return per week** (per unit notional; equity at each n = n × book; **no
liquidation at n = 0.10, 0.25 or 0.45 in any week**, so none at 0.40 either):

| entry | role | BTC / ETH % | **book %** | equity at n 0.10 | 0.25 | **0.40** | 0.45 | liquidation | construction |
|---|---|---|---|---|---|---|---|---|---|
| 2019-03-29 | selected | -5.2 / -2.5 | **-3.9** | -0.4 | -1.0 | **-1.5** | -1.7 | none | registered |
| 2019-05-10 | selected | -8.9 / -13.0 | **-10.9** | -1.1 | -2.7 | **-4.4** | -4.9 | none | ETH fallback |
| 2019-06-21 | selected | -4.5 / +1.1 | **-1.7** | -0.2 | -0.4 | **-0.7** | -0.8 | none | registered |
| 2019-07-12 | selected | -2.3 / -4.9 | **-3.6** | -0.4 | -0.9 | **-1.4** | -1.6 | none | registered |
| 2019-09-13 | selected | +4.5 / -1.7 | **+1.4** | +0.1 | +0.4 | **+0.6** | +0.6 | none | ETH fallback |
| 2019-09-20 | selected | -2.6 / -2.2 | **-2.4** | -0.2 | -0.6 | **-1.0** | -1.1 | none | registered |
| 2019-10-25 | selected | -21.6 / -6.6 | **-14.1** | -1.4 | -3.5 | **-5.6** | -6.3 | none | registered |
| 2020-01-10 | selected | -1.3 / -2.2 | **-1.8** | -0.2 | -0.4 | **-0.7** | -0.8 | none | ETH fallback |
| 2020-02-07 | selected | -3.1 / -9.2 | **-6.1** | -0.6 | -1.5 | **-2.5** | -2.8 | none | registered |
| 2020-02-14 | 2020-03 cluster (path only) | -1.7 / +3.2 | **+0.7** | +0.1 | +0.2 | **+0.3** | +0.3 | none | registered |
| 2020-02-21 | 2020-03 cluster (path only) | -0.4 / -0.1 | **-0.3** | -0.0 | -0.1 | **-0.1** | -0.1 | none | registered |
| 2020-02-28 | 2020-03 cluster (path only) | +3.0 / +4.3 | **+3.7** | +0.4 | +0.9 | **+1.5** | +1.6 | none | registered |
| 2020-03-06 | selected | -4.6 / -5.9 | **-5.2** | -0.5 | -1.3 | **-2.1** | -2.4 | none | registered |
| 2020-03-13 | 2020-03 cluster (path only) | -9.3 / -9.2 | **-9.3** | -0.9 | -2.3 | **-3.7** | -4.2 | none | ETH fallback |
| 2020-03-20 | 2020-03 cluster (path only) | +5.0 / +10.0 | **+7.5** | +0.7 | +1.9 | **+3.0** | +3.4 | none | BTC fallback |
| 2020-03-27 | 2020-03 cluster (path only) | +2.7 / +6.8 | **+4.7** | +0.5 | +1.2 | **+1.9** | +2.1 | none | registered |
| 2020-04-03 | 2020-03 cluster (path only) | +2.9 / -4.1 | **-0.6** | -0.1 | -0.2 | **-0.2** | -0.3 | none | ETH fallback |
| 2020-07-24 | selected | -3.5 / -4.9 | **-4.2** | -0.4 | -1.1 | **-1.7** | -1.9 | none | registered |
| 2020-08-28 | selected | -6.8 / -0.7 | **-3.7** | -0.4 | -0.9 | **-1.5** | -1.7 | none | registered |
| 2020-11-20 | selected | -1.9 / -5.2 | **-3.6** | -0.4 | -0.9 | **-1.4** | -1.6 | none | registered |
| 2020-12-11 | selected | -1.1 / +0.2 | **-0.5** | -0.0 | -0.1 | **-0.2** | -0.2 | none | registered |
| 2021-01-01 | selected | -10.0 / -14.7 | **-12.3** | -1.2 | -3.1 | **-4.9** | -5.6 | none | registered |
| 2021-01-15 | selected | +7.3 / +0.1 | **+3.7** | +0.4 | +0.9 | **+1.5** | +1.7 | none | registered |
| 2021-01-22 | selected | +6.6 / +4.7 | **+5.7** | +0.6 | +1.4 | **+2.3** | +2.5 | none | BTC fallback |
| 2021-01-29 | selected | +2.4 / +2.4 | **+2.4** | +0.2 | +0.6 | **+1.0** | +1.1 | none | ETH fallback |
| 2021-02-05 | selected | -8.9 / +8.1 | **-0.4** | -0.0 | -0.1 | **-0.2** | -0.2 | none | registered |
| 2021-02-19 | selected | -5.0 / -5.6 | **-5.3** | -0.5 | -1.3 | **-2.1** | -2.4 | none | registered |
| 2021-03-05 | selected | +0.4 / -1.1 | **-0.4** | -0.0 | -0.1 | **-0.2** | -0.2 | none | registered |

- **Worst realistic week: −14.1 % of notional** (2019-10-25: BTC jumped ~40 % within the first day after
  entry, the adversarial shape). 2021-01-01: −12.4 %. **2020-03-06: −5.3 %** — the crash came on day 6,
  when the put was already deep in the money and almost fully hedged; the week after (2020-03-13,
  struck at ~130 % IV) lost −9.3 %.
- **Worst 4-week path, 2020-03 cluster:** entries 2020-02-21 → 03-13, −11.1 % of notional summed
  (−1.1 / −2.8 / −4.5 / −5.0 % of equity at n 0.10 / 0.25 / 0.40 / 0.45, compounded). The worst 4-week
  window over all computed weeks, with uncomputed weeks counted at 0, is the single week 2019-10-25:
  **−14.1 %**; that is the registered realistic worst month.
- For reference only (reported, computed on the same weeks): the **unhedged** straddle lost −36 % /
  −40 % (BTC / ETH) in the 2020-03-06 week and −25 % / −52 % in 2021-01-01. The daily hedge is what turns
  the bound into the realistic number.
- **Reported stress, kept:** the adversarial single-gap bound (worst week −53.8 %, month −67.1 % of
  notional; the table of Revision 0 is in §5a below).

**The tail, priced (realistic).** The 21 out-of-calendar stress weeks sum to −67.0 % of notional; at
their historical frequency (each 1 in 390 weeks, 2019-01 → 2026-06) they cost **0.172 % of notional per
week** (≈ 8.9 %/yr of notional) — the **tail charge** of the deploy bar (§6). The adversarial charge
(0.264 %/wk) is reported beside it. The tails inside the calendars are in the samples' own means.

**Kelly** (`shortvol_sim.sizing_realistic`; synthetic net weekly book mixed with the 21 realistic weeks
as atoms, each at 1/390 a week; 3,000-week banks):

| | base world | fat world |
|---|---|---|
| tail-aware Kelly, VRP 0.05 / 0.10 / 0.15 | 0.01 / 1.78 / 2.99 | 0.01 / **1.65** / 2.38 |
| ¼ tail-aware Kelly, VRP 0.05 / 0.10 / 0.15 | 0.00 / 0.45 / 0.75 | 0.00 / **0.41** / 0.60 |
| *Revision 0, adversarial atom (reported):* tail-aware Kelly VRP 0.10 / 0.15 | 0.55 / 1.25 | 0.40 / 0.85 |
| tail-budget cap from the **adversarial stress**: worst week ≤ 25 % / month ≤ 35 % of equity | 0.46 / 0.52 | 0.46 / 0.52 |

(At VRP 0.15 the fat-world Kelly stops at 2.38 because the synthetic worst week, −42 %, would ruin a
larger book; at VRP 0.05 the book has no edge and Kelly is ~0.)

**Proposed size (D9): n = 0.40** — ¼ of the fat-world tail-aware Kelly at the prior VRP 0.10 (1.65 / 4 =
0.41), rounded down to 0.05. I keep the rule and add one condition: n must also keep the **adversarial
stress** inside the tail budget, which caps it at 0.46. At 0.40 both hold; the realistic tail rests on
21 single paths with thin 2019 ETH tapes, and the adversarial cap is the guard against that sample being
kind. At VRP 0.05 Kelly is ~0, which is the honest message: if the premium is only the size of the
costs, no size is right — that is what the discovery test decides.

**At n = 0.40:**
- **worst plausible week −5.6 %, month −5.6 % of equity (realistic)**; adversarial stress −21.5 % /
  −26.8 %;
- **expected %/yr** (synthetic mean net weekly book × 52 × n; base / fat): **≈ 0 % at VRP 0.05; +8.9 /
  +9.9 % at 0.10; +18.8 / +20.7 % at 0.15** — after the realistic tail charge (−3.6 %/yr): **+5.3 / +6.3 %
  at 0.10, +15.2 / +17.1 % at 0.15**;
- criterion 5 (≥ 3 %/yr at n) needs a mean ≥ 0.144 % of notional a week; the deploy bar needs ≥ 0.316 %
  (≈ gross VRP 0.09).

**Against the mandate.** At the prior the book is HLP-plus (5–6 %/yr after the tail charge); at VRP
0.15, 15–17 %/yr. **The 30–100 %/yr landing zone stays out of reach at any size the tail and Kelly
allow** (n = 1.65, full Kelly at the prior, would give ~37–41 %/yr gross with the adversarial stress at
−89 % of equity).

### 5a. Revision 0 (adversarial bound; kept as the reported stress)

*Superseded by Revision 1 for the Kelly, the tail charge and the size; kept verbatim as the reported stress.*


**Tail, from the unconditional underlying history only** (the scout's printed weekly excursions; no new
real-data computation). For a delta-hedged straddle the worst case is the whole excursion arriving as
**one gap from the strike between two hedges**: loss = max(|week low|, week high) − premium, with the
scout's stylised premium (7 % of spot BTC, 9 % ETH). This is an **upper bound** for the hedged book (a
loss spread over several hedge intervals is partly hedged) and equals or exceeds the unhedged close-based
loss. Book = ½ BTC + ½ ETH.

| event (week from Friday 08:00) | BTC excursion | ETH excursion | book loss per notional | **at n = 0.10** | at n = 0.25 | at n = 0.45 |
|---|---|---|---|---|---|---|
| 2020-03-06 (COVID) | low −60.4 % | low −63.2 % | −53.8 % | **−5.4 %** | −13.5 % | −24.2 % |
| 2021-01-01 (rally) | high +39.0 % | high +75.3 % | −49.2 % | **−4.9 %** | −12.3 % | −22.1 % |
| 2021-05-14 | low −43.1 % | low −53.5 % | −40.3 % | −4.0 % | −10.1 % | −18.1 % |
| 2022-06-10 (3AC) | low −33.4 % | low −43.5 % | −30.5 % | −3.0 % | −7.6 % | −13.7 % |
| 2022-11-04 (FTX) | low −25.3 % | low −33.0 % | −21.2 % | −2.1 % | −5.3 % | −9.5 % |
| 2025-10-10 | low −15.9 % | low −21.9 % | −10.9 % | −1.1 % | −2.7 % | −4.9 % |
| 2026-01-30 | low −27.6 % | low −36.6 % | −24.1 % | −2.4 % | −6.0 % | −10.8 % |
| **worst 4 weeks** (2020-12-11 →, scout's stylised close-based) | −61.1 % | −73.1 % | −67.1 % | **−6.7 %** | −16.8 % | −30.2 % |

Synthetic cross-check: the worst week of the 24 h-hedged book over 3,000 synthetic weeks is −33 %
(base) / −42 % (fat) of notional: the bound is not loose by much.

**The tail, priced.** 2020-03 and 2021-01 lie in neither test calendar (before DVOL). At their
historical frequency (2 in 390 weeks, 2019-01 → 2026-06) their adversarial losses cost **0.264 % of
notional per week** — about 13.7 %/yr of notional, roughly 60 % of the expected net premium at VRP 0.10.
This **tail charge** is subtracted in the deploy bar (§6). The tails inside the calendars (2021-05,
2022-06, 2022-11; 2025-10-10, 2026-01-30) are in the samples' own means.

**Kelly.**
| | base world | fat world |
|---|---|---|
| Gaussian Kelly μ/σ² on the synthetic net weekly book, VRP 0.10 (VRP 0.15) | 4.7 (9.8) | 2.5 (5.4) |
| **tail-aware Kelly**: argmax E log(1 + f r) with the adversarial out-of-calendar atom (−51.5 % of notional, probability 2/390 a week), VRP 0.10 (0.15) | **0.55** (1.25) | **0.40** (0.85) |
| ¼ tail-aware Kelly, VRP 0.10 (0.15) | 0.14 (0.31) | **0.10** (0.21) |
| tail-budget cap: worst week ≤ 25 % / worst month ≤ 35 % of equity | 0.46 / 0.52 | 0.46 / 0.52 |

*Revision 0 proposed n = 0.10* — one quarter of the tail-aware Kelly fraction in the fat world at
the prior VRP 0.10 (0.40 / 4), rounded down to 0.05. The Gaussian ¼ Kelly from the variance estimate
alone (0.6–1.2) is not used: it ignores the tail, and the tail budget (worst week ≤ 25 % of equity,
worst month ≤ 35 %) would cap it at 0.46 anyway.

*Revision 0 expected return at n = 0.10* (synthetic mean net weekly book return × 52 × n): ≈ **0 %/yr at VRP
0.05, +2.2–2.5 %/yr at 0.10, +4.7–5.2 %/yr at 0.15**, before the tail charge; worst plausible week
−5.4 %, month −6.7 % of equity. At the tail-budget cap n = 0.45: +10–11 %/yr at 0.10, +21–23 %/yr at
0.15, worst week −24 %, month −30 % — over the tail-aware full Kelly at 0.10, which the mandate's
"a fraction of Kelly" rules out. **The mandate's 30–100 %/yr landing zone is out of reach for this book
at any size the tail allows.** What 014 can deliver is a measured, priced, HLP-plus premium — or the
knowledge that the premium net of Deribit's costs is too small.

## 6. Criteria

**Discovery (per trial, each at one-sided 2.5 %; `shortvol.evaluate_discovery`), on the weekly book
returns r_w per unit notional. PASS = all five:**
1. mean r_w > 0 and **skew-corrected t ≥ max(t_{G−1}(0.975), 2.59)** (weeks are the clusters: one
   observation per week, BTC and ETH pooled inside it);
2. median r_w > 0;
3. mean > 0 in both halves (first and second half of the weeks, by time);
4. mean > 0 without the largest positive week;
5. annualised net on equity at n = 0.40 (mean × 52 × n) ≥ 3 %/yr (HLP's lending leg): a mean of at
   least 0.144 % of notional a week.

- *Why the median is required but uninformative, and the mean is the test.* A short-vol book collects
  the premium most weeks and gives it back in a few: its median is positive even when its mean is not.
  Criterion 2 only rejects a book that loses in a typical week; criterion 1 is the test.
- *Reported, never required:* the mean **without the worst week** (it flatters a short-vol book, so it
  is shown, not used), the plain t, the skew, the worst week, the worst 4 weeks, the hit rate, the max
  drawdown at n against the pre-registered worst month at n (§5), the economics of the deploy bar (below,
  computed on discovery for information), and the placebo.
- *Placebo:* 200 seeds; each week's **gross** return flipped in sign at random, its costs kept; the
  share of seeds passing criterion 1 (reported).
- **Decision:** any trial passing → `read_holdout` with the passing trials; none → `close`. The row
  stores every number; the holdout gate recomputes the decision from them (`recheck_discovery`).

**Holdout (read once, only through the gate; `shortvol.holdout_existence`): an EXISTENCE test** for
each trial carried forward: mean r_w > 0 and skew-corrected t ≥ max(t_{G−1}(q), floor), q = 0.95 with
one trial (floor 2.16), 0.975 with two (floor 2.52).

**Economic bar, applied only after an existence confirm, for deploy (`economic_after`):**
(mean − 0.172 % realistic tail charge) × 52 × n ≥ 3 %/yr (a mean of at least 0.316 % of notional a week
at n = 0.40), **and** the holdout's max drawdown at n no worse than the pre-registered **adversarial**
month at n (−26.8 %): a breach of the bound itself. Reported beside them: the adversarial tail charge
(0.264 %/wk) and the drawdown against the realistic month at n (−5.6 %).

**Decisions:** no confirm → `not_demonstrated` (014 closes; never "no premium"); confirm without the
economics → `exists_not_economic` (014 closes with a documented premium too small for its tail);
confirm with the economics → `deploy_paper_registration`: a forward paper period on live data, the book
fixed by this registration (T1 or T2 as confirmed; if both, T2 if its tail-charged annualised net is at
least T1's, else T1 — fixed now), its own registration and its own bars.

## 7. Reported, decides nothing (both runs)

p50 half-spreads; hedge costs × 2; unhedged; 8 h hedge; no funding; β_IV = 2 liquidation marks; no
liquidation check; by coin; mean return on premium; mean components per notional (premium, spread,
fees, payoff, delivery, hedge P&L, funding, hedge fees); placebo for T1 and T2; the sizing statement;
the deploy economics computed on discovery; the drawdown comparison. Counts printed **before** the
decision: missing straddles by reason, index and settlement sources, filled hours, liquidations, weeks,
stressed weeks, undefined stress flags. **No reported block may become a primary;** another tenor,
moneyness, hedge schedule, size, filter or cost level is a new trial in the Deribit family, on data after
this registration's calendars.

## 8. Known weaknesses, stated now

- **Low power at the prior** (§3): ~0.2 at VRP 0.10. 014 is decisive only if the net premium is large.
- **Exposure of the holdout.** Its first part (009's holdout A) is fresh to our code but covered by a
  published, positive short-vol backtest to 2024-08; its second part (the HL year) had its DVOL levels,
  ATM IV at snapshot hours, three full trade days and its underlying moves read unconditionally by the
  scout (`000k`). The test's information — premium against what followed — was never computed.
- **The synthetic tail is thinner than the real one** (put SD/mean 3.5–4.0 vs 4.9 in block medians). The floors are
  calibrated in the fatter world; a real tail fatter still means the realised α can exceed nominal.
- **The realistic tail is 21 single paths** (Revision 1): one realised path per stress week, timing
  matters (2020-03's crash came on day 6 and was mostly hedged; 2019-10-25's jump came on day 1 and
  was not), 2019 ETH tapes are thin (6 strike fallbacks, 8 perp-close settlements), and the IV proxy is
  the tape's hourly ATM IV, not DVOL. The adversarial bound (−53.8 % week) stays as the stress and caps
  n at 0.46.
- **Marks.** One IV per strike from the latest trade in a 4-hour window (p90 age 14–22 min, max 2.7 h);
  stale marks add noise, not bias, unless IV trends inside the window.
- **Hedge IV** is the entry IV shifted by DVOL's change, not the option's own IV; delta errors are
  second-order for the hedge P&L. The liquidation mark under-reacts to short-dated IV spikes; the
  β_IV = 2 block shows the sensitivity.
- **Settlement** approximates Deribit's TWAP from trade-time index prints (≥ 10 of 30 minutes; all 544
  settlements qualify).
- **Fees.** The perpetual taker rate is taken at 0.05 % throughout; if a higher historical rate applied
  in 2021, the hedge-cost × 2 block bounds it. The 2026-08-01 fee schedule is outside the calendars.
- **Funding** missing in 34 / 30 hours is 0.
- **Capacity** is far above our size (scout: a few $M of 7-day ATM notional a week).
- **Venue (warning only, never a blocker).** Deribit (Coinbase-owned since 2025-08-14) serves retail
  through its Panama entity, is not MiCA-authorised, and access for an EU-resident retail user is a grey
  zone; no source confirms it lawful for a German resident. A venue geoblock at onboarding is a practical
  fact to check before any paper period.

## 9. Built before any outcome is computed

- [x] `ct/shortvol.py` — pure: Black-76, IV, delta, Student-t quantiles, calendars, `straddle_entry`,
  `entry_coverage` (the build's twin, no marks), `settlement`, `settlement_coverage`, `position_pnl`
  (hedge, funding, fees, delivery, liquidation), `stress_flag`, `weekly_book`, `stand_aside`, `skew_t`,
  `summary`, `discovery_criteria`, `evaluate_discovery`, `recheck_discovery`, `holdout_existence`,
  `economic_after`, `decide_holdout`, `placebo`, `sizing_statement`. **Gates live only in
  `discovery_criteria` / `evaluate_discovery` / `holdout_existence` / `decide_holdout`**; the outcome
  functions carry no pass/fail logic and serve the primary and every reported block alike.
- [x] `ct/shortvol_sim.py` — the synthetic world, calibration diagnostics, constructions, floors, power,
  Kelly. Not on the decision path (it imports nothing the run uses); hashed for provenance.
- [x] `r014_fetch.py` — the pull (module docstring).
- [x] `r014_build.py` — verify, split, MANIFEST, coverage counts only.
- [x] `r014_tail.py` (Revision 1, `000m`) — the out-of-calendar realistic tail: end-bounded pulls before
  2021-03-24, selection on the underlying alone, the registered mechanics on the selected weeks. Its
  frozen output is `ct/shortvol.REALISTIC_WEEK`; hashed for provenance.
- [x] `r014_run.py` — the guard, `Panel` (whitelist and read lock), `positions`, `--discovery`
  (`discovery_started` row first, exactly one result row), `--holdout` ((g1)–(g4), (h'), (i); the
  decision recomputed from the committed row; one started row, one result row), `--power` (synthetic).
- [x] `tests/test_r014.py` — **126 checks pass** (`.venv/bin/python -m tests.test_r014`):
  - maths (parity, IV round trip, t quantiles); calendars (143 / 129 entries, the embargo week);
  - entry (nearest traded strike, window bounds, stale index → perp, missing reasons, coverage twin
    agrees); settlement (per-minute TWAP, < 10 minutes → perp, the trade at 08:00 excluded);
  - position (flat path, rally, hedge, delivery cap, funding sign, liquidation at 3× not at n, path
    validation, β_IV); stress (point in time: a spike after entry − 1 h changes nothing; undefined);
  - statistics (skew t below plain t on skewed data and monotone; criteria; criterion 5; recheck catches
    forged labels and a wrong n; holdout q by k and floors; tail charge; decisions; placebo);
  - synthetic smoke; **fetch** (end bound asserted before any request; typed errors after retries,
    nothing written; forward paging with inclusive restart; > 1,000 trades on one ms; a row at the
    cutoff; a DVOL continuation; short tail coverage; an empty funding window; new/resume/complete
    directories);
  - **build and read lock** (a tampered pull refused; counts-only coverage keys; built once; byte-
    identical rebuild; the holdout panel refused without the flag; a foreign path refused; a discovery run
    refused on a holdout directory; a MANIFEST mismatch refused);
  - **guard, throwaway repo** (DRAFT; (b); (d)+(e); a panel change; (f) on a started row, a deleted row,
    a side-branch row; the holdout gate: (g2), (h') missing or wrong sha, opens when right, (g3) prose
    added or code changed, (i), **(g4) alone on a forged pass over failing numbers**);
  - **end to end** (synthetic pull → build → the real `run_discovery` writes `discovery_started` then one
    row, counts before the decision, recomputable; a second run refused; the holdout refused before
    approval; after it, `run_holdout` writes one started and one result row with k = 2; a second refused);
  - **000m tail script** (a request ending on/after 2021-03-24 refused before any call; a row past it
    refused; entries 2019-01-04 → 2021-03-12; excursion from candles only; `select` touches no option data);
  - **this file, status-aware** (DRAFT or REGISTERED; while DRAFT the guard refuses and no line starts
    with an approval phrase; §9 hashes equal the disk; 014's code names no other data directory and
    imports no other registration's module). It does not go stale at REGISTERED.

**Pre-specified procedure.** After both reviews and REGISTERED (committed):

    .venv/bin/python -I r014_run.py --discovery   # guard; discovery_started; one result row

Commit both rows. On `read_holdout` (recomputed from the row), the approver commits, in a later commit,
one line at the start of a line: "Holdout: APPROVED", a date, "row" and the sha256 of the discovery row's
line. Then:

    .venv/bin/python -I r014_run.py --holdout     # gate; holdout_started; one result row

**SHA-256 of the code and data the outcome depends on** (read by the guard; recomputed by the
orchestrator at commit):

| file | sha256 |
|---|---|
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/shortvol.py` | `ce119f7c17cc01786091732c325fe3cd1a457ae67db8eec028367dbf9a7a9329` |
| `ct/shortvol_sim.py` | `1435acbfbfe7382e30a123ae09b802681dfa1797a882a818f30b61eb7b4cd48b` |
| `r014_run.py` | `c1025c84ddf811dccdb40bc30b450e0aa8859a14d633a0932e6bacc2dac150e8` |
| `r014_fetch.py` | `7c1987e1ae785289178865d7590fa3d135f27ce35fb03b5fd793c60617b8182e` |
| `r014_tail.py` | `7ca3baaed14137c7e395d7e43cf68e7193554c400ea0de8108ba330a262e802c` |
| `r014_build.py` | `4aac7529cd61fe94c6212d0d38c46616ac3b8d5240b43e28699fdad69c918bca` |
| `data/r014/panel_discovery/dvol.json` | `8c8b6b49986587270a1a0d18ae090990262b77bfddaa89071eef491c8ffb127e` |
| `data/r014/panel_discovery/perp.json` | `df4f0b27e38c88c6a88087cefe02c00ebf30a7fcca486d6cb48b580f8d942904` |
| `data/r014/panel_discovery/funding.json` | `65a85ba428a7426493843f6d153def0f1550d3e2e22d7a8a77f98ee15ff29bfc` |
| `data/r014/panel_discovery/trades.jsonl.gz` | `0cba2ab3ea7cebccda5680e9ab65ba0f02307ea2408829ba10b068b57840994a` |
| `data/r014/panel_discovery/MANIFEST.json` | `df9108b4abc3efd791877ed6d786821724607913560df1d60502edb6a4f90c1d` |
| `data/r014/panel_holdout/dvol.json` | `bc338516cd59f32d9d0c3b2131266b731e1a50c1f97ce2510dc39cbe4115ede4` |
| `data/r014/panel_holdout/perp.json` | `d7b03657952b9f4b9c046c8e3d698929bb9d0d3a28ec87ea6c5bf02c3b5bbec9` |
| `data/r014/panel_holdout/funding.json` | `b37a5b7c6875f42a429c46de7b5bf3cca62b2cad0d0fe1c3d7e15a5868321005` |
| `data/r014/panel_holdout/trades.jsonl.gz` | `ba7fcd3c2d9036700f11f56acd9afa550712ad79be3a4092eb99c0beb941f861` |
| `data/r014/panel_holdout/MANIFEST.json` | `1c5bf453116fe7516b882964c07496f2b91daf9e3f51db49c2e0c8526a238356` |

## 10. Owner decisions — PROPOSED

| # | item | proposed (recommendation) |
|---|---|---|
| D1 | venue, instruments, data | Deribit BTC/ETH options and perpetuals; the design-sized keyless pull of §2 (done; 0.25 GB) |
| D2 | calendars | discovery entries 2021-04-02 → 2023-12-22; embargo entry 2023-12-29; decision holdout entries 2024-01-05 → 2026-06-19; boundary rule §1; 2026-07-01 → never read |
| D3 | trials and family | T1, T2; Deribit-options family = 2; Bonferroni; no other family touched |
| D4 | construction | weekly 7-day ATM straddle, delta-hedged every 24 h on the perpetual, held to expiry, BTC + ETH equal notional (§3's rule) |
| D5 | stress rule (T2) | DVOL ≥ 1.25 × its trailing 7-day minimum (bars closing ≤ entry − 1 h), either coin → the whole book idles that week; < 120 bars → undefined, trade (counted) |
| D6 | fills, costs, margin | §4: p75 half-spreads 2.60 % / 2.31 %; option fee capped 12.5 %; delivery fee; perp 0.05 % + 1 bp; Deribit funding; standard margin per currency, hourly liquidation check |
| D7 | inference | Hall's skew-corrected t; floors 2.59 (discovery), 2.16 / 2.52 (holdout), calibrated in the fat synthetic world; weeks as clusters |
| D8 | discovery criteria | the five of §6; the median required for sanity, the mean is the test; without-the-worst-week reported |
| D9 | **size (owner)** | **n = 0.40** (Revision 1: ¼ of the fat-world tail-aware Kelly with the realistic tail, 1.65 / 4, rounded down; inside the adversarial stress's cap of 0.46). Alternatives: 0.25 (more margin against the realistic tail being a kind sample; ~5.6–6.2 %/yr at VRP 0.10 before the charge); 0.10 (Revision 0's adversarial-atom size; ~2 %/yr). Not offered: above 0.46 (the adversarial stress would exceed 25 % of equity in a week). Power is the same at every n (§3); n moves criterion 5, the deploy bar and the tail at size |
| D10 | holdout and deploy | existence test at q 0.95 / 0.975 by trials carried; economics (tail-charged, drawdown) only after a confirm; decisions of §6; a deploy goes to a forward paper registration |
| D11 | exposure | stated in §0–§1 and `000k`; this draft's reads in §12 for a ledger entry |

## 11. Open questions for the orchestrator and the owner

1. **D9, the size.** It barely changes power but sets whether a confirmed premium clears HLP: at n =
   0.40 the deploy bar needs a mean of ~0.32 % of notional a week (≈ gross VRP 0.09); at 0.25, ~0.40 %;
   at 0.10, ~0.75 %.
2. **The realistic tail (closed by Revision 1, `000m`).** Computed on 21 out-of-calendar stress weeks
   (§5). Residual doubts: one path per event, thin 2019 ETH tapes, an IV proxy from the tape instead of
   DVOL, and uncomputed non-stress weeks counted at 0 in the worst-month window. The adversarial bound
   is kept as the stress and as the size cap.
3. **Power.** 0.2 at the prior is low. The only designs with materially more power in §3 are D1 (daily
   straddles) at VRP ≥ 0.15 and nothing at 0.10. Pooling more entries per week (a mid-week 3-day
   straddle) would add correlated observations; not drafted.
4. **The holdout's second half is partly exposed** (§0); a stricter design would decide on 2024-01-05 →
   2025-07-25 only (≈ 82 weeks, lower power). Proposed: keep the brief's calendar and state the exposure.

## 12. Disclosures and real-data reads (for the ledger)

All requests end-bounded before 2026-07-01; **0 rows dated on or after 2026-07-01 received**. No
premium-versus-realised comparison, mark-to-market, payoff, P&L, hit rate, IV change after entry,
realised vol after entry or stress-versus-outcome statistic was computed on either test calendar
(2021-03-24 → 2026-06-30).

**Revision 1 (`000m`), authorised, all before 2021-03-24 (`r014_tail.py`; every request end-bounded below
2021-03-24 00:00 and asserted before it is sent; every row checked below it):**
- **Underlying pull** `data/r014/tail_pre2021/underlying_20261013/`: BTC/ETH perpetual 1h candles
  2019-01-01 (ETH 2019-03-14 13:00) → 2021-03-23 23:00, funding 2019-04-30 10:00 → 2021-03-23 23:00 (3
  empty funding windows before 2019-04-30, counted, not written); 112 requests, 7.3 MB; 5 files in
  SHA256SUMS (`b082f793…98e17`).
- **Selection** from it: weekly excursions of 115 entries (printed for the 21 selected only).
- **Options pull** `data/r014/tail_pre2021/options_20261013/`: every BTC/ETH option trade in
  [E − 24 h, E + 7 d) for the 28 computed weeks (21 selected + 7 more of the 2020-03 cluster); 1,699
  requests, 382.6 MB transferred, 333 MB on disk, 57 files in SHA256SUMS (`fd1fe717…bcd68f7`).
- **Computed and printed** on those 28 weeks only: entry IV and premium, settlement source, IV-proxy
  carry counts, the H24 book's net return per coin and per book at n = 0.10 / 0.25 / 0.45, liquidation
  flags, the 4-week paths, and (reported) the unhedged straddle's return. Results:
  `data/r014/tail_pre2021/tail_results.json`.

- **Probes (counts and field names only):** BTC option trades 2022-06-10 06:00–08:00 (791 trades, 1
  request, the field list); ETH 2021-04-09 06:00–08:00 (221 trades); BTC-PERPETUAL funding history
  2021-04-01 → 04-04 (71 rows, keys) and 2021-03-01 → 03-31 (719 rows, first/last stamps).
- **The pull** (§2), including **009's holdout A (2024-01-01 → 2025-07-27) for the first time**: DVOL,
  perpetual candles, funding and option-trade windows. Printed: per-series row counts and first/last
  stamps; progress counts (Fridays, requests, MB).
- **The build's coverage** (§2 table), over both calendars: availability classes, index and settlement
  sources, covered minutes as a class, chosen-strike trade ages (time only), daily-expiry presence, missing
  hours, row counts. To classify availability the build read index prices and perpetual closes at entry
  to choose a strike; no price, IV or mark was printed or stored.
- **Synthetic only:** every power, calibration, Kelly and test number.
- **No other data directory** was opened by 014's code (static test). The scout's `data/scout_n2` was
  not read by 014's code; its scripts were read as text.
- **Not done:** `r014_run.py` was not run in `--discovery` or `--holdout` mode; nothing was committed;
  nothing was published.
