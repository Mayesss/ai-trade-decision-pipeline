# 015 — The month-end liquidity cycle in US equities, with windows shifted by the settlement lag (B1 #1a)

**Status: DRAFT — Revision 1, 2026-10-13** (drafter; after reviews `reviews/015-review-a.md` and
`reviews/015-review-b.md` and the orchestrator's decisions on them; for a second-round recheck; not registered).
Programme: round 4, `docs/alpha-discovery-2026-10-13.md` (B1). Scout: `research/alpha/scouts/b1-equities-flows.md`
§3.1. Ledger entries read: `000s` (B1 exposure); `000p`–`000r` for context. Templates: `research/copytrade/r008_run.py`,
`r013_run.py`, their registrations and reviews (008-a/b/r1, 013-a/b/r1, 014-b).

## Revision 1 (2026-10-13): every finding and its fix

| finding | fix (section) |
|---|---|
| **A-M1** a pass means "existed 2014-01 → 2026-05", not "alive now"; the 19-month sign check could not tell | §0 and §4 say what a pass means; **after a US pass: paper only**. Real money needs (i) the pre-stated **Europe confirmation** and (ii) a forward registration **015F** of ≥ 31 months (2026-06 → 2028-12) with forward **t ≥ 1** (§8). Reported recent-window (2021-01 →) and per-era blocks (§7) |
| **A-M2** vol-scale y | **The primary is z_m = y_m / (√12·σ̂_m)**, σ̂_m = RMS of x over the 20 trading days ending T−9, floor 0.4 %/day (§3.4). Recalibrated on the same 13 worlds at 10,000 draws with the Monte Carlo SE; registered value = q95 + 2 SE, rounded up (§5). Raw y and v kept for the economics and the bar; the draft's raw-y rule is reported |
| **A-M3** D8 rationale | no double-alignment margin; the right null integrates over placement; the deterministic alignments are reported, with a placement scan (§5, D8) |
| **A-M4** reported blocks would spend fresh samples | **Europe removed from the read**; a Europe confirmation registration is pre-stated (§8); the Europe month-end calendar stays unread. **HMM dropped entirely**: `r015_hmm.py` deleted, the Treasury copies removed from the guard and the table |
| A-m1 overclaim | "the prediction a calendar fluke cannot fake" withdrawn (§0 item 5, §9) |
| A-m2 `xyz` collateral | `xyz` trails MES by **≈ 3.9 %/yr** in total return (USDC collateral earns nothing) (§6) |
| A-m3 the bar's regime | the bar uses the **T+1 regime**: ½·ȳ − 2·μ̂₁ − cost, and the SD of v over the 25 T+1 months (§4.2) |
| A-m4 tail | worst 5 days at 1× = **−22.0 %**; worst month −24.5 % (§6) |
| A-m5 Kelly | Kelly with an adverse −24 % month once in 240 months: 1× = 0.32 (full) / 0.61 (half) of Kelly (§6) |
| A-m6 / B-m8 name | the forward registration is **015F**, not "016-forward" (§8) |
| A-m7 long-only t | reported with the closed-form era-μ correction (§7) |
| A-m8 decomposition | reported: per era, P / T−s … T−1 / T … T+3; without quarter-end months (§7). FOMC and Russell exclusions not added (they need a new file) |
| A-m9 MC SE | stated per world (§5) |
| A-m10 expectation | rewritten with decayed worlds (§0) |
| **B-B1** guard bound to any clone or worktree | **REAL_REPO pinned** (`/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline`); main worktree only (`--git-dir` = `--git-common-dir` = REAL_REPO/.git); detached HEAD refused; a clone elsewhere refused even with the data copied or symlinked (§10). The clone test now uses synthetic data only and must refuse with (w) |
| **B-M1** `run(root)` mixed trees | `run()` takes no argument; guard, data, table and ledger are the real tree's; `_run_in()` (tests) refuses anything under the real repository; the row's code and data hashes must equal the HEAD table (`recheck`) (§10) |
| **B-M2** `-I` not enforced | `sys.flags.isolated` required; every loaded module from the pinned stdlib or r015_core / r015_run here; every cached bytecode from the fresh `pycache_prefix`; interpreter binary SHA-256 and stdlib-tree fingerprint pinned; git by absolute path with no system / global config, no replace objects, no hooks (§10) |
| **B-M3** git's view of the working tree | status and hash table read from **HEAD:./**; `hash-object --no-filters` = HEAD blob for the registration, the code and results.jsonl; skip-worktree / assume-unchanged flags and filter attributes refused; the shadow scan uses the blob comparison (§10) |
| B-m1 forged bytecode | the `__cached__` origin check (§10) |
| B-m2 gate on mutable globals | a byte copy of a registered real data file anywhere is gated; the guard uses `guard_failures` captured at import |
| B-m3 no trailing newline | (k) and `append_row` refuse; (f) refuses an unparseable line that mentions 015 |
| B-m4 showRoot / replace | `log --root`, `-c log.showRoot=true`, `--no-replace-objects` and `GIT_NO_REPLACE_OBJECTS=1` |
| B-m5 vacuous gate tests | the gate test now fails with the gate deleted (a mutation check in the suite) |
| B-m6 transient lock | commits retried 3 × 1 s; `started` rows carry `data_opened: false`, `failed` rows carry stage and `data_opened`; **one narrow restart** (§4.3) |
| B-m7 016 coupling | the broad shadow scan stays; §10 states that 016's draft (every top-level .py and package) must be committed before 015 runs; the refusal names the remedy |
| B-m9 forward overlap | **DATA_END = 2026-06-04** (2026-05's T+3 is 06-03); nothing of 2026-06's out-of-window days is read |
| B-m10 Homebrew pin | pins are the version, the interpreter binary hash and the stdlib-tree fingerprint (not a path); recovery stated (§10) |
| **016-B M2.2 (sibling)** a site-packages `.pth` runs even under `-I` | the run requires **`python -I -S`** (`sys.flags.no_site`), and refuses any `.pth` in site-packages not in a pinned (empty) list; every loaded module's origin is checked (§10) |
| **016-B M2.4 (sibling)** a COMMITTED top-level module shadowing the stdlib | research/alpha is **appended** to `sys.path` (the stdlib resolves first), and the shadow scan refuses any top-level module or package whose name is a stdlib module name, **tracked or not** (§10) |
| **016-B M1 (sibling)** injection when the real repository plays any role | `record()` refuses to write into the real repository unless the real guard passed and the default hook is in place; the gate refuses any path inside the real repository; `_run_in` refuses any role inside it; `run()` takes nothing (§10) |
| **016-B M4 (sibling)** the row must carry the REGISTERED commit | the guard derives **reg0** (the first first-parent commit whose registration says REGISTERED), stores it as `registration_commit`, and refuses (r) if the registration or any guarded code changed between reg0 and HEAD (§10) |
| B-m11 nits | the loaders hash the bytes they parse; `recheck` verifies y = v − (2s−4)μ; (f) also scans `research/copytrade/results.jsonl`; the B1 scout's Japan zip remains in `data/scout_b1/` (stated, §2.1) |

**What was and was not done on real data (draft and Revision 1).** No return, mean, t, P&L or any other statistic of
the French file was computed, printed or saved: not in any window, month, era or in total. The real-data reads were:
hashes; format checks (header lines, row counts, first and last dates, sortedness, rows after the cut-off); the
**trading dates** of the US file in [2013-11-01, 2026-06-04), read from its first field only, to build and pin the
calendar; and the unconditional US daily vol, 1.12 %, already ledgered in `000s` (power only). Revision 1 rebuilt the
calendar for the earlier start (dates only) and deleted the Treasury copies unread by any code path.

---

## 0. Hypothesis, mechanism, sign, and what has been seen

**Hypothesis.** In the US equity market, after Etula et al.'s sample, the month-end liquidity cycle existed: prices
were pressed down over the days on which institutions must sell to have cash on the last trading day T, and rebounded
afterwards, **with the boundary between pressure and rebound moving with the settlement lag in force**. The test is
on 2014-01 → 2026-05 (149 complete months), after the paper's sample and never read by this programme.

**What a pass means (Revision 1, A-M1).** "The cycle, as defined, existed in 2014-01 → 2026-05." It does **not** mean
that it is alive in the T+1 regime we would trade: an effect that died at publication passes ≈ 0.27 of the time
(review A, `p5`; §5). Deployability is decided only by the confirmations of §8.

**Mechanism** (Etula, Rinne, Suominen & Vaittinen, "Dash for Cash", RFS 33(1) 2020, §2.1; the published text was
read, CC BY).
1. Pensions, salaries and fund redemptions are paid on T and T+1. An institution needing cash on the morning of T must
   sell by the close of T−(s+1), where s is the settlement lag: with T+3 that is T−4 ("must sell securities ... before
   the market closes on T−4"; footnote 4: to receive cash by the close of T−1).
2. ANcerno institutions are net sellers from T−8 on, most on T−4 (Figure 6, footnote 19), and net buyers on
   T−1 … T+3; reinvestment adds buying pressure on T … T+3.
3. The paper's windows under T+3: pressure **T−8 … T−4**, rebound **T−3 … T+3**.
4. **Why it can persist:** the sellers are agents under reputational and tracking-error constraints (footnote 9, §5);
   the liquidity-provision risk is systematic, so hard to arbitrage (p. 84).
5. **The settlement shift.** US settlement moved T+3 → T+2 on **2017-09-05** (SEC Rel. 34-80295, compliance date,
   trade-date basis) and T+2 → T+1 on **2024-05-28** (SEC Rel. 34-96930). The last selling day moves T−4 → T−3 → T−2.
   Windows: **P = T−8 … T−(s+1)**, **R = T−s … T+3**; under T+3 exactly Etula's. The shift is the mechanism's
   prediction, **but 015's test cannot check it** (A-m1): an effect fixed on the T+3 windows also passes ≈ 0.6 of the
   time at full effect, and the paired contrast that could separate them has power ≈ 0.2 (§5, §7).

**Sign.** y_m > 0 (pressure-window abnormal returns negative, rebound positive).

**Published magnitudes** (CRSP VW, 1995-07 → 2013-12, T+3; abnormal = each day's return minus the sample's average
daily return): T−8 … T−4 **−37 bp**; T−3 … T−1 +25 bp; T−3 … T+3 **+48 bp** (raw −17 / +77 bp); reversal correlation
−0.39 (Table 2); Friday-T: T−3 … T−1 returns 2.5× larger. **Full effect for 015: 85 bp per month; half: 42.5 bp.**

**What has been seen, by whom, on which calendar (exposure).**

| look | overlap with 2014-01 → 2026-05 | what it computed or knew | exposure to 015 |
|---|---|---|---|
| Etula et al. (2020), main sample | none (ends 2013-12) | the effect, windows, magnitudes | the prior; it fixes every window and the sign |
| **Etula's international settlement test** (Table 3) | **2013-10 → 2015-10, our first 22 months** | T−2 autocorrelation fell in the European treatment group relative to a control group that **includes the US** (DiD −0.78) | aggregate; no window return reported. Stated, not removable |
| Etula footnotes 16–17 | 2017 → | the US move to T+2; the Fed Funds month-end spike not visible "in the most recent samples" | qualitative, money market |
| Harvey, Mazzoleni & Melone (2026-01 draft) | 1997-09 → 2023-03, extended "through 2025" | **conditional** month-end Calendar-rebalancing predictability, last 4–5 days | conditional on the month's drift, not the unconditional cycle. **The HMM block is dropped from 015 (A-M4)**; its post-publication months stay unread for a registration of its own |
| McConnell & Xu (2008); turn-of-month literature | none to partial | the T … T+3 premium to 2005 | public; it sits inside R in every era (§9) |
| B1 scout (`000s`) | the whole calendar | unconditional daily vols (US 1.12 %, regions 0.85–1.17 %, 10y 5.21 bp), the calendar (dates), auction / FOMC dates | the US vol is used for power; nothing conditional |
| N7 desk §A3 | — | the rule sketch | none |
| Reviews A and B (`000`-ledger by the orchestrator) | the whole calendar | dates of `calendar.json` and FOMC days; hashes, counts, formats; synthetic probes; public knowledge of crash dates (not placed against windows) | none on outcomes |
| Public market knowledge | 2008-10, 2020-02/03, 2025-04 | crash sizes | used only as **shapes** in synthetic worlds and for the tail |
| This drafter | the whole calendar | hashes, counts, first/last dates, trading dates; no value | none |

**Family.** New family **"US equity calendar flows": trial 1.** Programme-wide carry-over
(`docs/alpha-discovery-2026-10-13.md` §2), sharing no data with 015: HL discovery year 16 trials, no pass; HL holdout
(008); Binance new-perp listings 2 (013), no pass; Binance 2021–23 1 (009); L2 carry 3, closed; Deribit 0 (014
parked); HL `xyz` 0; 016 (crypto trend) in draft. About 23 registered trials, none passed; 015's own one-sided 5 %
applies, and the count is stated so the reader can discount.

**Written expectation (Revision 1, A-m10).** Prior over worlds: alive at full effect 1/6, alive at half 1/6, died at
publication 1/3, dead since 2014 1/3. With §5's power: **P(existence pass) ≈ 0.3**, of which about **a third would come
from effects no longer alive**. Hence paper only after a pass, and real money only after Europe and 015F (§8).

## 1. The trial (one primary; everything else reported)

| id | statistic | rule | why |
|---|---|---|---|
| **015** | **z_m = y_m / (√12·σ̂_m)**: the settlement-shifted long/short abnormal return, scaled by the trailing vol known when the position opens (§3) | **existence:** G = 149, mean z > 0, t(z) ≥ max(t_148(0.95), CAL_CRIT = 1.92), mean z > 0 without the largest month; then the economic bar on raw y in the T+1 regime (§4) | both legs predicted; the L/S doubles the signal and is nearly market-neutral; vol-scaling is WLS under heteroskedastic months (A-M2) |

- **Reported, deciding nothing (§7):** the draft's raw-y rule; the long-only rebound (era-μ corrected t); raw v;
  by era; the recent window 2021-01 →; the per-era decomposition; Friday-T; without quarter-end months; the mechanism
  contrast; the placebo; MES and `xyz` net; the T+1-era v.
- **Removed (A-M4):** Europe (held back for its confirmation, §8) and the HMM replication (dropped).
- **Considered and not registered:** a whole-shifted 5-day pressure window; per-year μ; the paper's full-sample μ;
  long-only as primary (§3.3); "post-2017 t ≥ 1" as a fifth criterion (A-M1 option 4: superseded by the Europe
  confirmation, as the reviewer allows).

## 2. Data, calendars, and what is held back

### 2.1 Files (pulled 2026-10-13; `research/alpha/data/r015/`, gitignored, pinned in §10)

| file | rows (data) | first → last | rows dropped at load |
|---|---|---|---|
| `french_20261013T093725Z/F-F_Research_Data_Factors_daily_CSV.zip` (Ken French, keyless, pulled fresh) | 26,317 | 1926-07-01 → 2026-08-31 | all before 2013-11-01 and from 2026-06-04 on (61 rows at the end), on their date, before any value is parsed |
| `french_20261013T093725Z/Europe_3_Factors_Daily_CSV.zip` | 9,436 | 1990-07-02 → 2026-08-31 | **not read by 015** (held back for the Europe confirmation, §8); not in the guard |

- The fresh US pull is byte-identical to the B1 scout's (`2f29e225…`).
- A Japan file was pulled for 015 and deleted after a format check (its T+2 date was unverifiable: JPX pages 403).
  **The B1 scout's own Japan zip remains in `data/scout_b1/french_20261013T090723Z/`** (B-m11).
- The Treasury copies made by the draft were deleted (A-M4). They were never read by any code path except the draft's
  format check (counts and dates).
- **The loader** skips every row outside [2013-11-01, 2026-06-04) on its date, before parsing; hashes the bytes it
  parses; refuses a hash differing from the HEAD table, a −99.99 value or a non-increasing date; and opens the real
  tree, or any byte copy of a registered real file, only under the real guard's gate token (§10).

### 2.2 The calendar (dates only; `data/r015/calendar.json`, pinned; rebuilt in Revision 1)

- **T** = the last trading day of the month in the US file. **Months 2014-01 → 2026-05: 149.**
- **Lag of a month:** the unique s ∈ {3, 2, 1} with lag(T−(s+1)) = s; lag(d) = 3 before 2017-09-05, 2 to 2024-05-27,
  1 from 2024-05-28 (compliance dates, trade-date basis). Boundary months: **2017-08** s = 3 (P ends 08-25; R runs to
  09-06 across the change); **2017-09** s = 2; **2024-05** s = 1 (T+1 began on T−3; P = 05-20 … 05-29), kept
  (both reviewers).
- **Counts: 44 / 80 / 25 months (T+3 / T+2 / T+1); 64 Friday-T; 1,332 out-of-window days; 456 placebo days
  dropped.** Dates loaded: 3,164, 2013-11-01 → 2026-06-03 (the start moved from 2014-01 to 2013-11 so that 2014-01's
  vol window exists; the end moved from 2026-06-09 to 2026-06-03, B-m9). The month rows equal the B1 scout's
  independently built calendar for all 149 months (tested), and review A's and B's independent rebuilds.
- P = 5 / 6 / 7 days, R = 7 / 6 / 5 days for s = 3 / 2 / 1; windows of different months never overlap.
  Out-of-window days: the 4th trading day to T−9. **Vol window: T−28 … T−9** (it may include the previous month's
  windows; all of it is known at the close of T−9, when the short opens).

### 2.3 Held back

- Rows from **2026-06-04** on are dropped at load. **2026-07-01 →** is held back programme-wide. The forward
  registration 015F (§8) claims 2026-06 → openly, after a result.
- **The Europe month-end calendar 2014-01 → 2026-05 stays unread** until the Europe confirmation is registered (§8).

## 3. The statistic (fixed now)

### 3.1 Return series

**x_d = Mkt-RF / 100**, daily US total-market excess return. Deployable instruments earn the excess return; RF would
only add a deterministic era tilt.

### 3.2 μ and the abnormal long/short

- **μ_s** = mean of x over the out-of-window days of every month of era s. Out-of-window: no contamination by the
  windows; per era: an equity-drift difference between eras cannot leak in (weights below).
- **y_m = Σ_R(x − μ_s) − Σ_P(x − μ_s) = v_m − (2s − 4)·μ_s**; **v_m = Σ_R x − Σ_P x** (the tradable 1× L/S excess
  return). The μ weight is +2 / 0 / −2 days for s = 3 / 2 / 1; its estimation error is ≈ 2 % of Var(ȳ) and is
  calibrated in.

### 3.3 Why long/short and not long-only

Both legs are predicted; at the published magnitudes the L/S t is 1.35× the long-only t; the long-only t is
anti-conservative under the null (era-μ error with weight 5–7 days that month clustering cannot see: 0.08–0.11 false
passes at t_148 in every null world, §5). It is reported with the closed-form correction (§7).

### 3.4 Vol scaling: the primary z (Revision 1, A-M2)

- **σ̂_m = max(0.4 %, √(mean of x_d² over the 20 trading days T−28 … T−9))**, all known at the close of T−9, when
  the position opens: no look-ahead. **z_m = y_m / (√12 · σ̂_m)** (√12 makes z_m ≈ y_m / σ_month).
- This is weighted least squares for a mean under heteroskedastic months, and deployable as a vol-targeted size. It
  never loses power in the worlds tried and gains most where the regime worlds put the data (GARCH t₄: half-effect
  power 0.44 → 0.62; §5). It also down-weights crash months, which is the protection against crash
  placement that a higher critical value cannot give (A-M3).
- 20 days and the 0.4 % floor are conventions fixed before any data (the floor binds only on unusually calm tapes).
- The raw y (and v) remain the basis of the economics (§4.2); the draft's raw-y rule is reported.

## 4. The decision

### 4.1 Existence (decides)

**EXISTS iff all hold** (`r015_core.existence` on the 149 z_m; recomputed by `recheck` from the stored per-month
y, v, s, σ̂ and μ):
1. G = 149 months (one observation per month);
2. mean z > 0;
3. **t(z) ≥ c\* = max(t_148(0.95) = 1.655, CAL_CRIT = 1.92)**, CAL_CRIT calibrated on the regime worlds (§5);
4. mean z > 0 without the largest month.

**A pass means "existed 2014-01 → 2026-05 as defined"** (§0). It never leads to real money by itself.

### 4.2 The economic bar (after EXISTS only; it routes between paper and no paper)

In the **traded regime, T+1** (A-m3): net_s = ½·ȳ − 2·μ̂₁ − 1.6 bp (the shrunk effect, the T+1 era's two-day short
tilt with μ̂₁ unshrunk, two MES round trips); vol = SD of v over the 25 T+1 months; Kelly f = net_s / vol²; size
L = min(f/2, 1.2×); annual = 12·L·net_s. **Meets the bar iff net_s > 0 and annual ≥ 4 %/yr** (HLP calm).

| label | condition | what follows |
|---|---|---|
| `not_demonstrated` | not EXISTS | 015 closes: "not demonstrated at this power"; Europe and 015F are not registered for this light |
| `exists_below_bar` | EXISTS, bar not met | register the Europe confirmation and 015F (§8); **no paper trading** |
| `exists_paper` | EXISTS and bar met | the same, **with paper trading** at L on MES over 015F's months |

**No label leads to real money** (§8 states the only route).

### 4.3 Run once, and the one narrow restart (B-m6)

Any 015 row anywhere refuses every later run, except one case: the **only** 015 row anywhere is a `started` row with
`data_opened: false`, committed in HEAD by a commit **not authored by the runner** (the runner commits as `r015_run`
and stops at once if its own commit fails, so such a row was committed by hand after a failed runner commit, before
any data was opened). Then one restart is allowed. A crash after the started row is committed by the runner is
terminal; the `failed` row records the stage and whether data had been opened.

## 5. Calibration, size and power (synthetic only; `r015_power.py`; `data/r015/logs/power.json`)

**10,000 draws per world**, each a full daily history on the real trading calendar (3,164 dates), daily SD 1.12 %
(`000s`). The registered estimator end to end (era μ, y, σ̂, z, the existence rule, the T+1 bar) and the reported
statistics.

**Worlds (null, no month-end structure):** iid normal; iid t₃; GARCH(1,1) normal (α .10, β .88); GARCH t₄ (α .12,
β .87); drift by era (+8 / 0 / −8 bp a day); drift by year (±10 bp, random sign); slow AR(1) drift (φ .995, SD 8 bp);
the 2008-10 / 2020-02–03 / 2025-04 sequences (rounded public daily moves, as shapes) **placed uniformly anywhere**
and **inside random month windows**; one 2020 or one 2008 sequence with its worst two-day fall ending on a pressure-
window end (the heuristic alignment); a pressure-rebound reversal structure without a mean; window-day vol × 1.3.
**Stress, reported only:** 2020 and 2025 sequences both aligned.

**Calibration (Revision 1).** CAL_CRIT = the largest 95th percentile, over the 13 calibration worlds, of the t that the
full rule needs (−∞ unless mean z > 0 and the without-largest mean > 0), **plus 2 Monte Carlo SE** of that percentile
(batch method, 10 × 1,000), rounded up to 0.01, never below t_148(0.95).
**Result: CAL_CRIT = 1.92** (largest q95 1.862 ± 0.026 in "crash aligned: 2008 down-leg ends on a P window end, garch t4").

**D8 restated (A-M3).** Under the null, crash timing is independent of month-ends, so the honest null integrates over
placement (the "anywhere" and "inside windows" worlds). The deterministic alignments are not the worst case (review A
found worse placements) and are reported, not calibrated with a margin; the double alignment is a ≈ 0.7 % event
(review A, `p2`) and is reported. The placement scan below gives the false-pass rate at c\* conditional on where one
documented sequence falls (150 evenly spaced placements × 200 draws, iid days otherwise):

| shape | mean false pass over placements | worst placement | share of placements > 0.05 |
|---|---|---|---|
| 2020 | 0.029 | 0.080 | 0.100 |
| 2008 | 0.033 | 0.085 | 0.153 |
| 2025 | 0.029 | 0.080 | 0.080 |

(each rate has a MC SE of about 0.015 at 200 draws, so the worst placement is biased upward by selection)

**False-pass rates at c\* = 1.92** (q95 ± its MC SE; the draft's raw-y rule at 1.91 alongside):

| null world | q95 of the gate t (± MC SE) | **false pass at c\* = 1.92** | at t_148 = 1.655 | pass and bar | draft raw-y rule at 1.91 | long-only t ≥ t_148 | placebo t ≥ t_148 |
|---|---|---|---|---|---|---|---|
| crash aligned: 2008 down-leg ends on a P window end, garch t4 | 1.862 ± 0.026 | **0.045** | 0.075 | 0.010 | 0.028 | 0.085 | 0.022 |
| crash aligned: 2020 down-leg ends on a P window end, garch t4 | 1.815 ± 0.018 | **0.041** | 0.068 | 0.011 | 0.050 | 0.068 | 0.014 |
| drift by era (+8 / 0 / -8 bp/day) garch | 1.655 ± 0.022 | **0.030** | 0.050 | 0.023 | 0.028 | 0.100 | 0.037 |
| drift by year (+-10 bp/day, random sign) garch | 1.650 ± 0.023 | **0.028** | 0.049 | 0.012 | 0.029 | 0.096 | 0.036 |
| garch normal (a .10 b .88) | 1.646 ± 0.024 | **0.028** | 0.049 | 0.013 | 0.028 | 0.096 | 0.040 |
| garch t4 (a .12 b .87) | 1.698 ± 0.020 | **0.032** | 0.054 | 0.008 | 0.032 | 0.103 | 0.036 |
| iid normal | 1.692 ± 0.025 | **0.032** | 0.054 | 0.016 | 0.033 | 0.100 | 0.037 |
| iid t3 | 1.634 ± 0.023 | **0.027** | 0.048 | 0.012 | 0.028 | 0.098 | 0.039 |
| reversal structure without mean (k .33), iid normal | 1.644 ± 0.024 | **0.027** | 0.049 | 0.013 | 0.028 | 0.097 | 0.039 |
| shocks 2008/2020/2025 anywhere, garch | 1.633 ± 0.026 | **0.029** | 0.048 | 0.014 | 0.027 | 0.103 | 0.037 |
| shocks 2008/2020/2025 inside random month windows, garch | 1.591 ± 0.020 | **0.024** | 0.043 | 0.012 | 0.029 | 0.112 | 0.031 |
| slow AR drift (phi .995, sd 8 bp/day) garch | 1.681 ± 0.031 | **0.030** | 0.053 | 0.013 | 0.029 | 0.098 | 0.036 |
| window-day vol x1.3, garch t4 | 1.630 ± 0.014 | **0.028** | 0.047 | 0.014 | 0.031 | 0.083 | 0.038 |
| STRESS two crashes aligned: 2020 and 2025 down-legs end on P window ends, garch t4 | 2.066 ± 0.014 | **0.070** | 0.121 | 0.025 | 0.091 | 0.093 | 0.018 |

**Power at c\*:**

| alternative world | mean t(z) | **power at c\* = 1.92** | draft raw-y rule at 1.91 | pass and bar | recent (2021-01 →) t ≥ 1 | contrast t ≥ t_104 (mean contrast t) |
|---|---|---|---|---|---|---|
| shifted effect x1.0 garch normal | 3.03 | **0.858** | 0.776 | 0.626 | 0.833 | 0.165 (0.60) |
| shifted effect x1.0 garch t4 | 4.46 | **0.986** | 0.882 | 0.744 | 0.962 | 0.224 (0.81) |
| shifted effect x1.0 garch, shocks anywhere | 2.75 | **0.787** | 0.701 | 0.583 | 0.793 | 0.145 (0.54) |
| shifted effect x1.0 iid normal | 2.66 | **0.769** | 0.780 | 0.597 | 0.772 | 0.167 (0.59) |
| shifted effect x1.0 proportional to trailing vol, garch t4 | 3.59 | **0.914** | 0.888 | 0.720 | 0.892 | 0.221 (0.79) |
| shifted effect x1.0 with reversal correlation (k .33) iid | 2.25 | **0.626** | 0.638 | 0.469 | 0.676 | 0.159 (0.57) |
| shifted effect x0.5 garch normal | 1.51 | **0.342** | 0.291 | 0.189 | 0.500 | 0.103 (0.31) |
| shifted effect x0.5 garch t4 | 2.25 | **0.620** | 0.443 | 0.243 | 0.685 | 0.127 (0.40) |
| shifted effect x0.5 garch, shocks anywhere | 1.37 | **0.293** | 0.247 | 0.172 | 0.467 | 0.101 (0.28) |
| shifted effect x0.5 iid normal | 1.32 | **0.278** | 0.288 | 0.175 | 0.445 | 0.106 (0.30) |
| shifted effect x0.5 proportional to trailing vol, garch t4 | 1.80 | **0.457** | 0.442 | 0.207 | 0.579 | 0.132 (0.42) |
| shifted effect x0.5 with reversal correlation (k .33) iid | 1.14 | **0.217** | 0.228 | 0.132 | 0.403 | 0.105 (0.30) |
| shifted effect x0.75 iid normal | 1.98 | **0.521** | 0.536 | 0.362 | 0.619 | 0.128 (0.44) |
| shifted effect x1.5 iid normal | 3.96 | **0.975** | 0.978 | 0.907 | 0.944 | 0.237 (0.88) |
| unshifted (T+3 windows in every era) effect x0.5 iid | 1.14 | **0.221** | 0.227 | 0.132 | 0.381 | 0.034 (-0.34) |
| unshifted (T+3 windows in every era) effect x1.0 iid | 2.28 | **0.634** | 0.649 | 0.468 | 0.638 | 0.016 (-0.68) |
| died at publication: full to 2019-12, zero after, iid | 1.29 | **0.266** | 0.276 | 0.167 | 0.162 | 0.083 (0.15) |
| linear decay: full 2014-01 -> zero 2026-05, iid | 1.32 | **0.271** | 0.284 | 0.170 | 0.263 | 0.086 (0.18) |
| decayed: full in the T+3 era only, x1.0 iid | 0.80 | **0.127** | 0.132 | 0.073 | 0.161 | 0.065 (0.02) |
| turn of month only: T..T+3 +23 bp abnormal, iid | 0.71 | **0.120** | 0.121 | 0.068 | 0.298 | 0.066 (0.00) |

**Reading.**
- **Size.** At c\* = 1.92 every calibration world is at or below 5 %: **0.024–0.045**. The binding world is "crash aligned: 2008 down-leg ends on a P window end, garch t4" (q95 1.862 ± 0.026); c\* adds 2 MC SE. The uniform-placement shock worlds are at 0.029 and 0.024. The double-alignment stress world: 0.070 (reported; c would be 2.10 if calibrated on it, at a cost of 0.01–0.06 of power: iid 0.769 → 0.709 full, 0.278 → 0.222 half).
- **Power at c\*:** full effect **0.77–0.99** (iid 0.769, GARCH t₄ 0.986); half effect **0.28–0.62** (iid 0.278, GARCH t₄ 0.620). The draft's raw-y rule at 1.91 on the same draws: full iid 0.780, GARCH t₄ 0.882; half iid 0.288, GARCH t₄ 0.443. Vol scaling gains where the months are heteroskedastic and costs little under iid.
- **Effect proportional to vol** (Etula: volatility amplifies the cycle), GARCH t₄: 0.914 / 0.457 (full / half); the raw rule 0.888 / 0.442.
- **Decayed worlds pass:** died at publication 0.266; linear decay 0.271; T+3 era only 0.127; turn-of-month only 0.120. Hence paper only after a pass (§8).
- **The primary does not test the shift:** the unshifted world passes 0.634 at full effect; the contrast's power is 0.17 (shifted) against 0.02 (unshifted).
- **Existence and the bar** (T+1 regime): full iid 0.597, half iid 0.175. The bar now uses only 25 months of vol and the T+1 tilt, so it binds more than in the draft.
- **Long-only at t_148** stays anti-conservative (0.08–0.11 in the nulls); its corrected t is what §7 reports. **The placebo** is at 0.01–0.04 everywhere.
- **Placement scan:** averaged over placements the false-pass rate is 0.029–0.033 (the honest null); 8–15 % of single placements exceed 0.05, the worst at 0.08–0.085 (each with a MC SE ≈ 0.015 and an upward selection bias). That is the residual crash-placement risk, stated (§9).

## 6. Costs, tail, sizing, capacity, expected return

**Costs.** MES ≈ 0.8 bp per round trip, two a month → **1.6 bp/month at 1×**; futures earn the excess return and the
margin cash earns T-bills. **`xyz` SP500 (reported only):** 4.4 bp/month in fees ± funding on (long − short)
calendar days (≈ +1.6 / 0 / −1.6 bp by era), **and its USDC collateral earns nothing: in total return `xyz` trails MES
by ≈ 3.9 %/yr** at any effect size (A-m2; 13.8 vs 9.9 %/yr at full effect, 8.7 vs 4.8 at half). **Equity-premium tilt
in the traded regime:** v = y − 2μ₁ in T+1, ≈ −6 bp/month (−0.7 %/yr); the bar includes it.

**Tail** (documented episodes as shapes, worst placement over every start on the real calendar, at 1× notional, flat
outside the windows; review A `p4`):

| scenario | 1× | 1.2× |
|---|---|---|
| worst 5 trading days (a 2020-style sequence inside R, or the 2020 rebound inside P) | **−22.0 %** | −26 % |
| worst month (2008-10 / 2020-03 sequences inside one month's windows) | **−24.5 %** | −29 % |
| 2025-04-style month | −12.6 % | −15 % |
| month SD (unconditional) | 3.9 % | 4.7 % |

Scenario bounds, not estimates. With vol-targeted sizing (the deployable form of z) the exposure entering a
high-vol month is smaller; the bounds above are for constant notional.

**Sizing.** Gaussian monthly Kelly: 5.5× (full effect), 2.7× (half). With an adverse −24 % month once in 240 months
(A-m5): **3.1× (full), 1.6× (half)**, so **1× = 0.32 Kelly at full effect and 0.61 at half** — above half Kelly at half
effect. The paper size is therefore min(½ Kelly of the shrunk T+1 estimate, 1.2×) (§4.2), not a fixed 1×.
**Capacity:** MES / ES effectively unlimited at our size.

**Expected return** (excess, MES costs, before T-bills on margin; at 1×): full effect **+10.0 %/yr** (vol 13.5 %,
Sharpe 0.74), half **+4.9 %/yr**, minus 0.7 %/yr of T+1 tilt; zero effect −0.2 %/yr. Against the goal: a 5–10 %/yr
book at 1×, below the mandate's landing zone; it is spent because it is cheap, powered and leverable.

## 7. Reported, decides nothing (pre-stated; each block computed independently, an error stored per block)

1. **The draft's rule on raw y** (equal-weight t at the registered c\*).
2. **Long-only rebound** with the **era-μ-corrected t** (A-m7): Var = s²/G + Σ_s (G_s/G)²·|R_s|²·σ_s²/N_s.
3. **Raw v**, **MES net**, **`xyz` net**, **v in the T+1 era**.
4. **By era** (z and y) and the **recent window 2021-01 → 2026-05** (65 months; z and y) (A-M1).
5. **Decomposition per era** (A-m8): mean abnormal sums over P, T−s … T−1 and T … T+3.
6. **Friday-T vs other months** (z); **without quarter-end months** (z; 100 months).
7. **The mechanism contrast** (paired, 105 T+2 / T+1 months; algebraically −2·x_{T−3} (T+2) or −2·(x_{T−3}+x_{T−2})
   (T+1) after μ): power ≈ 0.2; it cannot decide.
8. **The placebo** at T−12 (456 days inside real windows dropped).

No reported block may become a primary. **Europe and HMM are not computed.**

## 8. After the result (fixed now; separate registrations, written only after a result)

- **`not_demonstrated`:** 015 closes. The family keeps its count of 1. Europe's calendar and 015F's months are not
  read for this light; a later month-end trial on them states 015's result.
- **`exists_below_bar` or `exists_paper`:** **paper only** (with `exists_paper`), and two confirmations are registered:
  1. **Europe confirmation** (A-M4): the same statistic (z, settlement-shifted windows, era μ, vol scaling, the same
     criteria) on the French Europe region 2014-01 → 2026-05, its lag from **verified dates only** (2014-10-06, Etula's
     treatment group; other dates only if verified from a primary source). Its critical value is calibrated on null
     worlds that include the **US–Europe correlation of daily returns under the null** (global shocks hit both books),
     so the confirmation is not a re-read of the US pass. Read once, after a US pass. Expected power ≈ 0.9 at full
     effect (review A).
  2. **015F, forward** (A-M1, A-m6): months **2026-06 → 2028-12** (≥ 31 months; the first window uses 2026-06-17 →
     2026-07-06), the same statistic, read once after **2029-03-01**. **A live step needs forward t ≥ 1** (P ≈ 0.59 /
     0.35 / 0.16 at full / half / zero effect), plus a passed Europe confirmation, plus paper net > 0 over the same
     months. Paper trading may run on a sign check.
- **Real money therefore needs: the US pass, the Europe confirmation, and 015F's t ≥ 1.**

## 9. Known weaknesses, stated now

- **Decay.** Working-paper versions from 2015, publication 2020. Decayed effects pass ≈ 0.14–0.29 (§5); a pass is
  "existed", not "alive" (§0, §8).
- **The turn-of-month premium** (T … T+3) sits in R in every era; a TOM-only world passes ≈ 0.12 (§5). The
  decomposition (§7) shows it but cannot decide it.
- **The settlement shift is not tested** (A-m1): an effect on the unshifted windows passes too; the contrast is
  underpowered.
- **Transition months** (2017-08/09, 2024-05) are rule-assigned.
- **Crash placement:** some placements of a documented crash raise the false-pass rate above 5 % (§5 scan); the
  honest null integrates over placement; vol scaling reduces the sensitivity.
- **The French factor is the CRSP total market,** not the S&P 500 traded through MES.
- **The 0.4 % floor and the 20-day window** are conventions.

## 10. Built before any outcome is computed

- [x] `r015_core.py` (pure): lag rule; `build_calendar` (P, R, P3, R3, early, tom, vol, placebo, out-days; refusals
  on gaps, overlaps, a non-unique lag, a vol window that is not before P); era μ; y, v, σ̂, z; Student-t (no numpy);
  `existence`, `economic` (T+1), `decide`, `recheck` (y = v − (2s−4)μ, z = y/(√12σ̂), hashes against the table); the
  reported blocks.
- [x] `r015_power.py`: worlds, calibration with MC SE, power, placement scan (only real input: `calendar.json`).
- [x] `r015_run.py`: `--check`, `--fingerprint`, `--calendar` (dates only), `--run` (once). **`r015_hmm.py` deleted.**
  - **(s) (a) (b) (c), M3:** status and hash table read from **`HEAD:./registrations/015-…md`**; the registration's,
    every guarded code file's and results.jsonl's working-tree bytes (`git hash-object --no-filters`) must equal their
    HEAD blobs; **skip-worktree / assume-unchanged flags and filter attributes on any of them refuse.**
  - **(e)** every guarded code and data file listed once in the HEAD table and matching.
  - **(k)** results.jsonl equal to HEAD and empty or ending with a newline.
  - **(w), B1:** **REAL_REPO = `/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline`** (pinned in the
    hashed runner): `guard()` refuses unless the runner's own repository is that path; git's top level must be it; the
    **main worktree** only (`--git-dir` = `--git-common-dir` = REAL_REPO/.git); **a detached HEAD is refused**; the
    anchor `15c6421…` must be an ancestor. A clone of the real repository at any other path, with the data copied or
    symlinked, and a linked worktree, are refused (tested).
  - **(r), 016-B M4:** reg0 = the first commit on HEAD's first-parent history whose registration says REGISTERED; it
    is the `registration_commit` of every row; the registration and every guarded code file must be unchanged between
    reg0 and HEAD (`git diff --quiet reg0 HEAD`). Any amendment after REGISTERED is a new registration.
  - **(v), M2:** `sys.flags.isolated` **and `sys.flags.no_site`** (python **-I -S**) required, and no `.pth` file in
    site-packages outside a pinned (empty) list; no -O; **pins:** Python 3.14.8, interpreter binary
    SHA-256 `7ebde552…`, stdlib-tree fingerprint `c99bbd75…` (every .py / .so under the stdlib, tests and GUI
    packages excluded). **Every loaded module** must come from the pinned stdlib or be r015_core / r015_run in this
    directory; **every cached bytecode** from the fresh `sys.pycache_prefix`. research/alpha is **appended** to
    `sys.path`, after the stdlib. Shadow scan of research/alpha's top level by blob comparison, **plus a refusal of any
    top-level module or package named like a stdlib module, committed or not.** **Recovery** after an interpreter change: reinstall Python 3.14.8 until `--fingerprint` equals
    the pins; the pins are never edited after REGISTERED.
  - **(f):** no 015 row anywhere (working tree; `git log --all --reflog -m --root -p --text --no-textconv
    --no-ext-diff`, `log.showRoot=true`, no replace objects; plus `research/copytrade/results.jsonl`); an unparseable
    line mentioning 015 refuses; the narrow restart of §4.3.
  - **git** is called as `/usr/bin/git` with GIT_* stripped, `GIT_CONFIG_NOSYSTEM=1`, `GIT_CONFIG_GLOBAL=/dev/null`,
    `GIT_NO_REPLACE_OBJECTS=1`, `core.hooksPath=/dev/null`, `core.fsmonitor=false`. The runner commits as
    `r015_run <r015_run@localhost>` with `--no-verify`, retries a locked index 3 × 1 s, and never pushes.
  - **M1:** `run()` takes no argument; guard, data, HEAD table and ledger are the real tree's; the started row says
    `data_opened: false`; `primary()` stores code and data hashes that `recheck` compares with the HEAD table.
    `_run_in()` exists for tests only and refuses anything under the real repository. **`record()` refuses to write
    into the real repository unless the real guard passed in this process and the default commit hook is in place,
    whatever the caller passes (016-B M1).**
  - **Gate:** the loader refuses any path inside the real repository, the real data tree, or a byte copy of any registered real data file anywhere, unless
    the real `guard()` (constants captured at import, `guard_failures` captured at import) passed in this process.
    The one ungated read is `read_dates_only` (`--calendar`): the first field of each line only; the run refuses
    unless the dates it loads equal the pinned `calendar.json`.
  - Nothing is printed before the calendar counts; the decision only after the result row is committed in HEAD.
- **016 coupling (B-m7).** The shadow scan refuses while any top-level `.py` or package in `research/alpha` (016's
  included) differs from HEAD. **016's draft and every revision of it must be committed before 015's `--check`;** the
  refusal message names this remedy. Neither runner blocks the other once both are committed.
- [x] `tests/test_r015.py`: **200 checks pass** in the working tree (and 199 in the synthetic
  clones below). New in Revision 1: σ̂ / z (no look-ahead, floor), the T+1 bar, `recheck` (forged z, inconsistent y,
  lowered c, hashes); DATA_END; the vol-window refusal; **(b)/(d) with skip-worktree and assume-unchanged; a filter
  attribute; status read from HEAD; (k) no newline; append refused without a newline; (w) a clone with copied data, a
  clone with symlinked data, a linked worktree, a detached HEAD, a nested `git init`; (f) orphan root commit under
  `log.showRoot=false`, `git replace`, an unparseable 015 line; the restart rule (allowed: one hand-committed started
  row; refused: runner-authored, `data_opened: true`, two rows); commit retry through an index.lock; M1 (`run()` with a
  foreign RESULTS refused; `_run_in` refuses the real directory; `_run(other)` refused before any row); the gate test
  now FAILS when the gate is deleted (mutation check); a byte copy of a registered file gated; a stubbed
  `guard_failures` refused; without -I refused; **-I without -S refused; an unpinned .pth refused; committed
  csv.py / statistics.py refused by name, and the stdlib wins the import with research/alpha appended; record() into
  the real results.jsonl with a mock hook or without the guard refused, the file byte-unchanged; the gate refuses any
  path in the real repository; (r) a committed revision after REGISTERED refused naming reg0, and the returned commit
  is reg0;** a forged `statistics` named; r015_core bytecode from an existing `__pycache__` caught; the interpreter
  matches the pins.** Both layouts (top level and `research/alpha/`) for the
  guard, `record()` (the unfixed `HEAD:results.jsonl` refuses in the subdirectory layout) and the end-to-end run.
- **Commit-state simulation, synthetic data only** (`scratchpad/commit_sim_rev1.sh`; no real data is copied, B-B1):
  a `git clone --local` of the real repository with a synthetic data tree
  (`build_synthetic_tree`; marker `data/r015/SYNTHETIC`) and the hash table rewritten to the synthetic files. Committed
  at **DRAFT: 199 of 199 pass**; status changed to REGISTERED and committed: **199 of 199 pass** (the two real-byte
  identity checks are skipped on a synthetic tree; one committed-state check is added). The real guard (`python3 -I -S
  r015_run.py --check`) in the REGISTERED clone refuses with **(w) only** (the runner is not in REAL_REPO; git's top
  level is not REAL_REPO; not the main worktree of REAL_REPO). The clone was deleted. A first attempt showed that the
  suite's own "record() into the real ledger" check could write into a clone's ledger, because the clone's ledger is
  not under REAL_REPO; `record()` now also refuses the runner's own results.jsonl without the real gate, and the rerun
  above is clean.

**SHA-256 of the code and data the outcome depends on.**

| file | sha256 |
|---|---|
| `r015_core.py` | `6c416217edb16820c188f9f1dc2facf021d98dbf3df142b2e9a43d3e51d2f82c` |
| `r015_power.py` | `4f7d3e1551bcd4e27a5f358d6145cd5bf060f853ea2ab28731ffcd5e95ce5095` |
| `r015_run.py` | `79f94c650b316facb2e52a112bb4677ee476288f1b35e71f5b4daed45caef66f` |
| `data/r015/french_20261013T093725Z/F-F_Research_Data_Factors_daily_CSV.zip` | `2f29e22546069914890a712680a6f81f3680ebc3543b52864484d209ca13a7db` |
| `data/r015/french_20261013T093725Z/SHA256SUMS` | `d6d8d44be6b8952952d4ec26fae46f69d3bccaa5d0de6c567fba7426bfa0afc7` |
| `data/r015/calendar.json` | `48b2db397845d2e9951de46fa329a74769a869fd18d94d344f0f3f3f70d09113` |
| `data/r015/logs/power.json` | `4df6e43e01a886d2131e0c4efe19eb741bfc4a4cf50026cb421addfaee7b68b9` |

**Procedure.** (1) Recheck of this revision; REGISTERED (orchestrator), committed with the code; 016's files
committed too. (2) `python3 -I -S research/alpha/r015_run.py --check` in the real repository, on a named branch, in the
main worktree. (3) `python3 -I -S research/alpha/r015_run.py --run`, once. (4) The result written here in 015's terms.

## 11. Decision table (each PROPOSED, with the drafter's recommendation)

| # | item | PROPOSED | reason |
|---|---|---|---|
| D1 | calendar | months 2014-01 → 2026-05 (149); rows outside [2013-11-01, 2026-06-04) dropped at load; 2026-07 → held back | after the paper's sample; vol window for 2014-01; no overlap with 015F |
| D2 | lag per month | the unique s with lag(T−(s+1)) = s; 2017-09-05, 2024-05-28; 44 / 80 / 25 | SEC compliance dates |
| D3 | windows | P = T−8 … T−(s+1), R = T−s … T+3 | Etula's under T+3 |
| D4 | series | Mkt-RF | deployable instruments earn excess |
| D5 | μ | per era, out-of-window days | no contamination; era drift absorbed |
| D6 | primary | **z = y / (√12 σ̂), σ̂ = RMS of x over T−28 … T−9, floor 0.4 %** | WLS; power; crash robustness (A-M2) |
| D7 | existence | G = 149, mean z > 0, t(z) ≥ max(1.655, 1.92), mean z > 0 without the largest month | regime-calibrated, with MC margin |
| D8 | calibration set | the 13 worlds; the uniform-placement null is the honest one; deterministic alignments and the double alignment reported; no extra margin beyond 2 MC SE | A-M3 |
| D9 | economic bar | after EXISTS only, T+1 regime: ½ȳ − 2μ̂₁ − cost, T+1 vol, min(½ Kelly, 1.2×) ≥ 4 %/yr; routes paper only | A-m3 |
| D10 | costs | MES 0.8 bp RT, 2 RT/month; `xyz` reported (−3.9 %/yr in total return) | B1 §2.2, A-m2 |
| D11 | size and tail | paper at min(½ Kelly shrunk, 1.2×); worst 5 days −22.0 %, month −24.5 % at 1× | A-m4, A-m5 |
| D12 | reported blocks | §7; no Europe, no HMM | A-M4 |
| D13 | after a pass | paper only; Europe confirmation and 015F (2026-06 → 2028-12, t ≥ 1) both required before real money | A-M1 |
| D14 | run once | any 015 row refuses; one narrow restart (§4.3) | B-m6 |
| D15 | data | fresh French pull (identical to B1); Treasury copies deleted; Europe not read; Japan deleted | A-M4, verified dates only |
| D16 | family | new family, trial 1; carry-over stated | — |
| D17 | environment and repository | -I -S; no .pth; stdlib names refused; reg0 frozen; pins = version + interpreter hash + stdlib-tree fingerprint; REAL_REPO, main worktree, named branch | B-B1, B-M2, B-m10 |

## 12. Open questions for the recheck

1. Is the 0.4 %/day floor right, or should it be lower (0.3 %) to bind only on extreme calm? (Proposed: 0.4 %, the
   reviewer's value; fixed now.)
2. Should the Europe confirmation's lag also use a verified T+1 date if the EU moves before it is registered (EU T+1
   is planned for 2027-10-11, after its calendar)? (Not needed for 2014-01 → 2026-05.)

## 13. Disclosures and real-data reads (for the ledger)

- **Pulls (2026-10-13):** Ken French US, Europe, Japan daily zips (identical bytes to B1's); Japan deleted after the
  format check. Treasury yield CSVs copied from B1 (verified), then **deleted in Revision 1**.
- **Format checks printed (draft):** header lines, row counts, first/last dates, sortedness, rows after 2026-06-10
  (US 57, Europe 59, Japan 59; Treasury 2026: 85), rows in [2013-11-01, 2026-06-10) (US 3,168; Europe 3,288),
  Treasury layouts. No value.
- **Calendar:** draft — US date column 2014-01-02 → 2026-06-09 (3,127 dates); **Revision 1 — 2013-11-01 → 2026-06-03
  (3,164 dates)**; printed counts only; 0 mismatches with the B1 calendar.
- **Synthetic only:** every simulation and test. The tests read the real files for hashes, names and dates only; the
  clone simulation holds synthetic data only.
- **Power runs:** draft (CAL 1.91 for the raw y; superseded). Revision 1: pass 1 at the placeholder 1.99, stopped
  after its calibration (1.92) and early power worlds; the registered pass 2 at 1.92 (same seeds, same calibration);
  logs `logs/power_rev1_pass1.txt` (partial) and `logs/power_rev1_pass2.txt`.
- **Literature:** Etula et al. (2020); Harvey–Mazzoleni–Melone (2026-01 draft; now irrelevant to 015's read).
- **Seen, beyond hashes, counts and dates: nothing.**
