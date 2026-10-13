# 015 — The month-end liquidity cycle in US equities, with windows shifted by the settlement lag (B1 #1a)

**Status: DRAFT 2026-10-13** (drafter, for two independent reviews; not registered). Programme: round 4,
`docs/alpha-discovery-2026-10-13.md` (B1). Scout: `research/alpha/scouts/b1-equities-flows.md` §3.1. Ledger
entries read: `000s` (B1 exposure), and for context `000p`–`000r`. Templates followed: `research/copytrade/r008_run.py`,
`r013_run.py` and their registrations and reviews (008-a/b/r1, 013-a/b/r1, 014-b).

**What was and was not done on real data.** No return, mean, t, P&L or any other statistic of the French or Treasury
files was computed, printed or saved: not in any window, month, era, event or in total. The real-data reads were:
- SHA-256 hashes of every file;
- format checks: header lines, data-row counts, first and last dates, sortedness, and the count of rows dated on or
  after 2026-06-10 (§2.1);
- the **trading dates** of the US file in [2014-01-01, 2026-06-10), from its first field only (the value columns are
  never parsed), to build and pin the month-end calendar (§2.2);
- the one statistic already read by the B1 scout and ledgered in `000s`: the unconditional US daily vol, 1.12 %
  (2014-01 → 2026-06), used for power only.

Every mechanic is tested on synthetic data (§10). The runner `r015_run.py` refuses to compute anything until this
file says REGISTERED and is committed and unmodified, the code and data match §10's table, `results.jsonl` holds no
`015-` row anywhere in git, the environment matches, and no importable file shadows a module (§10).

---

## 0. Hypothesis, mechanism, sign, and what has been seen

**Hypothesis.** In the US equity market, after Etula et al.'s sample, the month-end liquidity cycle still exists:
prices are pressed down over the days on which institutions must sell to have cash on the last trading day T, and
rebound afterwards. **The selling deadline, and with it the boundary between pressure and rebound, moves with the
settlement lag in force.** The test is on 2014-01 → 2026-05 (149 complete months), which is after the paper's sample
and has never been read by this programme.

**Mechanism** (Etula, Rinne, Suominen & Vaittinen, "Dash for Cash", RFS 33(1) 2020, §2.1; the published text was
read, CC BY).
1. Pensions, salaries and fund redemptions are paid on T and T+1. An institution needing cash on the morning of T
   must sell by the close of T−(s+1), where s is the settlement lag: with T+3 that is T−4 (paper: "must sell
   securities ... before the market closes on T−4").
2. ANcerno institutions are net sellers from T−8 onward, most on T−4 (their Figure 6, footnote 19), and net buyers on
   T−1 … T+3. Reinvestment of the payments adds buying pressure on T … T+3 (Ogden 1990; their "buying pressure").
3. The paper's windows under T+3: pressure **T−8 … T−4**, rebound **T−3 … T+3**.
4. **Why it can persist:** the sellers are agents trading for liquidity under reputational and tracking-error
   constraints (their footnote 9 and §5), and the risk of providing liquidity is systematic, not security-specific,
   so it is hard to arbitrage (§2.1, p. 84).
5. **The settlement sharpening (the prediction a calendar fluke cannot fake).** US equity settlement moved
   T+3 → T+2 on **2017-09-05** (SEC Rel. 34-80295, compliance date, trade-date basis) and T+2 → T+1 on
   **2024-05-28** (SEC Rel. 34-96930). The last selling day moves from T−4 to T−3 to T−2. Windows:
   pressure **P = T−8 … T−(s+1)**, rebound **R = T−s … T+3**. Under T+3 they are exactly Etula's.

**Sign.** Pressure-window abnormal returns negative, rebound-window abnormal returns positive; the long/short
statistic y_m (§3) is positive.

**Published magnitudes** (CRSP VW, 1995-07 → 2013-12, T+3; abnormal = each day's return minus the sample's average
daily return): T−8 … T−4 **−37 bp**; T−3 … T−1 **+25 bp**; T−3 … T+3 **+48 bp** (raw: −17 bp and +77 bp). The
correlation between the T−8 … T−4 and T−3 … T−1 returns is −0.39 (CRSP VW since 1995, their Table 2). When T is a
Friday, the T−3 … T−1 returns are 2.5× larger and the reversal correlation 3× stronger (§2.1). The full published
effect for 015's statistic is therefore **85 bp per month** (48 − (−37)); "half effect" is 42.5 bp.

**What has been seen, by whom, on which calendar (exposure).**

| look | overlap with 2014-01 → 2026-05 | what it computed or knew | exposure to 015 |
|---|---|---|---|
| Etula et al. (2020), main sample | none (ends 2013-12) | the effect, windows, magnitudes | the prior; it fixes every window and the sign |
| **Etula's international settlement test** (Table 3) | **2013-10 → 2015-10, our first 22 months** | the T−2 autocorrelation fell in the European treatment group relative to the control group (which **includes the US**, then T+3), DiD −0.78 | a published, aggregate result touching our first 22 US months only as part of a control group; no window return is reported. Stated, not removable |
| Etula footnote 16–17 | 2017 → | the US move to T+2 in 2017; the Fed Funds month-end spike "no longer" visible "in the most recent samples" | qualitative, money-market only |
| **Harvey, Mazzoleni & Melone** (NBER w33554, 2026-01 draft) | 1997-09 → 2023-03, extended "through 2025" (fn. 32) | month-end **Calendar-rebalancing** predictability, concentrated in the last 4–5 days of the month (their Figure 3; Calendar × week4 coefficient −0.30) | a **conditional** month-end result on our calendar (its sign depends on the month's stock-bond drift), not the unconditional cycle. It is why the HMM block is a labelled replication (§7) |
| McConnell & Xu (FAJ 2008); turn-of-month literature | none to partial | the turn-of-month premium (T … T+3) to 2005 | public folklore; the classic effect sits inside R in every era (§9) |
| B1 scout (`000s`) | the whole calendar | **unconditional daily vols** (US 1.12 %, regions 0.85–1.17 %, 10y yield change 5.21 bp), the month-end calendar (dates), auction and FOMC calendars (dates) | the US vol is used for power; no window or conditional quantity was computed |
| N7 desk scout §A3 | — | the rule sketch, from the literature; no data | none |
| Public market knowledge | 2020-02/03, 2025-04 | the COVID crash and the April-2025 tariff fall (sizes from search summaries) | used only as **shapes** placed at random or adversarial positions in synthetic worlds (§5) and for the tail (§6); nothing about where they fell relative to any window was used or computed |
| This drafter | the whole calendar | hashes, row counts, first/last dates; trading dates (calendar); no value | none |

Nobody in this programme has computed a return over any 2014-01 → 2026-05 window. The calendar is fresh to our code
and not fresh to the public: the paper's working-paper versions circulated from 2015, and post-publication decay is
possible (§9). No post-2013 US study of Etula's windows was found by the scouts (B1 §2, "Decay: unknown after 2013").

**Family.** A **new family, "US equity calendar flows": trial 1.** Programme-wide carry-over (`docs/alpha-discovery-2026-10-13.md`
§2), none of which shares this data: HL discovery year 16 trials, no pass; HL holdout read by 008 only; Binance
new-perp listing windows 2 trials (013), no pass; Binance 2021–23 1 trial (009); L2 carry 3, closed; Deribit options
0 (014 parked); HL `xyz` 0. About 23 registered trials in the programme so far, none passed. 015 shares no data and no
selection with any of them, so its own one-sided 5 % applies; the count is stated so the reader can discount.

**Written expectation (before any data).** Prior that the shifted cycle survives after 2013 at ≥ half its published
size: about **1 in 3** (B1). With the power of §5 (≈ 0.78 at full effect, ≈ 0.29 at half), P(existence pass) ≈ 0.25
overall; P(pass and the economic bar) ≈ 0.2. A pass at near-full effect is the round's first credible 10 %+/yr path
at a modest leverage; a pass at half effect is lending-plus.

## 1. The trial (one primary; everything else reported)

| id | statistic | rule | why |
|---|---|---|---|
| **015** | **y_m, long/short abnormal, per month, settlement-shifted windows** (§3) | **existence:** mean > 0, month-clustered t ≥ the calibrated critical value (§4), mean > 0 without the largest month; then the economic bar on the shrunk estimate (§4.2) | the mechanism predicts both legs; the L/S doubles the signal and is nearly market-neutral (§3.3) |

- **Reported, deciding nothing (§7):** the long-only rebound; the raw L/S; by era; the mechanism contrast (shifted
  vs unshifted, paired); Friday-T vs other months; the mid-month placebo; Europe with its own settlement date; the
  HMM rebalancing front-run as a labelled replication; MES and `xyz` net series.
- **Considered and not registered:** a 5-day pressure window shifted whole (T−(s+5) … T−(s+1)): the paper's start at
  T−8 comes from when selling begins (footnote 19), which the lag does not move; the brief and B1 fix T−8. A
  per-year μ: costs power (§3.2). The paper's full-sample μ: contaminated by the windows themselves (§3.2). Long-only
  as primary: its t is anti-conservative under the null (§3.3).

## 2. Data, calendars, and what is held back

### 2.1 Files (pulled 2026-10-13; all under `research/alpha/data/r015/`, gitignored, pinned in §10)

| file | source | rows (data) | first → last | rows ≥ 2026-06-10 |
|---|---|---|---|---|
| `french_20261013T093725Z/F-F_Research_Data_Factors_daily_CSV.zip` | Ken French data library, keyless, pulled fresh | 26,317 | 1926-07-01 → 2026-08-31 | 57 (dropped at load, never parsed) |
| `french_20261013T093725Z/Europe_3_Factors_Daily_CSV.zip` | same | 9,436 (weekday calendar) | 1990-07-02 → 2026-08-31 | 59 (same) |
| `treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2014 … 2026.csv` | **byte-identical copies** of the B1 pull (`scout_b1/treasury_20261013T091152Z`, its SHA256SUMS verified first) | 249–251 per year; 195 in 2026 | 2014-01-02 → 2026-10-09 | 85 in the 2026 file (same) |

- **The fresh French pull is byte-identical to the B1 scout's** (US `2f29e225…`, Europe `89b84ff3…`): the file has not
  changed since the scout read its vol.
- A Japan file was pulled and **deleted unread** (format check only): its T+2 date (2019-07-16) could be confirmed
  only through search summaries of JPX pages that return 403 to us, so it fails the "verified dates only" rule. Asia
  Pacific ex Japan, Developed, Developed ex US and North America are not used: their members changed settlement on
  different dates (or, for North America, it is ≈ the US).
- **The US file's format** (header, 4 header lines, columns `,Mkt-RF,SMB,HML,RF`, percent, no −99.99 marker) and the
  rows used (3,127 trading days, 2014-01-02 → 2026-06-09) were checked; no value was printed.
- **The loader** (`r015_run.load_french`) skips every row dated before 2014-01-01 or on or after 2026-06-10 **on its
  date, before parsing its values**, refuses a file whose SHA-256 differs from §10, refuses a −99.99 value or a
  non-increasing date, and opens the real tree only under the guard's gate token (§10).

### 2.2 The calendar (dates only; `data/r015/calendar.json`, pinned)

- **T** = the last trading day of the month in the US file. **Months 2014-01 → 2026-05: 149** (2026-06 is incomplete:
  its T+3 is in July).
- **The lag of a month** is the unique s ∈ {3, 2, 1} such that the lag in force on trade date T−(s+1) equals s, with
  lag(d) = 3 before 2017-09-05, 2 to 2024-05-27, 1 from 2024-05-28 (both compliance dates, trade-date basis). Fixed
  before any data; the code refuses a non-unique answer. **Boundary months:**
  - **2017-08** (T = 08-31): last selling day T−4 = 08-25 under T+3 → s = 3. Its rebound runs to T+3 = 09-06, across
    the change; the selling decision was made under T+3.
  - **2017-09**: s = 2 (T−3 = 09-26).
  - **2024-05** (T = 05-31; T+1 began 05-28 = T−3): a trade on 05-29 (T−2) settles 05-30 = T−1 under T+1, so the last
    selling day is T−2 → s = 1. The pressure window T−8 … T−2 (05-20 … 05-29) spans the change: the first T+1
    month is a transition month (§9).
- **Counts: 44 / 80 / 25 months under T+3 / T+2 / T+1; 64 Friday-T months; 1,332 out-of-window days.** The calendar
  equals the B1 scout's independently built calendar (`scout_b1/derived_20261013T092459Z/month_end_calendar_US.csv`)
  for all 149 months (T, s, both windows), tested.
- Window lengths: P = 5 / 6 / 7 days, R = 7 / 6 / 5 days for s = 3 / 2 / 1. Windows of different months never
  overlap (refused otherwise). Out-of-window days of a month: its 4th trading day to T−9.

### 2.3 Held back

- **Rows dated on or after 2026-06-10** are dropped at load (the last complete window, 2026-05, ends 2026-06-03).
- **2026-07-01 →** is held back programme-wide. The forward step (§8) claims the months from 2026-06 openly, after a
  result.

## 3. The statistic (fixed now)

### 3.1 Return series

**x_d = Mkt-RF / 100**, the US total-market return in excess of the one-month T-bill, daily (Ken French; CRSP total
market, dividends included). **Why excess and not Mkt-RF + RF:** every deployable instrument (MES futures, the `xyz`
SP500 perp, a CFD) earns the excess return, financing being embedded; the mechanism is a price-pressure effect, not a
cash-rate effect; and RF would only add (|R| − |P|) × rf_d per month, a deterministic era tilt.

### 3.2 μ and the abnormal statistic

- **μ_s** = the mean of x over the out-of-window days of every month of era s (s = 3, 2, 1). **Why out-of-window:**
  the paper's full-sample mean contains the windows' own returns, so under the alternative it is biased toward the
  effect. **Why per era:** μ enters y_m with weight |R_m| − |P_m| = 2s − 4 = **+2, 0, −2 days** for s = 3, 2, 1. A
  single pooled μ lets an equity-drift difference between the T+3 era (2014–2017) and the T+1 era (2024–2026) leak
  into y; the era μ absorbs it (the drift-by-era world, §5). Per-year μ would cost power for no gain in the
  simulated worlds.
- **y_m = Σ_{d∈R_m}(x_d − μ_{s_m}) − Σ_{d∈P_m}(x_d − μ_{s_m})** = v_m − (2s_m − 4)·μ_{s_m}, where
  **v_m = Σ_R x − Σ_P x** is the tradable 1× long/short excess return (used for the economics).
- In the T+2 era (80 months) y_m = v_m exactly. The μ estimation error enters with weight ±2 days and opposite signs
  in the two other eras, so it is ≈ 2 % of the standard error of ȳ; the calibration includes it (§5 simulates the
  same estimator).

### 3.3 Why the primary is the long/short and not the long-only rebound

- The mechanism predicts both legs: pressure **and** rebound (Etula's whole argument is the reversal; the pressure
  leg is −37 bp of the 85).
- At the published magnitudes the L/S has the higher t: 85 / (σ√12) against 48 / (σ√7), a ratio of 1.35. Synthetic
  power (§5): L/S 0.78 at c\* vs long-only 0.64 even at the lower t_148 (iid, full effect).
- **The long-only t is anti-conservative under the null:** μ enters the long-only sum with weight 5–7 days and a
  common error within each era, which month clustering cannot see. Its false-pass rate at t_148(0.95) is
  **0.10 (0.08–0.11 across the null worlds)** in the iid null (§5). It is reported with that warning.
- The L/S is market-neutral in the T+2 era and nearly so elsewhere (±2 days), so slow equity drift barely enters.

## 4. The decision

### 4.1 Existence (decides)

**EXISTS iff all hold** (`r015_core.existence`; recomputed by `recheck` from the 149 per-month records stored in the
row):
1. **G = 149** months (one observation per month; windows never overlap, so months are the clusters);
2. **ȳ > 0**;
3. **t = ȳ / (sd(y)/√149) ≥ c\* = max(t_148(0.95) = 1.655, CAL_CRIT = 1.91)**, CAL_CRIT calibrated on the
   regime worlds of §5;
4. **the mean of y without the largest month is > 0.**

### 4.2 The economic bar (applied only after EXISTS; it only routes)

On the tradable v (raw L/S excess), **shrunk by half**: net_s = ½·v̄ − 1.6 bp (two MES round trips a month at 0.8 bp);
full Kelly f = net_s / var(v); size L = min(f/2, 1.2×) (½ Kelly, capped by the tail, §6); annual = 12·L·net_s.
**Meets the bar iff net_s > 0 and annual ≥ 4 %/yr** (HLP calm, the benchmark). Without the cap, ½ Kelly gives
annual = 6·net_s²/var(v), so the bar needs net_s ≥ 32 bp, i.e. **v̄ ≳ 67 bp/month at σ ≈ 3.9 %**. One point bar, on a
shrunk estimate, after a pass; it never turns a pass into a fail.

| label | condition | what follows |
|---|---|---|
| `not_demonstrated` | not EXISTS | 015 closes: "not demonstrated at this power" (§5), never "no effect". The forward months stay unread by 015 |
| `exists_below_bar` | EXISTS, bar not met | a **registered forward confirmation** (§8), read only, **no paper trading** |
| `exists_paper` | EXISTS and bar met | the same forward confirmation **with paper trading** at L (§8) |

No label leads to live trading. The row stores every number the decision uses; a row that does not recompute is
stored with `recheck_failed` and its decision is not printed.

## 5. Calibration, size and power (synthetic only; `r015_power.py`; log `data/r015/logs/power.json`)

**10,000 draws per world.** Each draw is a full synthetic daily history on the **real trading calendar** (the pinned
dates; 3,127 days), daily SD 1.12 % (the one real number, `000s`). The registered estimator end to end: era μ, y, the
existence rule, the economic bar; and the reported statistics (long-only, contrast, placebo).

**Worlds (null: no month-end structure).**
- iid normal; iid t₃; GARCH(1,1) normal (α .10, β .88); GARCH(1,1) t₄ (α .12, β .87, persistence .99);
- slow drift regimes (GARCH): drift by settlement era (+8 / 0 / −8 bp a day, the adversarial direction for μ); drift by
  calendar year (±10 bp a day, random sign); a slow AR(1) drift (φ .995, SD 8 bp a day);
- shocks: the 2008-10, 2020-02/03 and 2025-04 sequences (rounded public daily S&P moves, used as shapes) **placed
  anywhere**, and **inside random month windows** (crash months coinciding with window positions);
- **crash aligned**: the 2020 (or 2008) sequence placed so that its worst two-day fall ends on the last pressure day of
  a random month and its rebound falls in R (the adversarial alignment; GARCH t₄);
- a pressure-rebound reversal structure without a mean (R's first three days carry −0.33 × the P sum: the paper's
  −0.39 correlation, as variance only); window-day vol × 1.3.
- **Stress, reported, not in the calibration set:** two crashes (2020 and 2025) **both** adversarially aligned.

**Calibration.** CAL_CRIT = the largest 95th percentile, over the calibration worlds, of the t that the full rule needs
(−∞ unless ȳ > 0 and the without-largest-month mean > 0), rounded up to 0.01, never below t_148(0.95).
**Result: 1.91** (largest q95 1.904, in "crash aligned: 2020 down-leg ends on a P window end, GARCH t₄"). If the stress world were included: 2.13.

**False-pass rates of the registered rule at c\* = 1.91:**

| null world | q95 of the gate t | **false pass at c\* = 1.91** | at t_148(0.95) = 1.655 (uncalibrated) | pass and bar | long-only t ≥ t_148 | contrast t ≥ t_104 | placebo t ≥ t_148 |
|---|---|---|---|---|---|---|---|
| crash aligned: 2008 down-leg ends on a P window end, garch t4 | 1.678 | **0.030** | 0.053 | 0.009 | 0.088 | 0.092 | 0.024 |
| crash aligned: 2020 down-leg ends on a P window end, garch t4 | 1.904 | **0.049** | 0.087 | 0.018 | 0.066 | 0.106 | 0.017 |
| drift by era (+8 / 0 / -8 bp/day) garch | 1.669 | **0.033** | 0.051 | 0.029 | 0.105 | 0.062 | 0.037 |
| drift by year (+-10 bp/day, random sign) garch | 1.670 | **0.030** | 0.051 | 0.019 | 0.098 | 0.062 | 0.038 |
| garch normal (a .10 b .88) | 1.646 | **0.028** | 0.049 | 0.017 | 0.103 | 0.062 | 0.038 |
| garch t4 (a .12 b .87) | 1.644 | **0.026** | 0.049 | 0.007 | 0.108 | 0.065 | 0.034 |
| iid normal | 1.687 | **0.032** | 0.053 | 0.019 | 0.100 | 0.065 | 0.039 |
| iid t3 | 1.633 | **0.029** | 0.048 | 0.017 | 0.103 | 0.069 | 0.035 |
| reversal structure without mean (k .33), iid normal | 1.657 | **0.029** | 0.050 | 0.019 | 0.093 | 0.062 | 0.038 |
| shocks 2008/2020/2025 anywhere, garch | 1.605 | **0.024** | 0.045 | 0.013 | 0.103 | 0.062 | 0.033 |
| shocks 2008/2020/2025 inside random month windows, garch | 1.680 | **0.029** | 0.053 | 0.018 | 0.108 | 0.062 | 0.033 |
| slow AR drift (phi .995, sd 8 bp/day) garch | 1.658 | **0.027** | 0.050 | 0.015 | 0.096 | 0.063 | 0.036 |
| window-day vol x1.3, garch t4 | 1.632 | **0.029** | 0.048 | 0.017 | 0.081 | 0.059 | 0.039 |
| STRESS two crashes aligned: 2020 and 2025 down-legs end on P window ends, garch t4 | 2.125 | **0.086** | 0.144 | 0.043 | 0.098 | 0.158 | 0.020 |

**Power at c\* (and at the stress-calibrated value):**

| alternative world | mean t | **power at c\* = 1.91** | at 2.13 (stress-calibrated) | pass and bar | long-only t ≥ t_148 | contrast t ≥ t_104 (mean contrast t) | placebo t ≥ t_148 |
|---|---|---|---|---|---|---|---|
| shifted effect x1.0 garch normal | 2.72 | **0.775** | 0.709 | 0.719 | 0.639 | 0.170 (0.63) | 0.036 |
| shifted effect x1.0 garch t4 | 3.49 | **0.881** | 0.848 | 0.807 | 0.768 | 0.220 (0.80) | 0.041 |
| shifted effect x1.0 garch, shocks anywhere | 2.42 | **0.694** | 0.617 | 0.627 | 0.577 | 0.154 (0.54) | 0.039 |
| shifted effect x1.0 iid normal | 2.71 | **0.780** | 0.709 | 0.721 | 0.637 | 0.166 (0.61) | 0.038 |
| shifted effect x1.0 with reversal correlation (k .33) iid | 2.27 | **0.632** | 0.548 | 0.568 | 0.603 | 0.163 (0.59) | 0.037 |
| shifted effect x0.5 garch normal | 1.37 | **0.297** | 0.231 | 0.230 | 0.322 | 0.105 (0.31) | 0.038 |
| shifted effect x0.5 garch t4 | 1.75 | **0.445** | 0.369 | 0.225 | 0.423 | 0.128 (0.41) | 0.039 |
| shifted effect x0.5 garch, shocks anywhere | 1.22 | **0.243** | 0.177 | 0.184 | 0.301 | 0.091 (0.27) | 0.038 |
| shifted effect x0.5 iid normal | 1.36 | **0.294** | 0.231 | 0.235 | 0.324 | 0.103 (0.30) | 0.036 |
| shifted effect x0.5 with reversal correlation (k .33) iid | 1.14 | **0.222** | 0.167 | 0.178 | 0.301 | 0.101 (0.28) | 0.041 |
| decayed: full in the T+3 era only, x1.0 iid | 0.80 | **0.137** | 0.094 | 0.100 | 0.220 | 0.060 (-0.02) | 0.038 |
| shifted effect x0.75 iid normal | 2.02 | **0.542** | 0.460 | 0.470 | 0.473 | 0.130 (0.44) | 0.038 |
| shifted effect x1.5 iid normal | 4.02 | **0.981** | 0.967 | 0.972 | 0.875 | 0.242 (0.90) | 0.039 |
| unshifted (T+3 windows in every era) effect x0.5 iid | 1.17 | **0.230** | 0.171 | 0.175 | 0.291 | 0.030 (-0.34) | 0.035 |
| unshifted (T+3 windows in every era) effect x1.0 iid | 2.31 | **0.651** | 0.566 | 0.579 | 0.558 | 0.014 (-0.71) | 0.038 |

**Reading.**
- **Size.** At c\* = 1.91 every calibration world is at or below 5 %: **0.024–0.049**. The binding world is a single
  2020-style crash placed adversarially (its fall ending on a pressure-window end, its rebound in R). Uncalibrated,
  t_148(0.95) would give up to 0.087 there; the iid and GARCH worlds alone would need only ≈ 1.69.
- **Stress.** Two crashes both aligned: 0.086 at 1.91. Calibrating on it (2.13) costs ≈ 0.07 of power at full effect
  (0.78 → 0.71) and ≈ 0.06 at half. Proposed: not calibrated on (D8; the reviewers may choose 2.13).
- **Power at c\*:** full effect **0.69–0.88** over the iid, GARCH and shock worlds (0.63 if the rebound carries the
  paper's reversal correlation as extra variance); **half effect 0.22–0.45**; 0.75× effect 0.54.
- **Existence and the bar:** full effect 0.57–0.81; half 0.18–0.24. The bar binds mostly at half effect, as intended.
- **The primary does not discriminate the mechanism.** If the effect stayed on the unshifted T+3 windows, the
  shifted test still passes 0.65 of the time at full effect. The paired contrast separates the two only weakly (mean
  t +0.61 shifted vs −0.71 unshifted; P(contrast t ≥ t_104) 0.17 vs 0.01). An effect that died after the T+3 era
  passes 0.14.
- **The long-only rebound t is anti-conservative:** 0.08–0.11 false passes at t_148 in every null (the era-μ error, §3.3).
- **The placebo** passes at 0.02–0.04 in every world, as it should.

## 6. Costs, tail, sizing, capacity, expected return

**Costs** (B1 §2.2, published schedules).
- **MES (primary):** ≈ 0.8 bp per round trip; two round trips a month (open short at the close of T−9, flip at the
  close of T−(s+1), close at T+3) → **1.6 bp/month at 1×**. Futures earn the excess return; the margin cash earns
  T-bills on top (≈ 3.6–4.4 %/yr, 2025-07 → 2026-06), not counted in any figure here.
- **`xyz` SP500 perp (reported):** 2.2 bp RT taker → 4.4 bp/month, plus ≈ 0.8 bp per calendar day held long net of
  days held short (excess terms at the ≈ 5.5 %/yr funding baseline, `n1-xyz.md`): ≈ +1.6 bp (T+3 era, long longer),
  ≈ 0 (T+2), ≈ −1.6 bp (T+1, a short-heavy month earns it) → **≈ 3–6 bp/month**. Computed per month from the calendar
  in the reported block.
- **Equity-premium tilt:** v carries (2s − 4) days of the premium: ≈ +6 bp/month in the T+3 era, 0 in T+2, **≈ −6 bp
  (−0.7 %/yr) in the T+1 era**, which is the regime of any forward trading.

**Tail** (documented episodes, as shapes, worst alignment; at 1× notional; flat outside the windows):

| scenario | 1× | 1.2× |
|---|---|---|
| worst plausible **week**: long R through a 2020-03-style 5-day fall (−19.4 %), or short P through the 2020-03 rebound (+19.6 %) | **−19 to −20 %** | −23 to −24 % |
| worst plausible **month**: the 2020-03 or 2008-10 sequence inside one month's windows, worst alignment (−23.4 % / −22.4 %) | **−23 %** (≈ −25 % with the other days' noise) | −28 to −30 % |
| 2025-04-style month (−10.5 % two-day fall inside R) | −12 % | −15 % |
| month SD (unconditional, iid) | 3.9 % | 4.7 % |

These are scenario bounds, not estimates: nothing was computed on the real windows. HLP's worst drawdown was −2.5 %;
this is an equity book with an equity tail, two weeks a month.

**Sizing (fraction of Kelly).** Monthly Kelly for the L/S at σ_v ≈ 3.9 %: **full effect** f = (85 − 1.6) bp / 0.00152
≈ **5.5×**; **half effect** ≈ **2.7×**. **Proposed size: L = 1.0× notional** (≈ 0.18 Kelly at full effect, ≈ 0.37 at
half), cap 1.2× (the −30 % plausible month). The economic bar sizes at min(½ Kelly of the **shrunk** estimate, 1.2×).
**Capacity:** MES/ES are effectively unlimited at our size; `xyz` SP500 OI caps are far above it.

**Expected return** (excess of T-bills, MES costs, before the T-bill on margin cash):

| effect | net bp/month | 1.0× | 1.2× | vol (1×) | Sharpe |
|---|---|---|---|---|---|
| full (85 bp) | 83.4 | **+10.0 %/yr** | +12.0 %/yr | 13.5 %/yr | 0.74 |
| half (42.5 bp) | 40.9 | **+4.9 %/yr** | +5.9 %/yr | 13.5 %/yr | 0.36 |
| zero | −1.6 | −0.2 %/yr | −0.2 %/yr | 13.5 %/yr | — |

On `xyz`, subtract ≈ 0.2–0.5 %/yr more. In the T+1 era subtract the −0.7 %/yr premium tilt. **Against the goal:** only
at full effect and ≈ 2.6× (≈ ½ Kelly of the full effect) does this reach ≈ 26 %/yr, with a −60 % plausible month; at
the proposed 1× it is a 5–10 %/yr book (plus T-bills on the margin cash), below the mandate's landing zone and above
HLP calm only at ≥ half effect. It is spent because it is cheap, powered and leverable, not because it is the goal.

## 7. Reported, decides nothing (pre-stated; each block computed independently, an error stored per block)

1. **Long-only rebound** Σ_R(x − μ_s): summary (with the §3.3 warning).
2. **Raw L/S v**, **MES net** (v − 1.6 bp), **`xyz` net** (v − the per-month `xyz` cost).
3. **By era** (s = 3 / 2 / 1): mean and t of y.
4. **The mechanism contrast:** d_m = y_m(shifted) − y_m(unshifted T+3 windows, P3 = T−8 … T−4, R3 = T−3 … T+3, the
   same μ), paired by month, in the T+2 and T+1 eras (105 months), and separately per era. If the mechanism holds,
   E[d] = 2 × 37/6 ≈ 12 bp (T+2) and 2 × 2 × 37/7 ≈ 21 bp (T+1). **Its power is ≈ 0.15–0.22 (P(t ≥ t_104(0.95)))** at full effect
   (§5): it cannot decide, and a non-positive contrast is not evidence against the mechanism. The unshifted y over all
   149 months is reported alongside.
5. **Friday-T vs other months:** Welch t of y (64 vs 85 months); Etula: 2.5× larger when T is a Friday.
6. **The mid-month placebo:** the same shapes anchored at T−12 (P′ = T−20 … T−(s+13), R′ = T−(s+12) … T−9), with any
   day inside a real window (in practice the first three days of a month, the previous month's R) dropped; 456 days
   dropped over 149 months. Expected ≈ 0.
7. **Europe** (French "Europe" region, USD, weekday calendar): y with its own lag, T+3 → T+2 on **2014-10-06** (the date
   verified in Etula's text; their treatment group). 9 months T+3, 140 T+2. Caveats: country holidays appear as rows;
   Germany was T+2 already and Spain moved in 2016; FX noise. Plus its mechanism contrast.
8. **The HMM rebalancing front-run, a LABELLED REPLICATION** (inside the authors' sample to 2023-03-17 and their
   2025 extension; out of sample for nobody). Buildable from free data without a new pull: French Mkt total return
   for the S&P 500; the 10-year par yield (Treasury CSVs, copied) as a duration-return proxy for the note,
   r = y_{t−1}/252 − D(y_{t−1})·Δy, D the modified duration of a 10-year semiannual par bond. Signals per their (B.1)–
   (B.2) and §4: Threshold averaged over δ = 0 … 2.5 % in 0.1 % steps, rescaled −Threshold / 1.5 %; Calendar
   sign(−Calendar) in the last 5 business days, sign(Calendar_{t−4}) on the first business day, else 0; weight = the
   mean of the two; return (R_SP − R_10Y) on the next day. Interpretations of their text are written in
   `r015_hmm.py`. Reported: annualised mean, vol, Sharpe, skew, month-clustered t, over 2014-01 → 2023-03-17 and
   2023-03-18 → 2026-06-09, and the equity leg alone. A forward confirmation of HMM would be its own registration.

No reported block may become a primary.

## 8. The forward step (fixed now; a separate registration, written only after a result)

- **After `exists_paper` or `exists_below_bar`:** register **016-forward** on the months **2026-06 → 2027-12** (19
  months; its first window uses 2026-06-17 → 2026-07-06), the same statistic (era μ from the forward months'
  out-of-window days, all T+1), read **once**, after **2028-03-01** (French publication lag). Criterion: forward ȳ > 0
  (a sign check: P ≈ 0.83 / 0.68 / 0.50 at full / half / zero effect — a guard against a reversed effect, not a
  powered confirmation, and stated as such). With `exists_paper`, paper-trade MES at L (≤ 1.2×) over the same months,
  logging fills and costs; a live step needs forward ȳ > 0 **and** paper net > 0.
- **After `not_demonstrated`:** 015 closes. The family "US equity calendar flows" keeps its count of 1. The forward
  months are not read for this light; any later month-end trial on them must state 015's result.
- If the HMM replication is strong, it gets its own forward registration on 2026-07 → (post-publication).

## 9. Known weaknesses, stated now

- **Post-publication decay.** Working-paper versions from 2015 and publication in 2020; the effect may have been
  arbitraged. Power at half effect is ≈ 0.29: a fail is "not demonstrated", not "gone".
- **The classic turn-of-month premium** (T … T+3) sits inside R in every era, so the existence test can pass on it
  alone, with no pressure leg. The mechanism contrast (§7) is the only discriminator and is underpowered. A pass is
  "the month-end cycle as defined", not proof of the settlement channel.
- **Transition months** (2017-08/09, 2024-05) are assigned by the rule; habits may lag the rule.
- **Crash alignment.** A crash whose fall ends on a pressure-window end can manufacture a pass; the calibration holds
  one aligned crash at 5 %, the stress world with two aligned crashes gives 0.086 (§5).
- **Within-month persistence beyond a month** is not modelled beyond GARCH and slow drift worlds.
- **The French factor is the CRSP total market**, not the S&P 500 traded through MES; tracking is close but not exact.
- **Europe** is a mixed-calendar, USD-converted region; reported only.
- **HMM proxies** (par-yield duration return, French Mkt for S&P futures) and our reading of their signal text.
- **The forward step is weak** (19 months): a sign check, not a confirmation.

## 10. Built before any outcome is computed

- [x] `r015_core.py` (pure): the lag rule, `build_calendar` (P, R, P3, R3, placebo, out-days; refusals on gaps,
  overlaps, a non-unique lag), era μ, y, v, long-only, Student-t CDF/quantile (no numpy), `existence`, `economic`,
  `decide`, `recheck`, the reported blocks.
- [x] `r015_hmm.py` (pure): the HMM replication.
- [x] `r015_power.py`: the worlds, calibration and power above (only real input: `calendar.json`).
- [x] `r015_run.py`: `--check`, `--calendar` (dates only), `--run` (once).
  - **The guard** (every check a refusal, never an assert): (s) REGISTERED; (a) tracked; (b) unmodified vs HEAD;
    (c) a commit; (d) guarded code clean; (e) every guarded code and data file listed **once** in the table below and
    matching; (k) `results.jsonl` tracked and clean; **(w) `git rev-parse --show-toplevel` asked from the runner's own
    directory equals the repository two levels above it, and the anchor `15c6421…` is an ancestor of HEAD** (a passed-in
    path is never trusted; a nested `git init` is refused); (v) `-O` refused, Python pinned 3.14.8, **shadow-module scan**
    of research/alpha's top level (untracked, ignored or modified `*.py`, sourceless `*.pyc`, `*.so`/`*.pyd`, packages),
    r015_core / r015_hmm loaded from source, **a fresh `sys.pycache_prefix`** outside the repository set before any r015
    import; (f) **no `015-` row anywhere**: working tree and `git log --all --reflog -m -p --text --no-textconv
    --no-ext-diff` (every branch, reflog, merge and stash diffs; a renamed registration is caught by prefix).
    **GIT_\* variables are stripped** from every git call. **Every `<rev>:<path>` is `<rev>:./<path>`** (014 review B B1).
  - **Gate token on every data read:** the value loaders refuse the real tree unless the real `guard()` passed in this
    process; a forged token with a stubbed guard is refused. **Mocks refused on the real tree:** the real results file
    and the real data only together, only with the default commit hook, only with the real guard.
  - **Run once:** `started` row (committed), then the `result` row (committed and verified in `HEAD:./results.jsonl`),
    then a separate `reported` row; a crash before the result writes a `failed` row. Any existing 015 row refuses
    every later run (no restart clause; D14). The runner commits `results.jsonl` alone and **never pushes**.
  - **The one ungated read** is `read_dates_only` (used by `--calendar` before registration): it reads the first field
    of each line only and never parses a value (tested with non-numeric value columns), and the run refuses unless the
    dates it loads equal the pinned `calendar.json`.
  - Nothing is printed before the calendar counts; the decision is printed only after the row is committed.
- [x] `tests/test_r015.py`: **137 checks pass**, including:
  - calendar rules, boundary months, window lengths, overlaps, the placebo, Europe's lag;
  - y = v − (2s − 4)μ exactly; μ from out-of-window days only; the noiseless published effect gives 85 bp each month;
  - each existence criterion failing alone; the economic bar; the labels; `recheck` catching a forged summary, a
    lowered critical value and a forged economic label;
  - loaders: rows ≥ 2026-06-10 (non-numeric in the synthetic file) never parsed; hash mismatch, −99.99 and repeated
    dates refused; the real tree refused without the gate, and with a forged token;
  - **the guard in both layouts** (runner at the top level, and in `research/alpha/` of a throwaway repository with
    git run from the subdirectory): (s), (b), (d), (e), duplicate table rows, (k), (w) wrong top level, (w) nested
    `git init`, (w) anchor, (f) uncommitted / erased / reset / stashed / renamed / other-branch rows, a `-diff`
    attribute and `diff.external`, `GIT_DIR` pointing at a decoy;
  - **`HEAD:./` demonstrated:** with the unfixed `HEAD:results.jsonl`, `record()` refuses in the subdirectory layout
    (the 014 B1 failure) and still works at the top level (why it hid); with the fix it works in both;
  - a no-op commit hook makes `record()` refuse;
  - **end to end in both layouts** on synthetic files (including non-numeric rows after the cutoff): started → result →
    reported, each committed in HEAD; the result recomputes; every reported block computes; a second run is refused;
  - shadow modules; a forged `__pycache__` `.pyc` loads under plain Python and not under a fresh `pycache_prefix`;
    `-O` refused; the environment fingerprint;
  - real files (hashes, names, dates only): the table below matches; the French pull equals the B1 copy; the calendar
    rebuilds byte-identically from the date column and equals the B1 calendar; the power log was produced at the
    registered CAL_CRIT; the real repository passes (w) and (e).
- **Commit-state simulation** (`scratchpad/commit_sim.sh`: a `git clone --local` in the scratchpad, the real repository
  untouched): the 015 files committed at **DRAFT: 137 of 137 pass**; the status line
  changed to REGISTERED and committed: **137 of 137 pass**; the real guard (`--check`, all checks, environment included)
  in the REGISTERED clone: **"guard open"**. No outcome mode was run; the clone was deleted.

**SHA-256 of the code and data the outcome depends on.**

| file | sha256 |
|---|---|
| `r015_core.py` | `123a977b37d0ac24245c045069e13f079e863bdce76fc053029dfa6f2122fa36` |
| `r015_hmm.py` | `0643b0110559c8eb3e3db550056d2300f95d92e169cd5606c0bea1830f09f7db` |
| `r015_power.py` | `3afb1ecdde6196ca73ac559e7a19ee6c7225e43e6fdfa89025fd563fd9f3d7cf` |
| `r015_run.py` | `fd277a4cb2b27f352a9e9c07e25ba118ae880d04b001de7611e947520026c444` |
| `data/r015/french_20261013T093725Z/F-F_Research_Data_Factors_daily_CSV.zip` | `2f29e22546069914890a712680a6f81f3680ebc3543b52864484d209ca13a7db` |
| `data/r015/french_20261013T093725Z/Europe_3_Factors_Daily_CSV.zip` | `89b84ff3c1edbe6dcd237498b6a68a092f2467ad3fee5fa2dfa4c295ec946ddc` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2014.csv` | `8d560698e03e4c0d8d22ff8be7f5f7c5c0d472b1e33a22b6a75373a57e84aa0f` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2015.csv` | `b7665d1ce46d6ed6d08fbda34aae1831de38e274e3c7caed81904042aac9b5bc` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2016.csv` | `b4d0a8d86e2734bc64d84d9a9824434d841737621582daeac58f13c7129f325c` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2017.csv` | `56074da7c32103a28cf4ba2bdf5eaa12f98fd49852047aef1a6eb16e64bd6e83` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2018.csv` | `e8375524a7a8d3e8091bbe7690d0378b9365be09efff5b18b8eefddc25f718d3` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2019.csv` | `83fc47c17325e222c45fd3a1eb26c69ae01f62d33b793030b4b8b350ca0a290b` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2020.csv` | `edcf1c8be9622f3cd92eb7de1847aefc3977e2118e2ff9af67f46614a5c306a7` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2021.csv` | `1be57e49c63a99f6002431a1030c8fbe539b71066c3eaf14f9f58ddc34492cea` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2022.csv` | `c33cb2758e5e34c2c23b7ab759d18681091860f3f9e5960112859f64f31cd814` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2023.csv` | `edb7f3904e11dcaa502cdcc03ad9b1b16c893802d166ce9bc5fa1118251249d4` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2024.csv` | `6775840b76cd7fd41121f61d6f6c60184eca6b2ba49954ec7372c463cbeef34d` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2025.csv` | `cf2fa3da7f160384b63d2ead698532b1d76cd9f789ce4911e2304db85b14e6d5` |
| `data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_2026.csv` | `0e7164f6f348497a4541b61ce9b3e76fc00902c988540d08332a7d956350c764` |
| `data/r015/french_20261013T093725Z/SHA256SUMS` | `d6d8d44be6b8952952d4ec26fae46f69d3bccaa5d0de6c567fba7426bfa0afc7` |
| `data/r015/treasury_copy_20261013T093812Z/SHA256SUMS` | `da01fa48c1fc550959b9a6abcb3b2e81ed2e373fc1be47b4d4150aa9cfb0512a` |
| `data/r015/calendar.json` | `262aae0d9530440e02a38bc3bcb37b794ad92f797db588177255a34e6e617de4` |
| `data/r015/logs/power.json` | `49105bed7b29273aeb44dbb45033ac40a18d749fc080b7dafd032593b669c78e` |

**Procedure.** (1) Two independent reviews, a recheck, then REGISTERED (orchestrator), committed with the code; every
`r015_*.py` tracked and unmodified first. (2) `python3 -I research/alpha/r015_run.py --check`. (3)
`python3 -I research/alpha/r015_run.py --run`, once. (4) The result written here in 015's own terms.

## 11. Decision table (each PROPOSED, with the drafter's recommendation)

| # | item | PROPOSED | reason |
|---|---|---|---|
| D1 | calendar | months 2014-01 → 2026-05 (149); rows ≥ 2026-06-10 dropped at load; 2026-07 → held back | after the paper's sample; unread; last complete window |
| D2 | lag per month | the unique s with lag(T−(s+1)) = s; 2017-09-05 and 2024-05-28 (trade date); 44 / 80 / 25 | the selling deadline, from the SEC compliance dates, fixed before data |
| D3 | windows | P = T−8 … T−(s+1), R = T−s … T+3 | Etula's windows under T+3; only the deadline moves |
| D4 | series | Mkt-RF (excess), not total | deployable instruments earn excess; RF is a deterministic tilt |
| D5 | μ | per settlement era, from out-of-window days | no contamination by the windows; era drift absorbed |
| D6 | primary | the L/S y_m; long-only reported | both legs predicted; higher t; long-only t anti-conservative under the null |
| D7 | existence rule | G = 149, ȳ > 0, t ≥ max(1.655, CAL_CRIT = 1.91), mean > 0 without the largest month | month clusters; regime-calibrated |
| D8 | calibration set | §5 worlds; the two-aligned-crashes world as a reported stress (it would give 2.13) | calibrating on a deterministic double alignment costs ≈ 0.07 power for a ≈ 2 % event; the reviewer may choose 2.13 |
| D9 | economic bar | after EXISTS only: ½-shrunk net at min(½ Kelly, 1.2×) ≥ 4 %/yr; routes only | one bar, shrunk, after the existence test |
| D10 | costs | MES 0.8 bp RT, 2 RT/month; `xyz` reported | B1 §2.2 |
| D11 | size and tail | 1.0× notional proposed (cap 1.2×); week −20 %, month −23 to −25 % at 1× | ≈ 0.18–0.37 Kelly; tail from documented episodes |
| D12 | reported blocks | §7, each computed independently; none may become a primary | pre-stated |
| D13 | forward | 016-forward, 2026-06 → 2027-12, read once after 2028-03-01, sign check; paper only after `exists_paper` | the held-back months, claimed openly |
| D14 | run once | any 015 row refuses; no restart clause | simplest; a crash is terminal and ledgered |
| D15 | data | fresh French pull (identical to B1), Treasury copied byte-identically; Japan deleted unread | verified dates only |
| D16 | family | new family "US equity calendar flows", trial 1; programme carry-over stated | no shared data or selection |
| D17 | environment | Python 3.14.8, stdlib only; stdlib-source fingerprint stored | no third-party package on the decision path |

## 12. Open questions for the reviewers

1. D8: calibrate on the double-aligned stress world (2.13) or not (1.91)?
2. D14: add a restart clause (as 008's) for a mechanical crash before the result row?
3. Is the 2024-05 transition month better excluded (G = 148)? Proposed: kept (rule-based, fixed now).
4. Should the forward step be longer (2026-06 → 2028-12, 31 months) to raise its power (P(ȳ > 0) ≈ 0.88 at full)?

## 13. Disclosures and real-data reads (for the ledger)

- **Pulls (2026-10-13):** Ken French US, Europe and Japan daily factor zips, fresh (identical bytes to B1's); Japan
  deleted after the format check (its date unverifiable). Treasury yield CSVs copied from the B1 pull (verified).
- **Format checks printed:** header lines, data-row counts, first/last dates, sortedness and uniqueness, rows ≥
  2026-06-10 (US 57, Europe 59, Japan 59; Treasury 2026: 85), rows in [2013-11-01, 2026-06-10) (US 3,168; Europe
  3,288), the Treasury files' column layouts and blank-10-Yr counts (0). No value.
- **Calendar:** built from the US file's date column (3,127 dates, 2014-01-02 → 2026-06-09); printed counts (149
  months, 44 / 80 / 25, 64 Friday-T, 1,332 out-of-window days, 456 placebo days dropped); compared with the B1
  calendar (0 mismatches).
- **Synthetic only:** every simulation and test. The tests read the real files for hashes, names and dates only.
- **Literature opened:** Etula et al. (2020, published text, CC BY); Harvey–Mazzoleni–Melone (2026-01 draft, EDHEC
  copy): §1.2–1.3, §4, Appendix B, Tables 6, C.1. **Search only:** JPX's T+2 date (pages 403).
- **Power runs:** the registered log (`logs/power.json`, `logs/power_run2.txt`) was produced by the hashed code at
  CAL_CRIT = 1.91. An earlier run at the placeholder 1.99 (same seeds; it gave the same calibration, 1.91) was stopped
  during its power phase and its partial log deleted; a 200-draw smoke run preceded it. Synthetic only.
- **Seen, beyond hashes, counts and dates: nothing.** No real-data outcome was seen by the drafter.
