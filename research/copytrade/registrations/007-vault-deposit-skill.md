# 007 — Depositing into top-scored Hyperliquid user vaults (L1)

**Status: DRAFT** — Revision 2, written 2026-10-11 for the owner's decisions.

- **What has been done.** The mechanics are built and checked on synthetic
  data (§8). The eligible sets, data coverage and power were measured from
  behaviour only (§3).
- **What has not been computed.** No lookback score, no NAV change, no
  return and no score-versus-outcome statistic, for any vault.
- **What is pinned.** The ledgers, NAV grids, activity table and archive
  files the runner needs are pinned by hash (§2, §8). Their amounts and
  values were stored, never printed or inspected.
- **When the runner will compute.** `r007_run.py` refuses until all of the
  following hold:
  - this file says REGISTERED, is committed, and is unmodified;
  - the code and the data manifest match §8;
  - the manifest digest equals the one on the §8 "Data manifest" line;
  - `results.jsonl` holds no 007 row, in the working copy or anywhere in
    its committed history.
- **Status of the decisions.** Every decision in §9 is PROPOSED.

**Revision 2 (2026-10-11, after second review).** The review is
`registrations/reviews/007-review-r1.md`. It found no blocker and asked for
two code fixes; the orchestrator decided the items below. No outcome existed
for any version.

- **M-a: criterion 1 changes.**
  - The random-effects clustered Z is no longer a pass gate. The review
    showed it was about α 0.05 under heterogeneity across windows.
  - Criterion 1 now requires all three of: the tercile-weighted Fisher
    Z ≥ 2.33; a **t-test on the four window ICs ≥ t₃(0.975) = 3.182**; and
    IC > 0 in ≥ 3 of 4 windows.
  - The random-effects Z, and the window-resampling form, are reported only.
  - A **market-adjusted outcome** is added: net return − lookback net beta
    × BTC's return over the vault's measured span. Beta is point-in-time.
    It is a reported block in discovery and the required outcome at the
    holdout CONFIRM.
- **M-b: closure and near-empty capital.**
  - The behavioural closure, which read activity after the hold, is
    deleted.
  - The depositor now exits at the first in-hold grid point whose AV is
    ≤ max($100, 1% of equity at S) after an outflow ("exhaustion"). That
    interval's base counts only flows before the payout.
  - An interval that starts with AV < $1k ends the measurement at its
    start.
  - Nothing after the hold end can decide or change an outcome. A
    property test enforces it.
- **M-c: the holdout is an owner decision.**
  - Options: A, two windows; or B, four windows ending 2027-02-15 with
    sealed fetches.
  - Power: 5,000 simulations per cell, plus the null CONFIRM rate under a
    rally null with an exposure tilt.
  - Fixed for both options: the CONFIRM uses the market-adjusted outcome; a
    sign rule; and an entry on HLP's first grid point within 14 days of S,
    common to every vault.
- **Minors.**
  - Outcome variants report criterion 3 as n/a.
  - Every §3/§8 number is regenerated from the cited logs.
    `power_r2.md` is pasted verbatim.
  - Guard (f) now scans the committed history of `results.jsonl`.
  - Guard (g) compares the digest on the "Data manifest" line exactly.
  - The manifest is committed with the REGISTERED commit.
  - Added: a worst-case imputation variant, and unmeasurable counts by
    reason.
  - Criterion 3 fails if more than 10% of the top quintile's c3 values are
    missing.
  - The HLP-estimator wording is corrected.
  - Null rows use 2,000 simulations (holdout 5,000), with Wilson upper
    bounds. The calibration checks use 1,000 bootstrap draws.

**Revision 1 (2026-10-11, after the first review,
`registrations/reviews/007-review.md`).**

- A distribution became an outflow.
- The eligibility rule became point-in-time activity plus an outside
  depositor.
- The entry never uses a point before S: the order is grid point, then
  snapshot, then unmeasurable (imputed, with a 5% stop).
- The market factor was added to the null simulations.
- Criterion 1 got its net-exposure tercile control.
- Returns became unscaled, and criterion 3 got a common interval.
- Discovery reads end 2026-06-08.
- Gates were added for the NAV identity (with flows), the profit share and
  the unmeasurable share. The exit cost was added.
- The guard was hardened, archive files were pinned, BTC is filtered at the
  cutoff, and the bootstrap clusters by leader.

The count reads of drafting and of both reviews are ledgered in `000e`
(§7).

Programme: `docs/edge-discovery-2026-10-11.md` (light L1; §3 points 1–3).
Scout: `scouts/l1-vaults.md`. Earlier ledger entries: `000`–`000e`. Trials
`001`–`005`: **fourteen trials on one discovery year, no pass.**

**The hypothesis.** 001 T1's per-trip skill score is the plain t of return
on notional over a 91-day lookback. Among Hyperliquid **user vaults** that
show outside depositors, it ranks the return a depositor earns over the
next 56 days, net of the leader's 10% profit share and the exit cost. The
top quintile beats both the average eligible vault and HLP over the same
interval.

**Why this, and what is already known (disclosure).**

- **001 T1 passed in every hold-time bucket** (IC 0.40–0.44 per window,
  Z 40.9, wallet-clustered 31.6). That is per-trip, equal-weighted skill.
  001 excluded vault addresses by list before scoring, so **no vault has
  ever been scored**.
- **Vaults are day traders.** Their median hold is 10–17 h (§3). 001 T2b
  found that a follower loses day-trader skill within the hour. A depositor
  has zero lag, which is why this light exists.
- **The warning (programme §3.1).** 001 T2's risk-weighted per-trip ATR-R
  was −0.05. A depositor earns the size-weighted return. The size-weighting
  diagnostic (§6) measures this.
- **The benchmark is known (H7).** `docs/hlp-benchmark-2026-10-10.md`
  published HLP's monthly returns over the discovery year. The HLP bar over
  the four holds is roughly −3%, +5%, 0% and −0.5% (review). **The 56-day
  schedule was kept knowing HLP's path.** The schedule is 001's and was
  fixed before H7.
- **The year's direction is known.** BTC's log return over the four holds
  was −0.256, −0.253, +0.027 and −0.111 (market data, `power_r2.md`). It is
  used for the bear-year null.
- **Selection of the light.** L1 was chosen because T1 persisted on the
  archive year. No design value was set from an outcome.
  - The distribution counts in 000e changed the *definition* of the exit,
    because they showed the draft's closure was false.
  - No threshold was tuned to them.

**The fifteenth look, and why z 2.33.** This is **trial 15** on the
discovery year 2025-07-28 → 2026-06-30, after 14 with no pass.
- z 2.33 (one-sided α ≈ 0.01) is 005's convention, not a family-wise
  correction. Over 15 looks at α 0.01, the chance of at least one false
  discovery pass is about 14%.
- That is acceptable only because a discovery pass buys nothing but the
  holdout read (D12), and the holdout has its own CONFIRM bar (D14).
- The joint criteria pass a null far less often than α (§3, with upper
  bounds).

## 1. Trials registered here

| id | hypothesis | role |
|---|---|---|
| T1 | At each of 001's four selection dates S, the lookback plain per-trip t of eligible user vaults ranks their depositors' net returns over the hold (§4), within terciles of lookback net exposure, in every window (the window t-test). The top quintile beats the eligible mean and HLP over the same interval, and the result does not rest on one vault | primary, the only trial |

One trial, one-sided **z_crit 2.33**. **Cumulative on the discovery year:
fifteen trials.** A discovery pass earns only the holdout read (D12).
Everything in §6 is reported and decides nothing.

**Ring-fence.** No reported block or variant becomes a primary on the
discovery year. That includes the market-adjusted block and both clustered
Z forms. **No variant can unlock or consume the holdout.** The holdout is
read once, only after a primary pass, and only under D14. A follow-up that
a variant suggests is a new trial on new data.

**Written expectation.** The odds are about 1 in 3 that the score ranks
depositor returns at an IC of 0.2 or more. §3 gives the discovery and
end-to-end probabilities. This test can refute a large effect. It confirms
a moderate one only under holdout option B, and even then not often. The
most likely failure is the one 001 T2 showed: the rank holds per trip, and
the size-weighted return does not.

## 2. Data and periods

- **Periods.**
  - Discovery is 2025-07-28 → 2026-06-30, **read only up to 2026-06-08**,
    the last hold's end (`V.READ_END`). Within each window, the outcome
    reads only [S, S + 56 d].
  - The holdout (2026-07-01 →) is unread.
  - Every API request carried endTime 2026-06-30T23:59:59.999Z, or its
    response was truncated at load to t < 1782864000000.
- **Schedule (D3):** 001's windows, with lookback [S − 91 d, S) and hold
  [S, S + 56 d): S = **2025-10-27, 2025-12-22, 2026-02-16, 2026-04-13**.
  Holds do not overlap.
- **Vault list** (`data/cache/hl-vaults`, pinned). Only address, leader,
  name, createTimeMillis and relationship are read. HLP and its 7 children
  are excluded. `apr`, `pnls`, `tvl`, `isClosed` and `allowDeposits` are
  never read (000c).
- **Fills:** the pruned archive day files 2025-07-28 → 2026-06-07
  (**pinned, 315 files**), streamed one vault at a time. `realized_pnl` is
  never used.
- **Lookback features:** 001's p4 SQL, copied. It reproduces 001's
  `features_<S>.parquet` for every vault row and column on the four dates
  (0 mismatches). Pinned.
- **Lookback net exposure:** the median over lookback snapshot rows of
  `net_notional / account_value`, with snapshots before S and before the
  cutoff in the query. It is the criterion-1 control and the beta of the
  market-adjusted outcome. Pinned per date.
- **Activity table:** fill counts and first/last fill times per vault-day,
  for fills before 2026-06-08. **The runner reads only the part before S**
  (the 7-day activity rule). Pinned.
- **Snapshots** (pinned) give equity at S, and the entry fallback in
  [S, S + 10 d]. The query filters at the cutoff and at the read bound.
- **Ledgers (pinned):** `userNonFundingLedgerUpdates` for the 196
  candidates, from creation to 2026-06-30T23:59:59.999Z.
  - Fetch check: 0 stuck pages, 0 rows ≥ 2026-07-01.
  - The runner re-filters at load to < 2026-06-08.
  - Key names: `vaultWithdraw` carries `requestedUsd`, `netWithdrawnUsd`,
    `commission`, `closingCost`, `basis` and `user`. `vaultDeposit` and
    `vaultDistribution` carry `usdc` only.
- **NAV grids (pinned):** `vaultDetails` → allTime `accountValueHistory` /
  `pnlHistory`, with t < 1782864000000 and every current-state field
  dropped before saving. The runner bounds them at 2026-06-08.
- **HLP:** the parent's `vaultDetails` from the H7 cache (pinned),
  truncated at load. It has a 14-day grid with 4 points per hold, first
  point S + 3.0 d.
- **BTC:** Bitget 1H closes through `ct/vaults.Btc`. Its query keeps only
  candles that close before min(read bound, cutoff).
- **Terms:** a 10% profit share (verified at run time, gate 2) and a 1-day
  lock-up.

## 3. What was measured before writing this (behaviour only, no return)

Sources:
- the eligibility and coverage numbers come from `data/vaults/describe_r1.log`
  (`r007_describe.py describe`, unchanged in Revision 2);
- the power numbers come from `data/vaults/power_r2.log` /
  `describe/power_r2.md` (`r007_power.py`);
- the synthetic errors come from `data/vaults/test_vaults_r2.log`.

**Eligibility funnel (D2).** All reads are before S.

| S | lookback fills | ≥ 20 trips, eq ≥ $10k, snapshot ≤ 7 d | + maker ≤ 0.5, ≤ 500 fills/day (candidates) | − inactive (no fill in [S − 7 d, S)) | **full set** | with an outside depositor = **primary (proposed)** |
|---|---|---|---|---|---|---|
| 2025-10-27 | 1,575 | 158 | 101 | 2 | **99** | **74** |
| 2025-12-22 | 1,524 | 131 | 80 | 1 | **79** | **64** |
| 2026-02-16 | 1,195 | 114 | 65 | 2 | **63** | **50** |
| 2026-04-13 | 1,241 | 121 | 74 | 2 | **72** | **54** |

- **Distinct vaults.** Full set 182; outside set 140, present in 1/2/3/4
  windows as 77/37/13/13.
- **Leaders** (outside set). 65/59/46/50 leaders. 15/9/8/7 vaults share a
  leader; at most 4 per leader.
- **Behaviour** (outside set).
  - Median of median hold: 16.9/14.8/9.9/15.4 h.
  - Median lookback net exposure: 0.31/0.17/0.04/0.04.
- **Capacity** (outside set; equity at S ≥ 10× the deposit):
  - $10k fits 21/20/14/14 vaults;
  - $50k fits 5/2/4/6;
  - $250k fits 2/0/0/0.
- **Entry coverage.** Every eligible vault-window has an allTime point in
  [S, S + 10 d]. So there are 0 snapshot entries and 0 unmeasurable rows
  at the primary dates (grid presence only). Every eligible vault has ≥ 4
  grid points in each hold.

**NAV source (D5): `allTime` pnl / AV plus the vault's own ledger flows.**
Modified Dietz per grid interval, chain-linked, unscaled.

- **Level cross-check (draft).** The allTime AV against the snapshot AV
  within ±1 h agrees to a median of 0.15%; 91% of pairs agree within 1%.
- **Synthetic error** (`test_vaults_r2.log`; 300 vaults per row; exact
  unit ledger; 3%/day volatility):

  | grid | flows | error on the measured span (rmse, bias) | flows unknown | Spearman(estimate, true 56-day return) | unmeasurable |
  |---|---|---|---|---|---|
  | **7 d** | **0.3/day, ≤ 25% of equity** | **1.93%, −0.04%** | 2.32% | **0.931** | 0/300 |
  | 7 d | 1.0/day, ≤ 50% | 8.21%, −0.02% | 8.55% | 0.829 | 1/300 (thin base) |
  | 14 d (random phase) | 0.3/day, ≤ 25% | 2.29%, +0.29% | 2.63% | 0.869 | **92/300** (no point in [S, S + 10 d]) |

- **Distributions.**
  - One payout of 20–80% of equity with the vault trading on: 3.63% rmse
    and +0.04% bias against the exact time-weighted return. The gap to a
    depositor who holds the cash is 8.95% rmse (§4).
  - A full payout gives the exact return to the payout.
  - A full payout followed by a restart gives the same exit, at the payout.

**Power: joint, all criteria, by simulation.**
- Setup: `ct/vaults.simulate_power`, run by `r007_power.py`.
  - Inputs: the real eligible sets, identities and leaders.
  - Dispersion: idiosyncratic SD = max(5%, gross leverage × BTC's 56-day
    SD).
  - Market: one draw per window × lookback net exposure.
  - Score reliability ρ 0.42.
  - **NAV noise at Spearman 0.93 in every row.**
  - Neutral level: every vault's arithmetic mean equals HLP's.
- Criterion 1 is Fisher Z ≥ 2.33 **and** window t ≥ 3.182 **and** 3 of 4
  windows positive.
- The planted IC is the population Spearman IC between score and the
  *true* return.
- The tables below are `describe/power_r2.md` verbatim.

BTC 56-day log-return SD (discovery, 260 windows): 0.150. BTC log return over the four holds: -0.256, -0.253, +0.027, -0.111.

**Discovery, joint power** (2,000 data sets per row; Wilson 95% upper bound in brackets for IC 0):

| population | version | IC 0 | IC 0.1 | IC 0.2 | IC 0.3 | c1 at IC 0.2 |
|---|---|---|---|---|---|---|
| outside | (a) D > 0 | 0.004 (≤ 0.007) | 0.100 | 0.513 | 0.861 | 0.639 |
| outside | (b) D/SE ≥ 1.64 | 0.002 (≤ 0.004) | 0.048 | 0.314 | 0.690 | 0.639 |
| full | (a) D > 0 | 0.003 (≤ 0.006) | 0.118 | 0.572 | 0.907 | 0.715 |
| full | (b) D/SE ≥ 1.64 | 0.001 (≤ 0.004) | 0.064 | 0.394 | 0.777 | 0.715 |

**Nulls** (outside set, IC 0, 2,000 data sets):

| null | plain c1 | c1 (with window t) | all five (a) | all five (b) |
|---|---|---|---|---|
| bear null, corr -0.2, net-exposure terciles | 0.039 | 0.023 (≤ 0.031) | 0.006 (≤ 0.010) | 0.004 (≤ 0.007) |
| bear null, corr -0.2, control unrelated | 0.113 | 0.051 (≤ 0.062) | 0.013 (≤ 0.018) | 0.009 (≤ 0.015) |
| bear null, corr -0.4, net-exposure terciles | 0.096 | 0.049 (≤ 0.059) | 0.017 (≤ 0.023) | 0.013 (≤ 0.020) |
| bear null, corr -0.4, control unrelated | 0.327 | 0.131 (≤ 0.146) | 0.029 (≤ 0.038) | 0.025 (≤ 0.032) |
| bear null, corr -0.4, market-adjusted outcome | 0.004 | 0.001 (≤ 0.004) | 0.001 (≤ 0.004) | 0.000 (≤ 0.002) |
| window-shock null, shift sd 0.13 | 0.032 | 0.011 (≤ 0.017) | 0.008 (≤ 0.013) | 0.003 (≤ 0.007) |
| window-shock null, shift sd 0.26 | 0.086 | 0.021 (≤ 0.028) | 0.015 (≤ 0.021) | 0.008 (≤ 0.013) |

**Sensitivity** (outside, IC 0.2): k = 0.5 0.529 (a) / 0.340 (b); rho 0.25 0.515 (a) / 0.330 (b); average vault HLP - 2% 0.422 (a) / 0.270 (b); market-adjusted outcome (reported block) 0.341 (a) / 0.238 (b).

**Holdout options, P(CONFIRM)** (5,000 data sets per cell; Wilson upper bound for nulls):

| option | IC 0 | IC 0.1 | IC 0.2 | IC 0.3 | IC 0.2 at att. 0.88 | rally null +0.2 | rally null +0.4 | +0.4 on RAW outcome |
|---|---|---|---|---|---|---|---|---|
| A (2 windows: 07-06, 08-31; sets 50, 54) | 0.009 (≤ 0.012) | 0.072 | 0.256 | 0.505 | 0.217 | 0.008 (≤ 0.011) | 0.004 (≤ 0.006) | 0.050 |
| B (4 windows: + 10-26, 12-21; sets 74, 64, 50, 54) | 0.009 (≤ 0.012) | 0.122 | 0.430 | 0.711 | 0.382 | 0.004 (≤ 0.006) | 0.000 (≤ 0.001) | 0.019 |
| B conservative (sets 50, 54, 50, 54) | 0.012 (≤ 0.016) | 0.139 | 0.429 | 0.732 | 0.380 | 0.009 (≤ 0.012) | 0.005 (≤ 0.007) | 0.084 |

**End to end** (discovery outside (a) or (b) × holdout P(confirm), attenuation 0.93):

| option | (a) IC 0.2 | (a) IC 0.3 | (b) IC 0.2 | (b) IC 0.3 |
|---|---|---|---|---|
| A (2 windows: 07-06, 08-31; sets 50, 54) | 0.131 | 0.435 | 0.080 | 0.349 |
| B (4 windows: + 10-26, 12-21; sets 74, 64, 50, 54) | 0.220 | 0.612 | 0.135 | 0.490 |
| B conservative (sets 50, 54, 50, 54) | 0.220 | 0.630 | 0.134 | 0.505 |

The row labelled "B conservative" reuses the 2026-02-16 and 2026-04-13
sets twice, with maximal vault overlap. Its result is close to the
four-distinct-sets row, so the gain does not depend on the stand-in sets.

**Holdout reading.**
- At IC 0.2, option B raises P(confirm) from 0.256 to 0.430 (×1.68) and
  the end-to-end chance from 0.131 to 0.220 (version a).
- At IC 0.3, B gives 0.711 against 0.505; end to end, 0.612 against 0.435.
- Both options have a null CONFIRM of 0.009.
- Under the rally null tilted toward exposure, the market-adjusted outcome
  keeps CONFIRM at ≤ 0.008. On the raw outcome it would be 0.050 (A) and
  0.019 (B).
- **B is materially stronger, at the cost of four months.**

**Reading the power tables.**
- **The window t-test costs power.** It is still the honest rule for four
  windows: its null rate stays at the nominal one-sided level under any
  between-window heterogeneity. The plain criterion 1 can pass a null in
  which the IC moves by window. The window t-test removes that, as the
  window-shock rows show.
- **The bear-year null.** The score is tilted short (corr −0.4) and the
  market is fixed at the realised BTC holds.
  - Plain c1 passes far above nominal with an unrelated control.
  - Net-exposure terciles cut it.
  - The window t-test cannot see a same-sign shock.
  - The market-adjusted outcome removes most of what remains.
  - Criteria 2–5 hold the joint rate down.
- **Sensitivity** rows vary dispersion, reliability and the average
  vault's level.

**Schedule (D3): 001's four windows** (unchanged from the draft). The
28-day step would lose 2025-11-24 to the snapshot gap.

**Holdout (D14): owner's choice of option A or B** (tables above).

Fixed for both options:
- **The outcome is market-adjusted:** net − β̂ × BTC's simple return over
  the common span, with β̂ the lookback net exposure. In the simulations β̂
  carries noise at 0.5 × the cross-sectional SD of beta.
- **The sign rule:** the mean of the window ICs > 0, and D > 0.
- **The common grid entry.** The entry is HLP's first allTime point in
  [S, S + 14 d]; the exit is HLP's last at or before S + 56 d. Every vault
  is measured from its own grid point within ±1 day of each, from
  timestamps alone. If a vault has no such point, or a snapshot within
  ±1 day of the entry, it is unmeasurable.
- **CONFIRM** requires all of:
  - combined Fisher Z ≥ 1.64;
  - leader-clustered D / SE ≥ 1.64;
  - the sign rule;
  - the top quintile beats HLP (raw net, same span, at most 10% of c3
    missing).

The options:
- **A.** Windows 2026-07-06 and 2026-08-31, read once after 2026-10-26.
  The lookbacks need fills to 2026-08-30 (the 008 pull).
- **B.** Windows A plus **S3 = 2026-10-26** and **S4 = 2026-12-21**, end
  date **fixed now at 2027-02-15**, read once then, with no interim look.
  - Every ≤ 4 weeks from registration to 2027-02-15, run a sealed,
    prospective fetch: `vaultDetails` projected to allTime **and the
    `month` period** (finer points), plus every candidate's ledger, the
    HLP parent, and the snapshot equity rows needed at S3 and S4.
  - Each fetch is hashed into a dated manifest and never opened.
  - Lookbacks need fills to 2026-12-20: the 008 pull plus later archive
    days, with a byte count quoted first.
  - The cost is four months.
- **Recommendation (orchestrator): B, if its confirm power is materially
  higher.** The table shows its gain at IC 0.2 and 0.3, and the null
  CONFIRM rate of each option under the rally tilt.

**Holdout feasibility.**
- With A's re-fetch after 2026-10-26, a 14-day grid would be shared across
  vaults. Every vault then has a point near HLP's (the ±1-day rule), so the
  "30% unmeasurable" risk of Revision 1 disappears: the entry is HLP's
  point, not a 10-day window.
- If a vault's grid phase differs from HLP's, the vault is unmeasurable.
  The 5% stop then applies, and in B the sealed `month` series is the
  remedy.

## 4. T1 — score and outcome

**Score (computed only after registration).** The plain per-trip t of
001 §4, `mean(RoN) / sd(RoN) × √n`, over the vault's own fills in
[S − 91 d, S). Also reported: the cross-regime score, and t / √n. The
second answers the √n concern, since the plain t rewards activity.

**Outcome: the depositor's net return over the hold** (`hold_return`).
It reads only [S, S + 56 d] of the vault's grid and ledger.

1. **Entry.** The first allTime grid point in [S, S + 10 d]. If none, the
   first snapshot there; its first interval then uses AV minus the ledger
   flows. If none, the row is **unmeasurable**. **Never a point before S.**
2. **Per interval (a, b], Modified Dietz.** The return is
   r = (pnl_b − pnl_a) / (AV_a + Σ f_i (t_b − t_i)/(t_b − t_a)).
   - Flows are the vault's own ledger: create, deposit and transfer in;
     withdraw out (the field chosen by gate 1); **distribution out, as an
     ordinary outflow**.
   - Intervals are chain-linked in logs, unscaled.
3. **Exit,** whichever comes first:
   - **(i) Thin base.** An interval starting with AV < $1k ends the
     measurement at its start. If that is the entry, the row is
     unmeasurable.
   - **(ii) Exhaustion.** At the first grid point b with AV ≤ max($100, 1%
     of equity at S) after an outflow in (a, b], the depositor was paid
     out.
     - The exit is the last outflow in (a, b].
     - That interval's base counts only flows before it, weighted over
       [t_a, t_exit]. A payout one day into a 7-day interval is not scaled
       up.
     - A later restart is new money and is never looked at.
     - A vault that loses everything by trading, with no outflow, is a
       loss, not an exit.
   - **(iii)** The last grid point at or before S + 56 d.
   - Nothing after the hold end can change any of these (test, §8).
4. **What the return measures.** The NAV-per-share total return with any
   distribution reinvested (time-weighted). A depositor who receives a
   partial distribution holds the cash instead. The gap is definitional:
   8.95% rmse at payouts of 20–80%.
5. **Net and adjusted.**
   - **Net** = gross − 10% × max(gross, 0) − the exit cost. The exit cost
     is the median of closingCost / requestedUsd over the vault's
     non-leader withdrawals in [S − 91 d, S). A total loss ends at −100%.
   - **Market-adjusted** = net − β × (BTC_exit / BTC_entry − 1), with β the
     lookback net exposure.

**Criterion 3's interval.**
- **HLP** is measured from its first grid point in [S, S + 10 d] to its
  last at or before S + 56 d.
  - **HLP has no ledger here, so its net flow is placed at each interval's
    midpoint.** That is a different flow treatment from the vaults'
    ledger timing (synthetic cost: 2.32% rmse against 1.93%).
  - HLP has no profit share.
- **Each vault's c3** uses its own grid points within ±1 day of HLP's
  entry and exit. The estimator is the same as the vault's own outcome
  (ledger flows, exhaustion, thin base).

**Gates.** They run at run time, before any criterion. Any failure stops
the run, and an amendment follows.
1. **NAV identity.** Interval check: |ΔAV − Δpnl − Σ flows| ≤ 0.5% × AV_a.
   - Scope: [2025-07-28, 2026-06-08), AV_a ≥ $1k, the full eligible set.
   - Pass: flow-free ≥ 95%, and flows ≥ 90% under the better-agreeing
     withdrawal field.
2. **Profit share.** |commission − 10% × max(requested − basis, 0)| ≤
   max($0.01, 0.5%) in ≥ 90% of non-leader lookback withdrawals.
3. **Unmeasurable.** At most 5% of vault-windows. They are imputed at the
   window median and counted by reason.

## 5. Pass — all five

Per window, the rows are the D2 population with a score. An unmeasurable
row is imputed at the window median. The top quintile is the n // 5
highest scores.

1. **Ranking**, all of:
   - the Spearman IC within **terciles of lookback net exposure**, with
     **Fisher Z = Σ z_w / 2 ≥ 2.33**;
   - the **t statistic of the four window ICs, mean / (sd / 2), ≥ 3.182**
     (one-sided 0.975, 3 degrees of freedom);
   - **IC_w > 0 in ≥ 3 of 4 windows.**
2. **Top beats the eligible mean.** D = the mean over windows of
   (top-quintile mean − all-eligible mean). **Version (a):** D > 0.
   **Version (b):** D / SE ≥ 1.64, with SE a leader-clustered bootstrap of
   1,000 draws. Both are reported; the owner picks one (D9).
3. **Top beats HLP over the same interval.** The mean over windows of
   (top-quintile mean c3 − HLP's return) > 0. **The criterion fails if
   more than 10% of the top quintile's c3 values are missing.**
4. **Mean and median of all top-quintile vault-windows have the same
   sign.**
5. **Not one vault.** D > 0 after removing, from every window, the vault
   with the largest positive contribution.

**Reading rule (001).** An IC pass with criteria 2–3 failing is loser
persistence.

**Placebo.** 200 seeds, with scores shuffled within each window. The
criterion-1 share is calibrated if ≤ 5%; the |Z| ≥ 2.33 share is about 2%.

## 6. Reported, decides nothing

- **The other population** (the full set), under all five criteria.
- **Statistics.**
  - IC in the top half; decile means; plain IC per window.
  - **The window-and-leader Z in both forms**: random-effects τ² and
    window resampling, each with 1,000 draws.
  - **The market-adjusted outcome** under criteria 1, 2, 4 and 5.
  - IC within long-bias terciles and within trip-count terciles; t / √n;
    the IC of trip count against net return; size weighting (the score
    against the hold's per-trip t, its size-weighted RoN, and the net
    return).
  - BTC beta of the top quintile; monthly regime labels.
- **Capacity and counts.** Capacity of the top quintile. Counts by reason:
  unmeasurable (no_entry / no_exit / thin_base_at_entry / capital_base /
  span); exits (hold_end / exhausted / thin_base / total_loss); snapshot
  entries; c3 missing.
- **Variants.** Outcome variants **report criterion 3 as n/a** and pass on
  criteria 1, 2, 4 and 5, because only the primary carries c3:
  - cross-regime score; t / √n; long-bias control; trip-count control;
  - scaled to 56 d; AV-minus-flows; midpoint flows; the other withdrawal
    field; no exit cost; gross of the profit share; market-adjusted.

  These keep criterion 3: equity ≥ $100k; **worst-case imputation (the
  window minimum)**; winsorised at 2.5/97.5%.

The ring-fence (§1) applies to all of it.

## 7. Known weaknesses, stated now

- **Prior knowledge.** This is the fifteenth look; HLP's path was
  published in H7; the year's BTC path is known.
- **000e, count exposure.**
  - Drafting counted in-hold distributions (12/8/4/2).
  - The first review read event types and times (61/15/16, a payout on
    day 2) and non-leader withdrawal presence.
  - **Revision 1** read:
    - per-vault-day fill counts and first/last times through 2026-06-07
      (stored; only the row count was printed);
    - grid-point and snapshot presence in [S, S + 10 d], and the minimum
      in-hold grid-point count;
    - lookback net exposure;
    - pre-S ledger `user` versus leader;
    - BTC hold returns.
  - **Revision 2** added no new vault data read. It recomputed BTC's SD
    and hold returns from closes before 2026-06-08.
  - All of these are counts, times or behaviour. None is an amount.
- **Power.**
  - **The window t-test lowers discovery power** (§3).
  - **Option A's holdout is weak.** Option B is stronger but costs four
    months and its sealed fetches.
- **NAV estimator.**
  - About 7 uncovered days at the ends of the hold.
  - Modified Dietz error grows with flow intensity.
  - The return is time-weighted after partial payouts, not the depositor's
    cash.
  - The pnl and withdrawal semantics are verified only at run time.
- **Residual confounding.**
  - In discovery, net-exposure terciles do not fully remove a same-sign
    market. The window t-test cannot see one. Criteria 2–5 and the
    reported market-adjusted block carry the rest.
  - The holdout uses the market-adjusted outcome.
- **Size weighting and activity** (§6).
- **Investability.** A past outside withdrawal is not proof that deposits
  are open at S. Capacity is reported, not modelled.
- **Regime.** The discovery holds are bear or flat. The holdout is a rally.

## 8. Built before any outcome is computed

- [x] **`ct/vaults.py` (Revision 2).**
  - Loaders and the read-bound filters; features, net-exposure and
    activity SQL; eligibility.
  - Ledger flows, with distributions as outflows.
  - Entry and exit points, never before S and never after the hold.
  - Modified Dietz with **in-hold exhaustion and the thin-base stop**; the
    hold return; profit share; exit cost; **`market_adjusted`**;
    **`common_span`**.
  - The gates; imputation (median or worst case); winsorising.
  - The five criteria, with **criterion 1 = Fisher Z + window t + 3 of 4**,
    the c3 cap and the c3-n/a mode.
  - The window-and-leader Z (reported); the leader-clustered bootstrap and
    analytic SE; the placebo; **the holdout confirm on the market-adjusted
    outcome with the sign rule**.
  - The power simulation, with the market factor, window shocks, the
    noisy beta, `binom_upper`, and holdout options A and B.
  - The cutoff-filtered BTC loader.
  - **The guard: (f) scans the history, (g) checks the exact digest.**
  - The write-once manifest.
- [x] **`tests/test_vaults.py`: 101 checks pass, 0 fail.**
  - Run: `cd research/copytrade && .venv/bin/python -I tests/test_vaults.py`.
  - Log: `data/vaults/test_vaults_r2.log`.
  - New in Revision 2:
    - **a full payout, then a restart inside the hold**: the exit stays at
      the payout, and the return is identical;
    - **a payout one day into a 7-day interval**: exactly +2%, not ×7;
    - **post-hold invariance**: across 300 vaults, perturbing every grid
      point and flow after S + 56 d changes no outcome;
    - a thin-base stop, a thin base at entry, and the threshold;
    - a total trading loss is not treated as exhaustion;
    - heterogeneous windows: the Fisher Z passes, but the window t-test
      refuses;
    - the c3 cap at 10%, and c3 n/a for variants;
    - the window-shock null with 2,000 simulations: criterion 1 at 0.0150
      (Wilson ≤ 0.0213) against plain c1 at 0.0860;
    - the bear-year null with 2,000 simulations (harsh synthetic): plain
      c1 0.7550 with an unrelated control; 0.0925 with terciles; 0.0405
      after adding the window t; all five (a) at 0.0355;
    - IC 0 with 2,000 simulations: c1 0.009;
    - all simulations are deterministic across hash seeds: vault inputs
      are sorted, never taken in set order;
    - the bootstrap SE (1,000 draws) within 10% of the analytic SE;
    - worst-case imputation; the market-adjusted formula; the common span;
    - the holdout CONFIRM on the adjusted outcome, where the sign rule
      refuses a flipped effect;
    - guard (g): a digest quoted elsewhere is not accepted, and a replaced
      manifest is refused;
    - **guard (f): a row removed by a later commit is still refused.**
- [x] **Scripts.**
  - `r007_describe.py` is unchanged in Revision 2.
  - `r007_fetch.py` is unchanged; its `manifest` step writes once.
  - **`r007_power.py` was rerun** (`data/vaults/power_r2.log`).
- [x] **`r007_run.py`.**
  - Order: guard (a)–(g); the manifest check of every pinned file;
    eligible sets; gates; scores; outcomes (exhaustion, the HLP common
    span, market-adjusted); the unmeasurable gate; the criteria; one
    `results.jsonl` row.
  - `POPULATION` = `outside` and `VERSION` = `a` are the PROPOSED D2 and
    D9 values.
  - **The holdout mode is not in the runner yet.** As in 005, it is added
    by amendment before any holdout read, with D14's criteria unchanged.
    `ct/vaults.holdout_confirm` (market-adjusted outcome, sign rule) and
    `common_span(…, 14 d)` already implement and test them.
- [ ] **The owner's decisions (§9).** The REGISTERED commit recomputes this
  table and **commits a copy of `data/vaults/manifest_r1.json` with the
  registration**. `data/` is gitignored, so the copy goes to
  `registrations/007-manifest.json` at that commit.
  - The data manifest is unchanged in Revision 2: no data was added.

**Data manifest:** `data/vaults/manifest_r1.json` — 734 files, digest `c028104ebf22ee2f4fb07ffe5b66a88fa21f0cbb9c7df2d43da07033b0fe97dc`.

Its contents:
- 196 ledgers and 196 truncated allTime grids;
- 7 feature files and 7 net-exposure files;
- the candidate list and the activity table;
- 8 H7 HLP files and the vault list;
- `snapshot_accounts.parquet` and `btc_1h.parquet`;
- 315 archive day files.

**SHA-256 of the code and data the outcome depends on.** These are draft
values (2026-10-11), recomputed at the REGISTERED commit.

| file | sha256 |
|---|---|
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/vaults.py` | `8919fbe59d4e9157d50e02189cad01587c93be539c05b6be16e8398db81f40e7` |
| `r007_run.py` | `69427d000ff6baa95732dc9caf646466092f9f70685045415189c1b706b93573` |
| `ct/scores.py` | `1786a8ccfd2835c4ff058175bc779c168ef2c459bf79e050a5adbdbb0f3122e6` |
| `ct/stats.py` | `bfcc85f285dc7af28f364e2d77e0fbe04b6c2367724f4b822b7fbf6ff435d472` |
| `ct/leaders.py` | `378fbf403e8c372c2d2795b5b19c256199a1dc9360b8c6a4da55803f501ed329` |
| `ct/archive_fills.py` | `0727c03224e26ab13d70384fb1d974a47eaa51c00ef98f0ba61103a97715b418` |
| `ct/net.py` | `90092590941e805eb465e7e1f05467c05e376bf8b07fe6dbdacfdb6cf7e00006` |
| `ct/regimes.py` | `331c2bf1ee45236312785bd6a51fde2bc891e3c0fcf114386d29771dcc595191` |
| `ct/bitget.py` | `024ce24d11a4ec7366b8ea54dd719f30700f235b9f8396543f67b25b0684204d` |
| `data/vaults/manifest_r1.json` | `95de02701aa0662badf670fe632c9714282d629b937a2ed5e9bb54fc57afb8b5` |

## 9. Owner decisions — PROPOSED

| # | item | proposed |
|---|---|---|
| D1 | population base | listed **user** vaults (HLP parent and 7 children excluded), created before S |
| D2 | eligibility on [S − 91 d, S) | ≥ 20 completed trips; equity ≥ $10k at a snapshot ≤ 7 d before S; maker share ≤ 0.5; ≤ 500 fills per active day; ≥ 1 fill in [S − 7 d, S). **Primary: ≥ 1 non-leader vaultWithdraw in the lookback, 74 / 64 / 50 / 54** (orchestrator's recommendation). Alternative: the full set, 99 / 79 / 63 / 72, reported either way |
| D3 | schedule | 001's four windows; discovery reads end 2026-06-08 |
| D4 | score | the plain per-trip t; cross-regime and t / √n reported |
| D5 | NAV source | allTime pnl / AV plus ledger flows; Modified Dietz, chain-linked, unscaled |
| D6 | outcome | entry: the first grid point in [S, S + 10 d], else a snapshot, else unmeasurable. Exit: thin base (AV_a < $1k), then **in-hold exhaustion** (AV ≤ max($100, 1% of equity at S) after an outflow), then the hold end. A distribution is an outflow. Nothing after the hold is read |
| D7 | fees | 10% profit share (gate 2); exit cost = median lookback closingCost / requested; HLP none |
| D8 | criterion 1 | Spearman IC within net-exposure terciles: Fisher Z ≥ 2.33, **and window-IC t ≥ 3.182**, and IC > 0 in ≥ 3 of 4. The random-effects Z is reported only |
| D9 | criterion 2 version (owner) | **(a), recommended:** D > 0 in discovery, with the holdout CONFIRM requiring D / SE ≥ 1.64. **(b):** D / SE ≥ 1.64 in discovery too. Power and null rates for both are in §3 |
| D10 | gates | identity (flow-free ≥ 95%, flows ≥ 90%); profit share ≥ 90%; unmeasurable ≤ 5%. Any failure stops the run before any criterion |
| D11 | family | one trial; fifteen cumulative; the ring-fence in §1 |
| D12 | decision on a pass | ask the owner for the holdout read under the chosen D14 option, with its data pulls quoted first |
| D13 | decision on a fail | not detected, at the joint power in §3; **L1 closes on the archive year**; the pre-archive span stays unread for any other design |
| D14 | holdout (owner chooses **A** or **B**) | **Fixed for both:** outside-depositor population; **the market-adjusted outcome**; common entry and exit at HLP's first grid point in [S, S + 14 d] and its last ≤ S + 56 d, each vault within ±1 day. **CONFIRM** = Fisher Z ≥ 1.64, and leader-clustered D / SE ≥ 1.64, and the **sign rule** (mean window IC > 0 and D > 0), and the top quintile beats HLP (raw net, same span, c3 missing ≤ 10%). **A:** S1 2026-07-06, S2 2026-08-31, read once after 2026-10-26. **B:** A plus S3 2026-10-26 and S4 2026-12-21, end date **2027-02-15** fixed now, sealed and hashed fetches every ≤ 4 weeks, never opened before the end, read once. Orchestrator: B, if its confirm power is materially higher (§3). A CONFIRM leads to a forward paper period (a new registration); otherwise L1 closes |
| D15 | capacity | reported; about $10k in a few vaults |
| D16 | unmeasurable | within-window median imputation, counted by reason; worst case reported; > 5% stops the run |

## Open questions for the owner

1. **Holdout option A or B (D14),** from the confirm power and the
   rally-null rates in §3.
2. **Criterion 2: version (a) or (b) (D9).**
3. **Population: outside depositors or the full set (D2).**
4. **Discovery's residual exposure confound.** Should the market-adjusted
   outcome be required in discovery as well, rather than only reported?
   It would cost some power: the IC 0.2 row "market-adjusted outcome" in
   §3. It is not proposed, because discovery's question is the raw
   depositor return; the holdout requires it.
