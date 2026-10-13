# Second independent review of 007 Revision 1 (vault deposit skill), 2026-10-11

Scope: `registrations/007-vault-deposit-skill.md` (Revision 1), `ct/vaults.py`, `r007_run.py`,
`r007_describe.py`, `r007_fetch.py`, `r007_power.py`, `tests/test_vaults.py`, the R1 logs in
`data/vaults/`, and the ledger entries `000c` and `000e`. Brief: (1) check every first-review
finding against code and text; (2) look for new flaws.

**What this reviewer did.**
- `r007_run.py` was not run. No score, NAV change, return or outcome was computed on real data.
- `tests/test_vaults.py` was run: 89 PASS, 0 FAIL, exit 0.
- The SHA-256 values of all 11 files in the §8 table were recomputed. All match the files on disk.
- The logs were read: `describe_r1.log`, `power_r1.log`, `describe/power_r1.json`,
  `test_vaults_r1.log`, `manifest_r1.log` and `fetch.log`.
- Synthetic-only probes were written in the reviewer's scratch area. They read no real data and
  use no real identities or betas. They are summarised in the appendix.

**Outcome contact by Revision 1.** None was found.
- `describe_r1.log` prints pre-S quantities only: equity at S as threshold counts, lookback
  net-exposure quantiles, gross leverage and hold length. It also prints grid and snapshot
  *presence* counts in [S, S + 10 d].
- `power_r1.log` prints BTC log returns over the four holds. These are market data, not a vault
  outcome.
- `manifest_r1.log` prints a digest. `test_vaults_r1.log` is synthetic.
- `activity.json` holds in-hold fill counts per day. They are stored, and only their row count was
  printed.

---

## 1. First-review findings: verification against code

| finding | status | where |
|---|---|---|
| **B1** closure | **Fixed, with a residual (see new M-b).** A distribution is an outflow: `ct/vaults.py:352-355`. Closure is behavioural: `:414-429`. The distribution exclusion is gone from `eligible`: `:276-301`. The 7-day activity rule is at `:297`, fed by `last_fill_before(act, S)` at `r007_run.py:110`. The pre-S fallback is deleted: `measure_points` sets `lo = max(S, entry_lo)` at `:476` and asserts at `:499`. Tests: `no_pre_s_checks`, `distribution_outflow_checks`. §3's coverage claim is restated: 0 unmeasurable. | |
| **M1** market factor | **Fixed** for the β channel. The simulation draws `β_v·m_w` at `:981-987`. Criterion 1 uses net-exposure terciles: `window_ic` at `:789-791` with `control = nexp`, set at `r007_run.py:319`. The clustered Z is required at `:872`. **But the clustered Z it adds is miscalibrated (new M-a).** | |
| **M2** criterion 3 span | **Fixed.** Returns are unscaled by default (`hold_return(scale=False)`, `:503`). Criterion 3 uses the same interval: `r007_run.py:163-168`, with HLP's points at `:306-308`. Residual: HLP uses midpoint flows (`flows=None`, `:306`), while vaults use ledger-timed flows. §4 calls this "same estimator" at line 424 (minor 6). | |
| **M3** holdout and read bound | **Fixed.** `READ_END` is at `:52`. Bounded loaders: `r007_run.py:61-76`. Snapshots are filtered at `:89`, `Btc(end_ms=READ_END)` is at `:280`, and the betas use only intervals inside the bound (`:264`). The holdout dates are at `:57`. **New design problems: M-c.** | |
| **M4** gates | **Fixed.** `identity_checks` covers flow intervals (`:606-618`). The withdrawal field is chosen by agreement (`:621-636`). The commission check is at `:639-653`. The runner calls these before any criterion at `r007_run.py:288-301`. | |
| **M5** investability | **Fixed as proposed.** `outside_depositor` is at `:268-273` and `POPULATION='outside'` at `r007_run.py:33`. The §6 sentence is corrected (registration lines 67-70). | |
| **M6** criterion 2 untested | **Fixed as an owner choice.** Versions (a) and (b) are at `summarise:873-881` and in D9. The holdout requires D / SE ≥ 1.64 (`:926`). | |
| m1 bitget unhashed | Fixed: `GUARDED_CODE` (`r007_run.py:37-38`) and §8 line 645. The import-closure test passes. | |
| m2 archive unpinned | Fixed: `r007_fetch.pinned_files`. The manifest holds 734 files, 315 of them day files. | |
| m3 BTC holdout rows | Fixed: the `Btc` query filters at `:1095`. | |
| m4 ledger not re-filtered | Fixed: `r007_run.py:61-63`. | |
| m5 gate reads pre-archive | Fixed: `r007_run.py:289` reads [2025-07-28, READ_END). | |
| m6 manifest regeneration | Fixed: `write_manifest` refuses an existing file (`:1207-1212`), and guard (g) checks the digest (`:1192-1196`). Partial: (g) accepts *any* backticked digest in the file, and the manifest is still uncommitted (minor 3). | |
| m7 results row removable | Partial. HEAD is checked (`:1189-1191`), but a later commit that deletes the row re-enables the run (minor 3). | |
| m8 leader clustering | Fixed: `:748-770` and `:794-839`. | |
| m9 near-empty vaults | **Not fixed as asked.** Thin-base intervals are only counted (`:523-524`), and their returns still enter the outcome. Folded into M-b. | |
| m10 exit cost | Fixed: `exit_cost` (`:550-564`) uses pre-S, non-leader withdrawals. | |
| m11 ≥ $100k | Fixed: `r007_run.py:233`. | |
| m12 attenuation in power | Fixed in the code. The quoted numbers do not match the log (minor 2). | |
| m13–15 ring-fence, rationale, H7 | Fixed: registration lines 147-150, 127-135 and 114-118. | |
| m16 exposure record | `000e` exists. It does not list Revision 1's own reads (minor 7). | |
| m17 activity | Fixed: `score_sharpe` (`r007_run.py:127`) and the IC of n_trips (`:246`). | |

**Probes requested by the brief:**
- **Entry rule.** No route to a pre-S value was found.
  - Grid entry: `lo ≥ S`.
  - Snapshot entry: the query is bounded to [S, S + 10 d] (`r007_run.py:88-91`).
  - Interval flows lie in (a, b], with a ≥ S.
  - The c3 window starts at `he − 1 d`, which is at least S + 2 d.
  - Exit cost and the closure threshold use only pre-S data, which is legitimate.
- **Imputation.** It is not neutral for every criterion.
  - Criterion 1: neutral, because it uses ranks.
  - Criterion 2: neutral to within (mean − median)/n, under 0.1 pp.
  - Criterion 3: imputed rows are excluded, not imputed.
  - Criterion 4: pulled toward the median.
  - Missingness from `capital base` failures carries outcome information (minor 4).
- **Behavioural closure and eligibility.** Eligibility reads only pre-S data: ledger rows before S
  (`r007_run.py:108`) and `last_fill_before(act, S)`. Closure is outcome-only. **But closure reads
  fills and ledger events up to 2026-06-08, which is after the hold end for windows 1–3 (M-b).**
- **Guard and manifest.**
  - The runner hard-codes `manifest_r1.json` (`r007_run.py:30`), and that file's hash is in the
    §8 table. Pointing the runner at the old `manifest.json` therefore means editing a hashed
    file, and (e) fails.
  - The old digest (`e4f153f7…`) is not quoted in the registration.
  - The overwrite refusal and the HEAD check are real, and both are tested.

---

## 2. New findings

### BLOCKER

None.

### MAJOR

#### M-a. The random-effects clustered Z is anti-conservative. Its null rejection tends to about 5% at nominal 1%.

- **Where.**
  - `window_leader_z`, `ct/vaults.py:832-837`.
  - Registration §3 lines 307-313, §5 item 1, D8 and open question 3.
- **Why.**
  - Whenever τ̂² > 0, sd² = sd²_leader + τ̂². Because sd_leader ≈ 1 and within ≈ 1, this is about
    the sample variance s² of the four z_w.
  - So Zc = (Σz_w/2)/s = 2·z̄/s, which is exactly the Hartung–Knapp t statistic on three degrees
    of freedom, compared against the *normal* 2.33. The t₃ critical value at 0.99 is 4.54; 2.33
    is close to the t₃ quantile at 0.95 (2.35).
  - τ² is floored at 0, which is the anti-conservative direction. There is no upper-bound
    correction.
- **Evidence (synthetic).** The plain criterion 1 is used together with the robust rule.

  | true τ² (z units) | 0 | 0.25 | 1 | 2 | 4 | 9 |
  |---|---|---|---|---|---|---|
  | plain criterion 1 | 1.0% | 1.9% | 4.9% | 8.3% | 13.4% | 18.8% |
  | **criterion 1 with R1's τ² form** | 0.7% | 1.2% | **2.4%** | **3.5%** | **4.3%** | **4.9%** |

  - Running the real `summarise`/`window_leader_z` on synthetic windows (74/64/50/54, shared
    leaders, window-common IC shocks; 600 sims, 100 bootstrap draws) gives 1.5%, 2.0% and 3.7% at
    τ² ≈ 0.3, 1 and 4.
- **Why the draft did not see it.**
  - Heterogeneity was simulated only through the tercile-controlled β channel, so plain criterion 1
    stays at 0–1% in the random-market rows.
  - Each null row has 300 sims with 100 bootstrap draws, so "0.00" has a 95% upper bound near 1%.
  - Same-sign shocks (the bear-year null) cannot be detected by *any* window clustering. Only the
    exposure control addresses them.
- **Fix: pick one rule, rerun `r007_power.py`, and add a window-shock null row with ≥ 2,000 sims.**
  - Power figures are for criterion 1 at a per-window effect where the plain criterion passes
    0.855 and R1's form passes 0.765, which matches IC 0.2 in §3.
  - Joint figures scale R1's 0.54 by the ratio of criterion-1 power. They are approximate.

  | rule (conjoined with plain Z ≥ 2.33 and 3 of 4) | worst-case null | criterion-1 power | ≈ joint (a) at IC 0.2 |
  |---|---|---|---|
  | R1: τ² form against 2.33 | 4.9% | 0.765 | 0.54 |
  | **HK t₃ ≥ 3.18 (one-sided 0.975)** | **≤ 2.35%, exact under any τ²** | **0.596** | **≈ 0.42** |
  | hybrid: 2.33 if τ̂² = 0, else 3.18 | 2.4% | 0.628 | ≈ 0.44 |
  | hybrid with 4.54 (0.99) | 1.8% | 0.527 | ≈ 0.37 |
  | HK t₃ ≥ 4.54 (0.99) | ≤ 0.93% | 0.354 | ≈ 0.25 |
  | τ² at its 80% upper bound | < 1.3% | 0.435 (at 1.7) | ≈ 0.31 |

  - **Recommended:** HK t₃ at 0.975. It is exact and needs no τ² estimate. The joint null stays
    below 1% because criteria 2–5 are also required. The cost is about 0.12 of joint power at
    IC 0.2.
  - Update §3, D8, open question 3 and the "≤ 1%" sentence on lines 134-135.
  - Separately, a **market-adjusted outcome** removes the same-sign residual that no window
    clustering can reach: net − β_lookback × the BTC hold log return, with β from the pre-S net
    exposure (open question 4). Require it for criterion 1 or report it as a gate.

#### M-b. Closure depends on post-hold data. A full payout followed by any later activity falls into an undefined Modified Dietz regime.

- **Where.**
  - `r007_run.py:316`: `last_fill` is the maximum over all activity before 2026-06-08.
  - `ct/vaults.py:419-429`: closure requires silence until the read bound.
  - `:523-524`: thin-base intervals are counted, not excluded.
  - `modified_dietz:449-462`.
  - The tests cover only payouts of 20–80% (`test_vaults.py`, `distribution_outflow_checks`).
- **Failure.**
  - (i) The silence horizon after the hold differs by window: about 168 days for window 1 and 0 for
    window 4. Two identical in-hold paths therefore get different outcome treatments depending on
    whether the leader later restarted, which is outcome-correlated behaviour. Closure is not
    look-ahead into eligibility, but the outcome is not point-in-time.
  - (ii) Suppose a vault pays out about 100% and later trades or takes a deposit before
    2026-06-08. 000e found 16 vaults with deposits after a distribution. Then:
    - the interval containing the payout has base ≈ AV_a·(t_p − t_a)/(t_b − t_a), so a payout
      1 day into a 7-day interval scales the return by about 7;
    - later intervals run on a dust base;
    - the outcome can be extreme, or "unmeasurable: capital base" (outcome-informative), and is
      then imputed at the median.
    - This is the B1 case ("pays out and keeps trading") at its extreme.
- **Fix.**
  - Define exhaustion inside the hold: the depositor exits at the first in-hold grid point where
    AV ≤ max($100, 1% of equity at S) following an outflow, regardless of later activity. A later
    restart is new money the depositor does not own.
  - An interval with base < $1k ends the measurement at its start point.
  - Add a synthetic test of a 100% payout followed by a restart.
  - Count unmeasurable rows by reason.

#### M-c. The holdout design is weak and does not cohere with discovery's own confound controls

- **Where.** D14, `holdout_confirm` (`:920-928`), §3 lines 336-371, §7 line 546.
- **Problem (i): one regime.**
  - The holdout has 2 windows in one regime, which §7 says is "one rally".
  - It has no clustered Z, no sign-consistency rule and α 0.05.
  - In a synthetic null where the score is tilted toward net exposure and the market moves the same
    way in both windows, CONFIRM fires 3.3–4.6% at corr 0.2 and **6.5–11.8% at corr 0.4**. The
    true-null rate is 1.2–1.3%. The market moves are hypothetical, and each row is 1,500 sims.
  - Net-exposure terciles are on, as in the runner. They are not enough in one regime.
- **Problem (ii): power.** P(confirm) is 0.18–0.27 at IC 0.2, and end-to-end it is 0.09–0.13.
- **Problem (iii): feasibility.**
  - The discovery grid is "effectively common" in phase across vaults.
  - A 14-day fresh grid would then be **all-or-nothing per window**, not "about 30%". The 5% stop
    would void a whole window.
  - A 14-day entry window fixes coverage, but the entry dates and spans then differ by up to 13
    days across vaults. That matters under a common market factor.
- **Minimal honest design (owner options, fixed now).**
  1. Require the market-adjusted outcome (M-a) at CONFIRM.
  2. Entry rule: "the first grid timestamp in [S, S + 14 d] *common to HLP's grid*". Every vault and
     HLP then share one entry and one exit, chosen from timestamps only.
  3. **Add forward windows** S3 = 2026-10-26 and S4 = 2026-12-21, and fix the end date now
     (2027-02-15). Read once at that date, with no interim look. Every ≤ 4 weeks, run a sealed,
     prospective fetch of vaultDetails (including the finer `month` portfolio period) and of the
     ledgers. Hash each fetch and leave it unread, so the coarse-grid risk goes away for S2–S4.
  4. Synthetic gain: 4 windows against 2 raise P(confirm) at IC 0.2 from 0.34 to 0.47 (about
     ×1.4) at the same null of 1.2%. On the draft's sets, expect roughly 0.23 → about 0.33. The
     cost is four months.
- **Alternative.** Keep 2 windows and say plainly that the read is a weak confirm that cannot
  separate skill from a long tilt.

### MINOR

1. **Outcome variants always fail criterion 3.** `rows_for` sets `c3=None` for non-primary variants
   (`r007_run.py:183`), and `summarise` drops None (`ct/vaults.py:864`). As a result, scaled, av,
   midpoint, other-field, no-exit-cost and gross never "pass all five". Compute c3 per variant, or
   report criterion 3 as n/a.
2. **The quoted numbers do not reproduce from the cited logs.** Each pair below is §3/§8 against
   `power_r1.log`/`power_r1.json` or `test_vaults_r1.log`:
   - joint (a) at IC 0.2: 0.55 against 0.54 (0.540);
   - (b): 0.36 against 0.35;
   - full set: 0.61 against 0.603;
   - IC 0.1: 0.15 against 0.137;
   - resample: 0.32 against 0.29;
   - holdout low/0.88: 0.18 against 0.173;
   - end-to-end: 0.13/0.10 against 0.12/0.09;
   - harsh synthetic: 0.82/0.15/0.085/0.06 against 0.760/0.135/0.060/0.045, and "6%" against 4.5%.

   Quote the logged values.
3. **Guard gaps.**
   - (f) checks the working copy and HEAD only, so a later commit that removes the row re-enables
     the run. Use `git log -S'007-vault-deposit-skill.md' -- results.jsonl` over the history.
   - (g) passes on any backticked digest. Match the digest on the "Data manifest" line.
   - Commit a copy of `manifest_r1.json` at REGISTERED.
4. **Imputation.** Add a worst-case variant (the window minimum, or −100%) and split the counts by
   reason (`r007_run.py:202`).
5. **Missing criterion-3 values have no bound.** Criterion 3 can rest on a subset of the top
   quintile. Report the share that is missing, or stop above 5%.
6. **"Same estimator" (§4 line 424).** HLP uses midpoint flows while vaults use ledger-timed flows.
   State this.
7. **000e omits Revision 1's own reads.** These are the grid and snapshot presence in [S, S + 10 d],
   the in-hold grid-point counts (`grid_points_in_hold_min`), and the activity table that stores
   in-hold fill counts. §7 lists them; the ledger entry should too.
8. **Null precision.** The null rows use 300 sims and 100 bootstrap draws. Use at least 2,000 sims
   for every null row, and report a binomial upper bound next to each "0.00".

---

## 3. Verdict

**Not ready for the owner yet.** There is no blocker, and every first-review finding is fixed in
code, apart from m9 and parts of m6 and m7. Revision 1 introduces two code-level problems that the
owner should not be asked to decide on:

- **M-a:** the clustered Z that answers M1 is calibrated at roughly α 0.05, not 0.01, under
  window heterogeneity.
- **M-b:** the closure rule leaves the "full payout, then restart" case undefined, and it depends
  on post-hold data.

**After M-a and M-b are fixed, `r007_power.py` is rerun, and the §3/§8 numbers and hashes are
restated, the draft is ready for the owner.** M-c (holdout length, the market-adjusted outcome at
CONFIRM, and the entry-window rule) goes to the owner as a decision with the options above.

---

## Appendix: synthetic probes (no real data)

These scripts live in the reviewer's scratch area (session-local, not in the repo).

- **`probe_tau_a.py` and `probe_tau_b.py` (z-level model).**
  - Model: z_w = μ + u_w + e_w, with u_w ~ N(0, τ²) and e_w ~ N(0, 1).
  - 200k null sims per τ² for part A, and 100k null sims per τ² plus 50k power sims per μ for
    part B.
  - They compare R1's τ² form with HK on t₃ (0.99 and 0.975), the hybrids, and upper-bound τ².
- **`probe_tau_c.py`.** Runs the real `V.summarise` on synthetic windows (74/64/50/54, 12 vaults on
  6 shared leaders per window, a window-common IC shift a_w ~ N(0, s_a)); 600 sims per level, 100
  bootstrap draws.
  - The mean sd_leader was 1.08–1.10.
  - The mean τ̂² tracked the planted τ².
- **`probe_holdout_d.py`.** Runs `V.draw_windows` with `V.holdout_confirm` on synthetic identities
  and betas, with hypothetical one-sign market moves, at 1,500 sims.
  - It compares 2 and 4 windows at IC 0, 0.1, 0.2 and 0.3.
  - It runs the rally null at corr(score, β) of 0.2 and 0.4.
