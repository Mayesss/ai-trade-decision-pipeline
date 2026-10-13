# Independent review of 007 DRAFT (vault deposit skill), 2026-10-11

Reviewer brief: adversarial review of `registrations/007-vault-deposit-skill.md`, the code in
`ct/vaults.py`, `r007_describe.py`, `r007_fetch.py`, `r007_power.py`, `r007_run.py` and
`tests/test_vaults.py`, and the logs in `data/vaults/`. No score, return or NAV change was
computed. `r007_run.py` was not run. The synthetic tests were run (`tests/test_vaults.py`):
71 PASS, exit 0. The §8 SHA-256 table matches the files on disk today.

**Reviewer exposure (to be ledgered with the draft's own count exposure).** Three structural
checks were run on the pinned ledgers, allTime grids and vault list. They read only event types,
event times, ledger key names, the presence of `user` on `vaultWithdraw`, grid timestamps and
the list's `leader` field. No amount, AV level, pnl level or change was read or printed. The
results are quoted below. One timestamp-only read checked that `btc_1h.parquet` holds rows
after the cutoff.

---

## BLOCKER

### B1. "Exit at the first vaultDistribution" and "no distribution before S" rest on a closure semantics that the pinned ledgers contradict. One row already breaks "never a point before S".

- **Where.**
  - Registration §4 item 3 ("If the vault's first vaultDistribution at or after S falls inside the hold, the position ends there … the realised value"; "Vaults that distribute or close stay in the sample").
  - D2 ("no vaultDistribution on the vault's own ledger before S").
  - D6.
  - §3, lines 162–163: "Every eligible vault has ≥ 4 grid points in every hold it enters, so the `pre_s_start` fallback is never needed at these dates".
  - §4 item 1: "**Never a point before S**".
  - Code: `ct/vaults.py:223-224` (eligibility), `:365-371` (closed → truncate), `:373-379` (pre-S fallback), `:412-419` (closed rows unscaled), and `r007_run.py:132`.
- **Evidence (counts and times only).**
  - Of the 196 pinned ledgers, 61 have at least one `vaultDistribution`.
  - **15 have two or more.**
  - **16 receive a `vaultDeposit` more than a day after their first distribution.** At least a quarter of the "closures" are therefore partial distributions, and the vault lives on.
  - The scout already warned of this: "not proven to be one, and partial distributions may exist", and one vault traded 50 days after a distribution.
  - In-hold first distributions occur in 26 vault-windows, at days 2, 6, 6, 7, 7, 9, 11, 14, … after S.
  - **One of them (day 2) has no grid point in [S, t_dist).** `hold_points` will therefore prepend the last pre-S point (about S − 4 d), which puts lookback NAV into the outcome. That is the look-ahead the draft says cannot happen. The §3 claim is false.
- **Failure scenarios.**
  - (a) A vault that pays out part of its profit on day 6 and keeps trading gets a 6-day, unscaled return. That return sits in a cross-section of 56-day scaled returns. The truncation time is chosen by the leader after seeing performance, so the outcome is endogenous.
  - (b) Twenty-eight vault-windows (9 / 7 / 6 / 6) are excluded as "closed before S" while they were trading with at least $10k at S.
  - (c) Lookback performance enters one outcome directly, and it correlates mechanically with the score.
- **Fix.**
  - Define closure behaviourally and point-in-time: a distribution after which the vault has no fill and no `vaultDeposit` (ledger times and fill timestamps only, pre-S for eligibility).
  - Treat every other distribution as an ordinary outflow. `ledger_flows` already signs it −usdc, and Modified Dietz then gives the correct total return.
  - Exit only at a true closure.
  - Never prepend a pre-S point. A row with fewer than two points in [S, exit] gets no outcome. It is counted, and a reported variant imputes it conservatively (worst decile, or −100%).
  - Add a synthetic test for "distribution before the first in-hold grid point".
  - Re-state the §3 counts.

## MAJOR

### M1. The null calibration and the power model ignore the market factor that the draft itself uses as the dispersion model

- **Where.** §3, "Dispersion: SD = max(5%, k × leverage × BTC 56-day SD)"; `ct/vaults.py:686` (`x = … sd·(a·z + √(1−a²)·n)`, with `n` independent across vaults); `r007_power.py:140`; §5 placebo ("scores … shuffled within every window").
- **Failure scenario.**
  - Vault returns in a window share one BTC draw, scaled by each vault's net exposure. With 4 windows there are 4 market draws.
  - Suppose the score correlates with net exposure, for example high-t vaults run lower or more short-biased leverage in a bear year. Then IC_w moves by about corr(score, β) × sign(BTC_w) in every window, and the Fisher Z treats about 70 vaults as independent.
  - The long-bias tercile control removes direction, not size × direction.
  - The within-window shuffle breaks the score–β link, so the placebo cannot detect this. The stated null rate (1%) and the power (0.67) both come from a model with no common factor.
- **Fix.**
  - Simulate x = β_v·m_w + idiosyncratic. Take β_v from lookback net leverage: the snapshot `net_notional / account_value`, pre-S. Draw m_w from BTC's 56-day SD. Report the c1 null rate at corr(score, β) = 0.2 and 0.4.
  - Change the IC control to terciles of lookback **net exposure** (long-bias × leverage, or snapshot net leverage), or require c1 also on returns residualised on that pre-S β.
  - Require the vault-clustered Z (already computed, `r007_run.py:167`) ≥ 2.33 as well as the plain Z.

### M2. Criterion 3 compares mismatched, extrapolated spans against a jump-dominated benchmark

- **Where.** §4 item 4 and HLP paragraph; `r007_run.py:243` (`hold_return(hlp_grid, None, …)` with scale = True); `ct/vaults.py:419`.
- **The spans.**
  - Vaults are measured over about [S + 3, S + 52] and scaled by 56/49.
  - HLP is measured over [S + 3, S + 45] on its 14-day grid and scaled by 56/42.
- **Failure scenario.**
  - H7 shows that HLP's return is jumps: one day carries 91% of the year, and the jump of 2026-01-31 (+7%) sits inside hold 2.
  - Log-scaling by 1.33 turns one jump into drift, so the benchmark for hold 2 is overstated by about a third. Hold 1 is overstated in the negative direction.
  - The two sides also cover different calendar days (S + 45 → S + 52 is in the vaults but not in HLP).
- **Fix.** For criterion 3, measure each vault and HLP over the **same grid interval**, HLP's [first, last] in-hold points (the 7-day vault grid shares those dates within hours), and **unscaled**. Keep the scaled version as a reported variant.

### M3. The holdout is not clean and its power is optimistic

- **Where.** §3 "Holdout power"; D14; `r007_power.py:113`; `r007_run.py:208`.
- **Failure scenario.**
  - Window 2026-06-08's hold [06-08, 08-03) is 22/56 discovery-year days.
  - The discovery run already reads NAV changes on 06-08 → 06-30: `btc_betas` uses every grid interval below the cutoff (line 208), and the gate uses every interval (line 116).
  - Its lookback [03-09, 06-08) is 56/91 days of discovery window 4's hold.
  - P(confirm) 0.70 assumes 68 eligible in both windows, the same vault set, no NAV attenuation, and that a fresh `vaultDetails` fetch keeps 7-day steps. The scout found that allTime is capped near 100 points and coarsens to 14 days with age (HLP: 100 points, 14 d).
  - The end-to-end figure of 0.47 inherits all of these assumptions.
- **Fix.**
  - Start holdout holds at or after 2026-07-01. Lookbacks may stay in discovery.
  - Cap every discovery-run read of grids and ledgers at the last discovery hold end (2026-06-08).
  - Recompute holdout power with the 0.93 attenuation and a 14-day-grid case, and state the feasibility conditions in D14: snapshot coverage near 08-03, and the 008 pull dates.

### M4. The pnl-semantics gate does not test the intervals where the semantics matter. The withdrawal fields are ignored.

- **Where.** §4 "Mechanics gate"; `r007_run.py:110-123` (lines 119–120 skip every interval with a flow); `ct/vaults.py:255,272-275`.
- **The fields.** The ledger keys show that `vaultWithdraw` carries `basis`, `closingCost`, `commission`, `netWithdrawnUsd`, `requestedUsd` and `user`.
- **Failure scenario.**
  - On flow-free intervals, ΔAV = Δpnl holds almost by definition.
  - What is untested is whether Δpnl on an interval with flows equals ΔAV − Σ ledger flows. That covers which withdrawal amount actually leaves the AV, and whether forced-close slippage (`closingCost`, borne by the withdrawer) sits in the vault's pnl.
  - A wrong convention biases vaults with heavy withdrawals, which are the performance-chasing, outcome-correlated ones.
- **Fix.**
  - Extend the gate to intervals with flows: |ΔAV − Δpnl − Σf| ≤ tol·AV in at least X%. Pre-register the rule that picks `requestedUsd` or `netWithdrawnUsd` from which one reconciles.
  - At run time, on **lookback** rows only, check that commission ≈ 0.10 × max(requested − basis, 0), so that D7 is verified, not assumed.

### M5. The investability proxy is weaker than the data allows, and §6 is wrong about the data

- **Where.** §6: "vaultDeposit rows carry no depositor field, so leader and third-party deposits cannot be told apart"; Open question 6; `r007_run.py:176,258-259`.
- **Evidence.** `vaultWithdraw` carries `user`. The number of eligible vaults with a non-leader withdrawal in the lookback is 68 / 58 / 48 / 54 of 92 / 73 / 59 / 68. About one in five eligible vaults shows no third-party depositor before S. These are plausibly leader-only vaults, often closed to deposits.
- **Failure scenario.** The top quintile of per-trip t could be leader-only vaults that a depositor cannot enter. A pass would then send a paper period to vaults it cannot buy.
- **Fix.** Make "at least one non-leader `vaultWithdraw` or `vaultDeposit`-plus-third-party evidence in [S − 91 d, S)" either an eligibility filter or a sixth criterion (the pass holds in that subset). Correct the §6 sentence.

### M6. Criterion 2 is not tested at any confidence, in discovery or in the holdout

- **Where.** §5 c2 "D > 0"; D14 CONFIRM ("top minus all > 0"); `ct/vaults.py:566,611`.
- **Failure scenario.** The power log shows c2 passes 45% at IC 0. An IC carried by the bottom of the ranking (loser persistence, the 001 reading rule) passes c2 about half the time and then CONFIRMs with no SE anywhere. "The top quintile beats the average" is the economic claim, and it is never tested.
- **Fix (owner).** Keep D > 0 on discovery, but require pooled D / SE ≥ 1.64 on discovery plus holdout at CONFIRM. Alternatively, require the top-half IC (already computed) > 0 in discovery.

## MINOR

1. **Guard: unhashed import.** `ct/regimes.py:20` imports `ct/bitget.py`, which is not in `GUARDED_CODE` (`r007_run.py:30-31`). Add it to the guard and to the §8 table.
2. **Guard: unpinned archive.** The scores and the hold diagnostics read `data/pruned/…/fills` (`r007_run.py:84-105`). It is not pinned. Pin a digest of the day files 2025-07-28 → 2026-06-07, or of a per-vault extract.
3. **BTC holdout rows.** `btc_1h.parquet` holds **2,376 hourly closes ≥ 2026-07-01**, which contradicts §2 ("closes < 2026-07-01"). `Regimes()` loads them all (`ct/regimes.py:54-58`). Truncate at load, or restate §2.
4. **Ledger not re-filtered at run time.** `load_ledger` (`r007_run.py:57-58`) relies only on the fetch-time check. Filter t < cutoff there, the same way `grid_from_alltime` does.
5. **Gate reads pre-archive intervals.** The gate reads intervals before 2025-07-28 (`r007_run.py:116`), which conflicts with D13 ("pre-archive span stays unread"). Bound it to [2025-07-28, 2026-06-08].
6. **Manifest regeneration is not enforced.** `r007_fetch.py:169-180` overwrites `manifest.json` unconditionally. §8 says the table is recomputed at REGISTERED, so a regeneration before then goes unnoticed, and `data/vaults/` is gitignored. Make `manifest` refuse when the file exists. Commit `manifest.json` (it holds hashes only). Have the guard compare `m['digest']` with the digest literal in §8.
7. **Result row can be removed.** The guard's (f) reads the working copy of `results.jsonl`, so a deleted 007 row re-enables the run. Also require `results.jsonl` to be clean against HEAD.
8. **Leader clustering.** Within a window, 19 / 13 / 8 / 7 eligible vaults share a leader (up to 4 per leader). Cluster the bootstrap and the Z by leader (open question 5: yes).
9. **Near-empty vaults.** A near-empty capital base (`ct/vaults.py:349-352`) can produce extreme interval returns. Use the gate's own floor (AV_a < $1k gives no return for that interval) and count such cases.
10. **Exit cost.** `closingCost` exists. Haircut net returns by each vault's median lookback `closingCost / requestedUsd`, or report it.
11. **Capacity.** Add "equity at S ≥ $100k" as a reported variant under all five criteria. Today the result is about a population where $10k is 10–100% of the vault.
12. **Power wording.** §1 and §3 quote 0.67 / 0.47 without the 0.93 attenuation. State the attenuated figures.
13. **Ring-fence vs holdout.** §6 / D11 say a variant follow-up is "decided on the holdout only", but D12/D14 allow one holdout read, and only after a primary pass. State that a variant can never unlock or consume the holdout.
14. **Threshold.** z 2.33 is 005's convention, not a family correction. At 15 looks, P(at least one false discovery pass) is about 14%. It is acceptable only because a pass buys the holdout read. Say so in §1.
15. **H7 leak.** H7 published HLP's per-month path, so criterion 3's bar was known: roughly −3%, +5%, 0% and −0.5% over the four holds. That is small, and M2 matters more. Add "the 56-day schedule was kept knowing HLP's path" to the disclosure.
16. **Exposure record.** The in-hold event counts (flows and distributions per window), the reviewer checks above and the ledger key names are post-S behaviour. Ledger them as a 000-series count exposure.
17. **Activity, not skill.** The plain t scales with √n, so 001's persistence partly reflects persistent activity. The size-weighting diagnostic (§6) should also report the IC of n_trips against net return.

## Verdict

**Not ready**, because:

- the outcome definition and the eligibility rule treat partial distributions as closures, which the pinned ledgers contradict (B1);
- one row already uses a pre-S point;
- the null calibration omits the common market factor that the draft itself assumes (M1).

**Ready for the owner after fixes B1, M1, M2, M3 and M4**, with M5 and M6 put to the owner as decisions. After the fixes, the §3 counts, the coverage, the power and the §8 hashes must be recomputed.
