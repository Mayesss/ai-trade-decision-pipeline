# 008 Revision 1 — second-round independent review, 2026-10-13

Reviewer brief: verify every first-round blocker and major in code (not in the prose), probe the
mechanisms Revision 1 introduced, and find anything that still blocks registration. Read-only on every
file of the draft. `r008_run.py` and `r008_fetch.py` were never run against real data; nothing was
fetched; no commit was made in the real repository.

## How this was checked

- **Tests.** `cd research/copytrade && .venv/bin/python -m tests.test_r008` → **"all checks passed (120)"**
  (56 s).
- **Hashes and reuse.** The §11 hashes of `ct/__init__.py`, `ct/crowd008.py`, `r008_run.py`,
  `r008_fetch.py` and `008-manifest.json` match the disk. `git diff --stat efefc7f HEAD` over the 14 reused
  modules and `p5_run.py` is empty.
- **Code read in full:** `ct/crowd008.py`, `r008_run.py`, `r008_fetch.py`, `r008_power.py`, and the
  factor, decision, guard and bypass sections of `tests/test_r008.py`. Read in part: `ct/crowd.py`
  (`tilts`, `quintiles`, `long_short`), `p4_features.features`, `ct/leaders.Equity`, `p5_population`.
- **Probes** are in `research/copytrade/scratch_review008r1/`, with their outputs in `*.out`. All are
  synthetic, use throwaway git repos, and mock every hook.
  - `probe_stats.py`: an independent OLS + CR1 implementation of C1–C3. It agrees with
    `crowd008.confirm` to 9e-16 over 40 draws, with identical decisions. It reproduces §3, runs nulls the
    revision did not try, and tests persistence mitigations. 8,000 draws per cell.
  - `probe_fix2.py`, `probe_fix_1123.py`, `probe_crit.py`: the fix for B1 below, at both ends, and the
    critical value needed to absorb persistence.
  - `probe_guard_r1.py`: re-runs of the first-round bypass probes against the revised API (`split`,
    `delisted`, `inject`, `bytes`, `erase`), plus new probes:
    - `mockreal`: mock hooks on the real tree;
    - `commitfail`: the runner's own commit fails;
    - `only`: does `git commit --only` sweep in staged changes;
    - `shadow`: what check (d) misses;
    - `persist`: the end state for persistent refusals;
    - `env`: the environment fingerprint.

    For "the real repository" these probes use a stand-in temp repo, with `R.HERE` and `R.REAL_DATA`
    pointed at it.

**Reviewer's real-data reads (for the ledger).** None of HL, Bitget or any price/position data. Only
repository metadata:
- the absence of `data/r008`;
- `git check-attr` on `results.jsonl` (no attributes);
- `.gitignore` rules;
- the target of the venv interpreter symlink (Homebrew `python@3.14`, 3.14.8);
- the absence of git hooks and of commit signing;
- the subjects of the last 3 commits touching `results.jsonl`.

No quantity dated on or after 2026-07-01 was computed or seen.

---

## First-round findings: status

| finding | status | evidence |
|---|---|---|
| **A-B1** one pooled BTC β | **Fixed** for BTC. | `beta_cells`/`design` give one slope per (window, month) cell: 3 cells at 09-28, 5 at 11-23. Independent reproduction: N10 combined null 0.054 (draft 0.047); base size 0.053; power 0.287 / 0.137 (draft 0.295 / 0.139). **The same mechanism is still open for the alt and reversal loadings: see B1 below.** |
| **A-M1** alt factor omitted | **Fixed** with a constant loading. | EW alt index in C1 (`day_factors`, `design`). N4/N5 hold. Cohort-specific alt loading does not hold (B1). |
| **A-M2** vacuous bar, labels | **Fixed.** | `economic` uses `SHRINK * alpha - cost`. The bar edge is 2 × (7.29 + 0.82) = 16.2 bp/day (tested). Labels renamed. Both confirm labels lead to the same cost-reduced registration. No label trades. |
| **A-M3** earlier looks | **Fixed.** | §13 items 1–10 and §1 "what is not independent"; `000l` cited. |
| **A-M4** tail | **Fixed.** | One scenario. Arithmetic re-checked: 0.5·(−25) − 0.5·(−15) = −5 %, plus −1 % for a gap; week −8.7 to −9.0 %; month −12 to −15 %; Kelly 7.8×. |
| **B-B1** gate opened by a throwaway root | **Fixed for the reported paths.** | `split`: `run(root=throwaway, data_dir=real)` refuses with (B1). `compute(real)` outside `run` refuses (read lock). The tests cover a partial mock dict, real hooks from a throwaway repo, a direct real-hook call, and `-O`. Symlinked sub-trees still pass, which is B-m10, accepted. **New hole: mock hooks on the real tree (M1 below).** |
| **B-B2** fetch dead-ends on removed symbols | **Fixed.** | `delisted`: the first attempt completes with `unavailable: {'bitget_not_listed_now': ['C9USDT']}`. HL coins flagged `isDelisted` are handled the same way. Keyed refusals reach a 3-attempt end state (tested). |
| **B-M1** snapshot set not pinned | **Fixed.** | `inject`: an unlisted-directory file triggers (g3) "selected 84 ≠ pinned"; a second file in a date triggers (g3) "2 files". `compute` refuses without the pinned list. |
| **B-M2** cap and byte count | **Fixed.** | `bytes`: transfer-then-raise is logged at 14.502 GiB, matching the truth; the second attempt's row shows 14.942 GiB, matching the truth; the cap is cumulative and pre-checked. **Residual:** the log is an unversioned, editable file (m2). |
| **B-M3** `snapshot_accounts` holdout rows | **Fixed.** | D13 is explicit. Both readers bound the query to [S − 91 d, S): `p4_features.features` l.131 `snapshot_ms < sel_ms AND snapshot_ms >= lb_ms`, and `leaders.Equity` l.111. No other run-path reader exists (`p5_population` reads `features_*.parquet` only in `main`). See §D13. |
| **B-M4** survivorship direction | **Fixed in text and measurement; the rule is not implemented** (m4). | `survivorship()` measures on positions. The §5 reading rule ("annotated … printed with the decision") exists only in prose: no flag is computed, and `run` prints only counts, confirm, economic and decision (l.725). |
| A-m1–m10, B-m1–m12 | Fixed as listed in §14, with these exceptions. | **B-m1:** reset is caught, but deliberate `reflog expire` + `gc` reopens the gate, and a `-diff` attribute blinds the scan (m5, m6). **B-m3:** brittle (m3). **B-m11:** partial (m1). A-m3 is accepted as stated (§5 of this review). |

---

## BLOCKER

### B1. The A-B1 mechanism is fixed for BTC only. Cohort- or month-specific alt and reversal loadings false-confirm at 0.08–0.14.

**Where.**
- `ct/crowd008.py` `design` (l.471–486) gives BTC one slope per cell, but `alt_ew` and `rev_1d` get **one
  loading each** over both cohorts and all months.
- §3 "Reading" and D5 claim size 0.047–0.051 "under every market-, alt- and reversal-exposure null". Every
  alt and reversal null in §3 (N4–N7, N10) has a **constant** loading. Each such null is exactly in the
  model's span, so its size is a tautology.

**Why it matters.** A-B1's argument transfers word for word. The book's exposures are properties of the
cohort's positions:
- the cohort changes at S = 2026-08-03;
- the inverted book plausibly loads on reversal (`000l`, A-m9) and on the alt-minus-BTC factor (§9's
  "levered high-beta alts");
- those factors' drifts differ by month in a rally.

A pooled loading leaves (a_w − ĝ)·E[U_w] in α.

**Probe** (`probe_stats.py`, `probe_fix2.py`). 8,000 draws; no α. The model is the registered one
(base β 0.3, BTC month drifts as §3); only the named loading varies.

| null (2026-09-28) | registered (k 6) | alt+rev by window (k 8) | alt+rev by cell (k 10) |
|---|---|---|---|
| base | 0.052 | 0.051 | 0.051 |
| N10 (the revision's combined null) | 0.048 | 0.047 | 0.049 |
| **N11** alt loading A 0 / B 0.5; alt-minus-BTC drift Jul 0, Aug–Sep +0.2 %/day | **0.082** | 0.049 | 0.050 |
| **N11c** alt loading A −0.2 / B 0.3; drift Jul −0.2, Aug–Sep +0.2 %/day | **0.140** | 0.050 | 0.047 |
| **N12** reversal loading A 0 / B 1.0; reversal drift Aug–Sep +0.10 %/day | **0.084** | 0.049 | 0.049 |
| **N14** alt loading Jul 0 / Aug 0.4 / Sep 0; drift +0.3 %/day in Aug | **0.095** | 0.082 | 0.049 |
| **N15** reversal loading Jul 0 / Aug 1.0 / Sep 0; drift +0.15 %/day in Aug | **0.110** | 0.088 | 0.055 |
| N13 BTC t₃, residual SD ∝ \|BTC\| | 0.054 | 0.053 | 0.055 |
| N8 persistence (5 %, ρ 0.9) | 0.084 | 0.084 | 0.082 |
| **power, full / half** | 0.293 / 0.138 | 0.290 / 0.133 | **0.280 / 0.130** |

At 2026-11-23 (`probe_fix_1123.py`, 4,000 draws; 5 cells, k = 16, G = 21):
- registered against per-cell: size 0.043 / 0.043;
- power 0.424 / 0.404 (full) and 0.162 / 0.162 (half);
- the N11 analogue gives 0.081 for the registered rule and 0.045 per cell.

The magnitudes are the revision's own (N4's 0.2 %/day, N7's loading 1.0) and A-B1's cohort spread
(0.5). The reversal of a factor's drift between months is exactly the pattern `000l` describes.

**Why this blocks.** C1 is the one decisive statistic and is frozen at REGISTERED. The fix costs about
0.01 of power. The revision's headline claim (size held under every alt and reversal null) is false for
the cohort-specific case it was built to cover for BTC.

**Fix (cheap).**
- Give `alt_ew` and `rev_1d` one loading per BTC cell, as for BTC: `design` emits `alt[c]` and `rev[c]`
  for each cell c, so k = 1 + 3 × cells. This is the only variant that also holds N14/N15.
- Per-window loadings (k 8) are the minimum, but leave 0.08–0.09 under within-window month changes.
- Restate §3 with N11–N15 added, D5's size claim, and the k in the tests (`num['k'] == 6` becomes 10).
  Re-hash `ct/crowd008.py`.
- C2 and C3 need no change.
- The reported `factor_alpha` (costs, placebo, momentum) should use the same design so that it stays
  comparable.

---

## MAJOR

### M1. `fetch()` accepts mock hooks on the real data tree when called with the default root

**Where.** `r008_fetch.fetch` l.475–482. For a `hooks` argument, the only checks are:
- that the dict is complete;
- `real_tree_failures`, which refuses only when `root` is not this repository or a guard argument is
  overridden.

`hooks` itself is not counted as an override.

**Probe** (`probe_guard_r1.py mockreal`; a stand-in REGISTERED repo with `R.HERE` and `R.REAL_DATA`
pointed at it). `real_tree_failures(real, real/data, ∅) = []`, and then
`fetch(real, real/data, hooks=<complete mock dict>)`:
- **returns a `holdout_fetched` row**;
- `['holdout_fetch_started', 'holdout_fetched']` are **committed** in the "real" `results.jsonl`;
- 33 synthetic fill days are written into the real `pruned/` tree and pinned by the manifest.

**Consequence.** Once 008 is REGISTERED (and today is already past end + 7 d), one call such as "dry-run
the fetch with the test mocks" in the real repository:
- burns the one fetch: (f) refuses any later fetch, because a fetched row exists;
- freezes synthetic data as the holdout inputs, and the run would compute on them.

This is the B-B1 scenario with the roles reversed. §11's test "mock hooks writing into the real tree …
refuse" covers only a throwaway root.

**Fix.** In `fetch()`, refuse any non-None `hooks` when `data_dir` resolves to `REAL_DATA` (equivalently,
count `hooks` as an override in `real_tree_failures`). Add one test with `root = HERE` semantics (as in
the probe).

---

## MINOR

- **m1. The shadow-module check (d) misses packages and sourceless bytecode** (`r008_run.untracked_py`
  l.185–190). The filter keeps only `'/' not in f` or `ct/<x>.py`, so these are not flagged:
  - `duckdb/__init__.py` (a top-level package that shadows duckdb);
  - **`ct/stats/__init__.py`**: a package beats the module of the same name, and the probe shows
    `import ct.stats` resolving to the shadow instead of the hashed `ct/stats.py`;
  - `pyarrow.pyc` (sourceless);
  - any git-ignored file.

  **Fix:** flag any untracked or modified file (not only `*.py`) whose first path component, or whose
  `ct/<first component>`, is importable: `*.py`, `*.pyc`, `*.so`, or a directory holding `__init__.py`.
  Or simply refuse any untracked path at the top level and in `ct/`.
- **m2. The cumulative cap and the persistence clock live in an unversioned, editable file**
  (`data/r008/fetch_log.json`, gitignored). A deleted log refuses, but an edited one does not:
  - lowered `bytes` values bypass the 15 GiB cap;
  - back-dated `at` fields make a refusal "persistent" at once (`persist` probe: three attempts within
    5 h stay blocking; the same attempts with edited `at` fields become unavailable).

  There is also **no ceiling** on how many names may become persistent-unavailable: a systematic
  per-symbol failure, such as a rate limit, drops every affected symbol after 3 attempts (`persist`: 12 of
  12). Unavailable names silently leave the book (no vol, so no weight) and the factors.

  **Fix:**
  - append each attempt's summary (at, measured bytes, refusal keys) as a committed
    `holdout_fetch_attempt` row, and compute the spent bytes and the persistence chain from committed rows;
  - pre-specify a ceiling, e.g. more than 5 persistent names (or any name in ≥ 10 % of signal-days) stops
    the fetch for an owner decision.

  The owner's approval line, given after reading the fetched row, is the existing human check.
- **m3. The environment pin is brittle and has no recovery clause.**
  - The venv interpreter is `/opt/homebrew/opt/python@3.14` (3.14.8). Any `brew upgrade` (including the
    one `brew install` triggers for dependents) between the final fetch attempt and the run changes
    `sys.version`. Then (g2) refuses permanently, and (r) forbids any amendment after the fetch.
  - It is recoverable only by rebuilding exactly 3.14.8 with the same wheels, and that is not written down.
  - Conversely, the fingerprint hashes only the `.so`/`.dylib` files, not duckdb's or pyarrow's Python
    files.

  **Fix:**
  - compare Python by major.minor, and hash every file of the two distributions (or verify them against
    their RECORD hashes);
  - or pin a non-Homebrew interpreter (pyenv 3.14.8) before the fetch;
  - and add one sentence to §11: "do not upgrade Python or the venv between fetch and run; if it happens,
    restore 3.14.8 and the same wheels".
- **m4. The survivorship reading rule (§5) is prose only.**
  - No flag is computed, `recheck` does not cover it, and it is not printed. §5 says "It is printed with
    the decision".
  - "At least twice as often in the long quintile as in the short" is true for counts of 1 against 0.

  **Fix:** compute `survivorship_flag` in `compute()` from the stored per-window numbers, with a minimum
  count (e.g. ≥ 5 dropped coin-days ranked). Print it with the decision, and include it in `recheck`.
- **m5. A `-diff` (or textconv) attribute on `results.jsonl` blinds the history scan.**
  - `git log -p` then prints "Binary files differ", and a reset holdout row is no longer found. A
    working-tree `.gitattributes` line is enough (verified in a scratch repo).
  - **Fix:** `git log --all --reflog -p --text --no-textconv --no-ext-diff …` (verified to restore the
    row).
- **m6. Read-once still rests, in the end, on local history.**
  - `erase`: after `reset --hard HEAD~2` the gate stays shut (reflog). After `reflog expire --expire=now
    --all` and `gc --prune=now` it **reopens**.
  - That is deliberate destruction, and it cannot be prevented locally. But the branch tracks
    `origin/copytrade-research`, and `--all` scans remote-tracking refs.
  - **Fix (procedure, §11):** push immediately after each automatic commit (or have the runner do `git
    push` and report its exit status). The remote then witnesses the row.
- **m7. A failed automatic commit leaves the decision in plain text in the working tree** (`commitfail`).
  - A pre-commit failure, or a transient `index.lock` from an IDE's git integration, on the *second*
    commit leaves the holdout row uncommitted. The run exits before printing, but the file holds the
    decision.
  - If it is discarded, the committed `holdout_started` row correctly blocks until a restart line. The
    rerun is deterministic, so this is not an outcome lever, but the first row is lost.
  - `git commit --only` sweeps in no staged change (`only`: the commit holds `results.jsonl` alone, and
    `notes.txt` stays staged). That part is sound.
  - **Fix:** retry the commit a few times on `index.lock`. On final failure, the exit text should say
    "commit results.jsonl as is; do not discard". Ledger that rule in §11.
- **m8. Small text and code points.**
  - D5 and §10 give persistence size as 0.08–0.10, but §3 shows 0.116 at 2026-11-23. Say 0.08–0.12.
  - D13's "008 names, and alone may read … only for the dates named here: (i) … (ii) …" can be read as
    the complete list of 008's holdout reads. Add "(in addition to the pulled series of §2 and D4)".
  - `day_factors` (l.371–372) drops from the universe any coin without a close at entry + 24 h, which
    conditions on surviving the day. The book instead keeps the coin with a 0 return. This is harmless
    at 0 missing returns, but the two treatments differ. Say so, or use the same rule.
  - Check (d) now refuses on the other studies' untracked drafts (`ct/listings013.py`, `ct/shortvol*.py`,
    `r013_*`, `r014_*`). This is intended, but §11's procedure should say that everything is committed
    before the fetch.

---

## Answers to the brief, in order

**1. First-round findings.** See the status table. All 3 blockers and 8 majors are fixed in code as
claimed, re-verified by the reviewers' own probes ported to the revised API. A-B1 and A-M1 are fixed
only for constant alt and reversal loadings (B1). One new path opens because of B-B1's design (M1).

**2. The confirm regression.**
- **Point-in-time.**
  - The reversal and momentum sort variables use `close_at(k − 24 h)`, `close_at(k − 5 d)` and
    `close_at(k)`. `close_at(h)` is the close of the candle that ends at h, so all are known at entry.
  - BTC and ALT are same-span factor returns by design: regressors, not predictors.
  - The universe is the signal coins of the live tranches, known at entry, minus the next-day
    survivorship filter (m8).
  - The cells use the entry key's window and month.
- **Degrees of freedom.** With n 83, k 6 and G 13, CR1 at t₁₂ holds its size:
  - 0.052–0.054 under the base, N10 and a heavy-tailed BTC with residual SD ∝ |BTC| (N13), at 8,000
    draws;
  - at k 8 and k 10, 0.047–0.055.
- **The (n−1)/(n−k) factor** is applied correctly.
- **Reproduced cells:**
  - N10: 0.054 (draft 0.047);
  - base 0.053 (0.048);
  - power full 0.287 (0.295), half 0.137 (0.139).

  The differences are within Monte-Carlo error (SE ≈ 0.003–0.005).
- **"Median > 0".** It is well defined: a_t = α̂ + ê_t, residuals with an intercept sum to zero, so
  mean(a) = α̂, and n = 83 is odd. It is in-sample, as §3 states (A-m5).

**3. New mechanisms.**
- **(a) Auto-commit.**
  - It does not sweep staged changes.
  - It can fail half-way, leaving the decision uncommitted but unprinted (m7).
  - It survives `reset --hard` (reflog), but not `reflog expire` + `gc` or a `-diff` attribute (m5,
    m6).
- **(b) Environment fingerprint.** Stable across calls (0.2 s), but brittle against a Homebrew patch
  upgrade, and it ignores the packages' Python files (m3).
- **(c) Persistence end state.**
  - It is not selectively abusable without outcome knowledge, and BTC, fill days and snapshots are never
    eligible.
  - But its clock is an editable file, and there is no ceiling (m2).
- **(d) Window floor.**
  - It is computed after `build_days` and before `c8.book` (`compute` l.507–517): positions only, fixed
    parameters, pinned inputs. It cannot be triggered selectively.
  - The "both windows excluded" case returns `not_demonstrated` (tested).
- **(e) Shadow-module refusal.** It works for `duckdb.py`, but misses packages (including
  `ct/stats/__init__.py`), `.pyc` files and ignored files (m1).
- **(f) Survivorship reading rule.** Not implemented (m4).
- **(g) Economic bar and labels.**
  - The bar is correct: 16.2 bp/day gross; P(bar | confirm) is 0.28 / 0.18.
  - The two confirm labels have the same consequence, a cost-reduced registration that will need its own
    D18 amendment (third reader). This is stated in D6, so it is coherent.

**4. D13.**
- **Coherent with D18 (a)–(g).**
  - (b) already lets L3 read "the series its registration names". D13 only lifts (a)'s last sentence for
    `snapshot_accounts` rows in [S − 91 d, S), and names the raw signal-date partitions.
  - The `hl-funding` cache stays unread, since 008 pulls funding fresh.
  - (c), (e) and (g) are untouched. (d) is moot, because 012 closed at Stage 1.
  - (f): 008 counts the holdout family as 1 read. That is consistent, since 012 never read.
- **Coherent with `000c`.**
  - The filter is stated, and it lives in the query of the two hashed modules (`p4_features` l.131,
    `leaders` l.111).
  - Window A's bound is before 2026-07-01, so it reads no holdout row.
- **The orchestrator's approval** under the owner's 2026-10-12 delegation is recorded, and is to be
  confirmed at registration. Wording nit in m8.

**5. Month-scale persistence.**
- **Acceptable as a stated weakness. No cheap mitigation exists** (`probe_stats.py mitig`,
  `probe_crit.py`, 8,000 draws):

  | rule | base size | N8 (5 %, ρ 0.9) | N9 (3 %, ρ 0.97) | power full / half |
  |---|---|---|---|---|
  | registered | 0.048 | 0.083 | 0.099 | 0.278 / 0.135 |
  | + window-level sign agreement (mean a_t > 0 in A and in B) | 0.046 | 0.080 | 0.096 | 0.272 / 0.131 |
  | + month-level sign agreement (Jul, Aug, Sep) | 0.041 | 0.071 | 0.088 | 0.252 / 0.118 |
  | weekly-sum Newey–West (lag 2), t₁₂ | **0.071** | 0.098 | 0.121 | (size broken) |
  | crit raised to 2.18 | 0.024 | 0.052 | 0.066 | 0.191 / 0.079 |
- **What the options do.**
  - Window sign agreement does almost nothing, because C3's halves already do the same job.
  - Month agreement buys 0.012 of size for 0.026 of power.
  - Holding 0.05 under N8 costs a third of the power.
  - B1's per-cell fix does not change persistence (0.082).
- **Recommendation:**
  - keep the rule;
  - correct the stated range (m8);
  - optionally *report* a flag "persistence-sensitive" when 1.78 ≤ t < 2.18, which decides nothing.

**6. What the revision broke or left half-done.** M1 (the mock-hook path that the B-B1 design opened), and
m1, m2, m4 and m7.

---

## Checked and sound

- **Byte identity** of the 14 reused modules and `p5_run.py` with `efefc7f`. The §11 hashes match.
- **The independent C1–C3 implementation** equals `crowd008.confirm` to 9e-16. `beta_cells` gives 30/29/24
  at 2026-09-28. At 2026-11-23, window C's 3 September days merge into (B, Sep): 5 cells.
- **The calendar and read bound.**
  - The last IC return ends 09-27 02:00 (09-28 01:00 even with a late 09-24 snapshot), inside end + 2 h.
  - `fetch_closes` and `Closes`/`Funding.from_store` drop rows at or after the bound.
- **The guard.**
  - (s), (a)–(e), (n), (t), (k), (f) across branches and the reflog, and (g1)–(g3) with the snapshot pin.
  - (r) with exact header parsing; "approval plus prose" is refused (test).
  - (h') ordering; the restart rule.
  - `split` and `inject` are refused. The bytes are exact across attempts. Delisted names are recorded,
    never a stop.
- **Window B's holdout reads are bounded in the query**, and window A reads none.
- **Determinism.** Inputs, code and environment are pinned, and the placebo seed is fixed, so a restart
  reproduces the row.
- **Economic arithmetic and the §9 tail**, re-derived.
- **`git commit --only`** commits `results.jsonl` alone.

---

## Verdict

**Register after the listed fixes.** One blocker and one major, both cheap and both in 008's own files:
- **B1:** per-cell alt and reversal loadings in C1. Re-run §3 with N11–N15, restate D5, update the k in
  the tests, re-hash `ct/crowd008.py`.
- **M1:** refuse a `hooks` argument whenever `data_dir` is the real tree. Add a test.

Fix the minors in the same pass where they touch frozen code: m1, m2, m3, m4 and m5 are all in `r008_*`
or `crowd008`. m6, m7 and m8 are procedure and text.

After the fix, a re-check of B1 (`probe_stats.py`, `probe_fix2.py` against the new `design`) and of M1
(`probe_guard_r1.py mockreal`) is enough. A full third review is not needed.

---

## Revision 2 recheck (2026-10-13)

**Scope.** The fixes mapped in registration §15, checked in code and by re-running this review's probes
against the revised files. This is not a full third review.
- The §11 hashes of `ct/crowd008.py` (`8123b34a…`), `r008_run.py` (`bf9e702a…`) and `r008_fetch.py`
  (`979a7acf…`) match the disk.
- Tests: **132 of 132 pass**.
- New probes, all synthetic, in throwaway repos with every hook mocked:
  - `scratch_review008r1/probe_r2_stats.py`, which calls the **revised** `crowd008.confirm`;
  - `scratch_review008r1/probe_r2_guard.py`.

  Outputs are in `probe_r2_*.out`.
- No real data was read. No commit was made in the real repository.

| item | status | evidence |
|---|---|---|
| **B1** per-cell alt and reversal loadings | **Fixed** | `design` (l.474) emits a BTC, an alt and a reversal loading per cell. The revised `confirm` matches the independent k-10 fit to 1e-15 (k = 10). At 8,000 draws: N11 0.050 (r1 0.082), N11c 0.047 (0.140), N12 0.049 (0.084), N14 0.048 (0.095), N15 0.046 (0.110). N10 0.047, N13 0.049, base 0.048. Power 0.275 full / 0.134 half. N8 persistence 0.082, unchanged (stated). The reported factor α uses the same design (`per_cell=True` by default). |
| **M1** hooks on the real tree | **Fixed** | `real_tree_failures(…, hooks)` refuses any `hooks` argument on the real tree. In the stand-in REGISTERED repo, the real root with complete mocks and no override is refused with "(B1) mock network hooks are never used on the real data tree". A throwaway root is refused for both reasons. 0 rows were written and 0 fill days. |
| m1 shadow scan | **Fixed** | `shadow_modules` flags each of these: `duckdb.py`, `duckdb/__init__.py`, `ct/stats/__init__.py`, `ct/tool/__init__.py`, `pyarrow.pyc`, `duckdb.cpython-314-darwin.so`, `ct/_x.abi3.so`, a git-ignored `ignored_mod.py`, and a modified tracked `ct/stats.py`. Guard (d) refuses. |
| m2 committed attempts, bytes, clock, ceiling | **Fixed, one end state left open** | A transfer that raises is counted from the committed rows: 14.502 GiB, matching the truth, and `results.jsonl` stays clean. An end row edited down to 0 bytes changes nothing (the maximum over versions is taken). A killed attempt's 3 GiB reservation is counted. Back-dated `at` fields are ignored, because the clock is the commit time. 8 unavailable names complete the fetch; 9 refuse with `unavailable_ceiling`. **Residual (minor):** see the first bullet below this table. |
| m3 environment | **Fixed** | The fingerprint covers every file of the two packages: duckdb 56, pyarrow 755. It takes 0.1 s and is stable. The recovery clause and `--env-record` are in §11. `--env-record` is ungated, but it only writes an `env_record` row that no check reads, and it reads no data. |
| m4 survivorship flag | **Fixed** | `survivorship_flag` returns `False` for counts of 1 long against 0 short. It returns `True` for 6 coins ranked with 12 long against 4 short, and for an 11 % crowd share. It is stored, rechecked, and printed before the decision (`run` l.785–788). The persistence-sensitive flag (t in [1.782, 2.18)) is reported. |
| m5 history-scan flags | **Fixed** | `--text --no-textconv --no-ext-diff` in `result_rows`, and `--no-textconv` on `git show`. With an uncommitted `results.jsonl -diff` attribute, `reset --hard HEAD~2` still leaves (f) refusing. |
| m6 push | **Procedure** | §10 and §11: the owner pushes after each automatic commit. `reflog expire` + `gc` is a stated limit. |
| m7 failed commit | **Fixed** | Retried once, then "STOP: … Commit it by hand, AS IT IS …; do not discard". The decision is not printed. (k) and (f) refuse while the row is uncommitted. |
| m8 text | **Fixed** | The persistence range, D13's wording, the treatment of a missing next-day close, and the instruction to commit everything first. |

**Left open (minor; no blocker or major remains):**
- **The ceiling's end state is not specified.**
  - When more than 8 names fail permanently (9 Bitget removals in the probe), every attempt refuses, and
    the "orchestrator decision" has no mechanism. An amendment would change the registration commit, and
    the fetch's (f) check then refuses to resume.
  - This is the B-B2 dead end again, but only above 8 permanent removals (0 of 168 are gone today).
  - **Text-only fix at registration:** state the end state, e.g. "a ceiling refusal that persists on 3
    attempts ≥ 24 h apart closes 008 unread: no read, L3 not demonstrated, the calendar stays unread".
- **Forged persistence clock.** A hand commit with a back-dated `GIT_COMMITTER_DATE` can still forge the
  clock (probe: two forged rows make a key persistent). That is deliberate forgery, it stays visible in
  the history, and it is acceptable as stated.

**Verdict: register.** Add the one-sentence ceiling end state to §2/§11 when the status is set to
REGISTERED. It needs no code change and no re-hash.

---

## Amendment 1 recheck (2026-10-13)

**Scope.** Amendment 1 only, at `963dcc8` (§16; `r008_fetch.py` `cf559a32…`, `r008_run.py` `eb2ed60d…`).
- Tests: **143 of 143 pass**.
- Probe: `scratch_review008r1/probe_amend1.py`, output in `probe_amend1.out`. It uses throwaway repos and
  mocks only. Attempt 1 of the fill probe runs the REGISTERED `2bd855a` `r008_fetch.py`, taken from git.

**Reads.** Repository metadata only:
- `git diff 2bd855a 963dcc8`: the only guarded code that changed is `r008_fetch.py` and `r008_run.py`. The
  registration's diff is the status line, the two hash rows and §16.
- The started row `5fe7930` carries `registration_commit 2bd855a…` and `owner {'end_date': '2026-09-28'}`.
- No commit touches the registration or guarded code after `963dcc8`.
- The existence of `data/r008/` sub-directory names, from a listing.

No price, fill content or hash of a holdout file was read. Nothing was fetched or run against real data,
and nothing was committed.

| question | result |
|---|---|
| 1a. A second amendment line, an `amendment 2` line, an abbreviated sha, a forged 40-hex sha | **Refused** by (m), plus (f). |
| 1b. An amendment added **after** the fetched row, to swap code before the read | **Refused** by (g2) (code changed and code hashes differ) and (r) (the registration changed beyond approval lines). |
| 1c. An amendment that also moves the owner end | **Refused** by (t) and (f). |
| **1d. Edits to criteria, parameters or §12 text, or to guarded code, after the amendment commit and before the fetched row** | **Accepted (MAJOR A1).** See below. |
| 2. Binding of the started row | `accepted = {current commit, prior}`. The prior is whatever the single valid line names, and it must be an ancestor commit of the registration. In the real file it names `2bd855a…`, which is exactly the started row's commit. The owner end must still match. Sound. |
| 3. Cap and attempt numbering | Attempts continue as [1, 2]. The bytes of the pre-amendment attempt count toward the cap, and the fetched row's total is the sum across the amendment. The run guard opens after the approval. Real attempt 1's 3,594,315,926 bytes are in committed rows, so they count. |
| 4. Fill reuse | **Gap (minor A2).** With attempt 1 as it ran (no `fill_hashes`), a fill day halved on disk before attempt 2 is **accepted**. P3 G1/G2 are **not re-run** on reused days, because `p3_gates.json` exists and nothing new was pulled. The fetched row then pins the altered file. Once hashes are recorded, a later swap is refused with `fills_changed:` (verified). |
| 5. Boundary fix | Exclusive end: 0 refusals. Inclusive end: 0. Exclusive end with only the newest 200 rows: 0. An inclusive end combined with newest-200 truncation (not Bitget's verified behaviour) gives 28 refusals: the fix **fails closed**, it does not silently accept. |

**A1 (MAJOR): the amendment opens the fetch gate to any later edit until the fetched row.**
- **What the guard does now.**
  - Fetch-mode (f) now accepts the started row whenever its commit is the named prior. The current
    registration commit can be *any* later commit that keeps the one valid line.
  - In the probe, after a valid Amendment 1, a further commit that swaps guarded code (with its hash
    row) passed the fetch guard, and so did a commit that rewrote criteria text. The fetch completed, and
    the run guard opened after the approval.
  - The fetched row carries the latest commit, so (r) and (g2) never compare back to the amendment.
  - Before the amendment, any such edit was refused, because the started row's commit had to equal the
    current one.
- **The actual amendment is clean.** The diff `2bd855a..963dcc8` is limited to the two declared files and
  §16, and nothing has been committed since. So the hole only matters if something is committed before
  the fetched row.
- **Fix, either of:**
  - **Procedural, no re-hash.** Before writing the read approval line, the orchestrator checks that the
    fetched row's `registration_commit` equals `963dcc8…`. It ledgers that check, together with the
    verified diff scope `2bd855a..963dcc8`. If they differ, the read is not approved.
  - **In code** (needs a re-hash, and re-committing the amendment line so that it first appears in that
    commit). Accept the prior only when the current registration commit (fetch mode), or the fetched row's
    commit (run mode), is the commit that first holds the amendment line. Also require that
    `git diff prior..that commit` touches no guarded file other than `r008_fetch.py` and `r008_run.py`.

**A2 (minor): reused fills were never hashed.**
- **Before attempt 2,** do both of these (no code change):
  - delete `data/r008/p3_gates.json`, so that the fetch re-runs G1/G2 and the G3 sample on all 33 reused
    days;
  - have the orchestrator ledger each holdout fill file's size and mtime, and check that every mtime is at
    or before attempt 1's end commit `e069efb` (committer time 1791861876).
- **Attempt 2 then records the hashes,** and from then on the existing `fills_changed:` refusal and the
  manifest pin cover them.
- **The stronger option is a re-download.** It costs about 3.3 GiB, against 3.35 GiB spent of the 15 GiB
  cap, and needs a code change. It is not required.

**Verdict: proceed, on two conditions.** These are A1's procedural check of `registration_commit ==
963dcc8` before the approval line, and A2's two pre-attempt-2 steps. No blocker. If the orchestrator
prefers not to rely on the procedural check, fix A1 in code first and then proceed.

**Operational note.** As of this recheck, the working tree has uncommitted edits to `ct/listings013.py`,
`r013_build.py` and `r013_run.py` (013's drafter; not 008 files, and not made by this reviewer).
- 008's shadow scan (d) flags modified files in the top level and in `ct/`, so attempt 2 will refuse until
  they are committed or stashed.
- That refusal is correct behaviour, not a fault.
