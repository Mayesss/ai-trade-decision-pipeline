# 014 DRAFT (Revision 1): independent review B (guard, data path, leakage), 2026-10-13

Reviewer brief: adversarial review of `registrations/014-short-vol-deribit.md` (DRAFT, Revision 1) with
`ct/shortvol.py`, `ct/shortvol_sim.py`, `r014_fetch.py`, `r014_build.py`, `r014_run.py`, `r014_tail.py`,
`tests/test_r014.py` and `data/r014/`. The emphasis was the guard, the data path and leakage.

Rules I kept:
- No file of the draft was edited and nothing was committed.
- `r014_run.py` was never run in `--discovery` or `--holdout` mode. Its guard functions were called
  read-only.
- No premium-versus-realised comparison, payoff, MTM, P&L or IV/RV after entry was computed on
  2021-03-24 → 2026-06-30.
- Nothing dated on or after 2026-07-01 was requested or read.

## How this was checked

- **Tests.** `cd research/copytrade && .venv/bin/python -m tests.test_r014` gives **"all checks passed
  (126)"** in 8 s. Under `python -O` the run **stops at its first FAIL** ("a path with a gap, or one
  not starting at the entry, is refused"), because that check depends on an `assert` (m2).
- **The real guard, read-only** (`probe_real_guard.py`).
  - Discovery mode refuses on four grounds: the status line, (a), (c) and (d). Holdout mode also
    refuses on (g1).
  - **There is no (e) failure.** All 7 code hashes and all 10 panel hashes in §9 match the disk.
  - A guard pass loads only four repo modules: `ct`, `ct.shortvol`, `r014_run` and `__main__`.
- **Probes**, all in `research/copytrade/scratch_review014b/`. They use synthetic data in throwaway
  git repos, plus timestamp- and hash-only reads of the real files:
  - `probe_subdir.py`: the holdout gate in a repo laid out like the real one (B1).
  - `probe_erase.py`: rows erased by checkout, `reset --hard`, `--amend` or stash; a nested
    `git init`; `GIT_DIR` set to a decoy repo; and the proposed fix (M1).
  - `probe_readgate.py`: an audit hook **aborts at the first `open()`** of any real `panel_*` file, so
    nothing is read. Reaching that `open()` proves that no guard stood before it (M2).
  - `probe_shadow.py`: shadow-module attacks on a *copy* of the runner (M3).
  - `probe_forward.py`: synthetic size of the forward-versus-index error in the mark inversion (M4).
  - `probe_pull_bounds.py`: the pull's SHA256SUMS, every stamp against the cutoff, every trade file
    against its window, and the panels against their MANIFEST and §9.
  - `probe_tail.py`: tail-pull bounds and hashes, the selection re-derived from the underlying, and
    `REALISTIC_WEEK` against `tail_results.json`.
  - `probe_stamps.py`: whether perpetual candles are stamped at the open (out of calendar, 2020–21).

## 0. Answers to the brief, in short

| question | finding |
|---|---|
| Outcome while DRAFT, uncommitted or modified? | **Not through the CLI.** `guard()` hard-codes `HERE`, and every check refuses (probe). **Through the module API, yes (M2).** `Panel('discovery')` and `Panel('holdout', allow_holdout=True)` open the real panels for any importer. The e2e test's recipe with one line left out (the `PANELS` redirect) reaches the real discovery and holdout panels. |
| After a row exists? | Blocked while the row is in the tree or in any branch's history. **Not blocked after `git checkout`, `reset --hard`, `commit --amend` or `stash` (M1, probed).** The holdout's (i) reopens the same way: **the holdout can be read twice.** The runner never commits its rows. 008 fixed all of this this round, and 014 copied the older pattern. |
| A throwaway `root` pointed at the real `data/r014`? | `guard()` cannot be redirected. `guard_failures(root=…)` and `Panel` are not bound to each other (no gate token), so a stubbed guard plus the real `PANELS` reads real data (M2). The guard also trusts whichever repository git finds: a nested `git init` or a `GIT_DIR` decoy hides every row (M1). |
| `-O`? | The guard and `Panel` use `raise`, so they survive `-O`. `position_pnl`'s path checks and the build/tail integrity checks are `assert`s, which `-O` strips (m2). |
| `--power` or a count mode opening the panels? | **No.** `run_power` and `shortvol_sim` open no file. They only write `logs/power.json`. There is no count mode. |
| Holdout gate (g1)–(g4), (h'), (i) | **In the real layout it can never open (B1).** `git show HEAD:results.jsonl` and `git show <c>:<rel>` resolve from the repo root, not from `research/copytrade/`. (g2) and (h') therefore always refuse there. The tests pass only because their throwaway repos have the runner at the top level. When it does open (the top-level layout), the gate recomputes the decision from the row's own numbers. It does not bind the row to the panel (m1). |
| Panels pinned? | **Yes.** Both panels: files = MANIFEST = §9, guard (e) in both modes, the MANIFEST's pull id = the pull's SHA256SUMS hash, no extra files. |
| Outcome-adjacent build reads / coverage leaks | The build read entry index prices, perp closes at entry and settlement-window timestamps (minutes only). It stored aggregates per calendar and coin, not per week. **I find no material leak** (§ "Checked and sound"). Two count caveats are listed under m3. |
| Fetch integrity | Every request is end-bounded (`raise`, not `assert`). Every row is checked. Errors are never written. Paging is forward, and the tail coverage is checked. **0 rows on or after 2026-07-01; every trade file lies inside [F−4h, F); no empty window.** One documented rule is not implemented: an empty trade window would be cached (m3). |
| Settlement from the index TWAP | Correct as specified: [X−30 min, X), per-minute medians, at least 10 minutes. Avoiding `get_instruments` and `get_delivery_prices` is right. |
| Entry marks | **The mark IV is inverted against the index, but Deribit marks against the expiry's forward (M4).** The error depends on which leg traded last. It is ±2–9 % of premium at 10–25 %/yr carry, the same size as the net premium under test. The forward can be recovered from fields the pull already kept. |
| Tail (`r014_tail.py`) | Bounded and checked (`raise`). **Max stamp in any tail file: 2021-03-23 23:00.** Options files lie inside [E−24h, E+7d), the last week computed ends 2021-03-12, and the selection reproduces exactly from the underlying. Disclosure counts are wrong (m5). Revision 0 was never pinned, so "mechanics unchanged" cannot be verified from git (m4). |
| Hashes and shadow modules | All decision-path modules are hashed. **An untracked `statistics.py` beside the runner, an untracked package `ct/shortvol/`, or a stale `ct/__pycache__` .pyc replaces decision-path code while every §9 hash still matches (M3, probed).** |

---

## BLOCKER

### B1. The holdout gate can never open in the real repository, and fixing it after discovery is forbidden by (g3)

`guard_failures`:
- reads `HEAD:results.jsonl` with `_git(root, 'show', f'HEAD:{results_rel}')` (l. 139);
- reads `first_commit_containing`'s versions with `_git(root, 'show', f'{c}:{rel}')` (l. 102).

Git resolves `<rev>:<path>` **from the repository root**, not from `cwd`. The real runner lives in
`research/copytrade/`. In the real repo, `git show HEAD:results.jsonl` fails ("path
'research/copytrade/results.jsonl' exists, but not 'results.jsonl'"). Its stdout is empty, so:
- (g2) always reports "the discovery row is not committed in HEAD:results.jsonl";
- `first_commit_containing` always returns `None`, so (h') always reports "the approval must be
  committed strictly after…".

`probe_subdir.py` runs the same steps (rows committed, approval committed after them) in two layouts:

| layout | holdout gate |
|---|---|
| runner at repo top level (the tests' layout) | `OPEN` |
| runner in `research/copytrade/` (the real layout) | `(g2)`, `(g2)`, `(h')` (never opens) |

Why this is a blocker and not just a bug:
- The guarded code is frozen at REGISTERED.
- After the discovery run, (g3) refuses if any guarded code changed since that run.
- A discovery pass would therefore leave a holdout that cannot be read without a ledgered amendment of
  registered code, which is the very path the guard exists to close.
- It must be fixed before the status changes.

**Fix.**
- Use `HEAD:./{results_rel}` and `{c}:./{rel}`, exactly as `r008_run.py` l. 117 and l. 123 already do.
- Run `guard_checks` and `e2e_checks` a second time with the runner in a subdirectory of the
  throwaway repo.

---

## MAJOR

### M1. "Run once" and "read once" can be undone by erasing rows; the runner never commits; the repository is not pinned

`result_rows` scans the working tree plus `git log --all -p`. `probe_erase.py`, in throwaway repos:

| case | discovery gate (f) | holdout gate (i) |
|---|---|---|
| rows written, not committed, then `git checkout -- results.jsonl` | **OPEN** | **OPEN: holdout readable again** |
| rows committed, then `git reset --hard HEAD~1` (reflog still holds them) | **OPEN** | **OPEN** |
| rows removed by `commit --amend` | **OPEN** | — |
| rows stashed (`refs/stash` is a merge, and `log -p` shows no merge diff) | **OPEN** | — |
| nested `git init` in the runner directory | rows seen: **0** (outer history hidden) | — |
| `GIT_DIR`/`GIT_WORK_TREE` → a decoy repo (`-I` does not scrub `GIT_*`) | **OPEN** once the working-tree rows are gone | — |

- The pre-specified procedure says "Commit both rows" by hand. Until someone commits, a single
  `git checkout` reopens discovery. This holds even after the decision has been printed.
- Erasure combined with a recommitted registration (for example a different n) is a garden-of-forking-
  paths exploit that no check in 014 sees.
- `r008_run.py` closed these cases this round:
  - `--reflog` in `result_rows` (l. 102);
  - `commit_results()`, which commits `results.jsonl` alone right after appending (l. 345);
  - `untracked_py` and `real_tree_failures`.

  014 is a fresh copy of the older pattern.
- The guard_checks test even shows "history purged … discovery may run". That is accepted only for a
  full reflog expiry and gc, but the cases above need neither.

**Fix.**
- Use `log --all --reflog -m -p` in `result_rows`. The probe's second block shows that the reset,
  amend and stash cases then refuse.
- Have the runner commit `results.jsonl` **alone** right after each `*_started` row and after each
  result row (`git commit --only -- results.jsonl`). It refuses to continue if the commit fails.
- At start, refuse unless `results.jsonl` is clean against HEAD.
- Call git with every `GIT_*` variable removed from the environment.
- Require `git rev-parse --show-toplevel` to equal the known repo top, and pin the root commit
  (`git rev-list --max-parents=0 HEAD`) in §9.
- Block on any `registration` starting with `014-`, not on the exact file name (m8).
- Add one test per case.

### M2. The read path has no gate: `Panel` opens the real panels for any caller; the read lock is a caller label

From `probe_readgate.py` (the audit hook aborts at the `open()`, so nothing was read):

| call | result |
|---|---|
| `R.Panel('discovery')` from any importer | reaches `panel_discovery/MANIFEST.json` |
| `R.Panel('holdout', allow_holdout=True)` | reaches `panel_holdout/MANIFEST.json`. "Without the gate's flag" means a boolean the caller passes. |
| `R.Panel('discovery', panel_dir=<symlink to panel_holdout>)` | reaches the holdout MANIFEST. `panel_dir` skips `assert_allowed` entirely, and the name check is `'holdout' in d.name`. (The MANIFEST's calendar field then refuses, after the open.) |
| the e2e test's recipe (stub `R.guard`, redirect `HERE`/`REG`/`RESULTS`) **without the `PANELS` line**: `run_discovery()` | reaches the real discovery panel, with no 014 row in the real results |
| same recipe, `run_holdout()` with a synthetic discovery row in the temp results | reaches the real holdout panel |

- This is round 2's label-only lesson, in exactly the form review 013-B M4 flagged.
- §2's "`Panel` refuses any other path" is false whenever `panel_dir` is given.

**Fix.**
- `guard()` sets a module token `_GATE = (HERE.resolve(), mode, commit)`, and only after
  `guard_failures(HERE, …) == []`.
- `Panel` resolves its directory (following symlinks). If the directory lies under the real
  `DATA_ROOT`, `Panel` raises unless `_GATE` names the same repo and mode. The holdout requires
  `mode == 'holdout'`.
- `panel_dir` is accepted only outside the real `DATA_ROOT`.
- Drop `allow_holdout` as an argument.
- Add one test that a stubbed `guard` plus the real `PANELS` refuses.

### M3. Untracked shadow modules replace decision-path code while every §9 hash matches

`r014_run.py` inserts `HERE` at `sys.path[0]` (l. 43) *before* `ct.shortvol` imports `random` and
`statistics`. `-I` does not help here, because the runner adds the path itself. (d) checks only the
`git status` of the listed files. `probe_shadow.py` works on a copy of the runner, never the real tree:

| attack | result |
|---|---|
| untracked `statistics.py` beside `r014_run.py` (a different `fmean`) | **loaded by the real CLI entry** |
| untracked package `ct/shortvol/__init__.py` beside the hashed `ct/shortvol.py` | **the package wins** |
| a stale `ct/__pycache__/shortvol.cpython-314.pyc` compiled from a same-size, same-mtime tampered source (`SIZE_N = 9.99`), with the hashed source restored | **`SIZE_N 9.99` is loaded.** The source hash is unchanged. `__pycache__` is untracked or ignored. |

No such file exists today. I checked: no top-level name shadows the stdlib, and `ct/` holds no
package directory. But nothing would detect one.

**Fix.**
- Port `r008_run.untracked_py`, which covers untracked or modified `*.py` at the top level and in `ct/`.
- Also refuse any directory under `ct/` that contains `__init__.py`.
- After the imports, refuse if any `sys.modules` entry whose file lies under `HERE` is not in
  `GUARDED_CODE`, or if its `__spec__.origin` is not the hashed `.py` file.
- Before importing `ct`, set `sys.pycache_prefix` to a fresh empty temp directory, so no stale `.pyc`
  can be read. `-B` alone stops writing but not reading.

### M4. The entry premium is inverted against the index, not the forward Deribit marks against; the error is the size of the effect under test

`straddle_entry` does three things:
1. it inverts `mark_price × index` with Black-76 at **F = index**;
2. it uses **one** leg, whichever traded last at K;
3. it re-prices **both** legs at the index with that IV.

§4.4 justifies this as "a forward equal to the index; a 7-day basis is a few bp".

- **Deribit prices and IVs are relative to the expiry's forward.** Out of calendar, I took 50 BTC
  option trades on 2020-01-03 07:30–08:00 (one request; § reads). For each trade I solved the forward
  from its own `price` and `iv`.
  - **F/index − 1 = +0.13 % for every 7-day trade, calls and puts alike (≈ 7 %/yr), and +0.27 % for
    14-day.**
  - That is a calm week, and the forward is not "a few bp". The carry of 2021 Q1 and 2024 Q1 was far
    higher.
- **Size of the error** (`probe_forward.py`, synthetic). Straddle premium against its true value:

  | annualised carry | σ 0.5 | σ 0.8 |
  |---|---|---|
  | 2 % | ±0.7 % | ±0.4 % |
  | 10 % | ±3.5 % | ±2.2 % |
  | 25 % | ±8.6 % | ±5.4 % |
  | 40 % | ±13.8 % | ±8.6 % |

  The sign is + when a call traded last and − when a put did.
- **Why it matters here.**
  - The registered net premium at the prior is ≈ 5 % of premium (§3).
  - The error's sign follows the call/put mix of the last ATM trade, which is regime-dependent (call
    flow in contango rallies). It is not guaranteed noise, and it is largest exactly when carry is
    high.
  - It also adds variance to a test with power 0.2.
  - The realistic tail (§5) uses the same function, so n depends on it too.
- **The pull already holds the fix.**
  - Public trades carry no `underlying_price`. I confirmed the field list.
  - But `price` and `iv` are both in `KEEP`, in the panels and in the tail files. The forward is
    recoverable per trade, as above. Put–call parity on the latest call and put at K is a second route.

**Fix.**
- In `straddle_entry`, solve the forward F′ from the chosen trade's (`price`, `iv`, K, T).
- Invert the mark at F′, and re-price both legs at F′ re-anchored to the entry index (F′/index of the
  trade).
- If (`price`, `iv`) do not solve, use parity, or else mark the straddle missing (counted).
- Report the old index-based mark as a block.
- No new pull is needed. The panels are unchanged, and the code hashes change.
- **The tail must be recomputed** with the corrected function. The orchestrator should record in the
  `000m` addendum that this mechanics change came from review, not from the tail numbers. n is then
  re-derived by the same rule.

---

## MINOR

- **m1. (g4) binds the row only to itself.**
  - `recheck_discovery` recomputes the summary from the row's stored weekly series. It never checks
    that the series came from the panel:
    - the entries need not be the 143 discovery Fridays (`probe_erase.py` used entries 0..142, and
      the gate opened);
    - T2 is not checked to equal `stand_aside(T1, stressed)`;
    - G is not checked.
  - A hand-edited row, approved by sha, passes.
  - Fix: the gate recomputes `positions`, `stressed_entries` and `series_of` on the hash-pinned
    discovery panel (deterministic and already read) and requires equality with `weekly`.
- **m2. `-O`.**
  - `position_pnl`'s path checks (l. 391, 437) are `assert`s, and one test fails under `-O`.
  - The build's cutoff and window checks (`r014_build.py` l. 84, 94) are `assert`s.
  - So are the tail's SHA-256 and cutoff checks (`r014_tail.py` l. 119, 123, 260, 271).
  - Fix: `raise` instead. `guard()` refuses when `sys.flags.optimize`.
- **m3. Two count caveats.**
  - The "0 perp hours missing" counts flat placeholder candles as present. Out of calendar, the
    pre-2021 tape has 4 BTC and 5 ETH hours with o = h = l = c (2020-08-27 06–08:00 and 2020-01-25
    11:00: maintenance). A hedge at such an hour trades a stale price.
    - Count flat or zero-volume perp hours **inside the guarded run** with the other pre-decision
      counts. Do not count them in the build: counts over the hold are outcome-adjacent (the 013 M5
      lesson).
  - `fetch_window` writes `[]` for a window that returned no trades. The module docstring says an
    empty answer for a window that must have data is an error.
    - No window was empty (`probe_pull_bounds.py`), so this is latent.
    - Fix: raise on an empty trade window.
- **m4. Revision 0 was never pinned before the authorised tail computation.**
  - The registration and all 014 code are untracked. `000m` (commit 05d0bd1) carries no code hash.
  - `r014_tail.py` was last modified at 00:12:10, after the options pull ended (log at 00:11:56) and
    7 s before `tail_results.json` (00:12:17). `ct/shortvol.py` changed at 00:13:42.
  - "Entry, instrument, hedge, criteria and calendars are unchanged" therefore rests on the drafter's
    word.
  - It is corroborated in part: the docs log of 2026-10-12 records the same costs (4.9 %), floors,
    discovery power (0.21–0.24) and net-zero false-pass rates as Revision 1 §3.
  - Fix: commit the DRAFT files now, so the next revision is a diff. Any future ledger authorisation
    of a real-data computation pins the code hashes first. The `000m` addendum says so.
- **m5. Tail disclosure counts are wrong** (read from `tail_results.json`).
  - **Settlements on the perp close: 12 coin-weeks, 11 of them in selected weeks.** The registration
    says "8 coin-weeks", "thin 2019 ETH".
    - They include BTC 2019-03-29 and ETH 2020-01-10, **2020-03-06 (the COVID week)**, 2020-03-13 and
      2020-07-24.
    - Covered minutes were 2–9.
  - Strike fallbacks: §5 says 8, §8 says 6. The file has 8.
  - Rounding: §5 prose gives −12.4 % for 2021-01-01 against −12.3 in the table (−0.1235), and
    −5.3 % for 2020-03-06 against −5.2 (−0.0525).
  - The IV proxy was carried forward for up to 63 of 169 hours (ETH 2019-09-13). Report the
    distribution.
- **m6. Tail provenance.**
  - §12 quotes the two tail SHA256SUMS hashes by prefix only. `REALISTIC_WEEK` (hashed inside
    `ct/shortvol.py`) is the frozen output.
  - Put the full hashes in §9:
    - `underlying_20261013`: `b082f793401197786e98319c77a6b7c2ae8f4a583531d475676f8a3fb2998e17`
    - `options_20261013`: `fd1fe71727f00a9112c384859f395f4e2e8201371a49a33ddb64da74bbcd68f7`
- **m7. Environment.**
  - `versions()` records `platform.python_version()` only. The decision path is stdlib-only, so this
    is nearly enough.
  - Add `sys.version`, `sys.executable` and `sys.flags`. Refuse under `-O` (m2).
- **m8. Rows are matched by exact file name.** A renamed registration (`014-…-r2.md`) is not blocked
  by (f) or (i). Match `registration.startswith('014-')`.
- **m9. Failure ordering in `run_discovery`.**
  - `reported_blocks` (7 variant passes plus 2 placebos) runs after the decision is computed and before
    the result row is appended.
  - An exception there leaves only `discovery_started`. Discovery is burned with no row, and the
    pressure to erase that row (M1) follows.
  - Fix: append the primary result row first, then a separate `discovery_reported` row. Do the same
    in `run_holdout`.
- **m10. Unverified stamp semantics for DVOL.**
  - Perpetual candles are stamped at the **open**. I verified this out of calendar: each close stamped
    t sits 8–9 bp from the option-tape index just before t + 1 h, against 35–79 bp just before t, in
    2020-07, 2020-11 and 2021-02.
  - DVOL uses the same `r[0] + HOUR` mapping without a check.
  - If DVOL were close-stamped, the hedge and liquidation IV would look 1 h ahead. The stress flag
    would only become more conservative.
  - Add a check, for example DVOL's first row on 2021-03-24 against Deribit's published start.
- **m11. Wording that over-claims.**
  - §0 and the runner docstring say "in any branch's history". It is not the reflog and not the stash
    (M1).
  - §2 says "`Panel` refuses any other path" (M2).
  - §9 says "`shortvol_sim` imports nothing the run uses". It imports `shortvol`. The run does not
    import `shortvol_sim`, except `--power`.
- **m12. Strike distance is not reported.** The chosen strike may lie up to 2.5 % from the index. Add
  the distance distribution (a pre-entry fact) to the guarded run's pre-decision counts.

---

## Checked and found sound

- **Pull integrity** (`probe_pull_bounds.py`).
  - 555 files equal SHA256SUMS (sha `ca64f344…f1bd7`, as in §2).
  - DVOL runs 2021-03-24 00:00 → 2026-06-30 23:00 (46,200 rows per coin). Perp runs 2021-03-01 00:00 →
    2026-06-30 23:00 (46,752). Funding runs 2021-03-01 01:00 → 2026-06-30 23:00 (46,687).
  - 548 trade files hold 928,711 trades. Every trade lies inside its [F − 4 h, F) window, and no file
    is empty.
  - **Latest stamp 2026-06-30 23:00; 0 rows on or after 2026-07-01.**
- **Panels.**
  - Both satisfy files = MANIFEST = §9, with no extra files.
  - Bounds: discovery series [2021-03-01, 2024-01-01), Fridays 2021-04-02 → 2023-12-29. Holdout series
    [2023-12-24, 2026-07-01), Fridays 2024-01-05 → 2026-06-26.
  - The MANIFEST pull id equals the pull. Guard (e) checks both panels in both modes.
  - The holdout content is pinned. The 8-day look-back before 2024-01-05 is discovery-period data and
    carries no outcome.
- **Calendars.** 143 / 129 entries. The 2023-12-29 entry (expiry 2024-01-05) belongs to neither. Each
  settlement window lies in the next Friday's file, which is in the same panel.
- **Fetch.**
  - The end bound is checked with `raise` before every request, including `r014_tail.tget` (< 2021-03-24).
  - Every received row is checked with `raise` (`assert_pre_cutoff`, `below`).
  - Errors are typed and retried, then raised, and are never written. Writes are atomic.
  - Forward paging restarts inclusively and de-duplicates by `trade_id`. More than 1,000 trades on one
    millisecond is an error.
  - A DVOL `continuation` is an error, an empty funding window is an error, and the tail of every
    series is checked.
  - Avoiding `get_instruments` (today's state) and `get_delivery_prices` (no end bound, newest first)
    is right. Either would return rows past the cutoff.
- **Settlement reconstruction.**
  - The window is [X − 30 min, X); the 08:00 trade is excluded.
  - The price is the mean of per-minute medians of trade `index_price`, with at least 10 covered
    minutes, else the perp close.
  - That matches Deribit's 07:30–08:00 index TWAP closely enough, and all 544 calendar settlements
    qualify.
  - Expired instruments come from the history server (`include_old`). The strike universe is "traded
    in the window", a registered rule rather than survivorship. The coverage shows chosen-strike trade
    age p50 2–5 min.
- **Point in time.**
  - The stress flag uses DVOL bars opened ≤ E − 2 h (closing ≤ E − 1 h).
  - The perp `r[0] + HOUR` close mapping is verified (m10).
  - Hourly funding is applied to the hedge held over (t − 1 h, t].
- **Coverage counts (leakage).**
  - `coverage.json` stores only per-calendar, per-coin aggregates: availability classes, index source,
    trade-age quantiles, a settlement class (≥ 10 minutes: all pass), daily-expiry presence, window and
    trade totals, and missing hours.
  - Nothing is stored per week.
  - The settlement count touches the hold's end, but it is reduced to a class that every week passes.
  - The 34 / 30 missing funding hours are identical for BTC and ETH (exchange-wide) and are aggregated.
  - Unlike 013 (review B, M5), no per-event file of hold-window counts exists.
  - I judge the residual information to be negligible.
- **Order of outputs.**
  - Discovery appends `discovery_started` before any outcome is computed. The decision and statistics
    are printed only after the result row is appended. Only the counts are printed earlier, by design.
  - The holdout follows the same order.
  - `--power` reads nothing.
- **Tail (out of calendar)** (`probe_tail.py`).
  - Both tail SHA256SUMS verify (5 and 57 files).
  - The underlying runs 2019-01-01 (ETH 2019-03-14 13:00) → 2021-03-23 23:00. Funding starts
    2019-04-30 10:00.
  - The 56 option files all lie inside [E − 24 h, E + 7 d). **Max stamp in any tail file: 2021-03-23
    23:00.** The last computed week (2021-03-05) ends on 2021-03-12.
  - **Re-deriving the selection from the underlying alone gives exactly `selected.json`'s 21 weeks of
    115.** None of them was selected only because it was named.
  - `REALISTIC_WEEK` equals `tail_results.json`'s book values exactly.
  - No week liquidates, so the book is n-free and "none at 0.40" follows by monotonicity.
  - The tail charge is 0.6709 / 390 = 0.172 %/wk.
- **Hashing.** Every module a guard pass loads is in §9, and `ct/__init__.py` is the empty file. The
  shadow-module gap is M3.
- **Guard checks that do work.**
  - (a)–(e) as described. (g3) catches prose added to the registration and code changed after the run.
  - (h') rejects an approval that quotes the wrong sha. In the top-level layout it requires the approval
    strictly after the row's commit.
  - (g4) catches forged labels and a summary that differs from the weekly series.

## Verdict

**Not ready to register: 1 blocker, 4 majors.**

- **B1** (`HEAD:./` paths) is a two-line fix. Without it, a discovery pass would strand the holdout
  behind (g3).
- **M1–M3** port what `r008_run.py` already has: `--reflog -m`, auto-commit of `results.jsonl`, a
  clean `results.jsonl` at start, a scrubbed git environment and pinned top level, a gate token
  required by `Panel`, the untracked-`.py` check, a `sys.modules` audit and a fresh `pycache_prefix`.
- **M4** is a mechanics fix in `straddle_entry`. It needs no new pull (`price` and `iv` are kept), but
  it does need a tail recompute under `000m` and a re-derived n.

All fixes stay in 014's own new files and change §9's code hashes only. The panels and pull stand.
After the fixes, one re-review of the guard and of the mark function is enough.

## Reviewer's real-data reads (for the ledger)

1. **Real guard, read-only:** git metadata of the repo; SHA-256 of the 7 code files and 10 panel files.
   Panel bytes were hashed, never parsed.
2. **Pull `data/r014/pull_20261012/`:**
   - the SHA256SUMS check;
   - **timestamps only** of `dvol/`, `perp/` and `funding/` (2021-03-01 → 2026-06-30 23:00);
   - **timestamps only** of all 548 trade files (Fridays 2021-04-02 → 2026-06-26, [F − 4 h, F)).

   No price, IV or mark was used.
3. **Panels:** `MANIFEST.json` of both (bounds, row counts, hashes). `logs/coverage.json`, `build.log`,
   `pull_20261012.log`, `power_run.log` and `pull_report.json` (counts).
4. **Tail `data/r014/tail_pre2021/`, all before 2021-03-24:**
   - both SHA256SUMS;
   - stamps of every file;
   - perp hourly candles 2019-01-04 → 2021-03-19, to re-derive the excursion selection;
   - flat-candle counts 2019-01-01 → 2021-03-23;
   - `selected.json`;
   - `tail_results.json` (book values, settlement sources and minutes, constructions, IV-carry counts);
   - BTC perp closes against option-tape index prints in the weeks from 2020-07-24, 2020-11-20 and
     2021-02-19 (stamp semantics only).
5. **One network request, free and keyless:**
   - `history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time`, BTC options, 2020-01-03
     07:30:00.000 → 07:59:59.999 UTC, 50 trades;
   - used for the field list and for forward-versus-index from each trade's `price` and `iv`;
   - saved in the session scratchpad, not in the repo;
   - out of both calendars.
6. **Not done:**
   - no `--discovery` or `--holdout`;
   - no `Panel` opened on real data (the audit hook aborted each attempt at `open()`);
   - nothing dated on or after 2026-07-01;
   - no edits to the draft's files, no commits, nothing published.
