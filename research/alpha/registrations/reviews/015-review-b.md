# 015 — Review B: guard, integrity, data, calendar

Reviewer B, 2026-10-13. Draft at `012eff7`, `research/alpha/registrations/015-month-end-liquidity-cycle.md`.

**How this was checked.**
- Probes and their outputs are in `research/alpha/scratch_review015b/` (`*.sh` / `*.py`, each with a `.out`).
- Every attack ran in a throwaway repository in the session scratchpad, on synthetic data.
  - `make_synth_clone.py` builds a stand-in for the real repository. It runs `git clone --local` of the real
    repository at `012eff7`, so the real history (anchor `15c6421` included) and byte-identical `r015_*.py` are
    present.
  - It writes synthetic data with the suite's own `build_synthetic_tree()`, rewrites the registration's data hash
    rows to the synthetic hashes, sets REGISTERED and commits.
- The real repository was only read. Its data files were opened for hashes, row counts, dates and format only.
  `r015_run.py --run` was never executed against real data.
- The forged `statistics` module is stored as `evil_pythonpath/statistics.py.txt`, so it can never be imported
  from the repository. The probe copies it to a temporary directory.

## Verdict: register after the listed fixes

The data path, the loader cut-off, the hashes, and the calendar and its SEC dates are sound (see "Checked and
sound" below).

The guard has one blocker and three majors. All four are mechanical fixes, and each comes with a test.
- **B1:** the guard binds to "a repository", not to the real one.
- **M1:** `run(root)` computes the decision from a tree the guard never checked.
- **M2:** `-I` is not enforced.
- **M3:** the guard trusts git's view of the working tree.

B1 and M1–M3 must land, with their tests, before the REGISTERED commit. The guard diff then needs a short re-look.
Of the minors, m3 (missing trailing newline) and m8 (the "016-forward" name) are one-liners and should also land.

---

## BLOCKER

### B1. The guard binds to any clone or worktree that has the data, not to the real repository

**The claim (§10 (w)).** `git rev-parse --show-toplevel`, asked from the runner's directory, equals the repository
two levels above it, and the anchor is an ancestor of HEAD ("a passed-in path is never trusted"). Every clone and
every linked worktree of this repository satisfies both conditions by construction.

**Attacks.**
1. **The real guard opens in a clone, on the real data** (`probe_staleness_and_real_clone.sh`):
   - Setup: a `git clone --local` of the real repository, status flipped to REGISTERED and committed, with
     `research/alpha/data` symlinked to the real data tree.
   - Result: `python3 -I research/alpha/r015_run.py --check` prints **`guard open (registration commit 207d0df…)`**.
   - The drafter's own `scratchpad/commit_sim.sh` does the same thing. It copies the real data into the clone with
     `cp -R …/data/r015`, runs `--check` in the REGISTERED clone, and §10 reports "guard open". Then it runs `rm -rf`
     on the clone.
   - Changing that one flag to `--run` would read the real outcome, and every row would be deleted with the clone.
2. **A full run in a clone stays in the clone** (`probe_b1_clone_runsite.sh`, synthetic):
   - `--run` in the clone commits `started`, `result` and `reported` there.
   - The origin's `--check` afterwards still prints `guard open`.
   - The origin cannot even resolve the clone's result commit.
3. **Inside one repository, a linked worktree erases its rows** (`probe_b1_worktree.sh`, synthetic):
   - `git worktree add --detach` with the data symlinked: `--run` completes.
   - While that worktree exists, the main worktree's `--check` refuses with (f), because `--all` covers other
     worktrees.
   - After `git worktree remove --force`, it prints **`guard open`**. No gc and no reflog expiry was needed.
   - On a detached HEAD in the main worktree, the rows survive only in the HEAD reflog. They are then gone after
     `gc.reflogExpireUnreachable`.

**Why this is a blocker.**
- Run-once is the property the trial rests on ("never evaluate without a ledger row").
- "Guards bound to the real repo" is the first item under "Integrity faults recur until ported" in the orchestrator
  brief. 014 review B, M1 asked for "the known repo top".
- 015 ported that check as a relative path, which every copy satisfies. The documented procedure has already opened
  the real guard outside the real repository.

**Fix.**
- Pin `REAL_REPO = Path('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline')` in `r015_run.py`.
  The file is hashed, and §10 should state the path. `guard()` refuses unless `REPO.resolve() == REAL_REPO`. Because
  the symlinked-data clone has `REPO` = the clone, it fails this check.
- Main worktree only: `git rev-parse --absolute-git-dir` must resolve to `REAL_REPO/.git`. In a linked worktree it
  resolves to `.git/worktrees/<x>`.
- HEAD must be a named branch: `git symbolic-ref -q --short HEAD` equals the branch named in §10
  (`alpha-discovery`). A detached HEAD is refused.
- `guard()` keeps taking no arguments and uses only constants captured at import time (see M1).
  `guard_failures(...)` stays parameterised for the throwaway tests.
- Procedure text: in the commit-state simulation, the REGISTERED clone must now print exactly one refusal, the (w)
  path. "Guard open" in a clone becomes a test failure.

**Tests.**
- A clone with copied data refuses with (w).
- A clone with symlinked data refuses with (w).
- A linked worktree refuses with (w).
- A detached HEAD refuses with (w).
- The real repository shows no (w) (this test already exists).

---

## MAJOR

### M1. `run(root)`: the real guard checks the real tree, but the decision comes from `root` and is written to the real ledger

**Attack** (`probe_m1_run_root.py`). The setup is the synthetic stand-in, playing the real repository with null data.
The call `R.run(other_tree)` uses no monkeypatching: default commit hook, real `guard`, real `RESULTS` and `DATA`, so
`real_tree_failures() == []`.

```
decision recorded in the REAL-standin ledger: exists_paper  t = 3.69
REAL-standin HEAD:results.jsonl modes: ['started', 'result', 'reported']
row data_hashes == REAL-standin registration table? False
row US file hash == the hash in the REAL-standin table? False | actually read from …/synthX/…/F-F_Research_Data_Factors_daily_CSV.zip 56fddc4f05ff vs real 3964ba2e6edf
```

- `guard()` checks `HERE`, but `primary()` and `reported()` read the data, the calendar and even the hash table from
  `root`.
- `real_tree_failures()` checks the `RESULTS` and `DATA` globals, not `root`.
- Nothing compares the row's `data_hashes` and `code_hashes` with the registered table, and `recheck` recomputes only
  from the stored per-month records.
- Consequence: after REGISTERED, a harness or a test edit that calls `run(<synthetic tree>)` with the real guard
  burns 015, with a synthetic decision committed in the real ledger.
- §10's "a passed-in path is never trusted" is false for the computation path.

**Fix.**
- The production `run()` takes no argument and reads only `HERE`-anchored constants captured at import.
- The throwaway entry (`_run_in(root)`) refuses unless `root`, `RESULTS` and `DATA` are all outside the real tree.
  This adds `Path(root).resolve() == _HERE0` as a third member of the "used only together" rule.
- `primary()` refuses unless the table it uses equals the table in `HEAD:./registrations/015-….md` of the real
  repository.
- `recheck` also requires `row.code_hashes` and `row.data_hashes` to equal that table.

**Test.** Run `run(other)` with the real guard, the stand-in bound through the parameterised `guard_failures`. It must
raise before `append_row`, so the `results.jsonl` line count is unchanged.

### M2. Only `python -I` keeps injected modules off the decision path, and the guard does not check it

**Attack** (`probe_m2_isolation.sh`, synthetic clone). A forged `statistics.py` on `PYTHONPATH` loads the real module
and shifts `fmean` by +3 %. The run omits `-I`.

```
== --check without -I, PYTHONPATH=evil:     guard open
== --run without -I, PYTHONPATH=evil (NULL synthetic data):  "t": 7.745…   decision: exists_paper
row env … stdlib_sha256 f8284a947bd0   (honest -I fingerprint c0533ba7bb6c)
== honest -I run on the same null synthetic data:            "t": -1.614…   decision: not_demonstrated
```

- `recheck` passes because it uses the same forged module.
- The changed stdlib fingerprint is recorded but never compared with anything.
- The same route is open through:
  - a user-site `.pth` (`site.ENABLE_USER_SITE` is True without `-I`);
  - a `git` earlier on `PATH` (every check is an answer from git);
  - `~/.gitconfig` (m4).
- (v) checks only `-O`, the version, files at the top level of `research/alpha` and `__file__`.
- `-I` matters: with it, `PYTHONPATH` is ignored (shown in the probe). The procedure asks for `-I`, but nothing
  enforces it.

**Fix (all in (v)).**
1. Refuse unless `sys.flags.isolated`. Recommend `-I -S` and check `sys.flags.no_site` too. Homebrew's site-packages
   has no `.pth` today.
2. After the imports, refuse if any module in `sys.modules` has a `__file__` outside the stdlib directories of
   `sys.base_prefix` other than `r015_core.py`, `r015_hmm.py` and `r015_run.py` in `HERE`.
3. Refuse if any module has a `__cached__` outside `sys.pycache_prefix`. This catches m1.
4. Pin the stdlib fingerprint in §10 and compare it. That is stronger than the version pin and survives m10.
5. Call git by absolute path, and record its path and `--version`. After stripping `GIT_*`, set
   `GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_NOSYSTEM=1` and `GIT_NO_REPLACE_OBJECTS=1` (m4).

**Tests.**
- `python3 r015_run.py --check` without `-I`, run as a subprocess, refuses with (v).
- A `-I -c` entry that puts a forged `statistics` on `sys.path` before `import r015_run` refuses, naming the module.
- The m1 forged-pyc case refuses.

### M3. The guard trusts git's view of the working tree: skip-worktree, assume-unchanged and clean filters hide edited code

**Attack** (`probe_m3_skip_worktree.sh`, synthetic clone).
1. Edit `r015_core.py` so that both means get +3 %.
2. Put the edited file's hash into the working-tree registration.
3. Run `git update-index --skip-worktree` on both files.

```
== edited, no flags:          (b) … differs   (d) guarded code not clean   (v) … ['r015_core.py']
== edited + skip-worktree:    guard open
git status --porcelain: []  ls-files -v: S  S
== --run (NULL synthetic data):  "t": 7.745…   decision: exists_paper
row code hash of r015_core.py: 05ac31472735 | committed registration says 123a977b37d0
```

Each check that should have refused relies on a view git hides:
- (b) uses `git diff`;
- (d) uses `git status`;
- the shadow scan uses `ls-files -m`, which skips such entries;
- (e) reads the hash table from the working-tree registration.

`--assume-unchanged` does the same. A `.gitattributes` clean filter does it too, because diff and status compare the
filtered content.

**Fix.**
- Read the status line and the hash table from `git show HEAD:./registrations/015-….md`, never from the working tree.
- For the registration, every guarded code file and `results.jsonl`, require
  `git hash-object --no-filters <f>` == `git rev-parse HEAD:./<f>`.
- Refuse if `git ls-files -v` marks any of them `h` (lowercase) or `S`.
- The shadow scan uses the same blob comparison instead of `ls-files -m`.

**Tests.**
- Skip-worktree on the code and the registration refuses with (b) and (d).
- Assume-unchanged on the code and the registration refuses with (b) and (d).
- A committed `.gitattributes` with `filter=x` plus a local clean filter refuses.

---

## MINOR

- **m1. `loaded_from_source()` checks `__file__` only.**
  - Probe (`probe_minor_bytecode_gate.py`): a forged `__pycache__/r015_core.cpython-314.pyc` (CAL_CRIT 9.99), with
    the source's mtime and size, loads whenever `r015_core` is imported before `r015_run`. That covers any harness or
    test, though not the CLI.
  - Output: `CAL_CRIT in use 9.99 | loaded_from_source() -> []`. The control, importing `r015_run` first, gives 1.91.
  - Fix: the `__cached__` check in M2.
- **m2. The gate is a path check on mutable globals** (`probe_minor_bytecode_gate.py`).
  - (a) A byte copy of the guarded US zip outside `data/r015` loads with no guard (3,213 synthetic rows).
  - (b) Stubbing `guard_failures`, not `guard`, makes the real `guard()` issue the token, and the tree opens.
  - Fix: `load_french` and `load_treasury` already hash the file. Refuse any file whose SHA-256 appears among the
    registered data hashes unless the real guard passed in this process. Have `guard()` call only references captured
    at import.
  - Tests: a copy of a guarded file is refused; a stubbed `guard_failures` is refused.
- **m3. A missing trailing newline hides a 015 row** (`probe_minor_rows.sh`).
  - `results.jsonl` is shared with 016. When it ends without `\n`, the `started` row is appended onto the 016 row.
  - `record()` commits the merged line and then refuses ("not in HEAD"). The line no longer parses as JSON, so (f)
    misses the 015 row and the guard reopens. The 016 row is corrupted too.
  - Fix: `append_row` refuses unless the file is empty or ends with `\n`. (f) also refuses on any added line that
    contains `"registration": "015-` but does not parse.
  - Test: a file without a trailing newline is refused before the append.
- **m4. Two blind spots in (f) under deliberate git operations** (`probe_minor_rows.sh`).
  - With `log.showRoot=false` in a config file, a row committed as the root of an orphan branch is not seen.
  - `git replace <row commit> <decoy>` hides the row with no gc.
  - Fix: run `log` with `--root`, `-c log.showRoot=true` and `--no-replace-objects` (or `GIT_NO_REPLACE_OBJECTS=1`).
  - Test: both cases refuse with (f).
- **m5. The loader-gate tests are vacuous** (`probe_minor_vacuous_gate_test.sh`).
  - With both `_gate()` calls deleted (a mutant), "the gate: load_french …" and "the gate: load_treasury …" still
    pass. They pass `'x'*64`, so the hash check refuses anyway.
  - Fix: make `R.sha256` raise while these checks run, and assert that the refusal text is the gate's message.
- **m6. D14 (no restart clause) and a transient lock. This answers open question 2** (`probe_minor_index_lock.sh`).
  - A `.git/index.lock` at the `started` commit (an IDE's background `git status`, or 016 committing) makes the run
    refuse before any data is opened. After that, (k) and (f) refuse forever.
  - Recommendation:
    - retry the commit on a lock (3 × 1 s);
    - store `stage` and `data_opened` in every `failed` row, and catch `BaseException` for that row;
    - allow one restart only when the 015 rows contain no `result` row and every row has `data_opened: false`.
  - Otherwise keep D14.
- **m7. Coupling with 016. This answers brief item 3** (`probe_016_coupling.out`).
  - Today 015's scan refuses on `['ct16/__init__.py', 'r016_fetch.py', 'r016_run.py']`. **Refusing is correct:** the
    scan fails closed, and a scan narrowed by name can miss a shadow.
  - **There is no deadlock.** 016's scan (top level plus `ct16/`) sees 015's files as tracked and clean, so each
    runner opens as soon as the other's code is committed and unmodified.
  - It is a two-way **sequencing** constraint:
    - 015 cannot run while any 016 file is untracked or edited in the working tree, which includes 016's revisions.
    - 016's time-gated step refuses while a 015 revision is uncommitted.
  - Recommendation:
    - Keep the broad scan.
    - Add to §10's procedure: "every top-level `.py` and package in `research/alpha` (016's DRAFT included) committed
      before `--check`".
    - Make the refusal message name that remedy.
    - Never leave either draft's revision uncommitted across the other's scheduled step.
  - Reviewer scratch directories have no `__init__.py`, are not at the top level, and block neither runner.
- **m8. The forward name collides with 016.**
  - §8 and D13 call the forward registration "016-forward", but 016 is now crypto trend.
  - With 016's runner `REG_PREFIX = '016-'`, a "016-forward" row would block 016, and 016's rows would block the
    forward step.
  - Fix: use the next free number when the forward is registered.
- **m9. The forward overlaps 015's read.**
  - `DATA_END = 2026-06-10` loads 2026-06-04 … 06-09. 015 uses those days only in the HMM block (all days through
    2026-06-09).
  - They are also 2026-06's out-of-window days, which feed §8's forward μ.
  - Fix: set `DATA_END = 2026-06-04`, which is still after 2026-05's T+3 = 06-03, or end the HMM block at 06-03.
- **m10. The Python pin depends on Homebrew.**
  - Both `python3` and the copytrade `.venv` resolve to Homebrew's `python@3.14` (3.14.8). A `brew upgrade` before
    the run makes (v) refuse.
  - Fix: state the recovery (install 3.14.8 with uv or pyenv; the pin is never edited), or pin the stdlib fingerprint
    (M2).
- **m11. Nits.**
  - The loaders hash the file, then reopen it to parse. Hash the bytes that are parsed.
  - `recheck` could verify y_m = v_m − (2s−4)·μ_s against the stored `mu_by_era`.
  - "Japan … deleted unread" is true of 015's pull, but the B1 scout's Japan zip remains in
    `data/scout_b1/french_20261013T090723Z/`. Say so.
  - The ledger spans both roots, but (f) scans only `research/alpha/results.jsonl`. Also scanning
    `research/copytrade/results.jsonl` costs one call.

---

## Checked and sound

- **Data files** (`probe_data_format.py`; hashes, counts, dates and format only).
  - All 23 table rows match.
  - US zip: one member; header `,Mkt-RF,SMB,HML,RF`; 26,317 rows, 1926-07-01 → 2026-08-31, strictly increasing;
    57 rows ≥ 2026-06-10; 3,127 rows in [2014-01-01, 2026-06-10).
  - Europe zip: 9,436 rows; 59 rows ≥ 2026-06-10.
  - Both French zips are byte-identical to the B1 copies.
  - Treasury 2014–2026: 249–251 rows per year, 195 in 2026; 12/13/14/15-column layouts. The loader finds `10 Yr` by
    name. 85 rows in 2026 are ≥ 2026-06-10. All 13 files are byte-identical to `scout_b1/treasury_20261013T091152Z`.
- **The loader.**
  - Rows outside [2014-01-01, 2026-06-10) are skipped on their date before `split`/`float` (tested with non-numeric
    rows).
  - A hash mismatch is refused before parsing.
  - −99.99 and non-increasing dates are refused.
  - The real tree is gated in `load_french` and `load_treasury`; Europe and the Treasury files go through them.
  - `read_dates_only` reads the first field only.
  - The HMM and power modules have no file I/O. Power reads only `calendar.json`, which holds dates.
  - Nothing prints a statistic before the guard. `--check` prints reasons, `--calendar` prints counts, and `primary`
    prints calendar counts and then the decision only after the row is in HEAD.
- **Calendar** (`probe_calendar_dates.out`; dates only).
  - 149 months, 44 / 80 / 25, 64 Friday-T, 1,332 out-of-window days, 456 placebo days dropped. No weekend dates.
  - Closures and holidays are absent from the dates: 2018-12-05, 2025-01-09, 2024-05-27, 2017-09-04, 2020-04-10 and
    2022-06-20. The calendar is point-in-time from the file's own trading days.
  - Era boundaries: the first T+2 month is 2017-09, the last is 2024-04, and the first T+1 month is 2024-05.
  - 2017-08: P ends 08-25 (T−4) and R runs to 09-06. 2024-05: P = 05-20 … 05-29 (T−2).
  - The lag rule's unique fixed point matches the SEC compliance dates, trade-date basis: 34-80295 → 2017-09-05 and
    34-96930 → 2024-05-28.
  - Europe moves to T+2 on 2014-10-06 (9 / 140 months).
  - The calendar equals B1's for all 149 months (the suite's test).
  - **Open question 3:** keep 2024-05. The rule fixes it, and excluding it would be a post-hoc choice.
- **Staleness** (`probe_staleness_and_real_clone.sh`).
  - In a clone at `012eff7` (DRAFT, committed): **137 passed, 0 failed**.
  - After a REGISTERED commit: **137 passed, 0 failed**.
  - No test asserts a commit or status state of the real repository.
- **Checks that hold against their stated attacks.** The suite's throwaway tests and my clone probes confirm these:
  - (s), (a) and (c);
  - (b), (d) and (e) without index flags (see M3);
  - (k);
  - the nested `git init` case of (w);
  - (f) for uncommitted, erased, reset, amended, stashed, other-branch and renamed-prefix rows, and with `-diff` or
    `diff.external`;
  - `GIT_*` stripping;
  - `HEAD:./` in the subdirectory layout (the 014 B1 failure is shown fixed);
  - a no-op hook refused, `-O` refused, and the fresh `pycache_prefix` for the CLI entry;
  - the shadow scan for untracked, ignored or modified `.py`, sourceless `.pyc`, `.so` and packages;
  - `record()` verifying the line in HEAD, and the decision printed only after the result row is committed.
- **Run once on the normal path.** A second run is refused. In the real `.git` there are no hooks, no `hooksPath`, no
  signing, and no filter, `showRoot` or replace config.

## Reviewer's real-data reads (for the ledger)

- SHA-256 of every file in `data/r015/` and of the B1 French and Treasury copies.
- Row counts, first and last dates, header lines, column counts and sortedness of the two French zips and the 13
  Treasury CSVs.
- The `calendar.json` dates and summary.
- The 015 suite was run in a throwaway clone with the real data symlinked. It reads hashes, the dates-only calendar
  and the power log's calibration fields.
- `r015_run.py --check` was run in that clone: hashes only, computes nothing.
- `shadow_modules()` was run on the real `research/alpha`: a file listing.
- **No value column was parsed; no return, mean or statistic of any real file was computed or seen.**
