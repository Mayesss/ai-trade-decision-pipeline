# 016 — Review B: guard, fetcher, integrity, data

Reviewer B, 2026-10-13. Draft at `66df71a`: `research/alpha/registrations/016-crypto-trend-verbatim.md`,
`r016_run.py`, `r016_fetch.py`, `ct16/*.py` and `tests/test_r016.py`.

**How this was checked.**
- Probes and their outputs are in `research/alpha/scratch_review016b/`. Each `*.sh` or `*.py` has a matching `.out`.
- Every attack ran in a throwaway repository in the session scratchpad, on synthetic data.
  - `make_synth_clone016.py` builds a stand-in for the real repository.
    - It runs `git clone --local` of the real repository and checks out `66df71a`. The real history (anchor `9c131e2`
      included) and byte-identical 016 code are therefore present.
    - It writes a scout-like pull and the static exchangeInfo with the suite's own `_synthetic_tree()`.
    - It pickles the forward months for the suite's mock hooks.
    - It rewrites the two data hash rows to the synthetic hashes, sets the status to REGISTERED and commits.
  - "After 2028-01-08" is simulated in one way only: a throwaway venv (Python 3.14.8) whose site-packages holds a
    forged-clock `.pth`. Its source is `fakeclock_module.py.txt`, stored as `.txt` so it can never be imported from
    the repository. The runner is otherwise run through its own CLI, with `-I`.
  - The probe helpers refuse the real repository path.
- **The real repository was only read.**
  - `r016_run.py --run` and `r016_fetch.py` were never executed against it.
  - The 016 suite ran only in clones that had no data or synthetic data.
  - My reads of real files are listed at the end: hashes, names, months, counts and contract-type fields.

## Verdict: register after the listed fixes

The fetch path, the data the read depends on, the pinned pull, the point-in-time universe and the run-once
mechanics are sound for the attacks they were built against (see "Checked and sound").

The guard has every fault just found in 015, plus two of its own.
- **B1:** the guard binds to "a repository", not to the real one. A clone with the data symlinked runs the whole
  fetch → approval → read, and the origin stays open.
- **M1:** `fetch(root=<real repo>, store=<elsewhere>, hooks=<mocks>, now=<2028>)` passes every pre-check. The
  `now` argument opens the date gate on the real repository *today*, and the fetch commits real `fetched` rows.
- **M2:** isolation is not enforced.
  - `-I` is not checked.
  - Under `-I`, a site-packages `.pth` still runs.
  - The fetcher imports the guard module from `__pycache__`.
  - A committed top-level `csv.py` replaces the parser's `csv`.
- **M3:** index flags (`--skip-worktree`, `--assume-unchanged`) hide edited code and an edited hash table.
- **M4 (new, 016-specific):** nothing binds the 2028 fetch or read to the REGISTERED commit. A committed revision of
  `CAL_H1` and the hash table, made at any time in the next 15 months, passes every check.

B1 and M1–M4 must land, with their tests, before the REGISTERED commit. The guard diff then needs a short re-look.
Because the code is frozen for 15 months, the minor m7 (environment drift) and the trailing-newline case of m3
should land too.

---

## 0. Answers to the brief, in short

1. **015's list, applied to 016.**
   - B1 (clone, worktree), M2 (`-I`), M3 (index flags), m1 (bytecode: worse here, see M2), m2 (path gate, mutable
     global), m3 (trailing newline), m4 (`log.showRoot`, `git replace`), m5 (vacuous tests: seven of eleven
     mutants survive) and m10 (interpreter pin): **every one is present in 016**, in both the runner and the fetcher.
   - 015's M1 (`run(root)`) does not apply: `run()` takes no argument. **Its analogue is in the fetcher** (M1).
   - m6 (commit lock) is handled: the fetch is resumable, and the read has a restart line.
2. **The date gate (t)** is the wall clock, `datetime.now(UTC)`.
   - TZ, environment variables and git commit dates do not move it.
   - These do move it:
     - a `PYTHONPATH` `datetime.py` when `-I` is absent;
     - a site-packages `.pth` even under `-I`;
     - a forged `__pycache__/r016_run…pyc` on the fetch CLI;
     - the `now=` parameter on the real root (M1);
     - the system clock.
   - **The unforgeable part is the data.** The fetch cannot write a `fetched` row before the archive holds BTCUSDT
     through 2027-12-31, because `btc_months` and `btc_tail` are never persistable.
   - An early run still leaves a permanent `fetch_started` row, attempt rows that pre-age the persistence clock, and
     span-(c) files on disk (m1). The fix is a witness check, made before anything is written.
3. **The fetcher.**
   - Sound: forward paging with an exclusive `marker`; never caching an error, an empty answer or a bad checksum;
     atomic writes; the HL inclusive end handled as [a, b − 1 ms]; mocks refused on the real store; real hooks
     refused outside the guarded fetch.
   - It uses the archive only, so Binance REST `startTime`/`endTime` inclusivity and the 1000-row limits never arise.
   - Gaps (m5): no funding coverage rule; no tail check for symbols other than BTC; a resume adopts files already in
     the store without re-verifying them.
4. **The point-in-time universe** leaks no survivorship (m6 lists the minor issues).
   - The snapshot's `status` is never used.
   - The 31 pull symbols that are absent from it are not excluded.
   - The pull keeps 29 dead or delisted symbols, including LUNAUSDT to 2022-05 and FTTUSDT's dead rows to 2026-06.
   - The only symbols the 2026 snapshot excludes retroactively, among those with history before 2025-12, are three
     index perps (ALL, BTCDOM, DEFI), and the name list already excludes them.
   - Two issues are minor:
     - §3's claim that the perp-age rule is "irrelevant from 2025" is false for token migrations (POL, S, RENDER,
       SKY, A).
     - The "2026-10-11" snapshot carries `serverTime` 2026-10-09T07:16Z.
   - The LUNA cut, D9 (i), works on the archive's dead rows.
5. **Staleness.**
   - In a clone: 186 / 186 at DRAFT, at REGISTERED, and with 015's in-progress revision copied in.
   - With 015's revision uncommitted, 016's guard refuses with (d) on `r015_core.py`, `r015_power.py` and
     `r015_run.py`. That is correct: it fails closed, and nothing is due before 2028.
   - One check goes stale at approval time: "no approval or restart line exists yet" (m9).
6. **Real data seen.**
   - No code path reads a real price, volume or funding value before the guard. The guard reads hashes only;
     `Panel` opens files only behind the gate; the fetcher's reads come after the guard; `ct16` does no I/O; `--power`
     reads nothing.
   - The drafter's scratch scripts are synthetic only.
   - The drafter's commit simulation copied the real `SHA256SUMS` and exchangeInfo (metadata) into a clone. That is
     the B1 pattern: after 2028 the same procedure opens the guard in the clone.

---

## BLOCKER

### B1. The guard binds to any clone or worktree, not to the real repository (015 B1, unported)

**The claim (§11 (w)).** Git's top level is "this repository" and the anchor is an ancestor of HEAD.
- `guard()` passes `toplevel=REPO`, where `REPO = HERE.parent.parent`. Every clone and every linked worktree
  satisfies that by construction.
- The fetcher passes the same arguments. Its "real repository" test is `root == run016.HERE`, and `HERE` is the
  clone's own directory.

**Attack** (`probe_b1_clone_worktree.sh`, synthetic stand-in).

1. **A clone with the origin's data symlinked.**
   - Before the date, `--check` prints the same two lines in the origin and in the clone: (t) and (g1). (w) is
     silent.
   - After the date: `(g1) … fetch first`. A fetch with mock hooks and synthetic objects runs in the clone, its store
     is copied into the clone's `data/r016/forward` (which is the origin's data tree, through the symlink), and the
     approval line is committed.
   ```
   -- clone --check:  guard open: registration commit 6bd73389…
   -- clone --run:    outcome: alive_as_published route: paper_then_scale
   -- clone HEAD:results.jsonl 016 modes: fetch_started fetch_attempt fetched read_started read
   -- ORIGIN after the clone's run (--check, after the date):
   (g1) 0 fetch_started and 0 fetched rows; exactly one of each is required (fetch first)
   -- origin can resolve the clone's read commit? fatal: git cat-file: could not get object info
   ```
2. **A linked, detached worktree of the origin.** `--run` completes there. While the worktree exists, the origin
   refuses with (i), because `--all --reflog` sees it. After `git worktree remove --force`, the origin prints
   `(g1) 0 … fetch first` again: the read is erased with no gc.

**Why it is a blocker.**
- Run-once and fetch-once are what the trial rests on.
- The drafter's own commit simulation (`scratchpad/scripts/r016_commit_sim.sh`) is exactly the clone pattern. Its
  REGISTERED clone "refuses only on (t) and (g1)". From 2028-01-08 the same simulation with `--run` and the data
  symlinked would read the outcome and then delete it with `rm -rf`.
- The code is frozen at REGISTERED for 15 months, so this can only be fixed now.

**Fix** (as for 015, plus the fetcher).
- Pin `REAL_REPO = Path('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline')` in `r016_run.py`,
  and state it in §11.
- `guard()` and the real-hooks branch of `fetch()` refuse unless all of these hold:
  - `REPO.resolve() == REAL_REPO`;
  - `git rev-parse --absolute-git-dir` equals `REAL_REPO/.git` (the main worktree only);
  - `git symbolic-ref -q --short HEAD` equals `alpha-discovery`, or the branch named in §11 (no detached HEAD).
- Because of the 15-month horizon, add a recovery to the procedure that is not a code edit. A committed registration
  line `016 repo-path: APPROVED <date> <abs path>` is accepted by (r)/(g2) like the approval lines, so a moved
  repository can be re-pinned without touching hashed code.
- In the commit-state simulation, the REGISTERED clone must print the (w) refusal. "Only (t) and (g1)" becomes a test
  failure.

**Tests.** Each of these refuses with (w), in both modes:
- a clone with copied data;
- a clone with symlinked data;
- a linked worktree;
- a detached HEAD.

The real repository shows no (w) (read-only call).

---

## MAJOR

### M1. The fetcher's mock path accepts the real repository, and `now=` opens the date gate on it today

**Attack** (`probe_m1_fetch_root.py`, the REGISTERED stand-in in the role of the real repository, run on 2026-10-13):
```
today (wall clock): 2026-10-13 | real guard (t) present: True
call: F.fetch(root=RUN.HERE (the REAL-standin), store=<tmp>, hooks=<mocks>, now=2028-01-10)
fetched: 1251 files in the manifest after 1 attempt(s)
REAL-standin HEAD:results.jsonl 016 modes: ['fetch_started', 'fetch_attempt', 'fetched']
the REAL fetch on 2028-01-10: ['(f) the fetch has completed (a fetched row exists) — it runs once']
the REAL read on 2028-01-10:  ['(g3) the fetch manifest is missing', "(h') no committed line …"]
```
- `fetch()`'s pre-checks only protect the real **store**: "mock hooks are never used on the real data tree".
- With `root=HERE` and any other store, the real guard runs on the real repository with the caller's `now` and
  without (w)/(v). The rows are then committed into the real `results.jsonl` by `commit()`, which writes
  `root / RESULTS_REL`.
- After REGISTERED, a harness or test edit that passes `root=run016.HERE` therefore burns 016 permanently. The burn
  can happen in 2026, because `now` is a parameter.
- The same hole lets mocks write into a subdirectory of the real store: `real_store` is an `==` test, while
  `Panel`'s gate uses `is_relative_to`.

**Fix.**
- `fetch()` refuses unless **all three** of `root`, `store` and the results file are outside the real repository
  whenever `hooks` is given or `now` is passed. That is 015 M1's "used only together" rule, applied to `root`.
- The real path accepts no `now`, no `guard_kw`, and no `root` other than the import-time `HERE`, checked against
  `REAL_REPO` (B1).
- Use one containment test (`is_relative_to` both ways) for the store, as `Panel` does.

**Test.** `fetch(root=RUN.HERE, store=<tmp>, hooks=<mocks>, now=NOW)` must raise before any `commit`. The real
`results.jsonl` line count and `HEAD` must be unchanged afterwards. The same must hold for
`store=RUN.HERE/STORE_REL/'sub'`.

### M2. Isolation is not enforced, and three routes put foreign code on the decision path

1. **No `-I`** (`probe_t_dategate.out` (4)).
   - A `datetime.py` on `PYTHONPATH` that wraps the real module turns `--check` into `(g1) … fetch first`: the
     date gate is gone.
   - With `-I` it is ignored.
   - (v) never reads `sys.flags.isolated`.
2. **A `.pth` under `-I`** (`probe_t_dategate.out` (5)).
   - Run with `isolated 1 no_site 0`: `--check` prints only (g1), and the fetch-mode guard with `guard()`'s exact
     arguments returns `[]`. Only `-S` stops it.
   - The registered interpreter is `research/copytrade/.venv`. It is git-ignored, shared with copytrade, and already
     carries duckdb and pyarrow. Any later `pip install` into it (setuptools ships a `.pth`) runs code at every launch.
3. **The fetcher loads the guard from `__pycache__`** (`probe_m2_isolation.out`).
   - `r016_fetch.py` imports `r016_run` before `r016_run` sets `sys.pycache_prefix`.
   - A forged `__pycache__/r016_run.cpython-314.pyc`, stamped with the source's mtime and size, is loaded by the
     registered command `python3 -I r016_fetch.py`:
     ```
     FORGED r016_run loaded from __pycache__ (pycache_prefix=…/r016-pyc-…): the guard would return []
     ```
   - `LOADED` does not list `r016_run`. The real tree already holds `__pycache__/r016_run.cpython-314.pyc` (from the
     suite's subprocess check).
   - 015 had this only on harness paths (its m1). Here it is the CLI path, and the module replaced is the guard
     itself.
4. **A committed top-level stdlib name** (`probe_m2_committed_shadow.out`).
   - `HERE` is inserted at `sys.path[0]` before `ct16` is imported. The stdlib modules first imported after that
     point are `binascii`, `csv`, `struct` and `zipfile`.
   - A committed `research/alpha/csv.py` passes the shadow scan, because it is tracked and clean:
     ```
     --check (after the date): (g1) 0 fetch_started and 0 fetched rows …
     csv used by ct16.data: <clone>/research/alpha/csv.py | marker True | (v) loaded_from_source(): [] | shadow_modules: []
     ```
   - Over 15 months other studies will add top-level files to `research/alpha`. `calendar.py` or `stats.py`-style
     names are plausible, and a calendar study (015) already lives there.

**Fix (in (v), run by both `guard()` and the fetcher).**
- Refuse unless `sys.flags.isolated` and `sys.flags.no_site` are both set. Register the commands as `python -I -S`;
  the decision path is stdlib only.
- In `r016_fetch.py`, set `sys.pycache_prefix = tempfile.mkdtemp()` and `sys.dont_write_bytecode = True` **before**
  `import r016_run`.
- Add `('r016_run', 'r016_run.py')` and, in fetch mode, `('__main__', 'r016_fetch.py')` to `LOADED`.
- Refuse any module whose `__cached__` lies outside `sys.pycache_prefix`.
- Use `sys.path.append(str(HERE))` instead of `insert(0)`, so the stdlib always resolves first.
- Before `compute`, refuse if any `sys.modules` entry has a `__file__` outside `sys.base_prefix`'s stdlib other
  than the hashed files.
- Call git by absolute path, and record its path and `--version`. Run it with `GIT_CONFIG_GLOBAL=/dev/null`,
  `GIT_CONFIG_NOSYSTEM=1` and `GIT_NO_REPLACE_OBJECTS=1` (this also covers m3).
- Add the flags to `env_fingerprint()`.

**Tests.**
- `python3 r016_run.py --check` without `-I`, run as a subprocess, refuses with (v).
- `-I` without `-S` refuses with (v).
- A forged `r016_run` pyc in a throwaway tree is not loaded by `python -I -S r016_fetch.py`: the import is from
  source, under the fresh prefix.
- A committed `csv.py` beside the runner in a throwaway repository: the import resolves to the stdlib, and the
  provenance check passes. With `insert(0)` kept, it refuses, naming `csv`.

### M3. The guard trusts git's view of the working tree (015 M3, unported)

**Attack** (`probe_m3_index_flags.sh`).
1. Edit `ct16/stats.py` so that `CAL_H1 = 99.0`.
2. Put its hash into the working-tree registration.
3. Hide both files with `git update-index --skip-worktree`, or separately with `--assume-unchanged`.
```
== edited + --skip-worktree:  (g1) 0 fetch_started and 0 fetched rows …     git status --porcelain: []  ls-files -v: S S
== --check after fetch + approval:  guard open: registration commit 18598df…
== --run:  "crit": 99.0, … outcome: undecided route: not_demonstrated
   read row: params.cal_h1 = 99.0 | code hash of ct16/stats.py 1cc2be7da75b | committed table says df33f70154df
```
The `--assume-unchanged` case gives the same result.
- (b), (d) and the shadow scan (`ls-files -m`) ask git, and git hides flagged entries.
- (e) and the APPROVAL/RESTART searches read the working-tree registration.
- (g2) compares the fetched row with the disk, and both carry the edited hash.
- A `.gitattributes` clean filter gives the same result: diff and status compare filtered content.

**Fix.**
- Read the status line, the hash table and the approval and restart lines from
  `git show HEAD:./registrations/016-….md`, never from the working tree.
- For the registration, every guarded code file and `results.jsonl`, require
  `git hash-object --no-filters <f>` == `git rev-parse HEAD:./<f>`.
- Refuse if `git ls-files -v` marks any of them `h` or `S`.
- The shadow scan uses the same blob comparison.

**Tests.**
- Skip-worktree on the code and the registration refuses with (b) and (d).
- Assume-unchanged on the code and the registration refuses with (b) and (d).
- A committed `.gitattributes` `filter=x` with a local clean filter refuses.

### M4. Nothing binds the 2028 fetch or read to the REGISTERED commit (016-specific)

**Attack** (`probe_m4_revise_before_fetch.sh`).
1. After the REGISTERED commit `0c9828d`, commit a revision dated 2027-06: `CAL_H1 1.731 → 1.20`, with the hash table
   updated.
2. `--check` is identical before and after: only (g1).
3. Fetch, approve, and run:
```
"crit": 1.6991… (the floor now binds) | outcome: alive_as_published
read row: registration_commit 9f3e313d3531 (the REGISTERED commit was 0c9828d4ebcb) | cal_h1 1.2
git diff REGISTERED..HEAD: ct16/stats.py 2 +-, registration 4 +++-
```

**What this attack shows.**
- (c) takes the **latest** commit that touched the registration.
- (g2)/(r) only constrain changes after the **fetch**.
- (e) checks the current table against the current code.

**Why this is a major for 016 in particular.**
- For 008, 013 and 015 the fetch or read followed the registration by days. Here the gap is 15 months.
- §14 explicitly lets the owner trade the rule and watch span-(c) P&L in the meantime.
- The registration's central claim, "registered before its forward span exists", is then enforced only by someone
  reading `git log` in 2028.
- The read row records the revision commit and does not flag that it is not the registered one.
- There is a converse trap. After an early `fetch_started` (m1), *any* later registration commit, even a typo fix,
  makes (f) refuse forever: "the existing fetch_started row has another registration commit".

**Fix.**
- The guard derives `reg0`: the first commit on HEAD's first-parent history whose `HEAD:./registrations/016-…md`
  says `**Status: REGISTERED`.
- It requires:
  - `git diff --quiet reg0 HEAD -- <guarded code>`;
  - the registration's diff `reg0..HEAD` to consist only of approval, restart and repo-path lines (the (r) rule,
    anchored at `reg0` instead of at the fetch);
  - `fetch_started.registration_commit` and `fetched.registration_commit` to be `reg0` or a commit whose diff from
    `reg0` is only such lines.
- Store `reg0` in every row.
- The orchestrator writes `reg0` into the programme ledger at registration. The guard checks that the ledger line
  exists in `HEAD`, which is a second, cross-file pin.
- Any amendment becomes a new registration file with its own runner, and 016 stays frozen.

**Test.**
- In a throwaway repository: REGISTERED, then a committed edit to the code and the hash table. The fetch and the read
  refuse, naming `reg0`.
- A committed approval line alone still opens the read.

---

## MINOR

- **m1. The date gate: what an early run leaves behind** (`probe_t_early_fetch.out`).
  - Setup: a forged clock and mocks that serve only today's months (≤ 2026-09).
  - Result: `fetch stopped: … btc_months: BTCUSDT forward months 3 of 18`. That much is sound: no `fetched` row.
  - But the run committed `['fetch_started', 'fetch_attempt']`, wrote 72 span-(c) kline files (2026-07 → 09) to the
    store, and the fetch-mode guard now returns `[] … 'resume': True`.
  - In 2028 the resume adopts those files without re-verifying them (`path.exists()` → hash only), and counts the
    early attempts toward the 3 × 24 h persistence chain.
  - **Fix (witness first).** In the real-hooks path, before committing `fetch_started` or writing any file:
    1. list `…/klines/BTCUSDT/1d/` and require the `2027-12` key;
    2. require the HTTP `Date` header of that response (UTC) ≥ 2028-01-08.
  - Neither step reads data, and both are external to the machine's clock and Python.
  - Persistence counts only attempts made after `READ_NOT_BEFORE`.
  - **Test.** A mock listing without the 2027-12 key: refused before any row or file exists.
- **m2. The read gate is a path check on a mutable global** (`probe_minor_gate.out`).
  - A byte copy of the pinned pull outside `REAL_DATA` opens `Panel` with no guard, and its `SHA256SUMS` hash equals
    the registered one (816 files in the synthetic pull).
  - Assigning `_GATE` by hand opens the real path.
  - Stubbing `guard_failures`, rather than `guard`, makes the real `guard()` issue the token.
  - These are deliberate in-process acts, and the pull is designer-known span (a).
  - **Fix.**
    - `Panel` refuses any `SHA256SUMS` whose hash is in the registered table unless the real gate is set.
    - `guard()` calls only references captured at import.
  - **Tests.** A copied pull is refused. A stubbed `guard_failures` is refused.
- **m3. Run-once blind spots** (`probe_minor_rows.out`; 015 m3 and m4).
  - (a) `results.jsonl` is shared with 015. When it ends without a newline, 016's `record()` glues its row onto the
    015 line and commits it. It then refuses ("not in HEAD"), and `result_rows` sees `[]`: the 016 row vanishes. A
    glued `read` row would let the read run twice. The decision is never printed, because `record` raises first.
  - (b) With `log.showRoot=false`, a `read` row committed as the root of an orphan branch is not seen.
  - (c) `git replace <row commit> <decoy>` hides a `read` row, with no gc.
  - **Fix.**
    - `append_row` refuses unless the file is empty or ends with `\n`.
    - (f)/(i) refuse on any added line containing `"registration": "016-` that does not parse.
    - `log` runs with `--root -c log.showRoot=true`, plus the `GIT_NO_REPLACE_OBJECTS=1` of M2.
  - **Tests.** All three cases refuse.
- **m4. Seven of eleven guard and fetch protections can be deleted with the suite still green**
  (`probe_mutants.out`).
  - Method: each mutant deletes one protection in a clone. The clone's hash-table row is kept in step, so that only
    the targeted test can fail.
  - **Vacuous:**
    - the `HERE` term of the `Panel` gate;
    - "mock hooks never used on the real store";
    - the fetch's real-root pre-check;
    - `Panel`'s span-(a) month cap (`> 2026-06` refused);
    - `Panel`'s span-(a) row cut (`before=2026-07-01`);
    - the seam check (the suite always passes `a_sums={}`);
    - the BTC exemption from persistence (the suite tests `btc_months`, which is not persistable anyway, never
      `klines_error:BTCUSDT:…`).
  - **Caught:** the `Panel` gate, (t), fetch-once and the checksum.
  - The "mock on the real store" test passes only because the guard then refuses on (s)/(t). After REGISTERED and
    2028, that mutant writes mock data into the real store and commits a real `fetched` row.
  - **Fix.** One targeted test per vacuous mutant, each asserting the specific refusal text:
    - a synthetic pull with a 2026-07 file;
    - a 2026-06 file containing a 2026-07 row;
    - a seam case;
    - a `klines_error:BTCUSDT:2026-07` key over three attempts;
    - a mock fetch on the real store in a REGISTERED throwaway tree after the date.
  - An earlier version of this probe removed `_require_real()`. The suite's own "real hooks refuse" check then sent
    one GET for `https://data.binance.vision/x` (HTTP 404, no data). That mutant was deleted. The suite should run
    with a socket guard, so that no test can reach the network.
- **m5. Fetch coverage gaps.**
  - (a) **Funding has no coverage rule** (013 m1, unported). A missing month is zero funding, and it flatters the
    deployable variant that decides the route. Require a funding file for every (symbol, month) with a traded klines
    row in [2025-03, 2027-12]. Make it keyed and persistable, and count it in the report.
  - (b) **Only BTC's tail is checked.** Require every symbol that traded in the last 7 days of 2027-11 to have a
    2027-12 file (a second seam).
  - (c) Cross-check the S3 symbol listing against the 2028 exchangeInfo. Every `PERPETUAL`/`COIN` USDT symbol with
    an `onboardDate` before 2027-12 needs klines files. Otherwise a silently truncated listing drops whole new
    symbols.
  - (d) On resume, re-verify existing store files against `.CHECKSUM`. Alternatively, record each verified
    `(rel, sha)` in the attempt row and accept only those.
- **m6. Universe point in time** (`probe_universe_metadata.out`; names and dates only).
  - §3 says the perp-age rule "bites until 2021-03, irrelevant from 2025". It does bind in span (a):
    - 220 alive symbols were first archived between 2024-04 and 2025-06;
    - the migrations MATIC→POL (2024-09), FTM→S (2025-01), RNDR→RENDER (2024-07), MKR→SKY (2025-09) and EOS→A
      (2025-05) restart the age at the new ticker, whereas the paper ages the asset.
  - Decide now, before any read: keep perp age and state the deviation, or chain these five frozen pairs. A choice
    made after the read would be a degree of freedom.
  - The union rule applies the **2028** snapshot to the whole span. Restrict it to symbols absent from the 2026
    snapshot (first traded after 2026-10-09), so that a symbol reclassified later is not excluded retroactively.
  - The "20261011" file has `serverTime` 2026-10-09T07:16Z. Name it or describe it accordingly.
- **m7. Environment over 15 months.**
  - `PINS = {'python': '3.14.8'}`, and the registered interpreter is a venv whose `home` is Homebrew's
    `opt/python@3.14`. A `brew upgrade` between now and 2028 makes (v) refuse.
  - State the recovery now: a uv-managed CPython 3.14.8 at a fixed path, with the pin never edited. Name the exact
    interpreter path for the fetch and the read, which (g2) requires to be identical.
  - Pin the stdlib fingerprint as 015 does (015 M2.4).
  - Record the git version.
- **m8. Hash the bytes that are used.**
  - `Panel.load` re-reads `SHA256SUMS` and the static exchangeInfo after the guard hashed them, without re-hashing.
    Verify the bytes it parses against the table.
- **m9. One real-file check goes stale at approval time.**
  - "no approval or restart line exists yet" will fail after the 2028 approval commit, which is exactly when someone
    re-runs the suite before `--run`.
  - Make it conditional, as the date checks already are: no approval line before a `fetched` row exists.
- **m10. Sequencing.**
  - 016's broad scan refuses today on 015's uncommitted revision (`probe_staleness_015.out`). That is correct.
  - In January 2028 it will equally refuse on any later study's uncommitted top-level `.py`.
  - Keep it. Add to §11's procedure: "every top-level `.py` and package in `research/alpha` committed and clean
    before the fetch and before the read". Make the (d) refusal name that remedy.
  - With M2's `sys.path.append` and the provenance check, the scan becomes defence in depth, not the only line.
- **m11. Nits.**
  - §0 says "2027-12-31 + 7 days" and the code comment says "END + 7 days", but 2028-01-08 is END + 8. The suite
    asserts `+ timedelta(days=8)`.
  - `real_tree_failures` checks `guard is not _REAL_GUARD`, but not `guard_failures` (see m2).
  - A failed automatic commit of the `read` row leaves the decision in plain text in the working tree (008 r1 m7).
    State it in the procedure.

---

## Checked and sound

- **The date gate's inputs.**
  - TZ=`Pacific/Kiritimati` and `Etc/GMT-14`, environment variables (none read except the `GIT_*` strip) and a HEAD
    commit dated 2028 (author and committer) all leave (t) in place (`probe_t_dategate.out` (1)–(3)).
  - `guard()` never passes `now`.
  - `fetch(now=…)` is refused with the real hooks and on the real store (except as in M1).
- **The data gate cannot be faked with the real hooks.**
  - `btc_months` and `btc_tail` are outside `PERSISTABLE`, and `klines_error:BTCUSDT:*` is always blocking.
  - So no `fetched` row can be written before the archive holds BTCUSDT through 2027-12-31.
  - The read requires a committed `fetched` row, an approval committed strictly after it, and a matching manifest.
  - This is the realistic gate. The clock only decides whether an early attempt leaves rows behind (m1).
- **The fetcher.**
  - S3 `ListObjects` pages forward with the exclusive `marker` (or `NextMarker`). The suite's 3-per-page mock
    returns every key exactly once.
  - Months ≤ 2026-06 are never planned; months > 2027-12 are counted only.
  - A checksum mismatch, an empty answer or a download error raises and writes nothing. The checksum test is
    non-vacuous.
  - Writes are atomic (`.tmp` → `replace`).
  - HL `fundingHistory` windows are [a, b − 1 ms], matching the inclusive end of 008 §16. The first page from
    `HL_START` gives the first funding time.
  - Real hooks refuse outside the guarded fetch, and mocks must supply every hook.
  - The fetcher uses the archive only; no Binance REST klines or funding calls exist.
  - Every attempt commits an end row in a `finally`, and a failed commit stops the fetch.
- **`Panel`.**
  - Every pull file is verified against `SHA256SUMS`, and every forward file against the manifest.
  - Pull rows are cut at `< 2026-07-01`, and pull months above 2026-06 are refused.
  - Forward rows are cut to [2026-07-01, 2028-01-01), and overlapping days are refused.
  - Funding is cut to [2025-03-20, 2028-01-01).
  - Rows from the future are dropped.
  - These cuts are real but untested (m4).
- **The pinned pull** (`probe_pull_hashes.out`; hashes only).
  - All 19,730 files listed in `SHA256SUMS` exist and match. The only unlisted file is `FETCH_LOG.txt` (one line:
    895 symbols, `files_ok=19730`, `errors=0`).
  - The `SHA256SUMS` hash `ff50da31…` and the exchangeInfo hash `a7f720f5…` equal §16.
  - No symbol has an interior month gap. The last month is 2026-06.
- **No survivorship** (§0 item 4): delisted and dead symbols are kept, absent symbols are not excluded, and `status`
  is unused.
- **Run once on the normal path.** The suite's throwaway tests and my clone probes confirm:
  - (f)/(i) for amended, stashed, reset and other-branch rows, `-diff`/textconv, `diff.external` and `GIT_DIR`;
  - (h') strictly after the fetched row;
  - (r) after the fetch;
  - (g3) for a tampered forward file;
  - `record()` verifying the line in `HEAD:./results.jsonl`;
  - `read_failed` being terminal, and the restart line;
  - `HEAD:./` in both layouts (014 B1 shown fixed).
  - `-O` is refused, and there are no `assert` statements in the guarded code.
- **Staleness** (`probe_staleness_015.out`).
  - 186 / 186 in a clean clone at DRAFT (`b42c46c`), at REGISTERED, and with 015's in-progress code copied in.
  - No test asserts the commit state of the real repository (except m9, in 2028).
- **Nothing real is computed before the guard.** In order of execution:
  - The guard reads the registration, hashes and git.
  - `Panel` opens real files only after `guard('run')`.
  - The coverage counts are printed after the guard and contain no outcome.
  - `read_started` is committed before `compute`.
  - The decision is printed only after the `read` row is in HEAD and recomputes (`S.recheck`).
  - `ct16/*` does no file I/O, and `sim`/`--power` read nothing.
  - The drafter's scratch scripts (`dbg*.py`, `explore*.py`, `pilot.py`, `r016_tables.py`) are synthetic only.
  - The drafter's commit simulation copied only `SHA256SUMS` and the exchangeInfo, which are metadata.

## Reviewer's real-data reads (for the ledger)

- The SHA-256 of all 19,730 zips in `data/scout_c1/bn_um_klines_1d_monthly/`, of its `SHA256SUMS`, and of
  `data/r016/static/fapi_exchangeInfo_20261011.json`. No zip was opened.
- File names, symbols and months from `SHA256SUMS`, and the one-line `FETCH_LOG.txt` (counts).
- From the exchangeInfo: `serverTime`, the symbol count, and the counts of `contractType`, `underlyingType`,
  `underlyingSubType` and `status`. Also the names, contract types and `onboardDate` of the non-COIN USDT symbols.
- The real working tree's `r015_*.py` (code), copied into a clone to test the interplay.
- `git status`, `git log`, and a listing of `research/alpha/__pycache__`.
- One accidental network request from a mutant run: `GET https://data.binance.vision/x` (HTTP 404). It is not a data
  key and returned no data.
- **No price, volume, trade count or funding value of any real file was read or computed.** No network request for
  any date on or after 2026-07-01 was made.

---

## Appendix: Revision 1 recheck (2026-10-13, probes only)

**Scope.** Revision 1 is at `1555bf8`; real HEAD was `64cbffe` when this ran. Every round-1 attack was re-run against
the revised runner and fetcher.

**Stand-ins and procedure.**
- `make_standin016_r1.py` builds the stand-ins.
  - It runs `git clone --local` of the real repository. The real history, the anchor and **015's three committed
    ledger rows** are present, and the Revision 1 code is byte-identical.
  - It writes synthetic data with the suite's `_synthetic_tree()`.
  - It changes one line, `REAL_REPO`, to point at the stand-in, so that the stand-in plays the real repository.
  - With `--gate-open`, it also changes one line, `READ_NOT_BEFORE`, to 2026-10-01. Now that `-I -S` closes the `.pth`
    route, this is the only stand-in for "after 2028-01-08".
- Mock fetches use `r1_fetch_harness.py.txt`. It calls the stand-in's own no-argument `fetch()` with the network hooks
  replaced in process.
- Every probe after the first `probe_r1_m2.sh` run ran under `sandbox-exec` with the network denied. The incident in
  that first run is listed under "Reviewer's reads" below.
- Probe files: `r1_lib.sh`, `probe_r1_*.sh`, `.out`.

### Verdict: register after R1-M1, a one-line fix plus a test

B1, M1, M2 (as attacked in round 1), M3 and M4 are fixed. The 015 merge attack is refused. A legitimate stand-in run
works with another registration's rows already in the ledger. The suite kills all 26 mutants.

One new major remains, R1-M1: a command-line `-X pycache_prefix` brings back forged decision bytecode under `-I -S`.
Four minors follow. r1-m1, the squash-from-worktree attack, was carried as a minor in 015 too. It should get the cheap
tag pin, because 016 has 15 months in which a worktree squash can happen.

### Round-1 attacks: results

| attack | probe | result |
|---|---|---|
| Legitimate REGISTERED stand-in (015's 3 rows in the ledger) | `probe_r1_legit.out` | fetch (runner-committed `fetch_started`, `fetch_attempt`, `fetched`) → (h') → approval → `guard open` → `--run`: `outcome: alive`. A second `--run` is refused (i); a second fetch is refused (f) |
| B1: clone with data symlinked or copied; fetch CLI and mock harness in the clone; linked worktree (detached and on a branch); detached main HEAD | `probe_r1_b1.out` | (w) every time; the fetcher refuses with "(w) the fetcher runs only from the real repository" |
| B1: repo-path line in a clone while the pinned path exists | `probe_r1_b1.out` | ignored; (w) |
| M1: `_fetch_in` with root, store or a parent in the real tree; `fetch(now=)` / `fetch(hooks=)`; `guard_failures(real, now=2028)`; `record()` without the guard; `run()` today; `Panel` | `probe_r1_m1.out` | all refused; ledger 3 → 3 lines, HEAD unchanged, no store |
| M2: no flags; `-I` alone; `PYTHONPATH` with a forged `datetime`; a venv `.pth` under `-I -S` and under `-I`; a forged `__pycache__/r016_run` pyc on the fetch CLI; a committed `csv.py`; a committed `tokenize/` package | `probe_r1_m2.out` | refused by (v) or (d). The forged pyc is not loaded: the genuine guard runs |
| M3: `--skip-worktree`, `--assume-unchanged`, a filter attribute | `probe_r1_m3.out` | (b), (d) flag `S`/`h`/filter, (e) |
| M4: a committed revision; a `--no-ff` merge of REGISTERED + revision into another branch | `probe_r1_m4.out` E1, E2 | "(r) 2 different REGISTERED versions …", plus HEAD and `ct16/stats.py` differing from reg0 |
| M4: data gate | `probe_r1_m4.out` E4 | `guard('run')` passes with `_GATE = None`, and `Panel` refuses until `run()` has the started row in HEAD |
| M4: restart | `probe_r1_m4.out` E5, E6 | after a runner-committed `read_started` it is refused (i), also after `reset --hard`, a hand re-commit and a restart line, because the runner commit survives in the reflog. A hand-only `read_started` plus a restart line opens, as intended |
| m1: witness | `probe_r1_minors.out` | without the 2027-12 key, or with a server date before the gate, it is refused before any row or file (store 0 files, ledger 3 → 3) |
| m2: byte copy of the pull; `_GATE` by hand; stubbed `_GUARD_FAILURES` | `probe_r1_minors.out` | refused; the stub gives `_guard_ok = False` |
| m3: no trailing newline; orphan-root row with `log.showRoot=false` in repo config; `git replace` | `probe_r1_minors.out` | (k); (i); (i) |
| m4: mutants | `probe_r1_mutants_suite.out` | **26 of 26 killed; 8 of 8 baselines pass**. `tests/test_r016.py` passes 202/202 in a clone without data (the 205 count includes real-data-conditional checks) |
| m5–m11 | code and registration text (`probe_r1_minors.out`) | funding coverage, the second seam, the listing cross-check and resume re-verification (all mutant-tested); the 2028 snapshot classifies only symbols the 2026 snapshot lacks; migrations stated (§3); the pin recovery is `--fingerprint` with "pins never edited"; the approval check is conditional; the procedure says to commit every top-level `.py`; END + 8 |
| Real repository, read-only `--check` | `probe_r1_real_check.out` | only (s), (t) and (g1). (w), (v), (d) and the reg0 scan pass, with 015's rows in the ledger |

### MAJOR

**R1-M1. `-X pycache_prefix` restores forged decision bytecode under `-I -S`** (`probe_r1_m2.out`, "ATTACK").

`r016_run.py` keeps a pre-existing `sys.pycache_prefix` whenever its directory name starts with `r016f-pyc-` and it
lies in the temp directory. That rule exists so the fetcher's own prefix survives.

The attack:
1. Forge `ct16/stats.py`'s bytecode, stamped with the source's mtime and size, under
   `$TMPDIR/r016f-pyc-rbevil/<abs path>/ct16/`.
2. Run `python3 -I -S -X pycache_prefix=$TMPDIR/r016f-pyc-rbevil r016_run.py --check`.

```
(g1) 0 fetch_started and 0 fetched rows …          (no (v) refusal)
ct16.stats forged: True | stats __cached__ under …/T/r016f-pyc-rbevil/ | module_origin_failures: []
```

Why every check passes:
- `-I` and `-S` are both on the command line.
- `__cached__` lies under the current prefix.
- The source hashes match.

The same command with `--run` decides with the forged statistics.

**Fix.**
- Refuse if `sys._xoptions` is non-empty, or at least if it contains `pycache_prefix`.
- Keep a pre-existing prefix only when `__main__` is `r016_fetch.py` and the prefix equals the one the fetcher recorded
  (`r016_fetch._PREFIX`).
- Otherwise always call `mkdtemp`.

**Test.** The forged-stats case above refuses with (v).

### MINOR

- **r1-m1. reg0 can still be lost through a worktree squash** (015 r2-m1, unported). `probe_r1_m4.out` E3:
  1. In a linked worktree on branch `reg`, commit REGISTERED, then a revision.
  2. In main, run `git merge --squash` and commit.
  3. Run `worktree remove` and `branch -D reg`.
  - The original REGISTERED commit is reachable 0 times through `--all --reflog`. Main's `--check` shows only (g1),
    and `ct16.stats` is the forged one.
  - **Fix:**
    - the orchestrator creates the tag `r016-registered` on reg0, or pins reg0 in the programme ledger (round-1 M4's
      second pin);
    - the guard requires that pin and requires its blobs to equal HEAD's;
    - procedure line: "REGISTERED is committed in the main worktree".
- **r1-m2. The repo-path recovery is self-issued** (`probe_r1_b1.out`, ABUSE 1 and 2). Both need the real repository
  moved away, so they are deliberate.
  - (1) Run `mv` on the pinned path. A clone commits its own `016 repo-path: APPROVED … <clone>` line and runs fetch →
    approval → read. Once the origin is moved back, it shows no 016 row.
  - (2) Place a symlink at the pinned path pointing to a fresh clone. The guard opens in the clone with no line at all,
    because `resolve()` follows the link.
  - **Fix:**
    - refuse when `REAL_REPO` or any parent is a symlink;
    - accept a repo-path line only when the new location's `.git` holds a nonce file whose SHA-256 is pinned in the
      registration. `git clone` does not copy it, and a moved repository carries it.
- **r1-m3. The fetcher's network hooks are swappable module globals.**
  - The legitimate-run harness replaces `REAL_HOOKS` and `_real_witness_hooks`, and the real `fetch()` then fills the
    real store with mock data and commits a real `fetched` row (`probe_r1_legit.out`).
  - That needs in-process patching after 2028, but `guard`, `_GUARD_FAILURES` and `HOOKS['commit']` are already
    identity-checked.
  - **Fix:** capture the hook dict and the witness function at import, and refuse in `fetch()` unless they are
    identical.
- **r1-m4. Accepted in-process residuals.**
  - Assigning both `_GATE` and `_GUARD_PASSED` by hand opens `Panel`.
  - `module_origin_failures` accepts anything under the stdlib directory, which includes `site-packages`. That is
    unreachable under `-S`.
  - The stdlib fingerprint hashes `.py` and `.so` but not the stdlib's own `__pycache__`. That is a machine-level
    tamper, like the system clock.
  - No change is needed beyond a sentence in §11.

### Reviewer's reads in this recheck (for the ledger)

- **One real network request.**
  - The first, unsandboxed run of `probe_r1_m2.sh` ran the fetch CLI in a gate-open stand-in. Its guard passed, and the
    witness sent one `GET https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?prefix=data/futures/um/monthly/klines/BTCUSDT/1d`.
  - The answer holds key names: the months of existing BTCUSDT monthly files, through the latest published one, and
    the server's HTTP `Date`. No file was downloaded, nothing was parsed beyond key names, and no row or file was
    written. The fetch refused because no 2027-12 key exists.
  - It is a listing of span-(c) month names, not values. Every later probe ran with the network denied.
- **The real repository.**
  - `python3 -I -S r016_run.py --fingerprint` and `--check`: interpreter and stdlib hashes, the registration, the two
    data-file hashes, and git.
  - The first 80 characters of `results.jsonl` lines 1–3, which are 015's committed and already reported rows, seen
    while checking that the stand-in carries them.
- **No price, volume, trade count or funding value of any real file was read or computed.**
