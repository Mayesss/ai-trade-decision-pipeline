# 008 DRAFT — independent review B (guard, data path, leakage), 2026-10-12

Reviewer brief: adversarial review of `registrations/008-inverted-positioning-holdout.md` (DRAFT) with
`008-manifest.json`, `ct/crowd008.py`, `r008_run.py`, `r008_fetch.py`, `r008_power.py` and
`tests/test_r008.py`. The emphasis was the guard, the restart path, the fetch, leakage and reads, data
fidelity and the P0 exposure. Read-only: no file of the draft was edited. `r008_run.py` was not run
against real data. Nothing was fetched from S3, Bitget or Hyperliquid.

## How this was checked

- **Tests.** `cd research/copytrade && .venv/bin/python -m tests.test_r008` gives **"all checks passed
  (86)"** in 40 s.
- **Reuse.** `git diff efefc7f HEAD` is empty for all 14 reused modules, `p5_run.py` included. The
  §11 hashes of `ct/__init__.py`, `ct/crowd008.py`, `r008_run.py`, `r008_fetch.py` and
  `008-manifest.json` match the disk.
- **The real guard, read-only,** in `fetch` and `run` modes (`guard_failures(HERE, mode)`). It refuses
  on (s), (a), (c), (d), and on (g1) in run mode. **There is no (e) failure**, so all 7 static data
  files and all 206 pre-registration manifest files hash to their registered values.
- **Probes**, kept in `research/copytrade/scratch_review008b/`. All use throwaway git repos and the test
  suite's synthetic tree and mocked hooks:
  - `probe_guard.py`:
    - `owner`: parsing of the owner line;
    - `bytes`: byte accounting when a transfer raises;
    - `inject`: snapshot files added after the fetch;
    - `erase`: discarding rows, and `reset --hard`;
    - `mods`: the modules and the Python-level file opens of a full `run()`;
    - `split`: the guard evaluated on one repo while the data come from another.
  - `probe_hooks.py`: a partial mock dict.
  - `probe_delisted.py`: a mapped Bitget symbol removed after 2026-10-07.
- **Power.** `r008_power.py --sims 4000` was re-run, and every §3 cell reproduces.

**Reviewer's real-data reads (for the ledger).** No price, return, funding rate, position, tilt, cohort
or account value was computed or printed.
1. **Snapshot footers and file names.**
   - The Parquet footer schema (column names and types) of all 384 snapshot files, 98 of them dated
     2026-07-01 or later. Row counts were not printed.
   - From the file names: files per date directory, date gaps, and the hour of each file's name
     timestamp.
   - From that: one holdout snapshot is late (2026-08-23 16:52 UTC), against 64 late snapshots in
     discovery. Date 2026-03-24 holds 2 files.
   - The names, sizes and mtimes of the 2026-06-15, 2026-07-01 and 2026-10-06 snapshot directories.
2. **`btc_1h.parquet`, the `ts` column only.** It has 0 missing hours from 2026-05-28 to 2026-09-30,
   and 0 non-hourly steps in the whole file.
3. **Static files.**
   - P0 `report.json`: the top-level keys, the wallet count (50) and the key names of a wallet entry.
   - `symbols.json`: the status counts (168 ok, 56 delisted_on_hl, 9 no_bitget_contract,
     1 price_mismatch) and the key names of an entry.
   - `bitget_listing.json`: the count (168) and the number of null entries (0).
4. **Existence and listings.**
   - The `data/pruned` fill directories from 2026-06-25 on: none is dated 2026-07-01 or later; 338
     directories in all.
   - `data/r008` is absent.
   - The names of the `data/cache` subdirectories and the mtimes of `data/derived/hyperliquid`.
5. **SHA-256 of bytes,** through the real guard: the static data and the 206 manifest files.
6. **`results.jsonl`**: its line count (12) and the `git log` subjects for the file.

---

## 0. Answers to the brief, in short

| probe | finding |
|---|---|
| Can the runner compute or print an outcome while 008 is DRAFT? | **Yes, through the module API (B1).** `run(root=<throwaway repo with an open gate>, data_dir=<the real tree>)` computes the full decision; the read lock only checks a boolean that any passing `run()` sets. The fetch has the same defect, and a partial mock dict falls back to the real requester-pays hooks. Through the CLI (`--check`, `--run`), no. |
| Does it recompute rather than trust labels? | The decision is recomputed from the stored numbers (`recheck`), and its failure message leaks the label (m2). The read-once rows are labels in a file that can be discarded before they are committed (m1). |
| Are all decision-path modules hashed? | **Yes.** The run imports only `GUARDED_CODE` files (audit; `ct/eventstudy.py` is loaded by the tests only). `p5_run.py` is the tests' oracle, not on the run path, and not hashed (m9). |
| Owner end line | Sound. Only a line starting with the exact prefix counts. Quoted (`> `), indented, bold and inline forms do not match. Two lines, or a trailing character, refuse. |
| Restart path | It cannot yield a different outcome **while the inputs are pinned**. But the snapshot set is not pinned (M1), the rows can be discarded uncommitted (m1), and the Python environment is not frozen (m3). Together these allow: run, see, discard, change an unpinned input, run again. |
| Fetch: LIST-first, footer plan, cap, no error cached, forward paging, tail | Mostly as described. But **the fetch cannot finish** if any mapped symbol is removed by Bitget before it runs (B2). **The cap is not cumulative, and the fetched row under-reports bytes** whenever a transfer raises (M2). Partial interior price blocks are kept (m5). |
| Is cost bounded if the plan is wrong? | Within one attempt, yes: the overshoot is at most one object. Across attempts, no (M2). |
| Holdout reads before the guard | None that parse holdout content. The guard hashes bytes only. Coverage prints counts and timestamps only. |
| Holdout-dated caches | `bg-1h`, `hl-funding` and `hl-fills` are never opened (audit). `btc_1h.parquet` is loaded whole and used only before trip opens (declared). **The holdout rows of `snapshot_accounts.parquet` are read, contrary to D18 (a) and the mitigation in `000c` (M3).** |
| Reads after the end | Prices to end + 2 d and funding to end + 4 d are fetched and loaded but never used. `btc_1h` is loaded to 2026-10-07. Window C's snapshots are hashed under the 2026-09-28 end (m4). |
| Data fidelity | **Snapshots: the same product.** One schema across all 384 files. One holdout snapshot is late, handled as in 001. `snapshot_accounts` was built in one pass on 2026-10-07. **Fills:** the same `archive.download_columns` with p3's 14 columns, but none of the P3 gates is run on the holdout days (m6). **Bitget:** survivorship now runs in the hypothesis' favour and is not measured (M4). |
| P0 | A cohort count understates a wallet's weight. `000`'s own mitigation clause ("the untouched one decides") is not cited (m7). |

---

## BLOCKER

### B1. The gates check the `root` argument, not the repository whose data are read or fetched

**Where.**
- `r008_run.run` (l. 451–463) evaluates `guard_failures(root, …)`, then sets `_GATE['open'] = True`,
  then computes on `data_dir`, which can be any tree.
- `compute`'s read lock (l. 363) checks only `data_dir == HERE/data and not _GATE['open']`.
- `r008_fetch.fetch` (l. 354–360) has the same split. `hooks = {**HOOKS, **(hooks or {})}` fills every
  missing mock with the **real** network hook.
- `_s3_plan` (requester-pays footer GETs) and `_s3_list` have no `data_dir` assertion. `_s3_get_*`
  assert only that `data_dir` is the real tree, and `-O` strips the assertion.

**Probes.**
- `probe_guard.py split`. The stand-in for the real repository is DRAFT, and its guard refuses with (s)
  and (g1). `compute(<its data>)` is locked. But `run(root=<a throwaway repo with an open gate>,
  data_dir=<its data>)` returns `decision: not_demonstrated`, **computed on the stand-in's tree**. The
  rows are written to the throwaway repo, so nothing in the real `results.jsonl` records the read.
- `probe_hooks.py`. `fetch` on a throwaway REGISTERED repo with a mock dict missing one key
  (`s3_plan`) reaches the real `_s3_plan`. That means requester-pays footer reads of holdout objects,
  for a registration that is still DRAFT.

**Why this is a blocker.**
- The test suite already builds a throwaway repo with an open gate (`make_repo`, `Hooks`, `GK`).
  Turning it on the real tree takes one argument.
- An agent writing a "real-data smoke test in a throwaway repo, nothing committed" would read the
  holdout, or pay for it, without tripping anything.
- This is the "label-only guard" lesson again. The code is frozen at REGISTERED, so the fix has to land
  before registration.

**Fix (in 008's new files only).**
1. In `run()` and `fetch()`, when `Path(data_dir).resolve() == (HERE/'data').resolve()`, require
   `Path(root).resolve() == HERE` and no `guard_kw` override. Otherwise refuse.
2. Make `_GATE` hold the resolved `(root, data_dir)` that passed the default-argument guard, and have
   `compute` check that pair, not a boolean.
3. In `fetch()`, when `hooks` is passed it must contain all eight keys (no fallback). A real hook may
   be used only with `root == HERE` and default kwargs.
4. Use `if …: raise SystemExit` instead of `assert` for the data-directory check.
5. Add one test for each case.

### B2. The fetch can never finish if Bitget removes a mapped symbol before it runs

**Where.**
- `r008_fetch.fetch_closes` (l. 216–235) requests every one of the 169 symbols.
- `_bg_candles` turns any Bitget error into `FetchError`, and that aborts the attempt.
- The `live` contract set is fetched first (l. 387) but is not passed to `fetch_closes`. Coverage
  tolerates non-live symbols, but the fetch loop does not.
- This round's N3 scout (`scouts/n3-listings.md` l. 49, 166): **"Bitget does not serve history for
  symbols it has removed (codes 40309 and 40034)."**

**Probe** (`probe_delisted.py`). A symbol that has left the contract list and answers 40034 stops
**3 of 3 attempts** at the same point. No fetched row can ever be written.

**Consequences.**
- The symbol table is from 2026-10-07, and Bitget removes contracts routinely, so one removal among 168
  before the fetch is a real possibility.
- The code is frozen at REGISTERED. The only way out is an amendment, after which the fetch-started
  row carries the old registration commit and (f) refuses ("another registration commit").
- The same dead end follows from any refusal that a re-run cannot cure:
  - a live symbol whose closes stop early because Bitget halted it, which deletes its last block and
    refuses forever;
  - a live symbol whose history has a gap of 200 hours or more;
  - HL `fundingHistory` erroring for a coin that has left `meta`.

**Fix.**
1. Pass `live` to `fetch_closes`. For a symbol not in `live`, catch the error, write nothing, and
   record it in `report.json` and the fetched row as unavailable since 2026-10-07 (names and count).
   For live symbols, keep refusing on errors. Do the same for HL coins that `meta` lists as delisted
   or does not list.
2. Pre-specify a terminal outcome for a refusal that persists. For example, the same refusal on 3
   attempts at least 24 h apart is recorded as "unavailable" and counted, and the fetch completes.
3. Add a test for each.
4. Before registering, check the free public contract list for the 168 symbols. It holds no price.

---

## MAJOR

### M1. The run's snapshot set is not pinned; extra files change the decision without tripping the guard

**Where.**
- `compute` (l. 376) globs every `date=*/*.parquet` and selects by the timestamp in the file name
  (`signal_snapshots`).
- (g3) and the pre-registration manifest hash only the files that were present. Neither checks that
  the selected set equals the hashed set.
- The fetch's `disk_coverage` rule (exactly one file per needed date) is not applied at run time.

**Probe** (`probe_guard.py inject`), after the gate is open:
- one file is added in an unlisted directory (`date=2026-07-31`, name timestamp 2026-07-30 12:00);
- a second file is added in `date=2026-08-10`;
- the run guard returns `[]`;
- `signal_days` goes from 83 to 85, and the C1 t from −0.102 to −0.056.

A second file for a date appears whenever the archive republishes a date under a new key and anyone
runs `p2_snapshots.py`, which other scouts this round use. Combined with m1, a run can be seen,
discarded, and repeated on a different input.

**Fix.**
- `compute` takes its snapshot paths from `data/r008/manifest.json`, the fetch's list for the needed
  dates.
- The run refuses unless the files that `signal_snapshots` picks from a full glob are exactly those
  paths, and every needed date directory holds exactly one file.
- Add a test with an injected file.

### M2. The 15 GiB cap is not cumulative, and the fetched row's byte count is wrong after any transfer error

**Where.** `r008_fetch.py` l. 377–383. The line `attempt['bytes'] += s3_pull(...)` does not run when
`s3_pull` raises. That happens:
- on its own cap `FetchError`;
- on any `RuntimeError` from `archive.download_columns`: the row-count check, a short range, or an AWS
  error. These are raised after the ranges have been paid for.

Bytes from completed days in that attempt are also lost, and so are the footers when `s3_plan` raises
part-way.

**Probe** (`probe_guard.py bytes`).
- Plan: 33 days × 450 MiB = 14.5 GiB, which passes the cap.
- Day 33 raises after its transfer. The log shows **0.0001 GiB against 14.5 GiB actually transferred.**
- A second attempt completes. The fetched row's `bytes_requester_pays` says **0.44 GiB against 14.94
  GiB actually transferred.**
- A day that keeps failing adds about 0.44 GiB per re-run, and the cap never refuses.

The owner's condition was a cumulative hard cap. The procedure (§11) has the owner "read the byte
count … in the fetched row" before approving the read, and that number can be wrong by about 30×.

**Fix.**
- Measure with `archive.bytes_pulled` (incremented on every range and footer).
- Take its value before the plan, add the difference to the attempt in `finally`, and compute the
  remaining budget from that measured total.
- Have `s3_pull` record `done` before raising.
- Add a test with a hook that transfers and then raises.

### M3. 008 reads the holdout rows of `snapshot_accounts.parquet`, against two owner-approved texts it does not cite

Window B (S = 2026-08-03) takes equity as of S and typical leverage over 2026-05-04 → 08-03. That
reads `snapshot_accounts.parquet` rows from 2026-07-01 to 08-02, through `p4_features.features` and
`leaders.Equity`. Two approved texts say otherwise:
- **`000c` Mitigation:** "Any script that reads `snapshot_accounts.parquet` filters `snapshot_ms <
  1782864000000` in the query itself."
- **012 D18 (a), approved:** "the ~98 holdout snapshots in
  `data/derived/hyperliquid/snapshot_accounts.parquet` … **Neither is read by any registration.**"

D18 (b) ("L3 reads the series its registration names") arguably overrides both. But the draft's §2
whitelist, §12 D10 and §13 name only `bg-1h` and `hl-funding` as unread, and say nothing about the
conflict.

The product itself is fine. `p2_accounts.py` built the table in one pass on 2026-10-07 at 16:30, from
the same snapshot files for both periods.

**Fix.**
- Name this read in §2, D10 and §13 as an explicit exception, under D18 (b), to the last sentence of
  D18 (a) and to the mitigation in `000c`, for the owner to approve at registration.
- State that the bound [S − 91 d, S) is applied in the query by the two hashed modules.

### M4. Survivorship now favours CONFIRM, and the draft neither says so nor measures it

**The filter.**
- The universe is the 2026-10-07 table. On every date it excludes:
  - 56 coins with status `delisted_on_hl`;
  - 9 with `no_bitget_contract`;
  - 1 with `price_mismatch`.
- Bitget serves no history for symbols it has removed (N3). So a coin that died during the holdout is
  not in the book at all.

**The direction.**
- The inverted book is long where the losing cohort leans.
- If losers lean into coins that later die, dropping those coins removes the long leg's worst names,
  which biases toward CONFIRM.
- For 001's original sign, the same filter removed its best shorts. So part of 001's negative T3,
  which is the motivation (§0), may be this same artefact, and it would carry into the holdout.
- §4.4 and §10 mention survivorship but not its direction relative to the hypothesis.

**Fix.** These are reported only and decide nothing:
- per window, the number of coin-days where `crowd.tilts` returns a coin (≥ 3 cohort holders) that is
  dropped only because its table status is not `ok`, by status;
- the cohort's and the crowd's gross notional share in those coins.

Also add the direction to §0, §10, and the reading of a CONFIRM in §5.

---

## MINOR

- **m1. "Read once" depends on commit discipline.**
  - Rows are appended to the working tree only. After a run, `git checkout -- results.jsonl` reopens
    the gate (probe `erase`).
  - A committed holdout row removed with `git reset --hard HEAD~1` is invisible to `git log --all`,
    and the gate opens again.
  - Discarding an uncommitted `holdout_started` row also skips the restart rule and resets
    `restarts_after_mechanical_stop` to 0.
  - r009 and r012 follow the same pattern, but 008 adds a restart rule whose controls depend on it.
  - **Fix:** refuse unless `results.jsonl` is clean against HEAD. Have the runner commit
    `results.jsonl` alone right after each row, or require the started row to be committed (a
    `--start` / `--run` split). Scan `git rev-list --all --reflog`.
- **m2. A `recheck` failure prints the decision with no row** (l. 494–495): "stored decision X differs
  from recomputed Y", and the criteria. Because it is deterministic, a restart repeats it. **Fix:**
  append the holdout row with a `recheck_failed` flag, then exit.
- **m3. The environment is not frozen across a restart.** The duckdb, pyarrow and python versions are
  written only to the holdout row. **Fix:** write them, with the code hashes, to `holdout_started`,
  and require equality on a restart.
- **m4. Reads after the end.**
  - The last return needed ends at about 2026-09-28 01:00 (last entry before 09-25 plus 72 h). The
    fetch takes prices to 09-30 and funding to 10-02, and `Closes` and `Funding` load them whole.
  - `btc_1h` is loaded to 10-07.
  - The guard hashes window C's snapshots (09-28 → 10-06) under the 09-28 end.
  - So "Nothing after the end is read by 008" (D2) and "never opens" (§2) are not literally true.
  - **Fix:** fetch to end + 2 h, drop later rows on load, and say "hashed, not parsed" for window C.
- **m5. Partial interior price blocks are kept.**
  - `fetch_closes` writes any non-empty answer. Coverage refuses only whole missing blocks and early
    tails, and holes shorter than 24 h are not even counted.
  - A missing hour at an entry becomes a 0 return (`ret … or 0`) and can push vol below 20 points.
    001's cache behaved the same, but §11 claims partial answers are never kept.
  - **Fix:** count missing hours per live symbol after its first candle, report them in the fetched
    row, and re-ask once for any block shorter than its span.
- **m6. The P3 gates are not run on the 33 holdout fill days.** G1 (UTC partitions), G2 (dex column)
  and G3 (order within a millisecond) read no outcome, and round-trip reconstruction depends on G3.
  **Fix:** run G1–G2 on all 33 days and G3 on a sample, as part of the fetch's coverage, with the
  results in the fetched row.
- **m7. P0.**
  - A count of P0 wallets per quintile understates their weight, because one large wallet can dominate
    a quintile's gross notional.
  - `000`'s mitigation ("the archive's months after the holdout read form a second, untouched
    holdout … If the two holdouts disagree, the untouched one decides") is not cited.
  - The recommended end lies inside P0's window (2026-08-08 → 10-07). The follow-up "on data after
    008's end" would start on 09-28, which is still exposed to 10-07.
  - **Fix:** report the P0 wallets' share of each cohort's and crowd's gross notional on book days.
    Cite `000`. Start any follow-up's data on or after 2026-10-08.
- **m8. Only BTC beta is adjusted.**
  - The reason given for C1, a known rally, applies equally to an alt-market factor. A dollar-neutral
    alt book long the losers' coins may load on alt-season or small-cap dispersion that BTC beta
    does not absorb (§10 states this).
  - **Fix (reported only):** α on BTC plus the equal-weight universe return from the same closes,
    with the placebo distribution of that α.
- **m9. Text.**
  - §0 and §11 speak of 14 reused modules, byte-identical and hashed. The table has 13, because
    `p5_run.py` is not hashed. Say that it is the tests' oracle only.
  - §11's restart paragraph says (g2) compares the code with "the fetch commit". It compares it with
    the registration commit stored in the fetched row.
- **m10. The read lock and the whitelist are path checks.** Hard links, copies, or direct calls to
  `cohorts()` or `c8.book` read the real tree unguarded. After B1 that is acceptable if stated.
- **m11. Shadowing modules.**
  - `research/copytrade` is put first on `sys.path` before duckdb and pyarrow are imported lazily, and
    (d) checks only the listed files. An untracked `duckdb.py` would load.
  - **Fix:** make (d) also refuse untracked `*.py` files in `research/copytrade` and `ct/`. Today
    `ct/listings013.py`, `ct/shortvol.py` and `ct/shortvol_sim.py` are untracked.
- **m12. Nits.**
  - `HASH_ROW`: a duplicated row silently wins over the earlier one.
  - (r) skips changed lines whose text starts with `++` or `--` (the diff prefix plus the text reads as
    a `+++` or `---` header). That is harmless for the hash rows, the status line and the owner line.

---

## Checked and found sound

**Inheritance.**
- `git diff efefc7f HEAD` is empty for all 14 reused modules.
- `cohorts()` reproduces 001's membership: features, slow, not a vault, ≥ 4 trips in each regime,
  scored with a long bias, equity as of S and a typical leverage.
- `leaders.Equity` in 001 was bounded to the hold end, but `at(S)` and `typical_leverage(lb, S)` give
  the same values.
- The quintiles are in address order, stable-sorted by score. Tests 3–4 confirm this.

**Calendar.**
- Windows A and B (and C); 33 and 89 fill days; 30 / 53 / 9 signal days; 83 days in 13 ISO weeks.
- The `t_s ≥ 1782864000000` cut is applied. June signals are dropped. No outcome is formed in
  discovery.

**Guard elements.**
- DRAFT refuses in both modes.
- (a)–(e) work, and §11's code and data hashes all match.
- (f) holds across branches and across a deleted row.
- Approval and restart ordering: strictly later and an ancestor.
- (r) refuses prose. (g2) checks the end and the code. (g3) checks the manifest digest. (t) works.
- The offline lock covers `net._request`, `urllib.request.urlopen` and `archive._aws`.
- The outcome is printed only after the holdout row is written.

**What the run opens** (audit of a full synthetic `run()`).
- Only guarded modules are loaded.
- Python-level file opens are limited to the static files, `pruned/`, `archive/`, `r008/` and
  `cache/hl-vaults`.
- No `bg-1h`, `hl-funding` or `hl-fills` file is opened.

**Inference.**
- `ols_cluster` uses CR1 with G/(G−1)·(n−1)/(n−2).
- `t_quantile` matches the table.
- `recheck` recomputes the critical value, the criteria, the bar and the label.
- C2 and C3 are on the beta-adjusted series.
- Beta is identified by the same-span BTC return from the same closes.

**Arithmetic.**
- §0: 2 × 11 / 3 = 7.33 bp, and +0.17 bp net.
- §1: P(CONFIRM) 0.07 and 0.10; a confirm real about 41 % and 60 % of the time.
- §3: the bar needs about 10.8 bp gross.
- §9: −13 %, +8.7 %, Sharpe 0.84, and Kelly 7.8× gross, are all consistent.

**Data fidelity.**
- Snapshots:
  - all 384 files share one schema;
  - the holdout has one file per date, 98 dates, 2026-07-01 → 10-06;
  - one holdout snapshot is late (08-23 16:52), against 64 late in discovery; entry handling is 001's.
- Holdout fills will come through p3's own function and its 14 columns. No holdout fill day is on
  disk yet, and `data/r008` does not exist.

**BTC coverage.** `btc_1h.parquet` has every hour from 2026-05-28 to 09-30, so the "BTCUSDT has every
hour" refusal should be satisfiable.

**Fetch structure.**
- The listing is checked before any footer, and the footer plan before any GET.
- The cap is checked before the pull. The overshoot within an attempt is at most one object.
- Complete days are skipped.
- No error or empty answer is written.
- Bitget is paged forward by 200-hour blocks, and HL funding forward by 500.
- Tail checks exist for live symbols and coins.

**Power.** `r008_power.py --sims 4000` was re-run (5 min). Every §3 cell reproduces exactly:
- 2026-09-28: 0.048 / 0.141 / 0.305 (iid) and 0.048 / 0.134 / 0.278 (AR + t₄);
- beta nulls: 0.048 for the registered rule, against 0.145–0.182 for the raw rule;
- the 2026-11-23 and reference rows match too.

---

## Verdict

**Not ready to register.**
- **Two blockers.**
  - B1: the gates can be opened by a throwaway repo, and partial mocks fall back to the real
    requester-pays hooks.
  - B2: the fetch dead-ends on a symbol Bitget has removed, which the N3 scout documented this round.
- **Four majors.**
  - M1: an unpinned snapshot set.
  - M2: a cap and byte count that fail on errors.
  - M3: an undisclosed conflict with D18 (a) and the mitigation in `000c`.
  - M4: survivorship in the hypothesis' favour.

All six fixes are small and sit in 008's new files or text. Each needs a test, and the §11 hashes must
be redone.

The design itself is sound:
- an existence test before costs, beta-adjusted, at t_{G−1};
- the arithmetic, which is honest;
- the calendar;
- reuse that is byte-identical to `efefc7f`;
- an end recommendation made on mechanics and bytes only.

None of these findings needs any change to the rule.
