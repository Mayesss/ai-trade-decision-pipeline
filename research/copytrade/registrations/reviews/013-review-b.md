# 013 DRAFT — independent review B (guard, data path, leakage, survivorship), 2026-10-13

Reviewer brief: adversarial review of `registrations/013-new-perp-listings.md` (DRAFT) with
`ct/listings013.py`, `r013_fetch.py`, `r013_build.py`, `r013_run.py`, `tests/test_r013.py` and
`data/r013/`. The emphasis was the guard, point-in-time events, survivorship during the hold, data
integrity, the read whitelist, and leakage in drafting. Read-only: no file of the draft was edited,
nothing was committed, nothing was fetched. `r013_run.py` was not run on real data. Its guard functions
were called read-only.

## How this was checked

- **Tests.** `cd research/copytrade && .venv/bin/python -m tests.test_r013` gives **"all checks passed
  (124)"** in 17 s.
- **The real guard, read-only.** `guard_failures(HERE, …)` in discovery and confirmation modes, and
  `ordering_failures(HERE)`.
  - Discovery refuses on status, (a), (c) and (d). Confirmation also refuses on (g1).
  - **There is no (e) failure**, so all 7 code and 23 data hashes in §10 match the disk.
  - (h) returns `[]`: 009's committed close (row 7) is recomputed by `factors.recheck_discovery`.
- **Import audit.** A full guard pass loads only `ct`, `ct.listings013`, `ct.factors`, `ct.stats` and
  `r013_run` from the repo. All of them are hashed.
- **Probes** (all in `research/copytrade/scratch_review013b/`):
  - `probe_erase.py`: throwaway repos. Each run erases 013 rows (checkout, `reset --hard`, `--amend`,
    `stash`) and then reads the discovery gate. It also re-runs every case with a widened history scan.
  - `probe_nested.py`: a nested `git init` hides the outer history from the guard.
  - `probe_dead_rows.py`: trade counts in the raw 1h files of two contracts delisted in 2022.
  - `probe_headers.py`: the header lines of about 1,000 sampled raw zips (schema only).
- **Coverage-only reads** of `listing/`, `plan.csv`, `inputs/` and the panels' `events.json`,
  `symbols.json`, DISTINCT symbols and max timestamps. The list is at the end. No price, rate, return,
  sum or P&L was computed on any listing window.

## 0. Answers to the brief, in short

| question | finding |
|---|---|
| Outcome while DRAFT, uncommitted or modified? | **Not through the CLI.** `guard()` hard-codes `HERE`, and every check returns refusals. **Through the module API, yes (M4).** `Panel` and `evaluate` have no gate. The e2e test's recipe (stub `guard`, redirect `DATA` and `RESULTS`) is one omitted assignment away from a real read. |
| After a row exists? | Blocked while the row is in the tree or in any branch's history. **Not blocked after `git checkout`, `reset --hard`, `commit --amend` or `stash` (M3, probed).** 008 already moved to `--reflog` this round. 013 uses the older scan. |
| Throwaway `root` pointed at the real data? | **No split exists.** `guard()` evaluates `HERE`, and the run reads `DATA = HERE/data/r013`. The guard does trust whichever repository git finds at `HERE` (a nested `git init`, or `GIT_DIR`). (h) blocks that incidentally (m5). |
| Confirmation recomputes the decision from the row? | **Yes.** It recomputes from the stored aggregates (`recheck_discovery`), not from the per-event records in the same row (m8). (h') requires the approval strictly after the row's commit. (i) has the same erase weakness as (f) (M3). |
| Decision-path modules hashed? | Yes, all of them (audit above). |
| Decision printed without a row? | **No.** Both runs print the trial statistics and the decision only after `append_row`. Only the counts (which include liquidations and exits at the last trade) are printed before, by design. |
| Point-in-time events | The inclusion rules use no fact after entry. **But the event set includes ticker migrations, relistings under new tickers and asset-backed tokens (M2).** And "0 detectable relistings" is an artefact: dead contracts' files never stop (M1). |
| Pre-market flag | A d0 fact (no index file on d0). Knowable at entry from the contract announcement, and no later archive file is used. Sound, with the stated misclassification risk. |
| Survivorship during the hold | **The exit fallback rests on a false premise.** Binance archives keep writing flat, zero-trade rows for delisted contracts. A delisted listing therefore exits at a flat placeholder bar labelled `exit`, not `exit_last_trade`. **The market index includes 10–18% dead, flat constituents (M1, probed).** The build drops no event (255/257/148 in `events.json`). |
| Integrity | The checksum, never-cache, forward-paging and cutoff code is as described. **Nothing in any input or panel is dated on or after 2026-07-01.** One discovery event (JUPUSDT) lacks its d0-month funding file, and no rule covers it (m1). |
| Whitelist and locks | Discovery cannot open confirmation panels. **D17 (b) exists in prose only.** The confirmation panel sits on disk ungated (m7). |
| Leakage in drafting | The ALPHAUSDT first bar, as disclosed. **The printed funding-row and zero-trade-row totals are outcome-adjacent, and `events.json` stores them per event (M5).** The 900 vs 787 symbol-directory count is a count of the sealed period (m3). |

---

## BLOCKER

None.

---

## MAJOR

### M1. Dead contracts keep flat, zero-trade rows; the market index and the exit fallback treat them as live

**Fact.** Scout L4 §B (`scouts/l4-binance-years.md` l. 19, 119, 133) says: "Daily 1d klines for dead
contracts continue to 2026-06-30 … **Any loader must cut each symbol at its delisting date.**"
`probe_dead_rows.py` confirms this for the 1h files 013 pulled:
- FTTUSDT 2024-01: 744 rows, **744 with 0 trades, 1 distinct open**;
- SCUSDT 2024-01: 744 / 744 / 1;
- FTTUSDT 2026-06: 720 / 720 / 1.

Both contracts were delivered in 2022.

**Where 013 ignores it.**
1. **The market index (criterion 5, a decision criterion).**
   - `r013_build.assemble` stores `symbol, t, o` without the trade count.
   - `Panel.market` admits every symbol whose first daily file is ≥ 30 days old and that has an open at
     the entry hour.
   - Count of SETTLING contracts in exchangeInfo with delivery before the date, plus symbols absent from
     the snapshot, among the seasoned market symbols:

     | date | dead | absent from snapshot | seasoned |
     |---|---|---|---|
     | discovery 2024-10-01 | 14 | 15 | 304 |
     | discovery 2025-05-01 | 38 | 16 | 431 |
     | confirmation 2026-01-01 | 68 | 4 | 593 |
     | confirmation 2026-05-01 | 110 | 4 | 631 |

   - 39 contracts delivered before 2025-07-28 contribute 772 post-delivery monthly 1h files to the pull.
   - Each dead member contributes a 0 return. The "β = 1 hedge" is therefore about **β 0.82–0.90**,
     falling through the confirmation calendar.
   - That is permissive in exactly the falling-alt world that criterion 5 exists to block (§7.1 and the
     beta-only rows of §3). The §9 statement ("criterion 5 then remains slightly permissive") understates
     a deterministic attenuation.
2. **The exit fallback (§4, D4, §9).** "No bar in the 6-hour window (a delisting …): exit at the close
   of the last bar."
   - If the 1m archive behaves like the 1d and 1h archives (L4 says the 1d files keep normal sizes, and
     the 1h probe above agrees), a listing delisted mid-hold has placeholder bars at the exit target.
   - It then exits at their flat open with status `exit`. The pre-decision count `exit_last_trade` will
     not count delistings.
   - P&L is probably close to the intended last-close proxy. What is wrong is the text, and a count that
     the draft says is "counted before the decision".
3. **Relistings (§4).** "1d files run without a > 7-day gap for every symbol (0 detectable relistings)"
   holds because dead files never stop, not because there are no relistings.
   - RAYUSDT was delivered 2022-11-15 and still has 1d files to 2026-06-30. RAY came back as
     `RAYSOLUSDT@2024-12-10`, a discovery event.
   - The segment rule is vacuous. The scout found its 13 relistings with the metrics rule, not with 1d
     gaps.

**Fix (before REGISTERED; the code is frozen after).**
- Store `n` in `market_1h.parquet`.
- In `Panel.market`, admit a symbol only if it traded in the 24 h before the entry hour (known at entry).
  Take its exit price as the last **traded** open at or before the exit hour, so a perp that dies
  mid-hold keeps its last traded price.
- In `simulate`, treat zero-trade bars after the event's last traded bar as absent when searching for the
  exit bar, so a delisting reaches `exit_last_trade`. Alternatively, add a separate counted status
  `exit_on_dead_rows`. Fix the D4, §4 and §9 text.
- Replace the 0-relistings sentence with the L4 fact.
- Add one test with a flat zero-trade market symbol and one with a dead-tailed listing.
- New hashes (the build changes; the rebuild is from the verified raw files, so no new pull is needed).

### M2. The event set contains ticker migrations, relistings under new tickers and asset-backed tokens

The hypothesis concerns a **new token's** first perp: "supply arrives after the listing", "attention
fades". The rule "first archive segment of a symbol" admits continuations of existing contracts and
tokens. Names and dates only:
- **Ticker migrations of a Binance USDⓈ-M contract.** In the 1d listing:
  - MATICUSDT ends 2024-09-11; POLUSDT@2024-09-13 is an event;
  - RNDRUSDT ends 2024-07-30; RENDERUSDT@2024-07-26 is an event;
  - EOSUSDT ends 2025-05-21; AUSDT@2025-05-28 is an event;
  - FTMUSDT is delivered 2025-01-06; SUSDT@2025-01-16 is an event;
  - KLAY → KAIAUSDT@2024-12-04 is also an event.
- **Relistings under a new ticker:** RAYSOLUSDT@2024-12-10 (RAYUSDT delivered 2022) and VICUSDT@2025-03-12
  (TOMOUSDT ends 2024-05-28).
- **Confirmation calendar,** where the draft says "perp-first: 0 of 148 spot-first". These appear to be
  rebrands or migrations of seasoned tokens (SKYUSDT@2025-09-09 for MKR→SKY, FRAXUSDT@2026-01-15 for
  FXS→FRAX, MANTRAUSDT@2026-03-04, CFGUSDT@2026-03-16) or an asset-backed token (XAUTUSDT@2026-03-26,
  Tether Gold). **All of them have `spot_first = False`.** The Binance-spot-on-d0−1 flag cannot see them.
- **Discovery:** PAXGUSDT@2025-03-27 (tokenised gold).
- **Why it matters.**
  - None of these has a listing pop or a post-listing supply overhang.
  - Gold tokens in particular carry a macro beta that the alt market index does not hedge.
  - They are a few percent of T1 in each calendar. In a 126-event confirmation with an existence test,
    that is not negligible.
  - It also weakens §1's reason for not registering a perp-first trial ("equals T1 there").

**Fix.** Before any outcome, fix an exclusion rule and the list it yields. Names only, from Binance
announcements and the 1d listing:
- (a) ticker changes, migrations and redenominations of an existing Binance contract or token;
- (b) relistings under a new ticker;
- (c) asset-backed or RWA tokens (PAXG, XAUT, …).

Report them as a stratum. A point-in-time alternative: exclude an event when another Binance USDⓈ-M
symbol's 1d files ended within ±14 days of d0 with a public migration notice. Correct the "0 of 148
spot-first" reading in §1 and §2.2.

### M3. "Run once" and "read once" can be undone by erasing rows; 008 already closed this

`result_rows` scans the working tree plus `git log --all -p`. `probe_erase.py` shows the discovery gate
**open again** in each of these cases:

| case | gate after erasure |
|---|---|
| (1) rows written and not committed, then `git checkout -- results.jsonl` | `[]` |
| (2) rows committed, then `git reset --hard HEAD~1` (the reflog still holds them) | `[]` |
| (3) rows removed with `commit --amend` | `[]` |
| (4a) rows stashed: `log -p` hides merge diffs, and the stash commit is a merge | `[]` |

- Confirmation's (i) uses the same scan. A confirmation read can therefore be repeated the same way,
  and D17 (b)'s terminal state "a confirmation row" can vanish.
- In the same round, `r008_run.py` l. 95–102 moved to `git log --all --reflog -p` after review 008-B m1.
  013 copied the older pattern.
- With the scan widened to `log --all --reflog -m -p`, cases (2), (3) and (4a) refuse (probe, second
  block). Case (1) needs the row committed.

**Fix.**
- Use `--all --reflog -m` in `result_rows`.
- Refuse unless `results.jsonl` is clean against HEAD.
- Have the runner commit `results.jsonl` alone right after appending each `*_started` row. A
  `--start` / `--run` split that requires a committed started row also works.
- Add a test per case.

### M4. The read path has no gate: `Panel` and `evaluate` open the real panels for any caller

- `Panel.__init__` checks only that the **caller-supplied** `mode` string allows the calendar. It does
  this with `assert`, which `-O` strips.
- `evaluate`, `run_discovery` and `run_confirmation` read the module globals `DATA` and `RESULTS` and the
  module function `guard`.
- `tests/test_r013.run_e2e_checks` patches exactly these three (`R.guard = lambda mode: 'c0ffee'`,
  `R.DATA, R.RESULTS = root, …`). A "smoke test" that copies the recipe and omits the `DATA` line runs
  T1 and T2 on the real discovery panel. `R.evaluate(R.Panel('confirmation', 'confirmation'))` reads the
  confirmation calendar outright, with no row anywhere.
- This is the label-only lesson of round 2 in a new form: the whitelist is a label passed by the caller.

**Fix.**
- `guard()` sets a module token `_GATE = (HERE.resolve(), mode, commit)` only after
  `guard_failures(HERE, …) == []`.
- `Panel` raises, not asserts, when its resolved `data_root` lies under the real `DATA` and `_GATE` does
  not name the same mode.
- The tests keep passing a synthetic `data_root` (they already do). Add one test that a stubbed `guard`
  plus the real `DATA` refuses.

### M5. Window-level coverage counts are outcome-adjacent; they are printed and stored per event, ungated

- `r013_build` counts over d0 … d0+9, which contains the whole hold.
- **Funding rows.** The archive's settlement interval drops to 1 h only when the rate is at the cap (§0
  item 3 says so). The number of funding rows in a window therefore encodes whether, and for how long,
  funding was capped during the hold. That is the cost the scout calls "the open doubt".
- **Zero-trade rows** in the window encode halts and, after M1, dead tails. So does `rows_1m` set against
  `first_trade`.
- **What was printed.** §2.4 prints the calendar totals: funding rows 15,578 (discovery) and 9,551
  (confirmation), and zero-trade rows 3,068 and 303. Set against the schedule a fixed interval would give,
  these totals carry information about funding stress in **the confirmation calendar**. This reviewer did
  not derive that figure, and it should not be derived.
- **What is stored.** `panel_*/events.json` keeps `funding_rows`, `rows_zero_trades`,
  `internal_missing_minutes` and `rows_1m` **per event**. That file is opened without any guard by
  `week_structure` (`--counts`, `--power`) and by any script.
- The lesson of round 2 applies: "a brief that allows counts must forbid conditioning on … any future of
  the signal".

**Fix.**
- Restrict the build's per-event coverage to pre-entry facts: [d0, first trade + 25 h).
- Compute window-level counts, if wanted, inside the guarded run, printed with the other pre-decision
  counts.
- Rebuild (new hashes).
- Ledger the printed totals as outcome-adjacent exposure in the `000`-type entry for §13, alongside the
  ALPHAUSDT bar.

---

## MINOR

- **m1. Incomplete funding coverage has no rule.**
  - `JUPUSDT@2024-01-31` (discovery) needs funding months 2024-01 and 2024-02 and has only 2024-02. Its
    T2 feature, settlements in (first trade, entry], is therefore computed from a partial set.
  - The coverage table says "events without funding rows: 0" for discovery, because it counts only
    events with no rows at all.
  - **Fix:** if any month spanned by (first trade, exit] lacks its file, mark the T2 feature undefined
    (out of T2, counted) and count the event as "partial funding" before the decision.
- **m2.** The 4 reported-block events with no funding file are simulated with zero funding. State the
  rule in D6.
- **m3. A count of the sealed period was printed.**
  - `logs/list.txt` prints 900 universe-matching symbol directories, of which 787 have a 1d key before
    the cutoff. The 113 difference is, essentially, the number of USDT symbols first listed from
    2026-07-01 on.
  - §2.3 and §13 say later keys are "never saved or counted".
  - **Fix:** correct §13 and ledger it. It is a listing count, not an outcome.
- **m4. `assert` used as a guard.** The `Panel` whitelist, `r013_fetch.assert_before_cutoff` (the
  cutoff rule) and the build's cutoff assertions all vanish under `python -O`. **Fix:** raise
  explicitly.
- **m5. The guard trusts whichever repository git discovers at `HERE`.**
  - A nested `git init` in `research/copytrade` hides the outer history from (f): `probe_nested.py`,
    1 row → 0 rows.
  - `GIT_DIR` or `GIT_WORK_TREE` in the environment does the same, and `-I` does not clear them.
  - (h) blocks this incidentally, because 009's `registration_commit` must be an ancestor.
  - **Fix:**
    - strip `GIT_*` from `_git`'s environment;
    - require `rev-parse --show-toplevel` == the repo root;
    - require an anchor commit (for example `05d0bd1`) to be an ancestor of HEAD.
- **m6. Shadowing.** `HERE` is first on `sys.path`, and `duckdb` and `pyarrow` are imported lazily, so
  an untracked `research/copytrade/duckdb.py` would load. (d) checks only the listed files. This is the
  same as 008-B m11. **Fix:** refuse untracked `*.py` files in `research/copytrade` and `ct/`.
- **m7. D17 (b) is prose only.**
  - The programme line is in an uncommitted edit of `docs/edge-discovery-2026-10-12.md`. No later runner
    checks it, and `panel_confirmation/` is readable by anything.
  - Two things need saying in D17 (b): "days 0–9" bars any universe that admits coins listed fewer than
    10 days before in that calendar (010- and 012-style ≥ 7-day filters overlap), and 012 Stage 1 already
    touched days 7–9.
  - A discovery that crashes after its started row leaves **no terminal state**, so the lock never
    lifts. Define that state, for example a `discovery_failed` row, an owner line, or both.
  - **Fix:** commit the programme line before REGISTERED.
- **m8. (g4) recomputes from stored aggregates.**
  - The row's `events` already hold each event's `net`, `week`, `status`, `premarket`, `t2` and
    `mkt_adj`. From them, n, G, mean, t, median, halves, without-largest-week and the market-adjusted t
    could be recomputed and compared.
  - **Fix:** add `entry_t` and `exit_t` to the compact records, so that criterion 7 can be recomputed too.
- **m9. Counts.**
  - `evaluate` counts `premarket` only for entered pre-market events.
  - `no_entry` mixes primary and pre-market events.
  - Print `no_entry` for primary events separately.
- **m10. (f) and (g1) key on the file name.** A revised file name (`013-…-r1.md`) sees no earlier rows.
  **Fix:** match on the `013-` prefix.
- **m11. The environment is not pinned.** The duckdb, pyarrow and python versions are written only to
  the result rows. **Fix:** write them to the `*_started` row and compare them at confirmation.

---

## Checked and found sound

- **Guard logic.** All of the following hold, and every test probe passes:
  - status, (a)–(e), (f) for working-tree, deleted-in-history and side-branch rows;
  - (h) recomputed with 009's own module;
  - (g1), (g2), (s);
  - (g3): code diff, stored code hashes, stored data hashes against the table;
  - (r): only an approval line may change;
  - (h'): `first_commit_containing` and the strict-ancestor check, which also refuses the same commit;
  - (i).
- **No root/data split.** The CLI's `guard()` evaluates `HERE`, and the run reads `HERE/data/r013`.
- **Decision path hashed.** `ct/factors.py` and `ct/stats.py` match 009's hashes. `random` and the other
  stdlib modules are loaded before `HERE` enters `sys.path`.
- **No decision printed without a row.** The trial statistics and the decision are printed only after
  `append_row`, in both modes.
- **Calendars and cutoff.** In detail:
  - the largest date in `plan.csv` keys is 2026-06-27 (daily) or 2026-06 (monthly);
  - the 1d listing ends 2026-06-30;
  - no file name under `data/r013` is later than 2026-06-27;
  - panel maxima:
    - confirmation: bars 2026-06-27 23:59, funding 2026-06-27 20:00, market 2026-06-30 23:00;
    - discovery: bars 2025-07-26 23:59, funding 2025-07-26 20:00, market 2025-07-27 23:00 (data end
      2025-07-28);
    - reported: bars 2023-12-27 23:59.
  - Discovery and confirmation data are disjoint. Discovery mode cannot open a confirmation panel.
- **Fetch.**
  - `get` never returns None, and a 404 is retried and then raised.
  - `fetch_one` writes only on a SHA-256 match with Binance's `.CHECKSUM`.
  - `_verified.jsonl` records only successes.
  - `SHA256SUMS` is written only when every planned key is verified.
  - Forward paging uses `NextMarker` or the last key, and an empty truncated page and a missing
    `IsTruncated` both raise.
  - The build re-hashes every file against `SHA256SUMS`, which is itself hashed in §10.
- **Schemas.**
  - Funding columns are `calc_time, funding_interval_hours, last_funding_rate`.
  - Klines have 12 columns, and the build's indexes (`r[7]` quote volume, `r[8]` count) are right.
  - 65 of 315 sampled 1m files are headerless; `csv_rows` handles that.
  - Every funding stamp lies in the first minute of its hour (maximum offset 47 ms), so the minute floor
    lands on the settlement. The interval is never blank.
- **First trade.** No event has a 1m file before d0 (all 660 `m1_days_before_d0` are empty), so "first
  trade not on d0 = 0" is a real result, not an artefact of reading from d0.
- **No silent drops.** `events.json` holds 255, 257 and 148 events, including those with missing files.
  Missing minutes are handled by the fixed entry-window, exit-window and fallback rules.
- **Pre-market flag.** It uses d0's index file only. The 22 and 4 flags equal the raw no-index count
  from 2025.
- **TradFi and index exclusions.** They use `contractType` and `underlyingType`, which are fixed at
  listing. ALLUSDT (INDEX, 2025-08) is excluded. TradFi started 2025-12, and no listing from 2025-12-01
  is missing from the snapshot.
- **Market 1h coverage.** Only 2 universe symbol-months have 1d files but no 1h file.
- **Settlement convention.** Entry minute out, exit minute in matches holding [entry, exit). Funding is
  paid at the price at s, with the right sign for a short.
- **Exposure table (§0).** 008 and 014 read no Binance listing windows.

---

## Verdict

**Not ready to register.** There is no blocker. **Five majors** must be fixed in 013's own new files
before the REGISTERED commit, because the code is frozen after it:
- M1, dead-contract rows (index attenuation and the fallback premise);
- M2, the event definition;
- M3, erasable rows;
- M4, an ungated read path;
- M5, outcome-adjacent window counts.

M1, M2 and M5 change the build, so the panels and the §10 hash table must be regenerated from the
verified raw files; no new pull is needed. M5 and m3 also need ledger lines. After the fixes, a short
re-review of the build diff, the new hash table and the M3/M4 tests suffices.

---

## Reviewer's real-data reads (for the ledger)

None computed a price change, return, funding sum, liquidation or P&L on any listing window. Nothing
dated on or after 2026-07-01 was opened.
1. **Test suite**, including its real-file checks: the registration text, the hash table, results.jsonl
   with no 013 row, and `DESCRIBE` of the panel parquet files (column names only).
2. **The real guard, read-only:** `guard_failures` (both modes), `ordering_failures`. Also the
   `registration`, `mode` and `decision` fields of the 12 `results.jsonl` rows.
3. **`data/r013/logs/*.txt` and `panel_*/coverage.json`.** These contain counts already in the draft.
4. **`listing/event_keys.json`** (key names and dates):
   - per-event funding-month completeness, 1m files before d0, and index-on-d0 counts;
   - one event id named: JUPUSDT@2024-01-31, missing funding month 2024-01.
5. **`listing/um_daily_1d_days.json`:** the first and last 1d day of MATIC, RNDR, KLAY, FTM, EOS, TOMO,
   RAY, MKR, FXS, OM, CFX and BTCST USDT, and the file's maximum day.
6. **`panel_discovery` and `panel_confirmation` `events.json`:** all event ids (symbol and d0), plus the
   `spot_first` and `premarket` flags of 20 named events.
7. **`inputs/fapi_exchangeInfo.json`:** contract, underlying and sub-type counts; statuses; the delivery
   dates of SETTLING symbols; the list of excluded in-calendar symbols.
8. **`market_1h.parquet` (discovery and confirmation):** the DISTINCT symbol list and min/max `t`; and
   `symbols.json` first days. Used to count dead constituents (M1).
9. **`bars_1m` and `funding` parquet:** `max(t)` and `max(calc_time)` only. For funding, the count of
   stamps outside the first minute of the hour, the maximum millisecond offset, and the count of blank
   intervals. No rate was read.
10. **`plan.csv`:** key names (maximum date; 1h months per symbol, against the 1d months).
11. **Raw zips.**
    - The header lines of about 1,000 sampled files.
    - FTTUSDT 1h 2024-01 and 2026-06, and SCUSDT 1h 2024-01 (contracts delivered in 2022, not listing
      windows): row counts, zero-trade row counts, and the number of distinct open values (1).
