# Independent review of 012 DRAFT Revision 1 (HL–CEX funding carry), 2026-10-11

Reviewer brief: adversarial review of `registrations/012-hl-cex-funding-carry.md` (Revision 1),
`ct/carry012.py`, `r012_fetch.py`, `r012_build.py`, `r012_run.py`, `r012_describe.py` and
`tests/test_carry012.py`, with 010's `ct/carry.py`, `r010_*.py` imported read-only. Read-only.
`r012_run.py` was not run in any outcome mode. Nothing was fetched. No P&L, funding accrual, price
change or gap change was computed on real data.

**What was run.**
- `.venv/bin/python -m tests.test_carry012`: **110 PASS, "all checks passed (110)"**.
- `.venv/bin/python -m tests.test_carry`: all checks passed (010's suite, unchanged).
- SHA-256 of every §8 file: **31 of 31 match the disk**. The 21 rows shared with 010 are
  **identical to 010's §8**. `git status` on `ct/`, `r010_*.py`, `scouts/` and 010's registration is
  clean, apart from the untracked `ct/carry012.py`. Reuse integrity holds.
- The real guard in `stage1` mode (refusal check only): it refuses on status, (a), (c) and (d). The
  (h) ordering state is `close`, so 009's terminal state unlocks it.
- Scratchpad probes, all synthetic:
  - `probe_months.py`: the gated fetch's `months_in` with a monkeypatched S3 listing, in a temp dir;
  - `probe_guard.py`: seven guard probes in a throwaway git repo.

**Reviewer exposure (for the ledger).**
- Key names only of `data/carry/logs/describe.json` and `data/carry012/logs/describe.json`, plus the
  null-size power cells of the latter (already in §3).
- The first 4 bytes of each of the 209 Bybit 240-minute cache files: 188 have data, 17 are `[]`
  and 4 are `null`.
- For the 21 symbols without data, three things, with no rate, price or volume value:
  - per-coin **row counts** of BY and HL funding in the Stage-1 window;
  - BY daily-close row counts;
  - whether BY funding runs to the cut.
- The max timestamp of each Stage-1 panel file.
- `results.jsonl`: the registration and mode of each line, plus the keys, `by_year` and `n` of 010's
  row.
- The L2 scout's 2025 / 2026-H1 HL−CEX quantiles (`scouts/l2-carry.md`), which the drafters had
  already read.

---

## 0. Answers to the brief, in short

| probe | finding |
|---|---|
| Selection: is every degree of freedom frozen? | Yes, apart from the pair set (disclosed). θ, H, costs, tiers, floor, seasoning, name check, exits and liquidation all come from `ct/carry.py`, byte-identical to 010's (§8). The HL-only primary differs materially from the lead (discovery 337 vs 288; Stage 1 679 vs 581), and §0 says so honestly. Its discovery P&L was never computed, so the choice was not outcome-driven. |
| L2 trial 3 | Correct for L2. **The HL-year count is missing (M5).** |
| Has HL-pair P&L for 2025-01 → 2026-06 been computed anywhere? | **No.** Evidence: 010's row (line 9) covers only `by_year` 2023 and 2024 (n 2,135); no 010 holdout or exposed-block row exists in the tree or in `git log --all -p`; no 012 row exists; the describe files hold only counts, level SDs and synthetic power. **Caveat:** the scout's unconditional HL−CEX quantiles for 2025 and 2026-H1 (core median +3 to +5 %/yr) were read, so the sign of Stage 1's *gross funding* is partly anticipated. §0 discloses it. |
| Is the sign gate described honestly as a filter? | **Yes** (§0, §3 Reading, §7, D12, §10.2). It is computed only by `run_stage1` behind the guard (`r012_run.py:320-341`). But the joint false-pass rate is mis-framed (M4). |
| Stage-2 guard: every element present? | Yes: REGISTERED, (a)–(e), (f) over `git log --all -p`, (n1) anchored lines, (g1)–(g4), (h') sha and ordering, (t) end + 7 d, (i) started, built and read-once rows. All of 010-r1's ported probes refuse. My extra probes: approval before the row → refused; approval on a side branch → refused. **Gaps: M3 and m3.** |
| Fetch window | Stage 2 pulls 2026-05-01 → end, which is look-back (stated). The extension runs only through `stage2_gate('extension', …)` (r012_fetch.py:208-220). Correct. **But the build cannot run (B1), and completeness is not checked (M2).** |
| D18 | Not coherent as written, and not enforceable beyond 012's own reads (M6). Wording proposed below. |
| Power / nulls | Arithmetic matches `describe.json`. There is a window artifact (m1). The decisive α is above nominal in the raw bound (M4). Compression and the HL tail are in §7 but not in the simulation (m6, m7). |
| HL-specific costs and fills | No inconsistency: <br>- HL taker 4.5 bp (`carry.py:47`) is the base tier. <br>- HL 8h candles feed eligibility (the close of the bar ending at t) and the fill (the open of the bar starting at t), the same in both builders (`r012_build.py:52-59`, `r010_build.py:161-170`). <br>- HL volume = v × close in both stages. <br>- HL alt mm 16.7 % (`carry.py:56`) is the conservative (3×-max) end. |

---

## BLOCKER

### B1. The Stage-2 (and extension) build cannot run: the gated fetch never saves `binance/months.json`.

- **Where.**
  - `r012_fetch.py:68`: `_memo` treats **any dict without a non-empty `rows` key** as "empty" once
    `LIMIT > CUT`, and does not save it. The test was written for `bn_file`'s `{'bytes', 'rows'}`.
  - `r012_fetch.py:257`: `months_in` memoises its `{symbol: [months]}` dict through `_memo`, and
    that dict has no `rows` key.
  - `r012_build.py:134`: `months = load(base / 'binance' / 'months.json')` returns `None`.
  - `r012_build.py:138`: `{s: months[s] for s in hlmap}` then raises `TypeError`.
- **Probe** (`probe_months.py`, no network). Running `months_in` with `LIMIT` at the 2026-12-31 end
  returns the dict, but `months.json` is **not persisted**. With `LIMIT == CUT` it is.
- **Why it blocks.**
  - After Stage 1 the guarded code is frozen: (g3) diffs it against the Stage-1 row's commit, and
    the row stores its code hashes (`r012_run.py:155-158`).
  - So the bug cannot be fixed after Stage 1 without an amendment. The decisive read would stall
    after `stage2_fetch_started`.
  - The tests never exercise the gated fetch → build → run path end to end, which is why this
    slipped through.
- **Fix.**
  1. Save the listing with `_save` (not `_memo`), or make the emptiness test kind-specific (only
     `{'rows': []}` from `bn_file`).
  2. Add a synthetic end-to-end test: monkeypatch the HTTP primitives, then run `stage2()` →
     `r012_build.stage2()` → `run_stage2()` in a temp repo, for the `stage2` and `extension` kinds.

---

## MAJOR

### M1. The hashed Stage-1 panel is missing Bybit prices: two error answers cached as null, and Bybit-delisted coins lost.

- **Where.**
  - `r012_fetch.py:85-86`: any `retCode != 0` (a rate limit included) returns `None`.
  - `r012_fetch.py:68-70`: before the cut, `_memo` caches that `None` **permanently**.
  - §2, lines 172-173, reports "188 with rows" and stops there.
- **What the cache holds** (first bytes and row counts only):
  - **`null`:**
    - **AVAXUSDT** and **VIRTUALUSDT**. Both have full BY funding (1,596 and 3,191 rows), full HL
      funding (12,768 rows each) and 532 BY daily closes.
    - ACE and ARK, which have no HL funding, so they do not matter here.
  - **`[]` while BY funding and daily closes exist:**
    - **AI16Z** (297 BY daily closes), **MKR** (217) and **PUMP** (54).
    - All three have HL funding, and all three stopped settling on Bybit before the cut.
- **Effect.**
  - HL-BY can never be entered on these five coins, because an entry needs a BY close at t. The rule
    then takes HL-BN instead, or nothing.
  - AVAX and VIRTUAL are lost by accident.
  - The three delisted coins reproduce **010's B2 survivorship pattern**: Bybit's 240-minute endpoint
    does not serve them now, so their pre-delisting HL-BY positions (the divergence tail) are absent.
  - The 679 count and the panel hashes in §8 inherit both problems.
- **Fix.**
  1. Raise on `retCode` other than 0 and "unknown symbol", and never cache `None`.
  2. Delete the 4 null files, re-fetch (bounded), rebuild, re-hash, recount §3, and ledger the
     recount.
  3. For Bybit-delisted symbols, either build 4h bars from Bybit's public trade archive (end-bounded)
     or state the hole in §7 as survivorship, with the count of HL-BY coin-days lost. Print that
     count before the decision at both stages.

### M2. Stage-2 completeness is checked only for internal gaps in funding, and Stage 2 is read once.

- **Where.**
  - `r012_build.py:171` calls 010's `coverage_check` (`r010_build.py:239-253`), which only looks for
    gaps longer than 3 days *between* settlements.
  - `r012_fetch.py:276-279, 288-289`: the results of `hl_candles` and `by_kline` are discarded
    unchecked.
  - `r010_fetch.py:231-234`: two HTTP 500s from `_hl_info` return `[]`, read as "unknown coin".
  - `months_in` reads the S3 listing as it stands at fetch time.
- **Failure modes, each silent:**
  - a transient HL 500 burst removes a coin from the universe, or leaves it with no HL prices;
  - a `None` from Bybit leaves a symbol in the universe with no prices, so it has no HL-BY entries,
    and its exits fall to the raw or worst fill;
  - a December Binance file not yet listed on 2027-01-08 truncates every BN series at Nov 30. The
    series then reads as a delisting (`carry.py:377-380`), and the coverage check passes because a
    truncated tail is not an internal gap.
- **History.** 010-r1 N2 asked for "per-venue coverage up to end − 8 h"; only the internal-gap test
  was built.
- **Fix:** in 012's own `r012_build.stage2`, leaving 010 untouched, refuse unless all of these hold:
  1. Every venue-coin series that is live at end − 8 h reaches end − 8 h. Liveness comes from an
     independent listing: Binance daily files or exchangeInfo `deliveryDate`; HL `meta`, recorded at
     fetch time.
  2. Every venue-coin with funding has price bars covering its funding life with no gap > 3 days.
  3. The end month's Binance funding and kline files exist for every symbol that has the previous
     month and no delivery date.
  4. The HL probe treats a non-answer as a retry, never as "unknown"; check against HL `meta`.

  Print the counts before the decision.

### M3. The owner lines are not frozen at Stage 1, so q can be chosen after the Stage-2 data are on disk.

- **Where.**
  - `r012_run.py:142` (guard) and `:349` (run) read `owner_choice` from the *current* text.
  - Nothing compares them with the Stage-1 row.
  - A fetch or build can be repeated (`:173-181` only blocks once a stage has started or been read).
  - The raw Stage-2 JSON sits unguarded between build and run.
- **Probes** (`probe_guard.py`):
  - **P1:** after the Stage-1 row and its approval, a committed flip `Stage-2 extension (owner): YES
    → NO` leaves the fetch gate **open**, with ext = False. The first look then runs at
    t_{G−1}(0.95), not 0.975: a 2× change in α.
  - **P5:** a second fetch and a second build after a build are both **open**.
  - **P7:** the run gate is **open** with uncommitted `stage2_fetch_started` and `stage2_built`
    rows.
- **Fix.**
  1. Store `owner: {end_date, extension}` in the stage1 row and in `stage2_fetch_started`, and
     require equality at every later mode.
  2. Require that `git diff <stage1 registration_commit> HEAD -- <reg>` adds only lines matching
     `APPROVAL` or `EXT_APPROVAL`.
  3. Refuse a second `*_fetch_started` or `*_built` row unless its panel hashes are identical.
  4. Require the fetch-started and built rows to be committed before `--stage2`.

### M4. The decisive false-positive rate is mis-framed and runs above nominal.

- **Where.** D14 (line 618), §3 Reading (lines 277-279), Revision 1 bullets (lines 14-15), §7 "Few
  clusters" (lines 403-406).
- **The headline is the wrong number.**
  - The headline "joint false pass 0.020–0.028" multiplies Stage 2's own rate by the sign gate's
    ~0.5. That holds only under a *global* null (no edge in 2025–26 either).
  - The null that matters is "no edge in 2026-H2". In the world the drafters consider likely (HL
    above CEX in 2025–26, the scout's quantiles), Stage 1 passes with probability near 1.
  - The false-confirm rate is then **Stage 2's own: 0.049 (robust) to 0.064 (raw) for the owner
    design** (`describe.json`, `p_stage2_alone` at mu = 0). The raw figure is above the nominal 5 %.
- **§7 understates it.** It quotes only the synthetic 0.026 / 0.051 / 0.046 (`tests/test_carry012.py:213-244`).
  Those nulls have independent month shocks. They have no weekly common factor and no holds
  overlapping across month boundaries, and those two features are what make the month-clustered SE
  anti-conservative in the describe simulation.
- **Fix.**
  1. State α = Stage 2 alone (≤ 0.064, owner design) as the operative rate in §5 and D14.
  2. Label the joint figure "under a global null only".
  3. Optionally calibrate the per-look critical value on the simulated raw null, so that each look is
     ≤ 0.025.

### M5. The family counts are incomplete.

- **Where.** §1, lines 129-139; D16.
- **The HL year.**
  - Stage 1 computes outcomes on HL funding and HL prices inside the **HL discovery year**
    (2025-07-28 → 2026-06-30).
  - Programme doc §2 says that year stands at 15 trials, and that "any registration reading it again
    states the count and treats a pass as the reason to read the holdout".
  - 012 must state **HL year: trial 16**.
- **Binance 2025–26.**
  - Stage 1 also consumes Binance funding and prices over 009's holdout A (2024-01-01 → 2025-07-27)
    and holdout B (2025-07-28 → 2026-06-30) for the HL coins.
  - 009 closed, so this is allowed. But those windows can no longer be called unread for funding
    hypotheses on these coins. State it.
- **The holdout family of 2** (D18) needs a stated family-wise rate: at most the sum of the two
  rules' sizes.

### M6. D18 is not coherent as written.

- **Where.** D18 (line 622), §1 lines 136-139, §10.1.
- **Problems.**
  1. It conflicts with the programme rule (read once, and only for a rule that passed discovery)
     without saying so:
     - The calendar may be read three times: 012's first look, 012's extension, and L3.
     - 012's "discovery pass" is a filter that passes ~50 % of null worlds.
  2. **Order and exposure are not handled.**
     - 012's Stage-2 outputs reveal HL positioning-related levels for 2026-H2: HL funding vs CEX,
       the HL-short share, liquidations.
     - L3 (inverted positioning) is undrafted. If 012 reads first, L3 is drafted with that exposure.
  3. **The CEX calendar is ignored.** The programme holds back 2026-07 → for every light. 012's
     Stage 2 consumes Binance and Bybit 2026-H2 funding and klines for HL coins; D18 calls these
     "outside the HL calendar".
  4. **HL holdout data already on disk is not named:**
     - `data/cache/hl-funding` runs to 2026-07-10;
     - `snapshot_accounts.parquet` holds ~98 holdout snapshots.

     012 never reads either: the whitelist refuses `data/cache` (tested). D18 should say so.
  5. **Enforcement.**
     - 012's code enforces only 012's own reads:
       - the fetch asserts `t1 <= LIMIT` and is tested;
       - `Panel` reads `t < end`;
       - the Stage-1 panel holds no row past the cut except m4.
     - Nothing stops a third registration. That rule has to live in the programme doc and in each
       later guard.
  6. **The release of 010 §6** is recorded only inside 012. It belongs in the programme doc and the
     ledger; 010's frozen file should not be edited.
- **Proposed D18 wording (owner).**

  > **D18 — Hyperliquid holdout calendar (amends programme doc §2).**
  >
  > (a) **Calendar.** Every Hyperliquid series, market or account (funding, candles, trades, books,
  > fills, snapshots, vaults), dated 2026-07-01 → 2027-06-30. It is unread. On-disk copies
  > (`data/cache/hl-funding` to 2026-07-10, the holdout rows of `snapshot_accounts.parquet`) are
  > never read by any registration.
  >
  > (b) **Allocation.** Exactly two pre-registered rules may read it, each once, each through its own
  > guard:
  > - **012**: HL `fundingHistory` and 8h `candleSnapshot` only, for coins with a Binance USDT-M
  >   perp, 2026-05-01 → 2026-12-31. Its pre-registered extension to 2027-06-30 is part of its one
  >   read.
  > - **L3 (008)**: the series its registration names.
  >
  > No third registration reads it.
  >
  > (c) **Discovery condition.** For 012, the Stage-1 sign filter is accepted as the "discovery pass".
  > The owner accepts that 012's error control rests on Stage 2 alone (≤ 0.064 by simulation).
  >
  > (d) **Order.** Either L3 is REGISTERED and committed before 012's Stage-2 fetch, or the owner
  > records that L3 waives this calendar. Whichever rule reads second ledgers the first rule's
  > printed Stage-2 outputs as exposure.
  >
  > (e) **Multiplicity.** Holdout family = 2, with no adjustment. The family-wise false-confirm rate
  > is at most the sum of the two rules' sizes.
  >
  > (f) **CEX months.** 012's Stage 2 consumes Binance and Bybit funding and klines dated 2026-07-01 →
  > its end for coins with an HL perp. Any later registration on those series and months states 012
  > as exposure.
  >
  > (g) **010 §6.** Its condition "decided on data after the holdout end" is released by 010's closure
  > at discovery (no 010 row reads 2026-07+). This is recorded in the programme doc and the ledger;
  > 010's file is not edited.
  >
  > Optional enforcement: 012's Stage-2 gate also requires one anchored owner line,
  > `HL holdout allocation (owner): APPROVED <date>`.

---

## MINOR

- **m1. Power-simulation window artifact.**
  - `carry012.py:129-132`: with `extension_span_ms` = 365 d, window starts are confined to
    2025-01-15 → about 2025-07-01, the high-entry-rate months.
  - The owner design is therefore scored on 247 positions against 221 for one look (`describe.json`
    `stage2_mean_positions`). That flatters the extension in §3 and D15; "221–247" is this artifact,
    not real uncertainty.
  - Windows are not month-aligned: 6.5 clusters, against the real G = 6 at t₅(0.975) = 2.571.
  - Fix: draw month-aligned 6-month first looks from one common set for both designs, and headline
    the 2026-H1 rate (146 positions).
- **m2. A vacuous test.** "(g4) forged Stage-1 numbers" (`tests/test_carry012.py:544-549`) forges
  the month t, which is not a decision input. The refusal comes from (g1), the history duplicate.
  §8 line 528 overstates it. Relabel it, and add a case where the history is purged, so that (g4)
  alone must catch a forged `mean`.
- **m3. No `stage1_started` requirement.** Probe P2: a hand-written stage1 row, with no
  `stage1_started` row, opens the Stage-2 gate. Require exactly one committed `stage1_started` row
  with the same `registration_commit`, and stage1 `data_hashes` equal to §8.
- **m4. A row stamped at the cut.** §2, lines 189-192, says "0 in the build", but 193 rows of
  `panel_stage1/path.parquet` are stamped exactly 2026-07-01 00:00 (`r012_build.py:54`,
  `tc <= p_end`). They are close-time stamps of the 06-30 16:00 bar: their content is pre-cut, and
  `Panel` reads `t < end`. Reword.
- **m5. 79 vs 80 coins.** §2 line 160 says 79 HL coins; 010's `hl_hold.log` says 80. Reconcile.
- **m6. HL-only wicks.** §7 lines 393-395 says these are missed. Report a block that liquidates the HL
  short on HL's own 8h high: the gated pull already fetches HL candles, so store `high`. Stage 1
  spans the 2025 HL-specific squeezes.
- **m7. Gaussian power ignores the −N jump.** One HL alt liquidation is R ≈ −0.15, which is ~15–25 %
  of Stage 1's expected sum at a true 2–3 %/yr. The sign-gate pass rates at 2–5 %/yr are therefore
  optimistic. State it.
- **m8. The paper venue pair is chosen post hoc.** D14 lets "a named venue pair" be picked after the
  Stage-2 by-pair results. Fix the paper book now (both HL pairs, pooled), or make the choice part
  of the paper registration's selection.
- **m9. Inherited (h) probes.** 010's (h) bypass probes (off-branch, foreign commit, mislabelled,
  wrong sha, deleted) are not repeated in 012's tests. They are covered by `test_carry` (passing),
  and `r010_run.py` is hash-locked. Say so in §8.

---

## Verdict

**Not registrable yet.**

**What holds.**
- The design is honest about its post-hoc origin and its weak Stage 1.
- Every rule degree of freedom except the disclosed pair set is inherited byte-identically.
- No HL-pair P&L for 2025–26 exists anywhere.
- Both test suites pass.
- The ported guard probes refuse.

**What must change before registration.**
- **B1:** the decisive Stage-2 build would crash, and after Stage 1 it cannot be fixed without an
  amendment.
- **M1:** the hashed Stage-1 panel silently lacks Bybit prices for AVAX and VIRTUAL (cached errors),
  and for three Bybit-delisted coins (survivorship).
- **M2:** Stage-2 completeness is unchecked for read-once data.
- **M3:** q can be switched after the Stage-2 data are on disk.
- **M4–M6:** the error rate, the family counts and D18 need rewording, and owner sign-off on D18.

After B1 and M1–M3 are fixed in 012's own files (010 untouched), with §8 re-hashed, §3 recounted and
the recount ledgered, the draft is registrable. M4–M6 are text, plus D18 for the owner.
