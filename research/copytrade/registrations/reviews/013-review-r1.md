# 013 Revision 1 — second-round independent review, 2026-10-13

Reviewer brief: verify every first-round blocker and major in code (not in the prose), probe the
mechanisms Revision 1 introduced, and find anything that still blocks registration. Read-only on every
file of the draft (`02fbd1d`). `r013_run.py` was never run in `--discovery` or `--confirmation` mode;
nothing was fetched; no commit was made.

## How this was checked

- **Tests.** `.venv/bin/python -m tests.test_r013` → **120 PASS, then 1 FAIL, then exit 1**. The suite does
  not reach its last four real-file checks (m1). I ran those four by hand (`r0_real_file_rest.py`): the
  §10 table lists every guarded file and every hash matches; no `013-` row exists anywhere; the parquet
  schemas match; `events.json` holds the field names the test expects. The real guard (with `env_check`)
  refuses only on status ("not REGISTERED").
- **Code read in full:** `ct/listings013.py`, `r013_run.py`, `r013_build.py`, and the guard, gate, bypass
  and e2e sections of `tests/test_r013.py`. For parity: the guard half of `r008_run.py`.
- **Probes** are in `research/copytrade/scratch_review013r1/`, with outputs in `*.out` / `out_*.txt` /
  `dep_*.txt`. All are synthetic or use throwaway git repos.
  - `r1_own_sim.py`: an **independent** implementation of the registered path: own OLS + CR1 (it agrees
    with `L.ols_alpha` to 1e-15), own t quantiles (they agree with `L.t_crit` to 1e-4), own caps and
    criteria. It runs on the real first-trade times from `schedule.json`.
  - `r4_regime.py`, `r6_regime_registered_path.py`, `r7_regime_misaligned.py`: β that varies with the
    market's state (B1). r6 runs the same worlds through the **registered** `L.trial_stats` / `L.confirm`.
  - `r5_deploy.py`: the **unmodified** `L.deploy` after a confirm (M1, M2).
  - `r2_integrity.py`: history-scan attributes, shadow modules and the commit hook (M4, m2, m3), in
    throwaway repos.
  - `r3_book_bounds.py`: the tail bounds through `L.book` with an adversarial schedule (m4).
  - `r8_forged_pyc.py`: a forged `__pycache__` file (m3).
  - The first-round probes were re-run where they still match the API. `scratch_review013b/probe_erase.py`:
    reset, amend and stash now refuse; "discard an uncommitted row" still reopens the gate, but only if the
    runner's auto-commit is bypassed (M4). `probe_nested.py` no longer runs (the API changed); its case is
    test (5), which passes.

---

## First-round findings: status

| finding | status | evidence |
|---|---|---|
| **A-B1** raw-net confirmation in a known bear year | **Fixed for constant β.** Not fixed when β varies with the market's state (**B1 below**). | `trial_stats` / `confirm` decide on the OLS intercept, CR1, `alpha_crit` = max(t_{G−2}(q), CAL). Own re-implementation: the decayed-edge world (β 2, confirmation drift 2%/wk) gives P(confirm \| carried) **0.037** (registered 0.029) and joint 0.026 (0.021). |
| **A-M1** overlapping holds; placebo blind | **Fixed.** | Size is simulated with real first-trade times, overlapping holds and a continuous market. The k-scaling bug is gone (`sim_events` scales idio only). Month-block placebo. The raw t decides nothing. The calibration is honest and conservative (§2 below). |
| **A-M2** peso problem | **Partly fixed.** The deploy charge exists, but the deploy rule runs on raw net, so it does not stop false deployments (**M1, M2**). | `L.deploy` charges π_pool and a squeeze-month floor. In the squeeze world the joint false deploy is **0.017**, and **0.043** with the bear drift. §3 says "a deployment cannot". |
| **A-M3** tail bounds wrong against `book()` | **Fixed for every exit status except `exit_late`** (m4). | Through `book()`: week −17.5% and month −47.5%, exactly. With `exit_late`: −32.5% and −50.0%. Book capacity 161 / 83 / 220 reproduced. |
| **A-M4** deploy rule text ≠ code | **Fixed.** | One rule: carried trial, confirmation Kelly, pooled worst months (base and stressed), bars at c\*. The text in §6 and D12 matches `deploy()` line by line. |
| **B-M1** dead rows | **Fixed.** | `market_1h` stores `n` and `c`. `Panel.market` admits a symbol only if it traded in [entry hour − 24 h, entry hour), and uses the last traded closes before the entry and exit hours. `simulate` drops zero-trade bars, so a delisting reaches `exit_last_trade`. Point in time: both index endpoints are ≤ 59 min before the event's own entry and exit. |
| **B-M2** event set | **Rule added, but rule (c) is not mechanism-based and is applied inconsistently (M3).** | `CONTINUATIONS` dict; `evaluate` and `schedule` honour it. |
| **B-M3** erasable rows | **Fixed for the CLI path.** **Reopened through the commit hook (M4).** | `--all --reflog -m -p`; (k); `commit --only` after each row; `*_failed` rows. Tests (1)–(4) pass. B's probe: reset, amend and stash now refuse. |
| **B-M4** ungated read path | **Fixed.** | `_GATE` is set only by the real `guard()`. `Panel` refuses on the real directory, its parent or a symlink to it (`resolve()`). A stubbed guard refuses. Only a deliberate *copy* of the data opens ungated (accepted residual). |
| **B-M5** window-level coverage | **Fixed in substance.** Two small residues (m5). | Per-event coverage is [d0, first trade + 25 h). `--counts` is gone. `schedule.json` holds id, first trade, premarket and continuation only. |
| A-m1 – A-m10 | Fixed (A-m3 slippage on trailing 24 h, exit one tier worse; A-m4 sims use `book()` with both caps; A-m5 trimmed index; A-m7 drift sign; A-m8 deadline; A-m9 `exit_late`). | Code read. |
| B-m1 – B-m11 | Fixed. B-m6 (shadowing) and B-m5 (git environment) are **only partly** at r008's level (m2, m3). | `evaluate`: JUP's pre-entry month gives T2 undefined; hold months give `partial_funding`. No `assert` left in `r013_*.py` or `listings013.py`. Pins checked; versions in the started row. |

---

## BLOCKER

### B1. β that changes with the market's regime gives false confirms of 0.08–0.15. This is the 008-r1 B1 failure, and the confirmation calendar is the case it describes.

**What is not modelled.** `ols_alpha` fits a single pooled β over the calendar. §3 and §9 state that
"β that varies with the market's state" is not modelled. The confirmation calendar is known to have a
**concentrated** fall: the 2025-10-10 cascade and the decline into early 2026 (§0). Suppose the listings
of that period carry a different β from the rest. That is likely: correlations rise in sell-offs, and the
mix of listings shifts. Then the pooled β̂ sits between the two regimes. The residual (β_i − β̂)·mkt_i has
a non-zero mean exactly where the market fell, and the intercept absorbs it.

**Measured.** μ = 0 everywhere; listings with a first trade in 2025-10-06 → 2026-02-28 have β_in, the
rest β_out; the alt market drifts down only in that window. The rows are run through the **registered**
`L.trial_stats` + `L.confirm` (`r6`) and through my own code (`r4`).

| β_in / β_out, crash drift | k | CONFIRM rate, registered code | own code | with quarter-specific β |
|---|---|---|---|---|
| 2.5 / 1.0, 3%/wk | 1.5 | **0.147** | 0.129 | 0.043 |
| 2.0 / 1.25, 2%/wk | 1.5 | **0.076** | 0.074 | 0.039 |
| 2.0 / 1.25, 3%/wk | 1.5 / 1.0 | **0.094** / — | — / 0.115 | — / 0.040 |
| 1.5 / 1.5 (constant β), 3%/wk | 1.5 | — | 0.037 | 0.037 |

- The registered critical value of 1.90 already includes the calibration. These rates are on top of it.
- 008 r1 B1 (cohort- and month-specific loadings, false confirm 0.08–0.14) was a blocker for the same
  reason. `000l` requires that a confirm "not be attributable to a market-wide move already known to have
  paid in these months". A β-regime residual *is* such a move.

**Fix (pre-specify now, then re-run `--power`).**
- Let β differ by **calendar quarter of the first trade**: net = α + Σ_q β_q·mkt·1[q] + ε, with a common
  intercept, CR1 by listing week, df G − 2 as now. That is 4–5 slopes in the confirmation and 6–7 in
  discovery.
  - Measured: the false confirm drops to **0.039–0.043** in the aligned regime worlds.
  - Power cost is small: μ 4%, k 1.5: 0.940 → 0.908; μ 3%: 0.767 → 0.746.
- A shorter crash that is misaligned with the quarters (2025-11-15 → 2026-01-15) yields a smaller bias:
  0.064 registered, 0.070 with quarter β, 0.040 with **month** β. Month β costs about 0.10 power
  (0.806 → 0.705 at μ 4%, k 2).
- Quarter β is the proportionate choice. A state-based alternative, known at entry, is a separate β when
  the index's trailing-30-day return at entry is negative.
- Apply the same model to discovery criterion 1 and to criteria 2–4 (adj = net − Σ β̂_q·mkt·1[q]).
- Add the regime worlds above to `WORLDS` and to the calibration set (`NULL_PREFIX`). Re-derive
  CAL_C / CAL_D, then update §3, §7, D9, D11 and the hash table.

---

## MAJOR

### M1. The economic bars and the whole deploy rule run on the raw net, so the deploy decision follows the calendar's market path.

**Where.**
- Discovery criteria 5–6 (`discovery_criteria`: `s['mean']`, `s['annual_on_capital']` from raw `net`).
- `deploy()`: μ_conf, μ_tail, the book at c\*, the confirmation Kelly and the squeeze months are all computed
  from raw `net` (`r013_run._confirmation_row` passes the raw rows).

**Why it matters.**
- In the known-bear confirmation calendar, the raw mean of a short book is α + β·(the realised fall).
  The forward expectation is α + β·E[mkt], where E[mkt] is about 0 (or positive for crypto).
- The raw series also carries the market's variance. That shrinks the Kelly fraction and adds noise to
  every threshold.
- So, after a correct α-based CONFIRM, "paper" versus "close" is decided largely by what the alt market
  did in 2025–26. That is the `000l` failure, moved one step downstream.

**Measured** (`r5_deploy.py`; the unmodified `L.deploy`, conditional on a confirm unless stated "joint"):

| world (confirmation calendar) | P(deploy \| confirm), registered (raw) | same rule fed adj = net − β̂·mkt |
|---|---|---|
| α 4%, β 1.5, flat market | 0.50 | 0.73 |
| α 4%, β 1.5, alt drift −2%/wk | **0.81** | 0.73 |
| α 3%, β 2, flat | 0.38 | 0.52 |
| α 3%, β 2, −2%/wk | **0.73** | 0.49 |
| α 0, β 2, −2%/wk (false confirms only): **joint** false deploy | **0.020** | 0.003 |

- The same α deploys with probability 0.50 in a flat calendar and 0.81 in a bear one.
- With the adjusted series the decision does not depend on the market path: 0.73 / 0.73.
- In the β-only null, a false confirm becomes a paper deployment 56% of the time (0.020 joint).

**Fix.**
- Feed `deploy()` the adjusted nets (net − β̂·mkt, with B1's β̂). Compute discovery criteria 5–6 the same
  way, as α ≥ 100 bp and the adjusted book ≥ 3%/yr.
- Keep the raw series as **reported** ("what the unhedged book earned in this calendar").
- If the orchestrator wants "the raw book must also pay", use **min(raw, adjusted)**. That is
  conservative in both bear and bull calendars.

  Moving discovery's *significance* to α was right (§2). The economic bars should follow it.

### M2. Squeeze atoms: the confirmation is still not tail-robust (0.077–0.105). The deploy charge does not rescue it (joint false deploy 0.017–0.043).

**Measured** (`r1_own_sim.py`, own code; `r5_deploy.py`, registered `deploy`). The registered critical
value is 1.90.

| world | CONFIRM, registered | CONFIRM, tail-adjusted α (proposed) | P(deploy), registered (joint) |
|---|---|---|---|
| p 2% per event, β 1.5, k 1.5 (§3's world) | **0.077** (§3: 0.079) | **0.019** | 0.017 (β 2) |
| p 1%, β 1.5, k 1 | **0.105** | **0.032** | — |
| p 2%, β 2, alt drift −2%/wk | 0.067 | — | **0.043** |
| p 2%, μ 4%, k 1.5 (power) | 0.658 | 0.601 | — |

- §3's statement "a confirm can happen, but a deployment cannot" is **false**. Given a confirm, deploy
  follows 23% of the time in the plain squeeze world and 65% with the bear drift.
  - The reason is that μ_tail and the book are raw (M1).
  - The confirmation itself is also selected toward samples with few liquidations, so μ_conf is biased
    upward exactly when the peso charge is small.
- **Winsorising does not help.** It removes the −1 atoms from samples that contain them and leaves
  atom-free samples untouched. Atom-free samples are the ones that over-reject, so winsorising raises
  their relative advantage.
- **Proposed fix (cheap): a tail-adjusted α at confirmation.**
  - α_tail = α̂ − max(0, π_pool − π_conf)·(1 + mean net of the confirmation's non-liquidated events),
    with the same CR1 SE and the same critical value. This is §6's μ_tail construction moved inside the
    test.
  - Size: 0.019 (p 2%, k 1.5) and 0.032 (p 1%, k 1). With the uncalibrated t_{G−2}(0.95) it is
    0.033 / 0.043.
  - Power cost: 0.658 → 0.601 in the squeeze-plus-edge world. It is **zero in any world without
    liquidations**, because the adjustment vanishes when π_conf ≥ π_pool.
  - π_pool includes the 2020–23 spot-first block. If the orchestrator prefers a same-regime pool, use
    discovery plus confirmation. Say which, now.
- M1's fix alone, with the deploy fed the adjusted series, already brings the joint false deploy in the
  squeeze worlds to 0.004–0.007. The tail-adjusted α also makes the CONFIRM label itself honest, which
  matters for the ledger.

### M3. Continuation rule (c) is not mechanism-based and is applied inconsistently. The one confirmation name it removes with a "?" is a publicly known adverse event for the short.

- **(a), (b) and (d) are mechanical and mechanism-based.** For (a) and (b), the token, or its predecessor,
  already had a Binance USDⓈ-M perp: no first-perp attention, and shorting was already possible. For (d),
  the gold β is not hedged. These can be checked against Binance's own 1d listing, which the drafter
  already read.
- **(c) contradicts §2.5's own treatment of seasoned tokens.** "A rebrand or token swap of a token
  without a Binance perp, completed ≤ 180 days before d0" (LUMIA, SYRUP, CFG, ASTER, BEAMX) describes a
  *seasoned token receiving its first Binance perp*. §2.5 keeps exactly that case in T1 ("TON, AERGO, DIA
  … mechanism 2 applies"). A ticker rename does not change the mechanism.
- **(c) is not applied mechanically.** By the reviewer's knowledge of public notices (dates to be
  verified, like the "?" names):
  - **VANRYUSDT@2024-03-13**: Virtua TVK → Vanar VANRY swap, Dec 2023, about 100 days before d0; no
    TVK perp. It fits (c) and is **not** listed.
  - **AEVOUSDT@2024-03-13**: Ribbon RBN → AEVO migration at Aevo's TGE; no RBN perp. It fits (c) as
    literally as ASTER does (APX → ASTER at the 2025-09-17 TGE) and is **not** listed.
  - ASTER and AEVO also share what makes them *new* tokens by mechanism 1: a TGE with fresh airdrop or
    Launchpool supply.
- **Direction.** ASTER's TGE was followed by one of the most publicised rallies of 2025 (public market
  knowledge), and its Binance perp listed during it. Removing it under a "?" rule takes a publicly known
  adverse event for the short out of the 119-event confirmation sample. That feeds M2's peso problem.
  - No outcome was computed here. The point is that (c) gives an outcome-aware drafter a degree of
    freedom on the confirmation calendar.
- **Fix:**
  - Replace (a)–(c) with one rule: **"the token, or the token it was renamed, migrated or swapped from,
    had a Binance USDⓈ-M perp before d0"**. Drop the 90-day clause in (a) as well: SKY is a continuation
    of MKR whatever the date of the change. Keep (d).
  - Effect: LUMIA and SYRUP (discovery), CFG and ASTER (confirmation) and BEAMX (reported) return to the
    primary events. Discovery has 239 events, confirmation 121. VANRY and AEVO stay in T1.
  - Two of the three "?" names become moot. PUMPBTC's (a) is consistent with the drafter's 1d facts (the
    PUMPUSDT later segment at 2025-07-10) and with the reviewer's recollection of a Binance symbol change.
    Verify it.
  - No new pull is needed: the 1m files of every event, continuations included, are in the pull. Rebuild
    `schedule.json`, `events.json` and the hashes, and re-run `--power`.

### M4. A mocked commit hook on the real tree reopens "run once" (B-M3 case 1). This is 008-r1 M1 in 013's form.

- `run_discovery` and `run_confirmation` call `HOOKS['commit'](…)` and never check that the row reached
  HEAD.
- The e2e test's own recipe sets `R.HOOKS['commit'] = lambda msg: …`. Use it after REGISTERED with the
  real guard and the real `RESULTS` (the stubs of `DATA` and `RESULTS` omitted). The guard passes, the
  gate opens, the outcome is computed, and both rows stay uncommitted.
- Then `git checkout -- results.jsonl` leaves no trace in history or the reflog.
  - `r2_integrity.py` P4: the started row is not in HEAD; after the checkout, 0 rows are visible.
  - B's `probe_erase` case (1) still reopens on the same basis.
- **Fix (two lines):**
  - after each `HOOKS['commit']`, refuse unless the appended line is in `git show HEAD:results.jsonl`
    (checked with the real `_git`, not the hook);
  - and refuse any non-default `HOOKS['commit']` when `RESULTS` resolves to the real file.
  - Add one test.

---

## MINOR

- **m1. The committed suite fails on the committed state.**
  - `real_file_checks` asserts "(a) untracked", which no longer holds after `02fbd1d`. The suite exits
    at check 121 of 125, and the last four real-file checks (hash table, no 013 row, schemas, field names)
    never run.
  - §10's "125 checks pass" is not reproducible. I ran the four by hand; they pass.
  - **Fix:** drop the "(a)" clause, keeping "not REGISTERED".
- **m2. History and diff calls lack `--text --no-textconv --no-ext-diff`** (r008 has them; 008-r1 m5).
  - `r2_integrity.py`: with `results.jsonl -diff` in `.git/info/attributes` (untracked; `git status` is
    empty), `result_rows` finds 0 of 1 erased rows. A textconv driver does the same. r008's scan finds 1.
  - A `diff.external` set in `.git/config` blanks the (r) check's `git diff -U0 rc HEAD` (0 lines for a
    changed registration). `--no-ext-diff` restores it.
  - **Fix:** add the three flags (and `--no-color`) to every `log`, `diff` and `show` in the guard.
- **m3. The shadow-module check (v) is weaker than r008's `shadow_modules`.**
  - It uses `ls-files --others --exclude-standard -- *.py ct/*.py`. `r2_integrity.py` P3 shows (v) silent
    on four things r008 catches:
    - a top-level package `email/__init__.py`, which **executes** when `Panel` imports duckdb (duckdb
      imports `email`, `base64`, `ipaddress`, `quopri`, `string` and `urllib` after `HERE` is on
      `sys.path`);
    - a sourceless `ipaddress.pyc`;
    - a `quopri.py` hidden by `.git/info/exclude`;
    - `ct/listings013.<ext-suffix>.so`, which the import system tries **before** the hashed `.py`.
  - Shared gap (also r008): a forged `__pycache__/listings013.cpython-314.pyc` whose header carries the
    source's mtime and size is loaded instead of the source (`r8_forged_pyc.py`).
  - **Fix:** use r008's `shadow_modules`. Also set `sys.pycache_prefix` to a fresh temp directory before
    `from ct import …` (verified to load the source), or check that each guarded module's `__file__` is the
    hashed `.py`.
- **m4. `exit_late` breaks the "exact" tail bounds.**
  - An `exit_late` can occur up to the end of the pulled window (d0 + 10 d), about entry + 9 d. That
    exceeds the 7 d 6 h the 15- and 39-day caps assume.
  - Adversarially through `book()`: worst week **−32.5%** (13 exits) and worst month **−50.0%** (20 exits).
    Without `exit_late` they are exactly −17.5% and −47.5%.
  - It needs halts longer than 6 h at the exit target on many events at once: implausible, but §6 says
    "exact".
  - **Fix:** say "for every exit status but `exit_late`". Alternatively widen the windows to 17 and 41
    days, or cap `exit_late` at target + 24 h with a fallback to the last traded close.
- **m5. Two small residues of pre-entry-only coverage.**
  - `funding_months_missing` spans d0 … d0+9, so it can store a **hold**-month fact. A missing month
    file in the hold could encode a delisting. The printed totals (4 / 1 / 0) are all d0-month or
    no-file cases, so no realised exposure.
  - `cov_pre_entry` runs to first trade + 25 h, i.e. up to 60 min **after** entry.
  - **Fix:** store the pre-entry months only, and let `evaluate` derive hold-month gaps inside the gate
    from `event_keys.json`; end the coverage window at first trade + 24 h. Neither changes a decision.
- **m6. The later-segment list decides nothing, but it leaks post-window survival for two events.**
  - `later_segments.json` is hashed and never read by the runner. Events are first files, so there is
    no effect on inclusion. The d0 + 30 d floor means a death inside the hold is never detected.
  - A reported segment of an event symbol implies trading at or after d0 + 30. Two such events exist:
    **AIAUSDT** (a confirmation event) and **PUMPUSDT** (discovery). Each "survived its window".
  - **Fix:** ledger it with §13. It is negligible.
- **m7. The calibration is honest, conservative, and not gameable downward. It is incomplete.**
  - The uncalibrated confirmation size, 0.040–0.064 over 20 worlds × 1,500 draws, is what pure Monte
    Carlo noise around a true size of about 0.053 gives: E[max of 20] ≈ 0.053 + 1.87 × 0.0058 = 0.064.
  - Taking the largest of 40 noisy 95th percentiles as CAL_C therefore over-corrects (registered size
    0.027–0.043). It costs about 0.06 power at μ 2% (k 1.5: 0.538 → 0.473) and at μ 4% (k 2).
  - Adding worlds can only raise CAL, so the grid cannot be chosen to lower it.
  - But the calibration set excludes the squeeze worlds (q95 2.2–2.4) and has no regime worlds. B1 and M2
    address the gap; re-derive after them. A pooled quantile, or more draws per world, would give back
    some power.
- **m8. Report a split-β α.**
  - If listings fall more in market falls than they rise in market rises (downside β 2, upside β 1), the
    linear α is about 0.5·E|mkt| ≈ 4% per event: own sim, "confirm" 0.89 with no edge.
  - That is a real but different (market-convexity) return. It scales with the calendar's volatility,
    and the cascade year was volatile.
  - **Fix:** report α from net = α + β⁺·mkt⁺ + β⁻·mkt⁻ next to the decision. Reported only.
- **m9. The placebo flips the raw net.** The decision is on α. Flip the adjusted series and report the
  α-t share too.
- **m10. Parity with r008 on procedure.**
  - The pins compare version strings only. r008 hashes every file of duckdb and pyarrow (008-r1 m3).
  - There is no "push after each automatic commit" step (008-r1 m6).
  - The reverse gap, for the orchestrator: r008's history scan lacks `-m`, so its stash case is
    unprotected.
- **m11. The two ticker-reuse tokens (PUMP 2025-07, LIT 2025-12) lost by rule are acceptable.**
  - The rule is mechanical and costs one event per calendar.
  - The direction is not favourable to the short: pump.fun's PUMP fell publicly after launch, so dropping
    it is conservative.
  - This is the opposite of ASTER under (c) (M3).

---

## Answers to the brief, in order

1. **First-round fixes.** All are in code, as tabled. The holes that remain are M4 (hook), m2 and m3
   (scan and shadow parity), and m4 (bounds). B's erase probe now refuses reset, amend and stash.
2. **The α decision.**
   - The market return is point in time and dead-row free. It runs over the event's own hold, floored to
     the hour at both ends (≤ 59 min earlier, with the same offset). It uses symbols seasoned ≥ 30 d that
     traded in the 24 h before the entry hour, the event's own symbol excluded, ≥ 20 symbols, 5%-trimmed.
   - **Size cell reproduced** with my own code: β 2, confirmation drift 2%/wk, edge gone. Discovery
     passes 0.709 (registered 0.723); confirm \| carried 0.037 (0.029); joint 0.026 (0.021). Null β 1.5,
     k 1, drift 1%: confirmation 0.038 (0.040).
   - **Power cell reproduced:** μ 4%, k 1.5. Discovery 0.807 (0.807), confirmation alone 0.947 (0.957),
     joint 0.759 (0.772).
   - Calibration: m7.
   - Moving discovery significance to α is coherent and correct. The raw t carried the market variance
     and the drift. "Raw net feeds the economic bar only" is the part that is **not** coherent with
     `000l` (M1).
3. **Squeeze atoms.**
   - The deploy bar's pooled charge is not adequate on raw net: joint false deploy 0.017, and 0.043 with
     the bear drift (M2).
   - Make the confirmation tail-robust with α_tail. Size 0.019–0.032; power cost ≤ 0.06, and 0 without
     liquidations.
   - Winsorising goes the wrong way.
4. **Tail and size.**
   - Through `book()`, the bounds are exact for exit, `exit_last_trade` and liquidation; `exit_late`
     breaks them (m4). −29.7% = 19 × 0.625 × 2.5%.
   - Deploy: one definition, carried trial, deployed size; text = code (A-M4 fixed).
   - "Peso charge ≥ 4.6%/yr": f_eff = max(f_pool, 1/6.42) = 0.156 × 29.7% = 4.63% at c\* = 2.5%. It is
     charged only as max(0, f_eff − f_conf), so it is 0 when the confirmation itself contains a squeeze
     month. §6 states that correctly as "the floor alone".
5. **Exclusion list.**
   - (a), (b) and (d) are mechanism-based and point in time. (c) is ad hoc and inconsistently applied
     (M3).
   - No rule uses post-listing information. Later segments decide nothing (m6).
   - The two lost tokens are acceptable (m11).
6. **Integrity:** M4, m2, m3, m10. Sound: GIT_* stripped, top level and anchor, the gate token, refusals
   not asserts, `-O` refused, the pins, `*_failed` rows terminal, `commit --only`, (g4) from records.
7. **Coverage:** pre-entry in substance (m5). `events.json` is gated and `--counts` is gone. The
   later-segment detection cannot affect inclusion and cannot see the event's own window (m6).

---

## Checked and sound

- **`simulate`.** Zero-trade bars are dropped throughout, which is equivalent to "absent after the last
  trade", because flat bars cannot liquidate. `exit_last_trade` has exit_t ≤ t_x. Funding is sampled at
  the last traded price. The marks use the close of the last held bar.
- **`Panel.market`'s `b ≥ end` refusal cannot fire on a legitimate event.** Exits are ≤ d0 + 10 d < the
  data end, given the embargo. So it cannot turn a real run into a spurious `discovery_failed`.
- **`recheck_discovery` from per-event records.** `compact` carries everything `trial_stats` reads.
  `book` without marks gives the same `annual_on_capital`. The JSON floats round-trip exactly.
- **Gate.** It is per mode and per process, set only after `guard_failures == []`. Symlinks resolve.
  Discovery cannot open the confirmation panel.
- **"α > 0 without the best week" costs 0.000** in every world of `power_r1.json` (verified from the
  file).
- **Book capacity.** 161 / 83 / 220 events taken, i.e. 104 and 92 a year. Margin turnover is 2.6× and
  2.3×, as stated.
- **Hash table.** It is complete and matches the disk. No extra rows.

---

## Verdict

**NOT READY: one blocker, four majors. All are cheap, well-defined fixes. Register after a Revision 2
that applies them, with a short re-check of the new `power` log, the rebuilt event counts and the hash
table.**

- **B1:** quarter-specific β in the α model (discovery and confirmation), the regime worlds added to the
  calibration, CAL re-derived.
- **M1:** economic bars and `deploy()` on the market-adjusted series (or min(raw, adjusted)).
- **M2:** a tail-adjusted α at confirmation (state the π_pool calendars).
- **M3:** one continuation rule ("had a Binance perp, itself or its predecessor") + (d); LUMIA, SYRUP,
  CFG, ASTER and BEAMX return to the primary events. Rebuild the events and hashes; no pull.
- **M4:** verify HEAD after each hook commit; refuse mock hooks on the real `results.jsonl`.
- **Minors:**
  - fix the stale test (m1);
  - copy r008's scan flags and `shadow_modules`, and set a `pycache_prefix` (m2, m3);
  - qualify or widen the tail bounds (m4);
  - trim the coverage residues (m5);
  - ledger m6;
  - report the split-β α and the α placebo (m8, m9).

---

## Reviewer's real-data reads (for the ledger)

No price, return, funding value or sum, liquidation count or P&L on any listing window was read or
computed. Nothing dated on or after 2026-07-01 was opened. No paid or keyed source was used.
1. The test suite, including its real-file checks: the registration text, the hash table, the count of
   013 rows in `results.jsonl` (0), parquet column names (`DESCRIBE`), and `events.json` **field names**.
   The same checks by hand (`r0_real_file_rest.py`) and the real guard's refusal list (status only).
2. `data/r013/schedule.json`: primary events' first-trade times (the simulations' only real input, as in
   §3) and the event **ids** (symbol@d0) of the discovery and confirmation calendars, names only, as
   already printed by the drafter (§13).
3. `data/r013/logs/power_r1.json` (synthetic results).
4. Git metadata of the real repository: `git status`, `git log --oneline`, and `check-ignore` of a few
   paths.
5. **Public market knowledge cited, not computed:**
   - ASTER's TGE and its post-TGE rally (2025-09);
   - the TVK → VANRY swap (2023-12);
   - the RBN → AEVO migration (2024-03);
   - pump.fun PUMP's post-launch decline (2025-07);
   - the PumpBTC symbol change.

   These are exposure notes for `000`-type ledgering, like §0's cascade line.

---

## Revision 2 recheck (2026-10-13, `323899e`)

**Scope.** I re-ran my probes against `323899e` and checked each fix in code. This is not a third review.
- All work was done in a throwaway clone at `323899e`, in the session scratchpad, with `data/r013`
  symlinked read-only. Bytecode writes were off (`-B`).
- The probes and outputs are copied to `scratch_review013r1/r2/`. Nothing in the real repository was run,
  staged or committed. No `.py` was created at the top level or in `ct/`, so 008's running fetch is
  untouched.

**Tests.** In the clone, `.venv/bin/python -B -m tests.test_r013` → **"all checks passed (145)"**. The
real-file checks run to the end (r1 m1 fixed).

### Status of the r1 findings

| r1 | status | verified in code / by probe |
|---|---|---|
| **B1** regime β | **Fixed** | `alpha_fit` / `ols_cols` fit one β per calendar quarter of first trade. `beta_groups` merges any quarter with < 10 events into the previous group (a short first group joins the next). `quarter` comes from the first trade (`evaluate`) and is stored in `compact`, so the recheck uses the same grouping. Regime worlds are in the calibration set. My own code (r9, no import of `listings013`) gives, against `power_r2.json`: regime β 2.5/1.0, crash 3%/wk, k 1.5 → confirmation alone **0.037** (registered 0.031); at k 1 **0.040** (0.039). The same worlds gave 0.13–0.15 before. |
| **M1** economic bars on raw net | **Fixed** (conservatively) | Criteria 5–6 use `mean_econ` / `annual_econ` = min(raw, adjusted). `deploy` runs `deploy_one` on both the raw and the quarter-β-adjusted series: c\* is the smaller, and it deploys only if both do. The revised code on r5's worlds (r12) gives a joint false deploy of **0.005** with β 2 and −2%/wk (r1: 0.020), and **0.0025** with squeeze plus bear (r1: 0.043). |
| **M2** squeeze atoms | **Fixed** | `trial_stats(pool=…)`: α_tail = α − max(0, π_pool − π)·(1 + mean non-liquidated net); t_tail = α_tail / se(α); the same charge on the best-week refit. `confirm` decides on α_tail. Own code: p 2%, k 1.5 → **0.024** (registered 0.021; untailed 0.079); p 1%, k 1 → **0.025** (0.023; untailed 0.091). |
| **M3** continuation rule | **Fixed** | One rule (p): the token or its predecessor had a Binance USDⓈ-M perp before d0. Plus (d), gold tokens. `continuation_failures` checks point in time that the predecessor's first 1d file is before d0; the build refuses otherwise. I re-ran it on the 1d listing: **0 failures**. Every (p) predecessor dates from 2020-08 → 2025-04, before its d0. |
| **M4** mocked commit hook | **Fixed** | `record()` appends, commits through the hook, then refuses unless the line is in `HEAD:results.jsonl`, read with the real `_git`. P4a: a no-op hook is refused ("not in HEAD"). `real_tree_failures()` runs before and after `guard`. P4b: a mock hook on the real tree is refused. P4d: synthetic results with the real data, and vice versa, are refused. P4c: the default hook on the real tree passes. |
| m1 stale test | Fixed | 145/145. |
| m2 scan flags | Fixed | `--text --no-textconv --no-ext-diff --no-color` on `log`, `diff` and `show`. With `-diff` and with a textconv driver in `.git/info/attributes`, the erased row is still found (1/1). |
| m3 shadow / bytecode | Fixed | Ported `shadow_modules`: all four plants (top-level package, sourceless `.pyc`, excluded `.py`, `.so` in `ct/`) are refused. `sys.pycache_prefix` is a fresh temp directory set before `from ct import …`, and `loaded_from_source()` checks `__file__`. |
| m4 `exit_late` bounds | Fixed | The late exit is capped at target + 24 h; after that the exit is `exit_last_trade` at the last pre-target close (verified with a 30 h halt). Windows are now 16 and 40 days. Adversarially through `book()`: week ≤ −17.5%, month ≤ −47.5% for every status. Capacity is now 156 / 80 / 219 events (101 and 89 a year), matching §6. |
| m5 coverage residues | Fixed | `funding_months_missing_pre_entry` covers [d0, first trade + 24 h]; hold months are derived in the gated `evaluate` from `event_keys.json`; `PRE_ENTRY` is 24 h. |
| m7 / m8 / m9 / m10 | Fixed or reported | Calibration over 55 worlds. Split-β α and α placebo are reported. Environment fingerprint and push procedure are in place. |
| erase probe (B's `probe_erase`) | Same as r1 | Reset, amend and stash refuse. Cases (1) and (4b) need rows written outside `record()`, which now refuses to continue unless the row is in HEAD. |

### Reproductions (own code, 1,500 draws)

| cell | own | registered (`power_r2.json`) |
|---|---|---|
| **power** μ 4%, k 1.5: discovery / confirmation alone / joint | 0.794 / 0.925 / **0.729** | 0.803 / 0.911 / **0.725** |
| **regime size** β 2.5/1.0, crash 3%/wk, k 1.5: discovery / confirmation alone | 0.043 / **0.037** | 0.053 / **0.031** |
| regime size, k 1 | 0.039 / **0.040** | 0.037 / **0.039** |
| squeeze p 2%, k 1.5 / p 1%, k 1: confirmation alone | 0.024 / 0.025 | 0.021 / 0.023 |

### The tail-adjusted α, checked as asked

- **Sign is correct.** The charge subtracts only when the trial's own liquidation rate π is below π_pool.
  It is one-sided: max(0, ·), with no credit for an unlucky confirmation. Replacing one non-liquidated
  event by −1 moves the mean by −(1 + its net), hence the (1 + mean non-liquidated net) factor.
  - That mean is the *raw* non-liquidated net, so in a bear calendar the factor is larger. This is the
    conservative direction.
- **Same SE.** The variance of π_pool − π is ignored. Measured size is still 0.021–0.025 in the
  homogeneous squeeze worlds, so this is immaterial.
- **Can π_pool be gamed?** Not after registration.
  - The pool is fixed in code: the carried trial's primary events over reported + discovery +
    confirmation, the same rule in `_confirmation_row` and in `simulate_worlds`.
  - The discovery row does reveal the reported-block and discovery liquidation counts before the
    approval line. But no parameter can change after that ((r), (g3)), and a `DECLINED` line only closes
    013.
- **What the pool can do is dilute.** The 2020–23 spot-first block is about 41% of the pool. If its
  squeeze rate is lower than the perp-first regime's, the charge shrinks. `r13_pool_dilution.py` (own
  code; perp-first p 2%):
  - reported block p 0 → confirmation size **0.049**;
  - reported block p 0.5%, k 1 → **0.057**;
  - for comparison, 0.021–0.024 when the regimes match, and 0.077–0.091 untailed.

  So the fix degrades toward nominal but does not fail. **Minor (r2-m1):** use
  π_pool = max(π over all three calendars, π over discovery + confirmation). One line; same calibration
  logic.

### Continuation rule: applied point in time and consistently

- **VANRY, AEVO, ASTER: in T1.** Their predecessors TVKUSDT, RBNUSDT and APXUSDT have no 1d file in the
  listing; neither do ORN, MPL, INST, MC, UST or BEAM.
- **CFG: in T1.** CFGUSDT's first file is the event itself (2026-03-16).
- **PUMPBTC (p, PUMPUSDT):** PUMPUSDT's first file is 2025-04-12, before 2025-06-13. Point in time ✓.
  - The *mapping* (that PUMPBTCUSDT is PumpBTC's contract renamed) is still a public-notice fact that the
    code cannot check. It is consistent with the PUMPUSDT trading gap and the 2025-07-10 later segment.
- **DODOX (p, DODOUSDT):** DODOUSDT's first file is 2021-02-19, before 2023-08-08 ✓. Reported block only.
- USTC left the stratum by the same rule. UST had no perp, and it is algorithmic, not backed. Reported only.
- **Counts:** schedule 239 / 121 / 251 primary events; continuation stratum 14 / 5 / 4. All match the
  registration.
- Residual (accepted): the rule cannot detect a *missing* predecessor mapping. I found none in a
  name-by-name pass of both calendars.

### New observation (not blocking)

- **r2-m2. The min(raw, adjusted) deploy rule is conservative, not path-free.** For a true 4% edge,
  P(deploy | confirm) is **0.40** in a flat calendar and **0.61** at −2%/wk (r12). In a flat market the raw
  side's market noise binds; in a bear one the adjusted side does.
  - This was the orchestrator's choice. It adds no false deploys (0.0025–0.005), but it costs deploy
    power, mostly in calm calendars.
  - State it in §6's expected-return text: a deploy needs both the hedged and the unhedged book to pay.

### Verdict

**REGISTER.** B1 and M1–M4 are fixed in code and reproduced with independent code. No blocker or major
remains. The two items above (r2-m1: pool = max(all, 2024+); r2-m2: one sentence in §6) are minor and can
go in the REGISTERED commit or be left as stated weaknesses.

**Real-data reads in this recheck:**
- the test suite's real-file checks (registration, hashes, 013-row count, parquet column names,
  `events.json` field names);
- `schedule.json` (first-trade times, flags, continuation ids);
- `listing/um_daily_1d_days.json`: the first 1d day of the 21 predecessor symbols and the absence of 10
  more, as listed above (names and dates only);
- `logs/power_r2.json` (synthetic).

No price, return, funding, liquidation or P&L on any listing window was read or computed. Nothing dated on
or after 2026-07-01 was opened.
