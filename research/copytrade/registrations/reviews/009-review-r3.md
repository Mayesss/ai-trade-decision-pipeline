# 009 second review (Revision 3 / 3a draft): F1 weekly funding carry on Binance USDⓈ-M

An independent, adversarial second review, written 2026-10-11. Read only:
- `r009_run.py` and `r009_pull_holdout.py` (fetch / rest / build) were not run.
- No forward return, IC or book P&L was computed on real data.
- Every probe was synthetic, a throwaway git repo, or a count of files and
  listing metadata. The probes live in the session scratchpad, not in the repo.

**Verdict: NOT YET READY FOR THE OWNER.**
- **No blockers.**
- **Four majors**, all cheap: code guards, the hash table, D14 and
  disclosure. None needs a new measurement on real data.
- **The first review's findings are fixed** in code and text, with two
  partial exceptions: M3's "row for that commit" and the scope of B1.
- **The weak point is the holdout-A guard.** It trusts a label in
  `results.jsonl` that anyone can write. It does not check the pass, the
  pull conditions, or when the approval was given.

## How this was checked

- **Tests.** `.venv/bin/python -m tests.test_factors` gives 112 PASS, exit 0.
- **Hashes.**
  - All 11 rows of the §8 table match the files on disk.
  - `r009_pull.py` (`8b35d2fa…`) and `r009_describe.py` (`ca3ecd32…`) are
    not in the table.
- **`describe.json`.** Every §3 number was reconciled against it. The
  mismatches are listed in m2.
- **Probes (synthetic or metadata only):**
  - **p1:** `run_discovery` end to end on a synthetic world, with the guard,
    loader and results path monkey-patched. Every reporting block ran. The
    row was written; there was no crash.
  - **p2:** `guard_failures(mode='holdout_A')` in a throwaway git repo, fed
    forged, removed, early-approval and other-branch rows.
  - **p3:** the holdout pull's date gate, with no network. Also the
    holdout `plan.csv` already on disk, which is listing metadata only.
  - **p4:** months whose funding file is missing, counted from
    `fetch_state.jsonl` statuses (file existence only).
  - **p5:** what other on-disk Binance data covers 2024–26, from file
    names only.

---

## 1. First-review findings: status

| finding | status | where |
|---|---|---|
| **B1**, 010 overlap | **Fixed, but the scope is too narrow (see M4).** | §0 l.142–158 and 000g; D17 (l.674). `r009_run.checked` l.57–62; `binance_years.connect` l.215–216; `r009_pull_holdout` l.35. Static scan in `tests/test_factors.py` l.493–500. 010 side: `r010_run.ordering_failures` l.75–88, and `r010_build` l.101–102 refuses `binance_years` |
| **M1**, F2 drift | **Fixed.** `FAMILY = ('F1',)` and `Z_CRIT` = 1.6449 (factors.py l.81–83). `sim_inputs` uses drift costs (l.709–710). Breakeven 13.41 bp = `describe.json` | z = 1.645 is consistent in §1, §3, §4, §6, D6, D10, D13 and D14 |
| **M2**, beta | **Fixed in mechanics.** `beta_adjusted` l.600; `pull_conditions` l.613–623. Market factor in `sim_once` l.766–769. For the calibration caveat see m1 | weekly series joined on S (r009_run l.181–188) |
| **M3**, holdout built | **Built and hashed. The guard is weak (M1 below).** The first review asked for "a discovery-pass row **for that commit**", and that binding is missing | r009_run l.130–139, 240–260; `r009_pull_holdout` l.50–54 |
| **M4**, disclosure | **Fixed** for the two named statistics: §0 l.159–170 and 000g. The leg counts are removed from `r009_describe` (l.105–110). The F1 numbers carry a remaining overclaim (m7) | |
| **M5**, costs | **Fixed.** D13(b) in `pull_conditions` l.618–621, computed on a 2× book (r009_run l.213). Stress tiers at factors l.111–116. The "upper bound" wording is withdrawn (§2 l.322) | |
| minor 1, join on S | **Fixed** | r009_run l.181–188 |
| minor 2, hard end | **Fixed.** `LAST_EXIT_DISCOVERY` l.56. The assert is at l.341–342, called with the true exit (r009_run l.198). The view filter is in `connect` l.218. Tested one ms tight | |
| minor 3, guard (f) | **Fixed for HEAD history only** (m9). `ct/__init__.py` is hashed, and versions are recorded (l.191) | r009_run l.73–92 |
| minor 4, docstring | **Fixed** | factors.py l.1–28 |
| minor 5, float ties | **Fixed** | `derive` l.263 (`FUND_DECIMALS`) |
| minor 6, tie counts | **Fixed** (16 / 4 / 94-of-188, equal to `describe.json`) | §3 l.363–367 |
| minor 7, ICP | **Fixed** by bridging. For the look-ahead nuance see m4 | factors l.121–167 |
| minors 9–12, argument | **Fixed** | §0 l.118–135; §7; §1 table; r009_run l.215, 226 |

---

## BLOCKER

None.

---

## MAJOR

### M1. The holdout-A guard checks a label, not a recorded pass, a pull condition or a timely approval

**Location.**
- `r009_run.guard_failures` l.134–137 (g, h) and `result_rows` l.73–92.
- `run_holdout_a` l.242–243.
- Registration Rev 3 item 4 (l.54–59) and §6 (g) (l.511–512), which say
  (g) "needs a pass *and* both pull conditions".

**What the code accepts** (probe p2, throwaway repo). The holdout guard
returned **ALLOWED** in all of these cases:

1. A 009 row whose `decision` is `["pull_holdout_A", ["F1"]]`, whose
   `results.F1.pass` is **False** and whose `pull_conditions.all` is
   **False**. The row was uncommitted, in the working tree only.
2. A genuine `close` row, plus a second, hand-appended bare row
   `{"registration": …, "decision": ["pull_holdout_A", ["F1"]]}`. That makes
   two discovery rows, which the discovery guard can never produce.
3. A pull row that survives only as a **removed** line in git history:
   `result_rows` also reads `-` lines.
4. An approval line `Holdout A: APPROVED <date>` committed **before**
   discovery ran. As soon as the run writes a pull row, `fetch` and
   `--holdout-A` go through, and the owner never sees the discovery result
   or the byte count. D13's "then the owner approves with a byte count" is
   not enforced.

**Why it matters.**
- The pull conditions are computed by the guarded run and stored in the
  row, but nothing downstream reads them.
- They are as editable as any JSON line.
- The repo's own test writes exactly such a bare row, and the guard admits
  it (`tests/test_factors.py` l.625–627).
- A duplicated row also changes `taken`, so `n_taken = 2` and z = 1.96 (l.243).

**Fix (code; then the hash table is recomputed).** In mode `holdout_A`,
require all of:
- (g1) **exactly one** 009 discovery row in the tree plus history, using `--all`;
- (g2) that row is in `git show HEAD:results.jsonl`, i.e. committed;
- (g3) its `registration_commit` is an ancestor of HEAD, and
  `git diff --quiet <that commit> HEAD -- <GUARDED_CODE>` holds, so the code
  is unchanged since the run;
- (g4) the decision is recomputed from the stored numbers:
  - `fx.decide(row['results']) == tuple(row['decision'])`;
  - `pull_conditions.beta_adjusted_t ≥ Z_CRIT`;
  - `beta_adjusted_mean > 0` and `net_2x_mean > 0`;
  - every one of the five criteria is consistent with its stored statistic;
- (h') the commit that adds the approval line is **later** than the commit
  holding the discovery row. Better, the line quotes that row's sha256:
  `Holdout A: APPROVED 2026-xx-xx row <sha256>`. Optionally, the commit's
  author is the owner.

Add tests for cases 1–4 above.

### M2. The holdout pull's fetch and date gate live in an unhashed module

**Location.**
- `r009_pull_holdout.py` l.32 imports `r009_pull`, then uses `P.fetch`
  (whose per-key date assert is at `r009_pull.py` l.193–194), `P.manifest`,
  `P.read_listing`, `P.key_last_date` and `P.universe_names`.
- `GUARDED_CODE` (r009_run l.49–50) and the §8 table omit `r009_pull.py`.

**What is not covered.**
- The holdout guard's (d) and (e) do not cover the code that actually
  downloads holdout data, nor its "≤ 2025-07-28" assertion.
- `plan` is unguarded by design and already on disk. It writes
  `data/binance_years_holdoutA/plan.csv`, which is unhashed, and `fetch`
  re-reads it.
- The holdout universe and life table are built from
  `data/scout_l4/listings/*.csv` and `exchangeinfo/fapi_exchangeInfo.json`
  (`by.build` → `life_from_listing`; the plan's `universe_names`). None of
  them is hashed.
- Re-scraping the listing would silently change holdout A's universe, its
  delisting cuts and its plan.

**What p3 found (no network).**
- After `point_pull_here()` the gate is correct:
  - monthly 2025-07 is refused;
  - daily 2025-07-28 is allowed;
  - daily 2025-07-29 is refused;
  - REST is end-dated 2025-07-28 00:59:59 (l.39, l.100).
- The plan on disk has 25,193 files and 494 symbols, with a maximum key
  date of 2025-07-28.
- So **today** the pull fetches only holdout A plus the 23 h of holdout B's
  features-only week on 2025-07-28, which is filtered on read (m11). It
  fetches nothing from holdout B proper or from 2026-07 on. The problem is
  that this is not frozen.

**Fix.**
- Add `r009_pull.py` to `GUARDED_CODE` and §8.
- Add rows for the listing CSVs used (`daily_metrics`, `daily_klines_1d`,
  `monthly_klines_1h`, `monthly_fundingRate`) and for
  `fapi_exchangeInfo.json`.
- In `fetch`, assert that `plan.csv` equals a re-derivation from the hashed
  listing. Or hash the plan now; it is listing-only.
- Optionally, hash `r009_describe.py` for provenance of §3.

### M3. Holdout A decides without the cost and beta robustness required of discovery, though its sample is more T4

**Location.**
- `factors.holdout_confirm` l.638–647 and D14 (l.671).
- `r009_run.run_holdout_a` l.251–256, which computes only the 1× book and
  the weekly series.
- §7 l.549–553 says holdout A's extra breadth is mostly new T4 coins near
  the floor.

**Why it matters.**
- The first review's M5 reasoning ("holdout A … uses the same cost model,
  so a cost error passes straight through to a CONFIRM") was answered only
  at the discovery pull (D13 b).
- In the deciding sample:
  - the T4 share, where slippage evidence is weakest (T4's 6 bp is below
    the Abdi–Ranaldo p75 of 8.8 bp), will be **higher**;
  - the run computes no 2× book and no stress-tier block at all;
  - the beta-adjusted series is reported, but no test is applied to it.
- A CONFIRM that rests only on the 1× T4 cost assumption would be decisive.

**Fix (D-table and code, before registration).**
- `run_holdout_a` computes and stores the 2× book, the stress-tier book and
  `pull_conditions`, with the same code as discovery.
- D14 adds "net at 2× slippage mean > 0" (the cheapest choice). Or it
  states explicitly that no cost stress applies at the decision, and why.
- Optionally add "beta-adjusted mean > 0" as well.

### M4. B1's disclosure and hard rule cover `data/carry/` only; more Binance holdout data sits on disk

**Location.**
- §0 l.142–158 and l.171–177; 000g §1.
- `r009_run.checked` l.57–62 is a **blacklist** of one path.

**What is on disk** (p5, file names only):
- `data/scout_l2/binance/funding/`: Binance USDⓈ-M funding for **213
  symbols** in the quarterly months 2023-07 … 2026-04. That includes holdout
  A's **2024-01, -04, -07, -10, 2025-01, -04, -07**, holdout B's months, and
  monthly files for three core coins.
- `data/scout_l2/binance/klines_um` and `klines_spot`: BTC, ETH and SOL,
  monthly from 2023-01 to 2026-06.
- `data/binance/um/aggTrades*`: six majors, 2025-07-28 → 2026-06-30
  (holdout B).

**What it means.**
- The first is a cross-section of Binance funding inside holdout A, so the
  funding-persistence half of F1's payoff (the 56.5 bp/week trailing gap)
  can be computed from it.
- `scouts/l2-carry.md` says it computed no persistence. 009 and 000g
  disclose only "unconditional funding medians by year".
- Nothing written forbids the next scout or registration from computing
  per-coin funding persistence on it before 009's holdout A is read. That is
  the same class of exposure as B1, with nothing yet computed.

**Fix.**
- Disclose all three in §0 and 000g.
- Turn `checked` into a **whitelist**: `data/binance_years/`,
  `data/binance_years_holdoutA/`, the hashed listing files, and
  `results.jsonl`.
- Widen D17 (000g) to the programme: no computation of Binance
  cross-sectional funding persistence or returns dated 2024-01-01 →
  2025-07-28, from any directory, until 009's holdout A is read or closed.

---

## MINOR

**m1. The beta adjustment is calibrated circularly.**
- `sim_once` generates returns with the ex-ante β as the true β (l.769) and
  subtracts the same β (l.775). The test's "exactly −costs" (test l.522–524)
  is true by construction, so the 1.2–2.7% false-pull rate assumes a beta
  estimated without error.
- In reality β comes from 30 daily returns (FEATURE_SQL l.214–221), and the
  short leg is 69% T4. Non-synchronous trading likely makes the weekly β of
  illiquid coins larger than their daily β, so the book is more net short
  than estimated.
- The size is small on these data. The weekly market s.d. is 10.3%, and an
  analytic bound puts a 0.15 β error in a −2%/week bear at about +0.8 in t.
- **Fix:** also report the ex-post regression intercept of the weekly net on
  m, with a NW t, and state the calibration assumption in §3.

**m2. Text numbers disagree with `describe.json`.**
- **l.454:** "0.54 to 0.32". The data say 0.524 → 0.305, and the table says
  0.52 → 0.31.
- **l.418:** "48%" stopped. It is 1 − 0.0055/0.0115 = **52%**.
- **l.435:** "59–83%". It is **56–83%** (1 − 0.0265/0.0605 = 0.56).
- **§4 l.478–480:** "lets through only about the one-sided α share" is
  wrong. Of the beta-only passes, **17% (sampling) and 44% (σ_IC 0.15)**
  are let through. Unconditionally the false pull is 1.2–2.7%; say that
  instead.
- **Test l.544–546:** the check has an `or p_pass_and_pull ≤ 0.05` escape
  that makes it nearly vacuous.

**m3. The universe text does not match the code.**
- §2 item 6 (l.281) and D3 make "F1 available" a universe rule, but
  `in_universe` (factors l.277–278) does not require it.
- The unscored coin-weeks (15,262 − 15,169 = 93, ICP's 26 among them) do
  **not** change k or the weights (`buffered_targets` l.397). They **do**
  enter:
  - `market_returns` (l.591–597), and so the beta-adjusted net;
  - the β market (FEATURE_SQL l.214–218);
  - the placebo shuffle (l.555–563), where `None` scores are permuted onto
    scored coins;
  - the simulation's dispersion.
- This is harmless, but say it: "F1 is needed to be scored; the market is
  the whole universe".

**m4. The bridging uses a little future information.**
- A day is "active" if any bar that day traded (l.138–139). Membership at S
  therefore uses S's own Monday, which is after S.
- The `end_forward` and `start_back` moves happen only when metrics resume
  later. A relisted coin's earlier segment therefore runs over its last
  run-off day (CVC +1 day), while a coin that never relists does not.
- The effect on the discovery book is nil: ICP is unscored in those weeks,
  and CVC moves by one day.
- **Fix:** define activity with bars ≤ S − 1 h, or state the nuance in §2.

**m5. Missing funding settlements silently count as zero.**
- The outcome funding is `coalesce(…, 0)` (OUTCOME_SQL l.320–326), so
  missing settlements become F = 0.
- p4 found 36 symbol-months with a klines file but no funding file
  (TLM 16, BNX 10, ICP 10).
- **Fix:** report the book coin-weeks whose `n_settle` is below what the
  interval implies, and assert there are none in the book, or treat them as
  missing.

**m6. The F2 rejection has no provenance on disk.**
- The drift-inclusive F2 costs (15.07 / 15.44 / 28.7 / 29.1 bp) are in no
  log or JSON on disk, and the code that measured them is deleted.
- That rejection moves z from 1.96 to 1.645.
- The gate is behaviour only and pre-set, and it follows the first review's
  recommendation, so this is not a selection problem. But the 28-day value
  variant missed by **0.07 bp**. That is inside the gap between
  `turnover_path` and `run_book`:
  - 00:00 closes against 01:00 opens;
  - no feasibility filter;
  - no delisting close.
- **Fix:** archive the measuring code and its log, with a hash, and say
  that the gate measure is not the run measure.

**m7. One outcome-adjacent statistic is undisclosed, and the "no return" wording overclaims.**
- The market move (mean −0.13%/week, s.d. 10.3%) and the yearly dispersion
  are realised returns over **106 of the 107 discovery outcome windows**.
  The trailing week at S is the outcome window of S − 1w, shifted by 1 h.
- They are used unconditionally, which is legitimate. But with β_book at
  −0.10 they imply that the discovery beta P&L is about **+1 bp/week**.
  Revision 3a was drafted knowing this.
- Overclaims: l.10 ("No return after any signal time … computed"), §3's
  title (l.335) and its method step 5 (l.410), and factors.py l.24 and
  l.707.
- **Fix:** reword to "no return was paired with a score", and add the
  statistic to §0 and 000g.

**m8. D17 can deadlock, and its texts disagree.**
- 000g says 010 may proceed after "a discovery fail".
- 009 D17 says after "a fail or unmet pull conditions".
- `r010_run.ordering_failures` (l.75–88) opens only on decision `close` or
  on a holdout-A row. So `close_conditions_unmet`, or an owner who declines
  the pull, blocks 010 for ever.
- **Fix:** define a closing row (for example `holdout_A_declined`) and
  align the three texts.

**m9. The history scan misses other branches.**
- The 009 `result_rows` runs `git log -p` (HEAD only). `r010_run` uses
  `--all`.
- p2 case 4: a 009 row committed on another branch does not block a
  discovery re-run.
- **Fix:** use `--all`.

**m10. The ledger row is written last.**
- The row is written only after every block (r009_run l.231). A crash after
  `outcomes` leaves no row, and a re-run is then allowed. That conflicts
  with "never evaluate without a ledger row".
- p1 ran clean on synthetic data, so the risk is low.
- **Fix:** append a `started` row before `outcomes`.

**m11. The holdout data and holdout B are not frozen.**
- `run_holdout_a` loads `data/binance_years_holdoutA/panel` without
  checking it against its MANIFEST.
- **Fix:** record the MANIFEST and panel sha256 in the holdout row, or in a
  committed line before the run.
- The 2025-07-28 daily files bring 23 h of holdout B's features-only week
  onto disk. They are filtered on read; say so in §6.
- D15 (holdout B) has neither criteria nor a guard. Fix both now, or state
  that they will be registered after a CONFIRM.

**m12. The simulated and real market differ.**
- The simulated market move includes funding (r009_describe l.178:
  `ret7 − fund7`). The run's `mkt` is price only (factors l.595).
- The gap is small, about 18 bp/week of mean funding, but it is a
  definition gap between the gate's calibration and the run.

---

## Checked and found sound

**Pull conditions.**
- Both are computed inside the guarded discovery run, on discovery data only
  (r009_run l.212–214), and are stored in the row (`ev['pull_conditions']`).
- β is point in time: 30 daily closes stamped at or before S − 1 h,
  regressed on a market whose membership is fixed at S.
- The NW lag is 4, and z is 1.645.
- The residual is net − β_book·m_hold, where β_book is taken over the
  feasible target weights.
- The 2× book uses the same `run_book`, and its membership is identical.

**F2.**
- No code path computes F2 features or outcomes.
- `grep` of factors, the runner and describe finds only comments.
- The four variants are recorded with drift costs (§1 table).

**The market factor.**
- It is drawn i.i.d. and unconditionally from the trailing-week moves.
- It is never paired with scores.
- The idiosyncratic quantiles are assigned by the planted copula, not by
  real scores.

**Point in time.**
- Features use bars at or before S − 1 h and settlements before S.
- Outcomes use entry and exit opens.
- The funding hour bar is the bar at f.ts − f.ts % 1 h.
- `connect(max_ts)` hides everything after the true last exit.
- The holdout `build` asserts that every stamp is before 2025-07-29.

**Holdout pull.**
- `fetch`, `rest` and `build` each call `holdout_guard()` first.
- `plan` and `manifest` read only listing metadata or local files.
- The per-key gate and the REST end date are correct (p3).

**Costs.**
- Drift turnover equals what `run_book` charges, on the test case
  (test l.551–562).
- The 2× and stress-tier rates use the same formula in the simulation and
  in the run.
