# Second independent review of 010 DRAFT Revision 1 (cross-venue funding carry), 2026-10-11

Reviewer brief: adversarial re-review of `registrations/010-cross-venue-funding-carry.md` (Revision 1),
`ct/carry.py`, `r010_fetch.py`, `r010_build.py`, `r010_describe.py`, `r010_run.py`,
`tests/test_carry.py`. Read-only. `r010_run.py` was not run in any mode. Nothing was fetched. No P&L,
forward funding or price change was computed on real data.

**What was run.**
- `.venv/bin/python -m tests.test_carry`: **109 PASS, "all checks passed"**.
- `shasum -a 256` of the §8 files: all 11 match the table.
- Three synthetic-only probes in the scratchpad: `probe_opens.py` (B1 residue), a regex probe on the
  registration text (N1), and a throwaway git repo for the ordering guard (N3).

**Reviewer exposure (for the ledger).**
- Timestamp-only queries on the panel: per venue, the row count, the count with `t % 8h == 0`, and
  min/max `t` in `prices.parquet` and `levels.parquet`. The open-time alignment of the 213 HL 8h candle
  files was also checked (0 misaligned of 91,448 sampled).
- `describe.json` keys, and the discovery `opened` count by pair and year (already in §3).
- No rate, price, premium, level or volume value was printed.

---

## 1. First-review findings: fixed?

| finding | status | where |
|---|---|---|
| B1 persistence funnel | **Fixed.** `plan` skips a held coin before `candidates` (`ct/carry.py:249-250`). `counts` holds only `opened` (`:244, :261`). `000f` is written. D5 is frozen; open question 2 is closed (§9 D5, §10). The fresh holdout replaces the exposed one (§2). Residue: see N7. Stale docstring: `r010_run.py:9-10` still says "edge skips". | carry.py:242-267 |
| B2 OKX survivorship | **Fixed.** `PAIRS` has no OK leg (`ct/carry.py:45`). OKX is reported only (`r010_run.py:369-372`). §2, §3 table and D1 are corrected. | |
| B3 liquidation | **Fixed.** Liquidated → `gp = -1 + (liq_close/ref - 1)`, gap dropped, full fees (`ct/carry.py:359-361, 370`). Tests at `tests/test_carry.py:219-230`. The trigger reads the bar's high in (t_k − 8 h, t_k] after entry, and liquidation is checked before the top-up (`:292-298`), so the bar order is conservative. The 1× long correctly has no liquidation. Last-trade highs overstate liquidations relative to mark-price triggers, which is also conservative. | |
| M1 criteria | **Fixed (option b).** 1b: month t ≥ t_{G−1}(0.975) (`ct/carry.py:411, 421`). The point bar is kept and its P(pass) stated (§5.5, D12). Not Bonferroni-adjusted: see m-c. | |
| M2 HL gap | **Fixed.** HL legs use 8h `candleSnapshot` closes with `tc = o + 8h` (`r010_build.py:153-156`). Alignment: all 192,008 HL price rows and 320,808 HL level rows are on the 8h grid; 0 misaligned candle opens. The entry gap reads the bar *ending* at t (`r010_run.py:237-241`); no candle that closes after the decision is used. Residue: fill timing (N6). | |
| M3 slippage | **Fixed as tiers, not measured impact** (`ct/carry.py:50, 114-124`; `r010_run.py:219-220`). The ×2 block includes rebalances (`r010_run.py:315`). §7 states that no `bookDepth` was pulled. Accepted. | |
| M4 holdout pre-built | **Partly fixed.** The code is built, tested and hashed, and the month-clustered t is in `holdout_confirm` (`ct/carry.py:483-490`). The gate around it is broken or weak: see N1, N2. | |
| m1 HL mm 16.7 % | **Fixed.** | `ct/carry.py:56` |
| m2 exit lag ≤ 24 h | **Fixed** (`ct/carry.py:363`), but the missing positions now leave the primary: see N4. | |
| m3 weight reset | **Fixed.** | `ct/carry.py:315-329` |
| m4 history scan, versions | **Fixed** (`r010_run.py:51-73, 326-329`). | |
| m5 OKX last-trade | **Fixed.** | `r010_build.py:184-188` |
| m6 placebo | **Fixed.** | `ct/carry.py:433-458` |
| m7 tie-break ≤ 2024-12 | **Fixed.** | `r010_build.py:66, 90` |
| m8 "rate at t" | **Fixed.** | §4.1 line 315 |
| m9 §11 | **Fixed.** | §11 |

---

## 2. New findings

### BLOCKER

**N1. The owner's holdout option is not read reliably. Option A is already "chosen" by the instruction
text, and choosing B still runs A.**
- **Where.**
  - `r010_fetch.py:556`: substring test `f'Holdout option (owner): {opt}' in text`.
  - `r010_run.py:144`: guard (g) regex `Holdout option \(owner\): [AB]\b`.
  - `r010_run.py:385`: `re.search(...).group(1)`, the *first* match.
- **Cause.** The registration contains the literal instruction "`Holdout option (owner): A` or `B`"
  twice (§8 line 560, D14 line 610).
- **Probe on today's file.**
  - The (g) regex already matches twice, both on "A".
  - The fetch gate for A already passes its option test.
  - If the owner adds `Holdout option (owner): B` under D14, `main_holdout` still picks **A**, because
    §8 comes first. The gates for A and B then both pass.
  - So the end date is not frozen as the owner's choice. A run can silently read the 3-month span when
    the owner chose 6, and (f) then forbids the correct run.
- **Fix.**
  1. Parse one anchored line, e.g. `^Holdout option \(owner\): ([AB])$`, outside any table or quote.
  2. Require exactly one match in fetch, build and run.
  3. Reword the two instructions so that they cannot match.
  4. Better: read the date itself (`Holdout end (owner): 2026-12-31`) and assert that it equals
     `HOLDOUT_END[opt] - 1 day`.
  5. Add a test on the real registration text.

### MAJOR

**N2. The holdout fetch and build gate checks labels in the working tree only. It has no approval, no
commit check and no completeness check.**
- **Where.** `r010_fetch.py:546-563` (also used by `r010_build.py:106`).
- **What it checks.**
  - The `Status: REGISTERED` string in the working-tree file. It does not check that the file is
    committed, unmodified (b), has clean code (d), or matches the hashes (e).
  - That a working-tree `results.jsonl` line has `decision == 'read_holdout'`. It does not recompute
    that decision from the stored criteria, check that the row is committed, or check its
    `registration_commit`.
- **Approval is not encoded.** D13 says "ask the owner to approve the holdout fetch", but no code
  requires it. Compare 009's `APPROVAL` regex: `Holdout A: APPROVED <date> row <sha256>`, committed
  strictly after the row (`r009_run.py:56, 187-195`).
- **Completeness is not checked.**
  - Nothing checks that the end date has passed and the monthly files exist.
  - `_zip_rows` returns `None` on a 404, and `_memo` caches that `None` permanently
    (`r010_fetch.py:86-91, 162-165`). A Binance month fetched too early is lost for good.
  - In `outcome`, a funding series that stops early is treated as a **delisting** (`ct/carry.py:351-353`).
    Option B fetched before about 2027-01-05 would therefore produce truncated "delisted" outcomes, not
    an error.
- **Nothing marks the holdout as started.** The panel can be fetched, built and inspected repeatedly
  before `--holdout`, and no "started" row exists (009 writes `holdout_A_started`).
- **Fix.**
  - Port 009's (g1)–(g4) and (h') into one `holdout_gate` that fetch, build and run all call:
    exactly one 010 discovery row, committed in HEAD; its `registration_commit` is an ancestor of HEAD;
    the guarded code is unchanged since then; `decide` is recomputed from the stored `criteria`; and a
    committed approval line names the row's sha256.
  - Refuse unless now ≥ end + 7 days. Never cache a 404 for a month at or after 2026-07.
  - Assert per-venue coverage up to end − 8 h before building.
  - Append `holdout_fetch_started` before the first request, and require the run's panel hash to
    equal the hash recorded at build time.

**N3. The ordering guard (h) unlocks on any label, from anywhere, including deleted and uncommitted
rows.**
- **Where.**
  - `r010_run.py:76-95` with `result_rows` (`:51-73`).
  - The test "a 009 row deleted but in history still unlocks" (§8).
- **Probe (throwaway repo).**
  - An uncommitted line `{"registration": "009-binance-weekly-factors.md", "decision": "close"}`
    returns `close`.
  - A `{"mode": "holdout_A"}` line committed only on an unrelated branch returns `holdout_A`.
- **Why it is wrong.** Counting deleted and other-branch lines is right for a *blocking* check (f). For
  an *unlocking* check it is backwards: a reverted, erroneous or test row unlocks for ever. 009's own
  guard recomputes its decision from the stored numbers (`ct/factors.py:691-720`); 010 only reads the
  label.
- **Fix.** Unlock only on a 009 row that meets all of these:
  - it is present in `HEAD:results.jsonl`;
  - its `registration_commit` is an ancestor of HEAD;
  - for `close` and `close_conditions_unmet`: `fx.recheck_discovery(row['results'])` reproduces the
    stored decision, with no failures. This reads no data, so it is whitelist-safe; hash `ct/factors.py`
    if you import it;
  - for `holdout_A_declined`: its `discovery_row_sha256` equals the single 009 discovery row's.
  Ignore `-` lines and rows on other branches. Invert the test.

**N4. Missing positions leave the primary, and they are the divergence tail.**
- **Where.** `ct/carry.py:362-365`; `r010_run.py:298-301` (rows with `None` are dropped before
  `summarise`); §4 and §6 ("reported").
- **Why it matters.**
  - Entry already requires the name check, so a failed check *at exit* is not a mapping error. It means
    the two venues' prices diverged by a median of more than 5 % over 7 days, or one leg's prices
    vanished. These are the positions with the largest gap P&L.
  - The short leg is the high-funding venue, which usually trades rich. A one-venue squeeze or a
    deposit halt is plausibly a loss, so dropping these positions favours the hypothesis.
  - The first review asked for "missing-at-worst *in a reported block*". That was too weak.
- **Fix.**
  - In the primary, use the raw exit gap (no name check) whenever both prices exist within 24 h.
  - Only when a price is absent, use the worst observed return.
  - Keep "drop missing" as the reported variant. Print the missing count before the decision.

**N5. The decision table does not state the programme's joint power. The confirm rule stacks two point
bars.**
- **Where.** D12, D13, D14, §3 holdout table.
- **What is stated.** §3 and D14 say honestly that the holdout alone cannot confirm near the bar.
  T1 P(confirm) at 3 %/yr is 0.13 (A) / 0.21 (B) under σ_m = 6 %.
- **What is not stated.** Taking discovery × holdout as independent, T1 under σ_m = 6 %:

  | true edge | A | B |
  |---|---|---|
  | 3 %/yr | ≈ 0.05 | ≈ 0.08 |
  | 4 %/yr | ≈ 0.12 | ≈ 0.19 |
  | 5 %/yr | ≈ 0.20 | ≈ 0.34 |
  | 10 %/yr | ≈ 0.52 | ≈ 0.87 |

  - Without shocks, a true 4 %/yr gives ≈ 0.26 (A) / 0.56 (B).
  - So "Otherwise → L2 closes" closes an edge 1.7× the bar with probability ≥ 0.66.
  - Requiring annualised ≥ 3 % at both stages caps P at ≈ 0.25 at the bar by construction.
- **Fix.**
  - State the joint table in D13 and D14, and relabel a holdout non-confirm "not demonstrated at this
    power", not evidence of no edge.
  - Optionally make the holdout an existence test (both t-tests, annualised > 0), with the 3 % bar
    judged on the holdout point estimate as *reported*. Or pre-register an extension rule, e.g. B → a
    fixed 2027-H1 span when the point estimate is ≥ 3 % but the t-tests fail.
  - Either way, the owner signs the trade-off in D14.

### MINOR

- **N6. Fill timing at settlement.**
  - What happens:
    - Entry and exit fill at the close of the bar *ending* at t, which is pre-settlement.
    - Funding accrues over (t_in, t_x], so the position is treated as opened just *after* the
      settlement at t_in.
  - Why it is biased:
    - The short leg (the high-funding venue) sells at its pre-settlement premium without paying for it.
    - Price drops after settlement scale with the rate.
    - The bias is ≲ one 8h interval of the entry spread: about 1.5 bp on N at the median |s|, about
      4 bp at p90, so ≈ 0.15–0.4 %/yr on gross. It favours the hypothesis, and selection makes it
      one-sided.
  - Fix: either report a "one entry interval of s haircut" block, or accrue [t_in, t_x).
- **N7. B1 residue ("opened" counts).**
  - Probe (synthetic AR(1) spreads, the `plan` rule, H = 51 grid points): opens per qualifying
    coin-time falls monotonically with persistence. At a 5 % exceedance share it goes from 0.28 (iid)
    to 0.12 (5-day half-life) to 0.04 (50-day).
  - So `opened` together with the qualifying count is a lag-H persistence statistic. Revision 1 prints
    no qualifying count. One can be roughly interpolated from §3's level quantiles against θ ≈ 12 %/yr,
    which gives the trace to within about a factor of 2.
  - That is far coarser than, and subsumed by, `000f`'s direct 51 % / 36 %. The by-year split adds
    nothing beyond `000f`'s per-span figures.
  - **Judgement: small, already ledgered in substance; do not remove** the totals and the by-pair and
    by-year counts (power and criterion 3 need them).
  - Fix:
    1. Add one sentence to `000f`.
    2. Never print a share of grid times at |s| ≥ θ, immediate re-entry counts or per-coin open counts.
    3. Make `events()` and `r010_describe.py` refuse any `panel_holdout_*` directory.
- **N8. Composition.**
  - T1 is 1,847 / 2,135 = 87 % BN-BY. HL pairs number 288, and OKX X-Perps (the lawful candidate) is
    absent from the primary.
  - The hypothesis text is generic, and the share is stated (§3, §7, §10.2). But D13's "CONFIRM → paper
    on a lawful venue pair" transfers a CEX–CEX result to a pair T1 barely contains.
  - The per-leg floor was also introduced after `000f`. It is justified by M3, not by outcomes, but it
    is a post-exposure design change.
  - Fix:
    - State in §1 that T1 is in effect a Binance–Bybit test.
    - In D13, require the paper registration to treat its venue pair as untested.
    - Report the draft's Binance-only-floor universe as a block.
- **N9. Hash coverage.** The holdout panel's provenance code and lists are not frozen.
  - Not hashed in §8:
    - `scouts/l2_fetch.py` (`_zip_rows`, `hl_name_from_bn`);
    - `ct/net.py`;
    - `scouts/h2_binance_scout.py`;
    - `ct/__init__.py`;
    - `data/carry/binance/months.json`;
    - `data/carry/bybit/symbols.json`.
  - `data/carry/inputs/*.json` appear only as truncated hashes, which the guard cannot read.
  - The discovery outcome depends only on the hashed panel, so this matters for the holdout.
  - Fix: add the full hashes to the table.
- **m-a.** The run writes its row only at the end. Add `discovery_started` and `holdout_started` rows,
  as 009 does.
- **m-b.** The holdout universe is frozen to Binance symbols with a funding file ≤ 2026-06 and to the
  Bybit and HL maps from before 2026-07. Coins listed later are excluded. That is not biased, but it
  should be stated in §2.
- **m-c.** Criterion 1b and the holdout tests are not Bonferroni-adjusted over T1 and T2 (only the week
  z is). Under month shocks, where 1b binds, P(either passes | 0) ≈ 0.04. State it, or use t₁₈(0.9875).
- **m-d.**
  - A leg's funding-data hole during a hold is silently zero accrual. Report expected vs observed
    settlement counts per position.
  - Thin 2023 HL closes can be hours stale, so §3's "seconds of asynchrony" is optimistic for HL alts.
- **m-e.** The holdout power assumes Gaussian noise at the raw SD of 115 %/yr (robust 7.5), so it is a
  pessimistic bound, not an estimate. Say so in D14, since A vs B is being chosen on it.

---

## Verdict

**Not registrable yet.** Revision 1 fixes every first-review item in code and text (B1, B2, B3, M1–M3,
all MINORs; M4 in part), the tests pass, and the hashes match. Two problems remain.

- **The fresh-holdout path is the weak point.**
  - N1: the owner's A/B choice is mis-parsed. A is already satisfied today, and B would still run A.
  - N2: the fetch gate is label-only, with no approval and no completeness check.
  - N3: the 000g unlock trusts any label, including uncommitted, deleted or off-branch rows.
- **N4:** silently dropping exit-divergence positions from the primary biases T1.

After N1–N4 are fixed and N5's joint power is written into D13 and D14 for the owner's signature, the
design is registrable. The B1 residue (N7) needs only a ledger sentence and a refusal on the holdout
panel.
