# 009 review (Revision 2 draft): weekly cross-sectional factors on Binance USDⓈ-M

Independent, adversarial review. Written 2026-10-11. Read only: no forward
return, IC or book P&L was computed on real data. `r009_run.py` was not run.

**Verdict: NOT READY TO REGISTER.** There is one blocker, which is a
disclosure and sequencing fix and cheap. There are five majors. The
mechanics are sound: point in time, funding sign, tie-aware Spearman, the
guard and the split all check out. The problems are what the draft omits:
- other on-disk data that already covers holdout A;
- costs that the F2 gate leaves out;
- the book's beta;
- two statistics that carry forward information, used in decisions.

## How this was checked

- **Tests.** `.venv/bin/python -m tests.test_factors` ran clean: all PASS,
  exit 0.
- **Hashes.** SHA-256 of the four code files, `MANIFEST.tsv`, the three
  parquets and `life.json` match the §8 table exactly.
- **Data on disk.** No file in `MANIFEST.tsv` or `raw/` is dated after
  2024-01-01. The panel's maxima are 2024-01-01 23:00 for bars, 2024-01-01
  00:00 for funding and 2023-12-31 23:00 for metrics.
- **Behaviour-only probes** (scratch scripts, not in the repo). Every price
  each probe used is stamped before the S it is attached to, or is a
  trade-count existence check:
  - life segments against kline activity;
  - zero-OI and redenomination scans;
  - the ex-ante net beta of the buffered target book;
  - a proxy for the cost of re-sizing drifted holdings;
  - F1 boundary ties and their float representation;
  - the trailing funding of the F1 legs;
  - the tier mix of each leg.
- **Synthetic check.** `sim_once`'s IC equals `stats.spearman` on a week
  with 40% ties (−0.154352 both). The Revision 1 tie fix is correct.
- **Citations.** Schmeling, Schrimpf & Todorov's BIS abstract was
  re-verified. The Chi et al. full text was 403 again; the abstract does not
  give a direction. The draft's "unverified" stands.

---

## BLOCKER

### B1. Holdout A's payoff data is already on disk and is being worked on under 010, and §0 does not disclose it

**Location:** §0, "Selection shared with earlier looks" (l. 123–143). §6
"Pulled only after REGISTERED **and** a discovery pass" (l. 574). §2 "Nothing
dated on or after 2024-01-08 was requested".

**What is already on disk.** `data/carry/` holds data from registration
010, which is a draft under review:
- Binance USDⓈ-M funding from 2023-05 to 2026-06
  (`data/carry/panel/funding.parquet` and `data/carry/binance/fundingRate`);
- Binance 8h klines up to 2024-12-31 (`prices.parquet`);
- Binance **daily closes to 2026-06-30** (`levels.parquet`, 010 §2).

Together these hold both components of F1's payoff (the funding cash flow
and the price) for **all of holdout A and holdout B**. None of it is under
009's guard or manifest.

**What 010 has already read, and what it will compute.**
- 010's discovery runs from 2023-06-08 to 2024-12-31. That covers 12 of
  holdout A's 19 months.
- `data/carry/logs/describe.json` already holds spread levels and position
  counts that include Binance legs in 2024, for example
  `discovery/positions/short_venue/BN 1221`.
- Ledger `000f` records that in-hold spread persistence was read for
  2023-06 to 2026-06.
- 010's run will compute realised funding-carry P&L on Binance legs through
  2024. Each such leg is short the high-funding venue for a coin, so the run
  measures how Binance funding persists coin by coin. That persistence is
  the main cash-flow component of F1. At signal time, the trailing funding
  gap between F1's legs is **56.5 bp/week** (long leg −35.4, short leg
  +21.1). That is several times the 12.7 bp cost.

**Why it blocks.** The draft says holdout A "decides; read once". That
statement is not true at the data level, and it will stop being true at the
read level once 010 runs. The same kind of exposure made 010's holdout
"spent" (000f).

**Fix, before registration:**
1. **Disclose it in §0** and record it in a new ledger entry (000g): the
   data, its paths, and what 010 has printed about Binance funding in 2024–26.
2. **Set the sequencing in the D-table.** Either:
   - 010's discovery run (or anything computing per-coin Binance funding
     persistence or returns over 2024-01-01 → 2026-06-30) waits until 009's
     holdout A has been read or closed; or
   - the owner accepts that F1's holdout A is partially exposed, and F1's
     decisive confirmation moves to sealed data from 2026-07 onward.
3. **Add a hygiene rule.** No 009 code path may read `data/carry/`. The
   holdout-A pull must stay its own fetch with its own manifest, as §6 says.
   State that the copy in 010 exists and is not used.

---

## MAJOR

### M1. The F2 gate passes only because its cost leaves out drift re-sizing, which the registered book pays

**Location:** §3, the persistence table (l. 431–439): "target weights only,
legs ±1, no drift". §3, the gates: "buffered cost ≤ 15 bp … 14.93 bp".
Code: `factors.turnover_no_drift` (factors.py l. 549–562) against
`run_book` (l. 405–416), which charges `|target − drifted holding|`.

**The problem.**
- The registered book rebalances each leg back to ±$1 every Monday. Every
  held name therefore trades about |w·R| even when its target does not
  change.
- I estimated that extra cost behaviourally as Σ|w|·|trailing-week price
  return|·rate. This uses the same contemporaneous dispersion the power
  simulation uses, and no return after S.
- Result: **about 1.8 bp/week for F1 and 1.9 bp/week for F2** (medians 1.4
  and 1.5).
- Measured the way the registered book actually trades, F2 costs about
  16.8 bp. It fails the "≤ 15 bp" gate it was admitted on. The 28-day value
  version fails too: 14.6 + ~1.9.
- The power table's breakeven (12.8 / 14.9 bp) and its null means
  (−13 / −15 bp) are understated by the same amount, because `sim_inputs`
  uses `tov` without drift (factors.py l. 592).

**Fix.** Either:
- state that the gate was defined on target-only cost, and accept that the
  definition omits roughly 13% of the real cost; or
- re-apply the pre-set gate to the drift-inclusive estimate. F2 is then
  dropped under Revision 1's rule: family of 1, z 1.645.

Either way, redo the breakeven and power with the drift cost included. Do
not tune the buffer.

### M2. The book is dollar-neutral, not beta-neutral, and F1 carries a net short beta that §7 does not size

**Location:** §2 trade, step 1: weights ∝ 1/σ, each leg ±1
(`buffered_targets`, factors.py l. 355–360). §7: "A factor that loads on
beta … The legs are reported separately".

**What I measured.** Ex-ante net beta of the buffered target book, from the
30 daily returns before S:

| factor | against the equal-weight universe | by year | against BTC |
|---|---|---|---|
| F1 | mean −0.10 (p10 −0.21, p90 +0.01) | 2021 −0.12, 2022 −0.05, 2023 −0.15 | −0.105 |
| F2 | mean −0.03 (p10 −0.17, p90 +0.09) | — | — |

The F2 short leg's weighted σ is 1.12× the long leg's.

**Why it matters.**
- Discovery is mostly an altcoin bear market. Over it, −0.10 × the alt
  market's drift can add on the order of 10–20 bp/week to F1's gross. That
  is 15–30% of the ~60–80 bp the book criterion needs.
- That contribution is market direction, not a premium.
- The power simulation assigns returns by rank only (`sim_once`, l. 628–634).
  It has no common factor, so its "P(pass) ≤ 0.013 under the null" does not
  cover a beta-loaded null.

**Fix.**
- Report the ex-ante net beta per week, and a beta-adjusted net: net minus
  the ex-ante β times the equal-weight universe return over the hold.
- Make the holdout-A pull (D13) also require that the beta-adjusted net mean
  be > 0.
- Alternatively, scale each leg to equal ex-ante beta. That is a D7 change,
  allowed before registration with a new hash table.

### M3. Holdout-A mode is not built or hashed, so it can change after the discovery result is seen

**Location:** §6 and D13–D14. §8 lists only discovery code. `r009_run.py`
has no holdout path; only `factors.holdout_confirm` exists.

**What is missing.**
- the holdout pull, with date assertions, the 28-day metrics pre-period from
  2023-12-10, and the lone 2025-07-28 kline and settlement;
- the life table over 2024–25;
- a guard requiring a discovery-pass row for that commit and no holdout row
  yet;
- `holdout_confirm` wiring and the output row.

**Fix.** Build `r009_run.py --holdout-A` now, test it on synthetic data, and
put it in the §8 hash table. 010's review required the same (M4).

### M4. Two statistics used in decisions carry information about forward returns; disclose and ledger them

**Statistic 1: Spearman of F2 with minus the past 28-day return.**
- Location: §3 l. 421–427, Revision 2 item 1, and `r009_describe.py`
  l. 173–176.
- The value 0.20 (contracts) against 0.53 (value) drove the switch from
  value to contracts.
- The past 28-day return at S is the sum of the realised outcome windows of
  signals S−28d … S−7d. F2 is persistent (rank autocorrelation 0.52), so
  this statistic mixes the mechanical price-in-value effect with
  (persistence × the forward IC of earlier F2 scores).
- The mechanical part dominates by an order of magnitude, so the decision
  stands. But this is the same class of exposure as 010's B1 and 000f, and
  §0 says "no outcome existed for any version".

**Statistic 2: leg counts near delisting cuts.**
- Location: §3 l. 396–398 and §7 l. 637–643: "within 4 weeks of a cut sit
  23 long / 17 short".
- This crosses the book's legs with a future, return-relevant event.
  Delisted coins crash. The draft used it to argue the bias direction.

**Fix.**
- Ledger both. Add them to §0's disclosure.
- Add that neither may motivate any further change.
- Change "no outcome existed" to "no outcome was computed; two
  forward-exposed behaviour statistics were read".

### M5. Costs depend on the bottom tier, are set least favourably when they matter most, and holdout A cannot catch a cost error

**Location:** §2 cost tiers (l. 334–358), §7 "A fail at the registered costs
is a fail", and D9.

**How the money sits by tier.**
- 1/σ weighting moves dollars toward T4, because T4 has the lowest median
  σ (4.4%/day).
- F1's short leg puts **69% of its dollars in T4** and 25% in T3. The F2
  legs put 55–57% in T4.

**How weak the T4 cost evidence is.**
- For T4, the registered 6 bp sits below the per-coin Abdi–Ranaldo p75 of
  the matching ADV deciles: about 8.6–9.9 bp (`describe.json`, deciles 2–4).
- That estimator is 0 at the median and ≤ 0 pooled in 8 of 10 deciles, so it
  bounds nothing. Calling its p90 an "upper bound" (l. 351) is wrong: the p90
  of a noisy estimator is not a bound.

**Why the tiers get it backwards.**
- Tiers use trailing ADV, which rises in stress weeks such as LUNA and FTX.
- So coins move to *cheaper* tiers exactly when spreads widen.

**Why holdout A cannot help.** It uses the same cost model, so a cost error
passes straight through to a CONFIRM.

**Fix.**
- Make the 2× slippage block's net mean > 0 a condition of the holdout-A
  pull in D13. It stays non-gating for "pass" as such.
- Correct the "upper bound" wording.
- Optionally use slippage = max(tier, k·σ_daily), set now, from behaviour.

---

## MINOR

### Mechanics and code

1. **`r009_run.py` l. 150–151 misaligns F2's weekly series.**
   - `zip(ic_rows, book)` pairs 107 IC rows with F2's 104 book rows, so F2's
     reported weekly IC is shifted by 3 weeks and truncated.
   - Reported only, but it will mislead.
   - Fix: join on S.
2. **The outcome guard is looser than the data needs.**
   - `outcomes(…, hard_end=HARD_END_DISCOVERY)` asserts every exit ≤
     2024-01-08 (factors.py l. 51, 285–286). The true maximum exit is
     2024-01-01 01:00.
   - The panel holds 23 h of holdout-A-calendar bars (5,736 rows on
     2024-01-01) that no query needs.
   - Fix: assert exit ≤ last S + 7 d + 1 h, and drop 2024-01-01 bars after
     01:00 from the panel, or filter them on read.
3. **Guard (f) reads only the working-tree `results.jsonl`** (r009_run.py
   l. 82–91, same as `l7_run`). A run whose row is then reverted can be
   re-run.
   - Fix: also check `git log -p -- results.jsonl` / `git show
     HEAD:results.jsonl` for a 009 row.
   - Also record the `duckdb` and Python versions in the output row.
   - `ct/__init__.py` (empty) is unhashed; hash it, or assert it is empty.
4. **The module docstring is stale.** factors.py l. 21–24 still defines F2
   as 7-day OI value growth. Fix it, which changes the hash (a new §8 table).
5. **Float ties.**
   - The default-rate tie is exact float equality: 1,462 coin-weeks at
     `-0.0021000000000000003`, and 5 near-ties at `-0.002099999999999999`
     treated as distinct.
   - Fix: round `fund7` (e.g. to 1e-10) before ranking.

### Factual corrections to the draft

6. **The tie count is wrong** (§3 l. 408). It is not "20 of 107 weeks the
   F1 short leg's boundary is tied". It is 16 weeks with the short boundary
   tied and 4 with the *long* boundary tied.
   - In one week (2023-11) the long leg holds 94 of 188 coins, because the
     default-rate coins are the top quintile's boundary.
   - §7's "negative-funding coins vs the rest" is therefore sometimes
     "default-rate coins vs above-default".
7. **ICP is not a relisting; its metrics archive has a gap** (§3 l. 399).
   - ICPUSDT traded throughout (for example, 720/720 active bars and about
     $2.95 B volume in 2022-04). Its metrics files are missing from
     2021-12-24 to 2022-09-26.
   - The 4 KB life rule (binance_years.py l. 145–158) therefore excludes a
     T3 coin for about 44 weeks, including the restarted 30-day seasoning.
   - It is the only such case at the $10 M floor; TLM's gap really was dead,
     with cnt = 0.
   - Fix: correct the label. Optionally treat a segment hole as a relisting
     only when klines `cnt` is also 0.
8. **Survivorship is otherwise clean.** Only BTCSTUSDT (tiny files) is in
   the 2021-12 → 2023-12 kline listing without a life entry. Relistings
   (BNX, 1000BTTC/BTT) start fresh segments. No in-segment contract-size
   jump over 4× exists outside listing days. Zero-OI archive rows never fall
   inside a universe coin's F2 window. Dead-contract klines carry cnt = 0, so
   the `cnt > 0` entry and exit filters hold.

### Citations and argument

9. **The meaning of "carry" flips between the two analogues** (§0 l. 106–113).
   - The F1 sign is consistent with both sources. Koijen et al.'s "long
     high carry" means, for a perp long, carry = −funding, so long low
     funding. SST's "high crypto carry" is the basis earned by the
     *short-futures* side, so high funding; it "predicts crashes", which
     again supports shorting high funding.
   - But the word flips meaning between (a) and (c). Say so.
   - (b)'s "the short collects that and bears the crash risk" is backwards:
     a crash *pays* the short. The short bears squeeze risk. Under (c), F1's
     short leg gains on crashes, which reads as an anomaly, not compensated
     risk. State which story is claimed.

### Power and gates

10. **Power.**
    - σ_IC 0.15 is assumed; the 0.20 row roughly halves end-to-end power.
      State that power claims are conditional on it, and report the
      realised σ_IC.
    - For F1, the "planted IC" is on total return (R + F). F1's expected
      gross is largely mechanical funding (the 56.5 bp/week trailing gap).
      Report gross price and gross funding separately in the weekly series
      and the reading.
    - The holdout-A figures are called a lower bound (l. 522–523). They are
      not necessarily one: holdout A's extra breadth is mostly new
      2024–25 coins near the floor, in tier T4.
11. **The variant record is incomplete.**
    - The 7-day F2 in contracts was also computed: quantiles and a 0.93
      Spearman, in `r009_describe.py` l. 161, 169. List it with the others.
    - Under the null, the behaviour-only gates (turnover, autocorrelation)
      are functions of features alone, so trying three or four F2 variants
      does not inflate the outcome test. The exception is M4's reversal
      statistic.
    - No sign of iteration toward the gate: BAND 0.4, the tiers and the
      thresholds all pre-date the 28-day measurements (programme log,
      2026-10-11 entries).
12. **Delisting bias.** The "biases … against a pass" argument (§7) rests on
    23 vs 17 coin-weeks and ignores funding (often deeply negative before
    delisting, which pays the long). Add a reported block that excludes
    coin-weeks within 14 days before a cut, labelled as using future
    knowledge.

---

## Checked and found sound

**Point in time.**
- Bars are used only when complete before S (ts ≤ S − 1 h).
- Metrics rows are stamped 00:00–23:55 per daily file, and the F2 windows
  end at the S − 5 min row. The 1 h entry lag absorbs any stamping
  convention under an hour.
- Funding calc_time lands 0–10 ms after the hour and never before, so the
  settlement at S is excluded from F1 and is paid by the outgoing book.
- ADV and σ are strictly pre-S.
- Entry is at the S + 1 h open. Exit is at the S + 7 d + 1 h open, the same
  price as the next entry.

**Funding sign.** For a long, F = −Σ rate × price ratio. The short receives
w × −F > 0 when funding is positive.

**Delisting.**
- The cut exits at the last close of the last full metrics day. The real
  delivery is hours later (02:00–09:00 next day).
- No post-cut price or funding reaches a return.
- The 4 KB rule is known information by then (announced delisting).

**The split.**
- Discovery 107, holdout A 81, holdout B 47 signal weeks.
- The embargo reading is correct.
- Holdout A's last exit (2025-07-28 01:00) and settlement fall in holdout
  B's features-only week. That is acceptable.

**Criteria.** The 1–5 stack is coherent: criteria 3–5 cost about 2 points of
power at IC 0.03. The Bonferroni z is 1.960.

**Guard.** It checks status; (a)–(e) as specified; (f) as noted in Minor 3.
The runner calls `guard()` before any outcome. `r009_describe.py` never
calls `outcomes`.

## Sources

- [BIS WP 1087, Crypto carry](https://www.bis.org/publ/work1087.htm)
- [Chi et al. 2023, RePEc](https://ideas.repec.org/a/wly/jfutmk/v43y2023i8p1161-1180.html)
