# Independent review A of 015 DRAFT (month-end liquidity cycle, settlement-shifted), 2026-10-13

Reviewer brief: an adversarial review of `registrations/015-month-end-liquidity-cycle.md` (DRAFT, `012eff7`),
`r015_core.py`, `r015_power.py`, `r015_hmm.py`, `r015_run.py` and `tests/test_r015.py`. The emphasis is on design,
statistics, economics and the tail. Every existing file was read-only. `r015_run.py` was never run in any mode, and
no guard function was called against the real tree. No statistic of the French or Treasury files was computed.

**What was run.**
- `python3 -I research/alpha/tests/test_r015.py` printed **"137 passed, 0 failed"**. The SHA-256 of the four
  `r015_*.py` files, `calendar.json` and `logs/power.json` match the §10 table.
- Five synthetic probes in `research/alpha/scratch_review015a/` (a new directory; nothing else was written). The
  generator and the estimator are my own code (`common.py`), not `r015_core`. The only real inputs are the
  trading dates in `calendar.json` and the FOMC announcement **dates** in the B1 scout's derived file.

  | probe | what it checks | log |
  |---|---|---|
  | `p1_calendar.py` | independent rebuild of the 149 windows from dates; the contrast algebra; FOMC, quarter-end and Russell dates against P and R | `p1_log.txt` |
  | `p2_crash_offsets.py` | **every** placement of the 2020, 2025 and 2008 shapes on the real calendar; false-pass rate conditional on the placement; pairs | `p2_out.json` |
  | `p3_worlds.py` | 22 worlds × 10,000 draws: size and power of the registered rule, of a vol-scaled variant and of a trimmed variant; decayed, TOM-only and unshifted alternatives | `p3_log.txt`, `p3_out.json` |
  | `p4_econ.py` | closed-form power; the bar's implied hurdle; era tilt; venue returns incl. collateral yield; Kelly with a tail; forward-step power; tail over every placement | `p4_log.txt` |
  | `p5_decay_gate.py` | whether a post-2017 condition stops decayed effects from passing, and what it costs | `p5_log.txt` |

**Reviewer exposure (for the ledger).** Documents: the draft and its code, the brief's "Lessons" section, the
programme doc §1–§3, the B1 scout, `000s`, 013-review-a. The paper: Etula et al. (2020), the published CC-BY PDF
from the Aalto repository, fetched into the session scratchpad and text-extracted (pages 75–93 read). A web search
for one sentence of the paper. **Public knowledge, not computed:** I know the approximate dates and sizes of the
largest in-sample days (2020-02/03, 2025-04). I did not place them against any window, by memory or by code.
**Real-data reads:** `calendar.json` dates; `fomc_announcement_days.csv` dates. No value.

---

## Verdict: register after listed fixes (no blocker; four majors)

The statistic tests what the hypothesis says. The calendar, the lag rule, the abnormal construction and the
published magnitudes match the paper. The size claims and the power reproduce independently. The economics
arithmetic is right at the MES level. Nothing found invalidates the existence test as drafted. The four majors are
about **what a pass will be taken to mean**, **power left on the table**, **D8's rationale**, and **samples the
reported blocks would spend irreversibly**. Each is cheap, and M1, M3 and M4 must be settled before the freeze
because the run makes them irreversible.

| item | one line |
|---|---|
| M1 | A pass does not mean "alive now": decayed effects pass 0.13–0.29, and §8's live criterion is a coin flip under them |
| M2 | Vol-scaling y (WLS) raises half-effect power from 0.44 to 0.65 under GARCH t4, costs nothing under iid, and cuts crash sensitivity |
| M3 | D8: keep 1.91 and reject 2.13. The draft's "aligned" world is not the worst case, and the honest null (uniform placement) gives 0.028 |
| M4 | The Europe and HMM-post-2023 reported blocks would spend fresh samples. Hold Europe back as the pre-stated confirmation and truncate HMM |

---

## BLOCKERS

None from design, statistics, economics or the tail. The guard, integrity and sequencing are reviewer B's scope.
In passing: the untracked `r016_*.py` files at `research/alpha/` top level would trip 015's shadow scan, as the
programme doc already notes.

---

## MAJORS

### M1. A pass establishes "existed after 2013", not "exists now", and §8's live criterion cannot tell the difference

**Evidence** (`p3_log.txt`, `p5_log.txt`; registered rule at 1.91, 10,000 draws):

| world (full effect where present) | P(pass) | with post-2017 t ≥ 1.0 | with post-2017 t ≥ 1.28 |
|---|---|---|---|
| alive, shifted, iid (85 bp) | 0.772 | 0.757 | 0.736 |
| alive, half (42.5 bp), iid | 0.290 | 0.277 | 0.259 |
| died at publication (full to 2019-12, zero after) | 0.267 | 0.221 | 0.182 |
| linear decay from full (2014-01) to zero (2026-05) | 0.289 | 0.256 | 0.224 |
| T+3 era only (draft: 0.137) | 0.133 | 0.095 | 0.071 |
| TOM only (T … T+3 at Etula's +23 bp abnormal) | 0.123 | 0.117 | 0.108 |

- **The decay worlds are the likeliest false-deploy worlds,** not edge cases. The working paper circulated from
  2015, and publication was 2020. Take a prior of 1/6 alive at full effect, 1/6 at half, 1/3 died at publication
  and 1/3 dead since 2014. Then P(pass) ≈ 0.28 and **P(alive now | pass) ≈ 0.64**: about a third of passes would
  come from an effect that is gone in the T+1 regime we would trade.
- **A sign condition does nothing.** Requiring a post-2017 mean > 0 changes 0.133 → 0.132. Conditional on an overall
  pass, the 105 post-2017 months, which carry 70% of the weight, are almost always positive.
- **§8 makes the weakness binding.** It says "fixed now", and a live step needs forward ȳ > 0 **and** paper net > 0
  over 19 months. These are nearly the same event (same months, same sign), so under a dead effect the live gate is
  **P ≈ 0.50**. Chain it: P(live candidacy | died at publication) ≈ 0.27 × P(bar | pass) × 0.5 ≈ 0.09.
- **The draft's "written expectation"** (P(pass) ≈ 0.25, read as a measure of the effect surviving) does not count
  passes from decayed worlds.

**Fix.**
1. §0 and §4 state what a pass means: "the cycle existed in 2014-01 → 2026-05 as defined". Deployability is
   decided by the forward step only.
2. §8's live criterion becomes a powered one: a forward span of at least 31 months (2026-06 → 2028-12, Q4), and a
   live step needs forward **t ≥ 1** (P = 0.59 / 0.35 / 0.16 at full / half / zero; `p4_log.txt`) rather than two
   copies of the same sign. Paper trading may stay on the sign check.
3. Add two reported blocks (deciding nothing): the last 60 months (2021-06 → 2026-05), and y by era with the
   sub-window decomposition of m8.
4. **Optional:** add "post-2017 t ≥ 1.0" as criterion 5. It costs 0.015 of power at full effect and 0.013 at half,
   and cuts decayed-world passes by 20–30%. It is a cheap partial filter, not a cure; I would not insist on it if
   M4's Europe confirmation is adopted.

**Cost.** None to 015's power (except option 4). The deploy decision moves about a year later.

### M2. Scale y by trailing volatility: the same test, about 0.2 more power at half effect under vol clustering, and less crash-driven

The month noise of y is heteroskedastic in every realistic world (2020, 2022, 2025). An equal-weight t over 149
months is inefficient there, and its size is driven by the few high-vol months, which is exactly the crash problem
of M3. The variant: **z_m = y_m / (√12 · σ̂_m)**, where σ̂_m is the RMS of x over the 20 trading days ending T−9.
These are the days known at the position's open, so there is no look-ahead. A floor of 0.4%/day applies, and the
criteria are otherwise unchanged. It is weighted least squares, and it is deployable as a vol-targeted size.

**Evidence** (`p3_log.txt`, 10,000 draws; q95 = 95th percentile of the gated t):

| world | registered: q95 / pass at 1.91 | vol-scaled: q95 / pass at 1.85 |
|---|---|---|
| null iid / GARCH t4 / GJR t4 / AR −0.08 | 1.70 / 1.72 / 1.64 / 1.65 — 0.027–0.033 | 1.70 / 1.72 / 1.66 / 1.67 — 0.033–0.037 |
| null + 2020 aligned (the binding world) | 1.926 / **0.052** | 1.816 / **0.047** |
| null + 2020 & 2025 both aligned (stress) | 2.156 / 0.090 | 2.055 / 0.081 |
| full, iid | 0.772 | 0.782 |
| half, iid | 0.290 | 0.310 |
| full, GARCH t4 | 0.878 | **0.988** |
| half, GARCH t4 | 0.444 | **0.650** |
| full / half, GARCH t4, effect ∝ vol (Etula: vol amplifies the cycle) | 0.668 / 0.245 | 0.763 / 0.292 |

- Vol-scaling never loses power in the worlds tried.
- It gains the most where the draft's own regime worlds say the data live.
- Its binding calibration value is about 0.1 lower.

**Fix.** Make z the primary. Recalibrate on the same 13 worlds with the draft's rule; my estimate is CAL_CRIT ≈ 1.82,
and I would register 1.85 for Monte Carlo margin. Keep v (raw, or vol-targeted) for the bar and the economics.

**Cost.**
- One new pre-stated parameter pair (20 days, 0.4% floor), fixed by convention and not by data.
- A re-run of `r015_power.py` and new tests.
- In size: none. In power: +0.01 to +0.21.

If the drafter prefers to keep the raw y, the vol-scaled t should at least be a pre-stated reported block.

### M3. D8: keep 1.91 and do not calibrate on the double alignment. But fix the rationale: the "aligned" world is neither the worst case nor the right null

**Evidence** (`p2_out.json`): every placement of each documented shape on the real calendar (about 3,100 each).
Rates are false-pass rates conditional on the placement, at t_148 / 1.91 / 2.13, against iid month noise.

| shape | uniform placement (honest null) | the draft's "aligned" | worst placement | placements with rate > 0.05 at 1.91 |
|---|---|---|---|---|
| none | 0.052 / 0.030 / 0.018 | — | — | — |
| 2020 | 0.048 / **0.028** / 0.017 | 0.080 / **0.049** / 0.030 | 0.097 / **0.061** / 0.037 | 11.5% |
| 2025 | 0.051 / 0.030 / 0.018 | 0.083 / 0.051 / 0.031 | 0.086 / 0.052 / 0.031 | 6.2% |
| 2008 | 0.048 / 0.028 / 0.016 | 0.054 / 0.032 / 0.019 | 0.098 / 0.060 / 0.033 | 9.5% |
| 2020 + 2025 | 0.049 / **0.028** / 0.017 | 0.125 / **0.078** / 0.050 | 0.158 / **0.099** / **0.061** | both bad: **0.7%** |

- **Under the null, crash timing is independent of month-ends,** so the correct null integrates over placement.
  There, 1.91 has size 0.028 with one crash or with both: already conservative by about 0.25 in critical value.
- **The "aligned" heuristic is not adversarial.** The worst 2020 placement (0.061) beats it. Even **2.13 does not
  hold the worst pair (0.061)**, so 2.13 buys no guarantee.
- **The double-alignment event is about 0.7%,** not the "≈ 2%" of D8.
- **2.13 costs 0.07 of power** at full effect and 0.07 at half (closed form 0.778 → 0.707 and 0.283 → 0.214).
- **The registered value has Monte Carlo error.** My independent replica of the binding world gives q95 1.926 and
  0.052 at 1.91, against the draft's 1.904 and 0.049. The SE of a q95 from 10,000 draws is about 0.02.

**Recommendation.** **1.91, not 2.13.**
- Restate §5 and D8: "1.91 holds a single worst-heuristic crash near 5% and is conservative under the uniform-placement
  null (0.028). The double alignment (0.7%) is reported."
- Add the Monte Carlo SE.
- The protection that actually works against crash placement, including against designer or reviewer knowledge of
  crash dates, is M2, not a higher bar.
- Under M2, the same logic gives about 1.85.

**Cost.** None (it keeps the draft's number).

### M4. Two "reported" blocks spend fresh samples irreversibly. Hold Europe back as the confirmation; truncate HMM

**Europe (§7.7).**
- 2014-01 → 2026-05 in Europe is the only **independent replication sample available now**. It has another market's
  institutions and payment cycle, its own settlement date, and a daily vol in the ledgered 0.85–1.17% range.
- At full effect and about 1.0% daily vol, a Europe test has a noncentral t of about 3.0, so power is about 0.9 at
  t_148(0.95). That is far stronger than the 19-month US sign check (§8: 0.83 / 0.68 / **0.50**).
- Read as a "reported, decides nothing" block, it would be seen and spent. It would then inform the deploy decision
  informally: the worst of both.

**Fix:** remove the block, and pre-state in §8 that a US pass is followed by a **Europe confirmation registration**.
- It uses the same statistic and its own lag rule.
- Its calibration must include the US–Europe correlation of y under the null, because global shocks hit both books.
- It is read before or alongside the forward step.

The month-end calendar of Europe stays unread until then.

**HMM (§7.8).**
- The block reads 2023-03-18 → 2026-06-09, which includes the only post-publication HMM months that exist
  (≈ 2025-03 → 2026-06, plus 2026-01 → 06, which is outside even the authors' "through 2025" extension).
- A later HMM registration could no longer pool them.
- It also makes 015's guard depend on 13 Treasury files and a second replication codebase, which are unrelated to
  015's question.

**Fix:** either drop HMM from 015, giving it its own registration that can claim 2025-03 → openly, or truncate the
block at 2023-03-17 (pure in-sample replication, zero information cost, still useful to validate the proxies).

**Cost.** None to 015. M4's Europe confirmation also answers most of M1.

---

## MINORS

- **m1. Overclaim.** §0 item 5 calls the settlement shift "the prediction a calendar fluke cannot fake", and the
  programme doc's B1 entry (reason iv) repeats it. The registered test cannot check it.
  - An unshifted effect passes 0.653 (draft 0.651).
  - The contrast is algebraically just **d = −2·x_{T−3}** (T+2 era) or **−2·(x_{T−3}+x_{T−2})** (T+1 era)
    (`p1_log.txt`), with a noncentral t of 0.59.
  - Fix the wording in §0 and in the doc's reason (iv). Keep the contrast as reported, since it is free. It cannot be
    primary or secondary: it would need about 10× the sample.
- **m2. `xyz` collateral.** "On `xyz`, subtract ≈ 0.2–0.5 %/yr" omits that USDC collateral earns nothing (B1 §2.2),
  whereas MES margin earns T-bills. In total-return terms, `xyz` trails MES by **≈ 3.9 %/yr** at any effect size:
  13.8 vs 9.9 at full effect, and 8.7 vs 4.8 at half (`p4_log.txt`). Correct §6. MES stays primary.
- **m3. The bar uses the wrong regime's v.**
  - `economic` uses v̄ over all eras, about +0.8 bp from ȳ. The traded regime is T+1, where v = y − 2μ, about
    −6 bp/month at a 7.5 %/yr excess drift.
  - Fix: compute the bar on ½·ȳ − 2·μ̂₁ − cost (μ̂ not shrunk).
  - The draft states the tilt in prose (−0.7 %/yr) but not in the decision.
- **m4. Tail table.** Over every placement of the shapes at 1×, the worst 5 trading days are **−22.0 %** (2020),
  not −19 to −20 %. The worst months are −24.4 % (2020), −24.5 % (2008) and −12.6 % (2025) (`p4_log.txt`).
  Update §6. The 1.2× column becomes −26 % (week) and −29 % (month).
- **m5. Kelly fraction.**
  - The "¼–⅓ Kelly" claim is Gaussian.
  - Add an adverse −24 % month once in 240 months. Kelly is then 3.1× at full effect and 1.6× at half, so 1.0× is
    0.32 and **0.61** of Kelly.
  - At half effect, 1× is above half-Kelly. The bar's own size (½ Kelly of the shrunk estimate) already handles this
    for paper trading. Drop "¼–⅓" or state it as full-effect only.
- **m6. Name collision.** "016-forward" collides with registration 016 (crypto trend). Use 015F or the next free
  number.
- **m7. Long-only t.** Instead of reporting a t known to be anti-conservative (0.08–0.11), report it with the
  closed-form era-μ correction: Var(ȳ_LO) = s²/G + Σ_s (G_s/G)²·|R_s|²·σ²/N_s. One line of code.
- **m8. Reported decomposition and confound checks** (deciding nothing; cheap).
  - Report y's parts per era: P, T−s … T−1 and T … T+3. The last is Etula's T … T+3, about 23 bp abnormal; a
    TOM-only world passes 0.12, and 0.32 at twice that.
  - Report y excluding FOMC announcement days and their eves. **42 announcement days fall in R against 23 in P**
    (`p1_log.txt`). At a +25 bp announcement premium, that is +3 bp on ȳ (0.1 SE): small, but Etula footnote 21
    checked exactly this in-sample.
  - Report y excluding quarter-end months (49).
  - Russell reconstitution lands in R in 9 of 12 years. Report it if cheap.
- **m9. Calibration MC.** State the Monte Carlo SE of each q95 (about 0.02 at 10,000 draws) in the §5 table.
- **m10. The written expectation** (§0) should use a prior that includes decayed worlds (M1): P(pass) ≈ 0.28, of
  which about 0.36 would come from effects no longer alive.

---

## Answers to the drafter's open questions

1. **D8:** 1.91, not 2.13 (M3). With M2: about 1.85 after recalibration.
2. **D14 (restart clause):** mostly reviewer B's scope. From the design side, a narrow restart is harmless if a crash
   strictly precedes the first parsed value, because the gate token and the loader order make that checkable. Any
   crash after a value is parsed must stay terminal.
3. **2024-05:** keep it. It is rule-assigned, it is 1 of 149 months, and dropping it would be a data-free but
   arbitrary exception.
4. **Forward length:** yes, at least 31 months (P(ȳ > 0) 0.89 / 0.73 / 0.50), with the live criterion of M1.

---

## Checked and sound

- **Calendar.**
  - An independent rebuild from the pinned dates gives 149 months and **0 mismatches** with `calendar.json` rows.
  - The lag split is 44 / 80 / 25, with 64 Friday-T months.
  - Boundary months follow the rule: 2017-08 s = 3; 2017-09 s = 2; 2024-05 s = 1, with P from 05-20 to 05-29.
  - The lag rule is Etula's own logic generalised: sell by the close of T−4 under T+3 "to receive cash by the market
    close on day T−1" (their footnote 4), so the last day is T−(s+1). Their T+1-payment sellers trade until T−3 under
    T+3, which is T−s; that is why R starts there.
- **The paper.** Each of these matches the published text:
  - the windows T−8 … T−4 and T−3 … T+3;
  - abnormal = each day's return minus the sample's average daily return;
  - raw −17 / +77 bp, abnormal −37 / +25 / +48 bp (CRSP VW, 1995-07 → 2013-12);
  - the reversal correlation of −0.39 (Table 2);
  - Friday: 2.5× the T−3 … T−1 returns and 3× the correlation;
  - footnote 19 (net sellers from T−8; significant on T−5, T−4 and the morning of T−3);
  - footnotes 16–17;
  - the DiD (one year either side of 2014-10-06; control group includes the USA; magnitude 0.78).

  The extraction drops minus signs. One sentence, the 2003–13 cumulative pressure-period return "31%", is therefore
  ambiguous in my copy, but Table 1's −17 bp settles the full-sample sign.
- **Statistic.**
  - y = v − (2s−4)·μ_s with weights +2 / 0 / −2.
  - Era μ is the neutral choice: it removes era drift without using window days.
  - The μ-error share of Var(ȳ) is about 2%, and it is calibrated in.
  - Mkt-RF rather than total return is correct (RF is absorbed by μ, and futures earn the excess).
  - The L/S primary is justified at the published magnitudes (t ratio 1.35).
  - The long-only anti-conservatism is real; its mechanism is the common era-μ error.
- **Size.** In my own null worlds the registered rule is at 0.027–0.033 (iid, GARCH t4, asymmetric GJR t4, daily
  AR −0.08) and 0.023 with both crashes uniformly placed. Ungated, at t_148, it is 0.049–0.057.
- **Power.**
  - Independent replica: 0.772 / 0.290 (iid, full / half) and 0.878 / 0.444 (GARCH t4).
  - Closed form: 0.778 / 0.283.
  - Decayed T+3-only world: 0.133 against the draft's 0.137.
  - Unshifted world: 0.653 against 0.651.
- **Economics.**
  - MES: 2 round trips a month (short at T−9, a 2-unit flip, close at T+3) at 0.8 bp is plausible.
  - The bar is equivalent to v̄ ≥ 66.6 bp, i.e. t ≥ 2.09: effectively a second significance hurdle, harmless because
    it only routes.
  - +10.0 / +4.9 %/yr excess at 1× is right.
  - There is no double count: the T-bill on margin is excluded from the table, and the T+1 tilt is a real, separate
    subtraction.
- **Tests:** 137/137, and the hashes match.

*Scratch files:* `research/alpha/scratch_review015a/` (no `__init__.py`, no top-level `.py`). Delete or ignore it
before 015's guard runs.
