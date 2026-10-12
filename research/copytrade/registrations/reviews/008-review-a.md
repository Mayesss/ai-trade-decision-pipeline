# Independent review A of 008 DRAFT (inverted positioning, HL holdout), 2026-10-12

Reviewer brief: adversarial review of `registrations/008-inverted-positioning-holdout.md` (DRAFT),
`008-manifest.json`, `ct/crowd008.py`, `r008_run.py`, `r008_fetch.py`, `r008_power.py` and
`tests/test_r008.py`. The emphasis is statistical design and fidelity to 001 T3. Read-only on every
existing file. `r008_run.py` and `r008_fetch.py` were not run, and no guard function was called
against the real tree. Nothing was fetched.

**What was run.**
- `.venv/bin/python -m tests.test_r008`: **"all checks passed (86)"** in about 40 s.
- `git diff --stat efefc7f HEAD` over the 14 reused modules (incl. `p5_run.py`) is empty: the
  byte-identity claim holds.
- Five synthetic probes in `research/copytrade/scratch_review008a/` (new directory; nothing else
  written):
  - `probe_power.py`: an independent OLS + CR1 implementation of C1–C3. It agrees with
    `crowd008.confirm` to 1e-9 in every draw. It reproduces §3, then runs nulls the draft did not try.
  - `probe_fix.py`: C1 size at 12,000 draws, and the beta-by-window and beta-by-month fixes.
  - `probe_persist.py`: month-scale persistence, and P(economic bar | confirm).
  - `probe_halves.py`: what C2/C3 cost under a time-heterogeneous effect.
  - `probe_altfactor.py`: an omitted alt-market factor.

**Reviewer exposure (for the ledger).**
- Published numbers only: `results.jsonl` line 1 (the T3 block and windows); 001 §2, §8, §11, §12; 012
  D18; ledger `000`, `000c`, `000i`–`000k` (and a grep of `000b`, `000d`–`000h`).
- `docs/alpha-lab-spec.md`, the 2026-09-14 entries: full-period cross-sectional statistics on Bitget
  perps through 2026-09.
- The summary of `docs/inverted-ai-and-prompt-data-2026-10-11.md`. It covers Bitget/Capital AI
  decisions from 2026-06-27 to 2026-10-10; the headline is that the inverted AI earned +0.95 ATR per
  trade net at 48 h. This is holdout-month information on a contrarian payoff (see M3).
- **Real-data reads.** The status counts of `data/p0/2026-10-07/symbols.json` (168 ok,
  56 delisted_on_hl, 9 no_bitget_contract, 1 price_mismatch). The **discovery-only** BTC trailing
  30-day trend sign from `btc_1h.parquet`, with the query bounded to `ts < 1782864000000`: in window
  A's lookback (2026-03-09 → 06-07), 68 % of days were rising; in window B's discovery part
  (2026-05-04 → 06-30), 32 % were rising.
- No quantity dated on or after 2026-07-01 was computed.

---

## 0. Answers to the brief, in short

| probe | finding |
|---|---|
| Fidelity to 001 T3 inverted | **The book is faithful.** Cohorts, tilt, universe, entry, 3-day tranches, inverse-vol, costs and funding all come from 001's code, byte-identical. The three deviations in the data path are each forced: the June cut, a fresh funding pull, and 008's own Bitget store. **The decision rule is new, not inherited:** gross instead of net, Jensen α on BTC, t_{12} instead of z 2.33, IC dropped. The draft discloses this in §4 (a)–(d), but the title, §0 and D1 still say "everything else unchanged" (m1). |
| Confirm statistic | A before-costs existence test is legitimate and well argued (§0 consequence 2). **But its beta model is a single constant β over two cohorts and three months of very different BTC drift. That inflates false confirms to 0.11–0.25 (B1).** It also omits the alt-market factor (M1). β̂ is full-sample, in-sample and not point-in-time; that is fine for a test, but it is not stated (m5). |
| C1 calibration with 13 clusters | Fine under the draft's model: 0.053 at 12,000 iid draws; 0.053–0.056 with GARCH + t4 BTC, a rally drift path and residuals heteroskedastic in \|BTC\|; 0.04–0.06 with β chasing trailing BTC. Month-scale persistence that week clusters cannot absorb gives 0.07–0.12 (m3). |
| Two-point-bar problem in C2/C3 | **No.** They cost 0.005–0.02 of power, including when the effect lives in one window only (sound). |
| Power | Reproduced independently: 0.311 / 0.154 (full / half), against the draft's 0.305 / 0.141. σ = 0.5531 %/day, re-derived exactly. |
| Calendar | No look-ahead and no overlap. The June cut is clean. Nothing after 2026-09-28 enters an outcome: book days end 09-25 02:00, and the IC ends 09-27 02:00. Inherited book quirks across the window gap are described inaccurately (m2). |
| Economics and tail | The cost arithmetic is right: 7.33 bp/day steady, 7.29 over 164 days, net +0.17. **The economic bar is near-vacuous (P(bar \| confirm) 0.96–0.99), and the labels contradict §0 (M2). The tail numbers contradict each other (M4).** "A confirm leads only to a cost-reduced follow-up" is stated, not enforced, and `confirm_capturable` routes to a paper test of the *same taker book* (M2). |
| D18, family, P0 | D18 is coherent: 012 never read the calendar, the effective family is 1, and the remainder is left sealed. The P0 handling for window B is adequate. **The disclosure of earlier looks on the same calendar is incomplete (M3).** |

---

## BLOCKER

### B1. One constant BTC beta across two cohorts and three months of different drift: false confirms of 0.11–0.25 under plausible nulls.

**Where.**
- `ct/crowd008.py:363-383` (`confirm` → `ols_cluster(gross, btc, weeks)`, one slope);
- registration §5 (C1), §3's beta-null table, and D5 ("false-confirm 0.044–0.051 under every null in
  §3, beta nulls included").

**Why it matters.**
- The book's BTC beta is a property of the cohort's positions. The cohort changes at S = 2026-08-03,
  from window A (scored on the bear lookback 03-09 → 06-07) to window B (scored on 05-04 → 08-02).
- BTC's drift differs sharply between the months: 001 §2 gives Aug +24 % and Sep +8 %, and July is
  different again.
- A single pooled slope, fitted on 30 July days and 53 August–September days, leaves (β_B − β̂)·μ_B
  and (β_A − β̂)·μ_A in the intercept. That bias is of the order w_A·w_B·Δβ·Δμ ≈ 0.23 × 0.3 × 0.5 %
  ≈ 3.5 bp/day, half the full effect.
- The draft's own nulls use one β and one drift for the whole holdout, so they cannot show this.

**Probe** (`probe_power.py`, `probe_fix.py`). No alpha in any row; BTC N(drift by month, 2.5 %); Aug
and Sep drifts from 001 §2; July varied; 3,000 draws per row.

| null | registered rule | β by window | β by month |
|---|---|---|---|
| constant β 0.3 (draft's case) | 0.048 | 0.049 | 0.047 |
| β_A 0 / β_B 0.3, July flat | **0.109–0.116** | 0.049 | 0.047 |
| β_A 0 / β_B 0.5, July −0.3 %/day | **0.236–0.246** | 0.049 | 0.046 |
| β_A −0.2 / β_B 0.3, July −0.3 %/day | **0.236** | — | — |
| β_Jul 0 / β_Aug 0.4 / β_Sep 0 (within window B) | **0.155** | 0.098 | 0.047 |
| β_A 0.3 / β_B 0 (the reverse sign) | 0.013–0.021 (deflated) | — | — |
| **power**, full / half, constant β 0.3 | 0.303 / 0.145 | 0.301 / 0.139 | **0.298 / 0.138** |

- Δβ of 0.3 between two independently scored cohorts is ordinary for a dollar-neutral, inverse-vol alt
  book. The draft itself argues that the long leg is high-beta alts (§9).
- The direction of the bias is unknown in advance, so the test is not conservative.
- C1 is the registration's one decisive statistic, and it cannot be changed after REGISTERED. That
  is why this is a blocker rather than a major.

**Fix (cheap).**
- Give C1 one common intercept and **a separate BTC slope per calendar month**:
  y_t = α + Σ_m β_m·BTC_t·1[month(t) = m] + e_t. Keep CR1 by ISO week and t_{G−1}.
- For the 2026-09-28 end, this is three slopes. July is exactly window A, so the month split nests
  the window split.
- For 2026-11-23, use one slope per (window, month) cell, and merge cells under 10 days into their
  neighbour.
- C2 and C3 run on y − Σ β̂_m·BTC·1[m].
- Probe cost: about 0.005 of power, with size restored to 0.046–0.049 in every null above.
- Re-run §3 with these window- and month-beta nulls added, and restate D5.
- **Alternative**, closer to the economic meaning: hedge with an ex-ante, point-in-time book beta.
  That is Σ_i w_i·β_i, with β_i each coin's trailing 30-day beta from the same closes, known at entry,
  as the vol is. Test the hedged series' mean. It is tradable, and it removes the in-sample β̂
  question (m5).

---

## MAJOR

### M1. BTC is the wrong (or an incomplete) market factor for a dollar-neutral alt book.

**Where.** §5 C1 and `crowd008.confirm`: BTC is the only regressor. §10 concedes that "other regime
exposures are not adjusted (alt-season dispersion)" but takes no action.

**Why.**
- The book's systematic exposure is mostly to an alt index beyond BTC. If losers lean into high-beta
  alts (the draft's §9 mechanism), the inverted book is long the alt factor.
- In an alt-led stretch of the rally, the BTC-only regression leaves that loading × the factor's
  drift in α.

**Probe** (`probe_altfactor.py`; no alpha; BTC drift 0.4 %/day; book loading 0.3 on an alt-minus-BTC
factor):

| null | registered C1–C3 | C1 on BTC + EW-alt index |
|---|---|---|
| alt factor +0.3 %/day in August only | **0.105** | 0.049 |
| alt factor +0.2 %/day throughout | **0.180** | 0.051 |
| no loading (control) | 0.048 | 0.049 |

**Fix.**
- Add a second regressor: the equal-weight 24 h return of that day's signal universe. It is
  point-in-time, from the same Bitget closes and the same entry key.
- Combine it with B1's per-month slopes, and simulate size and power before registering.
- If the owner prefers to keep the primary minimal, then at the very least report α on BTC + EW-alt
  (§6), and state in §10 that a confirm without it cannot be attributed to positioning.

### M2. The economic bar is near-vacuous, and the labels contradict §0 and D6.

**Where.** §5's bar and labels; `crowd008.economic` and `decide`; §3 "P(CONFIRM and bar)".

**The numbers.**
- From the draft's own table, P(bar | confirm) = 0.299 / 0.305 = **0.98** (full) and 0.136 / 0.141 =
  **0.96** (half). `probe_persist.py` gives **0.989 / 0.960**.
- A confirm needs a gross α̂ of about 10.8 bp. The bar needs a net α̂ of 0.82 bp, i.e. a gross α̂ of
  about 8.1 bp. The bar therefore almost never binds.

**What it is computed on.**
- the winner's-curse-inflated point estimate (§3 says so);
- the ex-post, full-sample β̂;
- a costless BTC hedge.

**What follows.** Nearly every confirm will be labelled `confirm_capturable`. Its next step is "a
forward **paper** registration **of the same rule**". That is the taker book which §0 says has
"P(a deployable book at 001's costs) ≈ 0", and which D6 says is never deployed. The row's own label
will then contradict the registration.

**Fix.**
- Apply the bar to the quantity §6 already computes, `economic_shrunk`: half the gross α̂ minus the
  mean cost, × 365 ≥ 3 %/yr. Alternatively, apply it to the one-sided 95 % lower bound of the net α.
- Rename the labels to say what they certify, for example `confirm_gross` and
  `confirm_gross_net_positive`.
- Make the next step for **every** confirm the cost-reduced registration (maker or lower turnover),
  decided on data after the end through a D18 amendment, as D6 already intends.
- Add a `recheck` test for the renamed bar.

### M3. The disclosure of earlier looks on the same calendar is incomplete.

**Where.** §13 "Earlier looks that share 008's calendar", and its closing "Nothing in this
registration was chosen from a holdout number".

**The rule.** The programme's hard rule is: "Every new registration states which calendars it reads
and which earlier looks share them." 008's book is a cross-section of **Bitget USDT-M returns,
2026-07 → 09**. These looks share that calendar and are not named:

1. **The 2026-09-14 alpha-lab sweep** (`docs/alpha-lab-spec.md`).
   - It covered 83 Bitget USDT perps, 2024-03 → **2026-09**.
   - It produced cross-sectional rank ICs: 1-day reversal +0.0275 (t 4.0); 5-day momentum −0.021 (t
     −3.0).
   - It also produced costed quintile long/short books, which lose gross.
   - These are full-period statistics that include the holdout months, on the same venue and a heavily
     overlapping universe. The inverted 008 signal plausibly loads on exactly these reversal factors
     (m9).
2. **The 2026-10-11 inverted-AI study** (`docs/inverted-ai-and-prompt-data-2026-10-11.md`).
   - The contrarian ("fade the obvious move") payoff on Bitget/Capital decisions from
     2026-06-27 to 2026-10-10; the headline is +0.95 ATR per trade net at 48 h.
   - It was framed as "crowding", one day before L3 was ranked first and its pull approved.
   - This is the most relevant prior on 008's own months: a contrarian payoff was seen to work in
     them.
3. **The live AI trader** itself, which traded Bitget majors through the holdout months (owner-visible
   P&L).
4. `000i`, `000j`, `000k` (round 3) are not named. They are irrelevant to HL prices, but the rule asks
   for them to be stated.

**Fix.**
- Add these four looks to §13, with what was seen and what must not follow.
- Qualify "nothing was chosen from a holdout number": the ranking of N6 (2026-10-12) followed item 2.
  State that the rule itself is fixed by 001 and has no free parameter that item 2 could tune.
- Ledger the reviewer exposure above.

### M4. The tail numbers contradict each other and the scenario that produces them.

**Where.** §9 Tail. The book is "1× gross (0.5 long, 0.5 short)".

- **The spiral day.** "Long leg −15 to −30 % against the short leg's −10 to −20 %" gives
  0.5 × (−15 … −30 %) − 0.5 × (−10 … −20 %) = **−2.5 % to −5 % of gross**, not "−5 % to −10 %".
- **The worst week.** It is stated as "≈ −6 %: about 3 weekly SD with fat tails, plus one spiral day".
  But 3 × 1.6 % = 4.8 %, plus the stated spiral day of −5 to −10 %, makes −10 to −15 %. Even with the
  corrected spiral day it is about −7 to −10 %.
- **The worst month.** It is stated as −12 %. 3 × 3.0 % plus a spiral day is −12 to −19 %, depending on
  which spiral number holds.
- Then "1× gross … worst plausible month −12 %" is carried into the sizing paragraph and the D11
  framing.

**Fix.**
- Pick one scenario and carry it through. For example: spiral day −2.5 to −5 % (or a harsher
  divergence, long −30 % / short −5 %, giving −12.5 %); worst week about −8 to −10 %; worst month
  about −12 to −16 %.
- State what a gap or delisting of a long-leg coin adds.
- No capital rides on this book, but the programme treats an understated tail as a review-level
  defect (010 B3), and any paper or cost-reduced follow-up will inherit these numbers.

---

## MINOR

- **m1. Fidelity wording.**
  - The title, §0 ("What is inherited, unchanged … the costed book") and D1 ("everything else
    unchanged") should say: **the book is unchanged; the test is new** (gross, BTC-α, t_{G−1}, IC
    reported). §4 (a)–(d) already says so.
  - D3 and §2 say "windows dropped, never truncated", yet window A *is* truncated at its start (the June
    cut). That is forced and fine (§4 item 1), but the sentence is false as written.
- **m2. Inherited book mechanics, described inaccurately.** Leave the behaviour alone for fidelity,
  but describe it correctly. `crowd.long_short` (`ct/crowd.py:84-105`) ages tranches per signal day,
  not per calendar day. As in 001:
  - the 3 gap days 07-31 → 08-02 are never booked;
  - the 07-29 and 07-30 tranches (cohort A) earn the 08-03 and 08-04 returns, so `by_window`
    attribution is approximate;
  - the last two tranches never complete: book days end 2026-09-25 02:00, and their exit cost is never
    charged (2 × 11 / 3 bp over 83 days, ≈ 0.1 bp on the net mean);
  - the 16.9 h snapshot makes two consecutive day returns overlap by about 16 h.

  §2's "returns to 2026-09-28" should read "book returns end 09-25 02:00, IC returns 09-27 02:00".
  Prices and funding are fetched to 09-30 and 10-02 for slack and are never used.
- **m3. Persistence beyond a week.**
  - A slow residual component that week clusters cannot absorb gives false confirms of **0.068**
    (2 % of variance, ρ 0.9), **0.093** (5 %, ρ 0.9) and **0.117** (3 %, ρ 0.97) (`probe_persist.py`).
  - With 13 clusters nothing better is available, but "holds its size under every null" (§3 Reading,
    D5) should be qualified in §10.
- **m4. Forward-looking survivorship in the holdout universe.**
  - The 2026-10-07 symbol table drops 56 `delisted_on_hl` and 9 `no_bitget_contract` coins.
  - Coins delisted during Jul–Sep 2026 are removed *with hindsight*. On the draft's own mechanism
    (losers lean into dying alts, which sit on the inverted long side), this biases toward a confirm.
  - Report per window the count of HL markets with ≥ 3 cohort holders that are excluded by each
    non-ok status (counts, inside the read).
- **m5. State how β̂ is estimated.**
  - §5 should say that β̂ is full-sample OLS on the read's own days, and so is not point-in-time.
  - C2 and C3 inherit it, so "both halves > 0" is not an out-of-sample stability check.
  - §13's "a stricter rule than the raw mean under a rally … never a looser one" is false for a book
    with β < 0. In that case α̂ exceeds the raw mean; size is unaffected.
- **m6. No floor on cohort or breadth.**
  - If a window's scored set is under 5 (k = 0), every day has an empty signal. Those days still enter
    n and G with zero or partial exposure, and dilute power silently.
  - The discovery part of B's lookback is 68 % falling, so a collapse is unlikely, but the rule should
    say what happens. For example: "a window with k < 20 or a median under 10 coins is reported and its
    days are excluded", fixed now.
- **m7. Holdout fills land in the shared tree.** They go to `data/pruned/…/fills/perp/all/date=2026-07-*`,
  where glob-based scripts (`p3_checks.py:33`) and any future builder would silently include them.
  Add a ledger note and a sentinel, or a README, in the partition root.
- **m8. Fetch byte accounting** (review B's area, noted only).
  - `r008_fetch.py:383`: `attempt['bytes'] += s3_pull(...)` never runs when `s3_pull` raises mid-pull.
    A failed attempt therefore records only its footers, and the cumulative cap and the fetched row's
    `bytes_requester_pays` undercount.
  - The cap is checked after each object, so it can overshoot by one object.
  - Fix: count inside `s3_pull` into the attempt dict before raising.
- **m9. Factor confound.**
  - Cohort B is scored on July trips, and losers keep the coins that fell. The inverted signal
    therefore plausibly loads on 1–5-day reversal and anti-momentum, the factors found on the same
    venue and months (M3 item 1).
  - Report α controlling for a 1-day-reversal and a 5-day-momentum quintile book built from the same
    closes and universe (§6, reported only), so that a confirm can be read for mechanism.
- **m10. P0.** The handling is adequate. Optionally, report the C1 statistic with P0's 50 wallets
  removed from both cohorts.

---

## Checked and found sound

- **Byte identity** of the 14 reused modules with `efefc7f` (an empty diff). Every module the read
  imports is in the guarded list (`ct/stats`, `ct/scores`, `ct/regimes` → `ct/bitget` → `ct/net`, and
  so on).
- **Calibration arithmetic.**
  - NW se 4.3186 bp × √164 = **0.5531 %/day**.
  - Inverted gross +7.464 bp; halves +2.78 / +12.14; median ≈ +10.85 bp.
  - Cost 2 × 11 / 3 = 7.333 bp/day steady (7.29 measured over 164 days).
  - Inverted net +0.17 bp; t before costs +1.73.
  - P(CONFIRM) of 0.07 / 0.10, and the real share of 40 % / 60 %: right *given* the stated size
    (B1 changes this).
- **Power**, reproduced by an independent implementation: full 0.311, half 0.154 (draft 0.305 / 0.141).
  C1 size: 0.053 ± 0.002 at 12,000 draws. Raw-rule beta nulls are reproduced (0.19 at β 0.4,
  +0.25 %/day).
- **Robustness under constant β.**
  - GARCH + t4 BTC, with a rally drift path: 0.053–0.056.
  - Residual scale ∝ |BTC| with t3 residuals: 0.054.
  - β chasing trailing 7-day BTC: 0.042–0.061.
- **C2 and C3 cost little power**: C1-only against the full rule is 0.323 / 0.314 uniform, 0.305 /
  0.288 with the effect in window B only, and 0.263 / 0.243 with it in window A only. This is not a
  two-point-bar problem.
- **The calendar.**
  - The grid is 2025-10-27 + 56k, so S = 2026-06-08 and 2026-08-03, with hold ends 08-03 and 09-28.
  - There are 83 planned days, in ISO weeks W27–W39 (13).
  - The 33 holdout fill days run 07-01 → 08-02.
  - The filter `t_s ≥ 1782864000000` sits in `signal_snapshots`.
  - Cohort A uses only fills and equity before 06-08. Cohort B's lookback ends 08-02, before any of its
    signals.
  - No 001 return reaches 06-08. Nothing after 09-28 enters an outcome.
- **The CR1 formula and t_{G−1}**: G/(G−1)·(n−1)/(n−2) is Stata's CR1, and t_{12}(0.95) = 1.7823.
- **The decision machinery.** `recheck` recomputes the criteria and the label from the stored numbers.
  `orig_net = −gross − cost` is right for 001's sign.
- **The data path matches 001.** `score_t3` and `quintiles_by_score` mirror `p5_run.score_wallet` and
  `finish_copy_window`: the as-of equity strictly before S; address order with a stable sort.
- **D18.**
  - 012 closed at Stage 1 with no read, so (d) is moot and the effective family is 1.
  - 008's series are within (b) "the series its registration names", including the holdout rows of
    `snapshot_accounts.parquet` for window B.
  - The remainder, 2026-09-28 → 2027-06-30, stays sealed, and any reuse needs an open amendment.
- **P0 for window B.**
  - The pilot's period (08-08 → 10-07) touches B's book days, but not its lookback.
  - Its wallets were sampled from a leaderboard, not on returns.
  - The one copy result is for a day trader outside the slow population.
  - The overlap counts are reported.

---

## Verdict

**Not ready to register: one blocker (B1) and four majors (M1–M4).**

The book is a faithful, byte-identical 001 T3 with the sign flipped. The calendar is clean, and the
power arithmetic reproduces. But the decisive statistic models the market with one constant BTC beta
across two cohorts and three months of very different drift, and omits the alt factor. Under nulls
that are ordinary for this book, it false-confirms 2 to 5 times its nominal rate.

The fixes are cheap and cost about 0.005 of power:
- per-month BTC slopes, ideally with an EW-alt regressor or a point-in-time ex-ante hedge;
- a bar on the shrunk estimate, with honest labels;
- completed §13 disclosures;
- coherent tail numbers.

Re-run §3 with the new nulls, re-hash `ct/crowd008.py`, and re-review B1/M1 against the revised
simulation before REGISTERED.
