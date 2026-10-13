# 014 DRAFT (Revision 1): independent review A (option mechanics, statistics, tail and sizing), 2026-10-13

Reviewer A. Scope: the whole draft, deepest on pricing and fills, the hedge, margin, inference, the
Revision 1 tail and size, the economic bars and calendar exposure. Review B (`014-review-b.md`, guard
and data path) was read only after these findings were written. Where the two overlap, that is said.

## How this was checked

- **Read:** `014-short-vol-deribit.md` (all of it), `ct/shortvol.py`, `ct/shortvol_sim.py`,
  `r014_run.py`, `r014_tail.py`, the fetcher (`r014_fetch.py`) where it touches field semantics,
  `scouts/n2-options.md` (cost table), `000k`, `000m`, the orchestrator log's 014 entries, and
  `data/r014/logs/power.json` (synthetic).
- **Ran:** `.venv/bin/python -m tests.test_r014`: 126 checks pass. `sha256` of the six code files
  matches §9.
- **Probes**, all in `scratch_review014a/`:

| probe | data | what it does |
|---|---|---|
| `p1_floor_seeds.py` | synthetic | the registered floor procedure (`calibrate_floors('H24')`, fat world, 20,000 weeks, 6,000 draws) on 4 new seeds |
| `p2_forward_basis.py` | pre-2021 tail tape, entry windows [E − 4 h, E) only | trade price inverted on the **index** vs the trade's own Deribit `iv` (on the forward); call-mark vs put-mark IV at near-ATM strikes |
| `p3_leg_choice.py` | same | which leg sets `straddle_entry`'s IV, and how far the premium moves against the two-leg average |
| `p4_own_sim.py` | synthetic, own world | independent daily-hedged world (Markov calm/stress vol, t(5), down-jumps), using the registered costs, `summary` and `discovery_criteria`: null A (constant VRP), null B (regime VRP), power at net 0.05 of premium, on contiguous 143-week windows and 13-week blocks |
| `p5_regime_null.py` | synthetic | null B at two strengths; the 0.975 quantile of the skew-corrected t |
| `p6_day1_crash.py` | synthetic paths | the registered `position_pnl` (H24) under a −25 / −40 / −50 / +40 % move as one 12-hour gap on day 1, 3 or 6 |
| `p7_kelly_sens.py` | synthetic + the frozen `REALISTIC_WEEK` constants | `kelly_atoms` at VRP 0.05 / 0.075 / 0.10, with the 21 atoms, +1 or +2 single-gap weeks of −45 % |

- **No** calendar data (2021-03-24 → 2026-06-30) was opened by any probe. The tests hash the panel
  files. No request was sent to any network. Reads are listed at the end.

## BLOCKER

### B1. The entry premium is set by one leg's mark inverted on the index; on real tape the error is about 3 % of premium (median) and leans to the seller

Same mechanism as review B's M4, found independently. This review measures it on real tape (out of
calendar) and rates it a blocker.

`straddle_entry` (`ct/shortvol.py` l. 307–316) does three things:
- it takes the **latest trade at K, call or put**;
- it inverts `mark_price × index` with Black-76 at **F = index**;
- it prices **both** legs at that IV.

Deribit marks options off the expiry's forward, so the call and put marks give different IVs on the
index. Which leg traded last therefore moves the premium.

**Measured on the 28 tail weeks' entry windows** (pre-2021 tape, [E − 4 h, E), no outcome):
- **Index-based IV minus Deribit's own trade `iv`** (`p2`):
  - median over weeks **+2.1 vol pts for calls, −2.4 for puts**;
  - **±6 to ±10** in carry weeks (2019-06-21, 2020-02-14, 2021-02-19);
  - **−18 / +45** in the 2020-03-13 backwardation.
- **The ATM call-mark IV and put-mark IV differ by up to 15 vol pts** in the same window (BTC
  2021-02-19: 108.5 vs 93.4).
- **The premium `straddle_entry` produces vs the two-leg average** (`p3`, 39 coin-weeks with both legs):
  - **median |error| 3.0 % of premium**, max 15.8 % (BTC 2020-03-13), 11.1 % (ETH 2019-06-21);
  - **calls set the IV in 2 of 3 weeks**, so the **mean error is +0.9 % of premium, in the seller's
    favour**.

**Why this blocks:**
- The registered net edge at the prior is **≈ 5 % of premium**, and costs are 4.9 %. An entry error
  whose median is 3 % and whose mean leans +0.9 % is the size of the effect.
- The lean comes from a mechanism. Calls trade more in contango rallies, and the discovery (2021) and
  holdout (2024 Q1, Q4) have high-carry stretches. So the lean can be larger on the calendars than on
  the tail weeks.
- It adds variance to a test whose power is 0.2. The synthetic world has no basis, so §3's power and
  floors do not see it.
- The realistic tail (§5) and the IV proxy (`atm_iv_series`) use the same inversion, and so do the
  tail's fallback weeks. So n and the tail charge inherit the error (2020-03-13 BTC: premium −16 %
  against the two-leg average).

**Fix** (no new pull; the panels keep `price`, `iv`, `mark_price` and `index_price`):
- Recover the forward. Either solve F′ from the chosen trade's (`price`, `iv`, K, T), or take
  put-call parity on the latest call and put marks at K.
- Invert **both** legs' marks on F′. Price the straddle as call plus put at F′, re-anchored to the
  entry index by the trade's F′/index.
- If neither route solves, mark the straddle missing (counted). Keep the index-based mark as a
  reported block.
- Recompute the tail under a `000m` addendum that says the change came from review, re-derive n
  (see M1), and update the §9 hashes.

## MAJOR

### M1. n = 0.40 is not a robust ¼-Kelly size, and "worst plausible week −5.6 % of equity" is not plausible-worst

**The quarter-Kelly rests on two knife-edges** (`p7`, the draft's own `kelly_atoms` and banks):

| world, gross VRP | 21 realistic atoms | + one −45 % single-gap week | + two |
|---|---|---|---|
| base 0.10 | 1.78 (¼ = 0.45) | 0.58 (¼ = 0.15) | 0.01 |
| fat 0.10 | 1.65 (¼ = 0.41) | 0.51 (¼ = 0.13) | 0.01 |
| base 0.075 | 0.12 (¼ = 0.03) | 0.01 | 0.01 |
| fat 0.075 | 0.05 (¼ = 0.01) | 0.01 | 0.01 |

- One extra single-gap event in 390 weeks cuts Kelly by 3×. Two zero it: they wipe the whole premium
  at VRP 0.10.
- A gross VRP of 0.075 instead of 0.10 cuts Kelly by 15–30×.
- The size is therefore ¼ Kelly only under a sample with no single-gap book event and a VRP of at least
  0.10. That VRP is the unknown being tested.

**The realistic tail is path luck, not a bound.** `p6` runs the registered `position_pnl` with entry IV
0.5–0.8 and one 12-hour move, then flat:

| move | day 1 | day 3 | day 6 |
|---|---|---|---|
| −50 % | −46 % | −46 % | −45 % |
| −40 % | −36 % | −36 % | −35 % |

- A gap between two daily hedges costs nearly the whole move **on any day**. 2020-03-06 lost only
  5.2 % because that decline was a trend across five hedges, not because it came on day 6.
- §5's explanation ("the crash came on day 6 … almost fully hedged") misattributes the cause. A
  2020-03-style day-1 crash is covered **only** by the adversarial stress (−53.8 %, −21.5 % of equity
  at n = 0.40), not by the realistic tail, the charge or Kelly.
- The draft's own synthetic worst H24 weeks are −33 % (base) and −42 % (fat) of notional (§3). Both
  are 2.4–3× the "worst plausible" −14.1 %.

**The realistic worst month counts uncomputed neighbours of stress weeks at 0.** The pre-2021
underlying (`select`'s own excursion function) has 31 non-selected weeks with a 15–25 % excursion,
including:
- **2020-12-25 (24.8 %) and 2021-01-08 (24.6 %), on both sides of the −12.3 % week of 2021-01-01**;
- 2020-04-24 (24.9 %) and 2020-11-27 (23.2 %).

Vol clusters (the draft's own T2 premise), so 0 is not neutral. "Month = week = −5.6 % of equity" is
an artefact of that.

**Fix (pick one, stated before registration):**
- (a) Size by a rule that survives one more event: ¼ Kelly with the realistic atoms **plus** one
  single-gap atom, over VRP 0.075–0.10 (gives n ≈ 0.10–0.15, close to Revision 0's 0.10).
- (b) Decouple size from the test: report n only as the scale of criterion 5 and the deploy bar, and
  leave the deploy size to the paper registration, sized from the confirmed lower bound.

In either case:
- Register "worst plausible week/month" as the adversarial stress, with the realistic paths as the
  reported number.
- Under a `000m` addendum, re-run the 21 tail weeks with the **hedge clock shifted 0–23 h** (the data
  are on disk). That shows the timing dispersion of each event.
- Count uncomputed neighbouring weeks at the mean stress-week loss rather than 0, or compute them.

The orchestrator's go/no-go for 014 ("if the realistic tail allows a materially larger n, 014 goes to
review; if not, parked") rested on this number. It should be re-decided after B1 and M1.

### M2. The floors hold size only when the conditional VRP is flat; under the draft's own regime story the discovery α roughly doubles

My own world (`p4`, `p5`) adds a two-state weekly vol regime (13 % stress, mean stay about 6 weeks).
It uses the registered costs, statistic and floor, on contiguous 143-week windows:

| null (unconditional net mean exactly 0) | P(T1 passes c1, at 2.59) | 0.975 quantile of skew-t (disc) | holdout q95 / q975 |
|---|---|---|---|
| A: one VRP for all weeks | **0.004** (13-wk blocks 0.009) | — | — |
| B mild: calm +4 pts on m, stress solved (m 1.09 / 0.92) | 0.018 | 2.48 | 2.03 / 2.49 |
| **B strong: calm +8 pts, stress m 0.78** | **0.047** (blocks 0.038) | **3.16** | **2.46 / 3.11** |

- In the draft's world the floors hold, and are conservative in mine (null A). This confirms §3.
- When the VRP is positive in calm and negative after spikes, the conditional mean is persistent and
  the windows' stress share varies. A null with zero unconditional mean then passes discovery at about
  1.9× nominal. That mechanism is exactly T2's (Almeida's negative VRP after 2020-03).
- The holdout still protects the joint path (independent windows; about 0.047 × 0.1).
- But "error control at n = 0.40: discovery 0.025/0.028" (§3 Reading) is a property of a world with
  no conditional VRP.

**Fix:**
- Add a regime-VRP null to the floor calibration and take the max, or at least report it in §3 and §8
  with the α range.
- Say plainly that a T1 discovery pass with T2 ≥ T1 is the regime alternative, not a clean
  unconditional premium.

## MINOR

1. **The holdout k = 2 floor is a median draw, not a bound.** The registered procedure (max of two
   seeds) on four new seeds (`p1`):
   - discovery 2.44 / 2.54 / 2.55 / 2.47 (all ≤ 2.59, fine);
   - hold95 2.10 / **2.162** / 2.08 / 2.13 (one just above 2.16);
   - **hold975 2.52 / 2.55 / 2.50 / 2.54**: two of four exceed the registered 2.52.

   Take the max over ≥ 6 seeds, or add +0.05.
2. **The economic bars are vacuous at n = 0.40, not power-killing.** A discovery pass needs a
   skew-t ≥ 2.59, so mean ≳ 0.65 % of notional a week (SD ≈ 3 %, G 143). A holdout confirm needs
   ≥ 2.16, so mean ≳ 0.57 %. Both are well above criterion 5 (0.144 %) and the deploy bar (0.316 %).
   The drawdown test (−26.8 % at n) needs about −67 % of notional cumulative. So
   `deploy_paper_registration` is practically "existence confirmed". **Fix:** apply the deploy bar to
   a one-sided lower confidence bound of the holdout mean, or state that it never binds. Arithmetic
   checked: 0.03 / 52 / 0.4 = 0.1442 %; + 0.1720 % = 0.3163 %.
3. **`000m` says no criterion may change from the tail numbers, but criterion 5's threshold moved
   4×** (0.577 % → 0.144 %) through n. That raised the synthetic discovery null from 0.009 / 0.018 to
   0.025 / 0.028. It is allowed by "n may be set", but the ledger addendum should say so explicitly.
4. **The tail charge is a hybrid.**
   - It adds the pre-2021 stress weeks' net (winners included) over 390 weeks to a calendar mean that
     already holds its own stress weeks. Losses only, it would be 0.206 %/wk (+20 %).
   - Keeping winners is right for an expectation. Dropping them would be selection on outcome.
   - But then the charge is a partial full-history adjustment, not a "tail price". It also omits the
     94 non-selected pre-2021 weeks.
   - State it as a deliberately conservative add-on and keep the losses-only figure as a report.
5. **The tail's fallback strikes are near-ATM; their IVs carry B1's error.** "Struck at the index, IV
   of the nearest traded strike" used strikes just outside the 2.5 % band:
   - BTC 30000 vs 30,787 on 2021-01-22;
   - ETH 1280 vs 1,317 on 2021-01-29;
   - ETH 130 vs 126.5 on 2020-03-13;
   - BTC 6500 vs 6,314 on 2020-03-20.

   Their distance (2.6–3.3 %) makes the smile effect small. But ETH 2020-03-13's IV of 1.87 (BTC 1.33)
   is in the week where `p2` shows ±18–45 pts of index-vs-forward error. Fixing B1 fixes these too.
   Also state the fallback's maximum distance in §5.
6. **The coin-margined collateral is implicit.**
   - The USD accounting is exact for the inverse option and the inverse perp (each is linear in USD at
     its conversion price; checked).
   - The Black-76 delta 2N(d1) − 1 is the right hedge for a USD book.
   - But the equity "1 + n × P&L" is USD-stable only if the 1/n coin collateral is itself shorted on
     the perp. That leg pays or earns funding on 2.5× notional and is not modelled.
   - Margin on Deribit is checked in coin.

   State the assumption in §4.9–10. Irrelevant to liquidation at n = 0.40: the threshold is a loss of
   ~235 % of notional.
7. **`decide_holdout` returns a label, not the book.** The §6 rule "if both, T2 if its tail-charged
   annualised net ≥ T1's" is not coded, and neither is the case where only one confirmed trial meets
   the bar. Implement and test it.
8. **Disclosure.** The Deribit Insights 0.35 Δ weekend-strangle backtest (2020 → 2024-08) covers
   **the whole discovery calendar**, not only the holdout's first eight months. The §1 discovery row
   omits it. The cost table that sets h (p75) includes three holdout days (disclosed in §0; repeat it
   in §8).
9. **Inconsistencies:**
   - "2021-01-01: −12.4 %" vs −12.35 in code and −12.3 in the table;
   - "2020-03-06: −5.3 %" vs −5.25 / −5.2;
   - the expected %/yr uses the 6,000-week primary bank (0.427 / 0.477 %/wk) while Kelly uses the
     3,000-week bank (0.415 / 0.394 %/wk);
   - "deploy ~1 in 12" (§1) vs the simulated joint power at the prior, 1 in 15–17, and the
     orchestrator table's "1 in 6".
10. **Staleness (point 1 of the brief) is minor.** Chosen-strike ages are 2–5 min (p50) and 14–22 min
    (p90). The re-pricing is sticky-strike, at the new index. A drift bias needs IV to trend inside the
    window, and near-ATM smile slope is small. It is dominated by B1.
11. **The fill population is acceptable and conservative.** The p75 of 30–45 Δ, 3–7 d options
    (2.60 / 2.31 % of premium) is applied to a 50 Δ, 7 d straddle, whose premium is larger. Its own
    relative spread is lower, so p75 errs conservative. The scout's Δ buckets also used index
    inversion (B1), which slightly misbuckets them.
12. **The placebo** flips the gross return and keeps the cost, so its mean is −cost by construction
    and it will always "pass" trivially low. Say it is a sanity print only.

## Checked and found sound

- **Calendars and boundary.**
  - 143 / 129 entries, with the embargo entry 2023-12-29.
  - Expiries strictly before 2024-01-01 / 2026-07-01.
  - The holdout series start (2023-12-24) covers the T2 look-back of 2024-01-05.
  - The settlement trades lie in the next Friday's [04:00, 08:00) window.
- **Point in time:**
  - `perp_at[t]` / `dvol_at[t]` = the close of the bar opened at t − 1 h.
  - `stress_flag` uses opens ≤ E − 2 h.
  - Funding stamped t applies to (t − 1 h, t] with the hedge held then.
  - If Deribit's DVOL stamps were close times instead of open times, every use would be one hour more
    conservative, never forward-looking.
  - The hedge at t trades at the price at t.
- **Fees:**
  - option min(0.0003·S, 0.125·price) per leg;
  - delivery min(0.00015·S_X, 0.125·payoff) on the ITM value;
  - perp 0.05 % + 1 bp;
  - USD equivalents correct for coin-denominated fees.
- **Inverse payoff:** coin payoff × S_X = |S_X − K|, and the premium is converted at the entry index.
  Consistent.
- **Funding:** sign (a long pays) and the `interest_1h` field.
- **Margin:** the standard short-option MM (0.075 + mark per leg) and the liquidation inequality's
  algebra (equity S0/n + P&L < MM).
- **Hall's transform:** formula, sign for negative skew, monotonicity (dT/dθ = (1 + gθ/3)²).
- **Holdout q by trials carried** (0.95 / 0.975) is coherent: Bonferroni over the carried trials, and
  conservative given T1–T2 correlation.
- **The draft's world reproduces:**
  - floors: new seeds land within the registered values, except Minor 1;
  - power at net 0.05 of premium: own world **0.16–0.17** discovery (c1) vs the draft's 0.19–0.23,
    the same order;
  - null A in my world is conservative at 2.59 (0.004–0.009).
- **The Revision 1 numbers equal the code:**
  - `tail_charge()` = 0.17203 %;
  - adversarial 0.26397 %;
  - `sizing_statement()`: worst week/month −5.64 %, adversarial −21.52 % / −26.84 %;
  - the sum of the 21 weeks −67.09 %;
  - expected %/yr: 0.427 × 52 × 0.4 = 8.9 %, minus 3.58 % = 5.3 %;
  - full Kelly: 1.65 × 0.538 = 0.89.
- **Tests** 126 / 126; the §9 hashes match the disk.

## Verdict

**Not ready to register: 1 blocker (B1), 2 majors (M1, M2), 12 minors.**
- **B1** is a mechanics fix in `straddle_entry` and the tail's IV proxy, using fields already in the
  panels. It changes the hashed code and requires the tail to be recomputed under a `000m` addendum.
- **M1** needs a size rule that survives one more single-gap event and the VRP being 0.075 rather than
  0.10. Otherwise n has to be detached from the test. On the evidence here, n is about 0.10–0.15, not
  0.40. The programme decision that sent 014 to review on "n materially larger" should be re-taken.
- **M2** is a calibration or disclosure change.

Nothing found makes the design unsound in principle: the statistic, the calendars and the cost model
are defensible. But the headline economics (n 0.40, +5–6 %/yr after charge, worst month −5.6 %) do
not survive B1 and M1 as written.

## Reviewer's real-data reads (for the ledger)

All out of calendar, before 2021-03-24. Nothing from 2021-03-24 → 2026-06-30, and nothing on or
after 2026-07-01.

- **`data/r014/tail_pre2021/options_20261013/`** (the 28 computed weeks' tape):
  - only trades in each entry window [E − 4 h, E) for E = 2019-03-29 … 2021-03-05;
  - fields read: `price`, `mark_price`, `iv`, `index_price`, `instrument_name`, `timestamp`;
  - computed: entry IVs (index-based vs Deribit's), and the entry premium under the alternative leg
    choice;
  - **no payoff, MTM, hedge or post-entry quantity.**
- **`data/r014/tail_pre2021/underlying_20261013/`**: BTC/ETH perp hourly candles 2019-01-04 →
  2021-03-19, for `select`'s weekly excursion of the 115 entries (underlying only). Printed: the
  non-selected weeks with an excursion of 15–25 %. Probe 3 also looked up the perp close at each
  entry, as the entry-index fallback.
- **`data/r014/tail_pre2021/tail_results.json`** and **`selected.json`**: read (already computed under
  `000m`).
- **`data/r014/logs/power.json`**: synthetic.
- **The panels** (`panel_discovery`, `panel_holdout`): not opened by any probe. They were hashed only,
  by the test suite's §9 check.
- **No network request was made.**
