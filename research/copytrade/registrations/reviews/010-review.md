# Independent review of 010 DRAFT (cross-venue funding carry), 2026-10-11

Reviewer brief: adversarial review of `registrations/010-cross-venue-funding-carry.md`, `ct/carry.py`,
`r010_fetch.py`, `r010_build.py`, `r010_describe.py`, `r010_run.py`, `tests/test_carry.py` and
`data/carry/logs/describe.json`. `r010_run.py` was not run, in either mode. No P&L, forward funding,
price change or spread dynamics was computed on real data. The synthetic tests pass
(`.venv/bin/python -m tests.test_carry`: 79 PASS, "all checks passed"). The §8 SHA-256 table matches
the 3 code files and the 6 panel files on disk today. The code is untracked
(`?? ct/carry.py`, `?? r010_run.py`), so the guard refuses as the draft states.

**Reviewer exposure (for the ledger).**
- Metadata queries on the panel: per venue-coin min/max settlement **timestamps** in
  `funding.parquet`, and row-existence counts in `prices.parquet` / `levels.parquet`.
- No rate, price, premium or level value was printed.
- The results are quoted in B2.
- One synthetic-only probe (`scratchpad/probe_power.py`) calls `carry.criteria` on simulated rows.

---

## BLOCKER

### B1. The decision funnel measures spread persistence, in discovery and in the holdout, and it is not disclosed as such.

- **Where.**
  - §3, line 162–164: "133,225 coin-decisions with a qualifying pair; **125,799 of those while the coin was already held**".
  - `describe.json` `holdout_counts.counts`: qualifying 88,130, busy 82,280.
  - Produced by `ct/carry.py:219-225` (`counts['busy']`), printed unguarded by `r010_run.py:245-249` (`--events`) and `r010_describe.py:208, 252`.
- **Why it is persistence.** `busy` counts the decision times strictly inside a hold at which the same coin
  still has a pair with |s| ≥ θ. Dividing by the hold's grid points (from the printed `hold_h` mix) gives
  the following.

  | | grid points inside holds | qualifying inside holds | qualifying outside holds | lift |
  |---|---|---|---|---|
  | discovery | ≈ 246 k | **≈ 51 %** | ≈ 1.9 % (7,426 of ≈ 393 k) | ≈ 27× |
  | holdout (CEX–CEX) | ≈ 229 k | **≈ 36 %** | ≈ 0.6 % (5,850 of ≈ 916 k) | ≈ 56× |

  - Only about 2 of ≈ 51 points share the entry's 24-hour window, so this is not overlap.
  - The ratio is E[1(|s_{t+k}| ≥ θ) | entry at t] over k = 8 h … H. That is a conditional, forward
    statistic of the series that *is* the payoff.
  - Programme §3.4 and the 2026-10-11 direction log forbid exactly this ("measuring persistence would be
    running the trial").
  - It tells the drafter that, after entry, spreads stay beyond the 12 %/yr bar about half the time over
    two weeks in 2023–24, and about a third of the time in 2025–26.
- **Aggravating factor.** Open question 2 invites the owner to reconsider the hold length. Any change to
  H or θ now would be chosen with this knowledge.
- **Fix.**
  1. Write a ledger entry (000e-style) for both windows, quoting the four numbers above.
  2. State in §3 and §11 that these are persistence statistics.
  3. Freeze D5. Close open question 2 as "no change"; a shorter hold is a new trial decided on data after
     2026-07, not on this holdout.
  4. Remove `busy` from everything printed before the guard (print `qualifying_while_free` only).
  5. The owner decides whether the holdout's CEX–CEX persistence knowledge downgrades the holdout read to
     "supporting", with the sealed span as the confirm.

### B2. OKX legs are survivorship-filtered: every OKX instrument delisted before the fetch is silently ineligible.

- **Where.**
  - §2, lines 99–106: "Universe (point in time) … delisted included".
  - D1.
  - `r010_fetch.py:453-456` (OKX `history-candles` 1D/4H, which do not serve delisted instruments).
  - `r010_run.py:153-167` (the name check needs ≥ 3 OK daily closes).
  - `r010_run.py:193` (the entry gap needs OK prices).
- **Evidence (timestamps and existence only).**
  - Of 435 OK venue-coins, **82 stop settling before 2026-06. All 82 have zero rows in `levels.parquet`.**
  - 61 of them were live in discovery and have zero rows in `prices.parquet`.
  - Of 210 OK coins with discovery funding, **68 have no OK price at all**.
  - Bybit is clean: only 1 of its 129 delisted coins lacks levels.
- **Effect.**
  - About 29 % of discovery OK coins can never form an OK pair, because they later died on OKX. That is a
    selection on future information.
  - OK pairs are 40 % of T1 (1,940 positions).
  - The excluded coins are the ones heading for delisting, with forced closes, so the bias most likely
    favours the hypothesis.
  - The same filter applies to the holdout counts.
  - The §3 statement "Without the entry-gap requirement 73 more" does not reveal it, because the name check
    still bites.
- **Fix.**
  - Source OK levels and prices from a store that keeps delisted instruments (OKX's static market-data
    files, end-bounded). Otherwise, make the OK name check fall back to a funding-only rule.
  - If neither works, drop OK legs from the primary and report them.
  - Correct §2 and D1.

### B3. Leg liquidation is costed at the maintenance margin. The real loss is the short leg's whole margin, net of the long leg's gain at the bar close: roughly 2× to 30× larger.

- **Where.**
  - §4 "Rebalancing": "liquidates it … loses the short venue's maintenance margin mm".
  - `ct/carry.py:315-317` (`liq_cost = MM[...]`), with the gap P&L still taken as if both legs closed
    together (`:318-328`).
  - `tests/test_carry.py:216-219` asserts `liq_cost == MM`.
- **Arithmetic.**
  - A 1× short with margin N is liquidated at u* = (1 − mm)/(1 + mm). It loses ≈ N, and the remaining
    equity goes to the liquidator.
  - The long is closed at the bar close c, gaining (c/ref − 1)·N.
  - The net is −N·[1 − (c/ref − 1)]. That is ≥ 2mm/(1 + mm)·N if c is at the high, and about −0.8 N for a
    wick that closes at +20 %.
  - The model charges 0.10 N (HL alt) or 0.025 N (CEX alt).
  - At 1× on N, one liquidation of up to 0.4 of gross capital equals the required edge of about 300
    positions (3 %/yr × 17 d ≈ 0.14 % of gross each). A handful of meme-coin squeezes can decide T1.
- **Fix.**
  - On liquidation, the short leg's P&L is −N (its whole margin) and the long leg's is the price change to
    the liquidation bar's close. The gap term is dropped for that position.
  - Re-test, and re-hash.
  - Also see m1: the HL mm is understated.

---

## MAJOR

### M1. The binding criterion is a point estimate at the benchmark, and the precision behind it is the optimistic model's.

- **Where.**
  - §5 criterion 5.
  - `ct/carry.py:386`.
  - §3 "The binding criterion is 5 …".
  - D12: "80 % power at a true net edge of ≈ 3.3 %/yr".
- **Problem 1: the bar.** P(pass | true = 3 %) ≈ 0.5 by construction. The criterion therefore never controls
  the error of the claim that matters, "beats HLP".
  - Bonferroni (z = 2.24) protects only "mean > 0", which passes at any edge ≥ 2 %.
  - "Either T1 or T2 passes" roughly doubles the boundary false-pass rate.
- **Problem 2: precision.** The 0.3 %/yr SD rests on ρ = 0.071, an *unconditional* contemporaneous level
  correlation with a weekly factor.
  - Selected positions share direction: HL is short in 1,354 of 2,020 HL-pair positions.
  - They also share monthly funding regimes (2023-Q4, 2024-03 peak, 2024-08 spiral).
- **Synthetic probe.** 4,867 positions, 19 months, `carry.criteria` unchanged, a month-level common shock
  σ_m on the annualised edge.

  | σ_m | annualised SD | P(pass) at true 1 % | at 2 % | at 3 % | at 4 % |
  |---|---|---|---|---|---|
  | 0 | 0.2 %/yr | 0 | 0 | 0.55 | 1.00 |
  | 6 %/yr | 1.4 %/yr | 0.06 | **0.20** | 0.41 | 0.72 |
  | 10 %/yr | 2.3 %/yr | 0.05 | 0.16 | 0.25 | 0.38 |

  - Fat tails (t3) leave this about unchanged.
  - Criterion 1b is what binds when month shocks exist. **It is a valid tightening, not creep: keep it.**
- **Fix.**
  - Make criterion 5 a test of the *excess*: mean of (R − 3 %·hold_years) > 0 with a month-clustered t ≥ the
    Bonferroni z, or a lower confidence bound ≥ 3 %. Alternatively, keep the point bar and state P(pass) ≈ 0.5
    at 3 % and up to 0.2 at 2 %.
  - Restate D12's power claim as conditional on the weak-dependence model.
  - Compare the 19-cluster month t with t₁₈ (2.10), not 1.96.

### M2. The HL-pair price-gap model has an unmeasured conditional bias.

- **Where.**
  - §4 "Price gap" (p_HL − p_CEX).
  - §3 "Price-gap model error".
  - `r010_run.py:169-177`.
- **The mechanism.**
  - Both the HL premium and Binance's premium index are *impact-price* premia, clamped to 0 while the
    reference sits inside the impact bid/ask (scout §D: "impact price vs oracle"; Binance: "impact bid/ask
    vs index"). The HL value is also an hourly average.
  - Entries are selected exactly when one venue's premium has left the dead zone. Exits are typically back
    inside it.
  - The model error is therefore conditional on selection, of the order of the impact half-spread. It is
    not zero-mean noise.
- **What was measured.**
  - The 1.3–1.9 bp check is unconditional, core-only, in 2025–26.
  - The alt figure (7–11 bp) is an analogy from CEX pairs.
  - HL pairs are 42 % of T1 positions and 99 of 121 T2 positions.
  - A 5–10 bp mean bias on N moves T1's annualised figure by ≈ 0.5–1 %/yr, against a bar it must clear by
    0.0 %/yr.
- **Fix.**
  - Check whether HL `candleSnapshot` with an `endTime` bound serves 8h/1d last-trade candles for 2023–24.
    The 5,000-candle limit covers about 4.5 years at 8h. If it does, use last-trade closes for HL as for the
    CEX pairs, and the HL name check (§7 "HL has no level history before 2025") becomes possible too.
  - Otherwise, decide HL pairs on a funding-only P&L plus a stated gap haircut, and report the modelled gap
    separately.

### M3. Slippage for alts is not supported for the discovery period or the entry state.

- **Where.** §4 costs, D4 ("2 bp others"), and `ct/carry.py:47`.
- **Problem.**
  - The scout's proxy is the gap between opposite taker prints within 1 s. It is measured on HL in 2025–26
    (where it "collapses to one tick") and on Binance on cascade days. It is not impact at $10k.
  - It is not for 2023 HL alts.
  - It is not conditional on dislocations, which is when entries occur.
  - Each +1 bp per fill costs ≈ 0.44 %/yr of annualised net at a 17-day hold. Five bp per fill instead of
    two moves the result by ≈ −1.3 %/yr.
- **Fix.**
  - Use a period-matched impact estimate: Binance `bookDepth` 2023–24 is free (scout §G).
  - Alternatively, tier slippage by the liquidity floor's volume, and require the pass to survive
    "slippage × 2" (§6). The `r010_run.py:275` adjustment also omits the extra slippage in rebalances.

### M4. The holdout read is not pre-built, and its confirm rule is looser on dependence.

- **Where.**
  - D13 ("fetch …, rebuild the panel, new hash table").
  - `r010_run.py:8-10` ("holdout mode … added by amendment after a discovery pass").
  - `ct/carry.py:435-450`.
- **Problem 1: code written after the result.** The holdout runner, its HL-hold fetch, the entry-gap rule
  for the holdout and the price pull are all written *after* the discovery result is known. That leaves
  forking paths.
- **Problem 2: dependence.** `holdout_confirm` has no month-clustered criterion, although the
  overlapping-hold argument behind 1b applies equally.
- **Fix.**
  - Build and synthetic-test `--holdout` now. Hash it in §8, and pre-specify the exact fetch commands.
  - Add a month-clustered t to the confirm rule.

---

## MINOR

- **m1. HL alt maintenance margin is not "the conservative end".**
  - Where: `ct/carry.py:53` (`('HL','other'): 0.10`); §3 line 217; §4 table line 365.
  - Scout B4 gives 10–16.7 % for HL small caps (5×–3× max). The conservative end is 16.7 %, a distance of
    71 %, not 82 %.
  - Fix: use 1/(2 × max leverage) per coin, or 16.7 %.
- **m2. Registration and code disagree on the exit-gap lag.**
  - §4 says "latest at or before t_x **within 24 h**". `ct/carry.py:322-323` searches back to entry
    (`max_lag=max(DAY, t_x - t_in)`).
  - A broken exit can fall back to the entry gap, which sets the gap P&L to 0 and hides a divergence loss.
    This includes delisting forced closes (`:311-312`), where the last pre-delisting print is taken as the
    close.
  - Fix: align the two, and count fallbacks > 24 h as missing-at-worst in a reported block.
- **m3. The funding weight is not reset at a rebalance.**
  - `price_weight` (`ct/carry.py:281-290`) keeps P(T)/P(t_in) after "both legs closed and reopened" at N
    (`:265-266`), which overstates funding after an up-move.
- **m4. The guard's (f) check reads the working-tree `results.jsonl` only.**
  - Where: `r010_run.py:76-85`; the same in `l7_run.py:88-97`.
  - A reverted or deleted row passes.
  - Fix: also scan `git show HEAD:results.jsonl` and `git log -p -S '"registration": "010-'`.
  - Record the duckdb and pyarrow versions in the result row.
  - Otherwise the guard is sound: status, (a)–(e) and hashes match the tests. Nothing computes an outcome
    before `guard()`. Every Panel read filters `t < end`, and prices are asserted discovery-only.
- **m5. The OKX premium definition differs from the other venues.**
  - `r010_build.py:170`: OKX prem = ln(last/index), while BN and BY use impact-price premium indices. HL–OK
    gaps therefore mix definitions.
- **m6. The placebo pass share is uninformative.**
  - With costs kept it is ≈ 0 by construction (§5). Only `gross_abs_share` carries information.
- **m7. Universe tie-break uses the holdout.**
  - `r010_build.py:92` breaks duplicate venue-symbol claims by Binance funding months to 2026-06, which
    includes the holdout. This is trivial, but it is future information.
  - HL names (kDOGS, kNEIRO) stay unchecked. M2's candles would fix it.
- **m8. The look-ahead claim needs one qualification.**
  - No signal look-ahead was found. The window [t − 24 h, t) excludes the print at t (`ct/carry.py:148-150`,
    tested at `tests/test_carry.py:85-110`).
  - HL, Binance and Bybit stamps are settlement times, and each rate is fixed by its stamp.
  - Under OKX's pre-2024 cross-period collection, the rate at any stamp was known no later than the stamp
    under either convention. The risk is a one-interval *accrual* misattribution, not a signal leak.
  - However, eligibility does read the HL print *at* t (its premium, `r010_run.py:176,193`). It is known at
    t, but §4.1's "the settlement at t is never read" should say "the rate at t".
- **m9. §11 disclosure is incomplete.**
  - Missing: the B1 counts.
  - Missing: `abs_signal_at_entry` quantiles (levels, acceptable).
  - Missing: `hl_model_error` loading the scout's full core HL file through 2026-06 (`r010_describe.py:143-145`;
    only premiums at 264 hours were used).
- **Acceptable holdout reads.**
  - The holdout spread SD and quantiles, ρ, daily closes, the HL archive gap levels and the entry counts
    are levels or unconditional counts.
  - None of them sets a parameter: every parameter traces to fees, the scout's cost proxy, or the HLP
    benchmark.
  - The exception is the `busy` split (B1).

---

## Verdict

**Not registrable as written.** The arithmetic design is clean: H and θ come from costs, the signal has no
look-ahead, and the guard is sound. But three items must be fixed before hashing:

- **B1:** the persistence leak, which also touches the holdout. It needs a ledger entry, a frozen hold, and
  an owner ruling on the holdout's status.
- **B2:** OKX survivorship.
- **B3:** the liquidation loss.

M1–M4 should be resolved or explicitly accepted by the owner in §9. M1 especially: as drafted, "pass"
means "point estimate ≥ 3 %", which happens about 20 % of the time at a true 2 % under plausible regime
dependence.
