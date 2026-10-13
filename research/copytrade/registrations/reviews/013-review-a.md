# Independent review A of 013 DRAFT (short new Binance perps), 2026-10-13

Reviewer brief: an adversarial review of `registrations/013-new-perp-listings.md` (DRAFT),
`ct/listings013.py`, `r013_fetch.py`, `r013_build.py`, `r013_run.py` and `tests/test_r013.py`. The
emphasis is on design, statistics, economics and the tail. Every existing file was read-only.
`r013_run.py` was never run on real data, and no guard function was called against the real tree.
Nothing was fetched.

**What was run.**
- `.venv/bin/python -m tests.test_r013` printed **"all checks passed (124)"**. The §10 hash table
  matches the files on disk.
- Nine synthetic probes in `research/copytrade/scratch_review013a/`. This is a new directory, and
  nothing else was written. Every return in them is simulated. The real input is the events'
  first-trade times (coverage, see below).

  | probe | what it checks |
  |---|---|
  | `p0_structure.py` | listing-week structure; the book's cap count from first-trade times |
  | `p2_overlap.py`, `p2b_remedies.py`, `p2c_hh.py` | size of the listing-week CR1 t when 7-day holds overlap, and candidate fixes |
  | `p3_squeeze.py` | size of the t with a −100% squeeze atom, independent and clustered |
  | `p4_beta.py`, `p4b_power_alpha.py` | β ≠ 1 worlds; the raw t, the β = 1 adjusted t, and the OLS α with estimated β; size and power |
  | `p5_path.py` | the registered decision path end to end (the module's own `discovery_criteria`, `decide_discovery`, `confirm`, `deploy_bar`, `book`) in β and decayed-edge worlds |
  | `p6_tail_bound.py` | the book's structural worst week and month; the liquidation threshold through `simulate` |
  | `p7_power_repro.py` | an independent re-implementation of §3's model and criteria (own t-quantile, own clustered t) |
  | `p8_placebo.py` | whether the registered placebo can see the overlap mis-calibration |

**Reviewer exposure (for the ledger).**
- Documents: the brief and the round-3 programme document (§1–§3); the N3 scout; `000f`, `000i`,
  `000l`; 009 §0 and the D-table; grep reads of 010 and 012 for listing-age rules; `docs/alpha-lab-spec.md`,
  the 2026-09-14 entries (Bitget cross-sectional ICs, 2024-03 → 2026-09); `reviews/008-review-a.md`.
- Public knowledge, not computed: the 2025-10-10 liquidation cascade, and the broad crypto decline from
  2025-Q4 into 2026. Both fall inside 013's confirmation calendar. The programme's brief itself calls
  2025-07-28 → 2026-06-30 "the bear year" ("What is established", items 1 and 6).
- Real-data reads: listed at the end. **No price, return, funding value, liquidation count or P&L was
  read or computed on any real window. Nothing dated on or after 2026-07-01 was read.**

---

## 0. Answers to the brief, in short

| probe | finding |
|---|---|
| Trade realism: venue | The test is a **Binance** result, and Binance carries 100% of the events. Bitget had at least 66/131 (2024) and 143/239 (2025) listings by day 1; HL had 45/239 in 2025. Funding regimes differ, and Bitget has no free funding archive. A Bitget or HL deployment is not covered (m2). |
| Costs | Fees are right. The slippage tiers are keyed on the **busiest hour of the token's life** (day 0, hour 1), while the fills happen on day 1 and day 8. The tiers are probably optimistic at exit, but the 2× variant likely brackets them, and criterion 6 needs 100 bp (m3). |
| Funding | Sign, notional (rate · P_s / P_in), boundaries and stress are correct (sound). |
| Liquidation | Threshold verified: h* = 1.9037 × P_in with no funding (fee 6 bp, slip 5 bp); `simulate` exits at a high of 1.9030 and liquidates at 1.9045. Last-price high and MMR 5% are conservative for the decision (sound). |
| Delisting, survivorship, ADL | Sound. The archive keeps delisted symbols, the fallback keeps dying listings, and ADL only truncates winners. **Ticker reuse** is unresolved (m5). |
| Tail table | **Wrong against the code's own book.** The structural worst week is −24%, not −12%. The structural worst month is −57%, not −45%. With carried-in positions, the "plausible" month passes the −30% line at c = 3% (M3). |
| +15%/yr at 4% | The arithmetic is right: 196/253 events booked = 126.7/yr, and 126.7 × 3% × 4% = 15.2%. Kelly arithmetic is right. |
| Deploy-size rule | **Text and code disagree.** The text uses discovery's Kelly and worst months; the code uses confirmation's books (M4). |
| Overlap and clustering | **Not "mildly" anti-conservative.** CR1 size is 0.042 in discovery (nominal 0.025) and 0.079 in confirmation (nominal 0.05). The placebo cannot detect this (M1). |
| β = 1 and the raw-net confirmation | **BLOCKER (B1).** The confirmation is a raw-net test of a short book on the programme's known bear year. In a decayed-edge world with an alt drift of 1–2% a week, it falsely confirms 0.13–0.24, and `confirm_paper` follows 0.09–0.17. Criterion 5 at β = 1 lets 0.11–0.33 of β-only worlds through when β is 1.5–2. |
| Fat tails | Independent squeeze atoms push the confirmation's size to 0.06–0.12. Clustered squeeze weeks push it to 0.26: 30% of 39-week samples contain no squeeze week (M2). |
| Family rule | Valid. The confirmation runs a single test, and the discovery Bonferroni is conservative for a nested T2. Power was reproduced independently: 0.861 / 0.751 / 0.881 / **0.630**, against 0.87 / 0.76 / 0.90 / **0.63**. |
| T2 | Known at entry (sound). But Binance funding has a **positive interest floor**, so "Σ ≥ 0" means "premium never materially negative". T2 is probably most of T1, and the mechanism text is loose (m1). |
| Calendars and priors | No look-ahead, and the embargo holds. §0 omits two earlier looks at the confirmation calendar (m6). D17(b) has no deadline (m8). |
| Criteria | No two-point-bar killer inside discovery. The binding constraint is the raw-net confirmation, whose variance is dominated by the market. **Market-adjusting it raises power from 0.29 to 0.85 at μ = 4%, k = 2**, as well as fixing B1 and M1. |

---

## BLOCKER

### B1. The confirmation tests the raw net of a short book on a calendar the programme already knows was a bear market, and the only market control is β = 1.

**Where.**
- `ct/listings013.confirm` (lines 488–493) tests `s['mean']` / `s['t']` of the **raw net**, with no
  market term. §7.2 and D11 say the same.
- Criterion 5 (`discovery_criteria`, line 441) and the deploy bar (`deploy_bar`, line 500) use
  `net + market` with β fixed at 1. The deploy bar applies only a **sign** to it.
- §3 states the operative α as "the confirmation's own false-confirm rate, 0.049–0.052". §9 calls
  β = 1 "slightly permissive in a falling market".

**Why it is a blocker.**
- The confirmation calendar (2025-07-28 → 2026-06-21) is exactly the HL year that the brief calls
  "the bear year". It contains the 2025-10-10 cascade, which is public. A 1× short book earns the alt
  market's fall times its β. Nothing in the confirmation removes that.
- The draft's own §3 says "a world with an edge in 2024–25 and none in 2025–26 passes discovery almost
  surely, so only the confirmation protects against it". In a bear year, the confirmation does not
  protect against it.
- This is the same failure the orchestrator corrected in 008. `000l` requires that "a confirm … must
  not be attributable to a market-wide [move] already known to have paid in these months", and 008
  review A B1 found the same failure with a mis-specified beta.

**Probes** (`p4_beta.py`, `p5_path.py`).
- Setup: the real first-trade times; a continuous Brownian market sampled at each event's own entry and
  exit, so cross-week overlap is present; idiosyncratic noise t₄.₅; k = 1 scales (market 10.4%, idio
  8.3% per 7 d).
- Model: net = μ + β·u + e, where u is the market fall over the event's hold plus a drift per week.
- `p5_path.py` runs the registered code path end to end.

| world | discovery pass | CONFIRM | `confirm_paper` |
|---|---|---|---|
| decayed edge: μ 4% in discovery; μ 0 in confirmation; β 1; conf drift 0 | 0.795 | 0.058 | 0.045 |
| same, conf drift 1%/wk | 0.795 | **0.128** | **0.088** |
| same, conf drift 2%/wk | 0.795 | **0.241** | **0.151** |
| decayed edge, β 1.5, conf drift 1% / 2% | 0.527 | 0.093 / **0.171** | 0.093 / **0.171** |
| β-only in both calendars, β 1.5, drift 2% (μ 0 everywhere) | **0.249** | 0.071 | 0.071 |
| β-only in both calendars, β 2.0, drift 2% | **0.331** | **0.104** | **0.104** |
| reference: real edge μ 4%, β 1, no drift | 0.795 | 0.507 | 0.507 |

- **Given a carry, the false-confirm rate is 0.16 at 1%/wk and 0.30 at 2%/wk, against the claimed
  0.05.**
- The deploy bar's "market-adjusted mean > 0" passes 0.52 under β = 1 and 0.66–0.83 under β = 1.5–2
  with drift (`adj_mean_pos` column of `p4_beta.py`). It is not a control.
- Per-statistic rates, μ = 0 (`p4_beta.py`, 3,000 draws). "Raw" is the registered confirmation
  statistic in the confirmation rows and criterion 1 in the discovery rows; "adj β = 1" is criterion 5:

  | calendar, β, drift/wk | raw t ≥ crit | adj β = 1 t ≥ crit | **OLS α (β estimated)** |
  |---|---|---|---|
  | confirmation, β 1, 0 / 1% / 2% | 0.074 / 0.159 / 0.293 | 0.062 | **0.064** |
  | confirmation, β 1.5, 1% / 2% | 0.165 / 0.309 | 0.140 / 0.245 | **0.064** |
  | confirmation, β 2.0, 1% / 2% | 0.170 / 0.316 | 0.159 / 0.293 | **0.064** |
  | discovery, β 1.5, 1% / 2% | 0.147 / 0.347 | 0.106 / 0.254 | **0.025** |
  | discovery, β 2.0, 1% / 2% | 0.148 / 0.358 | 0.141 / 0.325 | **0.025** |

**Fix (recommended; cheap, and it raises power).**
1. Replace the confirmation statistic with the **intercept α of a pooled OLS** of the event's net on
   its own market return over its own hold (the series `Panel.market` already computes): net_i = α +
   β·(−mkt_i) + ε_i. Use listing-week CR1 and the critical value t_{G−2}(0.95). The probe's version is
   `ols_alpha_t` in `p4_beta.py`.
   - It holds its size in every β and drift world above: 0.064 in confirmation, 0.025 in discovery.
   - **Its power is far higher**, because the market variance leaves the denominator
     (`p4b_power_alpha.py`, confirmation, μ = 4%):

     | scales | raw | OLS α |
     |---|---|---|
     | market 20.8%, idio 16.6% (k 2) | 0.29 | 0.85 |
     | market 10.4%, idio 16.6% | 0.53 | 0.85 |
     | market 10.4%, idio 24.9% | 0.42 | 0.59 |

   - It answers §12 Q3: β is estimated from the time variation of the market across events, so no
     pre-listing history is needed.
2. Use the same α t for **criterion 5** (t_{G'−2}(0.975)), and the α mean > 0 in the deploy bar in
   place of the β = 1 sign.
3. Keep the raw net as **reported**, and as the economic bars (100 bp; book ≥ 3%/yr). The economic
   question is what the book earns.
4. Add "mean > 0 without the listing week with the largest positive sum" to the confirmation, as in
   discovery criterion 4. The 2025-10-10 cascade week alone can carry a raw short book.
5. Make the market index robust (see m5) before it decides anything.

---

## MAJOR

### M1. Overlapping holds make the listing-week CR1 t clearly anti-conservative, and the placebo cannot see it.

- **Measured** (`p2_overlap.py`, 4,000 draws, μ 0, β 1, the real first-trade times, a continuous
  market against an independent market path per listing week):

  | calendar | size with overlap | size, no overlap (§3's assumption) | SD(t) with / without |
  |---|---|---|---|
  | discovery, t₆₅(0.975) | **0.042** (nominal 0.025) | 0.031 | 1.16 / 1.03 |
  | confirmation, t₃₈(0.95) | **0.079** (nominal 0.05) | 0.056 | 1.17 / 1.06 |

  The size is invariant to scale, so it is the same at k = 1 and k = 2. A heavier market (15% against
  8.3% idio) gives 0.045 / 0.082.
- **Overlap is the norm, not an edge case.** 56 of 65 adjacent discovery listing-week pairs are
  consecutive ISO weeks (31 of 38 in confirmation; `p0_structure.py`).
- **Standard fixes do not rescue it at G = 39** (`p2b_remedies.py`, `p2c_hh.py`):
  - 2-week block clusters: 0.040 / 0.074;
  - Driscoll–Kraay lag 1 (Bartlett): 0.038 / 0.079;
  - Hansen–Hodrick lag 1 (weight 1): 0.035 / 0.080.

  The estimator noise eats the correction.
- **The placebo cannot report it**, against §9's claim ("The placebo reports the calibration"). Week
  sign flips randomise adjacent weeks independently, which destroys the very covariance that matters.
  On 30 synthetic overlap samples, `placebo` reports a flipped-t SD of **1.065**, while the true SD is
  1.17 (`p8_placebo.py`).
- **§3's simulator** also drops the non-shared third of the common variance. The √(2/3) factor scales
  the whole common part, so the per-event SD is 11.9%, not the 13.3% assumed in §6. It also has no
  cross-week link. Its nulls and power are therefore mildly optimistic: criterion 1 has power 0.78 in
  my overlap model against 0.88 in §3, at μ = 4%, k = 1.
- **Fix.**
  - B1's α test removes the common component that carries the overlap. Its confirmation size is
    0.059–0.064, a residual from CR1 with 39 unequal clusters and fat tails.
  - State the operative size from a simulation with the real event times and a continuous market.
  - Delete the placebo-calibration sentence, or add a month-block flip placebo.

### M2. The squeeze tail is a peso problem for a 39-week confirmation: the t over-rejects, and the deploy decision prices a tail the sample may not contain.

- **Measured** (`p3_squeeze.py`): a mean-zero null in which an event returns −1 with probability p,
  and otherwise a + σ·t₄.₅ with a = p/(1−p).

  | calendar | σ_e 8.3%, p 1% / 2% / 4% | σ_e 16.6%, p 1% / 2% / 4% | clustered: every event in a week liquidated w.p. 3% per week |
  |---|---|---|---|
  | discovery, crit 0.975 (nominal 0.025) | 0.057 / 0.069 / 0.058 | 0.033 / 0.038 / 0.040 | **0.174** |
  | confirmation, crit 0.95 (nominal 0.05) | 0.121 / 0.125 / 0.103 | 0.063 / 0.083 / 0.075 | **0.263** |

  P(no liquidation in 126 events) is 0.29 at p = 1%. **P(no squeeze week in 39 weeks) at 3%/week is
  0.30.**
- §6's "plausible worst month" (half of a month's entries liquidated) **is** a clustered squeeze. The
  draft pre-registers that it happens and then decides on a t that is blind to it when the sample
  misses it. The mandate's rule is "a risk premium is acceptable if it is priced; an unpriced tail is
  not".
- **Fix.**
  - Price the tail from the **longest sample**. The deploy bar should use the liquidation rate and the
    worst months pooled over the reported 2020–23 block, discovery and confirmation, not over 39 weeks.
  - Require the book to clear 3%/yr **after** charging the pre-registered plausible squeeze month at a
    stated frequency (for example, once a year, unless the pooled sample says otherwise).
  - Size the deploy rule on the worst of the observed months over all calendars and the pre-registered
    plausible month.

### M3. The pre-registered tail bounds understate the code's own book by 2× (week) and 12 points (month).

- `book()` caps entries by **entry** ISO week and month, but books P&L in the **exit** week and month.
  Liquidations land inside the hold. So one exit week can hold four exits from last week's entries and
  four liquidations of this week's entries.
- `p6_tail_bound.py`, through `listings013.book`:
  - **worst_week −0.24**, against the draft's "−12% structural bound";
  - **worst_month −0.57**: 4 carried in from the last ISO week of the prior month, plus 15 this month.
    The draft says "−45% structural".

  §6 itself says "at most about 24% of C is at risk at once".
- **The plausible month is also understated.** The −28.1% (15 × [0.5·(−1) + 0.5·(−0.25)] × 3%,
  arithmetic right) ignores up to 4 positions carried in. Adding them at the same mix gives
  19 × 0.625 × 3% ≈ −36%, beyond the −30% line at c = 3%.
- The book reports **realised** P&L at exit only. A mark-to-market week (8 open shorts in a +50% alt
  week ≈ −12% unrealised) is not reported.
- **Fix.**
  - Correct the tail table: week −24%, month −57% structural, plausible ≈ −36%.
  - Set c so that the corrected plausible month is ≤ −30% (about 2.5%), or cap the month at 12
    entries ((12 + 4) × 0.625 × 3% = −30%).
  - Report a daily mark-to-market drawdown at the run, which the 1m bars already allow.

### M4. The deploy-size rule is defined differently in the text and in the code.

- **Text** (§6, D13): "¼ Kelly from discovery, and the size at which the worse of **discovery's**
  observed base and stressed worst months equals −30%".
- **Code.**
  - `run_confirmation` (lines 434–436) builds `bb` and `bs` from the **confirmation** rows and passes
    them to `deploy_bar → deploy_size`.
  - `run_discovery` (line 407) also computes a `deploy_size_per_event` from `reported_blocks`, which
    uses **T1** rows, even when T2 is the carried trial.
- The deploy bar's "book ≥ 3%/yr" is evaluated at **c = 3%, not at c\***. A confirm with c\* = 1%
  passes a bar its deployed book would miss threefold.
- **Fix.**
  - Pick one source. My recommendation is the minimum over discovery and confirmation for the
    worst-month term (see M2), and confirmation's Kelly, because the discovery Kelly is
    selection-inflated after a pass.
  - Make the code match the text. Compute it for the carried trial only.
  - Evaluate the economic bar at c\*.

---

## MINOR

- **m1. T2's threshold sits on Binance's interest floor.**
  - Binance funding is F = P + clamp(I − P, ±0.05%), with I > 0. Any premium within about ±0.05% of I
    therefore prints the positive default rate.
  - "Σ rates ≥ 0" thus means "no material discount over the first day", not "longs pay". T2 is
    probably a **large majority** of T1, so it adds little and Bonferroni charges T1 for it: t₆₅(0.975)
    = 1.997 against 1.669 at 0.95.
  - It is still known at entry (sound): onboarding rows are excluded, and the entry-minute settlement
    is in the feature and out of the hold, consistently.
  - **Fix:** restate the mechanism in premium terms. Either keep T2 and accept the cost, or demote it
    to a reported stratum and test T1 alone at 0.95. A nested-subset family critical value from
    simulated max-t is a middle path.
- **m2. The venue.**
  - The result is a Binance result: Binance prices, Binance funding stamps and Binance
    shortability.
  - Bitget is a subset: at least 66/131 and 143/239 listings by day 1, with 65 and 96 unknown
    (survivorship).
  - Funding regimes differ: HL is hourly with a 4%/h cap, and Bitget has no free archive. Day-1 basis
    adds noise.
  - **Fix:** D12 names Binance as the paper and deploy venue. Any other venue is a new trial. The 6 bp
    decision fee can stay as a conservative cost.
- **m3. Slippage tiers.**
  - The tier variable is the quote volume of the **first hour of day 0**, the most liquid hour the
    token will have. Entry (day 1) and exit (day 8) typically run at a fraction of it.
  - For the median 2024 listing ($11 M first hour against about $4.7 M/h on day 0 overall), the
    8 bp exit tier is likely low by 2–3×.
  - The ×2 stress and the 100 bp criterion probably absorb this.
  - **Fix:** tier on the trailing-24 h quote volume at entry, which is known at entry, and charge the
    exit one tier worse. Or calibrate unconditionally from free bookDepth (§12 Q4); a level is not an
    outcome.
- **m4. The power simulator's book** applies the week cap only: 206 events in discovery, against 196
  with both caps. It is immaterial for criterion 7 but untested.
- **m5. Ticker reuse and the market index.**
  - The scout and §4 say PUMPUSDT 2025-04 and pump.fun's PUMP of 2025-07 are different tokens. Yet
    PUMPUSDT's 1d keys are continuous from 2025-04-12, and its 1h rows count 440 / 744 / 720 / 643 in
    2025-04 … 07 (row counts only; no price read).
  - If an underlying can change under a continuous ticker (the scout lists 11 reset-onboardDate
    tickers: CTK, CVC, LIT, TLM, SLP, CVX, MAVIA, BNX, AIA, ICP, PUMP), two things follow:
    - (a) a genuine new-token listing is silently not an event;
    - (b) the **equal-weight simple-return** index books the token switch as a return. One symbol's
      ×k jump adds (k − 1)/N, which can be tens of percent for N ≈ 300, inside criterion 5 and B1's
      α.
  - **Fix:** use the cross-sectional **median** (or a 5% trimmed mean) as the market return. Examine
    the 11 tickers by coverage (gaps, row counts), and decide inclusion before registration.
- **m6. §0's exposure table omits two earlier looks at the confirmation calendar.**
  - (a) Registrations 001–007 established its direction: the brief's "bear year".
  - (b) The 2026-09-14 alpha-lab sweep: Bitget, 83 perps, 4H bars from 2024-03 to 2026-09, with
    full-panel reversal, momentum and low-vol ICs. The panel includes the early days of the surviving
    2024–25 listings.
  - Both are aggregate. (a) matters for B1. State them, and ledger (a) alongside `000l`.
- **m7. Text inconsistencies.**
  - §7.1 says a β-only world passes criterion 1 "about 72%", while §3 and the log say 0.63 / 0.22.
  - The `_sim_calendar` docstring says u ~ N(−drift, …), while the code uses +drift.
  - §9's placebo-calibration claim is wrong (M1).
- **m8. D17(b) has no deadline.** After a discovery pass with neither an approval nor a `DECLINED`
  line, the confirmation calendar is locked for every other registration indefinitely. Add a default
  (`DECLINED` after N days) or a date.
- **m9. Internal data holes.**
  - One confirmation event has a 1,440-minute internal gap. Liquidation is not checked across it, and
    if it covers the exit target the fallback exits up to a day early at a stale close.
  - It is counted, and it is one event. State that a hole with bars after it is a data hole, not a
    stop. The reported-block event has a 660-minute gap.
- **m10. Expectations wording.** "About 1 in 2 that a discovery pass is then confirmed" is above the
  draft's own power table at k = 2 (0.14–0.69 given a carry, with k = 2 called the headline). With
  B1's fix it becomes defensible.

---

## Checked and found sound

- **Liquidation.** The formula and its threshold were verified through `simulate` (above). Funding
  enters the margin before each bar. A liquidation returns exactly −1 with components that add up.
  Last-price highs and MMR 5% only add losses, so both are conservative for a pass. They overstate the
  observed tail, which §9 states.
- **Funding.**
  - A positive archived rate is a receipt for the short. Notional is taken at the settlement price,
    and the stress applies ×2 to payments and ×0.5 to receipts, capped at ±2%.
  - Each boundary settlement is counted once: the entry minute is out of the hold and in the T2
    feature; the exit minute is in the hold.
- **Book arithmetic.**
  - From the first-trade times: 196 of 253 events, 126.7 a year (discovery); 98 of 126, 108.8 a year
    (confirmation).
  - 127 × 3% × 4% = 15.2%/yr.
  - Full Kelly ≈ 4μ / (10σ²) (n 4, ρ 0.5); ¼ Kelly ≥ 2.8% at μ ≥ 2%, k ≤ 2.
  - The −28.1% plausible-month arithmetic is right; the scenario is not (M3).
- **Power reproduced independently** (`p7_power_repro.py`, own code throughout).
  - μ 4%, k 1: T1 0.861, T2 0.751, any 0.881, **joint 0.630**, against 0.87 / 0.76 / 0.90 / 0.63.
  - Null: 0.002 / 0.006 / 0.007 / 0.
  - Own t quantiles: t₆₅(0.975) = 1.9971, t₃₈(0.95) = 1.6860, matching `L.t_crit`.
- **Family and carry rule.** Exactly one test at confirmation, so there is no confirmation
  multiplicity. The discovery Bonferroni is valid, and conservative for a nested, correlated T2.
- **Calendars and embargo.**
  - Every hold ends before its calendar's data end. `Panel.market` asserts the exit hour is before the
    end.
  - The build filters every monthly file by span (2025-07's 1h file is split at 07-28).
  - The discovery mode cannot open the confirmation panel (tested).
- **Survivorship.** data.binance.vision keeps delisted symbols (010's table: 365). The exit fallback
  keeps listings that die in the hold. Delisting inside 8 days is rare enough that the settlement-price
  proxy is immaterial; it is counted.
- **ADL.** A 1× profitable short ranks low (PnL% × leverage), and ADL only truncates winners. The claim
  stands.
- **Tests.** 124 of 124 pass. The hash table matches. The real guard refuses today.

---

## Verdict

**NOT READY TO REGISTER: one blocker, four majors.**

- **B1 must change before registration.** Decide the confirmation and criterion 5 on the OLS α of the
  net on the event's own market return (β estimated, listing-week CR1, t_{G−2}), with a robust (median)
  market index. That one change also:
  - removes most of M1 (the overlap acts through the common component);
  - raises confirmation power at μ = 4%, k = 2 from 0.29 to about 0.85;
  - answers §12 Q3.
- **M2–M4 are text-and-code corrections to the tail and sizing:**
  - price the squeeze from the pooled calendars;
  - correct the bounds to −24% for the week and −57% for the month, and the plausible month to about
    −36%;
  - set c so that the plausible month is ≤ −30%;
  - give one definition of the deploy size, evaluated at c\*.
- The minors can be handled in the same revision.

---

## Real-data reads by this reviewer (all coverage or schema; no value combined)

| file | what was read | date bound |
|---|---|---|
| `data/r013/panel_discovery/events.json` | `first_trade`, `premarket` and `calendar` per event (listing times, as disclosed in 013 §2.4); `p0_structure.py` stores them in `scratch_review013a/structure.json` | first trades 2024-01-01 → 2025-07-18 |
| `data/r013/panel_confirmation/events.json` | the same | first trades 2025-07-28 → 2026-06-21 |
| `data/r013/panel_confirmation/coverage.json` | printed (counts) | < 2026-07-01 |
| `data/r013/listing/um_daily_1d_days.json` | daily-file dates of 12 symbols (PUMP, MYX, ICP, BNX, AIA, MAVIA, LIT, CVX, TLM, SLP, CVC, CTK) and their segments and gaps | filtered < 2026-07-01 |
| `data/r013/panel_discovery/market_1h.parquet` | **row counts** per month for PUMPUSDT; no price column read | 2025-04 → 2025-07-27 |
| `data/r013/panel_confirmation/market_1h.parquet` | **total row counts** for LITUSDT and AIAUSDT; no price column read | 2025-07-28 → 2026-06-30 |
| `data/r013/logs/power_listing.txt` | the drafter's synthetic power log | — |
| `results.jsonl` | each row's registration, mode and decision (as the drafter did) | — |
| `tests.test_r013` (run) | reads panel column names and file hashes (schema) | — |

Nothing dated on or after 2026-07-01 was read. No paid, keyed or requester-pays source was touched.
Nothing was published or uploaded.
