# 009 — Weekly cross-sectional funding carry on Binance USDⓈ-M perpetuals, 2021-12 → 2023 (L4, F1)

**Status: REGISTERED 2026-10-11** (owner, 2026-10-11: "set to registered and proceed with recommendations"). Previously DRAFT — Revision 3, written 2026-10-11 after the independent
review (`registrations/reviews/009-review.md`), for the owner's approval of
the decision table (§9, every row PROPOSED).

- The discovery data are pulled and verified (§2).
- Mechanics are built and checked on synthetic data (§8). That includes
  the holdout-A mode and its pull, built now and hashed.
- **No return has been paired with a score, and no IC or book P&L has
  been computed on real data** (Revision 4 wording).
  - Three outcome-adjacent behaviour statistics were read during drafting.
    §0 names them; two are ledgered in `000g`, and the third needs the
    orchestrator's addition there.
  - The trailing-week returns used as unconditional distributions are
    earlier signals' outcome windows (§0).
- The runner (`r009_run.py`) refuses until all of these hold:
  - this file says REGISTERED, is committed and is unmodified;
  - the code and data match §8, including the pull code, the listing and
    the holdout plan;
  - `results.jsonl` has no 009 row of any kind, in the working tree or in
    the history of any branch.
- Nobody but the owner changes the status line.

## Revisions (all before any outcome)

**Revision 4 (2026-10-11, after second review,
`reviews/009-review-r3.md`).**

1. **M1, the holdout-A guard (code and tests).** It now requires ALL of:
   - (g1) exactly one 009 discovery result row, across the tree and every
     branch (`git log --all`);
   - (g2) the row is committed in HEAD;
   - (g3) its registration commit is an ancestor of HEAD, and the guarded
     code is unchanged since, checked by `git diff` and the code hashes
     stored in the row;
   - (g4) every criterion, both pull conditions and the decision are
     **recomputed from the stored numbers**, never from a label
     (`factors.recheck_discovery`);
   - (h') a line `Holdout A: APPROVED <date> row <sha256 of the row line>`,
     committed **strictly after** the commit that added the row;
   - (i) no holdout row anywhere.

   Further changes:
   - The reviewer's four bypasses are tests, and all are refused.
   - **m10:** a `discovery_started` row (and a `holdout_A_started` row) is
     appended before any outcome is computed. A crash cannot silently
     re-run.
   - An owner-decline mode (`--decline-holdout-A`) writes a terminal row.
2. **M2.** `r009_pull.py`, the four listing CSVs, `fapi_exchangeInfo.json`
   and the holdout `plan.csv` are in the guarded set and in §8.
3. **M3.** The holdout run computes the 2× slippage book, the stress tiers
   and the beta-adjusted NW t. **D14 CONFIRM adds "2× slippage net mean
   > 0"**; the others are reported.
4. **M4.** §0 discloses `data/scout_l2/binance/funding` and
   `data/binance/um`.
   - **`checked` is a whitelist.** 009 reads only `data/binance_years/`,
     its holdout directory, the hashed listing and exchangeInfo, the
     registrations and `results.jsonl`.
   - **D17 is now a programme-wide rule.**
5. **Minors, all fixed:**
   - m1: the intercept of the weekly net on the market is reported, and β
     calibration is stated;
   - m2: text numbers match `describe.json`;
   - m3: "F1 is needed to be scored; the market is the whole universe",
     and the placebo shuffles only scored coins;
   - m4: the bridging look-ahead is disclosed;
   - m5: a missing settlement gives no score, or no funding P&L and cash,
     and both are counted;
   - m6: the F2 record is stored in `describe.json`, and the 0.07 bp miss
     is stated;
   - m7: the market-move statistics are disclosed, and "no return"
     overclaims are reworded;
   - m8: D17 unlocks on any terminal 009 state;
   - m9: `--all`;
   - m11: the holdout panel is checked against its manifest, and holdout B
     is reported only;
   - m12: the simulated market is price only, as in the run.

   Power was re-simulated (§3).

**Revision 3a (orchestrator, before re-review).** The beta pull condition
(D13 a) becomes **the beta-adjusted weekly net has mean > 0 and one-sided
Newey–West t ≥ 1.645**. It replaces "mean > 0", which stopped only about
half of the beta-made passes. Power and null rates were re-simulated at
2,000 histories (§3), and a test was added (§8).

**Revision 3 (2026-10-11, after independent review).** Each item answers a
review finding (B = blocker, M = major).

1. **B1, data overlap with 010.** `data/carry/` (010) already holds Binance
   funding and prices covering F1's holdout A and B. This is disclosed in §0
   and ledgered in `000g`.
   - An ordering row (D17): 009 runs before any 010 outcome on Binance data
     dated from 2024-01-01 on.
   - A hard rule, asserted in code and tested: **no 009 code path reads
     `data/carry/`**.
2. **M1, F2 dropped. The family is F1 alone, one-sided z = 1.645.**
   - With drift re-sizing included (what the book really pays), every F2
     variant fails the 15 bp gate, whose intent was the full turnover cost.
   - All four variants are recorded as **rejected on turnover, before any
     outcome**: 7-day value, 7-day contracts, 28-day value, 28-day
     contracts (§1).
   - No further F2 variant is tried.
   - F1's cost, breakeven, required gross and power are recomputed with
     drift: **13.4 bp a week** (§3).
3. **M2, beta.** The book is dollar-neutral, with an ex-ante net beta of
   about −0.10 to the equal-weight universe (§3).
   - The power and null simulations now draw a common market factor.
   - **Holdout-pull condition (D13): the beta-adjusted net mean is > 0.**
     The adjusted net is the net minus the book's point-in-time beta times
     the equal-weight universe return over the hold. The raw net is
     reported beside it.
4. **M3, holdout A is built now.**
   - `r009_run.py --holdout-A` has its own guard: a recorded 009
     discovery row with decision `pull_holdout_A`, the owner's line
     "Holdout A: APPROVED <date>" in this file, and no holdout row yet.
   - `r009_pull_holdout.py` refuses to fetch until that guard passes.
   - Both are hashed in §8.
5. **M4.** Two forward-exposed statistics are disclosed in §0 and
   ledgered (`000g`). The wording "no outcome existed" is replaced by what
   was actually seen.
6. **M5, costs.**
   - **Holdout-pull condition (D13): the net at 2× slippage has a mean
     > 0.**
   - Reported: stress-week tiers taken from the minimum 30-day volume over
     the past 90 days.
   - "Upper bound" for the Abdi–Ranaldo p90 was wrong and is withdrawn.
7. **Minors, all fixed:**
   - no F2 code path remains;
   - the outcome assertion is at the true last exit (2024-01-01 01:00),
     and bars after it are invisible on read;
   - guard (f) scans git history, and `ct/__init__.py` is hashed;
   - the docstring is corrected;
   - F1 is rounded to 1e-10 before ranking, so equal funding rates tie
     exactly;
   - the tie counts are corrected (16 short, 4 long; one 94-name leg);
   - **the universe no longer needs metrics**, and a metrics-archive hole
     in a coin that kept trading is bridged by kline activity (ICP);
   - the argument errors are corrected: carry terminology, the crash/short
     sentence, the σ_IC dependence, the holdout "lower bound", and the
     delisting bias now including funding.

**Revision 2 (2026-10-11, orchestrator, before review).** F2 moved from
OI value to OI contracts. The value version's Spearman with minus the past
28-day return was 0.53; contracts measured 0.20. The economic bar was
stated, and two disclosures were added. Superseded by Revision 3: F2 is
dropped.

**Revision 1 (2026-10-11, orchestrator, before review).**
- The buffered rebalance became the primary book.
- The 7-day F2 was rejected on turnover.
- The 28-day F2 passed a target-only cost gate (14.6 bp).
- Citations were verified online.
- The power simulation's tie bias was fixed.

## §0. What this inherits, and what has been seen

**The hypothesis.** On Binance USDⓈ-M perpetuals, a weekly cross-sectional
**funding-carry** book earns a positive return net of Binance VIP0 taker
fees, liquidity-tier slippage and funding. The book is long the coins with
the lowest trailing 7-day funding and short the highest, dollar-neutral,
equal risk, buffered, rebalanced every Monday. The funding cash flow is part
of the return.

**Literature** (verified online 2026-10-11; the review re-checked the BIS
abstract):

| source | verified | what it supports |
|---|---|---|
| Koijen, Moskowitz, Pedersen & Vrugt, "Carry", *JFE* 127(2), 197–225, 2018 | yes | carry predicts returns across asset classes; **long high carry, short low carry**. No crypto |
| Schmeling, Schrimpf & Todorov, "Crypto carry", BIS WP 1087, 2023 | yes | the BTC/ETH futures-spot basis reaches ~60%/yr; "a high crypto carry predicts future price crashes". Time series, two coins, dated futures |
| He, Manela, Ross & von Wachter, "Fundamentals of perpetual futures", arXiv 2212.06888 | yes | funding anchors perps to spot; background only |
| Chi, Hao, Hu & Ran, *J. Futures Markets* 43(8), 1161–1180, 2023 | abstract yes; **direction unverified** | the basis is the strongest cross-sectional predictor of crypto futures returns (2017–21); weekly factor returns much weaker than daily |
| Basu & Miffre, *JBF* 37(7), 2013 | yes; **removed** | hedging pressure pays the side speculators take; no longer cited for any sign |
| Fan, Jiao, Lu & Tong (SSRN 4666425) | **unverified** | not used |

**The sign argument, stated carefully** (review minor 9).

- **"Carry" means opposite things in the two sources.**
  - In Koijen et al., carry is the return to a *holder* if prices do not
    move. For a perp long, carry is minus the funding paid, so their "long
    high carry" is **long low funding**.
  - In Schmeling et al., "high crypto carry" is the futures premium, which
    the *short-futures* side earns: high funding. Their "high carry
    predicts crashes" supports **shorting high funding**.
  - Both point the same way for F1, through opposite uses of the word.
- **Which story F1 claims.** It is not "the short is paid for bearing
  crash risk": a crash *pays* the short. The short bears squeeze risk.
- The claim is: crowded leveraged longs pay funding beyond what the price
  path gives back. If Schmeling et al.'s crash link holds in the cross
  section, F1's short leg gains on crashes. That would be an anomaly
  (mispricing of crowded longs), not a risk premium.
- **No verified paper tests the cross-sectional funding sort on crypto
  perpetuals.** F1 rests on verified analogues plus this mechanism (§7).

**Selection shared with earlier looks, and what has been seen.**

- **New data, new count.** F1 is the only trial on Binance 2021-12 → 2023.
  The four F2 variants were rejected on behaviour (turnover and
  autocorrelation) before any outcome, and are not counted (§1).
- **B1, the 010 overlap (`000g`).**
  - `data/carry/` (registration 010, a draft) holds:
    - Binance USDⓈ-M funding from 2023-05 to 2026-06;
    - Binance 8h klines to 2024-12;
    - Binance daily closes to 2026-06.
  - Together they contain both parts of F1's payoff (funding and price)
    for **all of holdout A and B**, outside 009's guard and manifest.
  - **Nothing in 009 has read them.** No 009 code path can, by an assert
    in `r009_run.checked` and `binance_years.connect`, with a static scan
    in the tests.
  - 010's printed statistics include Binance legs in 2024: spread levels
    and position counts in `data/carry/logs/describe.json`, and in-hold
    spread persistence for 2023-06 → 2026-06 (`000f`). Those are about
    cross-venue spreads, not the cross-sectional sort.
  - **No 010 outcome on Binance data has been computed.** The ordering
    rule (D17, binding on both registrations) keeps it that way until 009's
    holdout A is read or closed.
- **M4, two forward-exposed behaviour statistics were read** during
  drafting (`000g`):
  1. **F2's weekly Spearman with minus the past 28-day return** (0.53
     value, 0.20 contracts). The past window at S overlaps the outcome
     windows of the signals at S−4w … S−1w, and F2 was persistent. It drove
     Revision 2's switch to contracts. F2 is now dropped, so it decides
     nothing further.
  2. **Leg counts near delisting cuts** (Revision 1: 23 long / 17 short
     coin-weeks within 4 weeks of a cut). They cross the book's legs with a
     future, return-relevant event. They were used to argue a bias
     direction. They are withdrawn from §3 and §7 and decide nothing.
  - **Neither may motivate any further change.**
- **Other Binance data on disk that overlaps the holdouts** (review M4,
  disclosed in Revision 4). The whitelist means 009 reads none of it.
  - **`data/scout_l2/binance/funding/`:** Binance USDⓈ-M funding for
    **213 symbols** in quarterly months 2023-07 … 2026-04. That includes
    holdout A's 2024-01, -04, -07, -10, 2025-01, -04 and -07, holdout B's
    months, and monthly files for three core coins. The L2 scout printed
    only aggregate yearly medians and HL-minus-CEX quantiles from it. No
    per-coin persistence was computed.
  - **`data/scout_l2/binance/klines_um` and `klines_spot`:** BTC, ETH and
    SOL, monthly, 2023-01 → 2026-06.
  - **`data/binance/um/aggTrades*`:** six majors' trades on HL-cascade
    days, 2025-07-28 → 2026-06-30 (holdout B).
  - Under D17, no registration computes outcomes or cross-sectional
    funding persistence on any of these, for dates inside 009's holdout A,
    until 009 reaches a terminal state.
- **A third outcome-adjacent statistic** (review m7).
  - §3's market-move distribution (mean and s.d. of the weekly
    equal-weight move) and the yearly dispersion are realised returns over
    106 of the 107 discovery outcome windows. The trailing week at S is the
    outcome window of S − 1 w, shifted by an hour.
  - They are used only as unconditional distributions, never paired with a
    score.
  - With β_book ≈ −0.10, they imply that the discovery beta P&L is about
    +1.5 bp a week (−0.100 × the −0.15% mean price-only move). Revision 3a was drafted knowing the
    distribution. Ledger it in `000g` (orchestrator).
- **Other things seen.**
  - The L2 scout printed unconditional Binance funding medians by year
    (levels).
  - The L4 scout's listing tells which coins later delist. It is used only
    for the delisting cut and the holdout-A breadth count.
  - H6 (the old F2 idea) came from the Hyperliquid year, which is holdout B
    here. It is moot now.

## 1. Trials registered here

| id | signal (score; high = long) | status |
|---|---|---|
| **F1** | −round(Σ funding rates settled in [S − 7 d, S), 1e-10) | **the only trial**, 107 weeks |
| F2, 7-day value | −ln(mean OI value [S−1d, S) ÷ [S−8d, S−7d)) | **rejected on turnover before any outcome**: autocorrelation −0.20; buffered cost with drift 28.7 bp a week |
| F2, 7-day contracts | the same in contracts | **rejected on turnover**: −0.21; 29.1 bp |
| F2, 28-day value | −ln(mean OI value [S−1d, S) ÷ [S−29d, S−28d)) | **rejected on turnover** (Revision 3): 0.524; 15.07 bp > 15 |
| F2, 28-day contracts | the same in contracts | **rejected on turnover** (Revision 3): 0.516; 15.44 bp > 15 |
| F3 | (top-trader long/short or taker imbalance) | dropped in the draft (no citable weekly sign) |

**Family of 1: one-sided α = 0.05, z_crit = 1.645.**

- **The F2 gate.** It was set in Revision 1, before measuring: buffered
  cost ≤ 15 bp a week and autocorrelation ≥ 0.5. Revisions 1–2 measured
  the cost on target weights only (14.6 / 14.93 bp). Revision 3 measures
  what the book pays, drift re-sizing included: 15.07 / 15.44 bp. Both fail.
- **Provenance (Revision 4, m6).** The four measurements are stored in
  `data/binance_years/describe.json` (`rejected_F2_record`). They were made
  in Revision 3 with `turnover_path(drift=True)` on that revision's F2
  features; the code has since been removed.
  - **The 28-day value variant missed the gate by 0.07 bp** (15.07 vs 15),
    and the contracts variant by 0.44 bp.
  - The gate measure is a behaviour estimate, not the run measure: 00:00
    closes against 01:00 opens, no feasibility filter, no delisting close.
    So the miss lies inside the measurement gap.
  - The orchestrator ruled not to pursue F2 further. Revisiting it would be
    gate-chasing.
- **The review estimated ~16.8 bp** from a proxy (Σ|w|·|r|·rate). My exact
  drift-inclusive turnover is lower (about +0.5 bp), because drift partly
  nets against target changes. The verdict is the same.
- Under the null, the behaviour-only gates are functions of features
  alone, so trying four variants does not inflate F1's test (review minor
  11). The exception is M4's statistic, which is disclosed.

**Written expectation.** About 1 in 3 that a costed F1 premium exists. On
this data it is detectable only if large (§3).

## 2. Data, periods, universe, costs

**Pull (owner-approved 2026-10-11, discovery only).** Source:
data.binance.vision (public, free).

| dataset | span | files | bytes (zipped) |
|---|---|---|---|
| daily `metrics` | 2021-11-01 → 2023-12-31 | 123,064 | 1,389.4 MB |
| monthly `klines` 1h | 2021-11 → 2023-12 | 4,349 | 133.6 MB |
| monthly `fundingRate` | 2021-11 → 2023-12 | 4,313 | 3.6 MB |
| daily `klines` 1h, 2024-01-01 only | symbols alive on 2023-12-25 | 239 | 0.3 MB |
| REST `/fapi/v1/fundingRate`, end-dated 2024-01-01 00:59:59 | the 00:00 settlement | 239 JSON | — |

- **257 symbols** were pulled: every crypto USDT perpetual with a full
  metrics day in the span, minus the index composites and USDC.
- **131,965 zips, 1.527 GB, each verified against its `.CHECKSUM`: 0
  mismatches, 0 missing.**
- **Manifest:** `data/binance_years/MANIFEST.tsv` (sha256 `3cbcde2b…`).
  LITUSDT's REST call returned no row (relisted in 2025-12), so one
  settlement is missing.
- **Metrics are not used by F1** (Revision 3). The metrics files only
  define the life table, by the 4 KB rule, from the listing.
- **Nothing dated on or after 2024-01-08 was requested.** The runner reads
  nothing after **2024-01-01 01:00**, the true last exit (view filter plus
  exit assertion).

**Hygiene notes.**
- **One end-dated REST call per symbol** for the 2024-01-01 00:00
  settlement.
- **A side fetch** (accepted, Revision 1): a second paced process fetched
  the same plan's funding and klines files. The main fetch re-verified
  every key; the manifest is built from the main state.
- **Listing metadata after 2024-01-08** is read only for the delisting
  cut and the holdout-A breadth count.
- **HARD RULE (`000g`): no 009 code reads `data/carry/`.** All data paths
  go through `r009_run.checked`, and `binance_years.connect` refuses that
  path. The test suite asserts both and scans the 009 code.
- The 010 copy of Binance data exists and is not used.

**The split.** A signal S is a Monday 00:00 UTC; its features use
[S − 7 d, S) and its outcome runs from the S + 1 h open to the S + 7 d + 1 h
open.

| split | dates | signal weeks S | role |
|---|---|---|---|
| discovery | 2021-12-06 → 2023-12-31 | **107**: 2021-12-13 … 2023-12-25 | decides whether F1 earns the holdout-A pull |
| holdout A | 2024-01-01 → 2025-07-27 | **81**: 2024-01-08 … 2025-07-21 | decides; read once; last exit 2025-07-28 01:00 (accepted) |
| holdout B | 2025-07-28 → 2026-06-30 | 47 | confirmatory only |
| sealed | 2026-07-01 → | — | untouched |

The first week of each split carries features only: that is the embargo.

**Universe at each S** (`factors.derive`, point in time; **no metrics
needed**, Revision 3):

1. **A crypto USDT perpetual.** Not a TradFi or index contract, and not
   BLUEBIRD, FOOTBALL, BTCDOM, DEFI or USDC.
2. **Alive at S**, inside a life segment.
   - Segments come from full metrics days (≥ 4 KB) in the listing, and are
     **bridged by trading activity** (`factors.bridge_trading_gaps`). A day
     is active with ≥ 1 traded 1h bar.
   - Across a metrics hole, the earlier segment extends forward over the
     hole's leading active days, and the later one backward over its
     trailing active days. A hole active throughout merges the segments.
   - A first segment extends back over active days before it.
   - On the discovery panel, 137 adjustments were made. 135 extend first
     segments back to the coin's first traded day in the panel (no
     effect, because the first signal is 2021-12-13). **ICP's** first
     segment now ends at 2022-06-10, its last trading day before a dead
     July–August 2022 (it was cut at 2021-12-23 before). CVC's end moved by
     a day.
3. **Listed ≥ 30 days at S**, counted from the first 1d kline, or from the
   relisting day.
4. **Liquid:** trailing 30-day mean daily quote volume ≥ **$10 M**.
5. **σ available:** 30 daily log returns before S, ≥ 20 of them.
6. **F1 is needed to be scored, not to be in the universe** (Revision 4,
   m3).
   - A score needs settlements within 8 h of each end of [S − 7 d, S), and
     **every settlement of the window** (at least 168 h ÷ the longest
     interval seen; 21 at 8 h).
   - A coin-week missing one gets no score and is counted (m5).
   - Unscored universe coins never enter the book or the placebo shuffle.
     They do enter the equal-weight market (the beta-adjustment and β
     estimation) and the simulation's dispersion: **the market is the whole
     universe.**
   - A hold missing a settlement has no funding P&L (never 0) and is held
     as cash; the run counts these.

**Bridging look-ahead (m4, disclosed).** A day counts as active if any bar
that day traded. Membership at S can therefore rest on S's own Monday, and
segment moves happen only where metrics resume later. On discovery the
effect is nil: ICP is unscored in those weeks, and CVC moves by one day.

**The trade** (`factors.run_book`, `factors.outcomes`):

1. **Book: the buffered rule** (`factors.buffered_targets`).
   - For n scored coins, let k = ⌊n/5⌋ and b = ⌊0.4 n⌋.
   - A coin **enters** long at a score ≥ the k-th highest (all coins tied
     with it included), and short at ≤ the k-th lowest.
   - A **held** coin stays while inside its 40% band, ties included.
   - It **leaves** when it drops out of the band or the universe.
   - A coin in both legs is dropped from both.
   - Weights are ∝ 1/σ inside each leg, and each leg sums to 1: $1 a side.
   - A held coin inside its band is never traded out. Its size moves with
     drift and when the leg's members change.
2. **Entry** at the S + 1 h open. A coin with no trade in that bar is held
   as cash.
3. **Exit** at the S + 7 d + 1 h open. A coin cut inside the week exits at
   the close of its last traded bar before the cut.
4. **Coin return** = price return + funding P&L of a long:
   −Σ rate × (price at settlement ÷ entry), over settlements in
   (entry, exit]. A short earns the opposite.
5. **Costs:** (taker + tier slippage) × |target − drifted holding| at each
   rebalance. A cut coin's closing value pays the same rate. The first week
   pays the full entry; the last week pays no liquidation.
6. **Weekly net** = Σ w × (R + F) − costs. It is split and reported as
   gross price (Σ w R), gross funding (Σ w F) and costs.

**Costs (Binance USDⓈ-M VIP0):** taker 5 bp per side, plus slippage per
side by the trailing 30-day mean daily quote volume:

| tier | volume | slippage | round trip | share of universe coin-weeks | share of long $ / short $ |
|---|---|---|---|---|---|
| T1 | ≥ $1 B | 1 bp | 12 bp | 2.7% | 3.5% / 1.3% |
| T2 | $200 M – 1 B | 2 bp | 14 bp | 13.5% | 18.2% / 5.6% |
| T3 | $50 – 200 M | 4 bp | 18 bp | 30.0% | 34.1% / 24.5% |
| T4 | $10 – 50 M | 6 bp | 22 bp | 53.8% | 44.2% / **68.6%** |

- **Evidence for the tiers.**
  - The **lower bound** is one tick: the half-tick is ≤ 1.5 bp at p90 in
    every tier.
  - The per-coin Abdi–Ranaldo estimate on 1h bars is **not a bound**
    (Revision 3 corrects the draft). It is 0 at the median, its pooled
    moment is ≤ 0 in 8 of 10 deciles, and its p75 is 5.1 bp in T3 and
    8.8 bp in T4. **T4's 6 bp sits below that p75.**
- **The short leg holds 69% of its dollars in T4**, because 1/σ weighting
  favours T4's lower volatility. Costs therefore rest on the weakest
  evidence.
- **Hence two holdout-pull conditions (D13):**
  - the book at **2× slippage** has a mean net > 0;
  - the **stress-week** tiers (minimum 30-day volume over the past 90 days)
    are reported. They move 12–38% of coin-weeks to a cheaper-volume tier
    and raise the cost from 13.4 to 13.8 bp a week.

## 3. What was measured before writing this (behaviour only; no return paired with a score)

From `r009_describe.py` (`data/binance_years/describe.json`).

**Coverage, 107 weeks** (mean coins a week, min–max, coin-weeks):

| stage | mean | min–max | coin-weeks |
|---|---|---|---|
| alive (after bridging) | 161.1 | 131–236 | 17,236 |
| listed ≥ 30 days, with σ | 156.3 | 130–227 | 16,727 |
| universe ($10 M floor) | 142.6 | 100–213 | 15,262 |
| **… with F1** | **141.8** | **99–213** | **15,169** (244 symbols) |

- **By year** (with F1): 128 (2021-12), 131 (2022), 153 (2023).
- **Breadth by floor:** 155 / 154 / **142** / 112 / 65 at $0 / 5 / 10 / 20 /
  50 M.
- **Scoring** (Revision 4, m3/m5): the universe has 15,262 coin-weeks, of
  which 15,169 are scored and **93 unscored**.
  - 92 have no funding rows in the window. That includes ICP's 26 bridged
    weeks: data.binance.vision has **no ICP funding files before
    2022-09**.
  - **1 is missing a settlement inside the window.**
  - Fetching ICP's funding by REST is outside the approved pull (§10).
- **Delisting:** 19 coin-weeks have their cut inside the week. The legs are
  not crossed with cuts (M4).

**The F1 feature.**
- The trailing 7-day funding, annualised, has a median of 9.5% (2021-12),
  3.1% (2022) and 9.7% (2023). 26% of coin-weeks are negative.
- **9.7% of coin-weeks sit exactly on the default** (0.01% every 8 h,
  21 × 1e-4 after rounding to 1e-10).
- **Entry boundaries are tied in 20 weeks: 16 short, 4 long.**
- The largest leg is **94 of 188 names** (2023-11-27, long leg: the default
  coins form the top quintile's boundary). In such weeks F1 is
  "default-rate coins vs above-default" on the long side.
- Buffered legs average 38.1 long / 40.1 short names.

**Persistence and costs** (behaviour; target weights and the pre-S
drift):

| book | turnover a week | **cost a week** | a year |
|---|---|---|---|
| **buffered, with drift (registered)** | 1.34 | **13.4 bp** | 6.9% |
| buffered, target only (Revisions 1–2) | 1.27 | 12.7 bp | 6.6% |
| buffered, with drift, 2× slippage | 1.34 | 20.0 bp | 10.4% |
| buffered, with drift, stress-week tiers | — | 13.8 bp | — |
| plain quintile, with drift (reported) | 2.09 | 20.8 bp | 10.8% |

The 1-week rank autocorrelation of F1 is **0.58** (p5–p95 0.41–0.74).

**The target book, ex ante.**
- **Net beta** to the equal-weight universe (30 daily returns before S):
  mean **−0.10**, p10 −0.21, p90 +0.01. By year: −0.12 / −0.05 / −0.15.
- **The trailing funding gap** between the legs is **56.5 bp a week**:
  what the book would collect if every coin's last-week funding repeated.
  It is mechanical carry and not a forecast. It is several times the cost,
  which is why gross funding and gross price are reported separately.

**Contemporaneous dispersion and the market factor.**
- The cross-sectional s.d. of trailing weekly total returns is 10.8% /
  8.4% / 9.3% by year.
- The empirical weekly equal-weight **price-only** move (Revision 4, m12,
  matching the run) has a mean of −0.15% and an s.d. of 10.4%. The
  simulation draws from these.
- **These are outcome-adjacent and disclosed in §0** (m7). They imply a
  discovery beta P&L of about +1.5 bp a week.
- **β calibration** (m1): the simulation uses the ex-ante β as the true
  β, so its beta-only null assumes β is known without error. On these
  data, a 0.15 β error in a −2% a week bear moves the beta-adjusted t by
  about 0.8. The run therefore reports the realised slope and the
  intercept of the weekly net on the market (§5).

**Power (Revision 4): F1 alone, joint over all five criteria, by
simulation.**

- **Method** (`factors.simulate_power`, **2,000 histories per cell**, one-sided
  z 1.645):
  1. Each discovery week keeps its real universe, F1 ranks, buffered
     target weights and ex-ante betas.
  2. Costs are the behaviour costs **with drift**, at 1× and 2× slippage.
  3. Idiosyncratic returns are the week's demeaned empirical cross-section
     of *contemporaneous* trailing-week returns, assigned by a Gaussian
     copula at the planted Spearman IC, plus week-level IC noise to the
     stated σ_IC.
  4. **A common market move**, drawn from the 107 empirical weekly
     equal-weight moves, enters as β_i × m (M2).
  5. No return is paired with a score. The trailing weeks are earlier outcome windows, used as unconditional distributions (§0).
- **Holdout A:** 81-week circular blocks of the same structures, under
  CONFIRM at 1.645, **including the 2× slippage net > 0** (Revision 4).
- **End to end:** pass AND both pull conditions AND holdout confirm, in the
  same history.

| σ_IC | weekly IC | **P(pass)** | P(pass and pull conditions) | share of passes stopped by the pull | P(holdout A confirm) | **end to end** |
|---|---|---|---|---|---|---|
| 0.085 (sampling) | 0 | 0.012 | 0.006 | 52% | 0.018 | 0.000 |
| 0.085 | 0.02 / 0.03 / 0.05 | 0.26 / 0.54 / 0.94 | 0.20 / 0.50 / 0.93 | 22% / 8% / 1% | 0.23 / 0.47 / 0.87 | 0.05 / 0.25 / 0.81 |
| **0.15** | 0 | **0.017** | 0.012 | 29% | 0.026 | **0.000** |
| **0.15** | 0.02 / 0.03 / 0.05 | 0.17 / **0.35** / **0.78** | 0.14 / 0.31 / 0.76 | 17% / 12% / 3% | 0.17 / 0.31 / 0.68 | **0.025 / 0.09 / 0.52** |
| 0.20 | 0 | 0.024 | 0.020 | 15% | 0.027 | 0.001 |
| 0.20 | 0.02 / 0.03 / 0.05 | 0.14 / 0.26 / 0.62 | 0.11 / 0.23 / 0.59 | 16% / 11% / 5% | 0.14 / 0.25 / 0.52 | **0.013 / 0.05 / 0.31** |

Mean net per week at planted IC 0 / 0.02 / 0.03 / 0.05: −12 / +28 / +48 /
+88 bp. Revision 4's changes moved no end-to-end figure by more than 0.002:
the 2× confirm term, the price-only market and the scored-only placebo.

**Null rates (Revision 4).**
- **Empirical market moves, IC 0:**
  - P(pass all five) is 1.2–2.4%, and the false pull (pass and
    conditions) is 0.6–2.0%.
  - Without the market factor, P(pass) is 1.25%.
  - Criterion 1 alone passes 5.0–6.0% of the time, against a nominal 5%.
  - End to end is ≤ 0.1%.
- **Beta-only null** (IC 0, the price-only moves shifted to a −2% a week
  bear, so the book's net short beta alone earns): raw P(pass) is
  6.1–6.7%, and the **unconditional false pull is 1.2–2.7%**.
  - Among those beta-only passes, condition (a) lets **17% (sampling σ_IC)
    and 43% (σ_IC 0.15)** through; it stops 83% and 57% (m2 corrected).
- **How often the condition binds on top of a pass under a planted IC:**
  3–17% of passes at σ_IC 0.15, most at small effects.

**Required gross edge (F1, per week, $1 a side):**

| σ_IC | breakeven (= cost with drift) | s.e. of the weekly mean | gross for criterion 2 at 50% power | at 80% power |
|---|---|---|---|---|
| 0.085 | **13.4 bp** (7.0%/yr) | 28 bp | 59 bp | 82 bp |
| 0.15 | 13.4 bp | 36 bp | **73 bp** | **103 bp** |
| 0.20 | 13.4 bp | 44 bp | 85 bp | 122 bp |

**The economic bar, plainly.** The book criterion needs **about
60–120 bp a week gross (roughly 30–65% a year on $1 a side)** to pass at
50–80% power. Discovery can only detect a large factor. **A fail means
"not detected at that size", never "no premium".** Power claims are
conditional on the assumed σ_IC: at 0.20, end-to-end power at IC 0.05 falls
from 0.52 to 0.31 (0.525 → 0.307 in `describe.json`).

## 4. Pass — F1, all five (one-sided z = 1.645)

On the 107 discovery weeks:

1. **IC:** the weekly Spearman IC has mean > 0 and Newey–West t ≥ 1.645
   (Bartlett, 4 lags).
2. **Book:** the costed buffered book has a mean weekly net > 0 and
   NW t ≥ 1.645.
3. **Shape:** the mean and the median of the weekly net have the same sign.
4. **Halves:** the mean weekly net is > 0 in both halves (53 / 54 weeks).
5. **Concentration:** the mean weekly net is > 0 without the calendar month
   with the largest positive summed net. A loss month is never the one
   removed.

**Conditions of the holdout-A pull** (D13; not part of the pass; Revision
3):

- (a) the **beta-adjusted** weekly net, net − β_book × the equal-weight
  universe return over the hold, has a mean > 0 **and Newey–West t ≥
  1.645** (Revision 3a);
- (b) the book at **2× slippage** has a mean net > 0.

The raw net is reported beside (a). Under the beta-only null of §3, the
unconditional false-pull rate is given there; among beta-only *passes*,
condition (a) lets a share through that depends on σ_IC (Revision 4 wording,
m2).

**Placebo:** 200 seeds; scores are shuffled within each week **among the
scored coins** (Revision 4), and costs are kept. On synthetic null data with the buffered book, the pass share is
0.005 and the |IC t| tail 4%.

## 5. Reported, decides nothing

- **Weekly series, joined on S:** IC, n, gross, gross price, gross funding,
  cost, net, β_book, market return, beta-adjusted net, legs, turnover.
- **The regression of the weekly net on the market** (m1): the intercept
  with its NW t, and the slope (realised beta) beside the ex-ante β_book.
  The beta adjustment uses the ex-ante 30-day daily β. Non-synchronous
  trading in T4 likely understates the weekly β, so the realised slope is
  the check.
- **Counts:** coin-weeks with a missing settlement, in the feature week and
  in the hold (m5).
- **Blocks:** by calendar year; by liquidity tier; the long and short legs.
- **Variants:** the plain quintile book; 0× slippage; stress-week tiers.
- **Future knowledge, labelled:** the book without coin-weeks within 14
  days before a delisting cut.
- **No reported block may become a primary on discovery.** Any follow-up is
  a new trial, counted and decided on holdout A only.

## 6. Holdout A (built now; read once, only after a discovery pass, the pull conditions and the owner's approval)

- **The pull** (`r009_pull_holdout.py`, hashed) writes to
  `data/binance_years_holdoutA/` with its own manifest. It never touches
  `data/carry/`. It fetches:
  - monthly klines 1h and fundingRate, 2023-12 → 2025-06 (December is the
    warm-up);
  - daily klines 1h, 2025-07-01 → 2025-07-28;
  - the July 2025 settlements by end-dated REST, to 2025-07-28 00:59:59.
- **Its size:** the plan from the listing alone is 494 symbols, 25,193
  files and about 0.20 GB. The byte count is quoted to the owner first.
- `fetch`, `rest` and `build` **refuse until the holdout guard passes.**
- **The run** is `r009_run.py --holdout-A`. Its guard (Revision 4)
  requires:
  - REGISTERED, (a)–(e) as for discovery;
  - (g1) exactly one 009 discovery row (tree plus `--all` history);
  - (g2) the row is committed in HEAD;
  - (g3) the row's registration commit is an ancestor of HEAD, and the
    code is unchanged since (diff and stored hashes);
  - (g4) the criteria, both pull conditions and the decision are
    recomputed from the stored numbers, and the decision is
    `pull_holdout_A`;
  - **(h') the line `Holdout A: APPROVED <YYYY-MM-DD> row <sha256 of the
    discovery row line>`**, committed strictly after the row's commit;
  - (i) no holdout row anywhere.
- It **checks the holdout panel's raw files against the holdout MANIFEST**,
  stores the manifest and panel hashes in its row, and writes a
  `holdout_A_started` row before computing.
- It uses the same universe rules, floor, tiers, book and costs.
- **Reads stop at 2025-07-28 01:00.** The 2025-07-28 daily files bring 23 h
  of holdout B's features-only week onto disk, and those rows are invisible
  on read.
- **CONFIRM if all of:**
  - IC mean > 0 with NW t ≥ 1.645;
  - book mean net > 0 with NW t ≥ 1.645;
  - median weekly net > 0;
  - mean ≥ 0 without the largest positive month;
  - **the book at 2× slippage has a mean net > 0** (Revision 4, M3).

  The stress-week tiers, the beta-adjusted series with its NW t, and the
  market regression are reported.
- CONFIRM leads to a registered forward paper period. Otherwise F1 closes.
- **Holdout B is reported only.** It has no criteria and no guard here; if
  a CONFIRM comes, it gets its own registration before it is read.
- **Ordering (D17):** see §9.

## 7. Known weaknesses, stated now

- **The F1 sign is not tested in the literature.** No verified paper tests
  the cross-sectional funding sort on crypto perps. F1 rests on verified
  analogues (cross-asset carry; the BTC/ETH carry–crash link) plus a
  mechanism. Chi et al.'s basis direction is unverified (§0).
- **The economic bar is high** (§3). A fail means "not detected at that
  size", never "no premium". **Power claims are conditional on the assumed
  σ_IC.** The realised σ_IC will be reported; at 0.20, end-to-end power
  roughly halves.
- **F1's expected gross is largely mechanical funding.** The trailing gap
  is 56.5 bp a week. A pass could come from funding that persists while
  prices give back less than the funding. That is the claimed premium, but
  the weekly series splits gross price from gross funding so it can be read
  apart.
- **Beta.** The ex-ante net beta is −0.10, and discovery is mostly an
  altcoin bear. The beta-adjusted condition (D13 a) and the market-factor
  simulation address it. Condition (a) is a NW t-test (Revision 3a).
- **Costs rest on T4,** where the short leg holds 69% of its dollars. T4's
  6 bp is below the noisy per-coin Abdi–Ranaldo p75 (8.8 bp). D13 (b)
  requires a positive net at 2× slippage. Tiers can move cheaper in stress
  weeks, because trailing volume rises then; the stress-tier block is
  reported.
- **Holdout A's breadth is not a guaranteed advantage.** The listing shows
  303 coins alive a week against 156 in discovery. The extra coins are
  mostly 2024–25 listings near the floor, in T4. The holdout figures in §3
  use discovery's structure and are **not a lower bound** (Revision 3
  corrects the draft).
- **Delisting announcements are not used.** A live book would see the
  notice days ahead. The bias direction is not known.
  - Prices usually fall after the announcement, which hurts a long and
    pays a short.
  - Funding on a dying coin is often deeply negative, which **pays the
    long**.
  - The two effects pull in opposite directions; the draft's "biases
    against a pass" argument is withdrawn (M4).
  - A future-knowledge block without the 14 days before each cut is
    reported. Delisting affects 19 coin-weeks within their week.
- **ICP has no archived funding** before 2022-09, so 26 bridged coin-weeks
  carry no F1 score.
- **Funding is marked** at the settlement hour's open. One settlement is
  missing (LIT, 2024-01-01).
- **The cut** is the last full metrics day, and a cut coin exits at its
  last traded close, not the settlement price.
- **The regime.** Discovery is the 2022 bear and 2023's recovery.
  Criteria 4 and 5 and holdout A are the guards.
- **The venue (legality).** A warning; the owner's call.

## 8. Built before any outcome is computed

- [x] `r009_pull.py`: the discovery pull. It is keyed by the listing, with
  dates asserted < 2024-01-08, paced, resumable and checksum-verified.
- [x] `r009_pull_holdout.py`: **the holdout-A pull, built now** (M3).
  - It writes to its own directory and asserts dates ≤ 2025-07-28.
  - It never touches `data/carry/`.
  - Only `plan` (listing only) works before approval.
- [x] `ct/binance_years.py`: the panels.
  - Only verified zips are read, and rows are bounded by the pull's hard
    end.
  - It tolerates a pull without metrics (holdout A).
  - `connect` hides rows after a split's last exit and refuses
    `data/carry/`.
- [x] `ct/factors.py`:
  - the split, with the true last exits;
  - the metrics-free feature SQL: F1, σ, ex-ante β, ADV, the 90-day
    minimum ADV;
  - life-gap bridging, `derive` and the universe;
  - the outcome SQL, with exits asserted ≤ the true last exit;
  - the buffered book, with drift, costs, delisting closes and cash for
    missing entries, plus β_book and the gross price/funding split;
  - the IC, the five criteria, the placebo;
  - the pull conditions (beta-adjusted, 2×) and `decide`;
  - `holdout_confirm`;
  - the drift-inclusive turnover;
  - the power simulation, with a common market factor.
- [x] `r009_run.py`:
  - **discovery** and **`--holdout-A`**, each with its guard;
  - `checked` refuses `data/carry/`;
  - guard (f), (g) and (i) scan the working tree **and the git history**
    of `results.jsonl`;
  - the output row records the Python and duckdb versions.
- [x] `r009_describe.py`: §3, behaviour only.
- [x] `tests/test_factors.py`: **127 checks pass**
  (`.venv/bin/python -m tests.test_factors`). Revision 3 added checks for:
  - **no read of `data/carry/`** (the path guards plus a static scan);
  - **the holdout-A guard** in a throwaway git repo:
    - no discovery row (g);
    - a closed discovery (g);
    - no approval (h);
    - approved (allowed);
    - a holdout row exists (i);
    - a reverted row still refuses, through history;
  - **the beta-adjusted net on a planted-beta book:**
    - no alpha and a net short beta in a bear give a large raw net, a
      beta-adjusted net of exactly −costs, and a failed pull condition;
    - a planted alpha survives the adjustment;
  - **Revision 3a: a beta-only planted book passes discovery but fails the
    beta-adjusted NW t pull condition at about the one-sided rate.**
    Raw pass 1.0; pass and pull 0.03 (60 histories) and 3.7% of passes
    (400). The planted-alpha book passes the condition and the beta-only
    book fails it;
  - **Revision 4:**
    - the holdout-A guard refuses each of the second review's four
      bypasses:
      - a forged pull label over failing numbers, uncommitted: (g2), (g4);
      - a close row plus a bare pull row: (g1);
      - a pull row only in history: (g2);
      - approval committed before the row: (h');
    - and allows the valid path: one committed row, recomputed from its
      numbers, and a later approval quoting its sha256;
    - a code change after the run: (g3);
    - a holdout row on another branch: (i), via `--all`;
    - the started marker blocks a re-run;
    - the decline guard;
    - the whitelist refuses `data/carry/`, `data/scout_l2/`,
      `data/binance/` and `data/cache/`, and allows its own paths;
    - a missing settlement gives no score, and a hold missing one is cash
      with F = None;
    - the holdout confirm requires the 2× book;
  - drift turnover = what `run_book` pays;
  - gap bridging: active, dead and ICP-like partly dead holes;
  - the stress-week tiers; F1's rounding;
  - the exact hard end;
  - F1 and the universe need no metrics table.

  The earlier checks stand: the split, the buffer rule, costs, funding
  sign, the planted factor, noise, the placebo, point in time with leaky
  queries caught, the delisting cut, and the discovery guard.

**SHA-256** (Revision 4; read by the guard; recomputed at commit). Revision 4 adds the discovery pull code, the four L4 listing CSVs, exchangeInfo and the holdout `plan.csv` (review M2). The data
are unchanged since the draft.

| file | sha256 |
|---|---|
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/factors.py` | `a1e70b9a5a2c1b58f43d2b08a2eaeac5ce5a3fd45868a659d745a3354bd61197` |
| `ct/binance_years.py` | `0ad3e31cd98c9271f1fddf08b4e9355bd3d7e454f4ce47c960217eecca2b352a` |
| `ct/stats.py` | `bfcc85f285dc7af28f364e2d77e0fbe04b6c2367724f4b822b7fbf6ff435d472` |
| `r009_run.py` | `e79f76fb8c1425f782c2b37f49d4365f5025cc2086c2c4ded42a7a84c9d391e0` |
| `r009_pull.py` | `8b35d2fa9c08edca4c3c1eab3d06f9823e38c57030460942f4b616684af38b4b` |
| `r009_pull_holdout.py` | `b08e3eb0a3277204f7e038f2574699bc39bff3b1b1f44f9f0d709e4d0a7e1cbb` |
| `data/binance_years/MANIFEST.tsv` | `3cbcde2bd9bbaa42edbf26cf63738862a9ad35f99a89ddd58e9041babc50229c` |
| `data/binance_years/panel/bars_1h.parquet` | `38449117e8b91def9c411fb0219cdd9e413ef537ca81252f2a9c00adf7692267` |
| `data/binance_years/panel/funding.parquet` | `522986ffdda59182e02dd574d2139f0a13d1155644670adacb95ba22469950f5` |
| `data/binance_years/panel/metrics_1h.parquet` | `292a12f637ab45e70aa29fb54d2ce7d597997bebc015e667589078f5191a35cd` |
| `data/binance_years/panel/life.json` | `804a6fc37b9c72ea83fd1285d84e30e0b64fe79eaea5b5454970bc87e7127b70` |
| `data/scout_l4/listings/daily_metrics.csv` | `87a9e77022c22f196a1f6c7592cd0a6699efd0b67bac6e1986a881e01e0e6509` |
| `data/scout_l4/listings/daily_klines_1d.csv` | `56ecfc71c2686e5f6f00f8403512a0004c1ae786c8a8e9140b4386bfea760807` |
| `data/scout_l4/listings/monthly_klines_1h.csv` | `baf0eff4ba75c3335b341876d341e0190faac9a873038a6617028e42a8ec8196` |
| `data/scout_l4/listings/monthly_fundingRate.csv` | `ad19a15346ccca5d77f7252a3ced56baec8dfa028952a4c29411c71ae46b302e` |
| `data/scout_l4/exchangeinfo/fapi_exchangeInfo.json` | `a7f720f5ece63fe9ab087cbdd9fecc66d18645f113896b81ec171c89636ed39f` |
| `data/binance_years_holdoutA/plan.csv` | `4595001cb548fb1c3c4d4eca9c075f5ce200456ece7eab5eb397cbe8f7c7aa39` |

## 9. Owner decisions — APPROVED 2026-10-11 (owner, 2026-10-11: "set to registered and proceed with recommendations")

**Approved:** D1–D17 as written below; §10.1 (ICP's pre-2022-09 funding) **not fetched**, as drafted.

| # | item | PROPOSED |
|---|---|---|
| D1 | data | the discovery pull of §2 (manifest `3cbcde2b…`); holdout data only per D13; **never `data/carry/`** |
| D2 | split | discovery 107 signal weeks; holdout A 81 (last exit 2025-07-28 01:00, accepted); holdout B 47, confirmatory; 2026-07 → sealed; the first week of each split is features only |
| D3 | universe | crypto USDT perps; alive by life segments bridged by kline activity (look-ahead nuance disclosed); listed ≥ 30 d; ADV ≥ $10 M; σ; **no metrics needed**. F1 (every settlement of the window present) is needed to be **scored**; the equal-weight market is the whole universe |
| D4 | F1 | score = −round(Σ funding in [S − 7 d, S), 1e-10); long low, short high |
| D5 | F2 | **dropped**: all four variants rejected on turnover with drift before any outcome (§1) |
| D6 | family | **F1 alone; one-sided z = 1.645** |
| D7 | book | buffered (enter at the quintile with ties; keep inside the 40% band); 1/σ weights, legs ±1; Monday; entry S + 1 h, exit S + 7 d + 1 h; the plain quintile book reported |
| D8 | return | price + funding signed by position; the IC on the same coin return |
| D9 | costs | taker 5 bp + 1 / 2 / 4 / 6 bp slippage by tier, on the traded amount from drifted holdings; 0×, 2× and stress-week tiers reported |
| D10 | inference | Newey–West (Bartlett, 4 lags); z 1.645 |
| D11 | pass | the five criteria of §4; placebo reported |
| D12 | decision — fail | **"not detected at that size", never "no premium"**: discovery can only detect a large factor; the book criterion needs about 60–120 bp a week gross (≈ 30–65%/yr) to pass at 50–80% power; joint power ≈ 0.78 (0.76 with the pull conditions) at a weekly IC of 0.05 (σ_IC 0.15); F1 closes on this data |
| D13 | decision — pass | the holdout-A pull **only if** (a) the beta-adjusted net has mean > 0 with NW t ≥ 1.645 (Revision 3a) and (b) the 2× slippage net mean > 0, as recomputed by the guard from the stored numbers; then the owner, after seeing the row and a byte count, commits "Holdout A: APPROVED <date> row <sha256 of the row line>" here, strictly after the row's commit; read **once** with `r009_run.py --holdout-A`. The owner may instead decline (`--decline-holdout-A`) |
| D14 | holdout A CONFIRM | IC mean > 0 with NW t ≥ 1.645; book mean net > 0 with NW t ≥ 1.645; median > 0; mean ≥ 0 without the largest positive month; **2× slippage net mean > 0** (Revision 4). CONFIRM leads to a forward paper registration; otherwise close |
| D15 | holdout B | **reported only**: no criteria and no guard here; registered separately before any read, after a holdout-A CONFIRM |
| D16 | family accounting | Binance 2021-12 → 2023: 1 trial (F1). The rejected F2 variants are not counted (behaviour gates); any follow-up is a new trial decided on holdout A |
| D17 | **ordering, programme-wide** (Revision 4) | **No registration or scout computes outcomes, returns or per-coin funding persistence on Binance data dated 2024-01-01 → 2025-07-28 (009's holdout A), from any directory** (`data/carry/`, `data/scout_l2/`, `data/binance/` …), **until 009 reaches a terminal state for holdout A.** The terminal states are 009 result rows: a discovery `close`; a discovery `close_conditions_unmet`; a `holdout_A_declined` row (owner); a `holdout_A` result row. 009 itself is whitelisted to its own data. `000g` and 010's `ordering_failures` must be aligned to these four states (orchestrator) |

## 10. Open questions (after the review)

0. **For the orchestrator (cannot be done from 009's files):** align
   `000g` and `r010_run.ordering_failures` with D17's four terminal states,
   and ledger §0's third outcome-adjacent statistic in `000g`.
1. **ICP's missing funding.** Should its 2021-12 → 2022-06 settlements be
   fetched by end-dated REST? That is 26 coin-weeks, outside the approved
   pull. As drafted, no.
2. **The F2 gate measure.** The review's proxy (~16.8 bp) and my exact
   drift-inclusive turnover (15.44 bp) differ. Both exceed 15, so F2 is
   dropped either way.
3. **Settled (Revision 3a):** D13 (a) is a beta-adjusted NW t ≥ 1.645.
4. **The slippage tiers.** T4's 6 bp is below the noisy per-coin p75. D13
   (b) is the guard.
5. **Legality warning.** Execution is on Binance.

## 11. Ledger

- `000g`: the 009/010 data overlap, the ordering rule (D17), and the two
  forward-exposed statistics (§0), all disclosed above.
- Revision 4 adds two items that `000g` does not yet hold (§10 item 0): the
  other Binance holdout data (`data/scout_l2/`, `data/binance/`) and the
  market-move statistic.
- No outcome has been computed for this registration. `results.jsonl` has
  no 009 row.
