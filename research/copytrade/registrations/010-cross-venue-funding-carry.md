# 010 — Cross-venue perpetual funding carry: short the higher-funding venue, long the lower, conditional entry (L2)

**Status: DRAFT** — Revision 2, for the orchestrator and the owner; the
decision table (§9) is PROPOSED and needs the owner's approval, including
the two owner lines under §9 (holdout end, extension), both UNSET today. Mechanics are built and
checked on synthetic data (§8). On real data only behaviour was measured
(§3). **No funding accrued after an entry decision, no price change after an
entry, and no back-test was computed.** Spread persistence *was* read by
accident during drafting and is ledgered as `000f` (below). The runner
(`r010_run.py`) refuses to compute an outcome until this file says
REGISTERED, is committed unmodified, the code and panel match §8, no result
row of the mode exists anywhere in `results.jsonl`'s history, and the 000g
ordering rule allows it (D17).

**Revision 2a (2026-10-11, orchestrator, accepting §10.2 of Revision 2).**
The holdout CONFIRM threshold is now **t_{G−1}(0.95)**, where G is the
number of month clusters actually in the holdout window. The extension's
two looks use **t_{G−1}(0.975)** each. Joint power, the holdout-alone
false-confirm rate and the joint false-pass rate are recomputed (§3, D13).
A test shows the false-confirm rate at ≤ about 5 % with 4 clusters under
the null. The 1.645 / 1.96 values quoted in the Revision 2 bullets below
are superseded.

**Revision 2 (2026-10-11, after second review).** Review:
`registrations/reviews/010-review-r1.md`. Changes:

- **N1, the owner's holdout choice.** Exactly one anchored line
  `Holdout end (owner): <date>` (UNSET until the owner decides; the offered
  dates are 2026-09-30 and 2026-12-31) and one `Holdout extension (owner):
  <YES|NO>`. Fetch, build and run require exactly one match of each; UNSET
  refuses. No instruction in this file starts a line with either phrase. A
  test reads the real file.
- **N2, the holdout gate** follows 009's pattern, in one guard used by fetch,
  build and run (`r010_run.guard_failures`):
  - exactly one 010 discovery row, committed in HEAD;
  - its registration commit is an ancestor of HEAD and the code is
    unchanged;
  - the decision is recomputed from the stored numbers
    (`carry.recheck_discovery`);
  - an approval line `Holdout fetch: APPROVED <date> row <sha256 of the
    row>`, committed after the row;
  - no fetch, build or read before the end + 7 days;
  - no empty answer is ever cached;
  - a coverage check: no venue-coin funding series may have an internal
    gap over 3 days, or the build stops;
  - `holdout_fetch_started`, `holdout_built` (with the panel's SHA-256)
    and `holdout_started` rows, and the run checks the built hashes.
- **N3, the ordering guard (h)** unlocks only on a 009 row in
  HEAD:results.jsonl whose registration commit is an ancestor of HEAD.
  - For a discovery row: `close` or `close_conditions_unmet` must be
    recomputed by 009's own `factors.recheck_discovery` from its numbers
    (imported read-only; hashed in §8).
  - A declined or holdout-A row must match the discovery row's sha256.
  - The reviewer's bypass probes (uncommitted, off-branch, foreign commit,
    mislabelled, wrong sha, deleted) are tests that must refuse.
- **N4, no position leaves the primary.** A failed name check at exit uses
  the raw exit gap. Where no price exists within 24 h of the exit, the
  position gets the worst observed return of its pair in that span. Both
  counts are printed before the decision; "drop missing" is a reported
  variant.
- **N5, the decision restructured** (orchestrator's decision; the owner
  approves).
  - A discovery pass uses the six criteria, including the 3 %/yr bar.
  - **The holdout CONFIRM is an existence test:** mean > 0 with a
    month-clustered t ≥ 1.645, median > 0, and mean > 0 without the
    largest positive month. The 3 %/yr bar is reported at confirm and
    becomes the deploy threshold of the follow-up paper registration.
  - An optional pre-registered **extension** to 2027-06-30 is read once,
    Bonferroni-adjusted (both looks at t ≥ 1.96).
  - Joint power is in §3 and D13. A non-confirm reads "not demonstrated".
- **N6:** entry and exit fill at the **post-settlement** price: the open of
  the bar starting at t on each venue, i.e. the first trade after the
  stamp. The close is used where an open is missing, and that is counted.
- **N7:** `--events` and `r010_describe.py` refuse any holdout panel. The
  residue sentence is in §11 (for `000f`).
- **N8:** §1 says plainly that T1 is in effect a Binance–Bybit test, and
  D13 makes the paper step treat its venue pair as untested.
- **N9:** the helper modules and the symbol lists are hashed (§8).
- **Minor items.**
  - m-a: started rows.
  - m-b: the holdout universe is rebuilt point in time from holdout data
    (§2).
  - m-c: a Bonferroni note (§1).
  - m-d: funding holes are counted and left out of both legs' accrual.
  - m-e: the holdout power is labelled a bound.
  - The docstrings are updated.

**Revision 1 (2026-10-11, after independent review).** Review:
`registrations/reviews/010-review.md`; ledger entries `000f` (written by
the orchestrator from B1) and `000g` (009/010 data overlap). Changes:

- **B1, persistence was read** (`000f`). The draft's decision funnel split
  qualifying coin-decisions by "already held", which is a forward
  persistence statistic: about **51 % of in-hold decision times still
  qualified vs 1.9 % outside holds in discovery, and 36 % vs 0.6 % in
  2025-01 → 2026-06** (CEX pairs). Fixes:
  - `plan` now skips a held coin before evaluating anything and returns
    only the number of positions opened. `--events` and `r010_describe.py`
    print unconditional counts only.
  - **H and θ are frozen at their cost-arithmetic values** (D5); the
    draft's open question 2 is closed as drafted.
  - **Periods restructured.** Discovery stays 2023-06 → 2024-12 and is
    disclosed as exposed on persistence. 2025-01-15 → 2026-06-30 becomes
    a **reported, exposed block** that decides nothing. **The decisive
    holdout is new, unread data: 2026-07-01 → an end date fixed now**
    (the owner's end date; Revision 2 made it one anchored line, §9), on HL, Binance and
    Bybit, fetched only after REGISTERED and a discovery pass.
  - The holdout mode is built and hashed now (M4).
- **B2, OKX survivorship.** OKX's candle API does not serve delisted
  instruments, so OKX coins that later died could never pass the name
  check. **OKX is dropped from the primary** (HL, Binance, Bybit only) and
  is a reported variant with the caveat. Binance and Bybit (and HL) keep
  delisted instruments; the per-venue delisted-coin counts are in §3.
- **B3, liquidation loss.** A liquidated 1× short now loses its whole
  margin (−N). The long leg is closed at the liquidation bar's close, the
  gap term is dropped for that position, and fees are charged in full.
  Tested (§8).
- **M1, criteria.** Criterion 5 (annualised ≥ 3 %/yr) stays as the
  economic point bar. Statistical existence is criterion 1b: the
  month-clustered t ≥ t₁₈(0.975) = **2.10**. D12 states P(pass) at a true
  2 / 3 / 4 %/yr under a monthly regime shock of 6 %/yr (2,000
  simulations).
- **M2, HL price.** HL's `candleSnapshot` serves 8-hour and daily candles
  for 2023-06 onward (end-bounded; delisted HL coins included). HL legs
  now use HL's own last-trade closes for the gap, for the name check and
  for an HL volume. The premium model and its error estimate are retired
  (§3 records the old figures).
- **M3, slippage** is tiered per leg by that venue's own trailing 30-day
  mean daily quote volume: 1 / 2 / 4 / 6 bp per fill (≥ $1 B / $200 M /
  $50 M / $10 M), 009's tiers. The liquidity floor ($10 M, 30-day mean)
  applies to **each leg's own venue**.
- **MINORs.**
  - HL alt maintenance margin 16.7 % (distance 71.4 %).
  - Exit-gap search ≤ 24 h, else missing (reported at worst).
  - The funding weight resets at a rebalance.
  - The result-row check scans the working tree, HEAD and `git log -p`;
    versions are recorded.
  - OKX now uses last-trade closes like the others (variant only).
  - The placebo is reported honestly (§5).
  - The universe tie-break counts Binance months ≤ 2024-12 only.
  - §4.1 says "the rate at t".
  - §11 is completed.
- **000g ordering (D17).** 010 computes no outcome on Binance data dated
  ≥ 2024-01-01 until 009's holdout A is read or closed. The runner refuses
  unless `results.jsonl` holds a 009 discovery row that failed or a 009
  holdout-A row. No 010 code reads `data/binance_years/` (asserted).

Programme: `docs/edge-discovery-2026-10-11.md` (light L2). Scout:
`scouts/l2-carry.md`. Ledger entries read: `000b`, `000d`, `000f`, `000g`.

**The hypothesis.** When the trailing realised funding of one venue exceeds
another's for the same coin by more than the cost of a delta-neutral round
trip amortised over a fixed hold plus a benchmark margin, the funding
difference persists over the hold by enough that a 1×-per-leg,
equal-size, delta-neutral pair (short the higher-funding venue, long the
lower) earns more than HLP's lending leg (3 %/yr on gross capital), after
fees, slippage, the change in the cross-venue price gap, and rebalancing.

**What the earlier looks did and did not see (disclosure).**

- **`000f`: persistence was read on both earlier spans.** In discovery and
  in 2025-01 → 2026-06 the drafter, the reviewer and the orchestrator have
  seen that, after an entry at |s| ≥ θ, the trailing spread stays beyond θ
  at about half (2023–24) or a third (2025–26) of the hold's decision
  times. That is most of the answer about the funding leg's persistence.
  **Discovery is therefore partly in-sample on its main component**, and
  2025–26 can no longer decide anything: it is reported only. No parameter
  changed because of it; H and θ were set before it was seen and are
  frozen.
- The scout printed unconditional spread quantiles, hurdle shares and
  monthly sign-change counts for 2023 → 2026-H1. None of them sets a
  parameter (§4 lists every source).
- The light was selected because the scout showed wide spreads in 2023–24,
  the discovery span.
- `000g`: `data/carry/` holds Binance funding and prices over 009's
  holdout A (2024-01-01 → 2025-07-27); hence D17.
- `000d`: Bitget is excluded; nothing uses ARB specially.

**Family count.** New data, a new family: **the L2 count starts at 0 and
this registers 2 trials** (T1, T2). Shared selection: the HLP benchmark
(criterion 5, the margin m), the scout's quantiles, and the `000f`
persistence knowledge on both pre-2026-07 spans.

## 1. Trials registered here

| id | universe | role |
|---|---|---|
| T1 | **all coins, pairs HL-BN, HL-BY, BN-BY**, pooled (one position per coin at a time) | primary |
| T2 | **core coins BTC, ETH, SOL** — the T1 positions in those coins | secondary |

Week-clustered t ≥ **2.24** (one-sided 2.5 %, Bonferroni over T1 and T2)
**and** month-clustered t ≥ **t_{G−1}(0.975)** (2.10 at 19 months); then
the point bar of ≥ 3 %/yr (§5). Everything in §6 is reported and decides
nothing.

- **Bonferroni note (m-c).** Only the week z is adjusted over T1 and T2.
  Criterion 1b and the holdout tests are not. Under month shocks, where 1b
  binds, P(either trial passes | no edge) is about 0.04 (§3: 0.019 and
  0.026 each). It is stated, not adjusted.
- **What T1 is (N8).** 1,847 of its 2,135 discovery positions (87 %) are
  Binance–Bybit. T1 is, in effect, a **CEX–CEX test**. HL pairs are 288
  positions, and the lawful leg (OKX Europe) is absent. A CONFIRM therefore
  says nothing tested about HL or lawful pairs: the paper step (D13) treats
  its venue pair as untested.

**Written expectation.** About 1 in 2 that conditional 2023–24 spreads
persisted enough to clear costs, 1 in 3 that the net also clears 3 %/yr on
gross capital after the gap, tiered slippage and rebalancing. `000f` says
the funding leg persisted for a good part of the hold, which raises the
first figure but says nothing about costs, the gap or the tails. Lower for
the 2026-H2 holdout (compressed regime).

## 2. Data and periods

| span | dates | use |
|---|---|---|
| discovery | **2023-06-08 → 2024-12-31**; every exit before 2025-01-01 | the trial (exposed on persistence, `000f`) |
| exposed block | **2025-01-15 → 2026-06-30** | **reported only**, decides nothing (`000f`); its outcome is computed in the holdout run |
| decisive holdout | **2026-07-01 → the owner's end (2026-09-30 or 2026-12-31, the owner line under §9)**; every exit before the end | read **once**, only through the gate of §8; nothing in it is on disk |
| extension (optional, pre-registered) | **2026-07-01 → 2027-06-30**, cumulative | only if the owner line says YES and the first look ends with 0 < month t < 1.96; read once |
| after the end | — | never read |

- **Venues:** Hyperliquid (HL), Binance USDT-M (BN) and Bybit linear (BY).
  OKX is a reported variant (B2). Bitget is excluded (no date-bounded
  history, `000d`).
- **Legal access is a warning, not a design constraint** (owner,
  2026-10-11).
- **Universe (point in time).**
  - Coins are Binance USDT-M symbols (names from the S3 listing, delisted
    included: 779 with a funding file in 2023-05 → 2026-06).
  - Venue symbols are mapped by name (`1000X` ↔ `kX` on HL; `1000X`,
    `X1000` or `X` on Bybit).
  - A venue symbol claimed by two coins keeps the exact-name one, else the
    one with more Binance funding months **up to 2024-12**.
  - A pair is eligible at t when both legs are listed (first settlement
    ≥ 7 days before t, a settlement in [t − 8 h, t)) and **each leg's
    venue** passes the liquidity floor and the name check. An entry price
    must also exist on both venues at t (§4).
- **Discovery panel** (`data/carry/panel/`, `r010_fetch.py` →
  `r010_build.py`):
  - funding of HL (2023-05-25 → 2024-12; 2025-01 → 2026-06 for the 80 HL
    coins whose own volume reaches the floor in 2025–26, for counts),
    Binance (monthly `fundingRate`) and Bybit (`funding/history`) to
    2026-06-30;
  - last-trade closes on the 8-hour grid, discovery only (BN 8h klines,
    BY 240-minute klines, HL 8h `candleSnapshot`);
  - the Binance 8h high/low path, discovery only;
  - daily quote volume per venue and daily or 8-hourly closes for the name
    check to 2026-06-30 (levels).
- **Holdout panel** (`data/carry/panel_holdout_<end>/`; extension:
  `panel_extension_2027-06-30/`).
  - It is fetched by `r010_fetch.py holdout` and built by
    `r010_build.py --holdout`, both behind the gate of §8 (N2).
  - It spans 2024-11-01 → the end, so it serves both the exposed block and
    the holdout.
  - **Its universe is rebuilt point in time from holdout data (m-b):**
    - every Binance USDT-M symbol in the S3 listing at fetch time (names
      only) with a funding file in [2024-11, the end month];
    - its Bybit symbol, probed with a funding request bounded by the end;
    - its HL coin, probed with a `fundingHistory` request in
      [2024-11-01, end).
    Coins listed after 2026-06 are therefore included. Ties are broken on
    months up to the end.
- **Hygiene.**
  - Every request is end-bounded before 2026-07-01: HL `endTime`, Bybit
    `end`/`endTime`, OKX `after`, Binance file month ≤ 2026-06.
  - Every loader drops rows at or after the end before saving or
    printing: **0 rows were received at or after 2026-07-01**.
  - `data/cache/bg-funding/` and `data/cache/hl-funding/` were not read.
    `data/binance_years/` is never read (asserted).

## 3. What was measured before writing this (behaviour only, unconditional)

From `r010_describe.py` (`data/carry/logs/describe.json`) and
`r010_run.py --events`. Counts are of positions opened; nothing is split by
position state.

- **Data and hygiene.**
  - 7.07 M funding settlements; 718 coins on ≥ 2 venues (HL 213, BN 718,
    BY 695, OK 435, OK used for the variant only).
  - 1.30 M last-trade closes on the 8-hour grid (discovery only).
  - Daily volumes and name-check closes to 2026-06-30.
  - **0 rows received at or after 2026-07-01**; 0 non-finite values in the
    panel.
- **Coins with ≥ 2 venues live in discovery:** 372 (HL 153, BN 361,
  BY 368). **279 coins** open at least one position.

**Positions the rule opens (counts only, D10's universe).**

| | HL-BN | HL-BY | BN-BY | all | core (T2) | weeks / months | coins |
|---|---|---|---|---|---|---|---|
| **discovery (T1)** | 174 | 114 | 1,847 | **2,135** | **93** | 81 / 19 | 279 |
| exposed 2025-01-15 → 2026-06-30 (reported; no entry-price check) | 318 | 255 | 2,386 | 2,959 | 88 | 74 / 18 | 432 |
| OKX variant, discovery (all six pairs; survivorship-biased) | 113 | 57 | 1,150 | 2,909 (+ BN-OK 968, BY-OK 519, HL-OK 102) | 111 | 81 / 19 | 283 |

- **Discovery split.**
  - By year: 548 (2023, from June) and 1,587 (2024). The count halves
    split at 2024-04-09 08:00 UTC.
  - Short leg: BN 833, BY 1,089, HL 213.
  - Holds: 424 h (1,835), then 400 / 416 / 312 / 328 / 336 h.
- **Signal and cost at entry.**
  - |s| at entry: p10 / p50 / p90 = 12.6 / 16.1 / 42.5 %/yr.
  - Tiered round trip on N: p10 / p50 / p90 = 33 / 41 / 45 bp. The most
    common slippage is 4 + 6 bp per fill (916 positions), then 6 + 6 (400).
- **The per-leg HL floor removes most HL alt pairs in 2023–24.** HL pairs
  fall from 2,020 (draft, Binance-volume floor) to 288. T1 is now 87 %
  BN-BY.
- **B2: delisted coins per venue** (a venue-coin whose funding stops before
  2026-06-01; timestamps and existence only).

  | venue | delisted | with name-check closes | in a primary discovery position | note |
  |---|---|---|---|---|
  | Binance | 365 | 365 | 279 | data.binance.vision keeps delisted symbols |
  | Bybit | 384 | 356 (all 384 with own closes except 4) | 276 | the 28 without a check have no Binance close at the same time (Binance delisted first), not missing Bybit data. The only Bybit coins without closes are COCOS (2 days in 2023-05) and 3 listed on 2026-06-24/30 |
  | HL | (detected by exact-zero prints; 3 in discovery: MATIC, RNDR, STRAX) | 213 of 213 coins have 8h candles | — | `candleSnapshot` serves delisted coins (MATIC, RNDR checked) |
  | OKX (variant) | 210 OK coins live in discovery; 82 die before 2026-06 | **0 of the 82** | — | the history-candles API omits delisted instruments: the B2 hole |

**Unconditional levels** (every eligible coin-pair-time on the grid, held or
not; no dynamics):

| | discovery core | discovery others | exposed core | exposed others |
|---|---|---|---|---|
| spread s, n | 12,663 | 175,035 | 14,364 | 239,487 |
| p10 / p50 / p90 %/yr | −5.9 / +2.8 / +27.5 | −16.0 / 0.0 / +17.6 | −6.3 / +1.8 / +9.8 | −16.3 / 0.0 / +15.7 |
| SD (robust SD) %/yr | 17.6 (7.9) | 30.9 (6.2) | 8.3 (5.8) | 115.3 (7.5) |

- Discovery SD by year, core / others: 22.8 / 44.3 (2023) and 15.7 / 26.3
  (2024) %/yr.
- Contemporaneous cross-coin correlation of the standardised spread:
  ρ = 0.098 in discovery, 0.031 in the exposed span.
- **Gap level, within coin-pair SD** (last-trade closes, discovery):
  - core: HL-BN 5.9, HL-BY 5.9, BN-BY 2.3 bp;
  - others: HL-BN 9.8, HL-BY 9.8, BN-BY 28.6 bp;
  - pooled 4.8 / 26.7 bp.

**Price-gap error (M2).** Every leg is now a last-trade close of the bar
ending at t on its own venue, so no model remains. The residual error is
the bid-ask bounce of each close plus seconds of asynchrony between venues.
Both are ≤ 1 bp per close on core coins (the scout's spread proxy), and
the alt bound is the tier slippage. The premium model of the first draft is
retired. Its measured error was 1.3–1.9 bp SD for HL core in 2025–26 (HL
archive), and 7–11 bp for alts by the CEX index analogy.

**Margin distances at 1×** (short leg): HL core 95.1 %, **HL other 71.4 %**
(mm 16.7 %), CEX core 99.0 %, CEX other 95.1 %. The rebalance trigger is
50 %.

**Power — joint over all six criteria, by simulation** (`carry.simulate_power`,
**2,000 draws**, the actual discovery entry pattern).

- Each position's R = (planted edge + month shock) × hold + noise/2.
- Funding noise = σ_s × hold × (√ρ · weekly common + √(1−ρ) · idiosyncratic),
  with σ_s at the unconditional SD (persistent bound).
- Gap noise = √2 σ_g.
- The month shock is N(0, σ_m) on the annualised edge, common to every
  position entered in that month (the reviewer's probe).

| true net edge (gross capital) | T1, σ_m = 0 | **T1, σ_m = 6 %/yr** | T2, σ_m = 0 | **T2, σ_m = 6 %/yr** |
|---|---|---|---|---|
| 0 | 0.000 | 0.019 | 0.001 | 0.026 |
| 2 %/yr | 0.007 | **0.21** | 0.16 | **0.19** |
| 3 %/yr | 0.50 | **0.40** | 0.50 | **0.36** |
| 4 %/yr | 0.99 | **0.62** | 0.83 | **0.57** |
| 5 %/yr | 1.00 | 0.82 | 0.98 | 0.76 |
| 10 %/yr | 1.00 | 1.00 | 1.00 | 1.00 |

- **Without regime shocks, criterion 5 decides.** The annualised estimate's
  SD is 0.4 %/yr (T1) and 1.0 %/yr (T2).
- **With a 6 %/yr monthly regime shock, the 19 month clusters decide.**
  The annualised SD rises to 1.6 / 1.8 %/yr, and P(pass) at a true 2 %/yr,
  below the bar, is about 0.2.

**Holdout power — a BOUND, not an estimate (m-e).** The simulation uses
Gaussian noise at the exposed span's raw spread SD (115 %/yr for others,
against a robust 7.5 %/yr), so it is **pessimistic**. It draws random
contiguous windows of the exposed span's counted entry pattern (1,000
draws) and judges them by the **existence-test CONFIRM** of D13 (month t ≥
t_{G−1}(0.95), median > 0, mean > 0 without the largest positive month).

| true edge | 2026-09-30, T1 σ_m 0 / 6 % | 2026-12-31, T1 σ_m 0 / 6 % | 2026-09-30, T2 σ_m 0 / 6 % | 2026-12-31, T2 σ_m 0 / 6 % |
|---|---|---|---|---|
| positions, months | ≈ 430, 3.5 | ≈ 983, 6.5 | ≈ 13, 3.3 | ≈ 29, 6.2 |
| 0 (false confirm) | 0.076 / 0.066 | 0.052 / 0.050 | 0.061 / 0.070 | 0.066 / 0.065 |
| 2 %/yr | 0.17 / 0.12 | 0.28 / 0.15 | 0.38 / 0.13 | 0.71 / 0.20 |
| 3 %/yr | 0.24 / 0.16 | 0.45 / 0.22 | 0.58 / 0.18 | 0.95 / 0.30 |
| 5 %/yr | 0.40 / 0.25 | 0.79 / 0.41 | 0.87 / 0.31 | 1.00 / 0.53 |

**The holdout alone falsely confirms in 0.050–0.076 of null draws**
(t_{G−1}(0.95) on the clusters actually present).
- At the 2026-12-31 end it is at the nominal 5 % for T1.
- At the 2026-09-30 end (3–4 clusters) and for T2 it runs 1–3 points
  above. The cluster-robust SE with so few clusters is not exactly
  t-distributed.
- On the synthetic test with 4 clusters it is 0.049 (§8).
- A discovery pass is required first, so the path's joint false-pass rate
  is ≤ 0.003 (below).

**Joint power of the whole path** (discovery pass × holdout CONFIRM, T1,
monthly regime shock σ_m = 6 %/yr, **2,000 simulations** drawn together;
`carry.simulate_joint`):

| holdout design | true 0 | **2 %/yr** | **3 %/yr** | **5 %/yr** | P(discovery pass) at 2 / 3 / 5 % | P(confirm given a pass) at 2 / 3 / 5 % |
|---|---|---|---|---|---|---|
| end 2026-09-30 | 0.000 | **0.023** | **0.060** | **0.20** | 0.19 / 0.41 / 0.84 | 0.12 / 0.15 / 0.24 |
| end 2026-12-31 | 0.003 | **0.035** | **0.10** | **0.35** | 0.23 / 0.44 / 0.85 | 0.15 / 0.23 / 0.41 |
| end 2026-12-31 + extension to 2027-06-30 (two looks at t_{G−1}(0.975)) | 0.000 | **0.040** | **0.13** | **0.50** | 0.20 / 0.41 / 0.83 | 0.20 / 0.32 / 0.61 |

- With the exact thresholds and the extension, the path demonstrates a
  true 3 %/yr edge about one time in eight, and a true 5 %/yr edge half
  the time.
- Discovery's point bar (criterion 5) and its 19 month clusters cap the
  first factor; the holdout's few months cap the second.
- **A non-confirm means "not demonstrated at this power", never "no
  edge".**

## 4. The trade (T1; T2 is its core-coin subset)

**Decision grid.** t ∈ {00:00, 08:00, 16:00 UTC}; every CEX interval in the
data (1, 2, 4, 8 h) divides 8 h.

1. **Signal, from settlements strictly before t.** For each venue v,
   F_v(t) = sum of funding rates with settlement T in [t − 24 h, t). **The
   rate at t is never read** (a test poisons it and the decision does not
   change; a leaky window is caught). Annualised spread of a pair (A, B):
   s = (F_A − F_B) × 365. No predicted rates (no free history).
2. **Hold and threshold — frozen cost arithmetic (D5).**
   - C_design = 2 × (taker_A + taker_B) + 4 × design slippage. Taker base
     tier: HL 4.5, BN 5.0, BY 5.5 bp. Design slippage 0.5 bp core / 2 bp
     others (the scout's quoted-spread proxy).
   - m = 6 %/yr on N (HLP's 3 %/yr lending leg on gross capital 2N).
   - H = C_design / m rounded up to 8 h; θ = C_design / H + m.

   | pair | C_design core / other | H core / other | θ core / other (%/yr) |
   |---|---|---|---|
   | HL-BN | 21 / 27 bp | 312 / 400 h | 11.90 / 11.91 |
   | HL-BY | 22 / 28 bp | 328 / 416 h | 11.88 / 11.90 |
   | BN-BY | 23 / 29 bp | 336 / 424 h | 12.00 / 11.99 |

   The P&L charges the **tiered** slippage of step 4, not the design
   slippage. H and θ do not move with it (frozen, `000f`).
3. **Eligibility at t (known at t).** For each leg's own venue:
   - listed ≥ 7 days and settling;
   - **trailing 30-day mean daily quote volume ≥ $10 M** (UTC days before
     t's day; missing days count as 0);
   - **name check**: median |ln(close / Binance close)|, after the nearest
     power-of-10 scale, over closes in [t − 7 d, t) ≤ 5 % with ≥ 3 closes
     (HL, BY; BN is the reference);
   - a last-trade close on both venues at t.
4. **Entry.** If the coin holds no position and an eligible pair has
   |s| ≥ θ: take the largest |s| (ties: cheaper C_design, then the order
   HL-BN, HL-BY, BN-BY). **Short the higher-funding venue, long the
   lower**, equal coin size, notional N each, 1× each, separate margin,
   taker on four fills. Slippage per fill and leg = the tier of that
   venue's 30-day mean volume at t: 1 / 2 / 4 / 6 bp (≥ $1 B / $200 M /
   $50 M / $10 M).
5. **Exit.** Fixed at **t + H**; no early exit, no roll. A leg that stops
   settling closes the position at its last settlement. The coin may enter
   again at its exit time with full costs.
6. **Edge.** A position whose exit would reach the span's end is not
   opened.

**P&L per position, on N** (`ct/carry.outcome`):

- **Funding:** each leg's settlements with t_in < T ≤ t_x at its own
  interval (held at a stamp = the full rate). Short receives, long pays.
  Each settlement is scaled by P(T) / P(last reset), where the reset is the
  entry or a rebalance (size × price; Binance 8h closes).
  - **Funding holes (m-d).** A NaN rate is never a zero; it is dropped,
    leaving a hole.
  - A leg's spacing above 1.5 × its median interval is a hole. Its missing
    settlements are counted, and **both** legs' settlements inside the hole
    are left out of the accrual.
- **Price gap (N4, N6).** Eligibility at t reads the closes of the bar
  ending at t (known at t). The **fill** reads the first trade after the
  settlement at t on each venue: the open of the bar starting at t (BN 8h,
  BY 240-minute, HL 8h). Where an open is missing, the close at t is used
  and counted.
  - fill_dir(t) = ln P_short − ln P_long at the fill; P&L = fill_dir(t_in)
    − fill_dir(t_x).
  - The exit observation is the latest at or before t_x within 24 h at
    which both legs pass the name check. If none passes, the **raw** latest
    observation within 24 h is used: a divergence stays in the primary.
  - If no price exists within 24 h, the position gets **the worst observed
    return of its pair in the span**.
  - The raw-exit and worst-fill counts are printed before the decision.
- **Fees and slippage:** 2 × (taker_A + taker_B) + 2 × (slip_A + slip_B).
- **Rebalancing:** on the Binance 8h path from the entry close, a bar with
  an adverse move ≥ 50 % on either leg forces a rebalance at its close:
  both legs are closed and reopened at N (one more round trip at the
  position's cost) plus 10 bp of the moved amount. The reference and the
  funding weight reset.
- **Liquidation (B3):** a bar whose high reaches the short leg's 1×
  distance (1 − mm)/(1 + mm) liquidates it.
  - The short loses its whole margin, **−N**.
  - The long is closed at that bar's close, gaining close/ref − 1.
  - The gap term is dropped; fees are charged in full.
  - mm: HL 2.5 % core / **16.7 %** other; CEX 0.5 % / 2.5 %. The
    distances are 95.1 / **71.4** / 99.0 / 95.1 %.
- **Net return on gross capital:** R = (funding + gap − costs) / 2. Hold
  years = (t_x − t_in) / 8,760 h.

**Where every parameter came from.**

| parameter | value | source |
|---|---|---|
| grid, window | 8 h, 24 h | settlement intervals (not tuned) |
| taker fees | 4.5 / 5.0 / 5.5 bp | venue pages (scout B1) |
| design slippage | 0.5 / 2 bp | scout B2 proxy; sets H, θ only |
| margin m | 6 %/yr on N | HLP lending leg × 2 |
| H, θ | C_design / m ↑8 h; C_design / H + m | cost arithmetic, frozen (`000f`) |
| P&L slippage tiers | 1 / 2 / 4 / 6 bp per fill at $1 B / 200 M / 50 M / 10 M | 009's tiers (tick bound and Abdi–Ranaldo bounds, 009 §4) |
| liquidity floor | $10 M, 30-day mean, each leg's venue | 009's floor; capacity at N = $10 k |
| seasoning | 7 days | a priori |
| name check | 5 % | a priori |
| top-up trigger, transfer | 50 %, 10 bp | margin arithmetic; scout B3 |
| maintenance margins | HL 2.5 / 16.7 %, CEX 0.5 / 2.5 % | scout B4, conservative end |
| z | week 2.24; month t_{G−1}(0.975) | Bonferroni over T1, T2; the orchestrator (M1) |
| periods | as §2 | orchestrator; holdout end is the owner's |

## 5. Pass — each trial, all six

On R per position:

1. mean > 0 with **week-clustered t ≥ 2.24** (ISO week of entry);
1b. mean > 0 with **month-clustered t ≥ t_{G−1}(0.975)** — 2.10 at
   G = 19 months;
2. mean and median the same sign;
3. positive in both count halves of discovery;
4. **mean > 0 without the calendar month with the largest positive summed
   R** (a loss month is never removed);
5. **annualised net on gross capital ≥ 3 %/yr** (sum R / sum hold years):
   the economic point bar. It passes about half the time at a true 3 %/yr
   by construction, and more often at 2 %/yr under regime dependence (D12).

**The holdout confirm rule is in D13.** It is an existence test; the
3 %/yr bar is reported there and becomes the paper period's deploy
threshold.

**Placebo, reported honestly.**
- Every calendar month's positions get one random sign on their gross
  (funding and gap flip with the direction); the statistic is the
  month-clustered t at t_{G−1}(0.975). The same is done with weeks at
  1.96. 200 seeds.
- The informative number is **the share of seeds with |t| ≥ the critical
  value on the flipped gross alone**, nominally 5 %. On synthetic noise it
  is 5.8 % (months) (§8).
- **With costs kept the pass share is ≈ 0 by construction** and calibrates
  nothing; it is printed only for completeness.

**Leg-liquidation stress (reported):** the largest adverse single-leg move
during holds against the 1× margin distance, plus the counts of rebalances
and liquidations.

## 6. Reported, decides nothing

- **The exposed 2025-01-15 → 2026-06-30 block** (computed in the holdout
  run, labelled exposed, `000f`).
- **The OKX variant** (all six pairs). It is survivorship-biased: OKX coins
  that later died have no prices or closes and cannot form a pair (B2).
- Per pair; HL pairs vs BN-BY; others; slippage × 2 (on every round trip
  including rebalances); funding-only.
- **The drop-missing variant** (worst fills left out) and **the
  valid-exit-only variant** (raw exits left out). The primary keeps both
  (N4).
- The month and week placebos; components (funding, gap, fees,
  rebalancing); the stress block.
- Counts of delisted, rebalanced, liquidated, raw-exit, worst-fill,
  funding-hole and fill-fallback positions.

**No reported block may become a primary.** A pair, a coin set, a hold, a
window, an exit rule, a roll, OKX, or a cost level is a **new trial** in the
L2 family, decided on data after the holdout end.

## 7. Known weaknesses, stated now

- **Exposure (`000f`).** Discovery's funding-leg persistence is known to be
  substantial. A discovery pass is weak evidence on its own; the 2026-H2
  holdout decides. A fail remains informative: it would mean costs, the
  gap or the tails ate a persistent carry.
- **The holdout is short.** It runs 3 months (about 430 positions) or 6
  months (about 983), with an optional extension to 12, in the compressed
  regime. Its power bound and the joint power are in §3.
- **Fixed hold, no roll.** A persistent spread pays C every H. This is
  conservative (≈ 3 %/yr on gross capital for a spread that never dies).
- **Taker at today's base tier.** Maker execution would be a new trial.
- **Fill timing (N6).** The fill is the first trade after the stamp, so
  the short leg no longer sells at a pre-settlement premium it was not
  charged for. Where an open is missing, the close before the stamp is
  used.
  - That residual bias is ≲ one interval of the entry spread: about 1.5 bp
    on N at the median |s| and 4 bp at p90, i.e. ≈ 0.15–0.4 %/yr on gross.
  - It favours the hypothesis, and only on the counted fallbacks.
- **Composition (N8).** T1 is 87 % Binance–Bybit. Its result transfers to
  HL or lawful pairs only through a new, untested registration.
- **Few clusters in the holdout.**
  - At t_{G−1}(0.95), 3–7 month clusters still give a false-confirm rate of
    0.050–0.076 on its own: the cluster SE with few clusters is not exact.
  - The discovery pass in front keeps the path ≤ 0.003.
- **Slippage** tiers are 009's bounds, not measured impact. No
  period-matched Binance `bookDepth` for 2023–24 was pulled. Slippage × 2
  is reported.
- **HL liquidity in 2023–24 was thin.** The per-leg HL floor removes most
  HL alt pairs in discovery (§3), so T1 is dominated by BN-BY.
- **Price path from one venue.** Rebalances and liquidations read the
  Binance 8h high/low; a venue-specific wick is missed. A liquidation is
  assumed when the high touches the distance within a bar.
- **Venue and counterparty risk is not in the P&L** (scout F4).
- **OKX dropped.** The lawful leg candidate (OKX Europe) is untested; the
  OKX variant is survivorship-biased.
- **Universe.** Coins without a Binance perp are out.
- **Dependence.** Holds span 2–3 weeks and regimes span months; the
  binding statistic under regime shocks is criterion 1b with only 19
  clusters. The power simulations are Gaussian.
- **Return on capital deployed.** Idle capital is not counted.
- **Exposed-block counts** drop the entry-price requirement (no 2025–26
  prices on disk) and include HL funding only for the 80 HL coins whose own
  volume reaches the floor in 2025–26. Coins that never reach it can never
  be eligible, so the counts are unchanged by that restriction.

## 8. Built before any outcome is computed

- [x] `ct/carry.py`:
  - funding series with hole detection, the spread, the frozen H/θ
    arithmetic and tiered slippage;
  - `plan` (held coins skipped first; returns "opened" only);
  - `outcome`: funding at each venue's interval with a reset weight and
    holes left out; the post-settlement fill; the 24-h exit with the raw
    fallback; delisting; rebalances; the B3 liquidation loss;
  - the six criteria with t_{G−1} tables, the honest placebo and `decide`;
  - `recheck_discovery` (recomputes from stored numbers);
  - `holdout_confirm` (the existence test) and `extension_due`;
  - `simulate_power`, `simulate_holdout` and `simulate_joint`.
- [x] `r010_fetch.py` — bounded fetchers (30 s timeouts, retries). The
      gated `holdout` / `extension` pull: a point-in-time universe, no
      cached empty answers, and a started row.
- [x] `r010_build.py` — the discovery panel (closes and post-settlement
      opens). The gated holdout/extension panel has a coverage check and a
      built row.
- [x] `r010_run.py`:
  - `Panel` (per-leg floor, tiers, name check incl. HL, fill; refuses
    holdout panels unless guarded);
  - the guard: status; (a)–(e); (f) over the tree and every branch's
    history; (n1); (g1)–(g4); (h'); (t); (i); (x); and (h), the verified
    000g ordering;
  - the discovery, holdout and extension runs with started rows;
  - `--events` (unconditional counts only).
- [x] `r010_describe.py` — the §3 numbers, the power bound and the joint
      power.
- [x] `tests/test_carry.py` — **142 checks pass:**

      cd research/copytrade && .venv/bin/python -m tests.test_carry
      ... (142 x PASS) ...
      all checks passed

New in Revision 2:
- **N1 on the real file:** exactly one line of each, both UNSET → refused.
  A valid date and NO are accepted; an off-offer date, two lines, or a
  quoted or indented line are refused.
- **N2 gate:** (g1) no row; (g2) an uncommitted row; (g3) code changed;
  (g4) forged numbers; (h') a missing approval, or an approval committed
  together with the row; (t) before end + 7 days; (i) build before fetch,
  run before build, a second read. With everything in order the gate is
  open.
- **N3 probes inverted:**
  - Each of these unlocks nothing:
    - an uncommitted 009 close;
    - rows only on another branch;
    - a foreign registration commit;
    - a label the numbers do not support;
    - a 009 pass with holdout A pending;
    - a declined row with the wrong sha;
    - a row deleted from HEAD.
  - Verified close, close_conditions_unmet, declined and holdout_A rows
    unlock.
- **Recheck:** a 010 row's decision is recomputed from its numbers; forged
  numbers or labels are caught.
- **N4:** a failed name check at exit uses the raw exit gap; no price at
  all leaves the outcome to the worst fill.
- **N6:** the post-settlement fill, with a counted fallback to the close.
- **m-d:** a missing settlement is counted, and both legs leave the accrual
  inside the hole.
- **N5:**
  - an existence-test CONFIRM below the 3 % bar;
  - one huge month fails;
  - the extension is due only for 0 < t < t_{G−1}(0.975);
  - **Revision 2a:** a null with 4 month clusters and month-level shocks
    falsely confirms 0.049 of 2,000 times at t₃(0.95) = 2.353;
  - joint power: a 20 %/yr edge passes both stages, a zero edge almost
    never, and the extension path runs.
- **N7:** Panel refuses a holdout panel unless the run is guarded.

All Revision 1 checks remain (accrual, look-ahead, the rule, B3
liquidation, the rebalance weight reset, the placebo, the criteria, tiers,
the whitelist, the guard's (a)–(f) and DRAFT).

**Pre-specified holdout procedure.** The owner sets the two lines under §9
before registration. After a discovery pass, the owner commits an approval
line under the last D-row of §9, after the commit that added the
discovery row. It has the form `Holdout fetch: APPROVED <date> row <sha256
of that row>`, at the start of a line. Then, no earlier than the end + 7
days:

    .venv/bin/python -I r010_fetch.py holdout      # refuses unless the gate of r010_run passes
    .venv/bin/python -I r010_build.py --holdout    # coverage check; writes holdout_built with the panel hashes
    .venv/bin/python -I r010_run.py --holdout      # checks those hashes; holdout_started; the read, once

For the extension (only if pre-registered and due), an approval line of
the form `Extension fetch: APPROVED <date> row <sha256 of the holdout row>`
is committed in the same way, then the same three commands with
`extension` / `--extension`, no earlier than 2027-07-08.

**SHA-256 of the code and data the outcome depends on** (read by the guard;
recomputed by the orchestrator at commit):

| file | sha256 |
|---|---|
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/carry.py` | `38fbaeb55ca4818d3293c046f9656be4a5d4eddcf14b8243933c6dcea920720b` |
| `ct/eventstudy.py` | `08a780b4d40bad811d5d826d42d29c44d673246048d85cf31ec1be2eaf6018b0` |
| `ct/factors.py` | `a1e70b9a5a2c1b58f43d2b08a2eaeac5ce5a3fd45868a659d745a3354bd61197` |
| `ct/stats.py` | `bfcc85f285dc7af28f364e2d77e0fbe04b6c2367724f4b822b7fbf6ff435d472` |
| `ct/net.py` | `90092590941e805eb465e7e1f05467c05e376bf8b07fe6dbdacfdb6cf7e00006` |
| `r010_run.py` | `edca47afa70a45d8bf88e6010ddb6f337d0a008632d1b9f8ccf7ab3d5ae1581b` |
| `r010_fetch.py` | `2515c6b97630628898bdfd0251c9fe8dfb3d0ffacebb4e1ea2edaa87da45c8a3` |
| `r010_build.py` | `53b6433da32959f182e2c5ba812ec54944283a6d9a7404a9d1938405f848e45d` |
| `scouts/l2_fetch.py` | `3fcb6adde0b6f0a0af5e262d1c65a21ef6d71ddf6f785c3b20f21d3827abdc37` |
| `scouts/h2_binance_scout.py` | `8fa6cba20d45b15a6d9e12be87187a351fdeef503f94e68ae22f0c66f28805e9` |
| `data/carry/panel/universe.json` | `dc450d9677e792bd283a6cbd263464cc7751f9f256e892873563b3f7917ccdeb` |
| `data/carry/panel/funding.parquet` | `db3fbee09befd7b9ce2b50fe21987017b0ff14cdf848620e2d4ff1c6c41ade4c` |
| `data/carry/panel/prices.parquet` | `7e92c132f8be8eff1dcdf5f0f06f72e9241380f2702c558cb2de7e871cd5834c` |
| `data/carry/panel/path.parquet` | `b0fa7ad76124b613853dcc588dc9dfabfcd6528f74709f140de023b4d2beb033` |
| `data/carry/panel/volume.parquet` | `4b84565fad5f40a2d6c76ed19abc299a17bc74c4cf89d91ab8f99782feacef94` |
| `data/carry/panel/levels.parquet` | `986ec1ce1baca92d70f16069b1c182ecce34d569f12f1228087c5e0688ec596d` |
| `data/carry/inputs/um_fundingRate_symbols.json` | `6a6f5735ebb6ad2ab30f10bbeeb36292323aef171f6570c42dacf6cf5987dc53` |
| `data/carry/inputs/hl_first.json` | `c2620a2f00943506fb0570f540af3dcf340fa98d0886a3c3d88a1b38ac6eb360` |
| `data/carry/binance/months.json` | `49bc6f93ad6d51050fc46f3af5398d7773e77be1f18acac66839d99a6740cbc5` |
| `data/carry/bybit/symbols.json` | `bdb86ddfcbb767e71b6e98790190205102a99c6ba86db25692dc7e96201ef896` |

`ct/factors.py` and `ct/stats.py` are 009's modules, imported read-only by
guard (h). The panel is rebuilt with `.venv/bin/python -I r010_build.py
--with-hl-hold` from the raw downloads. The holdout panel's hashes are
written into its `holdout_built` row and checked by the run.

## 9. Owner decisions — PROPOSED

| # | item | proposed |
|---|---|---|
| D1 | venues, universe | HL, BN, BY USDT perps; coins = Binance USDT-M symbols (delisted included), pairs HL-BN, HL-BY, BN-BY where both list the coin point in time; OKX a reported variant (survivorship, B2); Bitget excluded |
| D2 | periods | discovery 2023-06-08 → 2024-12-31 (exits before 2025-01-01), exposed on persistence (`000f`); exposed block 2025-01-15 → 2026-06-30 reported only; **decisive holdout 2026-07-01 → the D14 end** |
| D3 | signal | trailing 24 h realised funding per venue from settlements in [t − 24 h, t), never the rate at t; s = (F_A − F_B) × 365; decisions at 00/08/16 UTC |
| D4 | costs | taker HL 4.5, BN 5.0, BY 5.5 bp; P&L slippage per fill per leg 1 / 2 / 4 / 6 bp by that venue's 30-day mean daily quote volume (≥ $1 B / $200 M / $50 M / $10 M) |
| D5 | hold and threshold — **frozen** | H = C_design / 6 %/yr ↑8 h (312–424 h); θ = C_design / H + 6 %/yr (11.88–12.00 %/yr); C_design uses 0.5 / 2 bp design slippage. No change after `000f` |
| D6 | eligibility | each leg's venue: listed ≥ 7 d and settling; 30-day mean daily quote volume ≥ $10 M; name check 5 % vs Binance (HL, BY); last-trade close on both venues at t |
| D7 | entry | one position per coin; largest |s| with |s| ≥ θ (ties: cheaper C_design, then pair order); short the higher-funding venue, long the lower; equal size, 1× per leg, taker |
| D8 | exit | fixed at t + H; delisted leg → last settlement; re-entry at the exit time with full costs; no early exit, no roll |
| D9 | P&L | funding at each venue's stamps, weighted by P(T)/P(last reset), funding holes counted and left out of both legs; the gap at the **post-settlement fill** (open of the bar starting at t; close where missing, counted); exit within 24 h with both names valid, else the **raw** exit gap, else the **worst observed return of the pair in the span** — no position leaves the primary; costs; rebalance at 50 % adverse (one more round trip + 10 bp of the moved amount); liquidation: the short leg loses −N, the long leg closes at the bar close, gap dropped, full fees; mm HL 2.5 / 16.7 %, CEX 0.5 / 2.5 %; R = net on N / 2 |
| D10 | trials and inference | T1 pooled (primary), T2 core; week-clustered t ≥ 2.24 (Bonferroni); month-clustered t ≥ t_{G−1}(0.975) (2.10 at 19 months) |
| D11 | pass (each trial) | the six criteria of §5, criterion 5 the economic point bar (annualised net on gross capital ≥ 3 %/yr) |
| D12 | decision — fail (both) | **L2 closes as a strategy candidate; the edge is "not demonstrated".** P(discovery pass) under a 6 %/yr monthly regime shock (2,000 simulations) at a true 2 / 3 / 5 %/yr: **0.21 / 0.40 / 0.82 (T1)**, **0.19 / 0.36 / 0.76 (T2)**. A fail is strong evidence only against edges well above the bar. The holdout is not read |
| D13 | decision — pass (either) → the holdout | the owner commits the approval line of §8; then, once and only through the gate, fetch → build → `r010_run.py --holdout`. **CONFIRM (per passing trial) = an existence test:** mean net > 0 with **month-clustered t ≥ t_{G−1}(0.95)**, G the month clusters actually in the window (Revision 2a); median > 0; mean > 0 without the largest positive month. **The annualised net (3 %/yr bar) is REPORTED**: it is the deploy threshold of the follow-up paper registration, not a confirm criterion. CONFIRM → a registered forward **paper** period on a lawful venue pair, which treats that pair as **untested** (T1 is a CEX–CEX result, N8). A non-confirm reads **"not demonstrated"**, never "no edge". **Joint power** (discovery × confirm, T1, σ_m 6 %/yr, 2,000 sims) at a true 2 / 3 / 5 %/yr: end 2026-09-30 **0.023 / 0.060 / 0.20**; end 2026-12-31 **0.035 / 0.10 / 0.35**; end 2026-12-31 + extension **0.040 / 0.13 / 0.50**. Joint false-pass ≤ 0.003; holdout-alone false confirm 0.050–0.076. The exposed 2025–26 block is reported in the holdout run and decides nothing |
| D14 | holdout end and extension (owner) | the owner sets the two lines below this table (exactly one of each; UNSET refuses). End: 2026-09-30 (data exist; ≈ 430 positions over 3–4 months; a pass reads it after 2026-10-07) or 2026-12-31 (≈ 983 over 6–7 months; read after 2027-01-07). **Extension:** if YES, both looks use month t ≥ **t_{G−1}(0.975)** (one-sided α 2.5 % each, Bonferroni over two looks = 5 %), and when the first look ends with 0 < month t < t_{G−1}(0.975) one extension to **2027-06-30** (cumulative from 2026-07-01, fixed now) is read once after 2027-07-07; if NO, one look at t_{G−1}(0.95). The holdout power is a **bound** (Gaussian at the raw SD; §3) |
| D15 | placebo | direction flipped per entry month (month t at t_{G−1}(0.975)) and per week (1.96), 200 seeds; the informative number is the |t| share on the flipped gross (nominal 5 %); the costs-kept pass share is ≈ 0 by construction |
| D16 | family | L2 count starts at 0; 2 trials here; shared selection: HLP benchmark, scout quantiles, `000f` persistence knowledge |
| D17 | **ordering (`000g`, programme-wide)** | 010 computes no outcome on Binance data dated ≥ 2024-01-01 — the discovery run included — until 009's holdout A reaches a terminal state recorded as a 009 row in `results.jsonl` (tree, HEAD or `git log --all`): a discovery row with decision `close` or `close_conditions_unmet`, a `holdout_A_declined` row, or a `holdout_A` row (`r010_run.ordering_state`, guard (h)). No 010 code reads outside `data/carry/` (whitelist; `data/binance_years/`, `data/scout_l2/`, `data/binance/` refused) |

Owner lines. The owner replaces UNSET with an offered date (2026-09-30 or
2026-12-31) and with YES or NO before registration. Each line stands
alone, starts the line, and appears once in this file.

Holdout end (owner): UNSET

Holdout extension (owner): UNSET

The runner reads its parameters from `ct/carry.py`; any change to D3–D11
before registration is a change there and here together, and a new hash
table.

## 10. Open questions for the orchestrator and the owner

1. **The holdout end and the extension.** My recommendation is 2026-12-31
   with the extension:
   - joint power at a true 3 %/yr is 0.13, against 0.10 without the
     extension and 0.060 for 2026-09-30;
   - the extension costs a t_{G−1}(0.975) bar at the first look but adds a
     second, longer look.
   - Neither design makes a near-bar edge likely to be demonstrated.
2. **The confirm critical value.** Resolved in Revision 2a: the
   threshold is t_{G−1}(0.95), and the holdout-alone false confirm is
   0.050–0.076.
3. **The per-leg liquidity floor** makes T1 87 % BN-BY (§1). BN-BY is not
   a lawful pair for a German resident (a warning only).
4. **Coins with no Binance perp are out** (the name check and the floor
   reference Binance).
5. **The 000f exposure.** Discovery is in-sample on the funding leg's
   persistence; the confirm needs unread data.

## 11. Disclosures and rules I was unsure about

- **N7 residue (proposed sentence for `000f`).** "The counts of positions
  opened, by span, pair and year, carry a weak lag-H trace of persistence
  through re-entry at exit; with the level quantiles one could interpolate
  a qualifying share within about a factor of 2. This is subsumed by the
  direct 51 % / 36 % figures above." `--events` and `r010_describe.py` now
  refuse any holdout panel. They never print a share of qualifying times,
  a re-entry count or per-coin counts.
- **Revision 2 reads on real data.**
  - Panel rebuilt from the raw downloads already on disk (no fetch): it
    adds post-settlement opens.
  - The §3 joint power reuses the counted entry patterns and level SDs.

- **`000f` (B1).** The draft's funnel split was a persistence statistic.
  - About 51 % vs 1.9 % in discovery and 36 % vs 0.6 % in 2025-01 →
    2026-06 (CEX pairs).
  - Seen by the drafter, the reviewer and the orchestrator; the
    persistence-bearing log files were deleted.
  - In this revision, `plan` also printed "edge" counts once before I
    removed them: 1,118 (discovery) and 614 (exposed). These are free-coin
    qualifying decisions inside the last H before a span end, a weak
    state-conditional count.
- **`000g`.** Binance funding (2023-05 → 2026-06) and 8h prices (to 2024-12)
  in `data/carry/` cover 009's holdout A. The pre-guard outputs over
  Binance 2024-01-01 → 2025-07-28 are all unconditional:
  - counts of positions opened (by pair, class, year, slippage tier,
    hold);
  - spread-level quantiles and SDs, and ρ;
  - gap-level SDs (prices to 2024-12);
  - delisted-coin coverage;
  - |s| and cost quantiles at entry;
  - the power simulations' entry patterns.

  No per-coin series, no conditional or forward statistic, no funding P&L.
  The scout's yearly Binance funding medians were disclosed in `000g`.
- **Holdout-window and exposed-window levels.**
  - 2025–26 spread levels and ρ, used for holdout power.
  - Daily and 8-hourly closes to 2026-06-30 (name check).
  - Daily volumes to 2026-06-30 (floor and tiers).
  - HL 8h candles to 2026-06-30.
- **The draft's HL premium-model check** read HL archive trade prices on
  11 days of 2025-08 → 2026-06 (levels, core coins). It also loaded the
  scout's full core HL funding file to 2026-06 for premiums; only 264 hours
  each were used. That model is now retired.
- **New real-data reads in this revision.**
  - HL `candleSnapshot` 8h for 213 coins, 2023-05-25 → 2026-06-30, plus 6
    end-bounded probes (BTC 8h / 1d / 1h, RNDR, MATIC, STRAX in 2023).
  - HL funding 2025-01 → 2026-06 for the 80 coins whose HL volume reaches
    the floor in 2025–26 (chosen from the candles).
  - Metadata queries on the panel: Bybit coins lacking closes (timestamps
    and existence).
  - A copy of the scout's two name lists into `data/carry/inputs/`.
  - All end-bounded; 0 rows at or after 2026-07-01.
- **The first draft's describe** printed the 2023–24 and 2025–26 position
  counts with OKX, the premium-model errors, and power. These are
  superseded.
- **Listing endpoints.** Binance S3 key names per symbol, names only
  (scout precedent).
- **Fetch hygiene.** HL requests now time out at 30 s with bounded retries.
  A hung job after a machine sleep was killed by the orchestrator and
  resumed from cache.
