# 012 — Hyperliquid–CEX funding carry: 010's frozen rule on the HL–BN and HL–BY pairs, decided on fresh data (L2, trial 3)

**Status: REGISTERED 2026-10-12** (orchestrator, under the owner's delegation: owner, 2026-10-12: "you are allowed to register yourself and take decision as you would recommend as long as they take us nearer towards the goal"). Previously DRAFT

**Revision 2 (2026-10-11, after independent review).** Review:
`registrations/reviews/012-review.md`. Every fix is in 012's own files; 010's
files are untouched and their hashes in §8 still equal 010's §8.
- **B1, the Stage-2 build could not run.**
  - The Binance listing is now saved with `_save` and re-listed on every
    fetch attempt; `_memo` handles row answers only.
  - A new end-to-end synthetic test runs the gated fetch → build → run on
    a fixture Stage-2 window, for both `stage2` and `extension` (§8). The
    fixtures include a listing, an HL 500, an empty Bybit coin, a Binance
    symbol HL does not list, and a December file missing on the first
    attempt.
  - Result: a clean coverage stop, then a resumed fetch, a build and a
    decision. Nothing crashes.
- **M1, missing Bybit prices.**
  - No error and no null is ever cached. A non-answer is retried, then
    raises a typed `FetchError`.
  - **The root cause of the Bybit holes was paging, not delisting.**
    Bybit's kline endpoint answers only the last `limit` bars before `end`,
    so backward paging returned nothing for a coin that stopped trading
    earlier in the window. Klines are now paged forward in windows of
    1,000 bars.
  - All 209 symbols were re-fetched (bounded before 2026-07-01): **204 now
    have rows, against 188**, including AVAX, VIRTUAL, AI16Z, MKR and PUMP.
    The 5 without are FET, LOOM, MATIC, RNDR and STRAX, which have no
    Bybit 240-minute bars in the window.
  - No Stage-1 coin is left with funding but no prices (0, printed before
    the decision). No delisted-instrument archive or exclusion is needed.
  - The panel was rebuilt (byte-identical on a second rebuild),
    re-hashed, and §3 recounted.
  - **For the orchestrator:** 010's `r010_fetch.by_kline` has the same
    backward paging. 010 is closed and its file is not touched here.
- **M2, Stage-2 completeness** (in 012's own build; tests in §8).
  - The fetch runs the build's coverage checks before it writes its
    `_fetched` row.
  - Refused, because a wait or a re-fetch fixes them:
    - a missing end-month Binance file;
    - a series with funding but no prices;
    - prices without funding;
    - a truncation at a month boundary.
  - Venue events a re-fetch cannot change are counted and printed before
    the decision: internal gaps, mid-month stops and delistings.
  - HTTP errors are typed; an HL 500 is an error, never "unknown coin".
    "Unknown" comes only from HL's `meta` universe or Bybit's explicit
    invalid-symbol code.
  - Binance kline months are listed separately from funding months.
- **M3, the owner lines are frozen at Stage 1.**
  - The Stage-1 row stores both lines; every later step requires them
    unchanged.
  - The registration may change after Stage 1 only by added approval
    lines.
  - Exactly one fetch (resumable until its `_fetched` row) and exactly one
    build. Each later step requires the earlier rows committed.
  - A `stage1_started` row with the same registration commit is required
    (m3).
- **M4, the headline error rate is Stage 2's own** false-confirm rate:
  0.043–0.052 under the owner design (§3, D14). The joint rate is
  secondary and holds only under a global null.
  - I chose to **state it, not calibrate**: 0.052 is within simulation
    error (±0.005) of 5 %, and it appears only in the pessimistic raw-SD
    bound at the 2025–26 rate.
  - 010's frozen t_{G−1}(0.975) is kept.
- **M5, family counts.**
  - **HL discovery year: trial 16.**
  - Binance holdout-A tail and holdout-B calendar disclosed.
  - Holdout family of 2, family-wise ≤ 2α.
- **M6:** D18 is replaced with the reviewer's wording, adjusted (a)–(g).
  012's code refuses any read dated on or after 2026-07-01 outside its
  Stage-2 gate (tested).
- **MINORs:**
  - month-aligned power windows, with the 2026-H1 rate as the headline
    (m1);
  - a real (g4) test (m2);
  - the `stage1_started` requirement (m3);
  - the close-stamp note (m4);
  - 79 vs 80 HL coins (m5);
  - a reported block liquidating the HL short on HL's own highs (m6);
  - a liquidation-jump note in the power section (m7);
  - the paper venue pair fixed now (m8);
  - 010's (h) probes cited (m9).

**Revision 1 (2026-10-11, orchestrator, before review).**
- **The primary rule is kept:** the rule chooses among HL pairs only (§0
  gives the reason). The lead's construction stays a reported variant.
- **Stage 1 becomes a SIGN gate:** PASS iff the point estimate of the mean
  net per position is > 0. The month-clustered t (with its t_{G−1}(0.95)
  reference), the median and everything else are reported, never required.
  - Rationale: Stage 1 is exposed (`000f`), so it can only filter, never
    confirm. A full t gate on it cost too much joint power (0.054–0.24 at a
    true 3 %/yr in the draft).
  - Under the null the sign gate passes about half the time (0.48–0.51 in
    simulation), so the path's false-pass rate is carried by Stage 2 alone.
  - Joint power is recomputed (§3, D12, D14).
- **The Hyperliquid holdout allocation** is drafted as D18 for the owner.
  The same row releases 010 §6's "after the holdout end" condition through
  010's closure.
- **The owner lines** stay 2026-12-31 / YES, to be confirmed at
  registration.
- Tests re-run (§8); §8 hashes updated. The drafting reads are ledgered as
  `000h`.

Drafted 2026-10-11 for the orchestrator and the owner. The decision table
(§9) is PROPOSED. The two owner lines under §9 carry 010's choices
(2026-12-31, extension YES), restated for the owner to confirm at
registration. The mechanics are built and checked on synthetic data (§8).
On real data only behaviour was measured (§3): **no funding accrual, price
change, gap change or P&L was computed on real data, in any period, and
nothing dated on or after 2026-07-01 was fetched.** The runner
(`r012_run.py`) refuses to compute an outcome until this file says
REGISTERED, is committed unmodified, the code and panel match §8, no 012 row
exists anywhere in `results.jsonl`'s history, and the 000g ordering allows
it.

Programme: `docs/edge-discovery-2026-10-11.md` (L2; the 2026-10-11 entry
"010 closes, with one lead"). Inherits: `registrations/010-cross-venue-funding-carry.md`
(REGISTERED, result §12). Reviews whose findings are carried over:
`reviews/010-review.md`, `reviews/010-review-r1.md`. Ledger entries read:
`000d`, `000f`, `000g`, and `000h` (this registration's own drafting reads).

## 0. Inheritance and disclosure

**The hypothesis.** Hyperliquid perpetual funding sits structurally above
Binance and Bybit funding. A delta-neutral pair (short the higher-funding
venue, long the lower, 1× per leg, equal size) on an HL–CEX pair, entered
and exited by 010's frozen rule, earns more than its costs: fees, tiered
slippage, the change in the cross-venue price gap, rebalancing and the
liquidation tail.

**What is inherited, unchanged.** 010 §4 and D3–D9, by reference and by
code. The signal (trailing 24 h realised funding from settlements strictly
before t), θ and H (frozen cost arithmetic, `000f`), taker fees, tiered
slippage, the per-leg $10 M floor, seasoning, the name check, the entry
price requirement, the post-settlement fill, the fixed exit, delisting,
funding holes, rebalancing, the B3 liquidation loss, the N4 exit fallbacks,
and R on gross capital. `ct/carry.py`, `r010_run.py`, `r010_fetch.py` and
`r010_build.py` are imported **read-only** and are not modified, so 010's run
record (commit `8371981`, `results.jsonl` line 9) stays reproducible. Their
hashes are in §8 and equal 010's §8.

**The one change: the pair set.** `PAIRS_012 = (HL-BN, HL-BY)`. The rule
picks among HL pairs only (the largest |s| clearing θ; ties to the cheaper
C_design, then HL-BN before HL-BY). One position per coin at a time, as in
010. An HL entry is therefore never displaced by a wider Binance–Bybit
spread on the same coin. The block that produced the lead, "010's
three-pair plan, then keep its HL-pair positions", is a **reported
variant** (§6).

**Why the primary is the HL-pairs-only rule, not the lead's construction
(Revision 1).**
- It is the rule a trader would run. An HL–CEX carry book has no BN–BY
  leg, so a BN–BY spread cannot be a reason to skip an HL entry.
- The lead's construction lets a separate signal, on a pair that failed in
  010 (BN–BY −16.8 bp, t −3.45), decide when the HL carry is held. That
  dependency is an artefact of 010's one-position-per-coin rule across
  three pairs, not part of the hypothesis.
- It also tests more of the hypothesis: 681 vs 572 Stage-1 positions (Revision 2 counts).
- The cost is stated: the primary is not an exact replication of the lead.
  The variant is reported at both stages, so the two can be compared, but
  the variant decides nothing.

**Post-hoc selection, stated plainly.**
- The HL-pair restriction was chosen **after** reading 010's discovery
  result by pair. In 010's discovery (2023-06 → 2024-12) the reported
  `hl_pairs` block made **+24.3 bp per position on N, week t 3.60, month
  t 2.85, +5.6 %/yr on gross capital** (HL–BN 174 positions, +27.8 bp;
  HL–BY 114, +19.0 bp), while T1 failed (−11.3 bp) on its 87 % BN–BY
  share.
- 010 §6 states that no reported block may become a primary, and that a new
  pair set is a new trial in the L2 family. **This is that new trial.** The
  2023–24 numbers are its motivation, never its evidence.
- **Winner's curse.** The block is the best of a handful of reported cuts
  (by pair, HL vs CEX, core vs others), so +5.6 %/yr is an upper-biased
  estimate of any true HL-pair edge. Power is stated at 2 / 3 / 5 %/yr (§3),
  not at 5.6.

**What the earlier looks saw of Stage 1's span (2025-01-15 → 2026-06-30).**
- `000f`: the drafter, reviewer and orchestrator of 010 saw that, after an
  entry, CEX–CEX spreads stayed beyond θ at about a third of the hold's
  decision times in this span (36 % vs 0.6 %). The HL-pair version of that
  number was not printed. But the span's spread levels and quantiles, ρ, the
  entry counts by pair (010 §3: HL–BN 318, HL–BY 255, a weak persistence
  trace per 010's N7) and the scout's HL-minus-CEX quantiles were all read.
- **Stage 1 is therefore discovery-grade.** It is a **sign filter** (the
  mean net must be positive) that can close 012 cheaply. It is **not a
  confirmation**: a Stage-1 pass is not evidence of an edge, and it earns
  only the Stage-2 read.
- Its **P&L has never been computed.** 010's run was discovery only; 010
  would have computed this block in its holdout run, which never happened.
- `000g`: Binance data from 2025-01-15 to 2025-07-27 lie in 009's holdout A.
  009 closed (terminal state, `results.jsonl` line 7), so the ordering lock
  is released; 012 keeps 010's verified guard (h) all the same.
- `000d`: Bitget is excluded; nothing uses ARB specially.

**Stage 2 is the only evidence.** 2026-07-01 → 2026-12-31 (extension to
2027-06-30): funding, candles and klines nobody has fetched. It is the
window 010 reserved for its holdout and never read (010 closed at
discovery). D18 drafts the allocation of the sealed Hyperliquid holdout
calendar and the release of 010 §6's wording, for the owner.

## 1. Trials registered here

| id | universe | role |
|---|---|---|
| T3 | coins with an HL perp; pairs **HL-BN and HL-BY**, pooled, one position per coin at a time | the single trial; Stage 1 filters it on the sign of the mean, Stage 2 decides it |

- **Family count.** L2 counts 3 trials: 010 T1 and T2 (both failed) and
  012 T3. Shared selection with 010: the HLP benchmark (the margin m), the
  scout's quantiles, `000f`'s persistence knowledge, **and 010's
  discovery result by pair, which chose this trial.**
- **Hyperliquid discovery year (2025-07-28 → 2026-06-30): trial 16**
  (Revision 2, M5).
  - Stage 1 computes outcomes on HL funding and HL prices inside that year,
    which programme doc §2 puts at 15 trials.
  - As that section requires, a pass there (here, the Stage-1 sign gate)
    is the only reason to read the holdout.
- **Binance 2025–26 (Revision 2, M5).** Stage 1 also consumes Binance
  funding and prices for the HL coins over:
  - **the tail of 009's holdout A**, 2025-01-15 → 2025-07-27;
  - **009's holdout-B calendar**, 2025-07-28 → 2026-06-30, which was
    never registered for reading.

  009 is closed, so this is allowed (`000g`'s ordering lock is released).
  But after Stage 1 those windows can no longer be called unread for any
  funding hypothesis on these coins. Later registrations must state 012 as
  exposure there.
- **No multiplicity adjustment across L2.** T3 is decided on data no
  earlier L2 trial touched. Within Stage 2 the two looks of the extension
  are Bonferroni-split (t_{G−1}(0.975) each).
- **Holdout family (D18): count 2.**
  - Stage 2 reads HL, Binance and Bybit data dated on or after 2026-07-01.
    Under D18 that calendar may be read by at most two pre-registered
    rules: this one and L3 (a future 008).
  - Each rule is tested at its own α, with no adjustment. **The family-wise
    false-confirm rate is at most the sum, ≤ 2α.** For 012, α is 0.043–0.052
    by simulation (§3).
- **Written expectation.** About 2 in 3 that Stage 1's sign gate passes:
  it needs only a positive mean, but 2025–26 spreads are compressed and HL
  alt liquidity is thin. About 1 in 5 that Stage 2 confirms, given a pass:
  about 1 in 8 overall. The 3 %/yr deploy bar is less likely still.

## 2. Data and periods

| span | dates | use |
|---|---|---|
| 010 discovery | 2023-06-08 → 2024-12-31 | read by 010; source of the lead; decides nothing here |
| **Stage 1** | **2025-01-15 → 2026-06-30**, every exit before 2026-07-01 | the filter (discovery-grade, §0) |
| **Stage 2** | **2026-07-01 → the owner's end (2026-12-31)**, every exit before the end | **decisive**; read once, only through the gate of §8 |
| extension (pre-registered, owner line YES) | 2026-07-01 → **2027-06-30**, cumulative | read once, only if the first look ends with 0 < month t < t_{G−1}(0.975) |
| after the end | — | never read |

- **Venues:** Hyperliquid (HL), Binance USDT-M (BN), Bybit linear (BY).
  Legal access to Binance and Bybit from Germany is **a warning, never a
  blocker** (owner rule, 2026-10-11).
- **Stage-1 universe.** Every coin of 010's registered universe with an HL
  symbol: 213 coins (BN 213, BY 209), names from the S3 listing, delisted
  included. 210 have ≥ 2 venues with funding.
  - 010 fetched 2025–26 HL funding for the **80** coins whose own HL
    volume reaches the $10 M floor at some point in 2025–26. **79** of them
    have HL funding in the panel from 2025-01-15 on.
  - The 80th is FTM. HL delisted it in January 2025: its last non-zero
    print is 2025-01-12, before Stage 1 starts, and its later exact-zero
    prints are dropped by 010's builder rule.
  - Coins without 2025–26 HL funding can never pass the per-leg floor, so
    no position is lost (Revision 2, m5).
- **Stage-1 panel** (`data/carry012/panel_stage1/`, `r012_build.py
  --stage1`), 2024-11-01 → 2026-07-01, exclusive:
  - funding, daily volumes and name-check closes: 010's registered panel
    rows for those coins (HL, BN, BY; OKX dropped);
  - last-trade closes and post-settlement opens on the 8-hour grid, and
    the Binance 8h path, built exactly as 010's builder does. Sources:
    - BN 8h klines and HL 8h candles: 010's raw downloads, which already
      ran to 2026-06-30;
    - **BY 240-minute klines: fetched for 012** (`r012_fetch.py stage1`,
      Revision 2: forward-paged, errors never cached). 209 symbols, **204
      with rows** in the window; FET, LOOM, MATIC, RNDR and STRAX have
      none.
  - `hlpath.parquet`: HL's own 8h high / low / close, for a reported stress
    block (m6).
  - A rebuild is byte-identical (checked).
  - **Close stamps (m4).** A bar is stamped at its close, so 193 path rows
    and 172 hlpath rows (the 2026-06-30 16:00 bars) carry the stamp
    2026-07-01 00:00. Their content is pre-cut, and every `Panel` read is `t < end`. Funding,
    prices, volumes and levels hold no stamp at or after 2026-07-01.
  - **Coverage of the Stage-1 panel** (counts only; Stage 1 is not
    refused on them):
    - 600 venue-coin funding series;
    - 32 ending before the cut (delistings);
    - 2 internal funding gaps over 3 days (LIT's relisting, PUMP's Binance
      funding);
    - 1 price gap and 1 path gap (LIT);
    - 17 mid-month stops (for example TON's Binance funding ends
      2026-06-23).

    Run on Stage 1, the Stage-2 refusal rules would fire only on
    artefacts of 010's selective HL fetch: 102 HL funding series end at
    2024-12-31 because 010 fetched no 2025–26 HL funding for coins that
    never reach the floor, and 24 HL coins have candles but no funding in
    the window. They would also fire on MATIC's Binance funding without
    prices. None of these coins can ever pass the floor or be entered.
    **0 entry-eligible venue-coins have funding without prices.**
- **Stage-2 panel** (gated; `data/carry012/panel_stage2_2026-12-31/`;
  extension `panel_extension_2027-06-30/`):
  - pull window 2026-05-01 → the end (61 days of look-back for the
    30-day floor, seasoning, the name check and the 24 h window);
  - **point-in-time universe from Stage-2 listings** (re-listed on every
    attempt, Revision 2 B1):
    - every Binance USDT-M symbol with a funding file in [2026-05, the end
      month];
    - its HL coin, if HL's `meta` universe lists the name at fetch time.
      Funding and 8h candles are then fetched. An HL 500 is retried, never
      read as "unknown";
    - its Bybit symbol, probed with bounded funding requests. Only Bybit's
      explicit invalid-symbol code means "unknown".
    - Binance kline months are listed separately: a symbol can keep funding
      files after its kline files stop.
  - Binance funding and 8h klines; Bybit funding, 240-minute and daily
    klines; HL funding and 8h candles. Every request is bounded by the end.
  - **No error, null or empty answer is cached** in a Stage-2 pull. A typed
    failure stops the attempt, and the pull resumes on re-run.
  - **Coverage before `_fetched` and again at the build (M2).** Refused:
    - a missing end-month Binance file (wait);
    - funding without prices, or prices without funding;
    - a series truncated at a month boundary.

    Counted and printed before the decision: internal gaps, mid-month
    stops, delistings.
- **Hygiene.** Before the gate the fetch limit is 2026-07-01 and every
  primitive asserts its end bound before any request (tested).
  - **0 rows at or after 2026-07-01** were received or kept: 0 dropped
    at the bound in the fetch, 0 in the build.
  - Reads are whitelisted to `data/carry/` and `data/carry012/` (tested).
    No 012 source names `data/binance_years/`, `data/scout_l2/`,
    `data/binance/` or `data/cache/`.

## 3. What was measured before writing this (behaviour only, unconditional)

From `r012_describe.py` (`data/carry012/logs/describe.json`). **Recounted
in Revision 2** on the rebuilt panel (forward-paged Bybit prices). Counts
are of positions **opened**. Nothing is split by position state; no share
of qualifying times and no per-coin count is printed (`000f`).

**Terms (010's, frozen).** HL-BN: C_design 21 / 27 bp, H 312 / 400 h. HL-BY:
22 / 28 bp, 328 / 416 h (core / other). θ = 11.88–11.91 %/yr.

**Positions the rule opens.**

| | HL-BN | HL-BY | all | core | weeks / months | coins |
|---|---|---|---|---|---|---|
| **Stage 1, primary (012 rule, entry prices required; Revision 2)** | 373 | 308 | **681** | 87 | 74 / 18 | 78 |
| Stage 1, variant: 010's plan, HL subset (Revision 2) | 315 | 257 | 572 | 84 | — | 73 |
| *Revision 1 draft, before the Bybit fix* | *389* | *290* | *679* | *87* | *74 / 18* | *78* |
| 010 discovery, under 012's rule (counts only; its P&L is never computed) | 209 | 128 | 337 | 85 | 62 / 16 | 52 |
| 010 discovery, 010's plan, HL subset (the lead; 010 §12) | 174 | 114 | 288 | — | — | — |

- **Stage 1 detail (Revision 2).**
  - By year: 535 (2025) and 146 (2026-H1).
  - Short leg: HL 524, BY 100, BN 57.
  - Holds: 400 h (326), 416 h (268), 312 h (47), 328 h (40).
  - |s| at entry, p10 / p50 / p90: 12.3 / 15.5 / 35.7 %/yr.
  - Tiered round trip on N, p10 / p50 / p90: 25 / 35 / 40 bp. The most
    common slippage pairs are 6+4 bp (239 positions), 6+2 (162) and 4+2
    (75). The 6 bp leg is HL alts.
  - The fix moved 16 positions from HL-BN to HL-BY. It added 2 in total.
  - **Venue-coins with funding but no prices: 0.**
- **Stage 2, expected entries.**
  - At the 2025–26 average rate (38.4 per 30 days): month-aligned 6-month
    windows hold **234–268 positions**. 12-month windows hold 425–512.
  - **At the 2026-H1 rate (the headline, §7 compression) the first look
    holds 146 positions over 6 months**, and the extension about 292.

**Power** (`carry012.simulate_stages`; **2,000 simulations** per cell; 010's
noise model `carry._draw` with a **monthly regime shock σ_m = 6 %/yr**).
- **Stage 1** uses the 681 counted Stage-1 positions and the sign gate.
- **Stage 2 windows are month-aligned and common to both designs**
  (Revision 2, m1): exactly 6 calendar months at the first look (G = 6,
  t₅(0.975) = 2.571) and 12 at the extension.
  - **Headline: the 2026-H1 rate.** The first look is 2026-H1's 146
    counted positions. The extension adds the same pattern shifted by 181
    days, with months recomputed.
  - **Secondary: the 2025–26 average rate**, from the 6 month-aligned
    12-month windows of Stage 1.
- The two stages draw independent noise and independent shocks.
- Noise inputs are 010's published behaviour levels; nothing new about the
  spread was measured:
  - σ_s, the 2025–26 spread SD: **raw** core 8.3 / others 115.3 %/yr (the
    pessimistic bound) and **robust** 5.8 / 7.5 %/yr. The truth lies
    between them;
  - ρ = 0.031;
  - σ_g, the HL-pair gap SDs: 5.9 / 9.8 bp.
- **The model is Gaussian and has no liquidation jump (m7).** One HL alt
  short liquidation is R ≈ −0.15 on gross capital, about 15–25 % of Stage
  1's expected summed R at a true 2–3 %/yr. The pass rates at a positive
  edge are therefore optimistic. The null rates are unaffected in
  direction.

**The decisive error rate (Revision 2, M4) — Stage 2's own false-confirm
rate.** Owner design, true edge 0 in 2026-H2:

| entry rate | raw-SD bound | robust SD |
|---|---|---|
| **2026-H1 (headline)** | **0.044** | **0.043** |
| 2025–26 average | 0.052 | 0.043 |
| *(one look at t₅(0.95) instead: 2026-H1 / 2025–26)* | *0.051 / 0.053* | *0.050 / 0.054* |

- **This is the operative α.** A world with an HL–CEX edge in 2025–26 and
  none in 2026-H2 passes Stage 1 almost surely, so only Stage 2 protects
  against it.
- The raw-bound cell at the 2025–26 rate is **0.052, at the nominal 5 %
  within simulation error (±0.005)**. **Choice: state it, not calibrate.**
  010's frozen t_{G−1}(0.975) per look is kept.
- **Joint false pass, under a global null only** (no edge in 2025–26
  either): 0.022–0.024 at the 2026-H1 rate. It is secondary, because the
  sign gate passes about half of null worlds.

**Power, owner design (end 2026-12-31 + extension, t_{G−1}(0.975) at each
look), at the 2026-H1 rate (headline):**

| true net edge (gross capital) | **Stage 2 alone, raw / robust** | Stage-1 sign pass, raw / robust | joint, raw / robust |
|---|---|---|---|
| 0 | **0.044 / 0.043** | 0.51 / 0.49 | 0.024 / 0.022 (global null only) |
| 2 %/yr | **0.10 / 0.20** | 0.76 / 0.90 | 0.074 / 0.18 |
| 3 %/yr | **0.14 / 0.37** | 0.86 / 0.98 | 0.12 / 0.36 |
| 5 %/yr | **0.26 / 0.75** | 0.96 / 1.00 | 0.25 / 0.74 |

**At the 2025–26 average rate (owner design):**
- Stage 2 alone at a true 0 / 2 / 3 / 5 %/yr: 0.052 / 0.12 / 0.17 / 0.32
  (raw) and 0.043 / 0.20 / 0.36 / 0.73 (robust).
- Joint: 0.019 / 0.089 / 0.14 / 0.30 (raw) and 0.018 / 0.18 / 0.35 / 0.73
  (robust).

**One look at t₅(0.95), 2026-H1 rate (for comparison):** Stage 2 alone at a
true 0 / 2 / 3 / 5 %/yr: 0.051 / 0.099 / 0.12 / 0.21 (raw) and 0.050 /
0.19 / 0.29 / 0.55 (robust).

**Reading.**
- **Stage 2 alone demonstrates a true 3 %/yr edge 1 in 7 (raw bound) to
  3 in 8 (robust) times, and a true 5 %/yr edge 1 in 4 to 3 in 4 times.**
  At a true edge ≥ 3 %/yr the sign gate almost never blocks (0.86–1.00),
  so joint power is close to Stage 2's own.
- The Revision 1 figures (random 184-day windows, 6.5 clusters) flattered
  the extension: they drew windows from the high-entry months of 2025.
- **The discovery estimate (+5.6 %/yr) is upper-biased** (§0), so the
  5 %/yr row is an optimistic case.
- **A non-confirm means "not demonstrated at this power", never "no
  edge".**

## 4. The trade

010 §4, unchanged, with `pairs = PAIRS_012`:
- the 8-hour grid (00/08/16 UTC);
- the signal from settlements in [t − 24 h, t);
- frozen H and θ;
- eligibility per leg (listed ≥ 7 days and settling; trailing 30-day mean
  quote volume ≥ $10 M **on each leg's own venue**; the name check vs
  Binance at 5 %; a last-trade close on both venues at t);
- entry on the largest |s| ≥ θ among the coin's eligible HL pairs, short
  the higher-funding venue, long the lower, taker, slippage per fill by
  each venue's volume tier (1 / 2 / 4 / 6 bp);
- the fixed exit at t + H, no roll, re-entry at the exit time with full
  costs; edge rule (no exit at or past the span end);
- P&L exactly as 010 D9 (`carry.outcome` via `r010_run.outcomes`):
  - funding at each venue's own stamps, weighted by P(T)/P(last reset),
    with holes left out of both legs;
  - the gap at the post-settlement fill;
  - the exit within 24 h with both names valid, else the raw exit gap,
    else the worst observed return of the pair in the span;
  - costs;
  - rebalance at a 50 % adverse move;
  - liquidation of the short leg at −N (HL alt mm 16.7 %, distance 71.4 %);
  - R = net on N / 2.

`r012_run.Panel` loads 012's panels with 010's `Panel` methods inherited
unchanged, and keeps only coins with an HL leg.

## 5. Pass rules

**Stage 1 (sign filter, Revision 1), on R per position over 2025-01-15 →
2026-06-30:** PASS iff **the point estimate of the mean net per position is
> 0** (at least 3 positions).

- Reported, never required:
  - the month-clustered t, with t_{G−1}(0.95) beside it (18 months →
    1.740);
  - the median, the week t and the annualised net against 3 %/yr;
  - the mean without the largest positive month;
  - 010's six criteria.
- The stored row holds the mean, from which the decision is recomputed
  (`carry012.recheck_stage1`).
- Stage 1 is exposed (`000f`). It can only filter, so it asks for the
  sign alone.

**Stage 2 (decisive), on fresh data — CONFIRM is an existence test
(`carry.holdout_confirm`):**
1. mean net > 0 with **month-clustered t ≥ t_{G−1}(q)**, G the month
   clusters actually present;
2. median > 0;
3. mean > 0 **without the largest positive month**.

q = 0.95 with one look. **With the extension pre-registered (owner line
YES), q = 0.975 at both looks.** The extension is read only when the first
look ends with 0 < month t < t_{G−1}(0.975). It is then read once,
cumulative from 2026-07-01.

**The 3 %/yr bar** (annualised net on gross capital, HLP's lending leg) is
**reported** at Stage 2. It becomes the deploy threshold of the follow-up
paper registration; it is not a CONFIRM criterion.

**Error control (Revision 2, M4).** 012's false-confirm rate is **Stage 2's
own**: 0.043–0.052 under the owner design, by simulation (§3). The Stage-1
sign gate adds no protection against a world with an edge in 2025–26 and
none in 2026-H2.

## 6. Reported, decides nothing (both stages)

- 010's six criteria on the primary, with the month-flip and week-flip
  placebos (200 seeds; the informative number is the |t| share on the
  flipped gross).
- By pair (HL-BN, HL-BY); core / others; HL short / HL long.
- Slippage × 2 (every round trip, rebalances included); funding only.
- Drop-missing and valid-exit-only variants (the primary keeps both, N4).
- **The variant "010's three-pair plan, HL subset"**: the construction of
  the lead.
- Components (funding, gap, fees, top-ups); the stress block; counts of
  delisted, rebalanced, liquidated, raw-exit, worst-fill, funding-hole and
  fill-fallback positions, printed before the decision.
- **HL short liquidated on HL's own highs (Revision 2, m6).** Every
  HL-short position is recomputed with the rebalance and liquidation path
  read from HL's own 8h candles, not Binance's. Its summary, liquidation
  count and top-up count are reported.
  - These are last-trade highs, not HL's mark or oracle; that is
    conservative for liquidations.
  - Stage 1 spans the 2025 HL-specific squeezes.
- **Coverage counts** (Revision 2, M2), printed before the decision:
  - venue-coins with funding but no prices;
  - the build's counts of internal gaps, mid-month stops and delistings;
  - at Stage 2, the fetch report: Binance symbols HL does not list, HL
    coins `meta` marks delisted, Bybit-unknown symbols.
- At Stage 2: the annualised net against 3 %/yr.

**No reported block may become a primary.** Another pair set, coin set,
hold, window, exit, roll, maker execution or cost level is a new L2 trial,
decided on data after this registration's end.

## 7. Known weaknesses, stated now

- **Post-hoc selection (§0).** The pair set was chosen on 010's discovery
  result, and the effect size is subject to the winner's curse. Only a
  Stage-2 CONFIRM is evidence.
- **Stage 1 is exposed** (`000f`, `000h` and the reads listed in §0). It
  is a sign filter: it can close 012 on a negative mean, it passes about
  half of null worlds, and a pass proves nothing.
- **The 2025–26 spread compression.**
  - Unconditional 2025–26 spreads are tighter than 2023–24 (010 §3: core
    p90 9.8 vs 27.5 %/yr).
  - The entry rate fell from 46 a month in 2025 to 24 a month in 2026-H1.
  - If 2026-H2 resembles 2026-H1, the first look holds about 146
    positions, and power falls (the sensitivity in §3).
  - A carry that paid in 2023–24 may not pay after costs in a compressed
    regime; a fail would say that and no more.
- **HL-specific risk, not in the P&L.**
  - Venue and custody risk (a DEX bridge, withdrawal halts).
  - **Oracle risk:** HL marks to its own oracle, so an oracle excursion can
    liquidate the HL short at a price no CEX traded.
  - **Auto-deleveraging (ADL):** a profitable HL leg can be closed by ADL
    in a squeeze, leaving the CEX leg naked.
  - HLP and the liquidation engine's behaviour in stress.
  - The primary liquidates on the Binance 8h high with HL's maintenance
    margin, so an HL-only wick is missed there. The reported block on HL's
    own highs (§6, m6) shows its size; it decides nothing.
- **Few HL alt pairs survive the per-leg floor.**
  - Only 79 HL coins have 2025–26 HL funding at the floor (80 fetched;
    FTM was delisted on HL before Stage 1), and 78 trade.
  - Positions concentrate in a few liquid HL names, and HL alts pay the
    6 bp tier (the most common pairs are 6+4 and 6+2).
  - HL is the short leg in 524 of 681 Stage-1 positions, so the book is
    directionally one regime: "HL funding above CEX". A regime shift moves
    every position together, which is what σ_m models.
- **Few clusters.** Exactly 6 month clusters at the first look. The
  cluster-robust t with so few clusters is not exactly t-distributed.
  - The simulated decisive null is 0.043–0.052 (§3). It includes the weekly
    common factor and holds that overlap month boundaries, both of which
    make the month-clustered SE anti-conservative.
  - The pure synthetic null runs at 0.026 (t₅(0.975)) and 0.051 (t₅(0.95)),
    and the two-look path at 0.046 (§8).
- **HL's `meta` at Stage-2 fetch time.** "HL does not list this coin" comes
  from `meta`. If `meta` omits some coins delisted between 2026-07 and the
  fetch, those coins are missing from Stage 2: a residual survivorship of
  010's B2 kind. The fetch report counts `meta`'s delisted flags; the
  omission itself cannot be counted.
- **Access (warning only).**
  - Binance and Bybit derivatives are not lawfully offered to German
    residents.
  - HL is a permissionless DEX whose legal status for a German resident is
    unclear.
  - A CONFIRM leads to a paper registration on the **pair set fixed now:
    both HL pairs, pooled, by this rule** (D14, m8). Its venue access is a
    warning, and the paper period treats it as untested.
- **Inherited from 010:**
  - taker at today's base tier;
  - fixed hold, no roll (a persistent spread pays C every H);
  - slippage tiers are 009's bounds, not measured impact;
  - the price path from Binance only;
  - idle capital not counted;
  - Gaussian power simulations, with no liquidation jump (§3, m7).
- **Universe.** Coins without a Binance perp are out: the name check and the
  floor reference Binance. HL-only listings are not tested.

## 8. Built before any outcome is computed

- [x] `ct/carry012.py`:
  - `PAIRS_012`, `plan` (= `carry.plan` on the HL pairs) and
    `hl_subset_of` (the variant);
  - `stage1_gate` (the sign gate), `decide_stage1` and `recheck_stage1`
    (recomputed from the stored mean);
  - `stage2_confirm`, `decide_stage2` and `extension_due_from_row`
    (recomputed from the stored month t and G);
  - `month_windows`, `replicate_half_year` and `simulate_stages` (Revision
    2: month-aligned windows, common to both designs).
- [x] `r012_fetch.py` (Revision 2):
  - every network call goes through four hooks, which the tests replace;
  - typed `FetchError` after retries; nothing erroneous, null, or (past the
    cut) empty is cached;
  - Bybit klines are paged forward;
  - an HL 500 is retried, and "unknown" comes only from HL `meta` or
    Bybit 10001;
  - the Binance funding and kline listings are re-listed and saved on
    every attempt;
  - `stage1`;
  - `stage2` / `extension`: gated and resumable; the build's coverage
    checks run before one `<kind>_fetched` row (owner lines, manifest
    hash, report).
- [x] `r012_build.py` (Revision 2):
  - `--stage1`, with `hlpath`;
  - `assemble_stage2` (pure) and `coverage` (the refusals and counts of
    §2);
  - `--stage2` / `--extension`: gated; refuses on coverage; one
    `<kind>_built` row with the panel's SHA-256 and the coverage counts;
  - `load` refuses any file under a Stage-2 or extension directory until
    the gate has raised the limit.
- [x] `r012_run.py` (Revision 2):
  - `Panel`: whitelisted. Without the Stage-2 flag it refuses an end past
    2026-07-01 and any Stage-2 directory. `hl_path` is for the m6 block;
  - the guard: status; (a)–(e); (f) over the tree and every branch's
    history; (n1), required at Stage 1 too; (g1)–(g4); (s) the
    `stage1_started` row; (r) only approval lines added since Stage 1;
    (o) the owner lines equal those stored at Stage 1 and in every later
    row; (h'); (t); (i) one fetch, one build and one read, each step after
    the earlier rows are committed; (x); and (h), 010's verified 000g
    ordering;
  - `--stage1` (stores the owner lines and the price-hole count),
    `--stage2` and `--extension` (print the coverage before the decision);
    `--counts`.
- [x] `r012_describe.py`: the §3 counts and power. It is not on the outcome
  path and not hashed.
- [x] `tests/test_carry012.py` — **154 checks pass** (Revision 2):

      cd research/copytrade && .venv/bin/python -m tests.test_carry012
      ... (154 x PASS) ...
      all checks passed (154)

  - **Pair filter, Stage-1 sign gate and its recheck, Stage-2 confirm and
    the extension, null calibration:** as in Revision 1. The sign gate
    passes 0.507 of synthetic nulls; Stage 2 0.026 / 0.051; the two-look
    path 0.046.
  - **Power (m1).** The windows are month-aligned (exactly 6 months at the
    first look and 12 at the extension) and common to both designs. The
    half-year replication has 12 distinct months. At 25 %/yr everything
    passes; at zero the joint path stays below Stage 2 alone.
  - **Fetch (M1):**
    - a persistent Bybit error raises a typed FetchError and caches
      nothing; a transient one is retried;
    - only retCode 10001 means unknown;
    - a null is never cached, and an old cached null is re-fetched;
    - **forward paging recovers a coin delisted early in the window**,
      against a fixture that mimics Bybit's last-`limit`-bars behaviour;
    - **an HL 500 is retried, not read as "unknown"**; persistent 500s
      raise.
  - **Coverage (M2).** A complete panel passes. Refused:
    - a December Binance file not yet listed (wait);
    - funding truncated at a month boundary;
    - HL funding without prices.

    Counted, not refused: Binance funding with no kline files listed; a
    mid-month stop; a 10-day suspension in both series; a delisting.
  - **The read lock (M6 e).**
    - Panel refuses an end past 2026-07-01 without the Stage-2 flag.
    - The build refuses files under Stage-2 and extension directories, and
      refuses to assemble Stage 2, while the limit is 2026-07-01.
    - The Stage-1 sources stay readable, and the hashed Stage-1 panel holds
      no funding stamp at or after the cut.
    - Every fetch primitive refuses a later end before any request.
    - The fetcher's gate refuses on this DRAFT.
  - **End to end (B1), fixture HTTP, both kinds:**
    - attempt 1 stops cleanly on the missing December file, with no
      `_fetched` row; the listing is saved; the HL 500 is retried and BBB
      kept;
    - attempt 2 resumes and completes, with one started row and one
      fetched row; the report counts the Bybit-empty coin and the symbol HL
      does not list;
    - the build writes one built row; the universe is AAA (HL, BN, BY),
      BBB and CCC (no Bybit leg), and not DDD;
    - the run writes one Stage-2 row with a decision at q 0.975;
    - the extension completes the same way;
    - a panel changed after its build is refused by the run.
  - **Guard, in a throwaway git repo.** Each of these is refused:
    - (a)/(c) untracked; (h) no or uncommitted 009 close; (n1) at Stage 1;
      (b); (d)/(e); (f), including a started row, a row deleted but in
      history, and a row on a side branch;
    - (s) no `stage1_started` row, or one of another commit, together with
      (g4) a genuine Stage-1 fail;
    - (g2) uncommitted; (h') no approval, another row's sha, or an
      approval committed with the row; (t);
    - **M3:** the extension flipped YES → NO after Stage 1 ((o) and (r));
      prose added after Stage 1 ((r)); building before the fetch completed
      or with uncommitted fetch rows; a second fetch; running with an
      uncommitted built row; a second build; a repeated
      `stage2_fetch_started` or `stage2_built` row; a second read;
    - the extension without a Stage-2 row, without its approval, before
      2027-07-08, a second extension fetch, the flip to NO before the
      extension build, a first look with t ≥ 2.571;
    - (g3) code changed;
    - **m2: with one stage1 row in all history, a forged negative mean
      under pass labels fails (g4) alone**;
    - DRAFT.

    With everything in order, the fetch, build and run gates open in
    sequence.
  - **This file.** DRAFT; exactly one of each owner line, parsed as
    2026-12-31 and YES; no line starts with an approval phrase; the real
    guard refuses today; and this §8 table matches every guarded file.
  - **010's (h) bypass probes** (off-branch, foreign commit, mislabelled,
    wrong sha, deleted; m9) are in `tests/test_carry.py`, which still
    passes. `r010_run.py` is hash-locked below.

`tests/test_carry.py` (010's 142 checks) is unchanged and still passes.

**Pre-specified procedure** (Revision 2: each step's rows are committed
before the next step). At registration the owner confirms the two lines
under §9. Then:

    .venv/bin/python -I r012_run.py --stage1       # guard; stage1_started; the read, once; the row (owner lines stored)

Commit the two rows. After a Stage-1 PASS (a positive mean; decision
`read_stage2`, recomputed from the row), the owner commits an approval line
under §9, in a commit after the one that added the row. Its form is
"Stage-2 fetch: APPROVED", a date, "row" and the row's sha256, at the start
of its own line. Then, **no earlier than 2027-01-08**:

    .venv/bin/python -I r012_fetch.py stage2       # gate; stage2_fetch_started; resumable; coverage; stage2_fetched
    #   commit the fetch rows
    .venv/bin/python -I r012_build.py --stage2     # gate; coverage again; one stage2_built row with the panel hashes
    #   commit the built row
    .venv/bin/python -I r012_run.py --stage2       # gate; checks the hashes; stage2_started; the read, once

A fetch that stops on a typed failure or a coverage refusal (for example,
the December files not yet published) is re-run later. It resumes, and it
writes no `_fetched` row until coverage passes.

For the extension (only if the Stage-2 row's decision is `extension_due`,
recomputed): an approval line of the form "Stage-2 extension fetch:
APPROVED", a date, "row" and the sha256 of the Stage-2 row, committed after
that row. Then the same three steps with `extension` / `--extension`,
**no earlier than 2027-07-08**.

**SHA-256 of the code and data the outcome depends on** (read by the guard;
recomputed by the orchestrator at commit). The 010 rows equal 010's §8.

| file | sha256 |
|---|---|
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/carry.py` | `38fbaeb55ca4818d3293c046f9656be4a5d4eddcf14b8243933c6dcea920720b` |
| `ct/carry012.py` | `46f60c0637f90408de22846b0536b758be20ecfe57fa1c3eee4065ccba821b0a` |
| `ct/eventstudy.py` | `08a780b4d40bad811d5d826d42d29c44d673246048d85cf31ec1be2eaf6018b0` |
| `ct/factors.py` | `a1e70b9a5a2c1b58f43d2b08a2eaeac5ce5a3fd45868a659d745a3354bd61197` |
| `ct/stats.py` | `bfcc85f285dc7af28f364e2d77e0fbe04b6c2367724f4b822b7fbf6ff435d472` |
| `ct/net.py` | `90092590941e805eb465e7e1f05467c05e376bf8b07fe6dbdacfdb6cf7e00006` |
| `r010_run.py` | `edca47afa70a45d8bf88e6010ddb6f337d0a008632d1b9f8ccf7ab3d5ae1581b` |
| `r010_fetch.py` | `2515c6b97630628898bdfd0251c9fe8dfb3d0ffacebb4e1ea2edaa87da45c8a3` |
| `r010_build.py` | `53b6433da32959f182e2c5ba812ec54944283a6d9a7404a9d1938405f848e45d` |
| `r012_run.py` | `22e5814aa76942189049b7624f2463b413e2495a09295ba1f4cd1d546b40f6b2` |
| `r012_fetch.py` | `82295cd7149db408d656f99b0c8cb3ae58c8334b1571db8d8ab133a751dc1f47` |
| `r012_build.py` | `69bc1d2baf3186c1a285d9db08d0cfc8abb3551ad74588954f98eb38f5a36efc` |
| `scouts/l2_fetch.py` | `3fcb6adde0b6f0a0af5e262d1c65a21ef6d71ddf6f785c3b20f21d3827abdc37` |
| `scouts/h2_binance_scout.py` | `8fa6cba20d45b15a6d9e12be87187a351fdeef503f94e68ae22f0c66f28805e9` |
| `data/carry012/panel_stage1/universe.json` | `077c16952396af15a53b753dc619d1a7f19874a947374746eecc426def05a838` |
| `data/carry012/panel_stage1/funding.parquet` | `4189a2b03671f5aa8848546f4daf5afd558d00518549ce3f9daafc85cdeabdbe` |
| `data/carry012/panel_stage1/prices.parquet` | `81f6d2f20bd81111275424b0a9b53cbdceb3698fa14235524615bfd8c08fdb64` |
| `data/carry012/panel_stage1/path.parquet` | `37617c064e429a6249229279efbc2e827b633a8fcf378821797eee3945aefe5b` |
| `data/carry012/panel_stage1/volume.parquet` | `3e79aa66deeb7cc970fbf6faf3d99211823cd0ea8968c78bbc1665fcd0d7bc38` |
| `data/carry012/panel_stage1/levels.parquet` | `6e2f87586c7e709da81cfad9145c2281b756374bacd481dcc0ca64ce11eb51b7` |
| `data/carry012/panel_stage1/hlpath.parquet` | `f44243b942bdd645fe9e9e949bec764b34111a674c0a7f6ce2f2fb3798225541` |
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

- `ct/factors.py` and `ct/stats.py` are 009's modules, imported read-only
  through 010's guard (h).
- The Stage-1 panel is rebuilt with `.venv/bin/python -I r012_fetch.py
  stage1` then `r012_build.py --stage1`; a rebuild is byte-identical
  (checked again in Revision 2).
- The Stage-2 panel's hashes are written into its `stage2_built` row and
  checked by the run.

## 9. Owner decisions — APPROVED 2026-10-12 (orchestrator, under delegation: owner, 2026-10-12: "you are allowed to register yourself and take decision as you would recommend as long as they take us nearer towards the goal")

**Approved:** D1–D17 as written. D15: end 2026-12-31, extension YES
(confirmed). **D18 (a)–(g) approved.** It amends the HL-holdout rule for
exactly two rules, 012 and L3. Under (d), the L3 decision (register 008 or
waive) is due before 012's Stage-2 fetch, which can be no earlier than
2027-01-07. It is assigned to the next research session.

| # | item | proposed |
|---|---|---|
| D1 | venues, pairs, universe | HL, BN, BY USDT perps; **pairs HL-BN and HL-BY only**; coins with an HL perp and a Binance USDT-M perp (delisted included); Stage 1 on 010's registered universe, Stage 2 on a point-in-time universe rebuilt from Stage-2 listings |
| D2 | periods | Stage 1: 2025-01-15 → 2026-06-30 (exits before 2026-07-01), exposed, a sign filter. **Stage 2: 2026-07-01 → the D15 end**, fresh, decisive. Extension: → 2027-06-30 if D15 says YES |
| D3–D9 | signal, costs, H and θ, eligibility, entry, exit, P&L | **010's D3–D9, frozen and unchanged** (code imported read-only, hashes in §8); the entry chooses among HL pairs only |
| D10 | trial and inference | one trial, T3 (HL pairs pooled); month-clustered t on R per position, G = month clusters present |
| D11 | Stage-1 gate (Revision 1) | **SIGN gate:** the point estimate of the mean net per position > 0 (at least 3 positions); recomputed from the stored mean. Month t (beside t_{G−1}(0.95)), median, week t, annualised net and the largest-month removal are reported, never required |
| D12 | Stage-1 FAIL (mean ≤ 0) | **012 closes; the HL–CEX lead is "not demonstrated"** and Stage 2 is not read. P(Stage-1 sign pass), owner design, at a true 0 / 2 / 3 / 5 %/yr: **0.51 / 0.76 / 0.86 / 0.96 (raw bound)** and **0.49 / 0.90 / 0.98 / 1.00 (robust)**. Under the null it is a coin flip by design; the Gaussian model has no liquidation jump, so the positive-edge rates are optimistic (m7) |
| D13 | Stage-1 PASS | not evidence. The owner commits the Stage-2 approval line (§8), after the row; then, only through the gate and no earlier than 2027-01-08, fetch → build → `r012_run.py --stage2` |
| D14 | Stage-2 decision | **CONFIRM** (existence test, §5) → a registered forward **paper** period on the **pair set fixed now: both HL pairs (HL-BN and HL-BY), pooled, by this rule** (m8: no pair is chosen after the Stage-2 by-pair results). Its deploy threshold is **3 %/yr annualised net on gross capital**; venue access is a warning, and the paper period treats the pairs as untested. **Non-confirm → "not demonstrated"**, never "no edge"; L2's HL–CEX carry closes. **The operative error rate is Stage 2's own false-confirm rate: 0.044 / 0.043 (raw / robust) at the 2026-H1 entry rate, and 0.052 / 0.043 at the 2025–26 rate**, owner design (M4: stated, not calibrated; 0.052 is within ±0.005 of 5 %). **Stage-2 power** at a true 2 / 3 / 5 %/yr: **0.10 / 0.14 / 0.26 (raw) to 0.20 / 0.37 / 0.75 (robust)** at the 2026-H1 rate. *Secondary:* joint (sign gate × Stage 2) 0.074 / 0.12 / 0.25 to 0.18 / 0.36 / 0.74; the joint false pass of 0.022–0.024 holds under a global null only |
| D15 | Stage-2 end and extension (owner) | the two lines below, **010's choices restated, to be confirmed at registration**: end 2026-12-31; extension YES → both looks at t_{G−1}(0.975), the second (cumulative to 2027-06-30) read once after 2027-07-07 only when the first ends 0 < month t < t_{G−1}(0.975). NO → one look at t_{G−1}(0.95) |
| D16 | family | L2 count 3 (010 T1, T2; 012 T3). **Hyperliquid discovery year (2025-07-28 → 2026-06-30): trial 16** (Stage 1 reads HL funding and prices there; its pass is the only reason to read the holdout). Binance funding and prices for the HL coins in 009's holdout-A tail (2025-01-15 → 2025-07-27) and holdout-B calendar (2025-07-28 → 2026-06-30) are consumed by Stage 1; 009 is closed and holdout B was never registered for reading; disclosed (§1). Holdout family 2 (D18). Post-hoc selection on 010's discovery result, stated (§0). Only a Stage-2 CONFIRM counts as evidence |
| D17 | ordering, whitelist and read lock | 010's verified 000g guard (h) kept; reads only `data/carry/` and `data/carry012/`; **nothing dated on or after 2026-07-01 is read outside the Stage-2 gate** (fetch limit, `Panel` end, build `load`; tested) |
| D18 | **Hyperliquid holdout calendar — amends programme doc §2 ("Hyperliquid holdout (2026-07-01 →): sealed … its allocation is decided once") and the rule that a holdout is read only by a rule that passed discovery. For the owner to approve** | **(a) Calendar.** Every Hyperliquid series, market or account (funding, candles, trades, books, fills, snapshots, vaults), dated 2026-07-01 → 2027-06-30, **and the Binance and Bybit funding and klines of the same months**. It is unread. **HL holdout data already on disk is never readable by 012:** `data/cache/hl-funding` beyond the cutoff (it runs to 2026-07-10) and the ~98 holdout snapshots in `data/derived/hyperliquid/snapshot_accounts.parquet`. 012's whitelist refuses both directories, and no 012 source names them (tested). Neither is read by any registration. **(b) Allocation.** Exactly two pre-registered rules may read the calendar, each once, each through its own guard. **012** reads HL `fundingHistory` and 8h `candleSnapshot` only, for coins with a Binance USDT-M perp, plus Binance and Bybit funding and klines for those coins, 2026-05-01 → 2026-12-31; its pre-registered extension to 2027-06-30 is part of its one read. **L3 (008)** reads the series its registration names. No third registration reads it. **(c) Discovery condition.** For 012 the Stage-1 sign filter is accepted as the "discovery pass". The owner accepts that 012's error control rests on Stage 2 alone (0.043–0.052 by simulation, §3). **(d) Order and exposure.** Either L3 is REGISTERED and committed before 012's Stage-2 fetch, or the owner records that L3 waives this calendar. **Whichever rule reads second ledgers, and discloses in its own registration, the first rule's printed Stage-2 result and outputs as exposure.** **(e) Enforcement.** 012's code refuses any read dated on or after 2026-07-01 outside its Stage-2 gate (fetch limit, `Panel` end, build `load`; tested, §8). Rules for other registrations live in the programme doc and in each later guard. **(f) Multiplicity.** Holdout family = 2, each rule at its own α, no adjustment; the family-wise false-confirm rate is at most the sum, ≤ 2α. **(g) 010 §6.** Its condition "decided on data after the holdout end" is released by 010's closure at discovery (no 010 row reads 2026-07+). This is recorded in the programme doc and the ledger; 010's frozen file is not edited. The owner approves (a)–(g) in this row |

Owner lines. Each stands alone, starts its line and appears once in this
file. The values below are 010's owner choices of 2026-10-11, restated, and
are **to be confirmed at registration**. The only offered end is
2026-12-31; the extension is YES or NO.

Stage-2 end (owner): 2026-12-31

Stage-2 extension (owner): YES

The runner reads its parameters from `ct/carry.py` (010, frozen) and
`ct/carry012.py`. Any change before registration is a change there and here
together, with a new hash table.

## 10. Open questions for the orchestrator and the owner

Closed in Revision 1:
- the primary rule: kept, reason in §0;
- Stage 1's power cost: the sign gate (D11);
- the owner lines: 010's choices, to be confirmed at registration.

Closed in Revision 2:
- B1 and M1–M3 in code (§8);
- M4: the α is stated, not calibrated;
- M5: the family counts (§1, D16);
- M6: D18 rewritten;
- the MINORs.

Still open:
1. **D18 for the owner.**
   - It amends programme doc §2 and binds L3 (a future 008).
   - It needs the owner's approval, and a matching entry in the programme
     doc and the ledger for the 010 §6 release (g). I did not edit either:
     012's files only.
   - The reviewer's optional extra gate, an anchored owner line approving
     the HL holdout allocation, is not built. Adding it before
     registration is a small change.
2. **The order with L3 (D18 d).** If 012 reads first, L3 is drafted with
   012's Stage-2 outputs as exposure: HL funding vs CEX, the HL-short
   share and liquidations in 2026-H2.
3. **The sign gate's null pass rate (~0.5).** It is deliberate: Stage 1 is
   exposed and cannot carry evidence. It closes 012 at no Stage-2 cost
   when the HL carry was negative in 2025–26.
4. **Noise model.** The raw 2025–26 SD for others (115 %/yr) is dominated by
   outliers; the robust one (7.5 %/yr) ignores them. The two power rows
   bracket the truth; neither is an estimate. Neither has a liquidation
   jump (m7).
5. **Stage-1 entry prices.** HL closes for 2025–26 come from HL's
   `candleSnapshot` (last trade), as in 010. Thin HL alts can have stale
   8-hour closes (010 m-d).
6. **010's Bybit paging** has the same backward-paging hole fixed here
   (M1). 010 is closed and its file is frozen. The orchestrator may want
   to note that 010's discovery lacked Bybit 240-minute prices for coins
   whose Bybit trading stopped well before 2024-12-31.

## 11. Disclosures and real-data reads (for the ledger)

Ledgered by the orchestrator as **`000h`** (Revision 1; the Revision 2 reads
below are for its addendum). All reads were end-bounded before 2026-07-01,
or were names-only listings. **0 rows dated on or after 2026-07-01** were
received or kept. No P&L, funding accrual, price change or gap change was
computed on real data.

**Draft and Revision 1:**
- **New fetch:** Bybit 240-minute klines (`/v5/market/kline`, linear) for
  209 symbols, 2024-11-01 → 2026-06-30. With the old backward paging, 188
  had rows; 0 rows were dropped at the bound. Stored in
  `data/carry012/stage1/`, and deleted and replaced in Revision 2.
- **Panel build:** 010's registered panel rows (funding, volume, levels)
  for the 213 HL coins, filtered to 2024-11-01 → 2026-06-30. 010's raw BN
  8h klines and HL 8h candles to 2026-06-30 were turned into closes, opens
  and the Binance path. Printed: row counts only.
- **Metadata:** 79 HL coins have 2025–26 HL funding in 010's panel, and 77
  of them a Bybit symbol (existence only).
- **Printed counts (unconditional; the residue noted in 010's N7
  applies):**
  - Stage-1 positions opened under 012's rule and under the variant, by
    pair, class, year, short venue, hold and slippage tier;
  - |s| and cost quantiles at entry;
  - the 2026-H1 count;
  - 337 discovery positions under 012's rule.
- **Incidental:** about 600 characters of an uncommitted 007 row, with
  per-date counts only (recorded in `000h`).

**Revision 2 (new, for the `000h` addendum):**
- **Bybit archive listing (names only):** `public.bybit.com/trading/` for
  AI16ZUSDT, MKRUSDT and PUMPUSDT, giving their date ranges (2024-12-30 →
  2025-11-06; 2022-02-15 → 2025-08-18; 2025-04-11 → 2025-06-03). HEAD sizes
  of one daily file each. The first 3 lines of
  `PUMPUSDT2025-05-10.csv.gz`: the header and 2 trades. Nothing from the
  archive is used.
- **Bybit kline probes, MKRUSDT (2025):**
  - 5 bars of 240-minute and daily klines around 2025-03-01 → 2025-03-05,
    which printed OHLC values for those bars;
  - a 240-minute request over 2024-11-01 → 2026-06-30, which returned an
    empty list (this is how the paging bug was found);
  - a request ending 2025-04-18, which returned 1,000 bars (count and first
    and last timestamps only).
- **Re-fetch:** Bybit 240-minute klines for all 209 Stage-1 symbols,
  2024-11-01 → 2026-06-30, forward-paged. 204 have rows; FET, LOOM,
  MATIC, RNDR and STRAX have none. 0 rows were dropped at the bound.
- **Rebuilt Stage-1 panel**, with `hlpath`. Printed: row counts, and the
  coverage counts and kinds of §2 with venue, coin and first/last dates
  (timestamps only), including TON's 2026-06-23 Binance funding stop and
  the LIT and PUMP gaps.
- **Metadata:** FTM's HL `_hold` file: its row count, the count of
  non-zero rows and the last non-zero timestamp (2025-01-12). Row counts
  of cut-stamped path and hlpath rows (193 and 172).
- **Recounted §3** (unconditional counts): 681 (HL-BN 373, HL-BY 308;
  535 / 146 by year; short HL 524, BY 100, BN 57; holds; tiers; |s| and
  cost quantiles); variant 572.
- **Power** re-simulated with month-aligned windows on the recounted
  positions and the same reused noise levels.

## Result — Stage 1, 2026-10-12 (run of commit 2afb057, `results.jsonl` line 12)

The sign gate on the exposed block 2025-01-15 → 2026-06-30, primary rule
(choosing among HL pairs). Counts before the decision:
- 681 positions (HL–BN 373, HL–BY 308);
- 7 liquidated;
- 1 raw exit gap;
- 133 funding-hole settlements in 4 positions;
- 0 venue-coins with funding but no prices.

| gate | result | verdict |
|---|---|---|
| Stage-1 mean net > 0 | mean **−25.8 bp** per position (on N); month t **−3.37** (18 clusters); median −5.7 bp; hit 34%; halves −26.6 / −25.0 bp; annualised on gross capital **−5.7%/yr** | **FAIL** |

**Decision: `close`.** 012 stops at Stage 1. **The 2026-07 → 2027-06
calendar is never read by 012.** Under D18 that leaves L3 as the only rule
that may claim it, so D18 (d) needs no ordering any more. L2 family: 3
trials, no pass. HL discovery year: trial 16, no pass.

**Diagnostics** (reported, per position, in bp of N):
- **All HL pairs:** funding **+27.8** (t 12.0, hit 80%), price gap
  **−38.0** (median +2.2: a fat left tail), fees −34.8, plus margin
  top-ups.
- **Core coins (87):** funding +13.9, gap +2.2, fees −24.2. Still
  negative.
- **Short HL (524):** funding +36.1, but the gap −41.3.
- **Long HL (157):** funding 0.0.
- **HL–BN alone:** −25.3 bp, t −2.68.
- **2× slippage:** worse.

**Reading.** The funding leg pays reliably: 80% of positions collect, about
+28 bp each. It is the price gap that ruins the trade. Most of the time the
gap closes slightly in our favour (median +2 bp), but when HL funding is
high, HL's price can run further from the CEX price in jumps, and the mean
gap loss (−38 bp) exceeds the funding. Add four taker fills and the carry
is negative in 2025–26. The 2023–24 lead (+5.6%/yr) was regime-specific,
and did not survive the next 18 months. The filter did its job: it closed
the lead cheaply, before the unread calendar was spent.
