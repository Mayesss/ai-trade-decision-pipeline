# 008 — Inverted positioning (L3): 001 T3's book with the sign flipped, a new existence test, decided on the Hyperliquid holdout only

**Status: REGISTERED 2026-10-13** (orchestrator, under the owner's delegation of 2026-10-12: "you are
allowed to register yourself and take decision as you would recommend as long as they take us nearer
towards the goal"). Revision 2 after two independent first-round reviews (`reviews/008-review-a.md`,
`008-review-b.md`), a second-round review and its Revision 2 recheck (`reviews/008-review-r1.md`, verdict:
register). Every row of §12 is approved as written, including D13 (the D18 amendment) and the end
2026-09-28.

**Added at registration (review r1, Revision 2 recheck, minor; text only, no code change):** a ceiling
refusal (more than 8 names unavailable) that persists on 3 attempts at least 24 h apart closes 008
unread, with a `not_demonstrated` reading and no outcome computed.

The decision table (§12) is APPROVED (orchestrator, 2026-10-13). The one owner line (§12) is confirmed at
2026-09-28. The mechanics are built and checked on synthetic data only (§11).

**What was and was not done on real data.** No position, tilt, signal, cohort, price, return, funding
rate or account value dated on or after 2026-07-01 was computed, printed or saved. No Bitget price and no
requester-pays object was fetched. Real-data reads were limited to:
- row counts, date coverage and file names;
- object sizes from one S3 LIST call;
- schemas, and SHA-256 hashes of input files;
- in Revision 1, Bitget's current public contract list: names and status only, no price (§13).

001 T3 was not re-run, in any variant, on any data.

**Revision 1 (2026-10-13), after `reviews/008-review-a.md`, `reviews/008-review-b.md` and `000l`.**
Every finding and the change that answers it is in the table in §14. The headline changes:
- **The confirm regression** (A-B1, A-M1, 000l):
  - one common α;
  - a BTC slope per (window, calendar month) cell;
  - an equal-weight alt index;
  - a point-in-time short-term reversal factor.

  §3 is re-simulated under every reviewer null. Size is 0.047–0.050 under all of them except month-scale
  persistence, which gives 0.08–0.10 (0.08–0.12 in Revision 2, §3). Power falls by about 0.01.
- **The economic bar** is on the half-shrunk α, and the labels are renamed (A-M2). Every confirm leads to a
  cost-reduced registration on the remaining calendar. Nothing leads to trading, or paper-trading, this
  taker book.
- **One tail scenario,** carried through (A-M4).
- **An explicit D18 amendment row** (B-M3, D13).
- **Complete earlier-looks disclosure** (A-M3, 000l).
- **The guard is bound to the real repository and tree** (B-B1).
- **Delisted names never dead-end the fetch,** with a pre-specified end state for persistent refusals
  (B-B2). Today 0 of the 168 mapped symbols are gone from Bitget.
- **Survivorship direction:** it favours CONFIRM, and it is measured at run time (B-M4).
- **Snapshots are pinned** (B-M1).
- **Bytes are measured and the cap is cumulative** (B-M2).
- **Read-once integrity:** rows are committed as they are written, the history and reflog are scanned, and
  the environment is frozen. All minors are addressed.

**Revision 2 (2026-10-13), after `reviews/008-review-r1.md`** ("register after the listed fixes"). The
change table is §15.
- **B1. Alt and reversal loadings per cell.** Each BTC cell (window × month) now carries its own alt and
  reversal loadings as well as its own BTC slope: k = 1 + 3 × cells (10 at 2026-09-28, 16 at 2026-11-23).
  - The reviewer's nulls N11–N15 are added to §3.
  - Size is 0.045–0.053 under every market, alt, reversal and heavy-tail null, against 0.08–0.14 for
    Revision 1's pooled loadings.
  - Power falls by about 0.01–0.02: 0.28 / 0.14 at 2026-09-28 and 0.40 / 0.16 at 2026-11-23 (full /
    half magnitude).
- **M1.** Any `hooks` argument is refused whenever the data tree is the real one, whatever the root.
- **Minors.**
  - **m1:** the shadow check covers packages, sourceless `.pyc` files and git-ignored files.
  - **m2:** every fetch attempt is a committed row. The cap and the persistence clock come from those
    rows, edits cannot lower them, and the clock is the commit time. A ceiling of 8 unavailable names
    applies.
  - **m3:** the environment pin covers every distribution file, with a recovery clause.
  - **m4:** the survivorship reading rule is computed, rechecked and printed before the decision, and it
    needs a minimum count.
  - **m5:** history scans use `--text --no-textconv --no-ext-diff`.
  - **m6:** the runner never pushes; the owner's push is the durable record.
  - **m7:** a failed commit is retried once, then the run stops loudly.
  - **m8:** text fixes.
- **A reported "persistence-sensitive" flag**, set when 1.782 ≤ t < 2.18. It is not a criterion.

The runner `r008_run.py` refuses to compute anything until all of these hold:
- this file says REGISTERED, is committed and unmodified;
- the code, the static data and the pre-registration manifest match §11;
- the guarded fetch `r008_fetch.py` has written and committed its two rows;
- an approval line names the fetched row;
- no 008 read row exists anywhere in `results.jsonl`'s history or reflog;
- on the real data tree, `root` is this repository and no guard argument is overridden.

Programme: `docs/edge-discovery-next-prompt-2026-10-12.md` (N6 / L3; the owner approved the pull on
2026-10-12). Inherits: `registrations/001-slow-traders-copy-and-crowd-divergence.md` §8 and §12
(REGISTERED, run of commit `efefc7f`, `results.jsonl` line 1). Holdout allocation:
`registrations/012-hl-cex-funding-carry.md` D18, amended here by D13. Ledger entries read: `000`, `000c`,
`000i`–`000l` (and `000b`, `000d`–`000h` for context). Reviews read: all of `registrations/reviews/`,
including both 008 reviews.

## 0. Inheritance, post-hoc selection, and the arithmetic to read first

**The hypothesis.** On Hyperliquid, where the bottom-quintile (losing) slow traders lean harder into a coin
than the top-quintile (skilled) slow traders, that coin outperforms over the next three days. The book goes
long those coins and short the coins the skilled money favours, equal risk, a third rebalanced daily.

**The book is unchanged; the test is new (A-m1).**
- **Book: 001 §8 by reference and by code.** That is:
  - the cohorts (top / bottom quintile of scored slow wallets, cross-regime score, trip floor 4);
  - the tilt;
  - the universe filters;
  - the 3-day horizon;
  - the costed inverse-volatility book, its costs and the funding proxy.
- **Reused modules, imported read-only and byte-identical to 001's run commit `efefc7f`** (`git diff
  efefc7f HEAD` is empty):
  - `ct/crowd.py`, `ct/scores.py`, `ct/stats.py`, `ct/schedule.py`, `ct/leaders.py`;
  - `ct/archive_fills.py`, `ct/archive.py`, `ct/regimes.py`, `ct/net.py`, `ct/bitget.py`, `ct/hl.py`;
  - `p3_checks.py`, `p4_features.py`, `p5_population.py`.

  These 14 are hashed in §11. `p5_run.py` is also byte-identical, but it is only the tests' oracle (B-m9):
  it is not on the read path and not hashed.
- **008's own code** is in new files only: `ct/crowd008.py`, `r008_run.py`, `r008_fetch.py`.
- **The test is not 001's.** It is an existence test before trading costs, on α net of market, alt and
  reversal factors, at t_{G−1}(0.95). The IC is reported only (§4, §5).

**The one change to the book: the sign.**

    signal_008(c) = −(tilt_top(c) − tilt_bottom(c))

The long and short quintiles swap. Weights, turnover and costs are identical.

**Post-hoc selection, stated plainly.**
- The rule was chosen **after** reading 001's discovery result (T3 costed book −0.148 %/day, NW t −3.4).
  001 §12 itself said a sign flip would be a new registration.
- The choice to spend the HL holdout calendar on it was made by an orchestrator who had seen a contrarian
  payoff in these same months (`000l`, §13).
- The discovery numbers are motivation, never evidence. Power is stated at half and full magnitude, and the
  expected return (§9) uses the half.

**The arithmetic that matters most (from published numbers only).**

`crowd.long_short` charges 11 bp one way (Bitget taker 6 bp plus 5 bp of slippage) on each tranche's gross of
1, at entry and at exit. A third of the book turns over each day, so:
- the cost is **2 × 11 / 3 = 7.33 bp per day** in steady state;
- over 164 days it averages **7.29 bp per day** (tests §3).

| per day | 001 as registered | 008 (sign flipped) |
|---|---|---|
| before trading costs (funding included) | −14.75 + 7.29 = **−7.46 bp** | **+7.46 bp** (≈ +27 %/yr on gross 1×) |
| after 001's costs | −14.75 bp (NW t −3.42) | −(−14.75) − 2 × 7.29 = **+0.17 bp** (≈ +0.6 %/yr, t ≈ 0.04) |
| standard error (NW, 164 days) | 4.32 bp | 4.32 bp |
| t before costs | −1.73 | **+1.73** |

**Consequences.**
1. **At 001's taker costs, the inverted discovery book earned nothing.** Half of 001's t −3.4 was its own
   cost.
   - The programme document's "+5 bp/day net (≈ 12 %/yr)" (docs/edge-discovery-2026-10-12.md §1, N6 row)
     used 001's synthetic "~5 bp/day" cost remark.
   - The code's cost is 7.29 bp/day, so the net is about 0. The orchestrator should correct that
     document.
2. The scientific claim, that the positioning gap anti-predicts 3-day returns, can only be tested **before**
   trading costs. 008's existence test is therefore on the book before trading costs, with funding kept. The
   economic bar applies after (§5).
3. **No version of this taker book is traded or paper-traded, whatever the result.** A CONFIRM leads only to
   a new, cost-reduced registration on the remaining calendar (§5, §9).
4. **Survivorship favours CONFIRM** (B-M4, A-m4).
   - The universe is 001's 2026-10-07 symbol table. Coins that died during the holdout are removed with
     hindsight: HL delistings by status, and Bitget removals because Bitget serves no history for them.
   - On the hypothesis' own mechanism (losers lean into dying alts, which sit on the inverted long side),
     dropping them removes the long leg's worst names.
   - Measured at run time on positions, needing no price (§6). A pre-specified reading rule for it is in
     §5.

## 1. Trial, family and the holdout claim

| id | rule | data | role |
|---|---|---|---|
| L3 | 001 T3's book, sign flipped; a new existence test | HL holdout only (§2) | the single trial (§5) |

- **HL holdout calendar (012 D18, 2026-07-01 → 2027-06-30).** 008 is the **first and only reader**.
  - 012 closed at Stage 1 and never read the calendar, so D18 (d) needs no ordering.
  - Holdout family: 1 read. One-sided α = 0.05, no adjustment.
  - 008's reads beyond D18 (a)'s wording are made explicit by **D13** (§12).
- **HL discovery year (2025-07-28 → 2026-06-30): 16 trials, no pass — unchanged.**
  - 008 computes no outcome there.
  - Discovery fills are read only to score the cohorts. Window A's lookback (2026-03-09 → 06-07) is wholly
    in discovery; window B's (2026-05-04 → 08-02) is partly in it.
  - The S = 2026-06-08 window was never computed by 001.
- **What is not independent** (§13, `000l`):
  - The rule is fixed by 001 T3 and has no free parameter.
  - The decision to spend the calendar on it, and the controls added to C1 in Revision 1, followed
    looks at the same months:
    - a contrarian payoff on Bitget/Capital AI decisions, 2026-06-27 → 10-10;
    - the 2026-09-14 alpha-lab reversal ICs over 2024-03 → 2026-09.
  - A confirm survives a market-wide reversal factor and market factors by construction. It cannot be
    called independent of the decision to look.
- **Written expectation.**
  - About 1 in 5 that the inverted effect exists in the holdout. The holdout is a rally, and that is a hard
    test.
  - P(CONFIRM) ≈ 0.2 × power + 0.8 × 0.05:
    - ≈ 0.07 at half magnitude;
    - ≈ 0.10 at full magnitude (end 2026-09-28).
  - A CONFIRM would be real about 40 % to 60 % of the time.

## 2. Data, periods and exactly what is read

| span | dates | use |
|---|---|---|
| 001 discovery | 2025-07-28 → 2026-06-30 | motivation only; lookback fills for cohort scores |
| **008 read (recommended end 2026-09-28)** | signals from snapshots 2026-07-01 → 07-30 and 08-03 → 09-24; book returns end 2026-09-25 02:00; IC returns end 2026-09-27 02:00 | **decisive**, read once through the gate |
| alternative end 2026-11-23 | signals add 09-28 → 11-19 | same, if the owner picks it |
| after the end | 2026-09-28 → 2027-06-30 | **never parsed by 008** |

Prices and funding are fetched and loaded only below the read bound, end + 2 h = 2026-09-28 02:00 (B-m4).
Window C's on-disk snapshots (09-28 → 10-06) are **hashed, not parsed**, under the 2026-09-28 end.
`btc_1h.parquet` is loaded whole by `Regimes`, but only closes strictly before each trip's open (≤ 2026-08-02)
are used.

**Embargo: none needed.**
- Every signal comes from a snapshot at or after 2026-07-01 00:00 UTC (`signal_snapshots`, tested).
- Every return starts at its entry, at least one hour later.
- Every cohort is scored on a lookback strictly before S.
- No discovery outcome overlaps.

**Selection schedule: 001's grid, continued** (first S = archive start + 91 d, step 56 d, lookback 91 d,
hold 56 d).
- Windows are dropped at their end, never truncated there.
- Window A is truncated at its start by the holdout cut (A-m1). That truncation is forced: its June days are
  discovery.

| window | S | lookback | hold | signal days used | holdout fills needed |
|---|---|---|---|---|---|
| A | 2026-06-08 | 2026-03-09 → 06-07 | 06-08 → 08-02 | 2026-07-01 → 07-30 (**30**) | none |
| B | 2026-08-03 | 2026-05-04 → 08-02 | 08-03 → 09-27 | 2026-08-03 → 09-24 (**53**) | 2026-07-01 → 08-02 (33 days) |
| C (2026-11-23 only) | 2026-09-28 | 2026-06-29 → 09-27 | 09-28 → 11-22 | 2026-09-28 → 11-19 (**53**) | 2026-07-01 → 09-27 (89 in all) |

There are 83 book days in 13 ISO-week clusters (136 days in 21 clusters for 2026-11-23).

**The book's inherited mechanics, described accurately (A-m2; behaviour left as 001's for fidelity).**
`crowd.long_short` ages tranches per signal day:
- the three gap days 2026-07-31 → 08-02 are never booked;
- the 07-29 and 07-30 tranches (cohort A) earn the 08-03 and 08-04 returns, so by-window attribution is
  approximate;
- the last two tranches never complete, and their exit cost (≈ 0.1 bp on the mean) is never charged;
- one late holdout snapshot (taken 16.9 h into 2026-08-23) makes two day returns overlap by about 16 h.

**Why 2026-09-28 is recommended** (mechanics and bytes only):
1. **Power per byte.** About 0.30 for ≈ 3.3 GiB, against 0.42 for ≈ 13.5 GiB, at full magnitude. At half
   magnitude, 0.14 against 0.17 (§3).
2. **Speed.** Everything else is on disk, and the pull is 33 fill days. 2026-11-23 needs a second pull of
   snapshots and cannot run before 2026-11-30.
3. **The unread remainder has a use.** It is where any confirm's cost-reduced follow-up is decided (D6).
   `000`'s clause applies: "the archive's months after the holdout read form a second, untouched
   holdout … if the two holdouts disagree, the untouched one decides". P0 read HL fills to 2026-10-07, so
   the follow-up's decisive span should start on or after **2026-10-08**, or disclose P0's 09-28 → 10-07
   overlap (B-m7).

**What 008 reads.** This is a whitelist, enforced by the code paths. The read is offline, and on the real
tree it runs only under the real guard (B-B1).
- `data/pruned/by_dex/hyperliquid/fills/perp/all/date=D/fills.parquet`, for the lookback days only.
- The **pinned** snapshot files `data/archive/by_dex/hyperliquid/snapshots/perp/date=D/*.parquet` for the
  signal dates. These are exactly the fetch manifest's list, with exactly one file per date (B-M1).
- **The holdout rows of `data/derived/hyperliquid/snapshot_accounts.parquet`.** They are read for window B's
  equity as of S, its lookback leverage and its features, bounded in the query to [S − 91 d, S) by the two
  hashed modules (`p4_features.features`, `leaders.Equity`). D13 makes this explicit (B-M3).
- `data/derived/regimes/btc_1h.parquet`, as above.
- 001's static lists:
  - `data/p0/2026-10-07/symbols.json`;
  - `data/derived/bitget_listing.json`;
  - `data/cache/hl-vaults/903952e394cd91c74dc593a3.json`;
  - `data/p0/2026-10-07/report.json`, the P0 wallets.
- `data/r008/`: 008's own pulls (closes, funding, report, manifest).

**Holdout-dated data on disk that 008 never opens:**
- `data/cache/bg-1h`, with blocks to 2026-07-16;
- `data/cache/hl-funding`, to 2026-07-10;
- `data/cache/hl-fills`;
- the `xyz` dex;
- snapshot partitions outside the signal dates.

These are path checks, not sandboxing (B-m10). After B-B1 the only route to the real tree is the real
guard.

**The pull** (owner approval 2026-10-12: only through the guarded fetch, after REGISTERED, LIST first,
15 GiB hard cap):

| end | requester-pays objects | listed (whole) | transfer (p3's 14 columns, ≈ 42 %) | cost |
|---|---|---|---|---|
| **2026-09-28** | `by_dex/hyperliquid/fills/perp/all/date=2026-07-01 … 2026-08-02/fills.parquet` (33) | 7.91 GiB | **≈ 3.3 GiB** (+ ≈ 2 MiB of footers) | ≈ $0.30 |
| 2026-11-23 | fills 2026-07-01 … 09-27 (89) + snapshots 2026-10-07 … 11-19 (44, ≈ 23 MB each) | 29.72 + ≈ 1.0 GiB | **≈ 13.5 GiB** | ≈ $1.2 |

- **The cap (B-M2, A-m8):**
  - it is cumulative over every attempt;
  - it counts footers;
  - it is measured with `archive.bytes_pulled` in a `finally`;
  - **every attempt is a committed `holdout_fetch_attempt` row** (review r1, m2). The cap and the
    persistence clock come from those rows:
    - a `start` row carries the reservation, written after the plan and before any GET;
    - an `end` row carries the measured bytes and the refusal keys, written in a `finally`;
    - edits can only add versions of a row, so the bytes of an attempt are the maximum over its versions;
    - an attempt killed before its end row counts its reservation;
    - the persistence clock is the commit time, not an editable field;
  - the fetch refuses before any object whose planned size would cross it.
- **P3 checks:** G1 and G2 run on all 33 pulled days, and G3 (chain breaks) on a fixed sample (B-m6).
- **Sentinel (A-m7):** a README marks the holdout days in the shared partition root.
- **Public pulls:**
  - Bitget 1H candles for the 168 mapped symbols (BTCUSDT among them), 2026-05-28 → the read bound;
  - HL `fundingHistory` for the 168 mapped coins, 2026-06-30 → the read bound;
  - HL `meta` and Bitget's contract list.
- **Today's contract list:** **0 of the 168 mapped symbols are gone from Bitget, and all 168 are
  `normal`** (827 contracts listed, 2026-10-12 21:51 UTC).
- **Ceiling (review r1, m2).** At most **8 names (about 5 % of the 168)** may become unavailable: not
  listed now, not in HL `meta`, or persistent after 3 attempts. Above that, the fetch stops for an
  orchestrator decision.
  - Why 8: today 0 of 168 are gone. Bitget removes a handful of contracts a month. Losing 8 leaves 95 % of
    the universe.
  - More than 8 points to something systematic (a rate limit, an API change). Dropping them would silently
    change the book.

## 3. Power and false-confirm rates (simulation, synthetic data only)

`ct/crowd008.simulate`, printed by `r008_power.py`, 4,000 simulations per cell, fixed seeds.

**Calibration** (001 §12; no data read):
- daily SD 0.553 %;
- effect sizes 0, half (3.73 bp/day) and full (7.46 bp/day);
- BTC N(month drift, 2.5 %), with Aug +24 % (0.70 %/day) and Sep +8 % (0.26 %/day) from 001 §2, and July 0
  or −0.3 %/day;
- alt-minus-BTC factor N(drift, 1.5 %), with the alt index = 1.1 × BTC + that factor;
- reversal factor N(drift, 0.8 %).

The base model has a constant β of 0.3. Rules compared:
- **registered (Revision 2):** C1–C3 on the regression of §5, with BTC, alt and reversal loadings per cell
  (k 10 / 16);
- **Rev 1:** per-cell BTC, with alt and reversal pooled (k 6 / 8);
- **Rev 0:** a single BTC slope;
- **raw:** the raw mean.

**Power and size, base model** (registered; Rev 1 / Rev 0 in brackets). P(bar | confirm) is P(confirm and
bar) divided by P(confirm).

| design | days / G / t_{G−1}(0.95) | effect 0 | half | full | P(confirm and bar), full |
|---|---|---|---|---|---|
| **2026-09-28**, iid | 83 / 13 / 1.782 | **0.045** (0.048 / 0.050) | **0.135** (0.139 / 0.137) | **0.282** (0.295 / 0.307) | 0.087 |
| 2026-09-28, AR 0.15 + t₄ | | 0.051 | 0.130 | 0.262 (0.269 / 0.274) | 0.108 |
| 2026-11-23, iid | 136 / 21 / 1.725 | 0.048 | 0.164 | 0.403 (0.424 / 0.431) | 0.043 |
| 2026-11-23, AR 0.15 + t₄ | | 0.053 | 0.171 | 0.373 (0.387 / 0.397) | 0.065 |

**The power cost of per-cell loadings** is about 0.01 at 2026-09-28 and 0.02 at 2026-11-23. Under the
exposures of N1, N10 and N11c, full-magnitude power is still 0.281–0.285 (2026-09-28) and 0.401–0.406
(2026-11-23).

**False-confirm rate under the reviewers' nulls** (no alpha), 2026-09-28 / 2026-11-23:

| null | registered | Rev 1 | Rev 0 | raw mean |
|---|---|---|---|---|
| N1 cohort betas A 0 / B 0.5, July −0.3 %/day | **0.047 / 0.047** | 0.048 / 0.050 | 0.229 / 0.146 | 0.280 / 0.212 |
| N2 cohort betas A −0.2 / B 0.3, July −0.3 %/day | **0.047 / 0.047** | 0.048 / 0.050 | 0.229 / 0.146 | 0.294 / 0.221 |
| N3 BTC beta Jul 0 / Aug 0.4 / Sep 0 | **0.051 / 0.045** | 0.049 / 0.048 | 0.148 / 0.135 | 0.202 / 0.158 |
| N4 alt loading 0.3, alt-minus-BTC +0.2 %/day throughout | **0.048 / 0.046** | 0.049 / 0.048 | 0.164 / 0.220 | 0.334 / 0.334 |
| N5 alt loading 0.3, +0.3 %/day in August only | **0.050 / 0.045** | 0.050 / 0.048 | 0.092 / 0.075 | 0.243 / 0.184 |
| N6 reversal loading 0.5, factor +0.10 %/day | **0.051 / 0.045** | 0.049 / 0.051 | 0.155 / 0.199 | 0.325 / 0.305 |
| N7 reversal loading 1.0, factor +0.10 %/day | **0.051 / 0.045** | 0.049 / 0.051 | 0.222 / 0.300 | 0.374 / 0.394 |
| N8 month-long persistence, 5 % of variance, ρ 0.9 | **0.083 / 0.086** | 0.081 / 0.084 | 0.084 / 0.088 | 0.218 / 0.173 |
| N9 month-long persistence, 3 % of variance, ρ 0.97 | **0.094 / 0.116** | 0.096 / 0.116 | 0.099 / 0.118 | 0.224 / 0.192 |
| N10 combined N1 + N4 + N6 | **0.049 / 0.049** | 0.047 / 0.051 | 0.506 / 0.545 | 0.514 / 0.493 |
| **N11** alt loading A 0 / B 0.5; alt drift Jul 0, later +0.2 %/day | **0.048 / 0.046** | 0.086 / 0.079 | 0.144 / 0.236 | 0.288 / 0.349 |
| **N11c** alt loading A −0.2 / B 0.3; drift Jul −0.2, later +0.2 %/day | **0.050 / 0.046** | 0.138 / 0.128 | 0.151 / 0.207 | 0.319 / 0.316 |
| **N12** reversal loading A 0 / B 1.0; drift Jul 0, later +0.10 %/day | **0.049 / 0.045** | 0.090 / 0.076 | 0.142 / 0.240 | 0.293 / 0.337 |
| **N13** heavy tails: BTC t₃, residual SD rising with \|BTC\| | **0.053 / 0.049** | 0.053 / 0.049 | 0.053 / 0.049 | 0.228 / 0.174 |
| **N14** alt loading Jul 0 / Aug 0.4 / Sep 0; drift +0.3 %/day in August | **0.050 / 0.045** | 0.089 / 0.090 | 0.109 / 0.101 | 0.268 / 0.203 |
| **N15** reversal loading Jul 0 / Aug 1.0 / Sep 0; drift +0.15 %/day in August | **0.050 / 0.046** | 0.105 / 0.096 | 0.126 / 0.104 | 0.280 / 0.220 |

**Reading.**
- **The registered rule holds 0.045–0.053** under every market, alt, reversal and heavy-tail null tried
  (N1–N7, N10–N15).
- **Pooled loadings fail when exposures vary.** Revision 1's pooled alt and reversal loadings gave 0.08–0.14
  under cohort- or month-specific exposures (review r1 B1), and Revision 0's single BTC slope gave
  0.09–0.55. The per-cell design is the only variant tried that holds within-window month changes
  (N14, N15).
- **Month-scale persistence is not controlled.** A slow residual component that 13 or 21 week clusters
  cannot absorb gives **0.08–0.12** (N8, N9).
  - Month clusters (G = 3) would leave no test.
  - A critical value raised to 2.18 would hold N8 at about 0.05, but at a third of the power (review r1 §5).
  - It is therefore stated, not fixed (§10).
  - A *reported* flag, "persistence-sensitive", is set when t_{G−1}(0.95) ≤ t < 2.18 (§5). It decides
    nothing.
- **Power stays low:** 0.28 at full magnitude and 0.14 at half (2026-09-28). A non-confirm means "not
  demonstrated at this power".
- **The bar binds.** At 2026-09-28, P(bar | confirm) is 0.31 at full magnitude and 0.21 at half.
- **In-sample estimates.** The loadings are full-sample OLS on the read's own days. They are not
  point-in-time, and C2 and C3 inherit them, so "both halves > 0" is not an out-of-sample stability check
  (A-m5).

## 4. The rule, frozen (001 §8; 008's changes in bold)

| parameter | value | source |
|---|---|---|
| venue, data | Hyperliquid main dex every-account daily snapshots and fills; Bitget USDT-M 1H closes for returns | 001 §2 |
| selection grid | lookback 91 d, hold 56 d, step 56 d from 2025-10-27; windows A, B (and C) | 001 §2 |
| population | slow: eligible (≥ $10k equity ≤ 7 d old at S, maker share ≤ 0.5, ≤ 500 fills per active day, ≥ 80 % mapped notional), median hold ≥ 24 h, ≥ 10 round trips; vaults excluded | 001 §3 |
| score / membership | cross-regime min(t_rising, t_falling), ≥ 4 trips per regime; long-bias, equity as of S and typical leverage present | 001 §4, D7 |
| cohort / crowd | top / bottom quintile by score (k = n // 5; address order, stable sort) | 001 §8 |
| tilt, universe | group net signed ÷ group gross; Bitget-mapped (2026-10-07 table), listed ≥ 31 days, ≥ 3 cohort holders | 001 §8 |
| **signal** | **−(tilt_top − tilt_bottom)** | **008** |
| entry, book | first full hour ≥ snapshot + 1 h; long top / short bottom quintile (≥ 5 coins), inverse 30-day vol, 0.5 gross a side, a daily tranche living 3 days | 001 §8 |
| costs, funding | 6 bp taker + 5 bp slippage one way per tranche entry and exit; HL funding as the proxy | 001 D2, D4 |
| **window floor** | **a window whose cohort is under 20 wallets, or whose median signal breadth is under 10 coins, is excluded from the test and reported**. It is decided on positions before any return (A-m6). For scale, 001's cohorts were 80–153. | **008** |

**Deviations from 001, each forced by data availability or by the holdout-only design:**
1. **The holdout cut.** Window A's June days are dropped (they are discovery).
2. **Funding proxy pulled fresh.** Same API; 001's cache ends 2026-07-10 and is never opened.
3. **Bitget closes in 008's own store.** Same endpoint and close semantics, tested equal. Never an empty or
   error answer cached. Bounded at end + 2 h.
4. **001's static lists reused unchanged** (vault list 2026-10-08, symbol table and listing dates).
   - **A point-in-time symbol table is not feasible without holdout reads.** Bitget serves no history for
     removed symbols, and the dates of HL delistings inside the holdout come from HL's own holdout data.
   - The resulting survivorship is measured and reported instead (B-M4, §5 reading rule).
5. **Not computed:** 001's T1/T2/T2b/T4 machinery and the retail-crowd variant.

**Decision-rule changes from 001 §8** (not rule parameters; each follows a programme lesson or a review):
- (a) an existence test at t_{G−1}(0.95);
- (b) before trading costs;
- (c) α net of per-(window, month) BTC slopes, an alt index and a reversal factor (A-B1, A-M1, `000l`);
- (d) the IC reported, not required;
- (e) the economic bar on the half-shrunk α (A-M2);
- (f) the window floor.

## 5. The decision (existence test, then the economic bar)

On the daily book series **before trading costs** (funding included), keyed by entry:

    gross_t = α + Σ_c (b_c · BTC_t + g_c · ALT_t + d_c · REV_t) · 1[cell(t) = c] + e_t        (one common α)

Every cell c has its own BTC, alt and reversal loading (Revision 2, review r1 B1): k = 1 + 3 × cells, which
is 10 at 2026-09-28 and 16 at 2026-11-23.

**The regressors** (`crowd008.day_factors`, `beta_cells`). All come from the same Bitget closes, for the same
span [entry, entry + 24 h), and are point-in-time.
- **BTC_t:** BTCUSDT's return.
- **cells:** one set of loadings per (grid window, calendar month).
  - Cells of fewer than 10 book days merge into the preceding cell, or the following one if they come
    first.
  - For 2026-09-28 the cells are (A, Jul) 30, (B, Aug) 29 and (B, Sep) 24 days: one slope per month.
  - For 2026-11-23, window C's three September days merge into (B, Sep), giving 5 cells.
- **ALT_t:** the equal-weight return of the day's universe.
  - The universe is the union of the signal coins of the three live tranches (this signal day and the two
    before), with BTCUSDT excluded.
- **REV_t:** the short-term reversal factor. Long the lower half and short the upper half of the same
  universe, ranked by the return over [entry − 24 h, entry), which is known at entry. It is equal-weight,
  0.5 gross a side, with the middle name of an odd count left out, and 0 on a day with fewer than 4 names
  (counted).

**The factor-adjusted daily return** is a_t = α̂ + ê_t = gross_t − (the fitted factor part). Its mean is α̂.

**CONFIRM iff all three hold:**
- **C1** α̂ > 0, with a **week-clustered** (CR1: G/(G−1)·(n−1)/(n−k)) t ≥ **t_{G−1}(0.95)**, and G ≥ 6 ISO
  weeks;
- **C2** median(a_t) > 0;
- **C3** both halves of a_t (by day count) have a mean > 0.

**Clusters:** ISO weeks of the entry (UTC).

**The economic bar, applied only after a CONFIRM** (A-M2).
- The quantity is **half** the factor-adjusted gross α̂, minus the book's mean daily cost at 001's costs,
  × 365. The bar is ≥ 3 %/yr on gross capital (HLP's lending leg). That needs a gross α̂ ≥ 16.2 bp/day.
- **Why half and not the 95 % lower bound.** At 2026-09-28 a confirm needs t ≥ 1.78. The one-sided 95 %
  lower bound of the gross α is then about α̂ − 1.65·se, close to 0. After 7.3 bp/day of cost the bar could
  essentially never be met, which makes the bar vacuous in the other direction. The half is the planning
  case used throughout (§0, §9), and it binds 18–28 % of confirms (§3).

| label | condition | what follows |
|---|---|---|
| `confirm_gross_and_shrunk_net` | CONFIRM, and the half-shrunk net clears 3 %/yr at 001's costs | a **cost-reduced registration** (maker execution or lower turnover) on the remaining calendar, 2026-09-28 → 2027-06-30, through a D18 amendment, with its decisive span starting on or after 2026-10-08 (§2). Size there, not here |
| `confirm_gross_only` | CONFIRM, shrunk net below the bar | the same cost-reduced registration. The effect exists before costs, but nothing here shows it can pay for them |
| `not_demonstrated` | otherwise | L3 closes: "not demonstrated at this power" (0.30 / 0.14 at full / half magnitude), never "no effect" |

**No label leads to trading, or paper-trading, this taker book.**

**Reading rule for survivorship** (pre-specified, decides nothing).
- `crowd008.survivorship_flag` computes it inside the read. It is stored, recomputed by `recheck`, and
  **printed before the decision** (review r1, m4).
- A CONFIRM is annotated **"possibly survivorship-driven"** if, in either window, either:
  - the cohort's or the crowd's gross-notional share in coins dropped only for table status is ≥ 10 %; or
  - **at least 5 distinct dropped coins** would have ranked in a long or short quintile, and their
    long-quintile coin-days are ≥ 2 × their short-quintile coin-days (and > 0).

**Persistence-sensitive** (reported, decides nothing): set when t_{G−1}(0.95) ≤ t < 2.18. That is the range
where a month-scale persistence of the size §3 simulates could carry a confirm. It is printed before the
decision.

The row stores every number the decision uses. `crowd008.recheck` recomputes the label from those numbers
(tested against a forged row). A row that does not recompute is stored with `recheck_failed` and its
decision is **not printed** (B-m2).

## 6. Reported, decides nothing

- **Raw book before costs:** mean, median, week-clustered t, NW t.
- **Net book at 001's costs:** the same raw statistics. **001 T3 as registered** (original sign) on the
  holdout: raw, and with the factor α.
- **Revision 0's BTC-only α**, and the factor α **with 5-day momentum added** (A-m9).
- **Cost sensitivity:** one-way 0, 2, 11 and 22 bp, as factor α annualised.
- **The economic block:** point and half-shrunk net.
- **The inverted 3-day IC.**
- **By window and by month.**
- **Breadth.**
- **Counts:**
  - cohort and crowd sizes;
  - the window floor;
  - signal days;
  - symbol-days without a return;
  - alt or reversal factor missing days;
  - names unavailable at the fetch.
- **Survivorship** (B-M4, A-m4), per window, measured on positions with no price needed:
  - coin-days of HL markets with ≥ 3 cohort holders that are dropped only for table status, by status
    (`delisted_on_hl`, `no_bitget_contract`, `price_mismatch`, not in the table);
  - where they would have ranked (long / short / middle quintile of the full inverted signal);
  - the cohort's and crowd's gross-notional share in them;
  - coins dropped as listed under 31 days, counted separately.
- **P0** (B-m7, A-m10):
  - P0 wallets in each cohort and crowd, by count and by **gross-notional share** on book days;
  - C1–C3 recomputed with P0's 50 wallets removed from both groups.
- **Placebo:**
  - within each day, the signal values are shuffled across that day's coins (200 seeds);
  - the same factor-adjusted t is recomputed for each;
  - reported: the share at or above the actual t, and at or above the critical value;
  - breadth and the volatility weighting are kept, but market exposure is not held fixed.

No reported block may become a primary.

## 7. Costs

- 001's book turns over a third of gross daily: 7.33 bp/day ≈ 26.8 %/yr of gross.
- At the discovery magnitude, the edge before costs (≈ 7.5 bp/day) and the cost are equal.
- Maker execution (≈ 2 bp, no slippage: ≈ 1.3 bp/day) is reported as an upper bound only. Resting orders on
  HL lose about 14 bp at 15 minutes to adverse selection (programme fact 4).

## 8. Clusters and placebo

- **Clusters:** ISO weeks, t_{G−1} (§5).
- **Placebo:** within-day signal permutation, 200 seeds (§6).
- **Persistence beyond a week** is the known limit of week clusters (§3, §10).

## 9. Expected return, tail and sizing (one scenario, carried through)

All at 1× gross (0.5 long, 0.5 short). The daily SD is 0.553 %, so:
- about 10.6 % a year;
- 1.46 % a week if iid, 1.6 % with AR 0.15;
- 3.0 % a month (3.3 % with AR).

**Expected return if confirmed**, from the inverted discovery estimate **shrunk by half**:

| per year, gross 1× | before costs | at 001's costs (7.3 bp/day) | at maker-like costs (1.3 bp/day) |
|---|---|---|---|
| half magnitude (3.7 bp/day), the planning case | +13.6 % | **−13 %** | **+8.7 %** (Sharpe ≈ 0.8) |
| full magnitude (7.5 bp/day) | +27 % | +0.6 % | +22 % (Sharpe ≈ 2.1) |

**The tail: one coherent scenario** (A-M4).

1. **The spiral day** (programme fact 2: spirals decide every mean; more than $77 M of liquidations per 5
   minutes).
   - The inverted long leg holds what the losing cohort leans into, often levered high-beta alts. In a
     long-liquidation spiral, that leg is the one being force-sold.
   - Assume the long leg's coins fall 25 % and the short leg's 15 %. At 0.5 a side that is
     0.5 × (−25 %) − 0.5 × (−15 %) = **−5.0 % of gross**.
   - Add one long-leg name that gaps or delists −40 % against the book's close. One of 20 long names is
     2.5 % of gross, so that is −1.0 %.
   - **The spiral day costs about −6 %.**
2. **The worst plausible week.** The spiral day (−6 %), plus the other six days at −2 SD of their combined
   SD (0.553 % × √6 = 1.35 %, or 1.5 % with AR): −2.7 to −3.0 %. **About −8 to −10 %.**
3. **The worst plausible month.** The spiral day (−6 %), plus the other 29 days at −2 to −3 SD of 2.98 %:
   −6 to −9 %. **About −12 to −16 %.**

A harsher divergence, with the long leg −30 % and the short leg −5 %, makes the spiral day −12.5 % and the
month about −20 %. That is outside "plausible", but within a 2026-scale spiral.

001 never measured this book's beta or worst day, and 008 may not compute them on discovery data. So this is
a scenario, not an estimate. HLP's worst drawdown was −2.5 %, and HLP is long that tail.

**Sizing** (it applies only to the cost-reduced follow-up; this taker book is never sized).
- At 001's costs, Kelly for the planning case is ≤ 0.
- For a cost-reduced version at the planning case (μ ≈ 2.4 bp/day net, σ 0.553 %):
  - full Kelly is μ/σ² ≈ 7.8× gross. That is meaningless: the estimate's own SE (≈ 4.3 bp/day) exceeds μ,
    and the spiral tail is unpriced;
  - **recommendation: at most 1/8 Kelly, and at most 1× gross until a forward period has measured real
    costs**;
  - at 1× that is ≈ +8.7 %/yr, a vol of ≈ 10.6 %, a worst week of −8 to −10 % and a worst month of −12 to
    −16 %.

## 10. Known weaknesses, stated now

- **Post-hoc, and not independent of looks at these months** (§0, §1, §13, `000l`).
- **Low power:** 0.30 / 0.14 at full / half magnitude.
- **Month-scale persistence is not controlled.** Size is 0.08–0.12 under it (§3). A reported flag marks a
  confirm with t below 2.18 (§5).
- **Factor model:**
  - β̂ and the loadings are in-sample (A-m5);
  - the reversal factor uses the 24 h move only, while the book holds 3-day tranches. A loading on 2–3-day
    reversal is not absorbed; 5-day momentum is reported (§6);
  - other exposures (sector, size) remain;
  - **the factors and the book treat a missing next-day close differently** (review r1, m8). The alt and
    reversal factors average only the coins that have a close at entry + 24 h. The book counts such a coin
    at a 0 return, as 001 does. The two are equal when nothing is missing. The run counts symbol-days
    without a return (§6), and the difference is stated, not changed, for fidelity to 001's book.
- **Survivorship favours CONFIRM** (§0, §5 reading rule, §6).
- **Regime:** a bear-year finding tested in a rally.
- **Costs:** the taker book is not capturable at the discovery magnitude.
- **Window B's cohort** depends on the requester-pays holdout fills (P3 G1–G3 run on them).
- **The funding proxy** is HL funding, not Bitget's (001 D4).
- **Fetch end state.** A refusal that persists on 3 attempts ≥ 24 h apart is recorded as unavailable, and
  the fetch completes without that name. A live symbol with a genuine data gap therefore delays the fetch by
  ≥ 48 h, and its gap enters the book as 001 did (a missing return counts 0).
- **Simulations** have no spiral jump, so power at a positive effect is optimistic.
- **Read-once rests on local history plus the owner's push** (review r1, m6).
  - The runner commits every row at once. It never pushes: the owner pushes.
  - A deliberate `git reflog expire --expire=now --all` plus `git gc --prune=now` can erase a local row
    before it is pushed, and that cannot be prevented locally.
  - The owner's push, right after each automatic commit, is the durable record. `--all` then also scans
    the remote-tracking refs.
- **The environment pin is strict** (review r1, m3). An interpreter or package change between the fetch
  and the run (for example a Homebrew patch upgrade of Python) makes (g2) refuse. The recovery clause is in
  §11.

## 11. Built before any outcome is computed

- [x] `ct/crowd008.py`:
  - calendar and the read bound;
  - stores bounded at load;
  - the rule;
  - `day_factors`, `beta_cells`, `ols_multi` (CR1, any k), `design`;
  - `confirm` (C1–C3 on the factor regression);
  - `economic` (half-shrunk), `decide` (renamed labels), `recheck` (its failure text names no label);
  - `simulate`, with every reviewer null.
- [x] `r008_fetch.py`:
  - all eight hooks or none. Real hooks only for the real repository, the real tree and default arguments,
    and they refuse otherwise (B-B1);
  - LIST first; footer plan; cumulative measured cap; P3 G1–G3; sentinel;
  - delisted names recorded, never a stop (B-B2);
  - keyed refusals with the 3-attempts ≥ 24 h end state; partial blocks refused while they block (B-m5);
  - pinned snapshots (B-M1);
  - rows committed as written; manifest (with the snapshot list) and environment in the fetched row.
- [x] `r008_run.py`:
  - the guard: (s), (a)–(e) with duplicate-row and shadow-module checks, (n), (t), (k), (f) over history
    and reflog, (g1)–(g3) with environment and snapshot pins, (r) with exact diff-header parsing, (h'),
    and the restart rule;
  - the real-tree binding and the (root, data) read lock;
  - coverage before `holdout_started`; started and result rows committed at once;
  - survivorship, P0 and window-floor blocks; recheck failure stored and not printed.
- [x] `r008_power.py`: the §3 tables. Not on the outcome path, not hashed.
- [x] `registrations/008-manifest.json`: SHA-256 of 206 on-disk inputs (unchanged from Revision 0).
- [x] `tests/test_r008.py` — **132 checks pass** (Revision 2; synthetic only; throwaway git repos; every hook
  mocked):

      cd research/copytrade && .venv/bin/python -m tests.test_r008
      ... all checks passed (132)

  New in Revision 2:
  - k = 10 in the decision and end-to-end checks.
  - **r1 M1:** mock hooks on a stand-in "real" repository and tree are refused before any row.
  - **r1 m1:** each of these is refused by (d):
    - an untracked `duckdb.py`;
    - a top-level `duckdb/` package;
    - a `ct/stats/` package;
    - a sourceless `pyarrow.pyc`;
    - a git-ignored `json.py`.
  - **r1 m2:** the attempt bytes are a committed row; edited rows cannot lower the cumulative bytes; a
    killed attempt counts its reservation; a back-dated clock does not make a refusal persistent; more than
    8 unavailable names stop the fetch; persistence needs 3 commits ≥ 24 h apart.
  - **r1 m4:** the survivorship rule needs its minimum count (1 against 0 is not flagged); the flag
    recomputes in `recheck`; the read prints it before the decision.

  New in Revision 1:
  - **Factors and the regression:**
    - cells (3 for 2026-09-28; 5 for 2026-11-23 with the merge);
    - `ols_multi` recovers planted slopes and equals the one-regressor form;
    - rank deficiency;
    - alt, reversal (point-in-time, odd middle name) and the live-tranche universe.
  - **Decision:**
    - planted edge (k = 6);
    - cohort-specific betas: a 60-draw bias check, where one pooled slope biases α by 8 bp/day and the
      per-month slopes leave none;
    - a pure-reversal book does not confirm;
    - the half-shrunk bar edge (16.2 bp/day);
    - the renamed labels.
  - **Calibration:** the combined null at 0.05 for the registered rule against more than 0.3 for the
    draft's.
  - **Guard:** each of these is refused:
    - a shadow `duckdb.py` (B-m11);
    - a dirty `results.jsonl` (k);
    - a duplicated hash row (B-m12);
    - a holdout row removed with `reset --hard`, found through the reflog (B-m1).
  - **Bypasses (B-B1):** each of these is refused:
    - a throwaway gate on the real tree;
    - an overridden guard argument;
    - real hooks from a throwaway repo;
    - a partial mock dict;
    - mock hooks writing into the real tree;
    - the read lock for another (root, data) pair;
    - a direct real-hook call;
    - the data-directory refusal under `-O`.
  - **Fetch:**
    - a transfer that raises still counts its bytes;
    - a deleted fetch log refuses;
    - an error on a live symbol becomes a keyed refusal, and the attempt continues;
    - a symbol Bitget no longer lists is recorded;
    - a partial block refuses twice, then is recorded as persistent on the 3rd attempt ≥ 24 h later, and
      the fetch completes;
    - the exact byte total across attempts;
    - rows committed.
  - **Run:**
    - an injected snapshot (unlisted directory) and a second file in a date are refused (B-M1);
    - restart ordering;
    - offline read with auto-commits;
    - k = 6;
    - survivorship counts for the `delisted_on_hl` coin;
    - P0 shares and the no-P0 confirm;
    - environment and bar fields;
    - determinism;
    - the window floor excludes both windows (not_demonstrated);
    - a second read refused, and still refused after `reset --hard`;
    - a recheck failure is not printed.

**Pre-specified procedure.** At registration the owner (or the orchestrator, under the 2026-10-12
delegation) confirms §12, including D13, and commits this file as REGISTERED.

**Before the fetch:**
- every file in `research/copytrade` and `ct/` is committed. This includes the other studies' drafts, since
  check (d) refuses any untracked, ignored or modified importable file there;
- the venv is not changed again until the read is done.

Then, no earlier than end + 7 days:

    .venv/bin/python -I r008_fetch.py      # gate; holdout_fetch_started (committed); LIST; plan; cap; pull; P3; coverage; holdout_fetched (committed)
    #   read the byte count, the unavailable names and the coverage in the fetched row
    #   add the line  008 holdout read: APPROVED <date> row <sha256 of the holdout_fetched line>  under §12, commit
    .venv/bin/python -I r008_run.py --check  # every reason the read may not start
    .venv/bin/python -I r008_run.py --run    # gate; coverage; holdout_started (committed); the read, once; one holdout row (committed)

- **A fetch that stops** on a typed error or a coverage refusal is re-run later. It resumes, and it writes no
  fetched row until coverage passes, or until a refusal has persisted on 3 attempts ≥ 24 h apart.
- **A mechanical stop during the read** (an exception after the committed `holdout_started` row):
  - the orchestrator ledgers it with its traceback;
  - it then adds, in a later commit, the line `008 holdout restart: APPROVED <date> started-row <sha256 of
    that row>` at the start of its own line;
  - the guard then allows one more read. The code cannot change: (g2) compares it with the registration
    commit stored in the fetched row (B-m9). Nor can the environment (g2) or the inputs (g3).

- **A failed automatic commit** (review r1, m7). It is retried once. If it fails again, the run stops with
  "commit results.jsonl by hand, as it is; do not discard". (k) then refuses every later fetch or run until
  that commit exists. The decision is never left silently uncommitted.
- **Pushing.** The runner never pushes. The owner pushes after each automatic commit (§10).
- **An interpreter or package change between the fetch and the run** (review r1, m3).
  - Rebuild the venv to the pinned versions, with the same Python patch release and the same wheels, until
    `r008_run.py --env-record` reports `equal_to_fetched: true`.
  - That command writes and commits its comparison as a `holdout_env_record` row.
  - The pin covers **every installed file** of duckdb and pyarrow (Python sources, binaries, data), not
    only their versions.

No other edit to this file is allowed after the fetch (r).

**SHA-256 of the code and data the outcome depends on** (read by the guard).

| file | sha256 |
|---|---|
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/crowd008.py` | `8123b34a105a5fcddb126d7fe7166ba5db95cf11f602cf1adfa0cdb99986fd9b` |
| `ct/crowd.py` | `1a7335579dda2c3f05f92bef2651d0e2cebf8f6b52c357094d801329e2da38ac` |
| `ct/scores.py` | `1786a8ccfd2835c4ff058175bc779c168ef2c459bf79e050a5adbdbb0f3122e6` |
| `ct/stats.py` | `bfcc85f285dc7af28f364e2d77e0fbe04b6c2367724f4b822b7fbf6ff435d472` |
| `ct/schedule.py` | `24d1337357499d1bf1df2bc904a4b3a90199cd5e71f57df9ff8ba340c86c781d` |
| `ct/leaders.py` | `378fbf403e8c372c2d2795b5b19c256199a1dc9360b8c6a4da55803f501ed329` |
| `ct/archive_fills.py` | `0727c03224e26ab13d70384fb1d974a47eaa51c00ef98f0ba61103a97715b418` |
| `ct/archive.py` | `a6bca7f3a5e3f429a5982a068b1d6fbec3562624d68cf9f4491c54370dbf48e3` |
| `ct/regimes.py` | `331c2bf1ee45236312785bd6a51fde2bc891e3c0fcf114386d29771dcc595191` |
| `ct/net.py` | `90092590941e805eb465e7e1f05467c05e376bf8b07fe6dbdacfdb6cf7e00006` |
| `ct/bitget.py` | `024ce24d11a4ec7366b8ea54dd719f30700f235b9f8396543f67b25b0684204d` |
| `ct/hl.py` | `943b3c96aab4aa5f8af9f5f18415e240c134a66cf1fa2bab39c747bcb03bac75` |
| `p3_checks.py` | `b82581211a490ce88172bf41fa87d16ff5f71ec0287f20ce98f78eca8e238eaa` |
| `p4_features.py` | `cbfff6122678f52dd78fd811f9b985fada49ac8eee99ef6e6bed90fc3c69b4b9` |
| `p5_population.py` | `26716a1b9e8acdae2cd8493c2f0ef6116aa26472cd10054de2b926da3c2cac7a` |
| `r008_run.py` | `bf9e702a565332b053444adf7bd81f42d120de89c72f4fe42a92afcaa7d3d5fc` |
| `r008_fetch.py` | `979a7acff6e5bc8c89d10869a4833e879aaff974b5e22422ad32d4075fa4fc97` |
| `data/p0/2026-10-07/symbols.json` | `02f0aaebd71be99ba0d49c7d7afbdfae5be41272979fb23eb71619c6033d010f` |
| `data/p0/2026-10-07/report.json` | `0aeb2042ce128d3f248570e950779e594865fe0c0c4c3f12224f0788b2aac3a6` |
| `data/derived/bitget_listing.json` | `f7334060966b962b40dcf95e812352dfd5e5f43a82719d2da1321639d2100153` |
| `data/cache/hl-vaults/903952e394cd91c74dc593a3.json` | `0cf1facb9acb30de849d86e46197832ed730433428fde73e7c80052734062c94` |
| `data/derived/regimes/btc_1h.parquet` | `dc8ff7356725b9f74fc093071321f5569c567559a0232342ffa7af8141b0b537` |
| `data/derived/hyperliquid/snapshot_accounts.parquet` | `4aebf98b1bbec7096b839574295960aedd0ce88a580e9cb8bc9b286a4419e9bc` |
| `registrations/008-manifest.json` | `b973031b4c4ee4a841d40c1eab2a4e2a8a23b4eb350b50734d5d4463db537b92` |

**Environment at drafting** (frozen at the fetch; (g2) requires the run to match the fetched row):
- Python 3.14.8 (Homebrew);
- duckdb 1.5.6, 56 files, digest `ea9da79d…`;
- pyarrow 25.0.1, 755 files, digest `bfdda389…`.

## 12. Owner decisions — APPROVED 2026-10-13 (orchestrator, under the owner's 2026-10-12 delegation; each row as proposed)

| # | item | proposed |
|---|---|---|
| D1 | the rule | **001 T3's book unchanged with the signal negated; the existence test is new** (§0, §4, §5) |
| D2 | calendar and end | holdout only, from the first snapshot ≥ 2026-07-01 00:00 UTC; **end 2026-09-28** (§2); the offered alternative is 2026-11-23. Nothing at or after the read bound is fetched or parsed |
| D3 | schedule | 001's grid continued: S = 2026-06-08 and 2026-08-03 (+ 2026-09-28); window A is truncated at its start by the holdout cut; the window floor applies (§4) |
| D4 | the pull | 33 requester-pays fill objects, ≈ 3.3 GiB, ≈ $0.30; LIST first; exact footer plan; **15 GiB cap, cumulative and measured**; only through `r008_fetch.py` after REGISTERED. Public: Bitget 1H, HL funding, HL meta, Bitget contracts, all bounded at end + 2 h |
| D5 | CONFIRM | §5: C1 factor-adjusted α, with BTC, EW-alt and 24 h-reversal loadings **per (window, month) cell** (k = 10; 16 at 2026-11-23), week-clustered t ≥ t_{G−1}(0.95), G ≥ 6; C2 median of a_t > 0; C3 both halves > 0. False-confirm **0.045–0.053** under every market, alt, reversal and heavy-tail null of §3 (N1–N7, N10–N15), and **0.08–0.12 under month-scale persistence (N8, N9), which is not controlled**. A persistence-sensitive flag (t < 2.18) and the survivorship reading rule are reported, not criteria |
| D6 | economic bar and what follows | after a CONFIRM: the half-shrunk net ≥ 3 %/yr at 001's costs. **Every confirm leads to a cost-reduced registration on 2026-09-28 → 2027-06-30** (decisive span from 2026-10-08, `000`). No label trades or paper-trades this taker book |
| D7 | family | one trial; the first and only reader of the HL holdout calendar; HL discovery year stays at 16; post-hoc and not independent of `000l`'s looks (stated) |
| D8 | reported | §6, including survivorship (reading rule in §5), P0 share and no-P0 confirm, momentum α and placebo |
| D9 | deviations | §4 items 1–5 |
| D10 | read discipline | §2 whitelist; offline; real-tree binding; (root, data) read lock; pinned snapshots; holdout caches never opened; the explicit snapshot exception in D13 |
| D11 | sizing | this book: none. A cost-reduced follow-up: ≤ 1/8 Kelly and ≤ 1× gross; worst week −8 to −10 %, worst month −12 to −16 % at 1× (§9) |
| D12 | a non-confirm | `not_demonstrated`; L3 closes; the inverted-positioning idea is not retested on the HL calendar without a registration that amends D18 openly |
| D13 | **D18 amendment (B-M3)** | 012 D18 (b) lets L3 read "the series its registration names". **In addition to the pulled series of §2 and D4, 008 names, and alone may read, through its guard and only for the dates named here:** (i) the holdout rows of `data/derived/hyperliquid/snapshot_accounts.parquet` with `snapshot_ms` in [S − 91 d, S), for S = 2026-08-03 (and 2026-09-28 at the alternative end), that is 2026-07-01 → 2026-08-02 (→ 2026-09-27); they are read by `p4_features.features` and `leaders.Equity`, bounded in the query; (ii) the raw holdout snapshot partitions for the signal dates of §2, as the pinned manifest list. **D18 (a)'s sentence "Neither is read by any registration" was written for 012's context, and it is amended to this extent only.** Everything else in `000c`'s mitigation stands: every other script filters `snapshot_ms < 1782864000000` in the query itself. **Approved by the orchestrator on 2026-10-12, under the owner's delegation** (owner, 2026-10-12: "you are allowed to register yourself and take decision as you would recommend as long as they take us nearer towards the goal"); to be confirmed at registration |

Owner line. It stands alone, starts its line and appears once in this file. The offered values are
2026-09-28 and 2026-11-23. The value below was confirmed at registration (orchestrator, 2026-10-13):
faster to read (data mostly on disk, ≈ 3.3 GiB), well inside the 15 GiB cap, and it leaves
2026-09-28 → 2027-06-30 unread for any follow-up.

008 holdout end (owner): 2026-09-28

The read's approval line is added only after the fetched row exists, and a restart line only after a
mechanical stop. Their forms are in §11. Nothing else may change in this file after the fetch: (r) refuses
any other edit.

## 13. Disclosures and real-data reads (for the ledger)

**Earlier looks at 008's months** (A-M3, `000l`). These are listed in full. What is and is not independent
of them is stated in §1.

1. **The 2026-09-14 alpha-lab sweep** (`docs/alpha-lab-spec.md` §8).
   - Scope: 83 Bitget USDT perps, 4H bars from 2024-03 through 2026-09.
   - Results seen: 1-day reversal IC +0.0275 (t 4.0); 5-day momentum −0.021 (t −3.0); costed quintile books
     that lose gross.
   - These are full-period statistics that include 008's months, on the same venue and a heavily
     overlapping universe.
   - What follows: C1 includes a 24 h reversal factor, and 5-day momentum is reported.
2. **The 2026-10-11 inverted-AI study** (`docs/inverted-ai-and-prompt-data-2026-10-11.md`).
   - The live AI's decisions from 2026-06-27 to 2026-10-10, inverted, earned +0.95 ATR per trade net at
     48 h, including when the AI itself was fading the move.
   - That is a contrarian payoff in exactly these months, seen one day before N6 was ranked first and its
     pull approved.
   - What follows: the reversal control (binding, `000l`), and the statement in §1.
3. **The live AI trader** traded Bitget majors through these months. The owner monitored its P&L.
4. **`000i`–`000k`** (round 3):
   - post-cutoff announcement titles;
   - current-state `xyz` docs;
   - unconditional vol levels.

   None touches HL positions or HL-coin returns.
5. **`000l`** itself, written after review A.
6. **`000` (P0).** HL fills 2026-08-08 → 2026-10-07 for 50 random leaderboard wallets, and one day-trader's
   copy result.
   - This overlaps window B's book period, not its lookback.
   - Its clause applies: if the two holdouts disagree, the untouched one decides (§2).
   - Reported: P0's count and notional share per group, and C1–C3 without P0's wallets.
7. **`000c`.** Counts only. D13 now names the exception to its mitigation.
8. **The BTC path is known** from 001 §2 (Aug +24 %, Sep +8 %), and `btc_1h.parquet` holds BTCUSDT hours to
   2026-10-07.
   - It shaped C1's per-month slopes.
   - For a book with β < 0 a market adjustment *raises* α relative to the raw mean (A-m5). Size is
     controlled either way (§3).
9. **The reviewers' reads.**
   - **Review A:**
     - the `symbols.json` status counts (168 ok, 56 delisted_on_hl, 9 no_bitget_contract, 1
       price_mismatch);
     - the discovery-only BTC trend sign in A's and B's lookbacks (68 % and 32 % rising);
     - the summaries of items 1–2.
   - **Review B:**
     - the footer schemas of all 384 snapshot files;
     - file names, date gaps and name timestamps. That read found one late holdout snapshot (2026-08-23
       16:52) and one discovery date (2026-03-24) holding 2 files;
     - the `ts` column of `btc_1h.parquet` (0 missing hours 2026-05-28 → 09-30);
     - the static-file key counts;
     - `pruned/` and `data/cache` listings;
     - SHA-256 hashes through the real guard;
     - `results.jsonl`'s line count and log subjects.
   - Neither computed any price, return, funding, position, tilt, cohort or account value dated on or after
     2026-07-01.
10. **012** never read the calendar.

**The drafter's real-data reads.** None computed a quantity dated on or after 2026-07-01 other than counts,
coverage, sizes and current-state names.

*Revision 0 (2026-10-12):*
1. **One S3 LIST** of `by_dex/hyperliquid/fills/perp/all/` and `by_dex/hyperliquid/snapshots/perp/` (LIST
   requests only).
   - Printed: 441 fill objects 2025-07-28 → 2026-10-11, and 389 snapshot objects.
   - Size sums by date range: discovery 103.13 GiB; 07-01 → 08-02 7.91 GiB; → 09-27 29.72 GiB; → 10-11
     36.15 GiB; snapshots 10-07 → 10-11 0.11 GiB.
   - Keys and sizes are kept in the session scratchpad, outside the repo.
2. **Snapshot file names:** 98 holdout dates; per-window counts 30 / 53 / 9; hour of day.
3. **`btc_1h.parquet`:** the count of rows ≥ 2026-07-01 (2,376), the max timestamp, and the all-period row
   count 63,517.
4. **Existence only:** `bg-1h` block files around the cut (167 of 168 to 2026-07-16), and the
   `hl-funding` file count (178).
5. **The sparse footer file size** of the 2026-09-15 fills object (448,632,947 bytes).
6. **Schemas only:** one discovery fill day (2026-06-10), one discovery snapshot (2026-06-15),
   `snapshot_accounts.parquet`.
7. **Metadata:**
   - P0 report keys;
   - the BTC entry of `symbols.json`;
   - the vault count, 9,477;
   - 168 listing dates and two 2025 values;
   - 168 mapped coins.
8. **SHA-256:** 114 discovery fill days, 92 holdout snapshot files, `snapshot_accounts.parquet`,
   `btc_1h.parquet` and the static JSONs (bytes only).
9. **`results.jsonl`:** line 1's T3 block, and the keys and modes of every row.

*Revision 1 (2026-10-12/13):*

10. **Bitget's current public USDT-M contract list** (`/api/v2/mix/market/contracts`, one request,
    2026-10-12 21:51 UTC). Symbol names and `symbolStatus` only, no price:
    - 827 contracts;
    - **0 of the 168 mapped symbols missing**;
    - all 168 `normal`.
11. **`git diff --quiet efefc7f HEAD -- p3_checks.py`:** byte-identical.
12. **Hashing for §11:** code, static data, and the Python environment's duckdb and pyarrow binaries.
13. **Synthetic only:** all of §3, and every test.

## 14. Revision 1: findings and the changes that answer them

Line numbers refer to Revision 1's files.

| finding | change | where |
|---|---|---|
| **A-B1** one constant BTC β (size 0.11–0.25) | one BTC slope per (window, month) cell, merge < 10 days; nulls N1–N3 re-simulated (0.047–0.050) | `ct/crowd008.py` `beta_cells` l.385, `design` l.471, `confirm` l.489; §3, §5 |
| **A-M1** alt factor omitted | equal-weight universe index in C1; nulls N4–N5 (0.048–0.050) | `day_factors` l.355; §5 |
| **000l / orchestrator 1** contrarian payoff seen | 24 h reversal factor in C1; N6–N7 (0.049–0.051); 5-day momentum reported | `day_factors`, `_ew_half_spread` l.344; `r008_run.py` `compute` l.475 |
| **A-M2** vacuous bar, contradicting labels | the bar on the half-shrunk α (16.2 bp/day gross); labels `confirm_gross_only` / `confirm_gross_and_shrunk_net`; every confirm leads to a cost-reduced registration, none to trading this book | `economic` l.525, `decide` l.538, `LABELS` l.64; §5, D6 |
| **A-M3** earlier looks incomplete | §13 items 1–10, and §1 "what is not independent" | §1, §13 |
| **A-M4** tail inconsistent | one scenario: spiral day −6 %, week −8 to −10 %, month −12 to −16 % | §9, D11 |
| **B-B1** gate bypass through a throwaway root | real-tree binding; (root, data) read lock; complete hook dicts or real hooks only for the real repo; refusals, not asserts; 8 bypass tests | `r008_run.py` `real_tree_failures` l.173, `run` l.666, `compute` l.475; `r008_fetch.py` `fetch` l.471, `_require_real` l.70, `_s3_get_*` l.97–110 |
| **B-B2** fetch dead-ends on removed symbols | non-live names recorded, never a stop; keyed refusals; 3 attempts ≥ 24 h → persistent and unavailable; contract list checked (0/168 gone) | `fetch_closes` l.288, `fetch_funding` l.318, `apply_persistence` l.432 |
| **B-M1** snapshot set not pinned | the manifest stores the snapshot list; the guard compares it with the full-glob selection, one file per date; the run reads only it | `snapshot_pin_failures` (run l.318), `pinned_snapshots` (fetch l.425), `disk_coverage` l.412 |
| **B-M2** bytes and cap | `archive.bytes_pulled` deltas in `finally`; cumulative persisted log; a missing log refuses; refuse before an object that would cross the cap | `_archive_call` l.75, `s3_pull` l.229, `fetch` `finally` l.560 |
| **B-M3** holdout `snapshot_accounts` rows | D13, the explicit D18 amendment | §2, D10, D13 |
| **B-M4** survivorship direction | stated in §0, §5 and §10; measured at run time on positions; pre-specified reading rule; point-in-time table infeasible (§4.4) | `survivorship` (run l.597); §5 |
| A-m1 wording | "the book is unchanged; the test is new"; window A truncated at its start | §0, §2, D1, D3 |
| A-m2 book mechanics | described accurately (gap days, cross-window tranches, last tranches, overlap) | §2 |
| A-m3 persistence | simulated (N8–N9: 0.08–0.10), stated, not fixable with 13 clusters | §3, §10 |
| A-m4 survivorship counts | per-window status counts, inside the read | `survivorship` |
| A-m5 β̂ in-sample; the β < 0 sentence | stated; the sentence corrected | §3, §13 item 8 |
| A-m6 cohort / breadth floor | cohort < 20 or median breadth < 10 means the window is excluded, decided before returns | `compute` l.475; §4; tested |
| A-m7 holdout fills in the shared tree | sentinel README; this entry | `sentinel` l.268 |
| A-m8 byte accounting | as B-M2 | — |
| A-m9 factor confound | the reversal factor in C1; momentum reported | §5, §6 |
| A-m10 C1 without P0 | reported | `compute` (no_p0) |
| B-m1 read-once depends on discipline | rows committed at once (`git commit --only results.jsonl`); (k) clean results; history and reflog scanned; restart lines | `commit_results` l.345, `result_rows` l.93 |
| B-m2 recheck failure prints the label | the row is stored with `recheck_failed`, nothing printed, failure text label-free | `run` l.716–722; `recheck` l.544 |
| B-m3 environment not frozen | `env_fingerprint` (versions plus binary hashes) in the started and fetched rows; (g2) requires equality | `env_fingerprint` l.158 |
| B-m4 reads after the end | read bound end + 2 h at fetch and at load; window C "hashed, not parsed" | `read_bound` l.67; `Closes`/`Funding.from_store` |
| B-m5 partial interior blocks | missing hours counted; a partial block is refused and deleted while it blocks | `closes_coverage` l.351 |
| B-m6 P3 gates on the holdout days | G1–G2 on all 33 days (refuse), G3 sample (reported) | `p3_gates` l.246 |
| B-m7 P0 weight; `000`'s clause | notional share; clause cited; follow-up span from 2026-10-08 | `p0_exposure` (run l.643); §2 |
| B-m8 alt factor | now in C1 | — |
| B-m9 text (13 hashed; restart wording) | 14 reused modules hashed (p3_checks added); p5_run is the oracle; restart wording fixed | §0, §11 |
| B-m10 path checks | stated as acceptable after B-B1 | §2 |
| B-m11 shadowing modules | (d) refuses untracked or modified `*.py` at the top level or in `ct/` | `untracked_py` l.185 |
| B-m12 duplicate hash rows; (r) headers | duplicates refused; only `+++ ` / `--- ` headers skipped | `guard_failures` l.194 ff. |

**Not fixed, and why.**
- **A-m3 (persistence):** with 13 week clusters, no cluster scheme absorbs month-scale persistence. Its size
  is stated instead.
- **A-m2:** the inherited book quirks are left as they are for fidelity to 001.
- **B-m10:** the whitelist remains path-based. After B-B1 that is acceptable, as the reviewer allowed.

## 15. Revision 2: the second-round review and the changes that answer them

From `reviews/008-review-r1.md`, verdict "register after the listed fixes". Line numbers refer to Revision
2's files.

| finding | change | where |
|---|---|---|
| **B1** alt and reversal loadings pooled: false confirms 0.08–0.14 under cohort- or month-specific exposures | one BTC, alt and reversal loading per (window, month) cell: k = 10 at 2026-09-28, 16 at 2026-11-23. N11–N15 added to §3; size 0.045–0.053; power −0.01 to −0.02; D5 restated. The reported factor α (costs, placebo, momentum, no-P0) uses the same design | `ct/crowd008.py` `design` l.474, `confirm` l.493, `simulate` l.635; `r008_power.py`; §3, §5, D5 |
| **M1** mock hooks accepted on the real tree with the default root | any `hooks` argument refuses when the data tree is the real one, whatever the root; stand-in-repo test | `r008_run.py` `real_tree_failures` l.221; `r008_fetch.py` `fetch` l.499 |
| m1 shadow check incomplete | file-system scan of the top level and `ct/`: `*.py`, sourceless `*.pyc`, `*.so`/`*.pyd`, packages (`<name>/__init__.py`; every file of a `ct/` package), git-ignored files included; 5 tests | `r008_run.py` `shadow_modules` l.188 |
| m2 editable fetch log; no ceiling | every attempt is committed `start` and `end` rows; bytes are the max over versions; a killed attempt counts its reservation; the persistence clock is the commit time; a ceiling of 8 names (~5 %), above which the fetch stops for the orchestrator; 4 tests | `r008_fetch.py` `attempt_history` l.433, `spent_bytes` l.456, `apply_persistence` l.460, `fetch` l.499 ff. (ceiling l.575) |
| m3 environment pin brittle, no recovery | the fingerprint hashes every distribution file of duckdb and pyarrow; recovery clause (rebuild to the pinned versions); `--env-record` writes a committed row | `r008_run.py` `env_fingerprint` l.165, `env_record` l.792; §10, §11 |
| m4 survivorship rule only prose; "twice as often" with no minimum | `survivorship_flag`: share ≥ 10 %, or ≥ 5 dropped coins ranked with long ≥ 2 × short; stored, rechecked and printed before the decision; tests | `ct/crowd008.py` `survivorship_flag` l.548, `recheck`; `r008_run.py` `compute`, `run` |
| m5 `-diff` attribute blinds the scan | `--text --no-textconv --no-ext-diff` on the history scan and the (r) diff; `--no-textconv` on `git show` | `r008_run.py` `result_rows` l.93, `first_commit_containing` l.121, guard (r) |
| m6 local history can be erased | the runner never pushes; the owner pushes after each automatic commit; `reflog expire` + `gc` stated as a known limitation | §10, §11 |
| m7 a failed commit leaves the decision uncommitted | retry once; then stop with "commit by hand, as it is; do not discard"; (k) refuses everything until committed | `r008_run.py` `commit_results` l.389; §11 |
| m8 text | persistence 0.08–0.12; D13 "in addition to the pulled series of §2 and D4"; factor vs book treatment of a missing next-day close stated; commit everything before the fetch | §3, §10, §11, D5, D13 |
| reviewer's persistence recommendation | kept as a stated weakness; a reported "persistence-sensitive" flag for 1.782 ≤ t < 2.18 | `r008_run.py` `compute`; `ct/crowd008.py` `PERSIST_T` l.60; §5 |

**Real-data reads in Revision 2:** none. The environment fingerprint hashes the venv's own package files,
which are code, not data.
