# 013 — Short new Binance USDⓈ-M perps at day 1, 1× isolated, 7 days (N3 i: new perp listings)

**Status: DRAFT**

**Revision 2 (2026-10-13), after the second-round review `reviews/013-review-r1.md`** (1 blocker, 4 majors,
minors) and the orchestrator's binding decisions. Every finding is in §R2 with its file and line.

- **B1, regime β.** α is now estimated with **β per calendar quarter of the first trade**:
  net = α + Σ_q β_q·mkt·1[q] + ε, with a common α and CR1 by listing week. This applies in discovery and
  confirmation and in the adjusted series. The reviewer's regime worlds (β and the market's fall
  concentrated in a window, aligned with quarters and misaligned) were added to the calibration set, and
  the critical values were re-derived (§3).
- **M1, economics on min(raw, adjusted).** Discovery criteria 5–6 and the deploy rule use the smaller of
  the raw and the market-adjusted net, so they are conservative in bull and bear calendars alike. Both
  are reported.
- **M2, tail-adjusted α.** The confirmation tests α_tail = α − max(0, π_pool − π_conf)·(1 + the mean
  non-liquidated net), the §6 correction moved inside the test. π_pool pools all three calendars.
- **M3, one continuation rule.** "The token, or its predecessor, had a Binance USDⓈ-M perp before d0",
  checked point in time against the 1d listing by the build, plus (d) gold tokens.
  - LUMIA, SYRUP, CFG, ASTER, BEAMX and USTC return to the events.
  - **Primary events: discovery 239 (65 weeks), confirmation 121 (38), reported 251 (117).**
- **M4, run-once through the hook.**
  - After each commit, the run refuses unless the row is in `HEAD:results.jsonl`, checked with the real git.
  - Mocked hooks, a stubbed guard or a mixed real/synthetic tree are refused on the real files (r008's
    `real_tree_failures`, ported).
- **Minors:**
  - m1: no test goes stale at commit or REGISTERED (checked by a throwaway commit simulation, §10);
  - m2: `--text --no-textconv --no-ext-diff --no-color` on every history and diff call;
  - m3: r008's `shadow_modules`, plus a fresh `sys.pycache_prefix` before `ct` is imported (a forged `.pyc`
    is shown loading without it and not with it);
  - m4: `exit_late` is capped at 24 h and the windows widened to 16 and 40 days, so the tail bounds are
    exact for every exit status;
  - m5: coverage is strictly pre-entry;
  - m6: ledgered;
  - m8, m9: the split-β α and the α placebo are reported;
  - m10: the environment fingerprint hashes every duckdb and pyarrow file; the runner never pushes.

**Revision 1 (2026-10-13)** answered the first reviews (`013-review-a.md`, `013-review-b.md`) and `000n`:
- the α decision;
- tail bounds via rolling caps;
- one deploy rule;
- Binance as the venue;
- continuations;
- dead-row handling;
- run-once and the read gate;
- pre-entry coverage.

Its table is §R.

**No outcome has been computed on any real listing window, in any calendar**, the reported 2020–23 block
included. The pull is done and checksum-verified (§2.3). The build counts only pre-entry coverage
(§2.4). The draft's hold-window totals are ledgered as `000n`.

The runner refuses to compute anything until all of these hold:
- this file says REGISTERED, is committed and is unmodified;
- the code and data match §10;
- 009's terminal state is recomputed (D17);
- `results.jsonl` is clean and holds no `013-` row anywhere in git;
- the environment matches;
- no importable file shadows a module.

Programme: `docs/edge-discovery-next-prompt-2026-10-12.md` (round 3, light N3). Scout: `scouts/n3-listings.md`.
Ledger entries read: `000f`, `000g`, `000i`, `000l`, `000n`. Reviews: `013-review-a.md`, `013-review-b.md`,
`013-review-r1.md`.

## 0. Hypothesis, mechanism, and what has been seen

**Hypothesis.** A new Binance USDⓈ-M perpetual of a new token, shorted 24 hours after its first trade and
held 7 days at 1× isolated margin, earns more than its costs: fees, slippage, funding at Binance's archived
values, and the squeeze tail. It must do so **beyond what the alt market's move over the same hold
explains**. The decision is about the regime we would trade: perp-first listings, 2024 onward. **It is a
Binance result** (D1).

**Mechanism.**
1. **Supply arrives after the listing.** Airdrop claims, unlocked tranches and Launchpool or Alpha
   distributions become sellable within days. In the perp-first regime the perp lists before or with the
   token's first liquid market, and holders who cannot yet sell hedge by shorting it.
2. **Attention fades.** Day-0 buyers are momentum and lottery demand. The literature documents a run-up
   and first-day pop that reverse within about two weeks (scout §D).
3. **Why it could survive arbitrage.** The squeeze tail (MYX, ALPACA, XPL; scout §G) looks unpriced. A 1×
   isolated short bounds the loss at its margin, which turns the tail into a cost we can size and charge
   (§6).
4. **Why it might not.** Hedgers crowd the short side, so funding may already charge the decline away; T2
   isolates that. And a short book earns the alt market's fall, which is **not** an edge. The α test
   removes it (§7).

**Public priors and earlier looks (exposure).**

| look | overlap | what it computed or knew | exposure to 013 |
|---|---|---|---|
| Public literature (`000i`) | 2023–2026 listing calendars | post-listing declines (direction) | the direction is public; magnitudes inform §6 only as context |
| **Public market knowledge** (Revision 1, A-m6) | confirmation calendar | **the 2025-10-10 liquidation cascade** and the broad crypto decline from 2025-Q4 into 2026 | a raw short book is expected to look good there. **This is why the confirmation is decided on α, not on the raw net (B1)** |
| **The programme's "bear year"** (brief, established items 1 and 6; `000l`) | 2025-07-28 → 2026-06-30 = the confirmation calendar | 001–007 established the HL year as a bear year; `000l` requires that a confirm "not be attributable to a market-wide move already known to have paid in these months" | as above; α test |
| **The 2026-09-14 alpha-lab sweep** (`docs/alpha-lab-spec.md`) | 2024-03 → 2026-09 | Bitget, 83 perps, 4H bars: full-panel reversal, momentum and low-vol ICs. Surviving 2024–25 listings' early days are in the panel | aggregate ICs only; no listing-window outcome |
| 009 F1 (closed, line 7) | 2021-12 → 2023-12 | funding carry, coins listed ≥ 30 days | none on listing windows |
| 009 holdout A | 2024-01 → 2025-07-27 = **013 discovery** | never read (D17 terminal) | none |
| 010 discovery (closed, line 9) | 2023-06 → 2024-12 | cross-venue carry, coins listed ≥ 7 days | possible overlap with days 7–9 of some 2024 holds; aggregates only |
| 012 Stage 1 (closed, line 12) | 2025-01-15 → 2026-06-30 | HL–CEX carry, coins listed ≥ 7 days | the same marginal overlap with days 7–9; aggregates only |
| N3 scout | all calendars | listing-day levels, coverage counts, non-event dispersion | levels at listing |
| **013 draft (`000n`)** | all three calendars | hold-window coverage totals: funding rows 8,441 / 15,578 / 9,551; zero-trade rows; internal missing minutes | weak aggregate exposure about capped-funding episodes. **No parameter changed because of them** (`000n`) |
| 013 Revision 1 (§13) | all calendars | pre-entry coverage; continuation and segment names | no outcome |
| **013 later segments (Revision 1; review r1 m6)** | discovery, confirmation | the later-segment list implies that **AIAUSDT** (confirmation) and **PUMPUSDT** (discovery) traded again at or after d0 + 30 days, i.e. "survived their windows" | negligible; ledgered (§13); nothing decides on it |
| **Second-round reviewer's public notes** (review r1) | discovery, confirmation | ASTER's TGE and post-TGE rally (2025-09); the TVK → VANRY swap (2023-12); the RBN → AEVO migration (2024-03); pump.fun PUMP's post-launch decline (2025-07); the PumpBTC symbol change | public knowledge, not computed. ASTER's rally is adverse to a short; ASTER returns to the events by the single rule (M3), so the rule removes no adverse event |

Our computations have never touched these windows' outcomes. Every calendar is fresh to the programme's
code and not fresh to public priors. Only data after 2026-07-01 is unseen by both, and it is sealed.

## 1. Trials registered here

| id | events | rule | mechanism reason |
|---|---|---|---|
| **T1** | every first-segment Binance USDⓈ-M USDT perp listing whose token, and any token it was renamed or swapped from, had **no Binance USDⓈ-M perp before d0**. Out: pre-market perps, continuations (§2.5), later trading segments, censored and excluded symbols. Entered events only | short at the open of the first traded 1m bar at first trade + 24 h, 1× isolated, exit at the first traded bar at entry + 7 d (§4) | the hypothesis |
| **T2** | T1, restricted to events whose funding settled in (first trade, entry] sums to **≥ 0** | the same trade | Binance funding = premium + clamp(interest − premium, ±0.05%), with a positive interest floor (0.01%/8 h). "Σ ≥ 0" therefore means **the perp did not trade at a material discount to its index over its first day** (A-m1). A sustained discount means hedgers already crowd the short and charge the decline through funding. Known at entry |

- **T2's likely share is large.** Most new perps print the positive default rate unless the premium is
  materially negative, so T2 is probably a majority of T1. Counting it would read funding values, so
  it is not counted. §3 assumes 70%. T2 adds a test of the funding doubt at a cost: Bonferroni,
  t₆₄(0.975) = 2.00 against 1.67. That cost is accepted. T2 is never a variant chosen after the fact.
- **Family of 2.** One-sided α 2.5% per trial at discovery. Exactly one trial is carried to confirmation:
  T1 if it passes, else T2 if it passes, else close.
- **Considered and not registered:**
  - **perp-first only.** Corrected reading (B-M2): the spot-first flag sees only a Binance spot pair on
    d0−1. It is 0 of 148 in the confirmation calendar, but continuations of seasoned tokens and seasoned
    tokens receiving their first Binance perp are among those 148 (§2.5). The perp-first stratum is
    reported.
  - +1 h or 00:00 entries; a 14-day hold; pre-market perps as a trial; long-the-listing. Reasons as in
    the draft: the first-day pop, a delay that varies with the time of day, funding and squeeze exposure,
    no supply before TGE, no mechanism.
  - **Ticker reuse as events** (PUMPUSDT 2025-07-10, LITUSDT 2025-12-23). Later trading segments are
    never events (§2.5).
- **Family counts.** Binance listing windows: 2 trials. The HL year (16) is not touched.
- **Written expectation (Revision 2).**
  - About 1 in 4 that T1 or T2 passes discovery with a positive α.
  - Given a pass, about 3 in 4 that the tail-adjusted α existence test confirms on 121 fresh events (§3).
  - About **1 in 5 overall**, and less after the deploy bar, which now runs on min(raw, adjusted) with the
    tail charge.

## 2. Data, calendars, pull and coverage

### 2.1 Venue

**Binance USDⓈ-M** is the only free, complete, point-in-time listing set, and **the result is a Binance
result** (A-m2): Binance prices, Binance funding stamps, Binance shortability.
- **The paper and deploy venue is Binance** (D1, D12). The owner reports that Binance works for them;
  legality is a warning line only.
- **Bitget or HL execution would be a new trial.** Bitget had only 66 of 131 (2024) and 143 of 239 (2025)
  listings by day 1 (unknown for 65 and 96), has no free funding archive, and HL's funding regime differs.
- The 6 bp decision fee stays as a conservative cost (Binance's base is 5 bp, reported).

### 2.2 Calendars (fixed; D2)

| role | first trade in | data read strictly before | primary events / listing weeks (Revision 2) |
|---|---|---|---|
| reported block | 2020-01-02 → 2023-12-22 | 2024-01-01 | 251 / 117 |
| **discovery** | **2024-01-01 → 2025-07-18** | 2025-07-28 | **239 / 65** |
| **confirmation** | **2025-07-28 → 2026-06-21** | 2026-07-01 | **121 / 38** |
| embargo | 2023-12-23 → 12-31; 2025-07-19 → 07-27; 2026-06-22 → 06-30 | — | 8 listings, never events |

- No hold crosses a boundary (asserted as a refusal, tested), so no truncation is needed.
- Discovery reads nothing dated on or after 2025-07-28.

### 2.3 The pull (owner-approved, run 2026-10-12; unchanged since)

| source | files | bytes | checksums |
|---|---|---|---|
| `raw_klines1m/` (days d0 … d0+9, 660 events) | 6,595 | 342,588,668 | all verified; SHA256SUMS `562944c8…` |
| `raw_funding/` | 851 | 741,183 | all verified; `05094fc4…` |
| `raw_klines1h/` (2024-01 → 2026-06, every universe symbol) | 13,275 | 387,085,581 | all verified; `4884d513…` |

- **Total 0.73 GB.** No key dated on or after 2026-07-01 was requested.
- Listing responses that named later keys were dropped in memory.
- **The draft printed one sealed-period count** (B-m3): 900 universe-matching symbol directories against
  787 with a pre-cutoff file. Ledgered (§13).

### 2.4 Coverage (pre-entry only, [d0, first trade + 24 h); `000n`; Revision 2)

| count | reported | discovery | confirmation |
|---|---|---|---|
| listing events | 255 | 257 | 148 |
| pre-market | 0 | 4 | 22 |
| continuations (§2.5) | 4 | 14 | 5 |
| **primary events / listing weeks** | **251 / 117** | **239 / 65** | **121 / 38** |
| spot-first (Binance spot on d0−1) | 224 | 61 | 0 |
| no bar with trades / first trade not on d0 | 0 / 0 | 0 / 0 | 0 / 0 |
| events missing the d0 or d0+1 file | 0 | 0 | 0 |
| pre-entry 1m rows / of which zero-trade | 367,050 / 4,327 | 370,080 / 165 | 213,120 / 37 |
| events with no pre-entry funding row | 4 | 0 | 0 |
| events with a missing **pre-entry** funding-month file | 4 | 1 (JUPUSDT@2024-01-31) | 0 |
| market 1h rows / symbols | — | 4,698,652 / 508 | 4,824,747 / 646 |

- **Revision 2, review r1 m5.**
  - The coverage window ends at first trade + 24 h, the entry target; it no longer runs to + 25 h.
  - `events.json` stores only the **pre-entry** missing funding months. Hold-month gaps are derived
    inside the gated run from the names-only listing (`Panel.funding_months`).
- Nothing about the hold window is counted, stored or printed.
- **Rebuild:** MANIFEST `ebf29b7a…`, from the verified raw files, byte-identical on a second rebuild
  (§10).

### 2.5 Event set: continuations and later trading segments (Revision 2, decision 4; review r1 M3)

**One rule** (`L.CONTINUATIONS`; checked point in time by the build, which refuses otherwise). An event is
a **continuation**, out of T1 and T2 and reported as its own stratum, when either:
- **(p)** the token, or the token it was renamed, migrated, redenominated or swapped from, **had a Binance
  USDⓈ-M perp before d0**. The predecessor's first daily file in the 1d listing must precede d0;
- **(d)** the token is commodity- or fiat-backed.

**Mechanism.**
- Under (p), first-perp attention is absent and shorting was already possible.
- Under (d), the token's β is not the alt market's.
- The draft's rule (c) (rebrands **without** a Binance perp) is **withdrawn**. Such a token is a seasoned
  token receiving its first Binance perp, which stays in T1 like TON, AERGO, VANRY or AEVO. The 90-day
  clause of the draft's (a) is dropped as well.

| calendar | continuations (rule; predecessor perp) |
|---|---|
| discovery (14) | POL (p; MATIC), RENDER (p; RNDR), G (p; GAL), KAIA (p; KLAY), S (p; FTM), D (p; DAR), HEI (p; LIT), FORM (p; BNX), AWE (p; STPT), A (p; EOS), PUMPBTC (p; PUMPUSDT), RAYSOL (p; RAY), VIC (p; TOMO), PAXG (d) |
| confirmation (5) | SKY (p; MKR), FRAX (p; FXS), MANTRA (p; OM), NOM (p; OMNI), XAUT (d) |
| reported (4) | 1000BTTC (p; BTT), T (p; KEEP), 1000LUNC (p; LUNA), DODOX (p; DODO) |

- **Returned to the events by the single rule:** LUMIA, SYRUP (discovery); CFG, ASTER (confirmation);
  BEAMX, USTC (reported).
- **The renames themselves** (MATIC → POL, …, OMNI → NOM, PumpBTC's PUMPUSDT → PUMPBTCUSDT) are the
  drafter's public knowledge. **Every predecessor's perp predating d0 is checked mechanically** against
  the 1d listing (`continuation_failures`; tested on the real listing, names and dates only).
- The PUMPBTC rename matches the reviewer's recollection of a Binance symbol change and the drafter's 1d
  facts. The only remaining judgement is whether a symbol pair is the same token. No outcome informs it.

**Later trading segments** (relistings and ticker reuse; never events). A segment is a run of traded hours
after ≥ 7 days without trades, examined from an event symbol's d0 + 30 days on. Starts from 2024-01-01 to
2026-06-21:
- AERGO@2025-04-16, MAVIA@2025-03-26, CTK@2025-04-30, CVX@2025-07-23, SLP@2025-07-23: relistings;
- **PUMP@2025-07-10** and **LIT@2025-12-23**: ticker reuse by new tokens, excluded by rule. The reviewer
  finds this acceptable: pump.fun's PUMP fell publicly, so dropping it is conservative for the short;
- AIA@2026-01-20: a resumption.

Ledger note (review r1 m6): the list implies that the events AIAUSDT and PUMPUSDT traded at or after d0 +
30 days (§13).

## 3. Size, power and calibration (synthetic only; `listings013.simulate_worlds`; Revision 2)

`r013_run.py --power`: **55 worlds × 1,500 draws**. Logs:
- `data/r013/logs/power_r2.txt` (rates at the uncalibrated and the registered critical values, and the
  quantiles);
- `power_r2_records.json` (per-draw records);
- `power_r2_registered.json`.

The hashed runner reproduces them exactly (§10). The only real input is `schedule.json`: the real
first-trade times of the primary events (discovery 239, confirmation 121, reported 251, the last for the
pooled liquidation rate).

**The model** (as Revision 1, plus the review r1 worlds).
- Entries are at first trade + 24 h and exits 7 days later, so holds overlap.
- The alt market is continuous: 7-day SD 10.4%, with a ×1.5 stress.
- net = μ − β_i·mkt + k·8.3%·t₄.₅, with k idiosyncratic.
- **Regime worlds (B1):** events with a first trade in a window carry β_in, and the market falls 2–3% a
  week inside the window. The windows:
  - discovery 2024-12-09 → 2025-04-07;
  - confirmation 2025-10-06 → 2026-02-28 (aligned with quarters);
  - or 2025-11-15 → 2026-01-15 (misaligned).
- **Squeeze worlds (M2):** −1 atoms per event (p 1–2%) or per listing week (3%). The reported block's
  liquidations enter the pooled rate.
- The registered path end to end:
  - `trial_stats` (**quarter-β α**, adjusted robustness, min(raw, adjusted) economics, the
    **tail-adjusted α** at confirmation);
  - `discovery_criteria`, `decide_discovery`, `confirm`.

**Calibration (decision 1; re-derived).**

| stage | uncalibrated size, over the 27 calibration worlds (null β / drift / market / k, and regime) | nominal | decision |
|---|---|---|---|
| discovery (family-wise) | **0.007–0.053** (worst: regime β 2.5 / 1.0, misaligned) | 0.05 | within nominal + 0.01 → **not calibrated** (CAL_D = 0) |
| confirmation (tail-adjusted α) | **0.043–0.090** (worst: misaligned regime β 2.5 / 1.0) | 0.05 | above → **CAL_C = 2.05**, the largest 95th percentile of the α t over every calibration world and both trials (2.052; regime β 2.5 / 1.0, k 1, T2) |

The Revision 1 value of 1.90 is superseded. The quarter β and the added regime worlds raised the noise
of the α t.

**Size at the registered critical values.**

| world (μ = 0 unless stated) | discovery | **confirmation alone** | confirmation, untailed α (reported) | P(confirm \| carried) | joint |
|---|---|---|---|---|---|
| null: β 1 / 1.5 / 2, drift 0–2%/wk, k 1–2, market ×1–1.5 (20 worlds) | 0.006–0.047 | **0.018–0.037** | 0.018–0.039 | 0.000–0.065 (≤ 70 carries) | ≤ 0.0020 |
| **regime, aligned** (β 2.5/1.0 or 2.0/1.25; fall 2–3%/wk; k 1–1.5) | 0.027–0.053 | **0.029–0.039** | 0.029–0.039 | 0.000–0.122 (≤ 80 carries) | ≤ 0.0033 |
| **regime, misaligned** (2025-11-15 → 2026-01-15) | 0.035–0.053 | **0.043** | 0.043–0.045 | 0.019–0.037 | ≤ 0.0020 |
| **decayed edge** (μ 4% in discovery, 0 in confirmation; β 1–2; confirmation drift 0–2%) | 0.71–0.92 | 0.023–0.038 | 0.025–0.039 | **0.023–0.038** | 0.017–0.033 |
| **squeeze atoms** p 2% (β 1.5) / p 1% (k 1) / p 2% with a bear drift (β 2) | 0.03–0.07 | **0.021 / 0.023 / 0.017** | 0.079 / 0.091 / 0.066 | 0.03–0.07 | ≤ 0.0020 |
| squeeze weeks, 3% per week | 0.001 | 0.001 | 0.014 | — | 0.000 |

- **B1 is fixed.** The reviewer's aligned regime worlds gave a false confirm of 0.08–0.15 with a single
  β. With quarter β they give 0.029–0.039, and the misaligned window 0.043.
- **M2 is fixed.** Independent squeeze atoms gave an untailed confirmation of 0.066–0.091 at the
  registered critical value. The tail-adjusted α gives **0.017–0.023**.
- The joint path (a false discovery, then a false confirmation) stays at ≤ 0.0033 in every null world.

**Power at the registered critical values** (β 1.5, no drift, T2 70% of T1):

| μ per event | k | discovery | confirmation alone | **joint** |
|---|---|---|---|---|
| 2% | 1 / **1.5** / 2 | 0.50 / **0.41** / 0.30 | 0.70 / **0.43** / 0.26 | 0.34 / **0.17** / 0.08 |
| 4% | 1 / **1.5** / 2 | 0.81 / **0.80** / 0.77 | 1.00 / **0.91** / 0.73 | 0.81 / **0.72** / 0.55 |
| 6% | 1 / **1.5** / 2 | 0.96 / **0.96** / 0.95 | 1.00 / **0.99** / 0.95 | 0.96 / **0.95** / 0.90 |
| 8% | 1 / **1.5** / 2 | 1.00 / **1.00** / 0.99 | 1.00 / **1.00** / 1.00 | 1.00 / **1.00** / 0.98 |
| 4%, 1%/wk drift in both calendars | 1.5 | 0.92 | 0.91 | 0.83 |
| 4%, a regime (β 2.0/1.25, 3%/wk fall) | 1.5 | 0.93 | 0.92 | 0.87 |
| **4%, squeeze atoms p 2%** | 1.5 | 0.71 | **0.54** (untailed 0.60) | 0.39 |

- **The power costs of the Revision 2 changes**, at μ 4%, k 1.5, joint power:
  - quarter β plus CAL 2.05: 0.77 (Revision 1) → **0.72**;
  - **the tail adjustment costs nothing without liquidations** (untailed and tailed agree to 0.0003), and
    0.056 with p = 2% atoms (0.60 → 0.54), which is its purpose.
- **"α > 0 without the best week"** costs **0.000** in every world, at most 0.0006 (squeeze p 2%). That
  is within ≤ 0.03, so it is required at confirmation.
- **Not modelled:**
  - β varying within a quarter beyond the regime windows above;
  - intraday market paths;
  - the trimmed index's noise;
  - squeeze atoms correlated with the market.

  Statements hold for the modelled worlds only.

## 4. The trade (fixed; `ct/listings013.py`)

| parameter | value | why |
|---|---|---|
| event | the first archive file of a `^[A-Z0-9]+USDT$` symbol, keyed (symbol, d0). TradFi, index and non-COIN contracts are out. **Continuations and pre-market perps are reported strata. Later trading segments are never events (§2.5)** | the listing of a new token is the event |
| pre-market | no `indexPriceKlines` file on d0, from 2025 | no supply before TGE; own-price index; fixed funding |
| first trade | the first 1m bar with trades | — |
| **entry** | the open of the **first traded** 1m bar in [first trade + 24 h, + 60 min); none → `no_entry`, counted | past the first-day pop; T2's feature has printed |
| **hold** | **7 days**: exit at the open of the first traded bar in [target + 7 d, + 6 h) | the first half of the documented reversal window, at half the funding and squeeze exposure of 14 d |
| **exit fallbacks (B-M1, A-m9; Revision 2, r1 m4)** | **zero-trade bars are absent.** No traded bar in the 6 h window but one in [target + 6 h, target + 24 h): `exit_late`, at that bar (a halt or a data hole). **No trade within 24 h after the target:** `exit_last_trade`, at the last traded close before the target (a delisting or a long halt; settlement proxy). All counted before the decision; none dropped. Capping the late exit at 24 h keeps §6's bounds exact | survivorship; Binance archives keep flat dead rows |
| margin and liquidation | 1× isolated, no stop. Liquidation at the 1m last-price high, MMR 5%; the event returns −1 | conservative against Binance's mark-price rule and a 1–2.5% tier-1 MMR |
| funding | archived settlements on their own stamps, paid at the price at s. **Funding coverage rule (B-m1, B-m2, r1 m5):** if a month file spanned by (first trade, entry] is missing (stored pre-entry by the build), T2's feature is undefined: out of T2, counted (JUPUSDT@2024-01-31). If a hold month's file is missing (derived inside the gated run from the names-only listing), the event keeps its available settlements (`partial_funding`, counted). An event with no funding file at all (4 reported events) has zero funding (`no_funding_rows`, counted) | stated before any outcome |
| week (cluster) | ISO week of the first trade | — |

## 5. Costs

| item | decision | reported |
|---|---|---|
| taker fee | 6 bp per fill (conservative) | Binance 5 bp |
| **slippage (A-m3)** | by the **average hourly quote volume of the 24 h before the entry target** (known at entry): ≥ $50 M 5 bp · $10–50 M 8 bp · $2–10 M 15 bp · < $2 M 30 bp. **The exit fill is charged one tier worse** (8 / 15 / 30 / 50 bp), because day 8 is thinner than day 1 | ×2 (stressed) |
| funding | archived | stressed: pays ×2, receipts ×0.5, capped ±2% per settlement; slippage ×2 |

The tiers are assumptions, not measured impact (bookDepth was not pulled; §12).

## 6. Tail, sizing, Kelly, deploy rule and expected return

**The book** (`take`, `book`; D13, Revision 2).
- c = **2.5%** of capital per event, 1× isolated.
- Entries are taken in entry order while the **trailing 16 days hold < 7** book entries and the
  **trailing 40 days < 19**.
- P&L is booked at exit. Every entry is at or after its target, and every exit, **`exit_late` included**,
  is at most 7 d + 24 h after it (the late exit is capped; review r1 m4). So every exit of an ISO week
  comes from entries inside a 15-day window, and every exit of a calendar month from a 39-day window.
  Therefore **exits per week ≤ 7 and per month ≤ 19, for every exit status, carried-in positions
  included**.

**Tail at the proposed size (corrected against `book()`; A-M3).** The draft's −12% / −45% bounds were
wrong: they capped entries but booked exits, and ignored carried-in positions.

| tail | value | arithmetic |
|---|---|---|
| worst event | **−2.5%** | one liquidation |
| worst exit week (bound) | **−17.5%** | 7 exits × −100% × 2.5% |
| worst exit month (bound) | **−47.5%** | 19 × −100% × 2.5% |
| **worst month, plausible** | **−29.7%** | squeeze month: half of 19 exits liquidated, half −25%: 19 × 0.625 × 2.5% |
| at the draft's c = 3% and 4/15 calendar caps (for reference) | −24% week, −57% month bound, ≈ −36% plausible (reviewer A, `book()`) | — |

- Tested on an adversarial dense schedule through `book()`, with exits at 1 h, 3 d, 7 d and **7 d + 24 h**
  (the late cap) and every exit a liquidation: the bounds held. Review r1 found −32.5% / −50.0% with
  uncapped late exits under the Revision 1 windows; both are closed by the cap and the 16 / 40-day
  windows.
- **Reported at the run:** worst event, week and month, realised drawdown, and the **daily mark-to-market
  drawdown** from each open position's midnight marks (A-M3).
- Margin turnover from the calendars' first-trade times under the caps (counts only):
  - discovery: 156 of 239 events, 101 a year → 2.5× capital a year;
  - confirmation: 80 of 121, 89 a year → 2.2×.

**The deploy rule: one definition (A-M4, decisions 2–3; `listings013.deploy`; Revision 2, r1 M1).** It is
computed in the confirmation run, for the **carried trial** only, at the deployed size c\*. **It is
evaluated twice, on the raw net and on the market-adjusted net** (net − β̂_q·mkt with the confirmation's
quarter betas; raw where there is no market return):
- c\* is the smaller of the two;
- each bar must hold on both, so deploy iff both deploy;
- pooled worst months and liquidation rates stay raw, because they are the unhedged book's risk;
- both sides are reported.

This is conservative in a bear calendar, where the raw net carries the market's fall, and in a bull
calendar, where it carries the rise.
1. **c\*** = min(2.5%, ¼ Kelly of the **confirmation** book, 2.5% × (−30%) / W). W is the worst exit month
   at 2.5% over the pooled calendars (reported 2020–23, discovery, confirmation), base and stressed
   costs. The confirmation's Kelly is used because the discovery Kelly is selection-inflated.
2. **Peso-problem charges (A-M2).**
   - π = liquidations per event. μ_tail = μ_conf − max(0, π_pool − π_conf) × (1 + the mean net of the
     confirmation's non-liquidated events): the confirmation mean, re-weighted to the pooled liquidation
     rate.
   - A **squeeze month** is an exit month whose book loses ≥ half the plausible month (≥ 14.8% at 2.5%).
     f_eff = max(f_pool, 1 / Y_pool), so at least one squeeze month is assumed per pooled sample (6.4
     years): a floor of 0.156 a year.
   - The annual book at c\* is charged (μ_conf − μ_tail) × events per year × c\* plus max(0, f_eff −
     f_conf) × |plausible month at c\*|.
   - The floor alone costs 0.156 × 29.7% ≈ **4.6%/yr at c\* = 2.5%**, even with no squeeze in any
     calendar.
3. **Deploy iff** μ_tail ≥ 100 bp, the charged annual book at c\* ≥ 3%/yr, and c\* > 0. This is applied
   after a CONFIRM only.

**Kelly arithmetic** (assumed, not measured): σ_event 13.3% × k, within-week ρ ≈ 0.5, 4 events a week.
¼ Kelly ≥ 2.8% for any μ ≥ 2% and k ≤ 2, so 2.5% sits at or below ¼ Kelly wherever a pass is
plausible. The −30% month line binds first.

**Expected return at c = 2.5%** (assumed true net μ per event; margin turnover above; before and after the
minimum tail charge):

| μ per event | 2% | 4% | 6% | 8% | 10% |
|---|---|---|---|---|---|
| book, %/yr (2024–25 rate, 2.5×) | 5.0 | 10.1 | 15.1 | 20.2 | 25.2 |
| book, %/yr (2025–26 rate, 2.2×) | 4.4 | 8.9 | 13.3 | 17.8 | 22.2 |
| after the minimum tail charge (−4.6%/yr) | ≈ 0 | ≈ 4.3–5.5 | ≈ 8.7–10.5 | ≈ 13–16 | ≈ 18–21 |

**Against the goal.** 30%/yr or more needs μ ≥ about 12% net per event at this size. That is an edge
several times the literature's first-week magnitudes after funding. At the plausible 4–6%, this is a
5–11%/yr book with a −30% plausible month. **That is honestly below the mandate's landing zone**, and it
is stated now so that a pass is read for what it is: a priced, bounded short premium that would need a
larger size, more events, or a second venue (a new trial) to matter.

## 7. Pass rules

### 7.1 Discovery (both trials; recomputed by `recheck_discovery` from the row's per-event records)

**Market adjustment (Revision 2, r1 B1).** Over the events with a market return, fit the pooled OLS
net_i = α + Σ_q β_q·mkt_i·1[quarter(i) = q] + ε_i.
- **There is one β per calendar quarter of the first trade** and a common α, CR1 by listing week; the df
  stay G − 2.
- **Quarter groups (`beta_groups`):** a quarter with fewer than 10 events joins the previous one, and a
  short first group joins the next.
- The adjusted net is adj_i = net_i − β̂_q(i)·mkt_i, whose mean is α.
- **Why quarters:** a single pooled β leaves the residual (β_i − β̂)·mkt_i wherever β and the market's fall
  are concentrated in one window. That describes the confirmation calendar's 2025-Q4 cascade and the
  decline into 2026, and the intercept absorbs it (review r1: false confirm 0.08–0.15). Quarter β removes
  most of it (§3).
- Significance and robustness are decided on the adjusted series. The economic bars use min(raw,
  adjusted).

| # | criterion |
|---|---|
| 0 | ≥ 20 events and ≥ 10 listing weeks with a market return |
| **1** | **α > 0 and α's clustered t ≥ max(t_{G−2}(0.975), CAL_D = 0: not calibrated, the stage size holds)** (§3) |
| 2 | median adj > 0 |
| 3 | mean adj > 0 in both halves of the listing weeks |
| 4 | α > 0 refitted without the listing week with the largest positive adjusted sum |
| 5 | economic: **min(raw mean, adjusted mean)** ≥ 100 bp per event. The adjusted mean uses adj for events with a market return and raw otherwise |
| 6 | economic: **min(raw book, adjusted book)** ≥ 3%/yr on capital at c = 2.5% |

- **The raw t is reported, not a criterion.** In the draft, criterion 1 was a raw t. Under the reviewers'
  β worlds it both over-rejected (drift) and cost power (market variance in the denominator). The draft's
  criteria 2–4 move to the adjusted series for the same reason.
- **The β-only null** (a short book that earns only the alt market's fall) has α = 0 and is what
  criteria 1–4 test against.

- **The market index (B-M1, A-m5):** the 5%-each-tail trimmed equal-weight return of every universe perp
  that meets all of these:
  - its first daily file is ≥ 30 days before the entry day;
  - it is not the event's own symbol;
  - **it traded in the 24 h before the entry hour.**

  Each return runs from its last traded close before the entry hour to its last traded close before the
  exit hour. A perp that stops trading keeps its last traded price; a dead contract's flat rows never
  enter. At least 20 symbols are required, else the event has no market return (counted).
- **Why α (A-B1).** α removes the market variance from the denominator and is size-correct, after
  calibration, in every β / drift world simulated (§3). Raising power is a side effect.

**Decision:** T1 passes → carry T1; else T2 → carry T2; else close.

### 7.2 Confirmation (the carried trial; read once, after the approval line)

**The tail-adjusted α (Revision 2, r1 M2):**
- α_tail = α̂ − max(0, π_pool − π_conf)·(1 + the mean net of the confirmation's non-liquidated events);
- t_tail = α_tail / se(α̂), with the same SE.

Here π is liquidations per event of the carried trial, and **π_pool pools all three calendars**:
reported 2020–23, discovery and confirmation. The longest sample is used, as in §6; the reported block's
spot-first regime is stated as a weakness (§9). This is §6's peso correction moved inside the test. It is
zero whenever the confirmation already holds at least the pooled share of liquidations.

**CONFIRM iff** all of:
- α_tail > 0, with **t_tail ≥ max(t_{G−2}(0.95), CAL_C = 2.05)**;
- **α_tail > 0 refitted without the listing week with the largest positive adjusted sum** (power cost
  0.000–0.0006 in every world, within ≤ 0.03, so it is required);
- ≥ 10 events and ≥ 5 listing weeks.

This is an **existence test on the tail-adjusted, market-adjusted net**. The raw net and the untailed α
are reported.

**Deploy (after a CONFIRM only):** §6's rule.
- Deploy → `confirm_paper`: a registered forward paper period on **Binance**, the carried trial
  unchanged, at c\*.
- Otherwise `confirm_below_bar`: 013 closes.
- No CONFIRM → `not_confirmed`: "not demonstrated at this power", never "no edge".

## 8. Reported, decides nothing

- The raw net and its t; components; stressed and Binance-fee variants.
- **α summaries per trial:** the quarter-β fit with its betas, **the single-β α**, and **the split-β α**
  (net = a + β⁺·mkt⁺ + β⁻·mkt⁻; review r1 m8). If listings fall more in market falls than they rise in
  rises, the linear α absorbs that market-convexity return. The split α shows it next to the decision.
- By year; spot-first and perp-first; **the pre-market and continuation strata**; by slippage tier.
- **Counts before the decision:**
  - no first trade; no entry (primary and other separately);
  - liquidated; `exit_late`; `exit_last_trade`;
  - T2 undefined; partial funding; no funding rows; market missing;
  - pre-market and continuation, counted over all events.
- Books (base, stressed, per trial; raw and adjusted) with the daily MTM drawdown.
- **Placebos** (200 seeds each):
  - calendar-month sign flips of the raw gross;
  - **calendar-month sign flips of the adjusted series** (the decision's own scale; review r1 m9).
- The 2020–23 block, with its continuation stratum.
- **At confirmation:**
  - the untailed α and its t;
  - the pooled liquidation rate and the pooled counts;
  - both deploy sides (raw and adjusted).
- Per-event compact records in the row (id, week, quarter, entry and exit times, status, net, stressed
  net, flags, market return): the inputs of the recheck.

No reported block may become a primary.

## 9. Known weaknesses, stated now

- **Public priors and the bear year.** The direction is public, and the confirmation calendar is a known
  falling market. The quarter-β α removes regime residuals aligned with quarters. **A regime shorter than
  a quarter and misaligned with it leaves a small residual bias** (§3's misaligned worlds). Month β would
  cost about 0.10 power (review r1).
- **Market convexity.** A downside β above the upside β makes the linear α positive with no listing edge
  (review r1 m8: "confirm" 0.89 in an extreme case). It is reported, not removed. A pass with a split-β α
  near zero should be read as a convexity return, not a listing edge.
- **π_pool includes the 2020–23 spot-first block.** Its liquidation rate may differ from the perp-first
  regime's. The pooled rate over the longest sample is used by choice, stated now.
- **Continuations:** the renames are the drafter's public knowledge; the predecessor perps are checked
  mechanically.
- **Ticker reuse loses two genuine new tokens** (PUMP 2025-07, LIT 2025-12) by rule.
- **Seasoned tokens' first perps stay in T1** (TON, AERGO, VANRY, AEVO, LUMIA, ASTER …). Mechanism 1 is
  weaker for them.
- **The confirmation calendar is small:** 121 events, 38 weeks.
- **Costs are modelled.** The tiers are assumptions; the exit is charged one tier worse; the stressed
  variant doubles them.
- **Liquidation on last-price highs** overstates liquidations: conservative for the decision,
  pessimistic for the observed tail.
- **A late exit beyond 24 h is booked at the last traded close before the target.** A halt longer than a
  day after the target is therefore priced at the pre-halt price, not at the resumption.
- **The peso floor** (one squeeze month per pooled sample, 4.6%/yr at 2.5%) is a convention.
- **The calibration takes the largest 95th percentile over many worlds.** With Monte Carlo noise this
  over-corrects slightly and costs some power (review r1 m7). It cannot be gamed downward: adding worlds
  only raises it.
- Funding is paid at the 1m open at the settlement minute.
- Access is a warning only.

## 10. Built before any outcome is computed

- [x] `ct/listings013.py` (pure functions).
  - Rules: calendars; events; `CONTINUATIONS` with `continuation_failures` (point in time); pre-market;
    later segments.
  - The trade: `simulate` (traded bars; late exit capped at 24 h; daily marks); `slip_tiers`.
  - Estimation: `ols_alpha`, **`ols_cols` (general OLS, CR1), `beta_groups`, `alpha_fit` (quarter β)**;
    `alpha_crit` (CAL_D 0, **CAL_C 2.05**).
  - The book: `take` / `book` (16 / 40-day caps; MTM).
  - Decisions: **`deploy` (raw AND adjusted) / `deploy_one`**; `trial_stats` (quarter β, min(raw,
    adjusted), **tail-adjusted α**, split-β α); `discovery_criteria`; `recheck_discovery` (from records);
    `confirm` (tail-adjusted).
  - Placebos: `placebo`, **`placebo_alpha`**.
  - Simulation: `sim_events` (**regimes**), `simulate_worlds` (pooled liquidations), `world_rates`.
- [x] `r013_fetch.py`: unchanged since Revision 1.
- [x] `r013_build.py`: the continuation check (refuses on failure); pre-entry window [d0, first trade +
  24 h); pre-entry funding months only.
- [x] `r013_run.py`.
  - The process starts with **`sys.pycache_prefix` set to a fresh temp directory before `ct` is imported**.
  - The guard runs status, (a)–(e), (h), (k), (w), (v), (f) and (g1)–(i). **(v) now includes r008's
    `shadow_modules` and `loaded_from_source`.**
  - Every history, diff and show call uses **`--text --no-textconv --no-ext-diff --no-color`**.
  - **`record()`:** append, commit through the hook, then refuse unless the line is in `HEAD:results.jsonl`.
  - **`real_tree_failures()`:** the real results file and the real data only together, only with the
    default hook, and only with the real guard.
  - **`env_fingerprint()`** goes in every started row and is compared at confirmation.
  - The gated `Panel` (with the names-only funding-month listing); `evaluate`; `--discovery`,
    `--confirmation` (pooled π, tail-adjusted α, two-sided deploy), `--power`. It never pushes.
- [x] `tests/test_r013.py`: **145 checks pass**, before and after a (throwaway) commit, at DRAFT and at
  REGISTERED (below).
  - **New in Revision 2:**
    - the general OLS against the one-regressor fit;
    - the quarter grouping rule;
    - **a β-regime world in which the single-β α is biased and the quarter-β α is not**;
    - the late-exit cap (12 h → `exit_late`; 30 h → `exit_last_trade` at the pre-target close);
    - the adversarial book bounds with late exits;
    - the continuation rule's form and `continuation_failures`;
    - **the tail-adjusted α** (zero charge when π_pool ≤ π, the exact charge otherwise, the same SE);
    - **deploy on min(raw, adjusted)**: a bear calendar whose raw +5% is all β does not deploy;
    - each criterion fails alone, with criteria 5–6 on min(raw, adjusted);
    - the α placebo; regime events in the simulator;
    - **the end-to-end runs commit through the DEFAULT hook in a temp git repo, and every row is verified
      in HEAD**;
    - **a mocked no-op hook makes `record()` refuse**;
    - **a mocked hook, a stubbed guard or a mixed tree on the real files is refused**;
    - (11) a `-diff` attribute and a textconv driver do not hide an erased row;
    - (12) `diff.external` cannot blank the (r) diff;
    - **(13) `shadow_modules`** catches a top-level `email/` package, a sourceless `ipaddress.pyc`, an
      ignored `quopri.py` and `ct/listings013<EXT_SUFFIX>`;
    - **(14) a forged `__pycache__` `.pyc` loads under plain Python and not under a fresh
      `pycache_prefix`**;
    - (15) the runner loads `ct` from source;
    - (16) the environment fingerprint;
    - the real-file checks no longer assert commit state (m1);
    - every continuation is point in time on the real listing.
- **Commit-state simulation** (decision 7; `scratchpad/commit_sim.sh`). A `git clone --local` into the
  scratchpad, with its own `.git`, so the real repository is untouched. 013's current files are copied in
  and committed, and the suite runs at DRAFT and again after the status line is changed to REGISTERED
  and committed. Result:
  - **145 of 145 pass when committed at DRAFT, and 145 of 145 when committed at REGISTERED.**
  - The real guard, called in the REGISTERED clone with all its checks, returns **no refusal**. No outcome
    mode was run there; the clone was discarded.
- **Other suites:** unchanged by 013. The pre-existing stale state test is `test_carry012`'s "status line
  says DRAFT".

**Procedure.**
1. Two reviews, then REGISTERED (the orchestrator), committed with the code. Every top-level `*.py` and
   `ct/` file must be tracked and unmodified first, or (v) refuses.
2. `.venv/bin/python -I r013_run.py --discovery`: the guard; `discovery_started` (committed and
   verified in HEAD); T1 and T2; one row (committed and verified); on a crash, `discovery_failed`.
3. On `read_confirmation`, recomputed from the row's records: commit `Confirmation: APPROVED <date> row
   <sha256>` after the row.
4. `.venv/bin/python -I r013_run.py --confirmation`: the same pattern.
5. The runner never pushes; the owner pushes.

**Rebuild:** `r013_build.py` from the verified raw files is byte-identical (MANIFEST `ebf29b7a…`).

**SHA-256 of the code and data the outcome depends on.** `ct/factors.py` and `ct/stats.py` are 009's
frozen modules, imported read-only.

| file | sha256 |
|---|---|
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/listings013.py` | `10e262816e9d5cee760529b1bd790a3d7a4c597f31308299edc575b6896fbbfa` |
| `ct/factors.py` | `a1e70b9a5a2c1b58f43d2b08a2eaeac5ce5a3fd45868a659d745a3354bd61197` |
| `ct/stats.py` | `bfcc85f285dc7af28f364e2d77e0fbe04b6c2367724f4b822b7fbf6ff435d472` |
| `r013_run.py` | `317d75a850e311a911a3dad3ad49b8e3eebe36de7591266ad97135f41fd5dc6e` |
| `r013_build.py` | `41cb8b70e47b3254fdd665d96f70488975bd710fec6f0f72b4c46b3b40256054` |
| `r013_fetch.py` | `6de5e028e7786858ef5519431834ce4771c1f924097a071aba0055cbe4c9fc1b` |
| `data/r013/panel_reported/events.json` | `15688e4b00e8ac2ce51244df80f04af4787cdba1e6c0536ab3313502f36e475c` |
| `data/r013/panel_reported/symbols.json` | `1dd416ad495ad589b09269b1028cc47ddde166d04b7c7a15a699e61cf1a4ef6a` |
| `data/r013/panel_reported/bars_1m.parquet` | `683a93a75dd25ef88132a7d314fc31858eb94fe08117f9b6ef2c7ef3603341bc` |
| `data/r013/panel_reported/funding.parquet` | `801e425392c9f217c62bd35b5bd0b21f1af533b6fad43b9f029ba4201fda0d64` |
| `data/r013/panel_reported/coverage.json` | `a9604405b684268a86c4e1205be6cca408f7a386f3ff81f92ce4074926caba7c` |
| `data/r013/panel_discovery/events.json` | `45bef2236e9da5963c8e1b3e82a250a70ea47493681ab308770a787371307f06` |
| `data/r013/panel_discovery/symbols.json` | `1dd416ad495ad589b09269b1028cc47ddde166d04b7c7a15a699e61cf1a4ef6a` |
| `data/r013/panel_discovery/bars_1m.parquet` | `23b6b47f35e88d45b39d1d002ccc0e66f9d579aa9f5c21d0c5ecc4e8f787b1a5` |
| `data/r013/panel_discovery/funding.parquet` | `0e664eb57782d51d662790a9acad7ade1465b3c585ea2d0f870f8065c1eb517e` |
| `data/r013/panel_discovery/market_1h.parquet` | `23ec4f14da13ff29df26ec83cfdb007c4c25452bdd1473d588421836ccf83993` |
| `data/r013/panel_discovery/coverage.json` | `de9e834a865a3530fcb9648dab05ad0e5e350904b6d2632adccac3f31029fedc` |
| `data/r013/panel_confirmation/events.json` | `e45045a042f35520578810cd74d599b7c196affb1e7fb4c4ac8e6551adde2545` |
| `data/r013/panel_confirmation/symbols.json` | `1dd416ad495ad589b09269b1028cc47ddde166d04b7c7a15a699e61cf1a4ef6a` |
| `data/r013/panel_confirmation/bars_1m.parquet` | `c80601c39fd5cc1286bd610d722b867deab1c56936f9582bb36d161e7964a077` |
| `data/r013/panel_confirmation/funding.parquet` | `0597f4f1c9c33746fc1d01624405f7129a33a225f1302116f08a354842503b34` |
| `data/r013/panel_confirmation/market_1h.parquet` | `5f230211f9a38f27222dc597ecca6702f91a48d69e5dcec795906c16f97f44a3` |
| `data/r013/panel_confirmation/coverage.json` | `6fd018740d4318d0cd2b7f08a0aa6cf5ffddbc046b44b4fc39699bef096e4af1` |
| `data/r013/MANIFEST.json` | `ebf29b7a5b482e01871a6d6d95940978a7477c0efb85ec5350fbf1917ce154dd` |
| `data/r013/schedule.json` | `c9effd7a7ff0e07f635c7ed91fc1b060d7acad6e8c0623881dcf9b773fcefd31` |
| `data/r013/later_segments.json` | `7cb8db8ca0cbadb88338624385e067b24ca7624ff985785e28ea97e276084154` |
| `data/r013/listing/um_daily_1d_days.json` | `70e3332b592c6ca35eb21f098510a180b9ab5c279da023caec14221e1a983d71` |
| `data/r013/listing/event_keys.json` | `1235a50518c8efe8682fac0e1dca2a72cad2ee0f2fd0544cc72e319e922fa044` |
| `data/r013/inputs/fapi_exchangeInfo.json` | `a7f720f5ece63fe9ab087cbdd9fecc66d18645f113896b81ec171c89636ed39f` |
| `data/r013/raw_klines1m/SHA256SUMS` | `562944c83f84f67b11527aa829f9bf76336245c5615e71c27bade908c2247fa4` |
| `data/r013/raw_funding/SHA256SUMS` | `05094fc4cbf4ff0503faaa84bb7e5cc0fe4e3bbef9d79bf4a701bc48f7a17130` |
| `data/r013/raw_klines1h/SHA256SUMS` | `4884d513be612e413a9f589ce0dd69e0360d030c559ebf0c27abc91c18ed4a95` |

## 11. Owner decisions — PROPOSED (Revision 2)

| # | item | PROPOSED |
|---|---|---|
| D1 | venue and data | Binance USDⓈ-M; **the result is a Binance result; paper and deploy on Binance; another venue is a new trial**. The pull of §2.3 |
| D2 | calendars | as §2.2 |
| D3 | events | first files of universe symbols; continuations by the **single rule** (the token or its predecessor had a Binance USDⓈ-M perp before d0, checked point in time; or gold-backed; §2.5), pre-market perps and later trading segments out of the trials and reported. No first trade or no entry: counted |
| D4 | trade | §4, with the dead-row fallbacks and `exit_late` capped at 24 h |
| D5 | margin and tail | 1× isolated, no stop, last-price-high liquidation at MMR 5% |
| D6 | funding | archived; the coverage rule of §4 |
| D7 | costs | §5 (exit one tier worse) |
| D8 | trials | T1, T2; family 2; one carried |
| D9 | discovery | §7.1: α with **quarter-specific β**; criterion 1 on α at max(t_{G−2}(0.975), CAL_D = 0: not calibrated, the stage size holds); criteria 2–4 on the adjusted net; criteria 5–6 on **min(raw, adjusted)** |
| D10 | discovery decision | carry T1 > T2 > close. Close, or `discovery_failed`, is terminal |
| D11 | confirmation | after `Confirmation: APPROVED <date> row <sha256>` committed after the row. **CONFIRM iff the tail-adjusted α (quarter β; π_pool over all three calendars) > 0, its t ≥ max(t_{G−2}(0.95), CAL_C = 2.05), and it is > 0 without the best listing week**, ≥ 10 events, ≥ 5 weeks. Read once; `confirmation_failed` is terminal |
| D12 | deploy | §6's rule (one definition, carried trial, at c\*, with the pooled peso charges), **evaluated on raw and on adjusted net; deploy only if both deploy, at the smaller c\***; paper on Binance |
| D13 | sizing | c 2.5%; ≤ 7 entries per 16 days, ≤ 19 per 40 days. Tail, exact for every exit status: event −2.5%, week −17.5% (bound), month −47.5% (bound) / −29.7% (plausible) |
| D14 | reported | §8 |
| D15 | placebo | month-block flips of the raw gross and of the adjusted series, 200 seeds each; reported |
| D16 | family | Binance listing windows: 2 trials |
| D17 | ordering and lock | (a) discovery only after 009's recomputed terminal state. (b) **No other registration or scout computes outcomes on Binance listing windows (days 0–9 after a perp's first trade) of the confirmation calendar until 013 reaches a terminal state, or until 2027-06-30, whichever comes first.** The terminal states are: a discovery `close`; a `discovery_failed` row; a `confirmation` or `confirmation_failed` row; an owner line `Confirmation: DECLINED <date>`; or a committed `*_started` row with no result row 7 days later (a hard crash). The orchestrator puts the lock in the programme document. (c) 013 reads only `data/r013/` |

## 12. Open questions

1. **The renames behind the continuation list** (§2.5) are public knowledge; the predecessor perps are
   checked mechanically. PUMPBTC's rename and DODOX's identity with DODO are the two a reviewer may
   want to confirm against announcements dated before d0.
2. **The peso floor** (one squeeze month per pooled sample) is a convention. The orchestrator may prefer
   one a year: 18%/yr at 2.5%, which would make deployment nearly impossible.
3. **Slippage** remains unmeasured; a bookDepth pull (about 3 GB) would measure it without outcomes.
4. **The two ticker-reuse tokens** could be added as events with a per-segment new-token judgement and a
   small 1m pull. Not proposed.
5. **The environment pins** (python 3.14.8, duckdb 1.5.6, pyarrow 25.0.1) mean an upgrade before the run
   is a change here.

## 13. Disclosures and real-data reads (for the ledger)

All end-bounded before 2026-07-01, or names-only listings filtered in memory. No outcome was computed.

**Draft (2026-10-12)**, as listed in the draft and now ledgered:
- the S3 listings;
- the probe of index, premium, mark and kline first-file dates for XPL, WLFI and KAITO;
- the scout's derived files (15 named symbols' listing-hour levels);
- header lines and the **ALPHAUSDT-1m-2020-11-23 first-bar row**;
- the pull; results.jsonl metadata;
- exchangeInfo type counts;
- the listing-week structure.

**Corrections, for the ledger:**
- **B-m3:** the draft printed "900 universe-matching symbol directories, 787 with a pre-cutoff 1d file".
  The difference (113) is essentially the number of USDT symbols first listed from 2026-07-01 on, a count
  of the sealed period. §2.3 and §13 of the draft wrongly said such counts were never made.
- **`000n`:** the draft's build printed hold-window coverage totals for all three calendars (funding rows
  8,441 / 15,578 / 9,551; zero-trade rows 42,236 / 3,068 / 303; internal missing minutes 1 / 660, 0, 1 /
  1,440). They are outcome-adjacent aggregate exposure about capped-funding episodes. Per `000n`, no 013
  parameter changed because of them.

**Revision 2 reads (2026-10-13):**
- `reviews/013-review-r1.md`, and the guard half of `r008_run.py` (to port `shadow_modules`,
  `real_tree_failures` and `env_fingerprint`; read only, nothing of 008 touched).
- **The rebuild** printed the pre-entry counts of §2.4 (window [d0, first trade + 24 h)). The build checked
  every continuation's predecessor first-file date against the 1d listing (names and dates only; all
  pass), and printed the same 8 later-segment names.
- **Book capacity** under the 16 / 40-day caps, from first-trade times: 156 / 80 / 219 events taken.
- **Synthetic simulations** (§3); the only real input is `schedule.json`.
- **Tests:** the real-file checks read the registration, the hashes, the parquet column names, the
  `events.json` field names, and the 1d listing's first-file dates (the continuation check).
- **Ledger note (review r1 m6):** the later-segment list implies that the events **AIAUSDT** (confirmation)
  and **PUMPUSDT** (discovery) traded at or after d0 + 30 days. That is aggregate survival knowledge of two
  events; nothing decides on it.
- Public knowledge cited by the reviewer, not computed: ASTER's 2025-09 TGE rally, TVK → VANRY, RBN →
  AEVO, pump.fun PUMP's post-launch decline, and the PumpBTC symbol change.

**Revision 1 reads (2026-10-13):**
- `reviews/013-review-a.md`, `reviews/013-review-b.md`, `000n`.
- Event names (symbol@d0) of all three calendars from `listing/event_keys.json`, printed in full, to
  apply the continuation rule.
- First and last 1d-file dates of 31 predecessor and candidate symbols from `listing/um_daily_1d_days.json`:
  MATIC, RNDR, KLAY, FTM, EOS, MKR, FXS, OM, TOMO, RAY, GAL, LIT, DAR, STPT, BNX, OMNI, CFG, PUMP, PUMPBTC,
  BTT, LUNA, KEEP, NU, DODO, and the absence of ORN, MPL, INST, APX, UST, MC and BEAM.
- The commit hash of `72124e2` (the guard's anchor).
- **The rebuild** (from verified raw files) printed the pre-entry counts of §2.4. It also printed the
  names and start dates of 8 later trading segments, computed from 1h trade counts of hours ≥ each event
  symbol's d0 + 30 days, 2024-01 → 2026-06.
- **Book capacity** from first-trade times under the rolling caps (counts only: 161 / 83 / 220 events
  taken).
- **Synthetic simulations** (§3), whose only real input is `schedule.json` (first-trade times, flags).
- **Tests:** the real-file checks read the registration, the hashes, parquet column names and the
  `events.json` field names (no values).

## R. Revision 1: every finding and its fix

File keys: L = `ct/listings013.py`, R = `r013_run.py`, B = `r013_build.py`, F = `r013_fetch.py`,
T = `tests/test_r013.py`. Line numbers are those of this revision.

| finding | change | where |
|---|---|---|
| **A-B1** raw-net confirmation in a known bear year; β = 1 | confirmation and every discovery significance or robustness criterion decided on the OLS intercept α and the adjusted net (β estimated, CR1 by listing week, max(t_{G−2}(q), CAL)); raw net only in the economic bars | L:413 `ols_alpha`, L:447 `alpha_crit`, L:589 `trial_stats`, L:615 `discovery_criteria`, L:693 `confirm`; §7 |
| **A-M1** overlapping holds: CR1 anti-conservative; placebo blind | size simulated with real first-trade times, overlapping holds, a continuous market (not scaled by k); confirmation critical value calibrated to 1.90; month-block placebo; the raw t decides nothing | L:767 `sim_events`, L:809 `simulate_worlds`, L:111 `CAL_ALPHA_CONFIRM`, L:712 `placebo`; §3 |
| **A-M2** peso problem | deploy rule charges the pooled liquidation rate and the plausible squeeze month (floor one per pooled sample) | L:536 `deploy`; §6 |
| **A-M3** tail bounds wrong against `book()` | c 2.5%; rolling caps (7 per 15 d, 19 per 39 d) make exits per week/month exact; corrected table; daily MTM drawdown | L:88 constants, L:475 `take`, L:487 `book`, L:252 `simulate` (marks); T `book_checks`; §6 |
| **A-M4** deploy rule differs in text and code | one rule: carried trial, confirmation book and pooled worst months, the bar at c\* | L:536 `deploy`, R:532 `_confirmation_row`; §6, D12 |
| A-m1 T2 on the interest floor | mechanism restated in premium terms; likely share (majority) stated; power at 70% | §1; R:564 `WORLDS` |
| A-m2 venue | Binance result; paper and deploy on Binance; another venue is a new trial | §2.1, D1, D12 |
| A-m3 slippage keyed on the busiest hour | tier by the average hourly volume of the 24 h before entry; exit one tier worse | L:242 `slip_tiers`, L:252 |
| A-m4 simulator book week cap only | simulations go through `trial_stats` → `book` (both caps) | L:809 |
| A-m5 ticker reuse; index jumps | trimmed-mean index; later trading segments; continuation rule | L:333, L:189, L:134; R:337 `Panel.market`; §2.5 |
| A-m6 exposure | the bear year, the 2026-09-14 sweep, `000l`, the 2025-10-10 cascade and the 2025–26 decline | §0 |
| A-m7 text inconsistencies | rewritten; the drift sign stated in `_market_path` | §3, §7, L:749 |
| A-m8 D17(b) deadline | lifts at 013's terminal state or 2027-06-30 | D17 |
| A-m9 internal data holes | `exit_late` at the first traded bar after the hole, counted | L:252 `simulate` |
| A-m10 expectations | restated against §3 | §1 |
| **B-M1** dead rows | `n` and `c` stored in `market_1h`; the index admits only symbols that traded in the 24 h before entry, at last traded prices; exits ignore zero-trade bars after the last trade; relistings from trade counts | B:114, B:176; R:337; L:264; L:189 |
| **B-M2** event set | continuation rule and list (a–d), reported stratum; "0 of 148 spot-first" corrected | L:134; §2.5, §1 |
| **B-M3** erasable rows | `log --all --reflog -m -p`; (k) results.jsonl clean; the runner commits results.jsonl alone after each started, failed and result row; `*_failed` rows terminal | R:95, R:210, R:290, R:454; T `bypass_checks` (1)–(4) |
| **B-M4** ungated read path | a gate token set only by the real `guard()`; `Panel` raises on the real data without it; refusals, not asserts | R:271, R:306; T `panel_checks` |
| **B-M5** window-level coverage (`000n`) | per-event coverage pre-entry only; window totals removed; panels rebuilt; `events.json` read only through the gated `Panel`; `--counts` removed; the simulation reads `schedule.json` (pre-entry) | B:156, B:243; §2.4, §13 |
| B-m1 JUPUSDT partial funding | a missing month in (first trade, entry] makes T2 undefined; one in the hold is `partial_funding`; both counted | R:358 `evaluate`; §4 |
| B-m2 4 reported events without funding | zero funding, `no_funding_rows`, counted | §4, R:358 |
| B-m3 a sealed-period count printed | ledgered | §13 |
| B-m4 asserts as guards | `refuse_unless` / `Refused` throughout; a static test that no `assert` remains | L:40, F:75, B, R; T `panel_checks` |
| B-m5 nested git / GIT_DIR | GIT_* stripped; top level required; anchor `72124e2` must be an ancestor | R:86, R:212, R:52; T `bypass_checks` (5)–(7) |
| B-m6 shadowed modules, `-O` | untracked `*.py` at the top level and in `ct/` refused; duckdb and pyarrow from the interpreter prefix; `-O` refused | R:166; T (8)–(9) |
| B-m7 D17(b) prose; crash | terminal states defined (failed rows; a started row with no result after 7 days); the deadline; the programme line (orchestrator) | D17; R:454 |
| B-m8 (g4) from aggregates | `recheck_discovery` recomputes every statistic from the per-event records (entry and exit times added) | L:657, R:448 |
| B-m9 counts | pre-market and continuation counted over all events; `no_entry` split | R:358 |
| B-m10 (f) keyed on the file name | any registration starting `013-` | R:91 |
| B-m11 environment | pins checked by (v); versions in the started rows and compared at confirmation | R:53, R:239 |
| Orch. 1 | as A-B1, A-M1, B-M1. "α > 0 without the best week": power cost 0.000 (§3) → required at confirmation | §3, §7.2, L:693 |
| Orch. 2–3 | as A-M2–M4 | §6 |
| Orch. 4 | as A-m2 | §2.1 |
| Orch. 5–6 | as B-M2, B-M1; PUMPUSDT reuse resolved by rule | §2.5 |
| Orch. 7–8 | as B-M3, B-M4 | R |
| Orch. 9 | as B-M5 | §2.4, §13 |
| Orch. 10 | D17(b) | §11 |
| Orch. 11 | as A-m6 | §0 |
| Orch. 12 | as B-m1, B-m2, A-m3, A-m1, B-m11, B-m10, B-m8 | above |

## R2. Revision 2: every second-round finding and its fix (`reviews/013-review-r1.md`)

File keys as in §R. Line numbers are those of this revision.

| finding | change | where |
|---|---|---|
| **B1** β varying with the regime (false confirm 0.08–0.15) | α with **one β per calendar quarter of the first trade** (common α, CR1 by listing week, df G − 2); groups of < 10 events merged; the same model for discovery, robustness (adj) and confirmation; regime worlds (aligned, misaligned; discovery and confirmation windows) added to the calibration set; critical values re-derived: CAL_D 0, **CAL_C 2.05**. Aligned regimes now 0.029–0.039, misaligned 0.043 | L:484 `ols_cols`, L:523 `beta_groups`, L:541 `alpha_fit`, L:718 `trial_stats`, L:116; R:690 `_regime`, R:695 `WORLDS`; L:940 `sim_events` (regime); §3, §7 |
| **M1** economics and deploy on raw net | discovery criteria 5–6 on **min(raw, adjusted)**; `deploy` evaluated on raw AND adjusted, c\* the smaller, deploy iff both; both reported | L:769, L:718, L:694 `deploy`, L:642 `deploy_one`; R:650; §6, §7 |
| **M2** squeeze atoms over-reject the confirmation (0.077–0.105) | **tail-adjusted α** inside the confirmation: α − max(0, π_pool − π)·(1 + mean non-liquidated net), same SE; π_pool over all three calendars (stated). Squeeze worlds now 0.017–0.023; no cost without liquidations | L:718, L:848 `confirm`; R:650; L:997 (pooled liquidations); §3, §7.2 |
| **M3** continuation rule (c) ad hoc | **one rule**: the token or its predecessor had a Binance USDⓈ-M perp before d0 (checked point in time; the build refuses otherwise), plus (d) gold. LUMIA, SYRUP, CFG, ASTER, BEAMX, USTC return. **Discovery 239, confirmation 121, reported 251** | L:138, L:160; B:231; §2.5 |
| **M4** a mocked hook reopens run-once | `record()` refuses unless the line is in `HEAD:results.jsonl` (real git); `real_tree_failures()` refuses a non-default hook, a stubbed guard, or a mixed real/synthetic tree | R:387, R:373, R:360; T:683 |
| m1 stale test | the real-file checks accept DRAFT or REGISTERED and assert no commit state; verified by a throwaway commit simulation | T:947; §10 |
| m2 textconv / ext-diff | `--text --no-textconv --no-ext-diff --no-color` on every history, diff and show call | R:104 and the guard; T:896 (11)–(12) |
| m3 shadowing, forged `.pyc` | r008's `shadow_modules`; a fresh `sys.pycache_prefix` before `ct` is imported; `loaded_from_source` | R:43, R:191, R:219; T (13)–(15) |
| m4 `exit_late` breaks the bounds | the late exit is capped at target + 24 h (then the pre-target last traded close); windows 16 / 40 days; bounds exact for every status (adversarial test) | L:74, L:300, L:91; §6 |
| m5 coverage residues | pre-entry funding months only; window ends at first trade + 24 h; hold-month gaps derived inside the gate | L:67; B:156; R:424, R:458 |
| m6 later-segment leak (AIA, PUMP) | ledgered | §13 |
| m7 calibration conservative but incomplete | regime worlds added; the rule (largest 95th percentile) kept; over-correction stated | §3, §9 |
| m8 split-β α | reported in `trial_stats` and the α summaries | L:718; §8 |
| m9 placebo on the raw net | the α placebo (month flips of the adjusted series) reported | L:892; §8 |
| m10 environment parity; push | `env_fingerprint` hashes every duckdb and pyarrow file, stored in the started rows and compared at confirmation; the runner never pushes | R:171, R:306, R:360 |
| m11 ticker reuse | accepted as is (conservative for the short) | §2.5 |
| Orch. 1–7 | as B1, M1, M2, M3, M4, the minors, and the commit-state simulation | above; §10 |
