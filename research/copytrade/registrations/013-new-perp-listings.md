# 013 — Short new Binance USDⓈ-M perps at day 1, 1× isolated, 7 days (N3 i: new perp listings)

**Status: DRAFT**

**Revision 1 (2026-10-13), after two independent reviews and the orchestrator's decisions.**
Reviews: `registrations/reviews/013-review-a.md` (1 blocker, 4 majors) and `reviews/013-review-b.md` (5
majors). Ledger entry `000n` records the outcome-adjacent coverage totals of the draft. Every finding and
its fix is listed in §R at the end. The main changes:

- **The market-adjusted decision (A-B1, A-M1, B-M1).** Discovery's significance criterion (now
  criterion 1; the draft's criterion 5) and the confirmation are decided on the **intercept α** of a pooled OLS of each event's net on its own market return over the
  same hold. β is estimated, the regression is listing-week clustered (CR1), and the critical value is
  max(t_{G−2}(q), the calibrated value registered now, §3). The market index is a 5%-trimmed equal-weight
  return of seasoned perps that traded in the 24 h before entry, from last traded prices. **The discovery
  robustness criteria use the adjusted net (net − β̂·market). The raw net feeds the economic bars only.**
- **The tail (A-M2, A-M3).**
  - Rolling-window caps make the bounds exact: c = 2.5%, ≤ 7 entries per 15 days, ≤ 19 per 39 days.
  - Worst exit week −17.5% (bound); worst exit month −47.5% (bound) and −29.7% (plausible).
  - A daily mark-to-market drawdown is reported.
  - A peso-problem tail charge is computed from the pooled calendars.
- **One deploy rule** (A-M4), at the deployed size. **Binance is the venue**; any other venue is a new
  trial.
- **The event set** (B-M2).
  - Continuations (migrations, relistings under new tickers, rebrands, gold tokens) are out of the trials
    and reported as a stratum. Discovery 16, confirmation 7, reported 6.
  - Relistings come from trade counts, not from files (B-M1).
- **Delistings** (B-M1): zero-trade rows after a listing's last trade are treated as absent.
- **Run-once integrity and the read gate** (B-M3, B-M4).
  - Reflog, merge and stash scan; results.jsonl must be clean; the runner commits each row.
  - A gate token: the real data open only after the real guard has passed.
  - Refusals instead of asserts; an environment check.
- **Coverage is pre-entry only** (B-M5, `000n`). The panels were rebuilt from the verified raw files; no
  new pull.

**No outcome has been computed on any real listing window, in any calendar.** That covers price changes,
returns, drawdowns, funding sums, liquidation counts, P&L and "did it dump" statistics, and it includes
the reported 2020–23 block.
- The pull (§2) is done and every file is checksum-verified.
- The build counts only pre-entry coverage (§2.4).
- Mechanics, statistics, fetch, build, the read gate and the guard are tested on synthetic data (§10).
- The draft printed hold-window coverage totals; they are ledgered as `000n` (§13).

The runner (`r013_run.py`) refuses to compute anything until all of these hold:
- this file says REGISTERED, is committed and is unmodified;
- the code and data match §10's SHA-256 table;
- 009's terminal state is committed and recomputed (D17);
- `results.jsonl` is clean and holds no row of any `013-` registration, anywhere in git (reflog
  included);
- the environment matches the pins.

Programme: `docs/edge-discovery-next-prompt-2026-10-12.md` (round 3, light N3). Scout:
`scouts/n3-listings.md`. Ledger entries read: `000f`, `000g`, `000i`, `000l`, `000n`. Patterns: 009, 012,
and every review in `registrations/reviews/`.

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

Our computations have never touched these windows' outcomes. Every calendar is fresh to the programme's
code and not fresh to public priors. Only data after 2026-07-01 is unseen by both, and it is sealed.

## 1. Trials registered here

| id | events | rule | mechanism reason |
|---|---|---|---|
| **T1** | every first-segment Binance USDⓈ-M USDT perp listing of a **new token**. Out: pre-market perps, continuations (§2.5), later trading segments, censored and excluded symbols. Entered events only | short at the open of the first traded 1m bar at first trade + 24 h, 1× isolated, exit at the first traded bar at entry + 7 d (§4) | the hypothesis |
| **T2** | T1, restricted to events whose funding settled in (first trade, entry] sums to **≥ 0** | the same trade | Binance funding = premium + clamp(interest − premium, ±0.05%), with a positive interest floor (0.01%/8 h). "Σ ≥ 0" therefore means **the perp did not trade at a material discount to its index over its first day** (A-m1). A sustained discount means hedgers already crowd the short and charge the decline through funding. Known at entry |

- **T2's likely share is large.** Most new perps print the positive default rate unless the premium is
  materially negative, so T2 is probably a majority of T1. Counting it would read funding values, so
  it is not counted. §3 assumes 70%. T2 adds a test of the funding doubt at a cost: Bonferroni,
  t₆₄(0.975) = 2.00 against 1.67. That cost is accepted. T2 is never a variant chosen after the fact.
- **Family of 2.** One-sided α 2.5% per trial at discovery. Exactly one trial is carried to confirmation:
  T1 if it passes, else T2 if it passes, else close.
- **Considered and not registered:**
  - **perp-first only.** Corrected reading (B-M2): the spot-first flag sees only a Binance spot pair on
    d0−1. It is 0 of 148 in the confirmation calendar, but some of those listings were continuations of
    seasoned tokens, now excluded (§2.5). The perp-first stratum is reported.
  - +1 h or 00:00 entries; a 14-day hold; pre-market perps as a trial; long-the-listing. Reasons as in
    the draft: the first-day pop, a delay that varies with the time of day, funding and squeeze exposure,
    no supply before TGE, no mechanism.
  - **Ticker reuse as events** (PUMPUSDT 2025-07-10, LITUSDT 2025-12-23). Later trading segments are
    never events (§2.5).
- **Family counts.** Binance listing windows: 2 trials. The HL year (16) is not touched.
- **Written expectation (Revision 1).**
  - About 1 in 4 that T1 or T2 passes discovery with a positive α.
  - Given a pass, about 3 in 4 that the α existence test confirms on 119 fresh events: §3, k = 1.5, for
    an effect large enough to pass discovery.
  - About **1 in 5 overall**, and less after the deploy bar's tail charge.

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

| role | first trade in | data read strictly before | primary events / listing weeks (Revision 1) |
|---|---|---|---|
| reported block | 2020-01-02 → 2023-12-22 | 2024-01-01 | 249 / 117 |
| **discovery** | **2024-01-01 → 2025-07-18** | 2025-07-28 | **237 / 65** |
| **confirmation** | **2025-07-28 → 2026-06-21** | 2026-07-01 | **119 / 37** |
| embargo | 2023-12-23 → 12-31; 2025-07-19 → 07-27; 2026-06-22 → 06-30 | — | 8 listings, never events |

- No hold crosses a boundary (asserted as a refusal, tested), so no truncation is needed.
- Discovery reads nothing dated on or after 2025-07-28.

### 2.3 The pull (owner-approved, run 2026-10-12; unchanged in Revision 1)

| source | files | bytes | checksums |
|---|---|---|---|
| `raw_klines1m/` (days d0 … d0+9, 660 events) | 6,595 | 342,588,668 | all verified; SHA256SUMS `562944c8…` |
| `raw_funding/` | 851 | 741,183 | all verified; `05094fc4…` |
| `raw_klines1h/` (2024-01 → 2026-06, every universe symbol) | 13,275 | 387,085,581 | all verified; `4884d513…` |

- **Total 0.73 GB.** No key dated on or after 2026-07-01 was requested.
- Listing responses that named later keys were dropped in memory.
- **The draft printed one sealed-period count** (B-m3): 900 universe-matching symbol directories against
  787 with a pre-cutoff file. Ledgered (§13).

### 2.4 Coverage (Revision 1: pre-entry only, [d0, first trade + 25 h); `000n`)

| count | reported | discovery | confirmation |
|---|---|---|---|
| listing events | 255 | 257 | 148 |
| pre-market | 0 | 4 | 22 |
| continuations (§2.5) | 6 | 16 | 7 |
| **primary events / listing weeks** | 249 / 117 | **237 / 65** | **119 / 37** |
| spot-first (Binance spot on d0−1) | 224 | 61 | 0 |
| no bar with trades / first trade not on d0 | 0 / 0 | 0 / 0 | 0 / 0 |
| events missing the d0 or d0+1 file | 0 | 0 | 0 |
| pre-entry 1m rows / of which zero-trade | 382,290 / 4,469 | 385,500 / 166 | 222,000 / 37 |
| events with no pre-entry funding row | 4 | 0 | 0 |
| events with a missing funding-month file (a listing fact) | 4 | 1 (JUPUSDT@2024-01-31, month 2024-01) | 0 |
| market 1h rows / symbols | — | 4,698,652 / 508 | 4,824,747 / 646 |

- **Nothing about the hold window is counted, stored or printed** any more: no funding rows, zero-trade
  rows, missing minutes or end of trading. `events.json` holds pre-entry fields only (tested).
- `events.json` is read only through the gated `Panel`. The ungated `schedule.json` holds first-trade
  times and flags only; it is the simulation's input.
- The rebuild is from the verified raw files: MANIFEST `d4d51b12…`, byte-identical on a second rebuild.

### 2.5 Event set: continuations and later trading segments (Revision 1, decision 5; B-M2, B-M1, A-m5)

**Continuation rule** (fixed now, from mechanism and point-in-time public facts; `L.CONTINUATIONS`). An
event is out of T1 and T2, and reported as the continuation stratum, when its token is not new at
listing:
- **(a)** a ticker change, migration or redenomination of a token that had a Binance USDⓈ-M perp, with
  the new perp listed within 90 days of the change;
- **(b)** a relisting under a new ticker of a token whose Binance USDⓈ-M perp was delisted earlier;
- **(c)** a rebrand or token swap of a token without a Binance perp, completed ≤ 180 days before d0;
- **(d)** a commodity- or fiat-backed token.

Each change is announced before the new perp lists, so the category is known at entry. **Mechanism:** none
of these has a new-token supply overhang or a first-listing pop. Gold tokens carry a macro β the alt index
does not hedge.

| calendar | names (category) |
|---|---|
| discovery (16) | POL (a, MATIC), RENDER (a, RNDR), G (a, GAL), KAIA (a, KLAY), S (a, FTM), D (a, DAR), HEI (a, LIT), FORM (a, BNX), AWE (a, STPT), A (a, EOS), PUMPBTC (a, PUMPUSDT ticker move **?**), RAYSOL (b, RAY), VIC (b, TOMO), LUMIA (c, ORN), SYRUP (c, MPL), PAXG (d) |
| confirmation (7) | SKY (a, MKR), FRAX (a, FXS), MANTRA (a, OM), NOM (a, OMNI), CFG (c, migration **?**), ASTER (c, APX **?**), XAUT (d) |
| reported (6) | 1000BTTC (a), T (a, KEEP+NU), 1000LUNC (b), USTC (b), DODOX (b **?**), BEAMX (c, MC) |

- **"?"** marks names where the drafter's knowledge of the public notice or its date is uncertain. A
  reviewer should verify them against Binance and project announcements dated before d0. A verified
  correction is a change to this list before registration, never after.
- **Predecessor perps, from the 1d listing (dates only):**
  - MATIC to 2024-09-11; RNDR to 2024-07-30; GAL to 2024-07-30; EOS to 2025-05-21; TOMO to 2024-05-28;
  - KLAY, FTM, MKR, FXS, OM, RAY, LIT, DAR, STPT, BNX and OMNI have files to 2026-06-30. Dead contracts
    keep writing files (scout L4 §B), so file dates do not date the delisting;
  - ORN, MPL, INST and APX never had a Binance USDⓈ-M perp.
- **Not continuations, by the rule:**
  - seasoned tokens receiving their first Binance perp (for example TON, AERGO, DIA, CHESS, fan tokens),
    for which mechanism 2 applies. They stay in T1; the spot-first stratum is a partial proxy, reported;
  - rebrands older than 180 days (VIRTUAL, FLUID).

**Later trading segments (relistings, ticker reuse; B-M1, A-m5).**
- Dead contracts' archive files never stop, so file gaps cannot detect relistings. The draft's "0
  relistings" was that artefact.
- A **trading segment** is now a run of hours with trades (1h trade counts) after ≥ 7 days without
  trades. An event symbol is examined only from its d0 + 30 days on, so nothing about its own window is
  read.
- **Later segments are never events.** Segments starting 2024-01-01 → 2026-06-21 (names):
  - AERGOUSDT@2025-04-16, MAVIAUSDT@2025-03-26, CTKUSDT@2025-04-30, CVXUSDT@2025-07-23 and
    SLPUSDT@2025-07-23: relistings;
  - **PUMPUSDT@2025-07-10** (pump.fun's PUMP reusing the ticker) and **LITUSDT@2025-12-23** (Lighter's LIT
    reusing Litentry's ticker): ticker reuse, i.e. genuine new tokens that the rule excludes;
  - AIAUSDT@2026-01-20 (a resumption).
- **The cost is stated:** two new tokens are lost. Including them would need a judgement per segment and
  new 1m pulls. The mechanical rule was preferred.
- **PUMPUSDT ticker reuse resolved:** the event PUMPUSDT@2025-04-12 is the first segment, and its window
  (April) precedes the reuse by about 3 months. The July token is a later segment, not an event.
  PUMPBTCUSDT@2025-06-13 is a continuation (a).

## 3. Size, power and calibration (synthetic only; `listings013.simulate_worlds`)

`r013_run.py --power`: 44 worlds × 1,500 draws. Log: `data/r013/logs/power_r1.txt`; per-draw records in
`power_r1_records.json`; rates at the registered critical values in `power_r1_registered.json`. Nothing real
enters except `schedule.json`: the primary events' **real first-trade times**, 237 in discovery and 119 in
confirmation.

**The model (revised after review A's M1 and B1).**
- Each event enters at first trade + 24 h and exits 7 days later, so **holds overlap as in reality**.
- **The alt market is a continuous path**: a daily random walk, linear within the day. Its 7-day SD is
  10.4%, the scout's measured market part of non-event coin-weeks. A ×1.5 stress is included. A **drift**
  of 0 / 1 / 2% a week down is a bear market the short earns from.
- Event net = μ − β·mkt + k·8.3%·t₄.₅.
  - k scales only the idiosyncratic part (listing windows are more volatile than other coins).
  - **Correction during this revision:** an intermediate run scaled the market by k as well. The tables
    below use the corrected model.
- β = 1, 1.5 or 2. Optional squeeze atoms: −1 per event with p = 2%, or every event of a listing week
  with p = 3%.
- T2 is 70% of T1.
- **The registered path is applied end to end:** `trial_stats` (OLS α, CR1, the book with both caps),
  `discovery_criteria`, `decide_discovery`, `confirm`.

**Calibration (decision 1).**

| stage | statistic | simulated size at t_{G−2}(q), over the β / drift / market / k null worlds | nominal | decision |
|---|---|---|---|---|
| discovery (family-wise, both trials, all criteria) | α t ≥ t_{G−2}(0.975) | **0.008–0.042** | 0.05 | within nominal + 0.01 → **not calibrated** (CAL_D = 0) |
| confirmation (α existence test) | α t ≥ t_{G−2}(0.95) | **0.040–0.064** (0.064 at k 1, β 1.5, drift 1%) | 0.05 | above nominal + 0.01 → **calibrated: CAL_C = 1.90**, the largest 95th percentile of the α t over every null world and either trial (1.896, at k 1, β 1.5, drift 1%, T2) |

**Size at the registered critical values.**

| world (true μ = 0 unless stated) | discovery pass | confirmation alone | P(confirm \| carried) | joint |
|---|---|---|---|---|
| β 1 / 1.5 / 2, drift 0–2%/wk, k 1–2, market ×1–1.5 (20 worlds) | 0.008–0.042 | **0.027–0.043** | 0.000–0.070 (on ≤ 60 carries) | **≤ 0.0027** |
| **decayed edge**: μ 4% in discovery, 0 in confirmation; β 1 / 1.5 / 2; confirmation drift 0–2%/wk; k 1.5 | 0.72–0.92 | 0.028–0.038 | **0.029–0.041** | 0.021–0.034 |
| squeeze atoms, p = 2% per event (β 1.5, k 1.5) | 0.041 | **0.079** | 0.097 | 0.004 |
| squeeze weeks, 3% per week | 0.003 | 0.015 | — | 0.000 |

- **A-B1 is fixed.** The reviewer's decayed-edge world falsely confirmed 0.13–0.24 at drift 1–2%. Here it
  confirms **0.029–0.041 given a carry**.
- **Independent squeeze atoms still over-reject (0.079).** A 119-event sample often contains too few
  liquidations: the peso problem (A-M2). It is handled at the deploy bar, not here. The deploy rule
  re-weights the confirmation to the **pooled** liquidation rate (§6). In this world μ_tail ≈ 0, which
  fails the 100 bp bar. So a confirm can happen, but a deployment cannot.

**Power at the registered critical values** (β 1.5, no drift; T2 70% of T1):

| true net per event μ | k | discovery (any) | confirmation alone | **joint** |
|---|---|---|---|---|
| 2% | 1 / **1.5** / 2 | 0.49 / **0.42** / 0.32 | 0.81 / **0.47** / 0.32 | 0.39 / **0.19** / 0.10 |
| 4% | 1 / **1.5** / 2 | 0.81 / **0.81** / 0.76 | 1.00 / **0.96** / 0.77 | 0.80 / **0.77** / 0.57 |
| 6% | 1 / **1.5** / 2 | 0.96 / **0.96** / 0.95 | 1.00 / **1.00** / 0.98 | 0.96 / **0.96** / 0.92 |
| 8% | 1 / **1.5** / 2 | 1.00 / **0.99** / 0.99 | 1.00 / **1.00** / 1.00 | 1.00 / **0.99** / 0.99 |
| 4%, with a 1%/wk alt drift in both calendars | 1.5 | 0.93 | 0.95 | 0.89 |

- The headline is k = 1.5: listing windows half again as volatile as arbitrary coins. The draft's k = 2
  doubled the market as well; this revision does not.
- **"α > 0 without the best week" costs 0.000** in every world: the pass rates with and without it are
  identical to four decimals. Under decision 1 it is therefore **required at confirmation** (§7.2).
- **Discovery is now the binding stage.** The economic bars (raw mean ≥ 100 bp; book ≥ 3%/yr) and the
  robustness criteria cap it near 0.8 at μ = 4%.
- **Not modelled:**
  - β that varies with the market's state;
  - intraday market paths;
  - the index's trimming;
  - a liquidation jump correlated with the market.

  The size statements hold for the modelled worlds only.

## 4. The trade (fixed; `ct/listings013.py`)

| parameter | value | why |
|---|---|---|
| event | the first archive file of a `^[A-Z0-9]+USDT$` symbol, keyed (symbol, d0). TradFi, index and non-COIN contracts are out. **Continuations and pre-market perps are reported strata. Later trading segments are never events (§2.5)** | the listing of a new token is the event |
| pre-market | no `indexPriceKlines` file on d0, from 2025 | no supply before TGE; own-price index; fixed funding |
| first trade | the first 1m bar with trades | — |
| **entry** | the open of the **first traded** 1m bar in [first trade + 24 h, + 60 min); none → `no_entry`, counted | past the first-day pop; T2's feature has printed |
| **hold** | **7 days**: exit at the open of the first traded bar in [target + 7 d, + 6 h) | the first half of the documented reversal window, at half the funding and squeeze exposure of 14 d |
| **exit fallbacks (B-M1, A-m9)** | **zero-trade bars after a listing's last traded bar are absent.** No traded bar in the window but trading later: `exit_late`, at the first traded bar (a halt or data hole). No trade again before the data end: `exit_last_trade`, at the last traded close (a delisting; settlement proxy). All counted before the decision; none dropped | survivorship; Binance archives keep flat dead rows |
| margin and liquidation | 1× isolated, no stop. Liquidation at the 1m last-price high, MMR 5%; the event returns −1 | conservative against Binance's mark-price rule and a 1–2.5% tier-1 MMR |
| funding | archived settlements on their own stamps, paid at the price at s. **Funding coverage rule (B-m1, B-m2):** if a month file spanned by (first trade, entry] is missing, T2's feature is undefined: out of T2, counted (JUPUSDT@2024-01-31). If one spanned by the hold is missing, the event keeps its available settlements (`partial_funding`, counted). An event with no funding file at all (4 reported events) has zero funding (`no_funding_rows`, counted) | stated before any outcome |
| week (cluster) | ISO week of the first trade | — |

## 5. Costs

| item | decision | reported |
|---|---|---|
| taker fee | 6 bp per fill (conservative) | Binance 5 bp |
| **slippage (A-m3)** | by the **average hourly quote volume of the 24 h before the entry target** (known at entry): ≥ $50 M 5 bp · $10–50 M 8 bp · $2–10 M 15 bp · < $2 M 30 bp. **The exit fill is charged one tier worse** (8 / 15 / 30 / 50 bp), because day 8 is thinner than day 1 | ×2 (stressed) |
| funding | archived | stressed: pays ×2, receipts ×0.5, capped ±2% per settlement; slippage ×2 |

The tiers are assumptions, not measured impact (bookDepth was not pulled; §12).

## 6. Tail, sizing, Kelly, deploy rule and expected return

**The book** (`take`, `book`; D13, Revision 1).
- c = **2.5%** of capital per event, 1× isolated.
- Entries are taken in entry order while the **trailing 15 days hold < 7** book entries and the
  **trailing 39 days < 19**.
- P&L is booked at exit. Every exit of an ISO week comes from entries inside a 14.3-day window (entry ≤
  target + 1 h; exit ≤ target + 7 d + 6 h; liquidations earlier). Every exit of a calendar month comes from
  a 38.3-day window. So **exits per week ≤ 7 and per month ≤ 19, carried-in positions included**.

**Tail at the proposed size (corrected against `book()`; A-M3).** The draft's −12% / −45% bounds were
wrong: they capped entries but booked exits, and ignored carried-in positions.

| tail | value | arithmetic |
|---|---|---|
| worst event | **−2.5%** | one liquidation |
| worst exit week (bound) | **−17.5%** | 7 exits × −100% × 2.5% |
| worst exit month (bound) | **−47.5%** | 19 × −100% × 2.5% |
| **worst month, plausible** | **−29.7%** | squeeze month: half of 19 exits liquidated, half −25%: 19 × 0.625 × 2.5% |
| at the draft's c = 3% and 4/15 calendar caps (for reference) | −24% week, −57% month bound, ≈ −36% plausible (reviewer A, `book()`) | — |

- Tested on an adversarial dense schedule through `book()`: every exit a liquidation, bounds held.
- **Reported at the run:** worst event, week and month, realised drawdown, and the **daily mark-to-market
  drawdown** from each open position's midnight marks (A-M3).
- Margin turnover from the calendars' first-trade times under the caps (counts only):
  - discovery: 161 of 237 events, 104 a year → 2.6× capital a year;
  - confirmation: 83 of 119, 92 a year → 2.3×.

**The deploy rule: one definition (A-M4, decisions 2–3; `listings013.deploy`).** It is computed in the
confirmation run, for the **carried trial** only, at the deployed size c\*.
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
| book, %/yr (2024–25 rate, 2.6×) | 5.2 | 10.4 | 15.6 | 20.8 | 26.0 |
| book, %/yr (2025–26 rate, 2.3×) | 4.6 | 9.2 | 13.8 | 18.4 | 23.0 |
| after the minimum tail charge (−4.6%/yr) | ≈ 0 | ≈ 4.6–5.8 | ≈ 9–11 | ≈ 14–16 | ≈ 18–21 |

**Against the goal.** 30%/yr or more needs μ ≥ about 12% net per event at this size. That is an edge
several times the literature's first-week magnitudes after funding. At the plausible 4–6%, this is a
5–11%/yr book with a −30% plausible month. **That is honestly below the mandate's landing zone**, and it
is stated now so that a pass is read for what it is: a priced, bounded short premium that would need a
larger size, more events, or a second venue (a new trial) to matter.

## 7. Pass rules

### 7.1 Discovery (both trials; recomputed by `recheck_discovery` from the row's per-event records)

**Market adjustment.** Over the events with a market return, fit the pooled OLS net_i = α + β·mkt_i + ε_i
(β estimated; CR1 by listing week). The adjusted net is adj_i = net_i − β̂·mkt_i, whose mean is α. Per
decision 1, **significance and robustness are decided on the adjusted series; the raw net enters only the
economic bars.**

| # | criterion |
|---|---|
| 0 | ≥ 20 events and ≥ 10 listing weeks with a market return |
| **1** | **α > 0 and α's clustered t ≥ max(t_{G−2}(0.975), CAL_D)** (CAL_D: §3) |
| 2 | median adj > 0 |
| 3 | mean adj > 0 in both halves of the listing weeks |
| 4 | α > 0 refitted without the listing week with the largest positive adjusted sum |
| 5 | economic: **raw** mean net ≥ 100 bp per event (what the unhedged book earns) |
| 6 | economic: the **raw** book ≥ 3%/yr on capital at c = 2.5% |

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

**CONFIRM iff** all of:
- α > 0, with **α's clustered t ≥ max(t_{G−2}(0.95), CAL_C = 1.90)**;
- **α > 0 refitted without the listing week with the largest positive adjusted sum**. Its power cost is
  0.000 in every simulated world, within the orchestrator's ≤ 0.03 (§3), so it is required. The
  2025-10-10 cascade week alone cannot carry a confirm;
- ≥ 10 events and ≥ 5 listing weeks.

This is an **existence test on the market-adjusted net**. The raw net is reported.

**Deploy (after a CONFIRM only):** §6's rule.
- Deploy → `confirm_paper`: a registered forward paper period on **Binance**, the carried trial
  unchanged, at c\*.
- Otherwise `confirm_below_bar`: 013 closes.
- No CONFIRM → `not_confirmed`: "not demonstrated at this power", never "no edge".

## 8. Reported, decides nothing

- The raw net and its t; components; stressed and Binance-fee variants; α summaries per trial.
- By year; spot-first and perp-first; **the pre-market and continuation strata**; by slippage tier.
- Counts before the decision:
  - no first trade; no entry (primary and other separately, B-m9);
  - liquidated; `exit_late`; `exit_last_trade`;
  - T2 undefined; partial funding; no funding rows; market missing;
  - pre-market and continuation, counted over all events.
- Books (base, stressed, per trial) with the daily MTM drawdown.
- **Placebo:** calendar-**month**-block sign flips (A-M1: week flips destroyed the overlap covariance),
  200 seeds.
- The 2020–23 block, with its continuation stratum.
- Per-event compact records in the row (id, week, entry and exit times, status, net, stressed net, flags,
  market return): the inputs of the recheck.

No reported block may become a primary.

## 9. Known weaknesses, stated now

- **Public priors and the bear year.** The direction is public. The confirmation calendar is a known
  falling market; the α test is the answer, but α at an estimated β can still carry residual market
  timing if new listings' β varies with the market's state. Not modelled.
- **The continuation list** (§2.5) rests on the drafter's knowledge of public notices. "?" marks three
  that need a reviewer's check.
- **Ticker reuse loses two genuine new tokens** (PUMP 2025-07, LIT 2025-12) by rule.
- **Seasoned tokens' first perps stay in T1.** Mechanism 1 is weaker for them.
- **The confirmation calendar is small**: 119 events, 37 weeks.
- **Costs are modelled.** The tiers are assumptions; the exit is charged one tier worse; the stressed
  variant doubles them.
- **Liquidation on last-price highs** overstates liquidations: conservative for the decision, pessimistic
  for the observed tail.
- **The peso charge** uses a floor of one squeeze month per pooled sample (4.6%/yr at 2.5%). It is a
  convention, not an estimate.
- **The CR1 t with overlapping holds** was anti-conservative for the raw mean (A-M1: 0.042 against
  0.025). The raw t no longer decides anything. The α test's size is simulated with overlapping holds
  and calibrated where it exceeded nominal + 0.01 (§3). The synthetic market is a daily random walk
  interpolated within the day; intraday structure and β that varies with the market's state are not
  modelled.
- **Funding paid at the 1m open at the settlement minute**, not at the mark price.
- Access is a warning only.

## 10. Built before any outcome is computed

- [x] `ct/listings013.py` (new; pure functions): calendars; the event, continuation, pre-market and
  later-segment rules; `simulate` (traded bars only; dead rows absent; `exit_late`, `exit_last_trade`;
  daily marks); `slip_tiers`; `ols_alpha` (CR1); `alpha_crit` (calibrated); `take` / `book` (rolling caps;
  MTM); `deploy` (the one rule); `trial_stats`; `discovery_criteria`; `recheck_discovery` (from per-event
  records); `confirm`; `placebo` (month blocks); `sim_events` / `simulate_worlds`; `Refused`.
- [x] `r013_fetch.py`: unchanged except refusals instead of asserts.
- [x] `r013_build.py`: pre-entry coverage; `market_1h` with `c` and `n`; continuation flags; later
  segments; `schedule.json`.
- [x] `r013_run.py`: the guard (status, (a)–(e), (h), (k), (w), (v), (f), (g1)–(i)); the gate token;
  `Panel`; `evaluate`; `--discovery`, `--confirmation` (with the pooled deploy rule), `--power`; rows
  committed alone; `*_failed` rows.
- [x] `tests/test_r013.py`: **125 checks pass** (`.venv/bin/python -m tests.test_r013`), on synthetic data.
  The real-file section reads the registration, the hashes, column names and `events.json` field names.
  - **Statistics:** t quantiles; CR1; **OLS α and its CR1 t against a brute-force sandwich**; the trimmed
    mean; the calibrated critical value.
  - **The trade:** costs; tiers (exit one worse); funding boundaries, stress and cap; liquidation at the
    threshold; no entry; first traded bar; **a dead-tailed listing exits at its last traded close as
    `exit_last_trade`**; a halt; **a data hole gives `exit_late`**; the data end; daily marks; T2's window.
  - **Events:** calendars and embargo; first files; later trading segments; the continuation list's form;
    pre-market; month spans.
  - **The book:** the rolling cap; **the structural week and month bounds on an adversarial dense schedule
    through `book()`**; the plausible month inside −30%; **the MTM drawdown sees a loss the realised series
    cannot**; Kelly; squeeze months; **deploy: the pooled liquidation charge on a liquidation-free
    confirmation; the bar at c\*; c\* scaled to the −30% month**.
  - **Decisions:** each criterion fails alone; T1 precedence; the recheck recomputes from records, so **a
    tampered record is caught**; a forged α t; the α existence test and its edges; the deploy decisions.
  - **Simulation:** events at the given times with overlapping holds and a continuous market; a large edge
    passes.
  - **Fetch:** forward paging; truncated page; 404 never "absent"; **cutoff refusals (not asserts)**;
    mismatch writes nothing.
  - **Build:** first trade; **pre-entry coverage only, no window totals**; microseconds; `n` and `c`
    stored; segments from trade counts; embargo; calendar refusal; **end to end: zips → build → `Panel` →
    `evaluate`, robust index +10% with the dead-row symbol absent**; ordered writer.
  - **The read gate:**
    - the real panels refuse without the gate, and with a stubbed guard;
    - pointing `data_root` at the real directory, or at its parent, refuses;
    - a discovery gate does not open confirmation;
    - the per-mode whitelist;
    - a static scan; **no `assert` remains in the decision path**.
  - **The full runs end to end:**
    - started → result, each committed via the hook;
    - the recheck reproduces the decision;
    - the strata are separate;
    - the confirmation is bound to the discovery row, with the deploy block;
    - **a crash after the started row writes and commits `discovery_failed`**.
  - **Guard:** DRAFT; (a); (h); (k); (b); (d)+(e); **(f) keyed on the `013-` prefix**; (g1); (g2)+(s)+(k);
    (h'); open in order; (r); **(i) on a `confirmation_failed` row**; **(g4) alone on tampered records in a
    clean history**; (h') in the same commit.
  - **The reviewers' bypasses:**
    - (1) an uncommitted row; (2) `reset --hard`; (3) `commit --amend`; (4) `stash`;
    - (5) a nested `git init`; (6) `GIT_DIR`; (7) the anchor;
    - (8) a shadowed `duckdb.py`; (9) `python -O`; (10) the pins.
  - **Real files:** DRAFT; no approval line; the real guard refuses; the hash table; no 013 row;
    schemas; `events.json` pre-entry fields only.
- **Other suites:** unchanged by 013. The pre-existing stale state test is `test_carry012`'s "status line
  says DRAFT" (012 is REGISTERED). `test_r014` belongs to a parallel draft.

**Procedure.**
1. Two reviews, then REGISTERED (the orchestrator, under the mandate), committed with the code. The
   untracked `r008_*` and `r014_*` files must be committed or moved first, or (v) refuses: an untracked
   `*.py` at the top level could shadow an import.
2. `.venv/bin/python -I r013_run.py --discovery`. The guard runs, then `discovery_started` (committed), T1
   and T2, and one row (committed); on a crash, `discovery_failed` (committed).
3. On `read_confirmation`, recomputed from the row's records: commit `Confirmation: APPROVED <date> row
   <sha256>` under §11, after the row.
4. `.venv/bin/python -I r013_run.py --confirmation`. The same pattern applies.

**Rebuild:** `r013_fetch.py list / plan / fetch …`, then `r013_build.py`. A rebuild from the verified raw
files is byte-identical (MANIFEST `d4d51b12…`, compared with `cmp`).

**SHA-256 of the code and data the outcome depends on.** `ct/factors.py` and `ct/stats.py` are 009's
frozen modules, imported read-only for D17's recheck. Their hashes equal those in 009's and 012's rows.

| file | sha256 |
|---|---|
| `ct/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `ct/listings013.py` | `971f1066605c7f74a98ac1df60a559fef004f12188f7e4f13585a15e85255c39` |
| `ct/factors.py` | `a1e70b9a5a2c1b58f43d2b08a2eaeac5ce5a3fd45868a659d745a3354bd61197` |
| `ct/stats.py` | `bfcc85f285dc7af28f364e2d77e0fbe04b6c2367724f4b822b7fbf6ff435d472` |
| `r013_run.py` | `7329b95389eb75d4f94a2e5574f1c2cb1233a51bec9085134943c2517f653436` |
| `r013_build.py` | `3a7cbb499eb59198067c5faed2c3baa8e7b29cd002ad8d5b5496f6922a67529f` |
| `r013_fetch.py` | `6de5e028e7786858ef5519431834ce4771c1f924097a071aba0055cbe4c9fc1b` |
| `data/r013/panel_reported/events.json` | `54235bc3ac945abf17cdb599480a85103bb9472bba9bb2efffc3cc8abf48a4ed` |
| `data/r013/panel_reported/symbols.json` | `1dd416ad495ad589b09269b1028cc47ddde166d04b7c7a15a699e61cf1a4ef6a` |
| `data/r013/panel_reported/bars_1m.parquet` | `683a93a75dd25ef88132a7d314fc31858eb94fe08117f9b6ef2c7ef3603341bc` |
| `data/r013/panel_reported/funding.parquet` | `801e425392c9f217c62bd35b5bd0b21f1af533b6fad43b9f029ba4201fda0d64` |
| `data/r013/panel_reported/coverage.json` | `d3c597560b203ccdee1f6242fcc9c6db0c1726f479289e0fc541f124d14df127` |
| `data/r013/panel_discovery/events.json` | `85eb7bb36ccb4d2da52e15918e932c6215a8f042284debc017fcb5641c5d29ef` |
| `data/r013/panel_discovery/symbols.json` | `1dd416ad495ad589b09269b1028cc47ddde166d04b7c7a15a699e61cf1a4ef6a` |
| `data/r013/panel_discovery/bars_1m.parquet` | `23b6b47f35e88d45b39d1d002ccc0e66f9d579aa9f5c21d0c5ecc4e8f787b1a5` |
| `data/r013/panel_discovery/funding.parquet` | `0e664eb57782d51d662790a9acad7ade1465b3c585ea2d0f870f8065c1eb517e` |
| `data/r013/panel_discovery/market_1h.parquet` | `23ec4f14da13ff29df26ec83cfdb007c4c25452bdd1473d588421836ccf83993` |
| `data/r013/panel_discovery/coverage.json` | `cb43eaaf33304cc78c3562d76f9e566c09f78c2452038e6b81c79feb699ce4f7` |
| `data/r013/panel_confirmation/events.json` | `2c2692f341c4e88efc7cfa9f5ca1aebc58e4e429143e099853cc45005f4ccf98` |
| `data/r013/panel_confirmation/symbols.json` | `1dd416ad495ad589b09269b1028cc47ddde166d04b7c7a15a699e61cf1a4ef6a` |
| `data/r013/panel_confirmation/bars_1m.parquet` | `c80601c39fd5cc1286bd610d722b867deab1c56936f9582bb36d161e7964a077` |
| `data/r013/panel_confirmation/funding.parquet` | `0597f4f1c9c33746fc1d01624405f7129a33a225f1302116f08a354842503b34` |
| `data/r013/panel_confirmation/market_1h.parquet` | `5f230211f9a38f27222dc597ecca6702f91a48d69e5dcec795906c16f97f44a3` |
| `data/r013/panel_confirmation/coverage.json` | `168cb0b6ee1d9afd94378c1b220720f9cd8029b26644ab2c3fe71f86adc6de7b` |
| `data/r013/MANIFEST.json` | `d4d51b12d90d03a7916f6e1bf83b190f1d2fd4e3599acfa8caf284778d5d9d8c` |
| `data/r013/schedule.json` | `062771c68c37a85b8067d30f637234dd84f83270ee139a524101da20240a1fc8` |
| `data/r013/later_segments.json` | `7cb8db8ca0cbadb88338624385e067b24ca7624ff985785e28ea97e276084154` |
| `data/r013/listing/um_daily_1d_days.json` | `70e3332b592c6ca35eb21f098510a180b9ab5c279da023caec14221e1a983d71` |
| `data/r013/listing/event_keys.json` | `1235a50518c8efe8682fac0e1dca2a72cad2ee0f2fd0544cc72e319e922fa044` |
| `data/r013/inputs/fapi_exchangeInfo.json` | `a7f720f5ece63fe9ab087cbdd9fecc66d18645f113896b81ec171c89636ed39f` |
| `data/r013/raw_klines1m/SHA256SUMS` | `562944c83f84f67b11527aa829f9bf76336245c5615e71c27bade908c2247fa4` |
| `data/r013/raw_funding/SHA256SUMS` | `05094fc4cbf4ff0503faaa84bb7e5cc0fe4e3bbef9d79bf4a701bc48f7a17130` |
| `data/r013/raw_klines1h/SHA256SUMS` | `4884d513be612e413a9f589ce0dd69e0360d030c559ebf0c27abc91c18ed4a95` |

## 11. Owner decisions — PROPOSED (Revision 1)

| # | item | PROPOSED |
|---|---|---|
| D1 | venue and data | Binance USDⓈ-M; **the result is a Binance result; paper and deploy on Binance; another venue is a new trial**. The pull of §2.3 |
| D2 | calendars | as §2.2 |
| D3 | events | first files of universe symbols; continuations (§2.5 rule and list), pre-market perps and later trading segments out of the trials and reported. No first trade or no entry: counted |
| D4 | trade | §4, with the dead-row and exit fallbacks |
| D5 | margin and tail | 1× isolated, no stop, last-price-high liquidation at MMR 5% |
| D6 | funding | archived; the coverage rule of §4 |
| D7 | costs | §5 (exit one tier worse) |
| D8 | trials | T1, T2; family 2; one carried |
| D9 | discovery | §7.1: criterion 1 on α at max(t_{G−2}(0.975), CAL_D = 0: not calibrated, the stage size holds); criteria 2–4 on the adjusted net; 5–6 economic on the raw net |
| D10 | discovery decision | carry T1 > T2 > close. Close, or `discovery_failed`, is terminal |
| D11 | confirmation | after `Confirmation: APPROVED <date> row <sha256>` committed after the row. **CONFIRM iff α > 0, α t ≥ max(t_{G−2}(0.95), CAL_C = 1.90), and α > 0 without the best listing week**, ≥ 10 events, ≥ 5 weeks. Read once; `confirmation_failed` is terminal |
| D12 | deploy | §6's rule (one definition, carried trial, at c\*, with the pooled peso charges); paper on Binance |
| D13 | sizing | c 2.5%; ≤ 7 entries per 15 days, ≤ 19 per 39 days. Tail: event −2.5%, week −17.5% (bound), month −47.5% (bound) / −29.7% (plausible) |
| D14 | reported | §8 |
| D15 | placebo | month-block flips, 200 seeds |
| D16 | family | Binance listing windows: 2 trials |
| D17 | ordering and lock | (a) discovery only after 009's recomputed terminal state. (b) **No other registration or scout computes outcomes on Binance listing windows (days 0–9 after a perp's first trade) of the confirmation calendar until 013 reaches a terminal state, or until 2027-06-30, whichever comes first.** The terminal states are: a discovery `close`; a `discovery_failed` row; a `confirmation` or `confirmation_failed` row; an owner line `Confirmation: DECLINED <date>`; or a committed `*_started` row with no result row 7 days later (a hard crash). The orchestrator puts the lock in the programme document. (c) 013 reads only `data/r013/` |

## 12. Open questions

1. **Verify the "?" continuations** (PUMPBTC, CFG, ASTER, DODOX) against announcements dated before d0.
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
