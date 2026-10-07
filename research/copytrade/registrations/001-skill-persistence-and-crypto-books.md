# 001 — Does skill persist, and can a follower capture it?

**Status: DRAFT — not registered.** It becomes the registration in the commit
that changes this line to `REGISTERED <date>` after the owner has approved
every item in §9. Until then no ranking score and no copy return is computed.

Plan: `docs/copy-trade-plan-2026-10-07.md`. Prior ledger entry: `000` (P0
looked inside the holdout). Inputs used to draft this: behaviour-only feature
distributions (`p4_features.py`, `p4_describe.py`, output in
`data/derived/p4_describe.txt`) — no returns, no `realized_pnl` read.

**Revision 2026-10-07 (still a draft):** the first draft made the book tests
(consensus H1, top-K H2) pass/fail trials. One book per window gives ~224
daily observations — a pass would need a very large effect and a fail would
mean "undetectable", not "absent". The deciding economic test is now
**cross-sectional** (T3): every eligible wallet is copied in simulation, with
delay and costs, and the question is whether the selection score predicts
the *follower's* return. H1/H2 are reported descriptively here; their
pass/fail belongs to the holdout and forward registrations, where they are
judged as the deployable product.

## 1. Trials registered here

| id | hypothesis | track | unit |
|---|---|---|---|
| T1 | **H0-leader** — a leader's own skill persists | crypto (main dex) | wallet × window |
| T2 | **H0-leader** | `xyz` (TradFi HIP-3) | wallet × window |
| T3 | **H0-copy** — the selection score predicts the **follower's** net, hedged return on Bitget | crypto | wallet × window (~10k single-leader copies) |

Three trials. Family-wise one-sided α = 0.05 / 3 = **0.0167** (z ≥ 2.13).
T3 is the deciding test; T1 tells whether a failure of T3 is "no skill" or
"skill a follower cannot capture". `xyz` H0-copy needs the Capital venue (plan
stage T1) and gets its own registration. Everything in §7 is reported and
decides nothing.

## 2. Data and periods

- Discovery only: main dex 2025-07-28 → 2026-06-30, `xyz` 2025-10-13 →
  2026-06-30 (`p3_fills.py`, gated in P3). **Holdout (2026-07-01 →) is not
  read** — that is a later registration at P6.
- Windows (`ct/schedule.py`): lookback 91 days, hold 56 days, step 56 days;
  a window whose hold would reach the holdout is dropped.
  - main: selections 2025-10-27, 2025-12-22, 2026-02-16, 2026-04-13 (4)
  - `xyz`: 2026-01-12, 2026-03-09, 2026-05-04 (3)
- Leader equity: snapshot table, as-of the latest snapshot strictly before
  the moment it is needed (P2 timing gate). Fills: archive, file order =
  execution order (P3 gate G3).
- Execution prices: Bitget public API (1-minute and 1-hour candles), fetched
  only for the 200-minute blocks that contain a simulated order, cached.

## 3. Cleaning — the eligible universe at each selection date S

Behaviour only, measured on the lookback [S − 91 d, S):

| filter | value | why (not from any return) |
|---|---|---|
| completed round trips | ≥ 20 | enough trips for a score with a usable standard error |
| account value as-of S | ≥ $10,000 | not dust; p90 of active wallets is ~$6–13k |
| account value age | ≤ 7 days | snapshots list position holders only; older values mis-size the copy (59–87% of filtered wallets pass) |
| maker share (by notional) | ≤ 0.5 | the follower always pays taker; a maker's edge is the spread |
| median holding time | ≥ 2 h | 12× the primary 10-minute poll |
| fills per active day | ≤ 500 | bots |
| mapped share (main only) | ≥ 0.8 | trades what Bitget lists |
| vaults / protocol accounts | excluded | address list from the Hyperliquid API, fetched and fixed **before** any score is computed |

Dropped as redundant: "share of trips under 1 h ≤ 0.5" (no wallet it would
remove survives the 2 h median-hold filter — identical counts in every
window). Universe sizes before the age and vault filters: main 2,248–3,968
per window; `xyz` 52 / 212 / 511.

## 4. Selection score (computed only after registration)

Per completed round trip *i* in the lookback (flat → flat per coin, flips
split, exact cash flow from fills — not the archive's `realized_pnl`, which
drifts on sub-cent coins, P3 G4):

    RoN_i = (cash flow_i − est. Hyperliquid fees_i) / max notional_i

Fees estimated per fill at Hyperliquid's base tier — 0.045% taker
(`crossed`), 0.015% maker — **to be confirmed against Hyperliquid's fee
schedule before the first score is computed; a correction then is an
amendment, allowed only before any outcome exists.**

    score = mean(RoN) / sd(RoN) × √n         (a t-statistic of return on notional)

Return on notional needs no equity and is blind to leverage and deposits —
the leaderboard's ROI is distorted by both.

**Long-bias** (used as a control below): long-opening notional / all
opening notional over the lookback.

## 5. The tests

Common to T1–T3: wallets in a window are split into **terciles of
lookback long-bias** — the data spans one large drawdown, so persistently
short wallets would "persist" without skill. Per window, Spearman IC between
the lookback score and the outcome is computed within each tercile and
averaged, weighted by count. Fisher z_w = atanh(IC_w) · √(n_w − 3); combined
Z = Σ z_w / √W.

### T1 / T2 — H0-leader

- **Outcome:** the same score over the hold window [S, S + 56 d), for
  eligible wallets with ≥ 5 completed trips there.
- **Pass:** Z ≥ 2.13 **and** IC_w > 0 in ≥ 3 of 4 windows (main) / ≥ 2 of 3
  (`xyz`).
- Reported: wallets dropped for < 5 hold trips, and the variant scoring them
  as 0.

### T3 — H0-copy (deciding)

- **Outcome:** for **every** eligible wallet, a single-leader copy over the
  hold window (`ct/replay.follow` with `ct/targets.Copy([leader])`, primary
  configuration §6): the follower's **mean daily net return on capital,
  BTC-hedged**. A wallet that stops trading simply yields a flat follower —
  no wallet is dropped, so no survivorship in the outcome.
- **Pass, all three:**
  1. Z ≥ 2.13;
  2. IC_w > 0 in ≥ 3 of 4 windows;
  3. **the top quintile by score earns**: its mean hedged daily net return,
     pooled over windows, is > 0. Rank correlation alone is never enough —
     the spec's reversion study predicted ranks perfectly well and lost
     money (it predicted the median and anti-predicted the mean). Reported
     with it: the top-quintile median, and its equal- and risk-weighted
     per-trip ATR-R.
- **Known limit (T1–T3):** wallets in one window share the market; Fisher z
  assumes independence and overstates precision. The tercile control and the
  every-window consistency rule are the guard. Stated, not fixed.

## 6. Simulator configuration — primary

| parameter | primary | justification (registration 000: pilot-equal values need one) |
|---|---|---|
| poll interval | **10 min** | the deployable default (existing watcher cadence); 1 and 60 min are robustness |
| capital | $10,000 nominal | "does the signal exist"; the real ~$100 run is reported (§7) |
| target leverage | 1.0× | one unit of gross exposure at the leader's typical risk |
| leader typical leverage | median gross leverage over the lookback snapshots | point-in-time |
| leader equity | as-of the latest snapshot before each poll | point-in-time |
| max leverage | 3× | account-safety cap in line with the live trader's risk posture, not tuned |
| rebalance band | 25% | trades only when a position drifts by a quarter — avoids paying 0.06% on small adjustments; a design rule, not tuned |
| taker fee | 0.06% | Bitget, all contracts |
| slippage | **10% of the minute's high–low** | roughly the half-spread of liquid perps at small size; 0% and 25% are robustness |
| maintenance margin | 1% of gross | conservative against Bitget's small-size tiers |
| funding | **Hyperliquid funding as the proxy** before 2026-07-09 (Bitget serves ~90 days) | no other history exists; "no funding" is robustness |
| Bitget listing | coins copied only after their first Bitget daily candle | P0 |
| hedge | BTC perp; beta from the trailing 30 daily follower returns, point-in-time, rebalanced daily; the hedge leg pays the same costs | plan §5.6 |
| window end | everything closed at the last minute, paying costs | plan §5.5 |

## 7. Reported, decides nothing

- **Books (descriptive):** H1 consensus (`targets.Consensus`, threshold 0.3,
  min exposure 0.05) and H2 top-K copy (`targets.Copy`) on the top K = 20 by
  score, with arms N1 (20 random K-draws), N2 (top K by USD realized PnL over
  the last 30 lookback days — the leaderboard view) and N3 (same leaders,
  positions lagged 7 days). Run only if T3 passes; every number printed with
  "descriptive — not a registered test".
- **Robustness of T1–T3:** poll 1 and 60 min; slippage 0% and 25%; no
  funding; unhedged; real capital ~$100 (every skipped order counted); T1/T2
  with dropped wallets as 0; quartiles instead of quintiles.

## 8. Built before any outcome is computed

Implementation, specified here; each gets a mechanics check like P0b's before
the first scored run:
- archive fills → `leader_events`, point-in-time Bitget listing dates;
- `equity_at(t)` and typical leverage from the snapshot table;
- Hyperliquid funding history; BTC hedge leg in `follow`; N3 lag;
- block-on-demand Bitget candle fetching at ~18 requests/s (limit 20);
- vault address list;
- score, tercile IC and Fisher combination, with a synthetic test: random
  scores must give |Z| < 2.13 in ≥ 95% of 200 seeds.

## 9. Owner decisions before this becomes REGISTERED

| # | item | proposed |
|---|---|---|
| D1 | primary poll interval | 10 min |
| D2 | primary slippage | 10% of minute range |
| D3 | K for the descriptive books | 20 |
| D4 | family-wise α | 0.05 / 3, one-sided |
| D5 | funding proxy for discovery | Hyperliquid funding |
| D6 | equity freshness | ≤ 7 days |
| D7 | cleaning thresholds | §3 as listed |
| D8 | direction-bias control | long-bias terciles |
| D9 | T3 outcome | follower's mean daily net return, BTC-hedged, nominal capital |
| D10 | T3 economic criterion | top-quintile pooled mean > 0 |
