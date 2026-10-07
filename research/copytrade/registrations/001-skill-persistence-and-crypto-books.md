# 001 — Skill persistence (H0), consensus (H1) and top-K copy (H2)

**Status: DRAFT — not registered.** It becomes the registration in the commit
that changes this line to `REGISTERED <date>` after the owner has approved
every item in §9. Until then no ranking score and no copy return is computed.

Plan: `docs/copy-trade-plan-2026-10-07.md`. Prior ledger entry: `000` (P0
looked inside the holdout). Inputs used to draft this: behaviour-only feature
distributions (`p4_features.py`, `p4_describe.py`, output in
`data/derived/p4_describe.txt`) — no returns, no `realized_pnl` read.

## 1. Trials registered here

| id | hypothesis | track | runs if |
|---|---|---|---|
| T1 | **H0** — skill persists | crypto (main dex) | always |
| T2 | **H0** — skill persists | `xyz` (TradFi HIP-3) | always |
| T3 | **H1** — consensus book makes money, hedged, after costs | crypto → Bitget | T1 passes |
| T4 | **H2** — top-K copy book makes money, hedged, after costs | crypto → Bitget | T1 passes |

Four trials. Family-wise one-sided α = 0.05 / 4 = **0.0125** (z ≥ 2.24).
`xyz` H1/H2 need the Capital venue (plan T1 stage) and get their own
registration later. Everything listed in §7 as robustness is reported but
decides nothing.

## 2. Data and periods

- Discovery only: main dex 2025-07-28 → 2026-06-30, `xyz` 2025-10-13 →
  2026-06-30 (`p3_fills.py`, gated in P3). **Holdout (2026-07-01 →) is not
  read** — that is registration 00x at P6.
- Windows (`ct/schedule.py`): lookback 91 days, hold 56 days, step 56 days;
  a window whose hold would reach the holdout is dropped.
  - main: selections 2025-10-27, 2025-12-22, 2026-02-16, 2026-04-13 (4)
  - `xyz`: 2026-01-12, 2026-03-09, 2026-05-04 (3)
- Leader equity: snapshot table, as-of the latest snapshot strictly before
  the moment it is needed (P2 timing gate). Fills: archive, file order =
  execution order (P3 gate G3).

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
window). Resulting universe sizes before the age and vault filters: main
2,248–3,968 per window; `xyz` 52 / 212 / 511.

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
the leaderboard's ROI is distorted by both. The top **K = 20** by score form
the selected set for H1 and H2; H0 uses the whole ranking.

## 5. H0 — does skill persist? (T1, T2)

- **Outcome:** the same score computed over the hold window [S, S + 56 d)
  for every eligible wallet with ≥ 5 completed trips there.
- **Direction-bias control:** the data spans one large drawdown, so a wallet
  that is persistently short would "persist" without skill. Wallets are split
  into terciles of lookback long-bias (long-opening notional / all opening
  notional); the statistic is computed within each tercile and averaged,
  weighted by count.
- **Statistic:** per window, Spearman IC between lookback score and hold
  score (tercile-weighted); Fisher z_w = atanh(IC_w) · √(n_w − 3);
  combined Z = Σ z_w / √W.
- **Pass:** Z ≥ 2.24 **and** IC_w > 0 in ≥ 3 of 4 windows (main) / ≥ 2 of 3
  (`xyz`).
- **Reported alongside:** the count of eligible wallets with < 5 hold-window
  trips (dropped), and a variant treating them as score 0.
- **Known limit:** wallets in one window share the market; Fisher z assumes
  independence and overstates precision. The tercile control and the
  every-window consistency rule are the guard. It is stated, not fixed.

## 6. H1 / H2 — do the books make money? (T3, T4 — only if T1 passes)

Simulator: `ct/replay.follow` with `ct/targets.Consensus` (H1) and
`ct/targets.Copy` (H2) on the same K = 20 leaders per window, built from
archive fills.

| parameter | primary | justification (registration 000: pilot-equal values need one) |
|---|---|---|
| poll interval | **10 min** | the deployable default (existing watcher cadence); 1 and 60 min are robustness |
| capital | $10,000 nominal | "does the signal exist"; the real ~$100 run is reported (§7) |
| target leverage | 1.0× | one unit of gross exposure at the leaders' typical risk |
| max leverage | 3× | account-safety cap in line with the live trader's risk posture, not tuned |
| rebalance band | 25% | trades only when a position drifts by a quarter — avoids paying 0.06% on small adjustments; a design rule, not tuned |
| consensus threshold | 0.3 | net agreement of ≥ 6 of 20 leaders; below that the vote is noise by construction |
| consensus min exposure | 0.05 | ignore dust positions |
| taker fee | 0.06% | Bitget, all contracts |
| slippage | **10% of the minute's high–low** | roughly the half-spread of liquid perps at small size; 0% and 25% are robustness |
| maintenance margin | 1% of gross | conservative against Bitget's small-size tiers |
| funding | **Hyperliquid funding as the proxy** for Bitget before 2026-07-09 (Bitget serves ~90 days) | no other history exists; "no funding" is robustness |
| Bitget listing | coins traded only after their first Bitget daily candle | P0 |
| hedge | BTC perp, beta from the trailing 30 daily book returns, point-in-time, rebalanced daily, hedge leg pays the same costs | fixed in plan §5.6 |

**Metrics:** daily net return on capital (primary), hedged and raw; per-trip
ATR-R equal-weighted and risk-weighted; buy-and-hold BTC.

**Arms (same windows, same simulator, same K):**
- N1 — K random eligible wallets, 20 independent draws per window
- N2 — top K by USD realized PnL over the last 30 days of the lookback
  (what a leaderboard shows)
- N3 — the same K leaders, positions lagged 7 days (keeps their style and
  exposure, removes their timing)

**Pass, each of T3 / T4:**
1. mean daily hedged net return over the pooled 224 days > 0 with a one-sided
   Newey–West (5 lags) p < 0.0125;
2. above the 95th percentile of the 20 N1 draws, and above N2 and N3;
3. equal-weighted and risk-weighted ATR-R both > 0;
4. holdout (P6) keeps the sign — a separate registration.

224 days is little: a pass needs a large effect. That is stated before the
run, not after it.

## 7. Robustness — reported, decides nothing

Poll 1 and 60 min; slippage 0% and 25%; no funding; raw (unhedged);
real capital ~$100 (with every skipped order counted); H0 variant with
dropped wallets as 0; K = 10 and 40.

## 8. Built before any outcome is computed

These are implementation, specified here; each gets a mechanics check like
P0b's before the first scored run:
- archive fills → `leader_events` with point-in-time Bitget listing dates;
- `equity_at(t)` from the snapshot table (as-of);
- typical leverage from the lookback snapshots (median);
- HL funding history download; BTC hedge leg in `follow`; N3 lag;
- vault address list; score and H0 statistic code with a synthetic test
  (random scores must give |Z| < 2 in most seeds).

## 9. Owner decisions before this becomes REGISTERED

| # | item | proposed |
|---|---|---|
| D1 | primary poll interval | 10 min |
| D2 | primary slippage | 10% of minute range |
| D3 | K | 20 |
| D4 | family-wise α | 0.05 / 4, one-sided |
| D5 | funding proxy for discovery | Hyperliquid funding |
| D6 | equity freshness | ≤ 7 days |
| D7 | cleaning thresholds | §3 as listed |
| D8 | H0 direction-bias control | long-bias terciles |
