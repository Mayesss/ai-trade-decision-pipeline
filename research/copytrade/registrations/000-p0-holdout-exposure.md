# 000 — P0 exposure to the holdout period

Ledger entry, not a hypothesis. Written 2026-10-07, before P2. Plan:
`docs/copy-trade-plan-2026-10-07.md` (§0, §11, §13).

## What happened

The P0 pilot and its rebuild (P0b) read Hyperliquid fills for **2026-08-08 →
2026-10-07** (60 days). The holdout is **2026-07-01 → latest**, so the pilot
window lies entirely inside it. The owner decided to keep the holdout as is
and record the exposure here.

## What was seen

- 50 wallets sampled at random (seed 7) from the 2026-10-07 leaderboard among
  accounts with ≥ $10k value and volume in the past month. Not selected on
  returns.
- Data quality: fill counts per wallet, chain breaks (27/50 wallets), mapping
  coverage, the leader `closedPnl` self-check (314/325).
- **Copy results for ONE wallet** (`0x3de9a354243a94e10060a198c7179b8e5393b9a1`,
  chosen as the wallet with the most copyable events): net +12–14% across
  poll intervals and capital sizes, fees + slippage about a third of gross
  (P0), a 30-minute delay costing ~20% of net (P0), median trip −0.10 to
  −0.16 ATR-R, risk-weighted +0.11 to +0.14, daily mean 0.22% / sd 2.4%.
- One leader's behaviour profile (204 fills/day, 14% maker, median hold
  10.8 h, typical leverage 0.81×).

## What must not follow from it

- No threshold, poll interval, cost level, band, leverage cap or selection
  feature may be chosen because of these numbers. Where a P4 parameter equals
  a pilot value (25% slippage fraction, 25% band, 3× cap, 1% maintenance
  margin), the registration must justify it on grounds other than this
  output.
- Wallet `0x3de9…` gets no special treatment in any arm.

## Mitigation

The archive's months after the holdout read form a second, untouched holdout
(plan §5.5). If the two holdouts disagree, the untouched one decides.
