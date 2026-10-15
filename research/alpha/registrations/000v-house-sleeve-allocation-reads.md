# 000v — House-sleeve allocation decision: HLP and LLP return reads through today

Ledger entry, not a hypothesis and not a trial. Written 2026-10-14 by the
orchestrator **before** any of the reads below, at the owner's request
("move to the HLP + Lighter LLP decision now").

## What will be read

The decision is an **allocation with a priced tail**, as A1 §3.1 said it
would be if taken: the NAV-level comparison was already found unpowered
(effective N ≈ 3 spiral events), so no alpha claim can come out of it.

1. **HLP** (`0xdfc24b077bc1425ad1dea75bcb6f8158e10df303`): NAV / PnL
   history over every span available, **including 2026-07-01 → 2026-10-14**,
   from the cached daily reconstruction (`research/copytrade/data/derived/
   hyperliquid/h7/`) and the public `vaultDetails` endpoint.
2. **Lighter LLP** (account `281474976710654`): share price / NAV / PnL
   history from the public API, **including 2026-07-01 → 2026-10-14**
   (A1 left this span unread; this entry opens it).
3. **LIT** price history and current staking terms (the 1 LIT per 10 USDC
   gate makes LIT part of the position), and whether a LIT perp exists to
   hedge it.
4. Current terms: lock-ups, withdrawal cooldowns, deposit caps, fees, TVL.

Return, volatility, drawdown, worst day/week, correlation and Kelly
arithmetic **will** be computed from 1–3.

## What must not follow from it

- No alpha or edge claim about HLP, LLP or a basket of them. The output is
  a sizing and exit-rule memo for the owner's capital.
- **Any future house-product trial** (A1 §3.1's HLP+LLP basket, or anything
  on HLP/LLP NAV) has **no historical holdout left**: both series are read
  through 2026-10-14. Such a trial would be forward-only, from 2026-10-15.
- XLP, GMX, JLP series stay as A1 left them (coverage only, 2026-07-01 →
  unread).

## Family count

Spends no trial. The trial count carried by 015/016 is unchanged.

## Outcome (addendum, 2026-10-14, after the reads)

Read as stated above (pulls `pull_20261014`, `pull_20261014b`; also LIT
funding on Binance and Bitget, and LLP's `/pnl` split into perp and spot
books). Memo: `docs/house-sleeve-decision-2026-10-14.md`. Independent
review: `reviews/h1-house-sleeve-review.md`. The review found the first
draft *unsound*: LLP's post-gate return was mostly a 2026-08 spot burst, and
the Kelly fraction was computed on gross returns instead of returns over
cash. Both were verified and accepted. Recommendation: **LLP no** (perp book
≈ 6%/yr; the leg earns 5–6% on all the capital it needs; Kelly over cash
0). **HLP optional, ≤ 6% of capital**, as an explicit spiral bet. Default
is cash. No trial spent.
