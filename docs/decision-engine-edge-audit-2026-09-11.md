# Does the decision engine predict anything? A statistical audit of a live LLM trader

**Draft, 2026-09-11.** Read-only audit over prod Neon plus refetched venue
candles. Companion to `docs/week-one-review-2026-09-10.md`, which asked why the
system was losing money; this one asks the prior question — whether its
decisions contain information about future returns at all.

Reproduction scripts: `.study/01-dump.mjs` … `.study/10-rfix-verify.mjs`.

## 0. Summary

Three months of live decisions from an LLM swing trader were tested for
out-of-sample information about volatility-normalised forward returns.

1. **No positive information at any horizon.** Every point estimate is
   negative; three of four horizons have confidence intervals containing zero.
2. **The one interval that excludes zero has the wrong sign** (48h: −1.11 ATR),
   survives the obvious artifact checks, and still does not clear a
   multiple-testing bar — and it is concentrated in one month under a since-
   replaced engine version.
3. **The sample cannot answer the question either way.** Effective n is 94, not
   453. Resolving a realistic edge needs ~4,900 decisions.
4. **The primary metric was broken.** R was divided by the risk *budget* rather
   than the risk *taken*, compressing every R by 5–16×. Found and fixed during
   this audit; it would have made the pre-registered 200-close read unfireable.

The honest conclusion is not "the engine has negative edge." It is **"no
detectable edge, on a sample far too small to have detected a realistic one"** —
and those two statements have very different consequences.

## 1. What was tested

| | |
|---|---|
| Unit of observation | one non-dry-run decision with action ∈ {BUY, SELL} |
| Sample | 453 decisions, 2026-06-27 → 2026-09-11 |
| Instruments | 25 symbols / 4 categories (crypto 169, index 149, commodity 114, forex 21) |
| Excluded | 20,083 HOLD, 139 CLOSE, all dry runs |

Entry price and 4H ATR come from each decision's stored `snapshot_json`, so the
engine's own view at decision time is used — no reconstruction. Forward prices
are 4H candles refetched from Bitget and Capital.

**Outcome.** `y_h = dir × (close_{t+h} − P_t) / ATR_t`, with `dir` +1 for BUY
and −1 for SELL, `h` in 4H bars. Dividing by ATR makes it a *risk-adjusted*
return — profit per unit of that instrument's own volatility — which puts a
BTC tick and a EURUSD tick on one scale. **H₀: E[y_h] = 0.**

**Dependence.** Decisions overlap in time and 25 instruments share one macro
tape, so the iid t-stat is badly inflated. All inference clusters on calendar
day; the 48h horizon additionally uses a moving-block bootstrap over the day
axis with block length matched to the horizon.

**Pre-registered before looking at outcomes:** primary horizon h=6 (24h),
chosen to bracket the observed 3.8h median hold and the 4H decision cadence.

## 2. Result: no positive information at any horizon

| horizon | n | mean (ATR) | 95% CI | t_day | p_block | hit rate |
|---|---|---|---|---|---|---|
| 4h | 453 | −0.051 | [−0.129, 0.043] | −1.23 | 0.258 | 45.5% |
| 8h | 450 | −0.165 | [−0.308, 0.012] | −2.24 | 0.046 | 42.4% |
| **24h (primary)** | 435 | **−0.312** | [−0.643, 0.076] | −1.66 | 0.088 | 44.1% |
| 48h | 411 | −1.113 | [−1.730, −0.236] | −3.19 | 0.008 | 34.5% |

The engine's own confidence label runs the wrong way too:

| subset | n | mean (ATR, 24h) | t_day | hit rate |
|---|---|---|---|---|
| `signal_strength = HIGH` | 192 | −0.736 | −2.27 | 37.0% |
| `signal_strength = MEDIUM` | 232 | +0.033 | 0.17 | 50.0% |

The decisions the model was most confident about are the ones that did worst.

## 3. The 48h result is not an artifact

Three checks, each of which would have killed it:

**Price/candle alignment.** A bid-vs-mid mismatch or a wrong series would show
as snapshot prices outside their containing bar. 453/453 decisions sit inside,
mean signed offset 0.0000 ATR, on both venues.

**Direction decomposition.** A constant price offset biases longs and shorts in
*opposite* directions. Both are negative at every horizon:

| horizon | n long | mean long | n short | mean short | tape drift (mean u) |
|---|---|---|---|---|---|
| 4h | 263 | −0.030 | 190 | −0.080 | +0.016 |
| 8h | 261 | −0.126 | 189 | −0.219 | +0.019 |
| 24h | 252 | −0.165 | 183 | −0.513 | +0.120 |
| 48h | 245 | −0.947 | 166 | −1.357 | −0.016 |

At 48h the tape went essentially nowhere on average (−0.016 ATR), yet both
sides lost ~1 ATR. When the engine bought, price fell; when it sold, price
rose. This also rules out drift-riding: the 57.9% long tilt contributes only
+0.019 ATR of the 24h result.

**Concentration.** Not outlier-driven — median −1.178 against mean −1.113,
5/95 winsorised −1.116, 17 of 25 symbols negative, median per-symbol mean
−0.262, and −0.740 excluding BTCUSDT (the largest single symbol at n=85).

A within-day permutation of direction labels — which holds each day's long/
short mix and the available cross-section fixed, so only *which instrument,
which way* varies — gives p < 0.0001. The null is not degenerate: 21 of 46 days
carry both directions, covering 402 of 453 observations.

## 4. Why it still does not support a claim

**Multiplicity.** 16 tests were run (4 horizons, 2 direction splits, 5 subsets,
5 periods). Bonferroni at 5% requires p < 0.0031; the 48h block bootstrap gives
0.0079. Harvey & Liu's |t| > 3.0 bar for a claimed new effect is cleared by the
day-clustered t (−3.19) but not the week-clustered one (−2.22) — and with only
11 week-clusters that SE is itself unreliable. The result sits *on* the bar,
not past it.

**Regime concentration.** The engine is not one fixed model.

| period | n (24h) | mean 24h | mean 48h |
|---|---|---|---|
| v1 pre-overhaul (→ 09-02) | 281 | −0.461 | −1.357 |
| v2 overhaul week (09-02 → 09-11) | 154 | −0.040 | −0.584 |
| v3 since the 09-11 freeze | 0 | — | — |

| month | n | mean 24h |
|---|---|---|
| 2026-06 | 17 | −0.396 |
| 2026-07 | 252 | −0.428 |
| 2026-08 | 11 | −1.374 |
| 2026-09 | 155 | −0.038 |

252 of 435 observations are July under a since-replaced configuration. For the
current engine the 24h estimate is −0.040 with CI [−0.398, +0.479]: noise.

**Out-of-sample status.** The individual decisions were genuinely ex-ante — the
model never saw the future. But every *specification* choice (gates, stop
floor, model, session windows) was made after seeing this data. For the
configuration frozen on 2026-09-11 there are 0 directional decisions and 10
closed trades. True out-of-sample n is effectively zero.

## 5. The sample cannot answer the question

Day-clustering implies an **effective n of 94**, not 435.

| bar | detectable mean at 80% power | n needed for a true 0.10 ATR edge |
|---|---|---|
| t > 1.96 (nominal 5%) | 0.527 ATR/trade | 2,599 |
| t > 3.0 (Harvey–Liu) | 0.722 ATR/trade | 4,888 |

Only an implausibly large edge — half an ATR per trade — would have been
visible here. At the current rate of directional decisions that is well over a
year of trading.

This directly indicts the pre-registered read in `week-one-review` §12 ("if avg
R is still ≤ 0 at 200 closes, the edge is not there"). At 200 closes the
confidence interval on mean R still spans zero whether the true edge is +0.1R
or −0.1R. **200 closes is a survival checkpoint, not an inference one.**

## 6. The metric itself was broken

The most consequential finding was not about the market.

`rStats.ts` computed `R = pnl_net / risk_sizing.risk_usd` — the risk
*budgeted* at entry. But `riskSizing.ts` clamps notional at
`EXPOSURE_CAP_EQUITY_MULT × equity`, which binds for any stop tighter than ~5%
of entry — essentially every swing stop. The code said so already: *"whenever
it binds, 'fixed dollar risk' stops being true."* The measurement was never
updated to match.

Measured over 122 closes with a stop on the placing decision:

| | budgeted risk | stop-implied risk | median &#124;pnl_net&#124; |
|---|---|---|---|
| bitget | $6.01 | $1.23 | $1.04 |
| capital | $9.05 | $0.56 | $0.27 |

Realized PnL tracks the stop-implied risk, not the budget. The denominator was
5–16× too large, so every R was compressed toward zero: median loser −0.05R,
89.8% of losers between −0.2R and 0, and **nothing in 125 trades worse than
−0.75R** in a design where a clean stop-out is −1R.

Mean R read −0.002 with CI [−0.042, 0.051] — precise-looking, and measured in a
unit an order of magnitude too big.

**Fixed in this audit.** R now divides by the risk actually taken: the fill's
own notional against the stop the placing decision shipped
(`resolveRealizedRiskUsd`, preferring `position_stop`, falling back to a newly
recorded `effective_risk_usd`). A row whose real denominator cannot be
reconstructed is counted as closed but **not measured** — mixing units across
trades is what made the mean meaningless in the first place.

| | avg loss | worst trade | measured rows | avg R |
|---|---|---|---|---|
| before (÷ budget) | −0.090R | −0.75R | 125 | −0.002 |
| after (÷ risk taken) | −0.639R | **−3.45R** | 151 | −0.158 |

Losses now land near the −1R a stop-out should cost. The "worse than −1.3R
means slippage or a gap" tripwire can finally fire — it had been hiding a
−3.45R event. Measured coverage went *up*, because the reconstruction path also
works on rows that never carried a budget.

## 7. What this changes

1. **No claim of edge, in either direction.** The defensible statement is "no
   detectable information, on a sample that could not have detected a realistic
   amount of it."
2. **Do not invert the engine.** The 48h anti-signal is one month, one engine
   version, at a horizon 12× the actual median hold, and it collapses in
   September. Trading it would be fitting the same noise from the other side.
3. **The 200-close target is a survival checkpoint.** Any inferential read of
   the freeze window needs to state the power it actually has.
4. **Measure the measurement first.** The R denominator was wrong for the
   entire life of the metric, and it was wrong in the direction that hides
   problems — compressing both the estimate *and* its spread toward zero. No
   amount of additional sample would have surfaced it.

## 8. Limitations

- Forward returns use 4H closes; intrabar path is captured only as MFE/MAE and
  is not used in the headline test.
- Costs (fees, spread, slippage) are **not** deducted from `y`. They would make
  every number worse, not better.
- The audit covers directional decisions only. Whether the engine's HOLD/skip
  behaviour is informative — the "strongest trader is the one that says no"
  hypothesis — is a separate test on a much larger sample (20,083 HOLDs) and is
  not answered here.
- `loadClosedPositionRiskRows` reads raw `swing.positions`; Capital's two-row
  close shape is not merged there. Only the transaction row carries `pnl_net`,
  so this does not double-count, but it has not been separately audited.
- Three months, one account, one venue pair, one model family. Nothing here
  generalises to LLM trading in general — it is one system, measured honestly.

## 9. Reproduction

```
node .study/01-dump.mjs                  # read-only prod dump (decisions + trades)
node --import tsx .study/02-candles.ts   # refetch 4H candles per symbol
node --import tsx .study/03-panel.ts     # build the forward-return panel
node .study/04-stats.mjs                 # primary tests, subsets, regimes, power
node .study/05-diag.mjs                  # artifact checks (§3)
node .study/06-robust.mjs                # overlap-safe inference, regime stability
node .study/07-trades.mjs                # realized R
node .study/08-rcal.mjs                  # R calibration (§6)
node .study/09-final.mjs                 # confidence bands, multiplicity
node .study/10-rfix-verify.mjs           # before/after the denominator fix
```

All DB access uses `BEGIN READ ONLY` on the **unpooled** connection string. Per
the 2026-09-10 incident (`week-one-review` §9), analysis must never issue a
session-level `SET` on the pooled URL.
