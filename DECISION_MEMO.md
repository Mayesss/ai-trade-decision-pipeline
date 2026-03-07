# Decision Memo: BTCUSDT Strategy Deployment

## Executive decision

**Do not trust the current 3-month selection-and-deploy process for live promotion.**

The report supports this clearly:

- selection windows looked very strong at **+0.736 R/trade**
- forward performance turned negative at **-0.271 R/trade** over 2 weeks and **-0.192 R/trade** over 4 weeks
- only **5/18** 2-week windows and **7/18** 4-week windows were profitable out of sample

That is strong evidence that the current process is **selecting recent winners that do not generalize**.

## What to keep

**Keep the two strategy families as research candidates.**

Reason:

- the report does **not** prove the core ideas are nonsense
- some management parameters looked reasonably stable locally
- the two strategies show only **partial overlap**, not full duplication
- combined performance reduced max drawdown slightly versus the guarded leg alone, so there is at least some diversification benefit

Interpretation:

- **CBP** and **Guarded** are still worth researching
- they should be treated as **candidate edge families**
- they should **not** be treated as validated deployable variants yet

## What to disable

**Disable automatic promotion based only on trailing 3-month ranking.**

Reason:

- this is the main failure mode in the report
- the forward degradation is too large to wave away as noise

**Disable any assumption that blocked Berlin hours `[10,11]` are now “proven.”**

Reason:

- they were best in the full-year aggregate
- but only best in **4/18** forward windows
- no-block was best in **8/18** windows, which means the filter is not stable enough to hard-code as truth

**Disable use of current execution stress results as evidence of safety.**

Reason:

- harsher execution barely changed outcomes
- in some cases results even improved slightly
- that strongly suggests the execution model is not realistic enough yet

## What to change before the next deploy

### 1) Add a promotion gate based on rolling forward results

A strategy should not be deployable just because it ranked first on the last 3 months.

Require something like:

- non-negative mean forward expectancy over recent walk-forward rolls
- minimum profitable-window rate
- acceptable forward max drawdown
- minimum trade count in forward windows

The exact thresholds can be tuned, but the principle should be hard:
**no forward evidence, no promotion.**

### 2) Separate research ranking from deployment eligibility

You currently have one process trying to do both:

- find promising variants
- decide what goes live

Split them.

Use:

- **research ranking** to surface candidates
- **deployment gate** to decide if any candidate is allowed live

This avoids shipping the “prettiest recent curve.”

### 3) Treat blocked hours as a falsifiable feature flag

For guarded strategy:

- keep `[10,11]` as an experiment
- compare it continuously against `none` and nearby alternatives
- require repeated out-of-sample wins before making it default

### 4) Fix execution model monotonicity

You need a test that asserts:

- worse slippage cannot improve expectancy
- wider spread cannot improve expectancy
- harsher execution cannot improve net PnL except for trivial rounding noise

Until this holds, stress tests should be considered unreliable.

### 5) Reduce selection complexity

Even inside a limited 29-scenario neighborhood, forward performance was poor.

That means adding more variants probably worsens selection luck.

So:

- shrink the candidate set
- prefer broad, stable parameter regions
- avoid single sharp optima

## Strategy-by-strategy verdict

### CBP

**Status: keep for research, not trusted for live promotion**

Why:

- some local parameter behavior looked okay
- but forward selection still failed badly enough that the deployed version is not trustworthy

Decision:

- keep family
- do not auto-promote its recent winners
- prioritize simpler, more stable variants

### Guarded

**Status: keep for research, not trusted for live promotion**

Why:

- strongest standalone full-year metrics
- but still failed the forward-selection test
- blocked-hour edge is not stable enough to certify as structural

Decision:

- keep family
- demote blocked-hour rule to experimental
- require forward validation before promotion

## Operational policy going forward

### Live capital

Use only with:

- paper trading
- shadow deployment
- or minimal risk allocation

Do **not** increase confidence from the full-year aggregate alone.

### Variant selection

A candidate may be “interesting” if it ranks well in-sample.
A candidate may be “deployable” only if it also passes the forward gate.

### Reporting

Every evaluation cycle should now include:

- in-sample rank
- forward expectancy
- profitable-window rate
- parameter stability judgment
- execution realism check
- whether hour filters remain stable out of sample

## Immediate next actions for the codebase

1. Implement a deployment eligibility check using rolling forward metrics.
2. Mark blocked-hour policy as experimental, not default truth.
3. Add execution monotonicity tests.
4. Narrow the scenario universe to robust plateaus.
5. Re-run walk-forward and promote only variants that pass forward criteria.

## Final recommendation

**Keep both strategy families alive, but stop trusting the current variant-selection process.**

The report does **not** say “BTC continuation ideas are dead.”
It says:

**your current deployment logic is mostly buying yesterday’s luck, and your execution stress model is not yet trustworthy.**

So the right decision is:

- **keep researching the families**
- **stop auto-promoting recent winners**
- **require forward proof before live deployment**
- **treat `[10,11]` as unconfirmed**
- **fix execution modeling before using stress tests for safety**
