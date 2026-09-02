---
name: swing-fixtures
description: >-
  Work on the contract/unit test suite: write or fix a contract test, re-capture
  the market-data fixtures, or re-baseline the conversation snapshots. Use
  whenever a snapshot diff appears after a prompt or decision-logic change,
  before running `npm test -- -u`, when a test errors on an unhandled outbound
  host, or when two scenarios in one file behave inconsistently. Encodes the
  traps that each cost about an hour.
metadata:
  pathPatterns:
    - 'test/**'
    - 'vitest.config.ts'
    - 'scripts/capture-analyze-fixtures.ts'
---

# Contract tests and fixtures

Two vitest projects ([vitest.config.ts](../../../vitest.config.ts)):
`npm run test:unit` (pure kernel, scrubbed env) and `npm run test:contract`
(msw with error-on-unhandled, ordered conversation snapshots, fake Postgres,
frozen clock). Tests mirror the source tree:
`test/unit/swing/<module>.<aspect>.test.ts`.

## Traps

- **One scenario per test FILE, not per test.** `pages/api/analyze.ts` and
  `lib/capital.ts` hold module-level state (persist map, session/epic caches,
  schema-ready flag). Two scenarios in one file contaminate each other — that is
  why `test/contract/analyze/` has 20 single-purpose files. Add a file, don't
  add a case.
- **The clock is frozen, but only `Date`.** Timers stay real, so retry/pacing
  sleeps genuinely elapse (Capital ticks take ~12s each — the rate limiter's
  waits grow linearly under a frozen clock). Contract tests freeze at the
  fixture's `capturedAtMs`.
- **Unhandled outbound hosts are a hard error, by design.** A new outbound call
  needs a world in [test/harness/worlds/](../../../test/harness/worlds/) (see
  `aiGateway.ts`) — never loosen `onUnhandledRequest`.
- **New default-ON features get pinned OFF** in
  [test/harness/setup-env.ts](../../../test/harness/setup-env.ts), with tests
  enabling them explicitly. Keeps existing scenarios stable when a flag flips.

## Re-capturing fixtures

```
npm run test:fixtures:capture -- SYMBOL [platform category]
# e.g. ... -- BTCUSDT          (bitget/crypto default)
#      ... -- EURUSD capital forex
```

Runs one real `dryRun=true` tick. Only **public market data** goes over the wire
and into `test/contract/fixtures/`; private account endpoints are stubbed, the
Capital session POST is passed through but never recorded, and the AI is always
stubbed so a capture is never billed. Capital captures need real
`CAPITAL_API_KEY` / `IDENTIFIER` / `PASSWORD` in the env (`.env.local`).

**After any re-capture, verify `promptSkipped` and the entry direction.** The
fixtures replay real market state, so the actionability outcome is
data-dependent: a BUY can be demoted to HOLD by the counter-trend rule, which
silently turns an entry test into a hold test.

## Re-baselining snapshots

`test/contract/analyze/__snapshots__/` captures the **full outgoing
conversation**, so every prompt edit produces a diff. Run `npm test -- -u` only
after reading that diff — an unexpected snapshot change means the edit reached
further than intended. That diff is the main review surface for prompt work.

## Before finishing

`npx tsc --noEmit -p tsconfig.json`, `npm run lint`, `npm test`. ESLint runs the
vanilla config with no rule overrides and `test/**` unlinted; accepted
exceptions carry inline disables rather than config changes.
