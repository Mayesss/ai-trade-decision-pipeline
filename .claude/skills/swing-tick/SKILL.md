---
name: swing-tick
description: >-
  Run one swing decision tick by hand and read back what the AI saw and decided.
  Use when asked to test a prompt/decision change end to end, reproduce a
  symbol's decision, debug a gate skip ("why was this HOLD/skipped"), inspect the
  outgoing prompt, or verify a change against real market data — anything that
  means "run a tick" / "analyze SYMBOL" / "dry run". ALWAYS the entry point for
  manually invoking /api/analyze, because dryRun defaults to FALSE and a bare
  call places real orders.
metadata:
  pathPatterns:
    - 'pages/api/analyze.ts'
    - 'lib/swing/prompt.ts'
    - 'lib/swing/decisionRules.ts'
---

# Run a swing tick by hand

## The one rule

`dryRun` defaults to **`false`** ([analyze.ts:408](../../../pages/api/analyze.ts#L408)).
A manual call without `dryRun=true` **places real orders on a live account**.

Always pass `dryRun=true` unless the user explicitly asked for live execution in
this conversation (AGENTS.md rule #2). Live execution is their call to make, not
an inference from "test it against prod".

## Which endpoint

| Route | Auth | Use |
| --- | --- | --- |
| `/api/analyze` | admin header required | **manual ticks — use this** |
| `/api/swing/analyze` | **none** (in `UNAUTHENTICATED_CRON_ROUTES`, [lib/admin.ts:4](../../../lib/admin.ts#L4)) | the Vercel cron path only |

`/api/swing/analyze` also arms the warm latch and is what the 26 crons in
[vercel.json](../../../vercel.json) hit every 15 min. Manual work goes to
`/api/analyze` so it stays out of the automation accounting.

GET only. Auth is `x-admin-access-secret: $ADMIN_ACCESS_SECRET` (or
`Authorization: Bearer <secret>`); the secret is in `.env.local` after
`vercel env pull` (gitignored).

## Parameters that matter

- `symbol` (default `ETHUSDT`), `platform` (`bitget` | `capital`), `category`,
  `newsSource` (`coindesk` for crypto, `marketaux` for capital) — copy the
  symbol's exact cron line from [vercel.json](../../../vercel.json) so the tick
  matches production, then flip `dryRun=false` → `dryRun=true`.
- `dryRun=true` — see above.
- `debugGates=true` — logs `[swing_gate_debug]` at each gate stage. First thing
  to add when a tick skips and you don't know why.
- `decisionPolicy` (`balanced` in prod), `notional`, `enforcePrimaryCloseGate`.

## A manual tick is NOT gated by the 4H close

The primary-close gate is `(EVAL_PRIMARY_CLOSE_ONLY && automationCron) ||
enforcePrimaryCloseGate` ([analyze.ts:800](../../../pages/api/analyze.ts#L800)).
`automationCron` is false for manual calls, so a hand-run tick **always takes
the full path and makes a real, billed AI call** — it is not free, and it is not
the same code path the cron takes. To reproduce cron cadence behaviour, add
`enforcePrimaryCloseGate=true`; to reproduce a quarter-tick scan you cannot —
`quarterTick` requires `automationCron`.

Other gates still apply and will set `promptSkipped: true` (ai-bouncer, venue
closed, cooldown, hard deactivation). `promptSkipped: true` means **no AI call
happened** — the decision in the response is synthetic. Check it before drawing
any conclusion about model behaviour.

## Reading the result back

The response carries `decision`, `execRes`, `promptSkipped`, `decisionPolicy`.
For the full stored prompt + decision:

- `GET /api/rest-history?symbol=SYMBOL&platform=PLATFORM` — indexed decision rows.
- `GET /api/dashboard/decision?symbol=SYMBOL&platform=PLATFORM[&ts=MS]` — one
  decision with its prompt.
- `swing.decisions` in Neon for anything historical (`npm run db:pg:health`
  proves the local DB env works).

## Local vs prod

- Local: `npm run dev`, then `http://localhost:3000/api/analyze?...`. Uses
  `.env.local`, which points at the **production** Neon DB and **live** broker
  keys — `dryRun=true` is what keeps a local tick from trading.
- Prod: `https://ai-trade-decision-pipeline.vercel.app/api/analyze?...`.

Curl calls that carry the admin secret can trip the auto-mode permission
classifier. If one is denied, say so and let the user approve or add a Bash
allowlist rule — do not reshape the command to slip past it.

## After a prompt/decision change

Follow the AGENTS.md prompt-change workflow: check `postprocessDecision` +
sanitizers in `lib/swing/decisionRules.ts`, execution in `lib/trading.ts`,
parsing in `pages/api/evaluate.ts`, then run the contract tests — any prompt edit
shows up as a snapshot diff. See the `swing-fixtures` skill before re-baselining.
