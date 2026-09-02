---
name: swing-prod-probe
description: >-
  Check whether the live swing pipeline is actually healthy, and triage it when
  it is not. Use after any deploy, and whenever the user says something is off,
  stale, or "no decisions are coming in" — dashboard looks stale/empty, no
  positions opening, AI seems dead, cron not firing, gateway or env-var doubts.
  Encodes the trap that a green dashboard can hide a total AI outage.
metadata:
  pathPatterns:
    - 'vercel.json'
    - 'pages/api/health.ts'
    - 'pages/api/debug-env-values.ts'
---

# Probe the live pipeline

Prod: `https://ai-trade-decision-pipeline.vercel.app`
Auth: `x-admin-access-secret: $ADMIN_ACCESS_SECRET` from `.env.local`
(gitignored, from `vercel env pull`). Curl calls carrying that secret can be
denied by the auto-mode permission classifier — if so, tell the user and let
them approve or allowlist it; do not reshape the command to get around it.

## The trap that matters

**A healthy-looking dashboard is not evidence the AI works.** On 2026-08-28 every
real AI call failed for ~5h while everything looked fine, because each tick was
gate-skipped before reaching the gateway; the first 4H-close cycle exposed it.
So health is never inferred from "recent rows exist" — it needs a call that
*forces* a real gateway round-trip.

## Order of checks

1. **Reachability** — `GET /api/health` → `ok` (admin-gated; 401 means the secret
   is wrong, not that prod is down).
2. **Runtime env** — `GET /api/debug-env-values` shows which keys are present in
   the function runtime. `AI_GATEWAY_API_KEY` must be there:
   `VERCEL_OIDC_TOKEN` is **NOT** injected into this project (OIDC federation is
   off), so anything assuming OIDC auth for the gateway is broken by definition.
3. **Force a real AI call** — `GET /api/evaluate?symbol=BTCUSDT&limit=5`. This is
   the only probe that proves the gateway path works outside a 4H-close cycle.
   (`limit` clamps to 5–30; `async=1` + `jobId` for the queued form.)
4. **Cron cadence** — `GET /api/dashboard/warm-status`, and
   `GET /api/dashboard/summary` / `timeline` for per-symbol freshness. The
   schedule in [vercel.json](../../../vercel.json): 26 symbols on `*/15`,
   `wake-watch` every minute, `postmortem-drain` at `:07/:22/:37/:52`,
   `summary-warm-fallback` at `:03/:18/:33/:48`, weekly digest Sun 05:30.
5. **Logs** — `npx vercel logs <deployment-url> --scope mayess-projects`. Get the
   current production deployment from `npx vercel ls ai-trade-decision-pipeline
   --scope mayess-projects` (the stable alias above always points at it).
   `debugGates=true` on a tick emits `[swing_gate_debug]` lines.
6. **Database** — `npm run db:pg:health` (uses `.env.local`, prints per-table row
   counts). Rising `swing.tick_log` with flat `swing.decisions` = ticks running
   but every one skipping.

## Reading the symptoms

| Symptom | Likely cause | Confirm with |
| --- | --- | --- |
| Decisions stop, ticks continue | gate skipping everything (bouncer, cooldown, venue closed) | step 3 + `debugGates=true` tick |
| Decisions stop, ticks stop too | cron/deploy problem, or hard deactivation | step 4, step 5 |
| AI errors in logs | gateway auth or credits | step 2, step 3 |
| Stale dashboard only | warm/cache layer | step 4, `warm-status` |

## Boundaries

- Read-only. A probe never becomes a live trade: to run a tick use the
  `swing-tick` skill, always with `dryRun=true`.
- Destructive SQL against prod Neon (DROP / TRUNCATE / mass DELETE) is never
  executed from a session — write it to a `.sql` file under `docs/` with expected
  row counts and have the user run it in the Neon console after taking a
  recovery branch. Precedent: [docs/drop-scalp-tables.sql](../../../docs/drop-scalp-tables.sql).
- Neon CLI: `npx neon@latest ... --project-id holy-resonance-21485949 --org-id
  org-icy-mouse-29976601` (without the IDs it stops on an interactive picker).
