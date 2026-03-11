# Scalp PG Runtime

This folder contains active Postgres primitives for scalp orchestration after cutover.

Design constraints:
- Keep database invocations minimal:
  - bulk upserts via `jsonb_to_recordset` + `ON CONFLICT`
  - queue claims via `FOR UPDATE SKIP LOCKED`
  - single-statement status transitions when possible
- Avoid per-row `upsert` loops for queue/task workloads.

Current modules:
- `client.ts`: Prisma singleton and config guard.
- `deployments.ts`: bulk deployment upsert + executable deployment query.
- `executionRuns.ts`: execution-run idempotency claim/finalize primitives (`scalp_execution_runs`).
- `jobs.ts`: queue enqueue/claim/complete primitives.
- `storeMirror.ts`: runtime/session/journal/ledger persistence helpers for PG primary runtime.
- `researchMirror.ts`: research cycle/task/cooldown persistence helpers for PG primary runtime.
- `researchTasks.ts`: research claim/defer/complete/fail primitives.
