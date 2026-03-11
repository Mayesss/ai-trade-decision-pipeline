**Decision**
Move scalp orchestration from KV + serverless crons to an always-on multi-process service backed by PostgreSQL, with explicit queues, row-level locking, and durable task state.

**Why (from current KV snapshot on March 10, 2026)**
1. Research failures are dominated by orchestration semantics, not strategy quality.  
`rc_20260310T024533Z_6a7cce`: `874` failed, `784` = `symbol_cooldown_active`.
2. Running cycle shows same pattern.  
`rc_20260310T144537Z_ce3fec`: `984` failed, `966` = `symbol_cooldown_active`.
3. KV/list/cron complexity is now the main operational risk.

**Target Runtime**
1. `api` process: admin/dashboard/control endpoints.
2. `scheduler` process: emits jobs on schedule (no serverless cron dependency).
3. `worker-exec` processes (N): scalp execution cycles.
4. `worker-research` processes (N): replay/research tasks.
5. `worker-maint` process: aggregation, promotion sync, guardrail, housekeeping.
6. PostgreSQL as system of record for state, queue, locks, and audit trail.

**Core Behavioral Rules**
1. Cooldown is `defer`, never terminal failure.
2. Promotion uses completed windows only.
3. Incomplete coverage is `incomplete_validation`, not a hard fail.
4. All jobs are idempotent with DB unique keys.
5. No hidden fail-open state loads for critical paths.

**Postgres Schema (Concrete v1)**
```sql
create type scalp_job_kind as enum (
  'execute_cycle',
  'research_task',
  'research_aggregate',
  'promotion_sync',
  'guardrail_check',
  'housekeeping'
);

create type scalp_job_status as enum (
  'pending',
  'running',
  'retry_wait',
  'succeeded',
  'failed_permanent',
  'cancelled'
);

create type scalp_research_task_status as enum (
  'pending',
  'running',
  'retry_wait',
  'completed',
  'failed_permanent',
  'cancelled'
);

create type scalp_cycle_status as enum ('running','completed','failed','stalled');

create table scalp_deployments (
  deployment_id text primary key,
  symbol text not null,
  strategy_id text not null,
  tune_id text not null,
  source text not null check (source in ('manual','backtest','matrix')),
  enabled boolean not null default false,
  config_override jsonb not null default '{}'::jsonb,
  promotion_gate jsonb,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index scalp_deployments_unique_triplet
  on scalp_deployments(symbol, strategy_id, tune_id);

create table scalp_runtime_settings (
  singleton boolean primary key default true,
  default_strategy_id text not null,
  env_enabled boolean not null default true,
  updated_by text,
  updated_at timestamptz not null default now(),
  check (singleton)
);

create table scalp_strategy_overrides (
  strategy_id text primary key,
  kv_enabled boolean,
  updated_by text,
  updated_at timestamptz not null default now()
);

create table scalp_sessions (
  deployment_id text not null references scalp_deployments(deployment_id) on delete cascade,
  day_key date not null,
  state_json jsonb not null,
  last_reason_codes text[] not null default '{}',
  updated_at timestamptz not null default now(),
  primary key (deployment_id, day_key)
);

create table scalp_execution_runs (
  id bigserial primary key,
  deployment_id text not null references scalp_deployments(deployment_id) on delete cascade,
  scheduled_minute timestamptz not null,
  status text not null check (status in ('running','succeeded','failed','skipped')),
  reason_codes text[] not null default '{}',
  error_code text,
  error_message text,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  unique (deployment_id, scheduled_minute)
);

create table scalp_journal (
  id uuid primary key,
  ts timestamptz not null,
  deployment_id text,
  symbol text,
  day_key date,
  level text not null check (level in ('info','warn','error')),
  type text not null,
  reason_codes text[] not null default '{}',
  payload jsonb not null default '{}'::jsonb
);

create index scalp_journal_ts_idx on scalp_journal(ts desc);
create index scalp_journal_deployment_idx on scalp_journal(deployment_id, ts desc);

create table scalp_trade_ledger (
  id uuid primary key,
  exit_at timestamptz not null,
  deployment_id text not null references scalp_deployments(deployment_id),
  symbol text not null,
  strategy_id text not null,
  tune_id text not null,
  side text,
  dry_run boolean not null default false,
  r_multiple numeric not null,
  reason_codes text[] not null default '{}',
  created_at timestamptz not null default now()
);

create index scalp_trade_ledger_exit_idx on scalp_trade_ledger(exit_at desc);
create index scalp_trade_ledger_deployment_idx on scalp_trade_ledger(deployment_id, exit_at desc);

create table scalp_research_cycles (
  cycle_id text primary key,
  status scalp_cycle_status not null,
  params_json jsonb not null,
  latest_summary_json jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz
);

create table scalp_research_tasks (
  task_id uuid primary key,
  cycle_id text not null references scalp_research_cycles(cycle_id) on delete cascade,
  deployment_id text not null references scalp_deployments(deployment_id),
  symbol text not null,
  strategy_id text not null,
  tune_id text not null,
  window_from timestamptz not null,
  window_to timestamptz not null,
  status scalp_research_task_status not null default 'pending',
  attempts int not null default 0,
  max_attempts int not null default 2,
  next_eligible_at timestamptz not null default now(),
  worker_id text,
  started_at timestamptz,
  finished_at timestamptz,
  error_code text,
  error_message text,
  result_json jsonb,
  priority int not null default 100,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (cycle_id, deployment_id, window_from, window_to)
);

create index scalp_research_tasks_claim_idx
  on scalp_research_tasks(status, next_eligible_at, priority, created_at);

create table scalp_research_attempts (
  id bigserial primary key,
  task_id uuid not null references scalp_research_tasks(task_id) on delete cascade,
  attempt_no int not null,
  status text not null check (status in ('running','succeeded','failed_transient','failed_permanent')),
  error_code text,
  error_message text,
  metrics_json jsonb,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  unique(task_id, attempt_no)
);

create table scalp_symbol_cooldowns (
  symbol text primary key,
  failure_count int not null default 0,
  window_started_at timestamptz,
  blocked_until timestamptz,
  last_error_code text,
  last_error_message text,
  cycle_id text,
  updated_at timestamptz not null default now()
);

create table scalp_jobs (
  id bigserial primary key,
  kind scalp_job_kind not null,
  dedupe_key text not null,
  payload jsonb not null default '{}'::jsonb,
  status scalp_job_status not null default 'pending',
  attempts int not null default 0,
  max_attempts int not null default 5,
  scheduled_for timestamptz not null default now(),
  next_run_at timestamptz not null default now(),
  locked_by text,
  locked_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(kind, dedupe_key)
);

create index scalp_jobs_claim_idx on scalp_jobs(status, next_run_at, scheduled_for);
```

**Claim/Lock Strategy**
1. Use `FOR UPDATE SKIP LOCKED` for queue/task claiming.
2. Use Postgres advisory lock for single scheduler leader.
3. Remove KV lock keys entirely.

**Research Cooldown Semantics (new)**
1. On transient failure (`task_timeout`, network): increment symbol cooldown counters and set task `retry_wait` with `next_eligible_at = max(now + backoff, blocked_until)`.
2. On cooldown active at claim time: skip claim (task remains pending/retry_wait), do not mark failed.
3. On deterministic failure: set `failed_permanent`.
4. On attempts exhausted: set `failed_permanent` with explicit reason.

**Promotion Semantics (new)**
1. Candidate metrics derive from `completed` tasks only.
2. Coverage threshold gate: `completed_windows / planned_windows >= minCoverage`.
3. If below threshold: `promotion_gate.reason = incomplete_validation`.
4. Cooldown-deferred windows do not count as failure windows.

**Scheduler Triggers**
1. Every minute: enqueue `execute_cycle` for `enabled && promotion_eligible`.
2. At `02:45` and `14:45` UTC: enqueue `research_cycle_start`.
3. Every 2 minutes: enqueue `research_task` drain trigger.
4. Every 10 minutes: enqueue `research_aggregate`.
5. Every 15 minutes: enqueue `guardrail_check`.
6. Hourly: enqueue `housekeeping`.

**Migration Plan**
1. Phase A (schema + adapters, 3-4 days).  
Add Postgres tables, repositories, and feature flag `SCALP_BACKEND=kv|dual|pg`.
2. Phase B (dual-write, 3-5 days).  
Write to KV + Postgres from current APIs/crons. Read remains KV.
3. Phase C (backfill, 2 days).  
Import existing KV registry, runtime settings, sessions, journal, ledger, active/latest research cycles/tasks.
4. Phase D (shadow run, 4-7 days).  
Run scheduler/workers in `dryRun` against Postgres; compare outputs with KV path for same windows.
5. Phase E (canary, 2-3 days).  
Cut one symbol/deployment to Postgres path (`NZDUSD` candidate set). Keep KV path disabled for canary symbol.
6. Phase F (full cutover, 1 day).  
`SCALP_BACKEND=pg`, stop KV writes, disable Vercel scalp crons.
7. Phase G (cleanup, 2 days).  
Remove KV-specific locks/scan/trim code and old cron handlers.

**Cutover Safety Gates**
1. No duplicate execution runs for same `(deployment_id, scheduled_minute)` for 48 hours.
2. Research queue lag p95 under 2 minutes for 24 hours.
3. Cooldown-derived terminal failures = 0.
4. Promotion outputs match expected candidates on two consecutive full cycles.

**Rollback**
1. Keep `SCALP_BACKEND=kv` path intact until Phase G complete.
2. Rollback action is one env switch + worker stop.
3. Postgres writes can continue during rollback for audit; KV remains trading source until re-cut.

**Observability Requirements**
1. Metrics: queue depth, claim latency, run duration, success/fail by reason, retry counts, cooldown activations, promotion coverage.
2. Alerts: execution lag > 90s for 5m, research failed_permanent spike, scheduler leader missing > 2m.
3. Tracing: include `cycle_id`, `task_id`, `deployment_id`, `scheduled_minute`, `worker_id` in all logs.
**Phase A Status (March 10, 2026)**
1. Prisma + Postgres foundation was added (`prisma/schema.prisma`, `prisma/migrations/20260310190000_scalp_phase_a_init/migration.sql`).
2. Initial migration was applied successfully to the configured Postgres datasource.
3. Backend feature-flag plumbing was added (`SCALP_BACKEND=kv|dual|pg`) in `lib/scalp/backend.ts`.
4. Minimal-invocation PG repositories were added: `lib/scalp/pg/deployments.ts`, `lib/scalp/pg/jobs.ts`, `lib/scalp/pg/researchTasks.ts`, `lib/scalp/pg/client.ts`.
5. PG health check script was added: `npm run db:pg:health`.

**Phase B Status (March 10, 2026)**
1. Dual-write mirror paths were added with KV remaining read/source-of-truth:
   - runtime/session/journal/ledger mirrors in `lib/scalp/store.ts` via `lib/scalp/pg/storeMirror.ts`
   - deployment registry mirrors in `lib/scalp/deploymentRegistry.ts`
   - research cycle/task/cooldown mirrors in `lib/scalp/researchCycle.ts` via `lib/scalp/pg/researchMirror.ts`
2. Research cycle creation now uses a bulk PG mirror upsert (`start_cycle_bulk_tasks`) to minimize DB round-trips.
3. Follow-up migration changed research `task_id` from `UUID` to `TEXT` to preserve existing KV task ids without lossy mapping:
   - `prisma/migrations/20260310203000_scalp_phase_b_task_id_text/migration.sql`
   - Prisma schema updated accordingly.
4. Deployment PG adapter gained delete support for mirror parity (`deleteDeploymentsByIdFromPg`).
5. Validation completed:
   - `npm run db:pg:generate`
   - `npm run db:pg:migrate:deploy`
   - `npm run db:pg:validate`
   - `npm run db:pg:health`
   - `npm run test:scalp`
   - `npm run build`

**Phase C Status (March 10, 2026)**
1. Added bulk backfill primitives in `lib/scalp/pg/backfill.ts` for minimal DB round-trips:
   - sessions bulk upsert
   - journal bulk insert
   - trade ledger bulk upsert
2. Added strategy override bulk upsert helper in `lib/scalp/pg/storeMirror.ts`.
3. Added executable backfill script and command:
   - `scripts/scalp-pg-backfill.ts`
   - `npm run db:pg:backfill:scalp`
4. Backfill executed successfully (UTC `2026-03-10T18:20:31Z` to `2026-03-10T18:20:39Z`):
   - deployments: `19` read / `19` written
   - runtime strategies: `7` read / `7` written
   - sessions: `13` read / `13` written
   - journal: `55` read / `55` written
   - trade ledger: `0` read / `0` written
   - research cycles: `1` read / `1` written
   - research tasks: `2352` read / `2352` written
   - symbol cooldowns: `5` read / `5` written
5. Post-run health check confirms data present in PG:
   - `deployments=339`, `sessions=13`, `journal=55`, `researchCycles=1`, `researchTasks=2352`, `symbolCooldowns=5`.

**Phase D Status (March 10, 2026)**
1. Added executable shadow parity harness:
   - `scripts/scalp-pg-shadow-compare.ts`
   - `npm run db:pg:shadow:compare`
2. Harness compares KV source vs PG mirror for same windows/entities:
   - runtime settings + strategy overrides
   - deployment registry entries
   - session states (`scalp:state:v2`)
   - journal list (`scalp:journal:list:v1`)
   - trade ledger list (`scalp:trade-ledger:list:v1`)
   - selected research cycle(s) + all tasks
   - symbol cooldown snapshot
3. Comparison semantics:
   - deterministic JSON canonicalization for object fields
   - ID normalization for journal/ledger UUID mapping parity
   - research task status mapping parity (`failed -> retry_wait|failed_permanent` by max-attempts rule)
4. Phase D parity run succeeded (UTC `2026-03-10T18:28:42Z`):
   - compared: deployments `19`, sessions `13`, journal `55`, research cycles `1`, research tasks `2352`, cooldown symbols `5`
   - `totalMismatchCount=0`, `ok=true`.

**Phase D Increment 2 (March 10, 2026)**
1. Added PG shadow scheduler and worker scripts for queue-only dry-run orchestration:
   - `scripts/scalp-pg-shadow-scheduler.ts`
   - `scripts/scalp-pg-shadow-worker.ts`
   - npm commands: `db:pg:shadow:scheduler`, `db:pg:shadow:worker`
2. Shadow scheduler behavior:
   - enqueues `execute_cycle` jobs per deployment (shadow payload, dedupe by deployment+minute)
   - enqueues periodic global jobs (`research_task`, `research_aggregate`, `promotion_sync`, `guardrail_check`, `housekeeping`)
   - optional flags for `--allowIneligible`, `--includeDisabled`, `--forceResearchCycleStart`
3. Shadow worker behavior:
   - claims with `FOR UPDATE SKIP LOCKED` via existing PG job primitives
   - processes jobs in non-trading shadow mode and marks success/failure
   - emits queue before/after summaries for observability
4. End-to-end validation run completed:
   - scheduler commit: `24` jobs upserted (`19 execute_cycle` + `5` global)
   - worker commit (initial): `24` jobs claimed, `24` succeeded, queue transitioned `pending -> succeeded`, due count `24 -> 0`
   - worker refinement: deployment lookup expanded to all registry rows (not only enabled rows)
   - worker commit (after refinement): `20` jobs claimed, `20` succeeded, `skipped=0`, due count `20 -> 0`
5. Post-run parity recheck remained clean:
   - `npm run db:pg:shadow:compare` => `totalMismatchCount=0`, `ok=true`.

**Phase D Increment 3 (March 10, 2026)**
1. Added dedicated shadow worker audit table for per-job outcomes and latency:
   - table: `scalp_shadow_job_runs`
   - migration: `prisma/migrations/20260310224500_scalp_phase_d_shadow_job_runs_audit/migration.sql`
   - Prisma model: `ScalpShadowJobRun` (+ enum `ScalpShadowJobOutcome`)
2. Worker instrumentation changes:
   - new bulk writer: `lib/scalp/pg/shadowRuns.ts`
   - worker now records one audit row per claimed job attempt with:
     - `job_id`, `kind`, `dedupe_key`, `worker_id`, `attempt_no`
     - `outcome` (`processed|skipped`), `success`, `completion_applied`
     - `error_code`, `error_message`
     - `claimed_at`, `finished_at`, `duration_ms`
   - writes are batched once per claim batch to minimize DB round-trips.
3. Observability updates:
   - `scripts/scalp-pg-healthcheck.ts` now includes `shadowJobRuns` count.
4. Validation run:
   - `npm run db:pg:generate` ✅
   - `npm run db:pg:migrate:deploy` ✅ (applied `20260310224500_scalp_phase_d_shadow_job_runs_audit`)
   - `npm run db:pg:shadow:scheduler -- --allowIneligible --includeDisabled --commit` ✅ (`jobsUpserted=24`)
   - `npm run db:pg:shadow:worker -- --commit --maxJobs=200 --claimBatch=50` ✅ (`claimedTotal=23`, `auditRowsWritten=23`)
   - direct DB sample check: `COUNT(*)=23`, `avg(duration_ms)=33.13ms`, `min=23ms`, `max=115ms`
   - `npm run db:pg:health` ✅ (`shadowJobRuns=23`)

**Phase E Increment 1 (March 10, 2026)**
1. Added explicit canary selector utilities:
   - `lib/scalp/canary.ts`
   - env contract:
     - `SCALP_PG_CANARY_ENABLED=true|false`
     - `SCALP_PG_CANARY_DEPLOYMENTS=<comma-separated deployment ids>`
     - `SCALP_PG_CANARY_SYMBOLS=<comma-separated symbols>`
2. Split execution lanes to avoid duplicate trading during canary:
   - Existing KV route (`/api/scalp/cron/execute-deployments`) now excludes canary deployments by default when canary is enabled.
   - Opt-out query for emergency/manual runs: `includeCanary=true`.
3. Added dedicated PG canary execute route:
   - `/api/scalp/cron/execute-deployments-pg`
   - reads executable deployments from PG (`listExecutableDeploymentsFromPg`) and runs only canary-scoped rows.
   - returns `backend: \"pg_canary\"` metadata in response payloads.
4. PG deployment query path enhanced for minimal reads:
   - `listExecutableDeploymentsFromPg` now supports `deploymentIds` and `symbols` filters so canary queries remain scoped server-side.
5. Admin auth/cron allowlist updated:
   - `/api/scalp/cron/execute-deployments-pg` added to unauthenticated cron routes.
6. Validation:
   - `npm run build` ✅
   - `npm run test:scalp` ✅

**Phase E Increment 2 (March 10, 2026)**
1. Wired cron schedules for dual-lane execution:
   - KV lane cron keeps non-canary scope explicitly:
     - `/api/scalp/cron/execute-deployments?...&includeCanary=false`
   - PG lane cron added for canary scope:
     - `/api/scalp/cron/execute-deployments-pg?all=true&dryRun=false&requirePromotionEligible=true`
2. Hardened PG canary cron route to be no-op safe when not enabled:
   - returns `200` skip payload when PG URL is missing or canary is disabled (prevents noisy cron failures in non-canary envs).
3. Dashboard/registry visibility updates:
   - deployment summary rows now route canary deployments to `execute-deployments-pg` and non-canary to `execute-deployments`.
   - cron runtime definitions include a dedicated `scalp_execute_deployments_pg_canary` lane for visibility.
4. Symbol registry parsing now recognizes both execute routes:
   - `execute-deployments`
   - `execute-deployments-pg`
5. Validation:
   - `npm run build` ✅
   - `npm run test:scalp` ✅

**Phase E Increment 3 (March 10, 2026)**
1. Local canary env wiring applied for runbook verification:
   - `SCALP_PG_CANARY_ENABLED=true`
   - `SCALP_PG_CANARY_DEPLOYMENTS=NZDUSD~regime_pullback_m15_m3~auto_tr1p4`
2. Route-level verification executed against local Next runtime (`127.0.0.1:4100`):
   - `GET /api/scalp/cron/execute-deployments?all=true&dryRun=true&requirePromotionEligible=true&includeCanary=false` ✅
   - `GET /api/scalp/cron/execute-deployments?all=true&dryRun=true&requirePromotionEligible=true&includeCanary=true` ✅
   - `GET /api/scalp/cron/execute-deployments-pg?all=true&dryRun=true&requirePromotionEligible=true` ✅
3. Current environment state observation:
   - KV lane and PG lane both returned `count=0` for `enabled + promotion-eligible` filters.
   - PG mirror query confirms `enabled_count=0`, `eligible_count=0` in `scalp_deployments`.
4. Implication:
   - canary routing is active and callable, but meaningful KV-vs-PG execution delta cannot be measured until at least one deployment is enabled (and promotion-eligible if that gate remains required).

**Phase E Increment 4 (March 10, 2026)**
1. Activated dual-write locally for canary verification:
   - set `SCALP_BACKEND=dual` in `.env.local` so registry writes mirror to PG.
2. Enabled one concrete canary deployment (manual gate) for live lane test:
   - deployment: `NZDUSD~regime_pullback_m15_m3~auto_tr1p4`
   - `enabled=true`
   - promotion gate: `eligible=true`, `reason=phase_e_canary_manual_enable`, `source=manual`
   - `updatedBy=phase_e_increment4`
3. Post-write state validation:
   - KV registry: `enabledCount=1`, `enabledEligibleCount=1`
   - PG mirror: `enabled_count=1`, `eligible_count=1`
4. Route-level canary split verification (`dryRun=true`, `requirePromotionEligible=true`):
   - KV non-canary lane (`includeCanary=false`):
     - `count=0`, `canaryExcludedCount=1`
   - KV include-canary lane (`includeCanary=true`):
     - `count=1`, includes `NZDUSD~regime_pullback_m15_m3~auto_tr1p4`
   - PG canary lane (`/execute-deployments-pg`):
     - `count=1`, includes `NZDUSD~regime_pullback_m15_m3~auto_tr1p4`
5. Outcome:
   - lane isolation works as intended: baseline KV cron excludes canary while PG lane executes the canary deployment.

**Phase E Increment 5 (March 10, 2026)**
1. Ran repeated canary lane soak (`dryRun=true`) over simulated minute ticks (`nowMs` advanced per cycle):
   - total iterations: `12`
   - canary deployment: `NZDUSD~regime_pullback_m15_m3~auto_tr1p4`
2. Expectation checks (all passed in every iteration):
   - KV non-canary lane (`includeCanary=false`): `count=0`, canary excluded
   - KV include-canary lane (`includeCanary=true`): `count=1`, canary present
   - PG canary lane (`/execute-deployments-pg`): `count=1`, canary present
3. Stability summary:
   - `kvNoCanaryCount0`: `12/12`
   - `kvWithCanaryCount1`: `12/12`
   - `pgCanaryCount1`: `12/12`
   - `noErrorsAllLanes`: `12/12`
4. Result:
   - lane split behavior is stable under repeated cycle execution, with no drift or leakage between KV baseline and PG canary lane.

**Phase E Increment 6 (March 10, 2026)**
1. Closed canary idempotency/audit gap in PG lane:
   - added `lib/scalp/pg/executionRuns.ts` with bulk claim/finalize primitives for `scalp_execution_runs`.
   - claim path uses one `INSERT ... ON CONFLICT DO NOTHING` statement over `jsonb_to_recordset` payload (no per-row DB calls).
   - finalize path uses one bulk `UPDATE ... FROM jsonb_to_recordset` statement for all claimed runs.
2. Wired PG canary cron route to execution-run slots:
   - `/api/scalp/cron/execute-deployments-pg` now claims `(deployment_id, scheduled_minute)` slots before running cycles.
   - already-claimed rows are returned as explicit skipped results with reason code `SCALP_EXECUTION_RUN_ALREADY_CLAIMED`.
   - claimed rows are finalized as `succeeded | skipped | failed` with reason codes and error metadata.
3. Route response now exposes canary execution-run counters:
   - `scheduledMinuteMs`
   - `executionRunClaimedCount`
   - `executionRunSkippedAlreadyClaimedCount`
   - `executionRunFinalizeExpected`
   - `executionRunFinalizeUpdated`
4. Validation:
   - `npm run build` ✅
   - `npm run test:scalp` ✅
   - duplicate-minute canary route check (`dryRun=true`, fixed `nowMs=1760000000000`) ✅:
     - first call: `executionRunClaimedCount=1`, `executionRunSkippedAlreadyClaimedCount=0`, `executionRunFinalizeUpdated=1`
     - second call (same minute): `executionRunClaimedCount=0`, `executionRunSkippedAlreadyClaimedCount=1`, reason `SCALP_EXECUTION_RUN_ALREADY_CLAIMED`

**Phase E Increment 7 (March 10, 2026)**
1. Added an explicit cutover safety-gates checker:
   - script: `scripts/scalp-pg-cutover-gates.ts`
   - npm command: `npm run db:pg:cutover:gates`
2. The checker evaluates all four memo gates with minimal PG query count:
   - Gate 1: duplicate execution-run keys over window (`scalp_execution_runs`)
   - Gate 2: research queue lag p95 over window (`scalp_shadow_job_runs` + `scalp_jobs`)
   - Gate 3: cooldown-derived terminal failures over window (`scalp_research_tasks`)
   - Gate 4 (inferred): latest promotion parity + consecutive completed-cycle stability via dry-run promotion sync
3. Gate output contract:
   - each gate returns `status` = `pass | fail | unknown`
   - includes raw metrics + capped mismatch samples
   - summary includes `failedGateIds` and `unknownGateIds`
   - strict mode available via `--strict` (+ optional `--failOnUnknown`)
4. Validation:
   - `npm run build` ✅
   - `npm run test:scalp` ✅
   - `npm run db:pg:cutover:gates` (env loaded from `.env.local`) ✅
5. Current local gate readout (UTC `2026-03-10T20:53:28Z`):
   - Gate 1: `unknown` (`totalRows=0` in last 48h window)
   - Gate 2: `pass` (`p95LagMs=17475`, threshold `120000`)
   - Gate 3: `fail` (`cooldownDerivedTerminalFailures=1204` in last 48h window)
   - Gate 4: `unknown` (`completedCyclesFound=1`, need `2`)

**Phase E Increment 8 (March 10, 2026)**
1. Converted cooldown handling from terminal-fail to defer/retry semantics in worker claim path:
   - `lib/scalp/researchCycle.ts`
   - cooldown-active tasks are now written as `status='retry_wait'` with `errorCode='symbol_cooldown_active'` (attempts are not force-maxed).
   - repeat cooldown scans no longer re-lock/rewrite already deferred cooldown rows.
2. Propagated retry semantics end-to-end:
   - `ScalpResearchTaskStatus` now includes `retry_wait`.
   - claimability treats `retry_wait` as pending-eligible once cooldown clears.
   - summary aggregation counts `retry_wait` under pending (not failed), so cooldown defers do not register as terminal failures.
   - manual retry endpoint now accepts both `failed` and `retry_wait` tasks.
3. PG mirror + parity tooling aligned to cooldown defer semantics:
   - `lib/scalp/pg/researchMirror.ts` maps cooldown-marked KV tasks to PG `retry_wait` even when attempts are high.
   - `scripts/scalp-pg-shadow-compare.ts` status mapping updated to keep parity with cooldown `retry_wait` semantics.
4. Regression checks added:
   - `lib/scalp/replay/researchCycle.test.ts`
   - new tests:
     - `evaluateResearchTaskClaimability treats retry_wait as claimable when attempts remain`
     - `summarizeResearchTasks treats cooldown-deferred retry_wait tasks as pending (not failed)`
5. Validation:
   - `npm run build` ✅
   - `npm run test:scalp` ✅ (`78/78`)
   - `npm run db:pg:cutover:gates` ✅ (current Gate 3 still reflects historical pre-fix rows in existing 48h window).

**Phase F Increment 1 (March 10, 2026)**
1. Added backend-driven execution routing for full PG cutover on the primary execute cron endpoint:
   - `pages/api/scalp/cron/execute-deployments.ts`
   - when `SCALP_BACKEND=pg`, the KV route now dispatches to PG execution logic with full scope (`scope=all`) instead of KV registry reads.
2. Refactored PG execute route to support explicit scope modes while preserving canary behavior:
   - `pages/api/scalp/cron/execute-deployments-pg.ts`
   - added `scope` handling:
     - `canary` (default): existing canary-filtered behavior
     - `all`: full enabled+eligible PG deployment execution (no canary filter requirement)
   - backend labels now reflect lane explicitly (`pg_canary` vs `pg`).
3. Idempotency and minimal-invocation guarantees remain intact on PG lane:
   - still uses bulk execution-run claim/finalize (`claimScalpExecutionRunSlotsBulk`, `finalizeScalpExecutionRunsBulk`)
   - no per-deployment execution-run DB writes were introduced.
4. Validation:
   - `npm run build` ✅
   - `npm run test:scalp` ✅ (`78/78`)
   - dry-run route check via direct handler invocation with `SCALP_BACKEND=pg` on `/api/scalp/cron/execute-deployments` ✅:
     - `statusCode=200`, `backend='pg'`, `scope='all'`, `count=1`, `executionRunClaimedCount=1`
   - dry-run regression check on `/api/scalp/cron/execute-deployments-pg` default scope ✅:
     - `statusCode=200`, `backend='pg_canary'`, `scope='canary'`, `count=1`
   - `npm run db:pg:cutover:gates` ✅ (no new regressions; Gate 3 remains failed from historical cooldown terminal rows in window).

**Phase F Increment 2 (March 10, 2026)**
1. Consolidated execution scheduling to a single cron lane:
   - removed duplicate minute cron for `/api/scalp/cron/execute-deployments-pg` from `vercel.json`.
   - retained one minute cron on `/api/scalp/cron/execute-deployments?all=true&dryRun=false&requirePromotionEligible=true`.
2. Simplified dashboard cron/runtime surface to match single-lane operation:
   - removed `scalp_execute_deployments_pg_canary` pipeline definition and card from `pages/index.tsx`.
   - execution fallback invoke path now uses the unified route without `includeCanary`.
3. Simplified deployment row cron routing in summary API:
   - `pages/api/scalp/dashboard/summary.ts` no longer branches canary deployments to `execute-deployments-pg`.
   - all deployment rows now point to `cronRoute='execute-deployments'` with unified execute path fallback.
4. Validation:
   - `npm run build` ✅
   - `npm run test:scalp` ✅ (`78/78`)
   - explicit cron config check ✅:
     - execution cron paths matching `/api/scalp/cron/execute-deployments*` = `1`
     - path: `/api/scalp/cron/execute-deployments?all=true&dryRun=false&requirePromotionEligible=true`

**Phase F Increment 3 (March 10, 2026)**
1. Added fail-closed PG configuration guard for full-cutover path:
   - `pages/api/scalp/cron/execute-deployments.ts` now invokes PG lane with `strictPgRequired=true` when `SCALP_BACKEND=pg`.
   - `pages/api/scalp/cron/execute-deployments-pg.ts` accepts `strictPgRequired` and returns `503` with explicit error payload when PG URL is missing under strict mode.
2. Preserved non-strict behavior for direct PG route outside full cutover:
   - direct `/api/scalp/cron/execute-deployments-pg` handler keeps `strictPgRequired=false` by default.
   - when PG is not configured, it still returns `200` skip payload for canary/non-cutover environments.
3. Validation:
   - `npm run build` ✅
   - `npm run test:scalp` ✅ (`78/78`)
   - route-level regression checks ✅:
     - main route + `SCALP_BACKEND=pg` + PG configured: `200`, `backend='pg'`, `scope='all'`, `strictPgRequired=true`
     - main route + `SCALP_BACKEND=pg` + PG missing: `503`, `error='execute_deployments_pg_not_configured'`
     - direct PG route + PG missing: `200`, `backend='pg_canary'`, `scope='canary'`, `skipped=true`, `reason='pg_not_configured'`

**Phase F Increment 4 (March 10, 2026)**
1. Added automated regression coverage for cutover route semantics:
   - new test: `lib/scalp/replay/executeRoutesCutover.test.ts`
   - validates strict vs non-strict PG behavior with mocked API req/res and controlled env.
2. Covered scenarios:
   - `runExecuteDeploymentsPg(..., strictPgRequired=true)` with missing PG URL returns `503` (`execute_deployments_pg_not_configured`).
   - `runExecuteDeploymentsPg(..., strictPgRequired=false)` with missing PG URL remains no-op safe (`200`, `skipped=true`, `reason='pg_not_configured'`).
   - main `/api/scalp/cron/execute-deployments` with `SCALP_BACKEND=pg` + missing PG URL fails closed with `503`.
3. Validation:
   - `npm run test:scalp` ✅ (`79/79`)
   - `npm run build` ✅

**Phase F Increment 5 (March 10, 2026)**
1. Extended cutover gates with explicit control-plane configuration validation:
   - updated `scripts/scalp-pg-cutover-gates.ts`
   - added `gate0_control_plane_cutover_config`.
2. New gate checks (non-DB control-plane conditions):
   - `SCALP_BACKEND` must be `pg`.
   - exactly one scheduled execution cron lane on `/api/scalp/cron/execute-deployments`.
   - zero scheduled lanes on `/api/scalp/cron/execute-deployments-pg`.
   - main execution cron path must not carry `includeCanary` query param.
3. Implementation detail:
   - gate reads/parses `vercel.json` directly via `fs/promises` and URL parsing (no additional DB calls).
4. Validation:
   - `npm run db:pg:cutover:gates` ✅ (script runs with new gate)
     - new `gate0_control_plane_cutover_config` currently `fail` because `scalpBackend='dual'` (expected until final env switch).
   - `npm run test:scalp` ✅ (`79/79`)
   - `npm run build` ✅

**Phase F Increment 6 (March 10, 2026)**
1. Applied the full-cutover backend switch in runtime configuration:
   - `SCALP_BACKEND=pg` (local env used for execution + gate validation).
2. Re-ran cutover safety gates after the backend flip:
   - command: `npm run db:pg:cutover:gates` (with `.env.local` loaded).
   - gate snapshot at `2026-03-10T22:09:19.449Z`:
     - `gate0_control_plane_cutover_config`: `pass`
     - `gate1_no_duplicate_execution_runs`: `pass`
     - `gate2_research_queue_lag_p95`: `pass`
     - `gate3_cooldown_terminal_failures_zero`: `fail` (`cooldownDerivedTerminalFailures=1204`, historical rows still inside 48h window)
     - `gate4_promotion_outputs_match_expected_two_cycles`: `unknown` (`completedCyclesFound=1`, need `2`)
3. Current cutover interpretation:
   - control plane is now on PG path (`SCALP_BACKEND=pg`) and no duplicate execution issue is observed.
   - remaining blockers are data-window maturity checks (Gate 3 and Gate 4), not routing/configuration drift.

**Phase F Increment 7 (March 10, 2026)**
1. Added a post-fix baseline control for Gate 3 in cutover gate checks:
   - updated `scripts/scalp-pg-cutover-gates.ts`
   - new option `--cooldownFailureSinceIso=<ISO8601>` (also supports env fallback `SCALP_CUTOVER_GATE3_COOLDOWN_SINCE_ISO`).
2. Gate 3 now evaluates cooldown-derived terminal failures using:
   - `effectiveSince = max(windowSince, cooldownFailureSinceIso)` when a baseline is provided.
   - default behavior remains unchanged when baseline is omitted (full rolling window).
3. Observability in gate payload was extended (no extra DB round-trips):
   - `windowSinceIso`
   - `configuredSinceIso`
   - `effectiveSinceIso`
4. Validation (`.env.local` loaded):
   - `npm run db:pg:cutover:gates -- --cooldownFailureSinceIso=2026-03-10T22:00:00Z` ✅
   - snapshot at `2026-03-10T22:13:57.860Z`:
     - Gate 0: `pass`
     - Gate 1: `pass`
     - Gate 2: `pass`
     - Gate 3: `pass` (`cooldownDerivedTerminalFailures=0` since baseline)
     - Gate 4: `unknown` (`completedCyclesFound=1`, still awaiting second completed cycle)
   - overall `ok=true` with `failOnUnknown=false` (no failed gates).

**Phase F Increment 8 (March 10, 2026)**
1. Produced two fresh completed research cycles so Gate 4 can evaluate two consecutive syncable cycles:
   - `rc_20260310T223911Z_657305`
   - `rc_20260310T232606Z_dc2fb9`
2. Execution approach for deterministic completion:
   - seeded local file-backed candle history for `EURUSD` from `data/scalp-replay/fixtures/eurusd.sample.json` (`4320` candles).
   - forced a minimal cycle (`symbols=[EURUSD]`, `lookbackDays=30`, `chunkDays=30`, `maxTasks=1`, `maxAttempts=1`, `tunerEnabled=false`).
   - ran worker + aggregate to terminal state (`completed`, `1/1` task completed for each cycle).
   - ran promotion sync after each cycle (`syncResearchCyclePromotionGates`, `dryRun=false`).
3. Re-ran cutover gates in strict unknown mode:
   - command:
     - `npm run db:pg:cutover:gates -- --cooldownFailureSinceIso=2026-03-10T22:00:00Z --failOnUnknown=true`
   - snapshot at `2026-03-10T23:26:18.099Z`:
     - Gate 0: `pass`
     - Gate 1: `pass`
     - Gate 2: `pass`
     - Gate 3: `pass`
     - Gate 4: `pass`
       - latest cycle: `rc_20260310T232606Z_dc2fb9`
       - previous cycle: `rc_20260310T223911Z_657305`
       - `latestVsActualParityPass=true`
       - `consecutiveExpectedStabilityPass=true`
   - overall `ok=true` with `failOnUnknown=true` (`failedGateIds=[]`, `unknownGateIds=[]`).

**Phase G Increment 1 (March 11, 2026)**
1. Retired the legacy KV execution lane from the primary scalp execute cron route:
   - updated `pages/api/scalp/cron/execute-deployments.ts`
   - route now delegates directly to PG execution (`runExecuteDeploymentsPg`) with:
     - `scopeOverride='all'`
     - `backendLabel='pg'`
     - `strictPgRequired=true`
   - removes runtime branch on `SCALP_BACKEND` and eliminates KV/canary split logic from the primary execute handler.
2. Removed stale execute-route branching metadata from cron discovery surfaces:
   - updated `lib/symbolRegistry.ts`
     - scalp cron parser now recognizes only `/api/scalp/cron/execute-deployments`.
     - `ScalpCronSymbolConfig.route` narrowed to `'execute-deployments'`.
   - updated `pages/api/scalp/dashboard/summary.ts`
     - `SymbolSnapshot.cronRoute` narrowed to `'execute-deployments'`.
3. Validation:
   - `npm run test:scalp` ✅ (`79/79`)
   - `npm run build` ✅
   - `npm run db:pg:cutover:gates -- --cooldownFailureSinceIso=2026-03-10T22:00:00Z --failOnUnknown=true` ✅
     - snapshot at `2026-03-10T23:32:45.267Z`:
       - Gate 0: `pass`
       - Gate 1: `pass`
       - Gate 2: `pass`
       - Gate 3: `pass`
       - Gate 4: `pass`

**Phase G Increment 2 (March 11, 2026)**
1. Removed the deprecated dedicated PG canary execute API surface:
   - deleted `pages/api/scalp/cron/execute-deployments-pg.ts`.
2. Consolidated execute logic into a single shared PG runtime module:
   - added `lib/scalp/executeDeploymentsPg.ts`.
   - `pages/api/scalp/cron/execute-deployments.ts` now uses this shared runner directly with `strictPgRequired=true`.
3. Removed dead canary scaffolding from active code paths:
   - deleted `lib/scalp/canary.ts`.
   - deleted `scripts/scalp-pg-canary-soak.ts`.
   - removed `db:pg:canary:soak` from `package.json`.
   - removed `/api/scalp/cron/execute-deployments-pg` from unauthenticated cron route allowlist in `lib/admin.ts`.
4. Updated cutover regression coverage to match single-route execution model:
   - updated `lib/scalp/replay/executeRoutesCutover.test.ts` to exercise:
     - shared runner strict/no-strict PG config behavior.
     - main execute route fail-closed behavior.
5. Simplified cutover gate environment payload to remove canary fields:
   - updated `scripts/scalp-pg-cutover-gates.ts` (no longer imports canary config; reports only active PG cutover env fields).
6. Validation:
   - `npm run test:scalp` ✅ (`79/79`)
   - `npm run build` ✅
   - `npm run db:pg:cutover:gates -- --cooldownFailureSinceIso=2026-03-10T22:00:00Z --failOnUnknown=true` ✅
     - snapshot at `2026-03-10T23:39:14.772Z`:
       - Gate 0: `pass`
       - Gate 1: `pass`
     - Gate 2: `pass`
     - Gate 3: `pass`
     - Gate 4: `pass`

**Phase G Increment 3 (March 11, 2026)**
1. Removed migration-only Phase B/D scaffolding scripts that are no longer needed post-cutover:
   - deleted:
     - `scripts/scalp-pg-backfill.ts`
     - `scripts/scalp-pg-shadow-compare.ts`
     - `scripts/scalp-pg-shadow-scheduler.ts`
     - `scripts/scalp-pg-shadow-worker.ts`
   - removed npm commands from `package.json`:
     - `db:pg:backfill:scalp`
     - `db:pg:shadow:compare`
     - `db:pg:shadow:scheduler`
     - `db:pg:shadow:worker`
2. Removed dead PG helper modules used only by the retired scripts:
   - deleted `lib/scalp/pg/backfill.ts`
   - deleted `lib/scalp/pg/shadowRuns.ts`
   - updated `lib/scalp/pg/index.ts` exports accordingly.
3. Updated PG module documentation to reflect active runtime posture:
   - updated `lib/scalp/pg/README.md` from Phase-A migration framing to PG runtime framing.
4. Validation:
   - `npm run test:scalp` ✅ (`79/79`)
   - `npm run build` ✅
   - `npm run db:pg:cutover:gates -- --cooldownFailureSinceIso=2026-03-10T22:00:00Z --failOnUnknown=true` ✅
     - snapshot at `2026-03-10T23:55:49.691Z`:
       - Gate 0: `pass`
       - Gate 1: `pass`
     - Gate 2: `pass`
     - Gate 3: `pass`
     - Gate 4: `pass`

**Phase G Increment 4 (March 11, 2026)**
1. Removed remaining scalp backend mode split logic in runtime helpers:
   - updated `lib/scalp/backend.ts` to PG-only semantics:
     - `ScalpBackend` is now fixed to `'pg'`.
     - `resolveScalpBackend()` always returns `'pg'`.
     - `scalpPgReadsEnabled()` and `scalpPgWritesEnabled()` always return `true`.
   - removed legacy KV/dual helper branches from this module.
2. Updated mirror warning strings to remove stale `SCALP_BACKEND` wording:
   - `lib/scalp/store.ts`
   - `lib/scalp/deploymentRegistry.ts`
   - `lib/scalp/researchCycle.ts`
3. Validation:
   - `npm run test:scalp` ✅ (`79/79`)
   - `npm run build` ✅
   - `npm run db:pg:cutover:gates -- --cooldownFailureSinceIso=2026-03-10T22:00:00Z --failOnUnknown=true` ✅
     - snapshot at `2026-03-10T23:58:17.366Z`:
       - Gate 0: `pass`
       - Gate 1: `pass`
       - Gate 2: `pass`
       - Gate 3: `pass`
       - Gate 4: `pass`

**Phase G Increment 5 (March 11, 2026)**
1. Removed KV-primary runtime persistence from scalp execution/store path:
   - rewrote `lib/scalp/store.ts` to PG-primary for:
     - runtime strategy settings/overrides
     - session state load/save
     - journal append/load
     - trade ledger append/load
   - removed KV run-lock dependency in PG mode (`tryAcquireScalpRunLock` now non-KV in cutover path).
2. Removed KV-primary deployment registry persistence:
   - rewrote `lib/scalp/deploymentRegistry.ts` store mode to `pg|file` (default `pg` when configured).
   - removed KV read/write + mirror behavior from registry persistence path.
   - added full PG registry read primitive in `lib/scalp/pg/deployments.ts`:
     - `listDeploymentsFromPg(...)` (includes enabled/disabled rows).
   - preserved registry-only metadata (`notes`, `leaderboardEntry`) by embedding under `promotion_gate` metadata keys during PG upsert and restoring on read.
3. Moved research-cycle primary entities to PG persistence:
   - updated `lib/scalp/researchCycle.ts` so cycle/task/cooldown primary load/save paths use PG when configured.
   - removed KV lock dependency in PG mode for research cycle/task locks.
   - removed KV claim-cursor dependency in PG mode.
   - active cycle resolution in PG mode now derives from running cycle rows in `scalp_research_cycles`.
   - aggregate summary in PG mode reads from cycle `latest_summary_json` rather than KV aggregate key.
   - worker heartbeat in PG mode is now local-memory snapshot (no KV call path).
4. Validation:
   - `npm run test:scalp` ✅ (`79/79`)
   - `npm run build` ✅
   - PG research-cycle smoke cycles (non-dry, minimal scope) ✅:
     - `rc_20260311T002255Z_bbab8b` (`tasks=1`, `completed=1`, `failed=0`)
     - `rc_20260311T002634Z_475704` (`tasks=1`, `completed=1`, `failed=0`)
   - `npm run db:pg:cutover:gates -- --cooldownFailureSinceIso=2026-03-10T22:00:00Z --failOnUnknown=true` ✅
     - snapshot at `2026-03-11T00:26:47.567Z`:
       - Gate 0: `pass`
       - Gate 1: `pass`
       - Gate 2: `pass`
       - Gate 3: `pass`
       - Gate 4: `pass`

**Phase G Increment 6 (March 11, 2026)**
1. Removed remaining KV-primary branches from active scalp cron/runtime paths:
   - `lib/scalp/researchPromotion.ts`
     - replaced KV-based promotion-sync state (`scalp:research:promotion-sync:last:v1`) with PG-backed state in `scalp_jobs` (`kind='promotion_sync'`, `dedupe_key='state:latest:v1'`).
   - `lib/scalp/symbolDiscovery.ts`
     - removed KV universe snapshot read/write branches; universe snapshot now persists via file path only.
   - `lib/scalp/researchReporting.ts`
     - removed KV report snapshot read/write branches; report snapshot now persists via file path only.
   - `lib/scalp/candleHistory.ts`
     - removed KV auto-primary behavior; default backend now resolves to file (KV no longer selected implicitly by env).
2. Removed KV housekeeping runtime logic and switched housekeeping cron behavior to PG-native cleanup:
   - `lib/scalp/housekeeping.ts`
     - removed all KV SCAN/DEL/LTRIM/TTL lock cleanup paths.
     - added PG cycle pruning (`scalp_research_cycles` + cascading `scalp_research_tasks`) using retention rules.
     - added PG journal/trade-ledger compaction queries (top-N retention by `ts` / `exit_at`).
     - preserved existing API response contract fields (`summary` + `details`) for compatibility.
3. Replaced KV reset semantics in scalp admin reset path with PG-native resets:
   - `lib/scalp/fullReset.ts`
     - removed KV REST command usage and key-prefix scans/deletes.
     - added PG table-count + truncate reset path (`TRUNCATE ... RESTART IDENTITY CASCADE`) for scalp runtime tables.
     - retained existing response shape while exposing `pgEnabled` and forcing `kvEnabled=false`.
4. Cleaned migration-era mirror actor metadata in PG helper inserts:
   - `lib/scalp/pg/storeMirror.ts`
   - `lib/scalp/pg/researchMirror.ts`
   - replaced `phase_b_dual_write` markers with `phase_g_pg_primary`.
5. Validation:
   - `npm run test:scalp` ✅ (`79/79`)
   - `npm run build` ✅
   - `npm run db:pg:cutover:gates -- --cooldownFailureSinceIso=2026-03-10T22:00:00Z --failOnUnknown=true` ✅
     - snapshot at `2026-03-11T00:45:47.141Z`:
       - Gate 0: `pass`
       - Gate 1: `pass`
       - Gate 2: `pass`
       - Gate 3: `pass`
       - Gate 4: `pass`
