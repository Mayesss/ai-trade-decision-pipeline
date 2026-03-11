-- Phase A: Postgres foundation for scalp orchestration.
-- This migration creates the durable queue/state schema while existing KV runtime remains primary.

CREATE TYPE scalp_job_kind AS ENUM (
  'execute_cycle',
  'research_task',
  'research_aggregate',
  'promotion_sync',
  'guardrail_check',
  'housekeeping'
);

CREATE TYPE scalp_job_status AS ENUM (
  'pending',
  'running',
  'retry_wait',
  'succeeded',
  'failed_permanent',
  'cancelled'
);

CREATE TYPE scalp_research_task_status AS ENUM (
  'pending',
  'running',
  'retry_wait',
  'completed',
  'failed_permanent',
  'cancelled'
);

CREATE TYPE scalp_cycle_status AS ENUM (
  'running',
  'completed',
  'failed',
  'stalled'
);

CREATE TABLE scalp_deployments (
  deployment_id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('manual', 'backtest', 'matrix')),
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  config_override JSONB NOT NULL DEFAULT '{}'::jsonb,
  promotion_gate JSONB,
  updated_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX scalp_deployments_unique_triplet
  ON scalp_deployments(symbol, strategy_id, tune_id);

CREATE INDEX scalp_deployments_enabled_idx
  ON scalp_deployments(enabled);

CREATE TABLE scalp_runtime_settings (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
  default_strategy_id TEXT NOT NULL,
  env_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  updated_by TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (singleton)
);

CREATE TABLE scalp_strategy_overrides (
  strategy_id TEXT PRIMARY KEY,
  kv_enabled BOOLEAN,
  updated_by TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE scalp_sessions (
  deployment_id TEXT NOT NULL REFERENCES scalp_deployments(deployment_id) ON DELETE CASCADE,
  day_key DATE NOT NULL,
  state_json JSONB NOT NULL,
  last_reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (deployment_id, day_key)
);

CREATE INDEX scalp_sessions_updated_at_idx
  ON scalp_sessions(updated_at DESC);

CREATE TABLE scalp_execution_runs (
  id BIGSERIAL PRIMARY KEY,
  deployment_id TEXT NOT NULL REFERENCES scalp_deployments(deployment_id) ON DELETE CASCADE,
  scheduled_minute TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
  reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  error_code TEXT,
  error_message TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  CONSTRAINT scalp_execution_runs_unique_deployment_minute
    UNIQUE (deployment_id, scheduled_minute)
);

CREATE INDEX scalp_execution_runs_scheduled_idx
  ON scalp_execution_runs(scheduled_minute DESC);

CREATE TABLE scalp_journal (
  id UUID PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  deployment_id TEXT,
  symbol TEXT,
  day_key DATE,
  level TEXT NOT NULL CHECK (level IN ('info', 'warn', 'error')),
  type TEXT NOT NULL,
  reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX scalp_journal_ts_idx
  ON scalp_journal(ts DESC);

CREATE INDEX scalp_journal_deployment_idx
  ON scalp_journal(deployment_id, ts DESC);

CREATE TABLE scalp_trade_ledger (
  id UUID PRIMARY KEY,
  exit_at TIMESTAMPTZ NOT NULL,
  deployment_id TEXT NOT NULL REFERENCES scalp_deployments(deployment_id),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  side TEXT,
  dry_run BOOLEAN NOT NULL DEFAULT FALSE,
  r_multiple NUMERIC(20, 8) NOT NULL,
  reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX scalp_trade_ledger_exit_idx
  ON scalp_trade_ledger(exit_at DESC);

CREATE INDEX scalp_trade_ledger_deployment_idx
  ON scalp_trade_ledger(deployment_id, exit_at DESC);

CREATE TABLE scalp_research_cycles (
  cycle_id TEXT PRIMARY KEY,
  status scalp_cycle_status NOT NULL,
  params_json JSONB NOT NULL,
  latest_summary_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE INDEX scalp_research_cycles_status_updated_idx
  ON scalp_research_cycles(status, updated_at DESC);

CREATE TABLE scalp_research_tasks (
  task_id UUID PRIMARY KEY,
  cycle_id TEXT NOT NULL REFERENCES scalp_research_cycles(cycle_id) ON DELETE CASCADE,
  deployment_id TEXT NOT NULL REFERENCES scalp_deployments(deployment_id),
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  tune_id TEXT NOT NULL,
  window_from TIMESTAMPTZ NOT NULL,
  window_to TIMESTAMPTZ NOT NULL,
  status scalp_research_task_status NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 2,
  next_eligible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  worker_id TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  error_code TEXT,
  error_message TEXT,
  result_json JSONB,
  priority INT NOT NULL DEFAULT 100,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_research_tasks_unique_window
    UNIQUE (cycle_id, deployment_id, window_from, window_to)
);

CREATE INDEX scalp_research_tasks_claim_idx
  ON scalp_research_tasks(status, next_eligible_at, priority, created_at);

CREATE INDEX scalp_research_tasks_cycle_status_idx
  ON scalp_research_tasks(cycle_id, status);

CREATE TABLE scalp_research_attempts (
  id BIGSERIAL PRIMARY KEY,
  task_id UUID NOT NULL REFERENCES scalp_research_tasks(task_id) ON DELETE CASCADE,
  attempt_no INT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed_transient', 'failed_permanent')),
  error_code TEXT,
  error_message TEXT,
  metrics_json JSONB,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  CONSTRAINT scalp_research_attempts_unique_task_attempt
    UNIQUE (task_id, attempt_no)
);

CREATE INDEX scalp_research_attempts_task_started_idx
  ON scalp_research_attempts(task_id, started_at DESC);

CREATE TABLE scalp_symbol_cooldowns (
  symbol TEXT PRIMARY KEY,
  failure_count INT NOT NULL DEFAULT 0,
  window_started_at TIMESTAMPTZ,
  blocked_until TIMESTAMPTZ,
  last_error_code TEXT,
  last_error_message TEXT,
  cycle_id TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX scalp_symbol_cooldowns_blocked_idx
  ON scalp_symbol_cooldowns(blocked_until);

CREATE TABLE scalp_jobs (
  id BIGSERIAL PRIMARY KEY,
  kind scalp_job_kind NOT NULL,
  dedupe_key TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status scalp_job_status NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,
  scheduled_for TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_by TEXT,
  locked_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT scalp_jobs_unique_kind_dedupe UNIQUE (kind, dedupe_key)
);

CREATE INDEX scalp_jobs_claim_idx
  ON scalp_jobs(status, next_run_at, scheduled_for);

CREATE OR REPLACE FUNCTION scalp_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER scalp_deployments_set_updated_at
  BEFORE UPDATE ON scalp_deployments
  FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();

CREATE TRIGGER scalp_runtime_settings_set_updated_at
  BEFORE UPDATE ON scalp_runtime_settings
  FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();

CREATE TRIGGER scalp_strategy_overrides_set_updated_at
  BEFORE UPDATE ON scalp_strategy_overrides
  FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();

CREATE TRIGGER scalp_sessions_set_updated_at
  BEFORE UPDATE ON scalp_sessions
  FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();

CREATE TRIGGER scalp_research_cycles_set_updated_at
  BEFORE UPDATE ON scalp_research_cycles
  FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();

CREATE TRIGGER scalp_research_tasks_set_updated_at
  BEFORE UPDATE ON scalp_research_tasks
  FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();

CREATE TRIGGER scalp_symbol_cooldowns_set_updated_at
  BEFORE UPDATE ON scalp_symbol_cooldowns
  FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();

CREATE TRIGGER scalp_jobs_set_updated_at
  BEFORE UPDATE ON scalp_jobs
  FOR EACH ROW EXECUTE FUNCTION scalp_set_updated_at();
