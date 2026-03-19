-- Scalp pipeline cutover: independent async job state

ALTER TABLE scalp_deployments
    ADD COLUMN IF NOT EXISTS in_universe boolean NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS worker_dirty boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS promotion_dirty boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS retired_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_prepared_at timestamptz;

CREATE INDEX IF NOT EXISTS scalp_deployments_in_universe_idx ON scalp_deployments(in_universe);
CREATE INDEX IF NOT EXISTS scalp_deployments_worker_dirty_idx ON scalp_deployments(worker_dirty);
CREATE INDEX IF NOT EXISTS scalp_deployments_promotion_dirty_idx ON scalp_deployments(promotion_dirty);

CREATE TABLE IF NOT EXISTS scalp_pipeline_jobs (
    job_kind text PRIMARY KEY,
    status text NOT NULL DEFAULT 'idle',
    lock_token text,
    lock_expires_at timestamptz,
    running_since timestamptz,
    next_run_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    last_run_at timestamptz,
    last_success_at timestamptz,
    last_error text,
    progress_label text,
    progress_json jsonb,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_pipeline_jobs_status_idx ON scalp_pipeline_jobs(status);
CREATE INDEX IF NOT EXISTS scalp_pipeline_jobs_next_run_idx ON scalp_pipeline_jobs(next_run_at);

CREATE TABLE IF NOT EXISTS scalp_pipeline_symbols (
    symbol text PRIMARY KEY,
    active boolean NOT NULL DEFAULT true,
    discover_status text NOT NULL DEFAULT 'pending',
    discover_attempts integer NOT NULL DEFAULT 0,
    discover_next_run_at timestamptz,
    discover_error text,
    last_discovered_at timestamptz,
    load_status text NOT NULL DEFAULT 'pending',
    load_attempts integer NOT NULL DEFAULT 0,
    load_next_run_at timestamptz,
    load_error text,
    weeks_covered integer NOT NULL DEFAULT 0,
    latest_week_start timestamptz,
    last_loaded_at timestamptz,
    prepare_status text NOT NULL DEFAULT 'pending',
    prepare_attempts integer NOT NULL DEFAULT 0,
    prepare_next_run_at timestamptz,
    prepare_error text,
    prepared_deployments integer NOT NULL DEFAULT 0,
    last_prepared_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_pipeline_symbols_active_idx ON scalp_pipeline_symbols(active);
CREATE INDEX IF NOT EXISTS scalp_pipeline_symbols_load_claim_idx ON scalp_pipeline_symbols(load_status, load_next_run_at);
CREATE INDEX IF NOT EXISTS scalp_pipeline_symbols_prepare_claim_idx ON scalp_pipeline_symbols(prepare_status, prepare_next_run_at);

CREATE TABLE IF NOT EXISTS scalp_deployment_weekly_metrics (
    id bigserial PRIMARY KEY,
    deployment_id text NOT NULL REFERENCES scalp_deployments(deployment_id) ON DELETE CASCADE,
    symbol text NOT NULL,
    strategy_id text NOT NULL,
    tune_id text NOT NULL,
    week_start timestamptz NOT NULL,
    week_end timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    next_run_at timestamptz NOT NULL DEFAULT NOW(),
    worker_id text,
    started_at timestamptz,
    finished_at timestamptz,
    error_code text,
    error_message text,
    trades integer,
    win_rate_pct numeric(10, 4),
    net_r numeric(20, 8),
    expectancy_r numeric(20, 8),
    profit_factor numeric(20, 8),
    max_drawdown_r numeric(20, 8),
    avg_hold_minutes numeric(20, 8),
    net_pnl_usd numeric(20, 8),
    gross_profit_r numeric(20, 8),
    gross_loss_r numeric(20, 8),
    metrics_json jsonb,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT scalp_deployment_weekly_metrics_unique_week UNIQUE (deployment_id, week_start)
);

CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_claim_idx
    ON scalp_deployment_weekly_metrics(status, next_run_at, week_start);
CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_deployment_week_idx
    ON scalp_deployment_weekly_metrics(deployment_id, week_start DESC);
CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_symbol_strategy_week_idx
    ON scalp_deployment_weekly_metrics(symbol, strategy_id, tune_id, week_start DESC);
