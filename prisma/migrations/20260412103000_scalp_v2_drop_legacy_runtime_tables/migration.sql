-- Full v2 cutover: remove retired legacy scalp v1 runtime/pipeline tables.
-- These tables are no longer used by /api/scalp/v2 runtime paths.

DROP TABLE IF EXISTS scalp_runtime_settings CASCADE;
DROP TABLE IF EXISTS scalp_strategy_overrides CASCADE;
DROP TABLE IF EXISTS scalp_sessions CASCADE;
DROP TABLE IF EXISTS scalp_journal CASCADE;
DROP TABLE IF EXISTS scalp_trade_ledger CASCADE;
DROP TABLE IF EXISTS scalp_execution_runs CASCADE;
DROP TABLE IF EXISTS scalp_deployment_weekly_metrics CASCADE;
DROP TABLE IF EXISTS scalp_discovered_symbols CASCADE;
DROP TABLE IF EXISTS scalp_pipeline_symbols CASCADE;
DROP TABLE IF EXISTS scalp_pipeline_job_runs CASCADE;
DROP TABLE IF EXISTS scalp_pipeline_jobs CASCADE;
DROP TABLE IF EXISTS scalp_shadow_job_runs CASCADE;
DROP TABLE IF EXISTS scalp_jobs CASCADE;
DROP TABLE IF EXISTS scalp_deployments CASCADE;
DROP TABLE IF EXISTS scalp_symbol_universe_snapshots CASCADE;
DROP TABLE IF EXISTS scalp_symbol_cooldowns CASCADE;
