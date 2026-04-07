-- Purge legacy v1 deployments for symbols that are no longer in the
-- FIXED_SCOPE_SYMBOLS_BY_VENUE allowlist.  All 25 off-scope symbols are
-- disabled (enabled = FALSE) and have zero v2 candidates — they are dead
-- weight from the old Bitget API auto-discovery pipeline.
--
-- Current fixed scope (34 symbols):
--   Bitget: BTCUSDT ETHUSDT SOLUSDT XRPUSDT DOGEUSDT LINKUSDT DOTUSDT
--           ADAUSDT LTCUSDT AVAXUSDT SUIUSDT WLDUSDT ARBUSDT OPUSDT
--           APTUSDT NEARUSDT TONUSDT INJUSDT PEPEUSDT FETUSDT
--   Capital: EURUSD GBPUSD USDJPY XAUUSD AUDUSD USDCAD XAGUSD EURGBP
--            NZDUSD USDCHF EURJPY GBPJPY AUDJPY CHFJPY

-- Use a CTE to define the allowed symbols once.
DO $$
DECLARE
  allowed_symbols TEXT[] := ARRAY[
    'BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','DOGEUSDT',
    'LINKUSDT','DOTUSDT','ADAUSDT','LTCUSDT','AVAXUSDT',
    'SUIUSDT','WLDUSDT','ARBUSDT','OPUSDT','APTUSDT',
    'NEARUSDT','TONUSDT','INJUSDT','PEPEUSDT','FETUSDT',
    'EURUSD','GBPUSD','USDJPY','XAUUSD','AUDUSD',
    'USDCAD','XAGUSD','EURGBP','NZDUSD','USDCHF',
    'EURJPY','GBPJPY','AUDJPY','CHFJPY'
  ];
BEGIN

  -- Safety: only delete disabled deployments outside the scope.
  -- Enabled deployments are never touched.

  -- 1. v1 ledger rows
  DELETE FROM scalp_trade_ledger
  WHERE deployment_id IN (
    SELECT deployment_id FROM scalp_deployments
    WHERE symbol != ALL(allowed_symbols)
      AND enabled = FALSE
  );

  -- 2. v1 weekly metrics
  DELETE FROM scalp_deployment_weekly_metrics
  WHERE deployment_id IN (
    SELECT deployment_id FROM scalp_deployments
    WHERE symbol != ALL(allowed_symbols)
      AND enabled = FALSE
  );

  -- 3. v1 execution runs
  DELETE FROM scalp_execution_runs
  WHERE deployment_id IN (
    SELECT deployment_id FROM scalp_deployments
    WHERE symbol != ALL(allowed_symbols)
      AND enabled = FALSE
  );

  -- 4. v1 sessions
  DELETE FROM scalp_sessions
  WHERE deployment_id IN (
    SELECT deployment_id FROM scalp_deployments
    WHERE symbol != ALL(allowed_symbols)
      AND enabled = FALSE
  );

  -- 5. v1 journal (deployment_id is nullable, also match by symbol)
  DELETE FROM scalp_journal
  WHERE symbol != ALL(allowed_symbols)
    AND (
      deployment_id IS NULL
      OR deployment_id IN (
        SELECT deployment_id FROM scalp_deployments
        WHERE symbol != ALL(allowed_symbols)
          AND enabled = FALSE
      )
    );

  -- 6. v1 deployments themselves
  DELETE FROM scalp_deployments
  WHERE symbol != ALL(allowed_symbols)
    AND enabled = FALSE;

  -- 7. v2 deployments (same off-scope symbols, all disabled)
  DELETE FROM scalp_v2_ledger
  WHERE deployment_id IN (
    SELECT deployment_id FROM scalp_v2_deployments
    WHERE symbol != ALL(allowed_symbols)
      AND enabled = FALSE
  );

  DELETE FROM scalp_v2_deployments
  WHERE symbol != ALL(allowed_symbols)
    AND enabled = FALSE;

  -- 8. Unstick the legacy v1 promotion_sync job stuck in 'running' since March 19
  UPDATE scalp_jobs
  SET status = 'succeeded',
      locked_by = NULL,
      locked_at = NULL,
      updated_at = NOW()
  WHERE kind = 'promotion_sync'
    AND status = 'running';

END $$;
