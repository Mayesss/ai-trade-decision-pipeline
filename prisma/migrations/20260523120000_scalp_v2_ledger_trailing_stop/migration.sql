ALTER TABLE scalp_v2_ledger
  DROP CONSTRAINT IF EXISTS scalp_v2_ledger_close_type_check;

ALTER TABLE scalp_v2_ledger
  ADD CONSTRAINT scalp_v2_ledger_close_type_check
  CHECK (close_type IN ('fill', 'stop_loss', 'trailing_stop', 'liquidation', 'manual_close', 'reconcile_close'));
