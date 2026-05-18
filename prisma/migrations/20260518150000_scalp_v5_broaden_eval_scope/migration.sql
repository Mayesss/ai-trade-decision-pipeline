-- v5 evaluator scope broadened from `enabled = TRUE` to `candidate_id IS NOT
-- NULL`, so v5 evidence builds across the full v3-promoted pool (not only
-- the live subset). The earlier partial index on `enabled=TRUE` no longer
-- covers the bulk evaluator scan; this index does.

CREATE INDEX IF NOT EXISTS scalp_v2_deployments_v5_eval_scope_idx
  ON scalp_v2_deployments(enabled DESC, v5_evaluated_at NULLS FIRST)
  WHERE candidate_id IS NOT NULL;
