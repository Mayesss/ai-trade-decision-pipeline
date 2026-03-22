-- Normalize scalp_sessions.state_json identity fields so legacy unsuffixed deployment ids
-- cannot persist after berlin tune suffix migration.
--
-- Rewrites root identity keys from canonical scalp_deployments row values:
--   - deploymentId
--   - tuneId
--   - strategyId
--   - symbol
--   - venue
--
-- Safe to re-run (idempotent): only updates rows with mismatched values.

DO $$
DECLARE
    normalized_rows bigint := 0;
BEGIN
    WITH normalized AS (
        UPDATE scalp_sessions s
        SET
            state_json = jsonb_set(
                jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            jsonb_set(
                                COALESCE(s.state_json, '{}'::jsonb),
                                '{deploymentId}',
                                to_jsonb(d.deployment_id::text),
                                true
                            ),
                            '{tuneId}',
                            to_jsonb(d.tune_id::text),
                            true
                        ),
                        '{strategyId}',
                        to_jsonb(d.strategy_id::text),
                        true
                    ),
                    '{symbol}',
                    to_jsonb(d.symbol::text),
                    true
                ),
                '{venue}',
                to_jsonb(
                    CASE
                        WHEN POSITION(':' IN d.deployment_id) > 0 THEN split_part(d.deployment_id, ':', 1)
                        ELSE 'bitget'
                    END::text
                ),
                true
            ),
            updated_at = NOW()
        FROM scalp_deployments d
        WHERE d.deployment_id = s.deployment_id
          AND (
                COALESCE(s.state_json #>> '{deploymentId}', '') <> d.deployment_id
             OR COALESCE(s.state_json #>> '{tuneId}', '') <> d.tune_id
             OR COALESCE(s.state_json #>> '{strategyId}', '') <> d.strategy_id
             OR COALESCE(s.state_json #>> '{symbol}', '') <> d.symbol
             OR COALESCE(s.state_json #>> '{venue}', '') <> CASE
                    WHEN POSITION(':' IN d.deployment_id) > 0 THEN split_part(d.deployment_id, ':', 1)
                    ELSE 'bitget'
                END
            )
        RETURNING 1
    )
    SELECT COUNT(*)::bigint INTO normalized_rows
    FROM normalized;

    RAISE NOTICE 'normalize_scalp_sessions_state_json_identity: updated % row(s)', normalized_rows;
END $$;
