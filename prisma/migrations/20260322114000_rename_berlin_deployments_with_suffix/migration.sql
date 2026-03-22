-- Rename current Berlin deployments to use a session-suffixed tune id (`*_berlin`).
-- This migration keeps FK integrity by inserting new deployment ids first,
-- rewriting dependent references, then deleting old deployment rows.

DO $$
DECLARE
    map_count bigint := 0;
    invalid_id_rows bigint := 0;
    deployment_id_conflicts bigint := 0;
    triplet_conflicts bigint := 0;
    table_row record;
BEGIN
    CREATE TEMP TABLE tmp_berlin_deployment_suffix_map (
        old_deployment_id text PRIMARY KEY,
        new_deployment_id text UNIQUE NOT NULL,
        old_tune_id text NOT NULL,
        new_tune_id text NOT NULL,
        symbol text NOT NULL,
        strategy_id text NOT NULL
    ) ON COMMIT DROP;

    INSERT INTO tmp_berlin_deployment_suffix_map (
        old_deployment_id,
        new_deployment_id,
        old_tune_id,
        new_tune_id,
        symbol,
        strategy_id
    )
    SELECT
        d.deployment_id AS old_deployment_id,
        regexp_replace(d.deployment_id, '~[^~]*$', '~' || (d.tune_id || '_berlin')) AS new_deployment_id,
        d.tune_id AS old_tune_id,
        d.tune_id || '_berlin' AS new_tune_id,
        d.symbol,
        d.strategy_id
    FROM scalp_deployments d
    WHERE LOWER(COALESCE(NULLIF(d.entry_session_profile, ''), '')) = 'berlin'
      AND d.tune_id NOT LIKE '%\_berlin' ESCAPE '\';

    SELECT COUNT(*)::bigint INTO map_count
    FROM tmp_berlin_deployment_suffix_map;

    IF map_count = 0 THEN
        RAISE NOTICE 'rename_berlin_deployments_with_suffix: no matching berlin rows to rename';
        RETURN;
    END IF;

    SELECT COUNT(*)::bigint INTO invalid_id_rows
    FROM tmp_berlin_deployment_suffix_map
    WHERE new_deployment_id = old_deployment_id;

    IF invalid_id_rows > 0 THEN
        RAISE EXCEPTION 'rename_berlin_deployments_with_suffix: invalid deployment_id format in % row(s)', invalid_id_rows;
    END IF;

    SELECT COUNT(*)::bigint INTO deployment_id_conflicts
    FROM scalp_deployments d
    JOIN tmp_berlin_deployment_suffix_map m
      ON d.deployment_id = m.new_deployment_id
    WHERE d.deployment_id <> m.old_deployment_id;

    IF deployment_id_conflicts > 0 THEN
        RAISE EXCEPTION 'rename_berlin_deployments_with_suffix: new deployment_id conflicts found (%)', deployment_id_conflicts;
    END IF;

    SELECT COUNT(*)::bigint INTO triplet_conflicts
    FROM scalp_deployments d
    JOIN tmp_berlin_deployment_suffix_map m
      ON d.symbol = m.symbol
     AND d.strategy_id = m.strategy_id
     AND d.tune_id = m.new_tune_id
    WHERE d.deployment_id <> m.old_deployment_id;

    IF triplet_conflicts > 0 THEN
        RAISE EXCEPTION 'rename_berlin_deployments_with_suffix: (symbol,strategy_id,tune_id) conflicts found (%)', triplet_conflicts;
    END IF;

    INSERT INTO scalp_deployments (
        deployment_id,
        symbol,
        strategy_id,
        tune_id,
        entry_session_profile,
        source,
        enabled,
        config_override,
        promotion_gate,
        in_universe,
        worker_dirty,
        promotion_dirty,
        retired_at,
        last_prepared_at,
        updated_by,
        created_at,
        updated_at
    )
    SELECT
        m.new_deployment_id,
        d.symbol,
        d.strategy_id,
        m.new_tune_id,
        d.entry_session_profile,
        d.source,
        d.enabled,
        d.config_override,
        d.promotion_gate,
        d.in_universe,
        d.worker_dirty,
        d.promotion_dirty,
        d.retired_at,
        d.last_prepared_at,
        COALESCE(d.updated_by, 'migration_rename_berlin_suffix'),
        d.created_at,
        NOW()
    FROM scalp_deployments d
    JOIN tmp_berlin_deployment_suffix_map m
      ON m.old_deployment_id = d.deployment_id;

    FOR table_row IN
        SELECT
            c.table_name,
            BOOL_OR(c.column_name = 'deployment_id') AS has_deployment_id,
            BOOL_OR(c.column_name = 'tune_id') AS has_tune_id
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name LIKE 'scalp_%'
          AND c.table_name <> 'scalp_deployments'
          AND c.column_name IN ('deployment_id', 'tune_id')
        GROUP BY c.table_name
        ORDER BY c.table_name
    LOOP
        IF table_row.has_deployment_id THEN
            EXECUTE format(
                'UPDATE %I t
                    SET deployment_id = m.new_deployment_id
                   FROM tmp_berlin_deployment_suffix_map m
                  WHERE t.deployment_id = m.old_deployment_id',
                table_row.table_name
            );
        END IF;

        IF table_row.has_tune_id AND table_row.has_deployment_id THEN
            EXECUTE format(
                'UPDATE %I t
                    SET tune_id = m.new_tune_id
                   FROM tmp_berlin_deployment_suffix_map m
                  WHERE t.deployment_id = m.new_deployment_id
                    AND t.tune_id = m.old_tune_id',
                table_row.table_name
            );
        END IF;
    END LOOP;

    DELETE FROM scalp_deployments d
    USING tmp_berlin_deployment_suffix_map m
    WHERE d.deployment_id = m.old_deployment_id;

    RAISE NOTICE 'rename_berlin_deployments_with_suffix: renamed % deployment(s)', map_count;
END $$;
