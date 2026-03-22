-- Cleanup reintroduced unsuffixed Berlin deployment duplicates.
-- Requires matching *_berlin row to already exist, rewires child refs, then deletes old rows.

DO $$
DECLARE
    map_count bigint := 0;
    missing_target_rows bigint := 0;
    incompatible_target_rows bigint := 0;
    table_row record;
BEGIN
    CREATE TEMP TABLE tmp_cleanup_reintroduced_berlin_map (
        old_deployment_id text PRIMARY KEY,
        new_deployment_id text UNIQUE NOT NULL,
        old_tune_id text NOT NULL,
        new_tune_id text NOT NULL,
        symbol text NOT NULL,
        strategy_id text NOT NULL
    ) ON COMMIT DROP;

    INSERT INTO tmp_cleanup_reintroduced_berlin_map (
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
    FROM tmp_cleanup_reintroduced_berlin_map;

    IF map_count = 0 THEN
        RAISE NOTICE 'cleanup_reintroduced_unsuffixed_berlin_dupes: no reintroduced rows found';
        RETURN;
    END IF;

    SELECT COUNT(*)::bigint INTO missing_target_rows
    FROM tmp_cleanup_reintroduced_berlin_map m
    LEFT JOIN scalp_deployments target_d
      ON target_d.deployment_id = m.new_deployment_id
    WHERE target_d.deployment_id IS NULL;

    IF missing_target_rows > 0 THEN
        RAISE EXCEPTION 'cleanup_reintroduced_unsuffixed_berlin_dupes: missing *_berlin target rows (%)', missing_target_rows;
    END IF;

    SELECT COUNT(*)::bigint INTO incompatible_target_rows
    FROM tmp_cleanup_reintroduced_berlin_map m
    JOIN scalp_deployments target_d
      ON target_d.deployment_id = m.new_deployment_id
    WHERE target_d.symbol <> m.symbol
       OR target_d.strategy_id <> m.strategy_id
       OR target_d.tune_id <> m.new_tune_id;

    IF incompatible_target_rows > 0 THEN
        RAISE EXCEPTION 'cleanup_reintroduced_unsuffixed_berlin_dupes: incompatible target rows (%)', incompatible_target_rows;
    END IF;

    -- Avoid unique collisions before rewiring.
    DELETE FROM scalp_sessions old_s
    USING tmp_cleanup_reintroduced_berlin_map m, scalp_sessions new_s
    WHERE old_s.deployment_id = m.old_deployment_id
      AND new_s.deployment_id = m.new_deployment_id
      AND new_s.day_key = old_s.day_key;

    DELETE FROM scalp_execution_runs old_r
    USING tmp_cleanup_reintroduced_berlin_map m, scalp_execution_runs new_r
    WHERE old_r.deployment_id = m.old_deployment_id
      AND new_r.deployment_id = m.new_deployment_id
      AND new_r.scheduled_minute = old_r.scheduled_minute;

    DELETE FROM scalp_deployment_weekly_metrics old_w
    USING tmp_cleanup_reintroduced_berlin_map m, scalp_deployment_weekly_metrics new_w
    WHERE old_w.deployment_id = m.old_deployment_id
      AND new_w.deployment_id = m.new_deployment_id
      AND new_w.week_start = old_w.week_start;

    -- Rewire all scalp_* deployment_id/tune_id references.
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
                   FROM tmp_cleanup_reintroduced_berlin_map m
                  WHERE t.deployment_id = m.old_deployment_id',
                table_row.table_name
            );
        END IF;

        IF table_row.has_tune_id AND table_row.has_deployment_id THEN
            EXECUTE format(
                'UPDATE %I t
                    SET tune_id = m.new_tune_id
                   FROM tmp_cleanup_reintroduced_berlin_map m
                  WHERE t.deployment_id = m.new_deployment_id
                    AND t.tune_id = m.old_tune_id',
                table_row.table_name
            );
        END IF;
    END LOOP;

    DELETE FROM scalp_deployments d
    USING tmp_cleanup_reintroduced_berlin_map m
    WHERE d.deployment_id = m.old_deployment_id;

    RAISE NOTICE 'cleanup_reintroduced_unsuffixed_berlin_dupes: cleaned % row(s)', map_count;
END $$;
