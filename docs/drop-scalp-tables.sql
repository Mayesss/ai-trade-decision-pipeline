-- =============================================================================
-- Forex/scalp retirement — Phase 5: drop the scalp tables from the shared
-- Neon database. Run MANUALLY on Neon, in this order, after the trimmed code
-- has run cleanly for a soak period (recommended: 2-4 weeks).
--
-- The swing system uses ONLY the `swing` schema — nothing here touches it.
-- The forex subsystem stored its state in KV, not Postgres, so there are no
-- forex tables to drop.
--
-- STEP 0 (before anything): create a Neon branch as a snapshot backup.
--   Neon console -> Branches -> "Create branch" from current head, e.g.
--   name it `pre-scalp-drop-2026-09`. That branch preserves every table
--   dropped below and can be promoted/queried if anything is missed.
-- =============================================================================

-- STEP 1 — inventory: review what will be dropped (nothing is modified).
-- Everything outside the `swing` schema is a candidate; scalp tables live in
-- `public` and start with `scalp_`.
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(format('%I.%I', schemaname, tablename))) AS total_size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'swing')
ORDER BY pg_total_relation_size(format('%I.%I', schemaname, tablename)) DESC;

-- Sanity check: the swing tables that must remain untouched.
SELECT tablename FROM pg_tables WHERE schemaname = 'swing' ORDER BY tablename;
-- Expected: account_snapshots, ai_cooldowns, ai_threads, break_triggers,
--           decisions, lessons, positions, postmortems, tick_log, weekly_digests

-- STEP 2 — review the exact DROP statements this will execute (still no changes):
SELECT format('DROP TABLE IF EXISTS %I.%I CASCADE;', schemaname, tablename) AS stmt
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename LIKE 'scalp\_%' ESCAPE '\'
ORDER BY tablename;

-- STEP 3 — execute the drops. Only run this after Step 0 (branch snapshot)
-- and after reviewing Step 2's output.
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename LIKE 'scalp\_%' ESCAPE '\'
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', r.schemaname, r.tablename);
        RAISE NOTICE 'dropped %.%', r.schemaname, r.tablename;
    END LOOP;
END $$;

-- STEP 4 — verify nothing scalp-named remains and swing is intact:
SELECT tablename FROM pg_tables WHERE tablename LIKE 'scalp\_%' ESCAPE '\';  -- expect 0 rows
SELECT count(*) AS swing_tables FROM pg_tables WHERE schemaname = 'swing';   -- expect 10

-- STEP 5 (optional) — reclaim space is automatic on Neon; if any leftover
-- non-swing, non-scalp tables showed up in Step 1 (e.g. one-off research or
-- migration tables), review them individually before dropping — they are not
-- covered by this script on purpose.
