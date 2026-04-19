-- Remove legacy candidate status "shadow" from scalp_v2_candidates.
-- Deployment live_mode still supports "shadow" and is intentionally unchanged.

-- 1) Rewrite existing rows to discovered so they re-enter research backlog.
UPDATE scalp_v2_candidates
SET
  status = 'discovered',
  updated_at = NOW(),
  reason_codes = (
    SELECT ARRAY(
      SELECT DISTINCT x
      FROM unnest(
        COALESCE(reason_codes, '{}'::text[]) || ARRAY['SCALP_V2_SHADOW_STATUS_REMOVED']::text[]
      ) AS x
    )
  )
WHERE status = 'shadow';

-- 2) Drop any old check constraint that still allows shadow.
DO $$
DECLARE
  con RECORD;
BEGIN
  FOR con IN
    SELECT c.conname
    FROM pg_constraint c
    INNER JOIN pg_class t
      ON t.oid = c.conrelid
    INNER JOIN pg_namespace n
      ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relname = 'scalp_v2_candidates'
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) ILIKE '%status%'
      AND pg_get_constraintdef(c.oid) ILIKE '%shadow%'
  LOOP
    EXECUTE format(
      'ALTER TABLE public.scalp_v2_candidates DROP CONSTRAINT %I',
      con.conname
    );
  END LOOP;
END $$;

-- 3) Enforce status set without shadow.
ALTER TABLE scalp_v2_candidates
ADD CONSTRAINT scalp_v2_candidates_status_check
CHECK (status IN ('discovered', 'evaluated', 'promoted', 'rejected'));
