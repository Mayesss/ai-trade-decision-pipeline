ALTER TABLE scalp_research_attempts
  DROP CONSTRAINT IF EXISTS scalp_research_attempts_task_id_fkey;

ALTER TABLE scalp_research_attempts
  ALTER COLUMN task_id TYPE text USING task_id::text;

ALTER TABLE scalp_research_tasks
  ALTER COLUMN task_id TYPE text USING task_id::text;

ALTER TABLE scalp_research_attempts
  ADD CONSTRAINT scalp_research_attempts_task_id_fkey
  FOREIGN KEY (task_id) REFERENCES scalp_research_tasks(task_id) ON DELETE CASCADE;
