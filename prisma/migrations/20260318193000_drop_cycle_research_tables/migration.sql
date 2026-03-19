-- Drop cycle-era research tables after async-job cutover

DROP TABLE IF EXISTS scalp_research_attempts;
DROP TABLE IF EXISTS scalp_research_tasks;
DROP TABLE IF EXISTS scalp_research_cycles;
DROP TABLE IF EXISTS scalp_research_report_snapshots;

DROP TYPE IF EXISTS scalp_research_task_status;
DROP TYPE IF EXISTS scalp_cycle_status;
