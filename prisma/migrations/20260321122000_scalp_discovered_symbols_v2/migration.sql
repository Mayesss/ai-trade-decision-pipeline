CREATE TABLE IF NOT EXISTS scalp_discovered_symbols (
    symbol text PRIMARY KEY,
    last_discovered_at timestamptz,
    load_status text NOT NULL DEFAULT 'pending',
    load_attempts integer NOT NULL DEFAULT 0,
    load_next_run_at timestamptz,
    load_error text,
    weeks_covered integer NOT NULL DEFAULT 0,
    latest_week_start timestamptz,
    last_loaded_at timestamptz,
    prepare_status text NOT NULL DEFAULT 'pending',
    prepare_attempts integer NOT NULL DEFAULT 0,
    prepare_next_run_at timestamptz,
    prepare_error text,
    prepared_deployments integer NOT NULL DEFAULT 0,
    last_prepared_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scalp_discovered_symbols_load_claim_idx
    ON scalp_discovered_symbols(load_status, load_next_run_at);

CREATE INDEX IF NOT EXISTS scalp_discovered_symbols_prepare_claim_idx
    ON scalp_discovered_symbols(prepare_status, prepare_next_run_at);

INSERT INTO scalp_discovered_symbols(
    symbol,
    last_discovered_at,
    load_status,
    load_attempts,
    load_next_run_at,
    load_error,
    weeks_covered,
    latest_week_start,
    last_loaded_at,
    prepare_status,
    prepare_attempts,
    prepare_next_run_at,
    prepare_error,
    prepared_deployments,
    last_prepared_at,
    created_at,
    updated_at
)
SELECT
    symbol,
    last_discovered_at,
    load_status,
    load_attempts,
    load_next_run_at,
    load_error,
    weeks_covered,
    latest_week_start,
    last_loaded_at,
    prepare_status,
    prepare_attempts,
    prepare_next_run_at,
    prepare_error,
    prepared_deployments,
    last_prepared_at,
    created_at,
    updated_at
FROM scalp_pipeline_symbols
WHERE active = TRUE
ON CONFLICT(symbol)
DO NOTHING;
