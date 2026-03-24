CREATE TABLE IF NOT EXISTS scalp_adaptive_selector_snapshots (
    snapshot_id text PRIMARY KEY,
    symbol text NOT NULL,
    entry_session_profile text NOT NULL DEFAULT 'berlin',
    strategy_id text NOT NULL,
    status text NOT NULL DEFAULT 'shadow',
    trained_at timestamptz NOT NULL,
    window_from_ts bigint NOT NULL,
    window_to_ts bigint NOT NULL,
    catalog_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    lock_started_at_ms bigint,
    lock_until_ms bigint,
    baseline_max_drawdown_r numeric(20, 8),
    updated_by text,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT scalp_adaptive_selector_snapshots_status_check
      CHECK (status IN ('shadow', 'active', 'archived')),
    CONSTRAINT scalp_adaptive_selector_snapshots_entry_session_profile_check
      CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney')),
    CONSTRAINT scalp_adaptive_selector_snapshots_window_check
      CHECK (window_to_ts > window_from_ts)
);

CREATE UNIQUE INDEX IF NOT EXISTS scalp_adaptive_selector_snapshots_active_unique
    ON scalp_adaptive_selector_snapshots(symbol, entry_session_profile, strategy_id)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS scalp_adaptive_selector_snapshots_symbol_strategy_status_idx
    ON scalp_adaptive_selector_snapshots(symbol, entry_session_profile, strategy_id, status);

CREATE INDEX IF NOT EXISTS scalp_adaptive_selector_snapshots_status_trained_idx
    ON scalp_adaptive_selector_snapshots(status, trained_at DESC);

CREATE TABLE IF NOT EXISTS scalp_adaptive_selector_decisions (
    id bigserial PRIMARY KEY,
    ts timestamptz NOT NULL DEFAULT NOW(),
    deployment_id text NOT NULL,
    symbol text NOT NULL,
    strategy_id text NOT NULL,
    entry_session_profile text NOT NULL DEFAULT 'berlin',
    snapshot_id text NULL REFERENCES scalp_adaptive_selector_snapshots(snapshot_id) ON DELETE SET NULL,
    selected_arm_id text,
    selected_arm_type text NOT NULL DEFAULT 'none',
    confidence numeric(10, 6),
    skip_reason text,
    reason_codes text[] NOT NULL DEFAULT '{}'::text[],
    features_hash text,
    details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT scalp_adaptive_selector_decisions_arm_type_check
      CHECK (selected_arm_type IN ('pattern', 'incumbent', 'none')),
    CONSTRAINT scalp_adaptive_selector_decisions_entry_session_profile_check
      CHECK (entry_session_profile IN ('tokyo', 'berlin', 'newyork', 'sydney'))
);

CREATE INDEX IF NOT EXISTS scalp_adaptive_selector_decisions_symbol_session_ts_idx
    ON scalp_adaptive_selector_decisions(symbol, entry_session_profile, ts DESC);

CREATE INDEX IF NOT EXISTS scalp_adaptive_selector_decisions_deployment_ts_idx
    ON scalp_adaptive_selector_decisions(deployment_id, ts DESC);

CREATE INDEX IF NOT EXISTS scalp_adaptive_selector_decisions_snapshot_ts_idx
    ON scalp_adaptive_selector_decisions(snapshot_id, ts DESC);
