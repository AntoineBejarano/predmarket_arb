-- PredMarket Arb: tablas para dual-write / lectura Postgres (Supabase compatible).
-- Ejecutar en el SQL editor de Supabase o con: psql "$DATABASE_URL" -f ...

CREATE TABLE IF NOT EXISTS signal_observations (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signal_obs_condition
    ON signal_observations ((payload ->> 'condition_id'));

CREATE INDEX IF NOT EXISTS idx_signal_obs_created
    ON signal_observations (created_at DESC);


CREATE TABLE IF NOT EXISTS arb_events (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    strategy text NOT NULL,
    payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_arb_events_strategy_created
    ON arb_events (strategy, created_at DESC);


CREATE TABLE IF NOT EXISTS sixcycle_engine_rows (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sixcycle_created
    ON sixcycle_engine_rows (created_at DESC);


CREATE TABLE IF NOT EXISTS latency_sports_cycle_snapshots (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_latency_snap_created
    ON latency_sports_cycle_snapshots (created_at DESC);


CREATE TABLE IF NOT EXISTS strategy_state_rows (
    slug text PRIMARY KEY,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS model_state_rows (
    slug text PRIMARY KEY,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
