"""Tablas iniciales de persistencia (señales, arb, sixcycle, latency, estado).

Revision ID: 20260209120001
Revises:
Create Date: 2026-02-09

Equivalente a supabase/migrations/20260209120000_init_persistence.sql
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "20260209120001"
down_revision = None
branch_labels = None
depends_on = None

_UP_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS signal_observations (
        id bigserial PRIMARY KEY,
        created_at timestamptz NOT NULL DEFAULT now(),
        payload jsonb NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_signal_obs_condition
        ON signal_observations ((payload ->> 'condition_id'))
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_signal_obs_created
        ON signal_observations (created_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS arb_events (
        id bigserial PRIMARY KEY,
        created_at timestamptz NOT NULL DEFAULT now(),
        strategy text NOT NULL,
        payload jsonb NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_arb_events_strategy_created
        ON arb_events (strategy, created_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS sixcycle_engine_rows (
        id bigserial PRIMARY KEY,
        created_at timestamptz NOT NULL DEFAULT now(),
        payload jsonb NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sixcycle_created
        ON sixcycle_engine_rows (created_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS latency_sports_cycle_snapshots (
        id bigserial PRIMARY KEY,
        created_at timestamptz NOT NULL DEFAULT now(),
        payload jsonb NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_latency_snap_created
        ON latency_sports_cycle_snapshots (created_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_state_rows (
        slug text PRIMARY KEY,
        payload jsonb NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS model_state_rows (
        slug text PRIMARY KEY,
        payload jsonb NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
]


def upgrade() -> None:
    for raw in _UP_STATEMENTS:
        op.execute(text(raw.strip()))


def downgrade() -> None:
    for stmt in (
        "DROP TABLE IF EXISTS model_state_rows",
        "DROP TABLE IF EXISTS strategy_state_rows",
        "DROP TABLE IF EXISTS latency_sports_cycle_snapshots",
        "DROP TABLE IF EXISTS sixcycle_engine_rows",
        "DROP TABLE IF EXISTS arb_events",
        "DROP TABLE IF EXISTS signal_observations",
    ):
        op.execute(text(stmt))
