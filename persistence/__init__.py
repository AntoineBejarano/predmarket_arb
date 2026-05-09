"""Persistencia opcional en Postgres (Supabase-compatible)."""

from persistence.config import (
    database_url,
    persistence_active_for_writes,
    primary_store_postgres,
)
from persistence.stats import snapshot as write_stats_snapshot

__all__ = [
    "database_url",
    "persistence_active_for_writes",
    "primary_store_postgres",
    "write_stats_snapshot",
]
