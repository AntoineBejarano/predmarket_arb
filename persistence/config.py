"""Flags y URL para persistencia Postgres (p. ej. Supabase)."""

from __future__ import annotations

import os


def database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def supabase_writes_enabled() -> bool:
    v = os.getenv("SUPABASE_WRITES", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def primary_store_postgres() -> bool:
    """Si true, la API y agregados leen señales / PnL desde Postgres cuando hay datos."""
    return os.getenv("PRIMARY_STORE", "").strip().lower() == "postgres"


def persistence_active_for_writes() -> bool:
    return bool(database_url()) and supabase_writes_enabled()
