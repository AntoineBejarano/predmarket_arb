"""Flags y URL para persistencia Postgres (p. ej. Supabase)."""

from __future__ import annotations

import os

_FALSE = frozenset({"0", "false", "no", "off"})
_CSV_READ = frozenset({"csv", "file"})


def database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def supabase_writes_enabled() -> bool:
    """Dual-write a Postgres: con ``DATABASE_URL`` definida, **activo por defecto**.

    Opt-out: ``SUPABASE_WRITES=false`` (o ``0`` / ``no`` / ``off``).
    """
    if not database_url():
        return False
    v = os.getenv("SUPABASE_WRITES", "").strip().lower()
    if v in _FALSE:
        return False
    return True


def primary_store_postgres() -> bool:
    """Lecturas desde Postgres: con ``DATABASE_URL``, **activo por defecto**.

    Opt-out: ``PRIMARY_STORE=csv`` (o ``file``) para forzar solo CSV/ficheros.
    """
    if not database_url():
        return False
    v = os.getenv("PRIMARY_STORE", "").strip().lower()
    if v in _CSV_READ or v in _FALSE:
        return False
    return True


def persistence_active_for_writes() -> bool:
    return bool(database_url()) and supabase_writes_enabled()
