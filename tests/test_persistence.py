"""Persistencia Postgres opcional (sin DATABASE_URL en CI)."""

from __future__ import annotations

from pathlib import Path

import pytest

from lab.paths import data_dir


def test_flags_with_monkeypatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_WRITES", raising=False)
    monkeypatch.delenv("PRIMARY_STORE", raising=False)
    from persistence import config as pc

    assert pc.database_url() == ""
    assert pc.supabase_writes_enabled() is False
    assert pc.primary_store_postgres() is False


def test_pool_none_without_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_WRITES", raising=False)
    monkeypatch.delenv("PRIMARY_STORE", raising=False)
    from persistence.pool import get_pool

    assert get_pool() is None


def test_aggregate_csv_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PRIMARY_STORE", raising=False)
    from scripts import account_metrics as am

    agg = am.aggregate_pnl_from_strategy_logs(Path(data_dir()) / "logs")
    assert "pnl_total" in agg
    assert isinstance(agg["trades_total"], int)
