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


def test_defaults_on_when_database_url_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("SUPABASE_WRITES", raising=False)
    monkeypatch.delenv("PRIMARY_STORE", raising=False)
    from persistence import config as pc

    assert pc.supabase_writes_enabled() is True
    assert pc.primary_store_postgres() is True
    assert pc.persistence_active_for_writes() is True


def test_opt_out_writes_and_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("SUPABASE_WRITES", "false")
    monkeypatch.setenv("PRIMARY_STORE", "csv")
    from persistence import config as pc

    assert pc.supabase_writes_enabled() is False
    assert pc.primary_store_postgres() is False
    assert pc.persistence_active_for_writes() is False


def test_pool_none_without_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_WRITES", raising=False)
    monkeypatch.delenv("PRIMARY_STORE", raising=False)
    from persistence.pool import get_pool

    assert get_pool() is None


def test_clear_postgres_reset_no_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_WRITES", raising=False)
    monkeypatch.delenv("PRIMARY_STORE", raising=False)
    from persistence.writes import clear_postgres_on_arb_data_reset, clear_postgres_sixcycle_rows

    r = clear_postgres_on_arb_data_reset(include_validator_signals=True)
    assert r["postgres_engine_logs_cleared"] is False
    assert r["postgres_errors"] == []
    r2 = clear_postgres_sixcycle_rows()
    assert r2["postgres_sixcycle_cleared"] is False


def test_aggregate_csv_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PRIMARY_STORE", raising=False)
    from scripts import account_metrics as am

    agg = am.aggregate_pnl_from_strategy_logs(Path(data_dir()) / "logs")
    assert "pnl_total" in agg
    assert isinstance(agg["trades_total"], int)


def test_pool_needed_true_with_database_url_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset y mantenimiento Postgres deben poder usar pool aunque opt-out CSV/escrituras."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@127.0.0.1:59999/postgres")
    monkeypatch.setenv("SUPABASE_WRITES", "false")
    monkeypatch.setenv("PRIMARY_STORE", "csv")
    from persistence import pool as pp

    with pp._pool_lock:
        pp._pool = None
    assert pp.pool_needed() is True


def test_clear_postgres_on_arb_data_reset_executes_deletes(monkeypatch: pytest.MonkeyPatch) -> None:
    from persistence import writes as pw

    class _Conn:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, sql: str, params: object | None = None) -> None:
            self.statements.append(sql.strip())

    class _Pool:
        def __init__(self) -> None:
            self._conn = _Conn()

        def connection(self):
            return self

        def __enter__(self):
            return self._conn

        def __exit__(self, *args: object) -> None:
            return None

    fake = _Pool()
    monkeypatch.setattr(pw, "get_pool", lambda: fake)
    out = pw.clear_postgres_on_arb_data_reset(include_validator_signals=True)
    assert out["postgres_engine_logs_cleared"] is True
    assert out["postgres_errors"] == []
    stmts = " ".join(fake._conn.statements)
    assert "DELETE FROM arb_events" in stmts
    assert "DELETE FROM sixcycle_engine_rows" in stmts
    assert "DELETE FROM latency_sports_cycle_snapshots" in stmts
    assert "DELETE FROM signal_observations" in stmts


def test_clear_postgres_sixcycle_only(monkeypatch: pytest.MonkeyPatch) -> None:
    from persistence import writes as pw

    class _Conn:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, sql: str, params: object | None = None) -> None:
            self.statements.append(sql.strip())

    class _Pool:
        def __init__(self) -> None:
            self._conn = _Conn()

        def connection(self):
            return self

        def __enter__(self):
            return self._conn

        def __exit__(self, *args: object) -> None:
            return None

    fake = _Pool()
    monkeypatch.setattr(pw, "get_pool", lambda: fake)
    out = pw.clear_postgres_sixcycle_rows()
    assert out["postgres_sixcycle_cleared"] is True
    assert fake._conn.statements == ["DELETE FROM sixcycle_engine_rows"]
