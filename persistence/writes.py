"""Escrituras opcionales a Postgres (dual-write); fallos no interrumpen CSV."""

from __future__ import annotations

import logging
from typing import Any

from psycopg.types.json import Json

from persistence import stats
from persistence.config import persistence_active_for_writes
from persistence.pool import get_pool

log = logging.getLogger("persistence.writes")


def append_validator_signal(row: dict[str, Any]) -> None:
    if not persistence_active_for_writes():
        return
    pool = get_pool()
    if pool is None:
        return
    try:
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO signal_observations (payload) VALUES (%s)",
                (Json(row),),
            )
        stats.record_write_ok()
    except Exception as e:
        stats.record_write_fail(e)
        log.warning("Postgres append_validator_signal: %s", e)


def update_validator_signals_result(condition_id: str, result: str) -> None:
    """Alinea ``result`` en filas existentes (misma semántica que update_csv_results)."""
    if not persistence_active_for_writes():
        return
    pool = get_pool()
    if pool is None:
        return
    try:
        with pool.connection() as conn:
            conn.execute(
                """
                UPDATE signal_observations
                SET payload = payload || jsonb_build_object('result', %s::text)
                WHERE payload->>'condition_id' = %s
                """,
                (result, str(condition_id)),
            )
        stats.record_write_ok()
    except Exception as e:
        stats.record_write_fail(e)
        log.warning("Postgres update_validator_signals_result: %s", e)


def append_arb_event(strategy: str, row: dict[str, Any]) -> None:
    if not persistence_active_for_writes():
        return
    pool = get_pool()
    if pool is None:
        return
    try:
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO arb_events (strategy, payload) VALUES (%s, %s)",
                (strategy, Json(row)),
            )
        stats.record_write_ok()
    except Exception as e:
        stats.record_write_fail(e)
        log.warning("Postgres append_arb_event [%s]: %s", strategy, e)


def append_sixcycle_row(row: dict[str, Any]) -> None:
    if not persistence_active_for_writes():
        return
    pool = get_pool()
    if pool is None:
        return
    try:
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO sixcycle_engine_rows (payload) VALUES (%s)",
                (Json(row),),
            )
        stats.record_write_ok()
    except Exception as e:
        stats.record_write_fail(e)
        log.warning("Postgres append_sixcycle_row: %s", e)


def append_latency_cycle_snapshot(payload: dict[str, Any]) -> None:
    if not persistence_active_for_writes():
        return
    pool = get_pool()
    if pool is None:
        return
    try:
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO latency_sports_cycle_snapshots (payload) VALUES (%s)",
                (Json(payload),),
            )
        stats.record_write_ok()
    except Exception as e:
        stats.record_write_fail(e)
        log.warning("Postgres append_latency_cycle_snapshot: %s", e)


def upsert_strategy_state(state: dict[str, Any]) -> None:
    if not persistence_active_for_writes():
        return
    pool = get_pool()
    if pool is None:
        return
    try:
        with pool.connection() as conn:
            for slug, payload in state.items():
                conn.execute(
                    """
                    INSERT INTO strategy_state_rows (slug, payload, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (slug) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        updated_at = now()
                    """,
                    (str(slug), Json(payload)),
                )
        stats.record_write_ok()
    except Exception as e:
        stats.record_write_fail(e)
        log.warning("Postgres upsert_strategy_state: %s", e)


def clear_postgres_on_arb_data_reset(*, include_validator_signals: bool) -> dict[str, Any]:
    """
    Borra filas en tablas espejo de logs del motor (paralelas a CSV bajo DATA_DIR).
    Requiere ``DATABASE_URL`` (mismo pool que escrituras; no depende de ``SUPABASE_WRITES``).
    """
    out: dict[str, Any] = {
        "postgres_engine_logs_cleared": False,
        "postgres_errors": [],
    }
    pool = get_pool()
    if pool is None:
        return out
    try:
        with pool.connection() as conn:
            conn.execute("DELETE FROM arb_events")
            conn.execute("DELETE FROM sixcycle_engine_rows")
            conn.execute("DELETE FROM latency_sports_cycle_snapshots")
            if include_validator_signals:
                conn.execute("DELETE FROM signal_observations")
        out["postgres_engine_logs_cleared"] = True
    except Exception as e:
        msg = str(e)
        out["postgres_errors"].append(msg)
        log.warning("clear_postgres_on_arb_data_reset: %s", e)
    return out


def clear_postgres_sixcycle_rows() -> dict[str, Any]:
    """Borra solo ``sixcycle_engine_rows`` (reset parcial Sixcycle desde /api/sixcycle/reset-data)."""
    out: dict[str, Any] = {"postgres_sixcycle_cleared": False, "postgres_errors": []}
    pool = get_pool()
    if pool is None:
        return out
    try:
        with pool.connection() as conn:
            conn.execute("DELETE FROM sixcycle_engine_rows")
        out["postgres_sixcycle_cleared"] = True
    except Exception as e:
        out["postgres_errors"].append(str(e))
        log.warning("clear_postgres_sixcycle_rows: %s", e)
    return out


def upsert_model_state(state: dict[str, Any]) -> None:
    if not persistence_active_for_writes():
        return
    pool = get_pool()
    if pool is None:
        return
    try:
        with pool.connection() as conn:
            for slug, payload in state.items():
                conn.execute(
                    """
                    INSERT INTO model_state_rows (slug, payload, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (slug) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        updated_at = now()
                    """,
                    (str(slug), Json(payload)),
                )
        stats.record_write_ok()
    except Exception as e:
        stats.record_write_fail(e)
        log.warning("Postgres upsert_model_state: %s", e)
