"""Lecturas desde Postgres cuando hay ``DATABASE_URL`` (por defecto; opt-out ``PRIMARY_STORE=csv``)."""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from persistence.config import database_url
from persistence.pool import get_pool

log = logging.getLogger("persistence.readers")


def _parse_ts_utc(s: str) -> datetime | None:
    raw = (s or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _float_cell(x: Any) -> float | None:
    if x is None:
        return None
    try:
        v = float(str(x).strip().replace(",", "."))
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except ValueError:
        return None


def load_signals_dataframe_from_postgres(limit: int = 500_000) -> pd.DataFrame:
    """Reconstruye un DataFrame alineado con ``CSV_COLUMNS`` del validador."""
    from scripts.validate_edge import CSV_COLUMNS

    pool = get_pool()
    if pool is None:
        return pd.DataFrame()
    rows: list[dict[str, str]] = []
    try:
        with pool.connection() as conn:
            cur = conn.execute(
                "SELECT payload FROM signal_observations ORDER BY id ASC LIMIT %s",
                (int(limit),),
            )
            for (payload,) in cur:
                d = dict(payload) if isinstance(payload, dict) else {}
                rows.append({c: str(d.get(c, "") if d.get(c) is not None else "") for c in CSV_COLUMNS})
    except Exception as e:
        log.warning("load_signals_dataframe_from_postgres: %s", e)
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame(columns=list(CSV_COLUMNS))
    return pd.DataFrame(rows, dtype=str)


def per_slug_pnl_today_from_postgres() -> dict[str, dict[str, Any]] | None:
    """Misma forma que ``per_slug_pnl_today`` desde tablas Postgres."""
    today = datetime.now(timezone.utc).date()
    pool = get_pool()
    if pool is None:
        return None
    out: dict[str, dict[str, Any]] = {}
    try:
        with pool.connection() as conn:
            cur = conn.execute("SELECT payload FROM sixcycle_engine_rows ORDER BY id ASC")
            slug = "crypto_5m_sixcycle"
            pnl_d = 0.0
            tr_d = 0
            w_d = 0
            for (payload,) in cur:
                row = dict(payload) if isinstance(payload, dict) else {}
                ph = str(row.get("phase", "")).upper().strip()
                res = str(row.get("resolved", "")).lower().strip()
                if ph != "SETTLED" or res not in ("win", "loss"):
                    continue
                ts = _parse_ts_utc(str(row.get("timestamp_utc", "") or ""))
                if ts is None or ts.date() != today:
                    continue
                pnl = _float_cell(row.get("pnl_usdc")) or 0.0
                pnl_d += pnl
                tr_d += 1
                if res == "win":
                    w_d += 1
            if tr_d:
                out[slug] = {
                    "pnl_hoy": round(pnl_d, 6),
                    "trades_hoy": tr_d,
                    "win_rate_hoy": round(w_d / tr_d, 4),
                }

            cur2 = conn.execute("SELECT strategy, payload FROM arb_events ORDER BY id ASC")
            by_slug: dict[str, list[tuple[float, datetime | None]]] = {}
            for strat, payload in cur2:
                if strat == "crypto_5m_sixcycle":
                    continue
                row = dict(payload) if isinstance(payload, dict) else {}
                pnl = _float_cell(row.get("fict_pnl_est_eur"))
                if pnl is None:
                    continue
                ts = _parse_ts_utc(str(row.get("ts", "") or ""))
                if ts is None or ts.date() != today:
                    continue
                by_slug.setdefault(str(strat), []).append((pnl, ts))
            for strat, items in by_slug.items():
                pnl_d2 = sum(x[0] for x in items)
                tr_d2 = len(items)
                if tr_d2:
                    out[strat] = {
                        "pnl_hoy": round(pnl_d2, 6),
                        "trades_hoy": tr_d2,
                        "win_rate_hoy": None,
                    }
    except Exception as e:
        log.warning("per_slug_pnl_today_from_postgres: %s", e)
        return None
    return out


def aggregate_pnl_from_postgres() -> dict[str, Any] | None:
    """
    Suma PnL desde tablas ``arb_events`` y ``sixcycle_engine_rows``.
    Devuelve None si no hay pool o error (el caller puede hacer fallback CSV).
    """
    today = datetime.now(timezone.utc).date()
    pool = get_pool()
    if pool is None:
        return None
    pnl_today = 0.0
    pnl_total = 0.0
    trades_today = 0
    wins_today = 0
    six_trades_today = 0
    trades_total = 0
    wins_total = 0
    try:
        with pool.connection() as conn:
            cur = conn.execute("SELECT payload FROM sixcycle_engine_rows ORDER BY id ASC")
            for (payload,) in cur:
                row = dict(payload) if isinstance(payload, dict) else {}
                ph = str(row.get("phase", "")).upper().strip()
                res = str(row.get("resolved", "")).lower().strip()
                if ph != "SETTLED" or res not in ("win", "loss"):
                    continue
                pnl = _float_cell(row.get("pnl_usdc")) or 0.0
                pnl_total += pnl
                trades_total += 1
                if res == "win":
                    wins_total += 1
                ts = _parse_ts_utc(str(row.get("timestamp_utc", "") or ""))
                if ts is not None and ts.date() == today:
                    pnl_today += pnl
                    trades_today += 1
                    six_trades_today += 1
                    if res == "win":
                        wins_today += 1

            cur2 = conn.execute(
                "SELECT strategy, payload FROM arb_events WHERE strategy <> %s ORDER BY id ASC",
                ("crypto_5m_sixcycle",),
            )
            for _strategy, payload in cur2:
                row = dict(payload) if isinstance(payload, dict) else {}
                pnl = _float_cell(row.get("fict_pnl_est_eur"))
                if pnl is None:
                    continue
                pnl_total += pnl
                trades_total += 1
                ts = _parse_ts_utc(str(row.get("ts", "") or ""))
                if ts is not None and ts.date() == today:
                    pnl_today += pnl
                    trades_today += 1
    except Exception as e:
        log.warning("aggregate_pnl_from_postgres: %s", e)
        return None

    wr_today = (wins_today / six_trades_today) if six_trades_today else 0.0
    return {
        "pnl_today": round(pnl_today, 6),
        "pnl_total": round(pnl_total, 6),
        "trades_today": int(trades_today),
        "win_rate_today": round(wr_today, 4),
        "trades_total": int(trades_total),
        "win_rate_total": round((wins_total / trades_total) if trades_total else 0.0, 4),
    }


