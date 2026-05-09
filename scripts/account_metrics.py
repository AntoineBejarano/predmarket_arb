"""Agregación ligera de PnL desde CSV de logs (multi-estrategia)."""

from __future__ import annotations

import csv
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lab.paths import data_dir


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


def per_slug_pnl_today(log_dir: Path) -> dict[str, dict[str, Any]]:
    """PnL agregado hoy (UTC) por slug de CSV bajo ``log_dir``."""
    from persistence.config import primary_store_postgres
    from persistence.readers import per_slug_pnl_today_from_postgres

    if primary_store_postgres():
        pg = per_slug_pnl_today_from_postgres()
        if pg is not None:
            return pg

    today = datetime.now(timezone.utc).date()
    out: dict[str, dict[str, Any]] = {}
    if not log_dir.is_dir():
        return out
    for path in sorted(log_dir.glob("*.csv")):
        slug = path.stem
        try:
            with path.open(newline="", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                fields = set(rdr.fieldnames or [])
                rows = list(rdr)
        except OSError:
            continue
        if not rows:
            continue
        pnl_d = 0.0
        tr_d = 0
        w_d = 0
        if slug == "crypto_5m_sixcycle" and "pnl_usdc" in fields:
            for row in rows:
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
        elif "fict_pnl_est_eur" in fields and "ts" in fields:
            for row in rows:
                pnl = _float_cell(row.get("fict_pnl_est_eur"))
                if pnl is None:
                    continue
                ts = _parse_ts_utc(str(row.get("ts", "") or ""))
                if ts is None or ts.date() != today:
                    continue
                pnl_d += pnl
                tr_d += 1
        if tr_d:
            wr = round(w_d / tr_d, 4) if tr_d else 0.0
            out[slug] = {
                "pnl_hoy": round(pnl_d, 6),
                "trades_hoy": tr_d,
                "win_rate_hoy": wr if slug == "crypto_5m_sixcycle" else None,
            }
    return out


def aggregate_pnl_from_strategy_logs(
    log_dir: Path,
) -> dict[str, Any]:
    """
    Suma PnL hoy (UTC) y total desde CSV bajo ``log_dir``.

    - ``crypto_5m_sixcycle``: filas ``phase=SETTLED`` y ``resolved`` win/loss, columna ``pnl_usdc``.
    - Otras estrategias: columna ``fict_pnl_est_eur`` si existe, timestamp ``ts``.
    """
    from persistence.config import primary_store_postgres
    from persistence.readers import aggregate_pnl_from_postgres

    if primary_store_postgres():
        pg = aggregate_pnl_from_postgres()
        if pg is not None:
            return pg

    today = datetime.now(timezone.utc).date()
    pnl_today = 0.0
    pnl_total = 0.0
    trades_today = 0
    wins_today = 0
    six_trades_today = 0
    trades_total = 0
    wins_total = 0

    if not log_dir.is_dir():
        return _empty_agg()

    for path in sorted(log_dir.glob("*.csv")):
        if not path.is_file():
            continue
        slug = path.stem
        try:
            with path.open(newline="", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                fields = set(rdr.fieldnames or [])
                rows = list(rdr)
        except OSError:
            continue
        if not rows:
            continue

        if slug == "crypto_5m_sixcycle" and "pnl_usdc" in fields:
            for row in rows:
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
            continue

        if "fict_pnl_est_eur" in fields and "ts" in fields:
            for row in rows:
                pnl = _float_cell(row.get("fict_pnl_est_eur"))
                if pnl is None:
                    continue
                pnl_total += pnl
                trades_total += 1
                ts = _parse_ts_utc(str(row.get("ts", "") or ""))
                if ts is not None and ts.date() == today:
                    pnl_today += pnl
                    trades_today += 1

    wr_today = (wins_today / six_trades_today) if six_trades_today else 0.0
    return {
        "pnl_today": round(pnl_today, 6),
        "pnl_total": round(pnl_total, 6),
        "trades_today": int(trades_today),
        "win_rate_today": round(wr_today, 4),
        "trades_total": int(trades_total),
        "win_rate_total": round((wins_total / trades_total) if trades_total else 0.0, 4),
    }


def _empty_agg() -> dict[str, Any]:
    return {
        "pnl_today": 0.0,
        "pnl_total": 0.0,
        "trades_today": 0,
        "win_rate_today": 0.0,
        "trades_total": 0,
        "win_rate_total": 0.0,
    }


def default_logs_dir() -> Path:
    return data_dir() / "logs"
