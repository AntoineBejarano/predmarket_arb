"""Persistencia de estrategias enabled/disabled + capital ficticio (paper ROI)."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "data" / "strategy_state.json"

_SLUGS = [
    "bundle_arb",
    "cross_exchange",
    "market_maker",
    "combinatorial_arb",
    "term_structure",
    "latency_arb",
    "latency_arb_sports",
    "crypto_5m_sixcycle",
]

# Columnas extra en CSV de arb (unidad «EUR» = notionale paper, mismo escala que USDC del CLOB)
FICTIONAL_CSV_FIELDS = (
    "fict_stake_eur",
    "fict_pnl_signal_est_eur",
    "fict_pnl_exec_est_eur",
    "fict_pnl_est_eur",
    "fict_pnl_cum_eur",
    "fict_roi",
)


def _default_fictional_capital() -> float:
    return float(os.getenv("ARB_FICT_CAPITAL_EUR", "1000"))


class StrategyStateManager:
    """
    Gestiona enabled/disabled por estrategia y capital ficticio (PnL acumulado estimado).
    Thread-safe vía asyncio.Lock; persiste en data/strategy_state.json.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._state: dict = {}
        self._load()

    def _default_state(self) -> dict:
        cap = _default_fictional_capital()
        return {
            s: {
                "enabled": False,
                "enabled_at": None,
                "disabled_at": None,
                "fict_capital_eur": cap,
                "fict_pnl_cumulative_eur": 0.0,
                "fict_trades": 0,
                "fict_last_stake_eur": None,
                "fict_last_pnl_est_eur": None,
            }
            for s in _SLUGS
        }

    def _merge_entry(self, slug: str, data: dict[str, Any]) -> dict[str, Any]:
        """Completa claves nuevas sin borrar enabled_at existentes."""
        defaults = self._default_state()[slug]
        out = dict(data)
        for k, v in defaults.items():
            if k not in out:
                out[k] = v
        if "fict_capital_eur" in out:
            try:
                out["fict_capital_eur"] = float(out["fict_capital_eur"])
            except (TypeError, ValueError):
                out["fict_capital_eur"] = _default_fictional_capital()
        if "fict_pnl_cumulative_eur" in out:
            try:
                out["fict_pnl_cumulative_eur"] = float(out["fict_pnl_cumulative_eur"])
            except (TypeError, ValueError):
                out["fict_pnl_cumulative_eur"] = 0.0
        if "fict_trades" in out:
            try:
                out["fict_trades"] = int(out["fict_trades"])
            except (TypeError, ValueError):
                out["fict_trades"] = 0
        return out

    def _load(self) -> None:
        if STATE_FILE.exists():
            raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            self._state = {}
            for slug in _SLUGS:
                self._state[slug] = self._merge_entry(slug, raw.get(slug, {}))
        else:
            self._state = self._default_state()
            self._save()

    def _save(self) -> None:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(self._state, indent=2), encoding="utf-8")
        try:
            from persistence.writes import upsert_strategy_state

            upsert_strategy_state(dict(self._state))
        except Exception:
            pass

    def _reload_if_file(self) -> None:
        if STATE_FILE.exists():
            raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            for slug in _SLUGS:
                self._state[slug] = self._merge_entry(slug, raw.get(slug, self._state.get(slug, {})))

    async def is_enabled(self, slug: str) -> bool:
        async with self._lock:
            self._reload_if_file()
            return bool(self._state.get(slug, {}).get("enabled", False))

    async def enable(self, slug: str) -> None:
        async with self._lock:
            self._reload_if_file()
            if slug in self._state:
                self._state[slug]["enabled"] = True
                self._state[slug]["enabled_at"] = datetime.now(timezone.utc).isoformat()
                self._save()

    async def disable(self, slug: str) -> None:
        async with self._lock:
            self._reload_if_file()
            if slug in self._state:
                self._state[slug]["enabled"] = False
                self._state[slug]["disabled_at"] = datetime.now(timezone.utc).isoformat()
                self._save()

    async def disable_all_strategies(self) -> None:
        """Apaga el interruptor de todas las estrategias (un solo guardado en disco)."""
        async with self._lock:
            self._reload_if_file()
            now = datetime.now(timezone.utc).isoformat()
            defaults = self._default_state()
            for slug in _SLUGS:
                if slug not in self._state:
                    self._state[slug] = defaults[slug]
                self._state[slug]["enabled"] = False
                self._state[slug]["disabled_at"] = now
            self._save()

    async def get_all(self) -> dict:
        async with self._lock:
            self._reload_if_file()
            return dict(self._state)

    async def reset_fictional_paper(self) -> None:
        """Pone a cero PnL/ops paper por estrategia; conserva enabled y fict_capital_eur."""
        async with self._lock:
            self._reload_if_file()
            for slug in _SLUGS:
                ent = self._merge_entry(slug, self._state.get(slug, {}))
                cap = float(ent.get("fict_capital_eur") or _default_fictional_capital())
                ent["fict_capital_eur"] = cap
                ent["fict_pnl_cumulative_eur"] = 0.0
                ent["fict_trades"] = 0
                ent["fict_last_stake_eur"] = None
                ent["fict_last_pnl_est_eur"] = None
                self._state[slug] = ent
            self._save()

    async def enrich_row_with_fictional(self, slug: str, row: dict[str, Any]) -> dict[str, Any]:
        """
        Para SIGNAL / EXECUTED con size_usdc:
        - pnl_signal_est = stake * edge_signal (teórico)
        - pnl_exec_est = stake * edge_exec (ejecutable)
        Acumula el ejecutable y rellena columnas CSV.
        Unidad «EUR» es notionale (misma magnitud que presupuesto paper en USDC).
        """
        empty = {k: "" for k in FICTIONAL_CSV_FIELDS}
        action = str(row.get("action") or "")
        if action not in ("SIGNAL", "EXECUTED"):
            return {**row, **empty}

        try:
            stake = float(row.get("size_usdc") or 0)
        except (TypeError, ValueError):
            return {**row, **empty}

        def _as_float(v: Any) -> float:
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        edge_signal = _as_float(row.get("edge_signal", row.get("edge")))
        edge_exec = _as_float(row.get("edge_exec", row.get("edge")))
        if stake <= 0 or edge_exec <= 0:
            return {**row, **empty}

        async with self._lock:
            self._reload_if_file()
            ent = self._merge_entry(slug, self._state.get(slug, {}))
            cap = float(ent.get("fict_capital_eur") or _default_fictional_capital())
            stake_use = min(stake, cap)
            pnl_signal_est = stake_use * edge_signal
            pnl_exec_est = stake_use * edge_exec
            cum = float(ent.get("fict_pnl_cumulative_eur") or 0.0) + pnl_exec_est
            n_tr = int(ent.get("fict_trades") or 0) + 1
            ent["fict_pnl_cumulative_eur"] = cum
            ent["fict_trades"] = n_tr
            ent["fict_last_stake_eur"] = stake_use
            ent["fict_last_pnl_est_eur"] = pnl_exec_est
            roi = cum / cap if cap > 0 else 0.0
            self._state[slug] = ent
            self._save()

        out = dict(row)
        out["fict_stake_eur"] = f"{stake_use:.4f}"
        out["fict_pnl_signal_est_eur"] = f"{pnl_signal_est:.6f}"
        out["fict_pnl_exec_est_eur"] = f"{pnl_exec_est:.6f}"
        # Compat hacia atrás para dashboards que aún lean la columna legacy.
        out["fict_pnl_est_eur"] = f"{pnl_exec_est:.6f}"
        out["fict_pnl_cum_eur"] = f"{cum:.6f}"
        out["fict_roi"] = f"{roi:.6f}"
        return out
