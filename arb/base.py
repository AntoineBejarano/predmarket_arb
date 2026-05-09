"""Clase base para estrategias de arbitraje (CSV + loop + estado)."""

from __future__ import annotations

import asyncio
import csv
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lab.paths import data_dir
from risk.strategy_state import FICTIONAL_CSV_FIELDS

log = logging.getLogger("arb.base")


def logs_csv_path(slug: str) -> Path:
    return data_dir() / "logs" / f"{slug}.csv"


class ArbStrategy(ABC):
    name: str
    slug: str

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        self.config = config
        self.dry_run = dry_run
        self.csv_path = logs_csv_path(self.slug)
        self._state_manager: Any = None
        self._ensure_csv()

    @property
    def all_csv_columns(self) -> list[str]:
        return list(self.csv_columns) + list(FICTIONAL_CSV_FIELDS)

    def _ensure_csv(self) -> None:
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.csv_path.exists():
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self.all_csv_columns, extrasaction="ignore")
                w.writeheader()
            return
        with open(self.csv_path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            old_fields = r.fieldnames or []
            if all(c in old_fields for c in FICTIONAL_CSV_FIELDS):
                return
            rows = list(r)
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self.all_csv_columns, extrasaction="ignore")
            w.writeheader()
            for row in rows:
                out = {k: row.get(k, "") for k in self.all_csv_columns}
                w.writerow(out)

    def log_signal(self, row: dict[str, Any]) -> None:
        """Escribe una fila; rellena ts, strategy, dry_run; action y reason obligatorios."""
        out = dict(row)
        out["ts"] = datetime.now(timezone.utc).isoformat()
        out["strategy"] = self.slug
        out["dry_run"] = str(bool(self.dry_run))
        if "action" not in out:
            out["action"] = "ERROR:INTERNAL"
        if "reason" not in out:
            out["reason"] = "missing_reason"
        for k in FICTIONAL_CSV_FIELDS:
            out.setdefault(k, "")
        for k in self.all_csv_columns:
            out.setdefault(k, "")
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=self.all_csv_columns, extrasaction="ignore").writerow(out)
        try:
            from persistence.writes import append_arb_event

            append_arb_event(self.slug, out)
        except Exception:
            pass

    async def log_signal_async(self, row: dict[str, Any]) -> None:
        """Igual que log_signal pero enriquece capital ficticio (paper) si hay state_manager."""
        if self._state_manager is not None:
            row = await self._state_manager.enrich_row_with_fictional(self.slug, row)
        self.log_signal(row)

    @property
    @abstractmethod
    def csv_columns(self) -> list[str]:
        ...

    @abstractmethod
    async def run_once(self) -> None:
        """Una iteración de la estrategia."""
        ...

    async def run_loop(self, state_manager: Any) -> None:
        """Loop infinito; respeta StrategyStateManager (sin spamear CSV si disabled)."""
        self._state_manager = state_manager
        interval = float(self.config.get("poll_interval", 30))
        while True:
            try:
                enabled = await state_manager.is_enabled(self.slug)
                if not enabled:
                    await asyncio.sleep(interval)
                    continue
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("[%s] run_once error", self.slug)
                await self.log_signal_async(
                    {
                        "action": "ERROR:INTERNAL",
                        "reason": str(e)[:200],
                    }
                )
            await asyncio.sleep(interval)
