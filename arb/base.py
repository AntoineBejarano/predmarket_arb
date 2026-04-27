"""Clase base para estrategias de arbitraje (CSV + loop + estado)."""

from __future__ import annotations

import asyncio
import csv
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("arb.base")


def logs_csv_path(slug: str) -> Path:
    base = Path(os.getenv("DATA_DIR", ".")).resolve()
    return base / "logs" / f"{slug}.csv"


class ArbStrategy(ABC):
    name: str
    slug: str

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        self.config = config
        self.dry_run = dry_run
        self.csv_path = logs_csv_path(self.slug)
        self._ensure_csv()

    def _ensure_csv(self) -> None:
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.csv_path.exists():
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self.csv_columns, extrasaction="ignore")
                w.writeheader()

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
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=self.csv_columns, extrasaction="ignore").writerow(out)

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
                self.log_signal(
                    {
                        "action": "ERROR:INTERNAL",
                        "reason": str(e)[:200],
                    }
                )
            await asyncio.sleep(interval)
