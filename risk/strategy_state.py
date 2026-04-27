"""Persistencia de estrategias enabled/disabled para el control plane."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "data" / "strategy_state.json"

_SLUGS = [
    "bundle_arb",
    "cross_exchange",
    "market_maker",
    "combinatorial_arb",
    "term_structure",
    "latency_arb",
]


class StrategyStateManager:
    """
    Gestiona enabled/disabled por estrategia.
    Thread-safe vía asyncio.Lock; persiste en data/strategy_state.json.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._state: dict = {}
        self._load()

    def _default_state(self) -> dict:
        return {
            s: {
                "enabled": s in ("bundle_arb", "cross_exchange"),
                "enabled_at": None,
                "disabled_at": None,
            }
            for s in _SLUGS
        }

    def _load(self) -> None:
        if STATE_FILE.exists():
            self._state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        else:
            self._state = self._default_state()
            self._save()

    def _save(self) -> None:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(self._state, indent=2), encoding="utf-8")

    def _reload_if_file(self) -> None:
        """Sincroniza con disco para que el API y arb_engine vean los mismos toggles."""
        if STATE_FILE.exists():
            self._state = json.loads(STATE_FILE.read_text(encoding="utf-8"))

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

    async def get_all(self) -> dict:
        async with self._lock:
            self._reload_if_file()
            return dict(self._state)
