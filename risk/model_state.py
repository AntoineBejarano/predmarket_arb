"""enabled/disabled por modelo ML (persistencia). El worker validate_edge aún no ramifica por slug."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from risk.ml_model_registry import ML_MODEL_SLUGS

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "data" / "model_state.json"


class ModelStateManager:
    """
    Igual idea que StrategyStateManager: toggles en JSON bajo data/.
    validate_edge puede leer este archivo más adelante para elegir pipeline.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._state: dict[str, dict[str, Any]] = {}
        self._load()

    def _default_entry(self, slug: str) -> dict[str, Any]:
        # El modelo principal arranca «habilitado» en UI; stubs desactivados.
        return {
            "enabled": slug == "crypto_5m_lgbm",
            "enabled_at": None,
            "disabled_at": None,
        }

    def _default_state(self) -> dict[str, dict[str, Any]]:
        return {slug: self._default_entry(slug) for slug in ML_MODEL_SLUGS}

    def _merge_entry(self, slug: str, data: dict[str, Any]) -> dict[str, Any]:
        defaults = self._default_entry(slug)
        out = dict(data)
        for k, v in defaults.items():
            if k not in out:
                out[k] = v
        out["enabled"] = bool(out.get("enabled", False))
        return out

    def _load(self) -> None:
        if STATE_FILE.exists():
            raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            self._state = {}
            for slug in ML_MODEL_SLUGS:
                self._state[slug] = self._merge_entry(slug, raw.get(slug, {}))
        else:
            self._state = self._default_state()
            self._save()

    def _save(self) -> None:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(self._state, indent=2), encoding="utf-8")

    def _reload_if_file(self) -> None:
        if STATE_FILE.exists():
            raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            for slug in ML_MODEL_SLUGS:
                self._state[slug] = self._merge_entry(slug, raw.get(slug, self._state.get(slug, {})))

    async def get_all(self) -> dict[str, dict[str, Any]]:
        async with self._lock:
            self._reload_if_file()
            return {k: dict(v) for k, v in self._state.items()}

    async def enable(self, slug: str) -> None:
        async with self._lock:
            self._reload_if_file()
            if slug not in self._state:
                return
            self._state[slug]["enabled"] = True
            self._state[slug]["enabled_at"] = datetime.now(timezone.utc).isoformat()
            self._save()

    async def disable(self, slug: str) -> None:
        async with self._lock:
            self._reload_if_file()
            if slug not in self._state:
                return
            self._state[slug]["enabled"] = False
            self._state[slug]["disabled_at"] = datetime.now(timezone.utc).isoformat()
            self._save()
