"""
condition_id que la IA descartó (reject / sin candidato válido): no deben volver a la cola «pendiente» en la UI.

Persistencia: ``DATA_DIR/logs/latency_sports_ai_rejected.json``
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lab.paths import data_dir

_REJECTS_FILENAME = "latency_sports_ai_rejected.json"


def ai_rejects_path() -> Path:
    return data_dir() / "logs" / _REJECTS_FILENAME


def load_ai_rejected_condition_ids() -> set[str]:
    p = ai_rejects_path()
    if not p.is_file():
        return set()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return set()
    if not isinstance(raw, dict):
        return set()
    items = raw.get("items")
    if not isinstance(items, dict):
        return set()
    return {str(k).strip() for k in items if str(k).strip()}


def _load_items() -> dict[str, dict[str, Any]]:
    p = ai_rejects_path()
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    items = raw.get("items")
    if not isinstance(items, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for k, v in items.items():
        ks = str(k).strip()
        if ks and isinstance(v, dict):
            out[ks] = dict(v)
    return out


def _save_items(items: dict[str, dict[str, Any]]) -> None:
    p = ai_rejects_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": 1, "items": dict(items)}
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def record_ai_reject(condition_id: str, *, reason: str = "ai_reject") -> None:
    cid = str(condition_id or "").strip()
    if not cid:
        return
    items = _load_items()
    items[cid] = {
        "reason": str(reason or "")[:120],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_items(items)


def clear_ai_reject(condition_id: str) -> bool:
    """Quita el bloqueo (p. ej. tras aprobar match manual o borrar enlace en UI)."""
    cid = str(condition_id or "").strip()
    if not cid:
        return False
    items = _load_items()
    if cid not in items:
        return False
    del items[cid]
    _save_items(items)
    return True
