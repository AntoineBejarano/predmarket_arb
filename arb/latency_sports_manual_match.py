"""
Enlaces manuales Poly (condition_id) ↔ evento odds-api.io (event_id).

Laboratorio personal: JSON bajo DATA_DIR/logs; el motor relee cada ciclo.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lab.paths import data_dir

MANUAL_MATCHES_FILENAME = "latency_sports_manual_matches.json"


def manual_matches_path() -> Path:
    return data_dir() / "logs" / MANUAL_MATCHES_FILENAME


def load_manual_matches() -> dict[str, dict[str, Any]]:
    p = manual_matches_path()
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    items = raw.get("items")
    if isinstance(items, dict):
        out: dict[str, dict[str, Any]] = {}
        for k, v in items.items():
            if isinstance(v, dict) and str(k).strip():
                out[str(k).strip()] = dict(v)
        return out
    return {}


def save_manual_matches(items: dict[str, dict[str, Any]]) -> None:
    p = manual_matches_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": 1, "items": dict(items)}
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def upsert_manual_match(
    condition_id: str,
    odds_event_id: str,
    *,
    swap_sides: bool = False,
    poly_home: str = "",
    poly_away: str = "",
) -> dict[str, Any]:
    cid = (condition_id or "").strip()
    eid = (odds_event_id or "").strip()
    if not cid or not eid:
        raise ValueError("condition_id y odds_event_id son obligatorios")
    items = load_manual_matches()
    from datetime import datetime, timezone

    items[cid] = {
        "odds_event_id": eid,
        "swap_sides": bool(swap_sides),
        "poly_home": (poly_home or "").strip(),
        "poly_away": (poly_away or "").strip(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    save_manual_matches(items)
    try:
        from arb.latency_sports_ai_rejects import clear_ai_reject

        clear_ai_reject(cid)
    except Exception:
        pass
    return items[cid]


def delete_manual_match(condition_id: str, *, clear_ai_reject: bool = True) -> bool:
    cid = (condition_id or "").strip()
    if not cid:
        return False
    items = load_manual_matches()
    if cid not in items:
        return False
    del items[cid]
    save_manual_matches(items)
    if clear_ai_reject:
        try:
            from arb.latency_sports_ai_rejects import clear_ai_reject as _clear_rej

            _clear_rej(cid)
        except Exception:
            pass
    return True


def list_manual_matches_for_api() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cid, v in sorted(load_manual_matches().items(), key=lambda x: x[0]):
        if not isinstance(v, dict):
            continue
        rows.append(
            {
                "condition_id": cid,
                "odds_event_id": str(v.get("odds_event_id") or ""),
                "swap_sides": bool(v.get("swap_sides")),
                "poly_home": str(v.get("poly_home") or ""),
                "poly_away": str(v.get("poly_away") or ""),
                "updated_at": str(v.get("updated_at") or ""),
            }
        )
    return rows
