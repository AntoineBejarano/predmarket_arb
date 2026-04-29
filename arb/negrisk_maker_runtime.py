"""Estado persistido, reconciliación (3 fuentes) y kill switch — NegRisk maker."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from clients.poly_clob import PolyCLOBClient
from lab.paths import data_dir


def negrisk_maker_paths() -> dict[str, Path]:
    base = data_dir() / "logs"
    base.mkdir(parents=True, exist_ok=True)
    return {
        "state": base / "negrisk_maker_state.json",
        "orders": base / "negrisk_maker_orders.json",
        "events": base / "negrisk_maker_events.json",
    }


@dataclass
class EventRuntimeState:
    event_id: str
    state: str = "EMPTY"
    tokens: list[str] = field(default_factory=list)
    target_q: float = 0.0
    filled_q_by_token: dict[str, float] = field(default_factory=dict)
    avg_price_by_token: dict[str, float] = field(default_factory=dict)
    open_order_ids: list[str] = field(default_factory=list)
    last_reconcile_ts: str = ""
    partial_since: str = ""
    reconcile_fail_streak: int = 0

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "EventRuntimeState":
        return cls(
            event_id=str(d.get("event_id", "")),
            state=str(d.get("state", "EMPTY")),
            tokens=list(d.get("tokens") or []),
            target_q=float(d.get("target_q") or 0),
            filled_q_by_token={str(k): float(v) for k, v in (d.get("filled_q_by_token") or {}).items()},
            avg_price_by_token={str(k): float(v) for k, v in (d.get("avg_price_by_token") or {}).items()},
            open_order_ids=list(d.get("open_order_ids") or []),
            last_reconcile_ts=str(d.get("last_reconcile_ts", "")),
            partial_since=str(d.get("partial_since", "")),
            reconcile_fail_streak=int(d.get("reconcile_fail_streak") or 0),
        )


def load_all_states() -> dict[str, EventRuntimeState]:
    p = negrisk_maker_paths()["state"]
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, EventRuntimeState] = {}
    for eid, blob in raw.items():
        if isinstance(blob, dict):
            blob = {**blob, "event_id": str(eid)}
            try:
                out[str(eid)] = EventRuntimeState.from_json(blob)
            except (TypeError, ValueError):
                continue
    return out


def save_all_states(states: dict[str, EventRuntimeState]) -> None:
    p = negrisk_maker_paths()["state"]
    p.parent.mkdir(parents=True, exist_ok=True)
    obj = {eid: s.to_json() for eid, s in states.items()}
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


async def cancel_order_ids_best_effort(poly: PolyCLOBClient, order_ids: list[str]) -> None:
    for oid in order_ids:
        oid = str(oid).strip()
        if not oid:
            continue
        try:
            await poly.cancel_order(oid)
        except Exception:
            pass


async def reconcile_event(
    poly: PolyCLOBClient,
    *,
    token_ids: list[str],
    open_order_ids: list[str],
) -> dict[str, Any]:
    """
    Tres fuentes: open orders por asset, detalle por order id, trades por asset.
    Requiere L2; en dry-run o sin credenciales devuelve ok=False.
    """
    out: dict[str, Any] = {"ok": True, "open_pages": [], "order_details": [], "trades_pages": []}
    if poly.dry_run:
        out["ok"] = False
        out["reason"] = "dry_run_no_l2_reconcile"
        return out
    try:
        for tid in token_ids:
            page = await poly.get_open_orders(asset_id=tid)
            out["open_pages"].append({"token_id": tid[:16], "page": page})
        for oid in open_order_ids:
            detail = await poly.get_order(str(oid))
            out["order_details"].append({"order_id": oid[:16], "detail": detail})
        for tid in token_ids:
            tr = await poly.get_trades(asset_id=tid)
            out["trades_pages"].append({"token_id": tid[:16], "trades": tr})
    except Exception as e:
        out["ok"] = False
        out["reason"] = str(e)[:200]
    out["ts"] = datetime.now(timezone.utc).isoformat()
    return out


class MakerKillSwitch:
    """Fase 5.5: fallos globales de reconcile."""

    def __init__(self) -> None:
        self.reconcile_fail_cycles = 0
        self.live_posting_disabled = False

    def record_reconcile(self, ok: bool, max_fail: int) -> None:
        if ok:
            self.reconcile_fail_cycles = 0
            return
        self.reconcile_fail_cycles += 1
        if self.reconcile_fail_cycles >= max_fail:
            self.live_posting_disabled = True

    def reset(self) -> None:
        self.reconcile_fail_cycles = 0
        self.live_posting_disabled = False
