"""
Matching automático vía OpenRouter (laboratorio privado).

Lee ``DATA_DIR/logs/latency_arb_sports_pending_matches.json`` (mismo snapshot que el motor)
y aplica cambios directamente en ``latency_sports_manual_matches.json``:
  - ``decision: "match"`` + ``odds_event_id`` válido → upsert
  - ``decision: "reject"`` o id inválido / sin respuesta para esa fila → delete (si existía enlace)

Credenciales hardcodeadas a petición del operador (no usar en repos públicos).
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import aiohttp

from arb.latency_sports_ai_rejects import ai_rejects_path, record_ai_reject
from arb.latency_sports_manual_match import delete_manual_match, manual_matches_path, upsert_manual_match
from lab.paths import data_dir

def _api_log_info(msg: str, *args: Any) -> None:
    """Misma etiqueta que el proceso Uvicorn (`[api]`) para que sea visible en consola."""
    logging.getLogger("api").info("[latency_sports_ai] " + msg, *args)


def _api_log_warning(msg: str, *args: Any) -> None:
    logging.getLogger("api").warning("[latency_sports_ai] " + msg, *args)

# Laboratorio privado — rotar si el repo deja de ser privado.
_OPENROUTER_API_KEY = (
    "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"
)
# Qwen3.5-9B a veces llena la ventana de salida sin cerrar JSON; Mimo suele ser más compacto en JSON.
_OPENROUTER_MODEL = "xiaomi/mimo-v2.5"
_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
# Presupuesto de completion alto para arrays largos; el prompt se acota abajo para no disparar coste.
_OPENROUTER_MAX_TOKENS = 8192

_PENDING_REL = Path("logs") / "latency_arb_sports_pending_matches.json"
_MAX_PENDING_ROWS = 20
_MAX_CANDS_PER_ROW = 35


def pending_matches_path() -> Path:
    return data_dir() / _PENDING_REL


def _load_pending_document() -> dict[str, Any]:
    p = pending_matches_path()
    if not p.is_file():
        return {"pending": [], "pending_count": 0, "updated_at": None}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {"pending": [], "pending_count": 0, "updated_at": None}
    except (OSError, json.JSONDecodeError, TypeError):
        return {"pending": [], "pending_count": 0, "updated_at": None}


def _extract_json_array(content: str) -> list[Any]:
    s = (content or "").strip()
    if not s:
        return []
    # Quitar cerco tipo ```json ... ```
    fence = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```\s*$", s, flags=re.I)
    if fence:
        s = fence.group(1).strip()
    try:
        data = json.loads(s)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        pass
    i = s.find("[")
    j = s.rfind("]")
    if i >= 0 and j > i:
        try:
            data = json.loads(s[i : j + 1])
            return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _extract_decisions_list(content: str) -> list[Any]:
    """Acepta array JSON, o objeto {matches|results|decisions|items: [...]}."""
    arr = _extract_json_array(content)
    if arr:
        return arr
    s = (content or "").strip()
    fence = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```\s*$", s, flags=re.I)
    if fence:
        s = fence.group(1).strip()
    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        return []
    if isinstance(data, dict):
        for k in ("matches", "results", "decisions", "items", "data"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    return []


def _assistant_message_text(msg: dict[str, Any]) -> str:
    """OpenRouter puede devolver content string o lista de partes tipo Chat Completions."""
    raw = msg.get("content")
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: list[str] = []
        for p in raw:
            if isinstance(p, dict):
                if p.get("type") == "text" and isinstance(p.get("text"), str):
                    parts.append(p["text"])
                elif isinstance(p.get("text"), str):
                    parts.append(str(p["text"]))
        return "".join(parts)
    return str(raw or "")


def _build_ai_tasks(raw_pending: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for row in raw_pending[:_MAX_PENDING_ROWS]:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("condition_id") or "").strip()
        if not cid:
            continue
        cands_in = row.get("io_candidates") if isinstance(row.get("io_candidates"), list) else []
        slim: list[dict[str, str]] = []
        ids: list[str] = []
        for c in cands_in[:_MAX_CANDS_PER_ROW]:
            if not isinstance(c, dict):
                continue
            eid = str(c.get("event_id") or "").strip()
            if not eid:
                continue
            slim.append(
                {
                    "event_id": eid,
                    "home": str(c.get("home") or ""),
                    "away": str(c.get("away") or ""),
                    "bookie": str(c.get("bookie") or ""),
                }
            )
            ids.append(eid)
        tasks.append(
            {
                "condition_id": cid,
                "poly_home": str(row.get("poly_home") or ""),
                "poly_away": str(row.get("poly_away") or ""),
                "sport_slug": str(row.get("sport_slug") or ""),
                "slug": str(row.get("slug") or "")[:240],
                "raw_title": str(row.get("raw_title") or "")[:400],
                "candidate_event_ids": ids,
                "candidates": slim,
            }
        )
    return tasks


async def run_openrouter_on_pending() -> dict[str, Any]:
    doc = _load_pending_document()
    pending = doc.get("pending") if isinstance(doc.get("pending"), list) else []
    tasks = _build_ai_tasks([r for r in pending if isinstance(r, dict)])
    if not tasks:
        _api_log_info(
            "skip: no hay filas pending con condition_id (pending_count=%s path=%s)",
            doc.get("pending_count"),
            pending_matches_path(),
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "no_pending_rows",
            "openrouter_model": _OPENROUTER_MODEL,
            "manual_matches_path": str(manual_matches_path()),
            "pending_path": str(pending_matches_path()),
            "results": [],
        }
    _api_log_info(
        "OpenRouter request tasks=%s model=%s pending_path=%s",
        len(tasks),
        _OPENROUTER_MODEL,
        pending_matches_path(),
    )

    payload_user = {
        "instructions": (
            "Para cada tarea, decide si algún candidato es el mismo partido que Polymarket (mismos equipos, "
            "tolerando abreviaturas). Devuelve SOLO un array JSON válido, sin markdown ni texto antes/después. "
            "Sé compacto: solo el array, una fila lógica por tarea. "
            "Cada elemento: "
            '{"condition_id":"<igual que en tasks>","decision":"match"|"reject",'
            '"odds_event_id":"<uno de candidate_event_ids o null>","swap_sides":true|false}. '
            "Si Poly home/away están invertidos respecto al evento IO, swap_sides=true. "
            "Si ningún candidato encaja, decision=reject y odds_event_id=null."
        ),
        "tasks": tasks,
    }
    body = {
        "model": _OPENROUTER_MODEL,
        "temperature": 0.1,
        "max_tokens": int(_OPENROUTER_MAX_TOKENS),
        "messages": [
            {
                "role": "system",
                "content": "You output only valid JSON arrays. No markdown, no commentary.",
            },
            {"role": "user", "content": json.dumps(payload_user, ensure_ascii=False)},
        ],
    }
    headers = {
        "Authorization": f"Bearer {_OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost/latency_sports",
        "X-Title": "predmarket_arb_latency_sports_ai",
    }
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(_OPENROUTER_URL, headers=headers, json=body) as resp:
            text = await resp.text()
            if resp.status != 200:
                _api_log_warning("OpenRouter HTTP %s: %s", resp.status, text[:500])
                raise RuntimeError(f"OpenRouter HTTP {resp.status}: {text[:400]}")

    try:
        outer = json.loads(text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"OpenRouter respuesta no JSON: {e}") from e
    choices = outer.get("choices") if isinstance(outer, dict) else None
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("OpenRouter: sin choices")
    msg0 = choices[0] if isinstance(choices[0], dict) else {}
    msg = msg0.get("message") if isinstance(msg0.get("message"), dict) else {}
    finish_reason = str(msg0.get("finish_reason") or "").strip()
    content = _assistant_message_text(msg)
    arr = _extract_decisions_list(content)
    if not arr:
        if finish_reason == "length":
            _api_log_warning(
                "finish_reason=length (salida truncada). content_len=%s. Primeros 1200 chars:\n%s",
                len(content),
                (content or "")[:1200],
            )
        _api_log_warning(
            "respuesta sin JSON aplicable (len=%s finish=%s). Primeros 1200 chars:\n%s",
            len(content),
            finish_reason or "—",
            (content or "")[:1200],
        )
        raise RuntimeError(
            "OpenRouter: la IA no devolvió un array JSON ni un objeto {matches:[...]} parseable. "
            "No se modificó manual_matches (evita borrados masivos por error)."
        )
    by_cid: dict[str, dict[str, Any]] = {}
    for item in arr:
        if not isinstance(item, dict):
            continue
        cid = str(item.get("condition_id") or "").strip()
        if cid:
            by_cid[cid] = item

    task_ids = [t["condition_id"] for t in tasks]
    id_to_task = {t["condition_id"]: t for t in tasks}

    results: list[dict[str, Any]] = []
    for cid in task_ids:
        t = id_to_task[cid]
        allowed = set(t.get("candidate_event_ids") or [])
        ai = by_cid.get(cid)
        if not isinstance(ai, dict):
            deleted = delete_manual_match(cid, clear_ai_reject=False)
            record_ai_reject(cid, reason="no_ai_row")
            results.append(
                {"condition_id": cid, "action": "delete", "deleted": deleted, "reason": "no_ai_row"}
            )
            continue
        decision = str(ai.get("decision") or "").strip().lower()
        eid = str(ai.get("odds_event_id") or "").strip()
        swap = bool(ai.get("swap_sides"))
        if decision == "match" and eid and eid in allowed:
            row = upsert_manual_match(
                cid,
                eid,
                swap_sides=swap,
                poly_home=str(t.get("poly_home") or ""),
                poly_away=str(t.get("poly_away") or ""),
            )
            results.append({"condition_id": cid, "action": "upsert", "item": row})
            continue
        deleted = delete_manual_match(cid, clear_ai_reject=False)
        record_ai_reject(cid, reason="reject_or_invalid")
        results.append(
            {
                "condition_id": cid,
                "action": "delete",
                "deleted": deleted,
                "reason": "reject_or_invalid",
                "ai_decision": decision,
                "ai_odds_event_id": eid or None,
            }
        )

    matched = sum(1 for r in results if r.get("action") == "upsert")
    deleted_n = sum(1 for r in results if r.get("action") == "delete" and r.get("deleted"))
    deletes_calls = sum(1 for r in results if r.get("action") == "delete")
    rejects_recorded = sum(
        1 for r in results if r.get("action") == "delete" and r.get("reason") in ("no_ai_row", "reject_or_invalid")
    )
    _api_log_info(
        "aplicado tasks=%s matched=%s delete_calls=%s delete_removed_existing=%s ai_rejects_saved=%s ai_rows=%s",
        len(tasks),
        matched,
        deletes_calls,
        deleted_n,
        rejects_recorded,
        len(arr),
    )
    return {
        "ok": True,
        "skipped": False,
        "openrouter_model": _OPENROUTER_MODEL,
        "openrouter_finish_reason": finish_reason,
        "manual_matches_path": str(manual_matches_path()),
        "pending_path": str(pending_matches_path()),
        "tasks_count": len(tasks),
        "ai_decision_rows": len(arr),
        "matched": matched,
        "deleted_existing": deleted_n,
        "delete_calls": deletes_calls,
        "ai_rejects_recorded": rejects_recorded,
        "ai_rejects_path": str(ai_rejects_path()),
        "results": results,
    }
