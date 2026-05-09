"""Persistencia y validación de ``sixcycle_config.json`` (filtros + trading Sixcycle)."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lab.paths import data_dir

log = logging.getLogger("sixcycle_config_store")

_CONFIG_LOCK = threading.RLock()
_LAST_UPDATED_ISO: str | None = None

CONFIG_PATH = data_dir() / "sixcycle_config.json"
VERSION_LOG_PATH = data_dir() / "sixcycle_config_versions.jsonl"
_VERSION_LOG_MAX_BYTES = 800_000

DEFAULT_SIXCYCLE_CONFIG: dict[str, Any] = {
    "fill_min": 0.12,
    "fill_dead_zone_low": 0.18,
    "fill_dead_zone_high": 0.24,
    "fill_max": 0.40,
    "liquidity_max": 1500.0,
    "score_min_abs": 3,
    "timing_max_minutes": 3.0,
    "timing_dead_zone_low": 2.5,
    "timing_dead_zone_high": 3.0,
    "clob_threshold_high": 0.65,
    "clob_threshold_low": 0.35,
    "clob_extreme_high": 0.70,
    "clob_extreme_low": 0.30,
    "min_edge": 0.05,
    "max_stake_usdc": 5.0,
    "kelly_fraction": 0.25,
    "stake_usdc": 1.0,
    "max_daily_loss_usdc": 20.0,
    "max_concurrent_trades": 1,
    "dry_run": True,
    "enabled": True,
    "profile_slug": "default",
    "profile_note": "",
}

# Claves persistidas (enabled/dry_run se guardan para referencia; enabled vivo viene de strategy_state).
_CONFIG_KEYS = frozenset(DEFAULT_SIXCYCLE_CONFIG.keys())
# Parámetros que definen la «versión» para analítica (excluye enabled: espejo de strategy_state).
_FINGERPRINT_KEYS = frozenset(_CONFIG_KEYS - {"enabled", "profile_slug", "profile_note"})
_PROFILE_SLUG_RE = re.compile(r"^[a-zA-Z0-9._-]{1,64}$")

SCHEMA: dict[str, dict[str, Any]] = {
    "fill_min": {"type": "float", "min": 0.0, "max": 1.0, "description": "Fill mínimo permitido (lado apostado)"},
    "fill_dead_zone_low": {"type": "float", "min": 0.0, "max": 1.0, "description": "Inicio zona muerta fill"},
    "fill_dead_zone_high": {"type": "float", "min": 0.0, "max": 1.0, "description": "Fin zona muerta fill"},
    "fill_max": {"type": "float", "min": 0.0, "max": 1.0, "description": "Fill máximo permitido"},
    "liquidity_max": {"type": "float", "min": 1.0, "max": 1e9, "description": "Liquidez USDC por encima de la cual se veta"},
    "score_min_abs": {"type": "int", "min": 0, "max": 100, "description": "|score| mínimo para no vetar"},
    "timing_max_minutes": {"type": "float", "min": 0.0, "max": 10.0, "description": "Minutos transcurridos máx. antes de veto duro"},
    "timing_dead_zone_low": {"type": "float", "min": 0.0, "max": 10.0, "description": "Inicio zona muerta timing (min)"},
    "timing_dead_zone_high": {"type": "float", "min": 0.0, "max": 10.0, "description": "Fin zona muerta timing (min)"},
    "clob_threshold_high": {"type": "float", "min": 0.01, "max": 0.99, "description": "YES CLOB umbral alto (override extremo)"},
    "clob_threshold_low": {"type": "float", "min": 0.01, "max": 0.99, "description": "YES CLOB umbral bajo (override extremo)"},
    "clob_extreme_high": {"type": "float", "min": 0.01, "max": 0.99, "description": "YES CLOB etiqueta extremo DOWN"},
    "clob_extreme_low": {"type": "float", "min": 0.01, "max": 0.99, "description": "YES CLOB etiqueta extremo UP"},
    "min_edge": {"type": "float", "min": 0.001, "max": 0.5, "description": "Edge mínimo CLOBSignalFilter"},
    "max_stake_usdc": {"type": "float", "min": 0.01, "max": 1e7, "description": "Tope capital Kelly (USDC)"},
    "kelly_fraction": {"type": "float", "min": 0.001, "max": 1.0, "description": "Fracción Kelly"},
    "stake_usdc": {"type": "float", "min": 0.01, "max": 1e7, "description": "Notional USDC por fill (override Kelly)"},
    "max_daily_loss_usdc": {
        "type": "float",
        "min": 0.01,
        "max": 1e9,
        "description": "Circuit breaker: pérdida diaria máx. (USDC) antes de vetar fills",
    },
    "max_concurrent_trades": {
        "type": "int",
        "min": 1,
        "max": 100,
        "description": "Máx. settles simultáneos / posiciones abiertas por motor",
    },
    "dry_run": {"type": "bool", "description": "Paper: sin órdenes reales (ver restricciones LIVE)"},
    "enabled": {"type": "bool", "description": "Espejo deseado; estado vivo en strategy_state"},
    "profile_slug": {
        "type": "str",
        "description": "Nombre de la versión de config (p. ej. timing_v3); va en cada fila CSV/Postgres",
    },
    "profile_note": {"type": "str", "description": "Nota libre para el historial de versiones (opcional)"},
}


def get_config_path() -> Path:
    return CONFIG_PATH


def get_last_updated_iso() -> str | None:
    return _LAST_UPDATED_ISO


def _merge_defaults(disk: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(DEFAULT_SIXCYCLE_CONFIG)
    if not disk:
        return out
    for k, v in disk.items():
        if k in _CONFIG_KEYS:
            out[k] = v
    return out


def _coerce_bool_key(key: str, v: Any) -> Any:
    """Acepta bool o strings típicos si el JSON se editó a mano (p. ej. \"false\")."""
    if key not in ("dry_run", "enabled"):
        return v
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return bool(int(v))
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("false", "0", "no", "off", ""):
            return False
        if s in ("true", "1", "yes", "on"):
            return True
    return v


def _normalize_bool_fields(cfg: dict[str, Any]) -> None:
    for k in ("dry_run", "enabled"):
        if k not in cfg:
            continue
        cfg[k] = _coerce_bool_key(k, cfg[k])


def load_config() -> dict[str, Any]:
    """Lee JSON desde disco; si falta o falla, devuelve defaults."""
    with _CONFIG_LOCK:
        p = CONFIG_PATH
        if not p.is_file():
            return dict(DEFAULT_SIXCYCLE_CONFIG)
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return dict(DEFAULT_SIXCYCLE_CONFIG)
            out = _merge_defaults(raw)
            _normalize_bool_fields(out)
            return out
        except (OSError, json.JSONDecodeError, TypeError) as e:
            log.warning("sixcycle_config load failed: %s", e)
            return dict(DEFAULT_SIXCYCLE_CONFIG)


def validate_config(cfg: dict[str, Any]) -> None:
    """Lanza ValueError si el dict completo es inválido."""
    c = cfg
    fm = float(c["fill_min"])
    fdl = float(c["fill_dead_zone_low"])
    fdh = float(c["fill_dead_zone_high"])
    fx = float(c["fill_max"])
    if not (0 <= fm <= 1 and 0 <= fdl <= 1 and 0 <= fdh <= 1 and 0 <= fx <= 1):
        raise ValueError("fill_* deben estar en [0,1]")
    if not (fdl < fdh):
        raise ValueError("fill_dead_zone_low debe ser < fill_dead_zone_high")
    if not (fm < fx):
        raise ValueError("fill_min debe ser < fill_max")
    if float(c["liquidity_max"]) <= 0:
        raise ValueError("liquidity_max debe ser > 0")
    if int(c["score_min_abs"]) < 0:
        raise ValueError("score_min_abs debe ser >= 0")
    tmax = float(c["timing_max_minutes"])
    tdl = float(c["timing_dead_zone_low"])
    tdh = float(c["timing_dead_zone_high"])
    if not (0 <= tdl <= 10 and 0 <= tdh <= 10 and 0 <= tmax <= 10):
        raise ValueError("timing_* fuera de rango")
    if tdl > tdh:
        raise ValueError("timing_dead_zone_low debe ser <= timing_dead_zone_high")
    cth = float(c["clob_threshold_high"])
    ctl = float(c["clob_threshold_low"])
    ceh = float(c["clob_extreme_high"])
    cel = float(c["clob_extreme_low"])
    for name, v in (
        ("clob_threshold_high", cth),
        ("clob_threshold_low", ctl),
        ("clob_extreme_high", ceh),
        ("clob_extreme_low", cel),
    ):
        if not (0.01 < v < 0.99):
            raise ValueError(f"{name} debe estar en (0.01, 0.99)")
    if ctl >= cth:
        raise ValueError("clob_threshold_low debe ser < clob_threshold_high")
    if cel >= ceh:
        raise ValueError("clob_extreme_low debe ser < clob_extreme_high")
    me = float(c["min_edge"])
    if not (0.001 < me < 0.5):
        raise ValueError("min_edge debe estar en (0.001, 0.5)")
    if float(c["max_stake_usdc"]) <= 0 or float(c["kelly_fraction"]) <= 0:
        raise ValueError("max_stake_usdc y kelly_fraction deben ser positivos")
    if float(c["stake_usdc"]) <= 0:
        raise ValueError("stake_usdc debe ser > 0")
    if float(c["max_daily_loss_usdc"]) <= 0:
        raise ValueError("max_daily_loss_usdc debe ser > 0")
    if int(c["max_concurrent_trades"]) < 1:
        raise ValueError("max_concurrent_trades debe ser >= 1")
    if not isinstance(c.get("dry_run"), bool):
        raise ValueError("dry_run debe ser booleano")
    if not isinstance(c.get("enabled"), bool):
        raise ValueError("enabled debe ser booleano")
    slug = str(c.get("profile_slug") or "default").strip() or "default"
    if not _PROFILE_SLUG_RE.match(slug):
        raise ValueError("profile_slug: 1-64 caracteres [a-zA-Z0-9._-]")
    note = str(c.get("profile_note") or "")
    if len(note) > 2000:
        raise ValueError("profile_note: máximo 2000 caracteres")


def canonical_params_for_fingerprint(cfg: dict[str, Any]) -> dict[str, Any]:
    """Subconjunto estable de parámetros para hash y registro de versiones."""
    out: dict[str, Any] = {}
    for k in sorted(_FINGERPRINT_KEYS):
        if k not in cfg:
            continue
        v = cfg[k]
        if isinstance(v, bool):
            out[k] = bool(v)
        elif isinstance(v, int) and not isinstance(v, bool):
            out[k] = int(v)
        elif isinstance(v, float):
            out[k] = float(v)
        else:
            try:
                if k == "max_concurrent_trades" or k == "score_min_abs":
                    out[k] = int(v)
                else:
                    out[k] = float(v)
            except (TypeError, ValueError):
                out[k] = v
    return out


def config_fingerprint(cfg: dict[str, Any]) -> str:
    """Identificador corto único por conjunto de parámetros (excluye enabled / metadatos de perfil)."""
    canon = canonical_params_for_fingerprint(cfg)
    raw = json.dumps(canon, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _last_version_record() -> dict[str, Any] | None:
    p = VERSION_LOG_PATH
    if not p.is_file():
        return None
    try:
        lines = [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
        if not lines:
            return None
        return json.loads(lines[-1])
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def append_config_version_record(full: dict[str, Any]) -> None:
    """Añade una línea JSON al log cuando cambia la config persistida (evita duplicar guardados idénticos)."""
    fp = config_fingerprint(full)
    slug = str(full.get("profile_slug") or "default").strip() or "default"
    note = str(full.get("profile_note") or "")[:2000]
    params = canonical_params_for_fingerprint(full)
    prev = _last_version_record()
    if prev and prev.get("config_fingerprint") == fp and prev.get("profile_slug") == slug and prev.get("profile_note") == note:
        try:
            if json.dumps(prev.get("params"), sort_keys=True) == json.dumps(params, sort_keys=True):
                return
        except (TypeError, ValueError):
            pass
    rec = {
        "applied_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "profile_slug": slug,
        "profile_note": note,
        "config_fingerprint": fp,
        "params": params,
    }
    line = json.dumps(rec, ensure_ascii=False) + "\n"
    VERSION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with VERSION_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line)
    except OSError as e:
        log.warning("sixcycle version log append failed: %s", e)
        return
    _trim_version_log_if_needed()


def _trim_version_log_if_needed() -> None:
    try:
        p = VERSION_LOG_PATH
        if not p.is_file() or p.stat().st_size <= _VERSION_LOG_MAX_BYTES:
            return
        lines = [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
        keep = lines[-400:]
        p.write_text("\n".join(keep) + ("\n" if keep else ""), encoding="utf-8")
    except OSError:
        pass


def load_version_tail(n: int = 50) -> list[dict[str, Any]]:
    """Últimas n entradas del log de versiones (más reciente al final)."""
    p = VERSION_LOG_PATH
    if not p.is_file():
        return []
    try:
        lines = [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    except OSError:
        return []
    out: list[dict[str, Any]] = []
    for ln in lines[-max(1, n) :]:
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return out


def stats_by_fingerprint_from_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Agrega trades SETTLED win/loss por ``config_fingerprint`` (filas sin huella → ``__legacy__``)."""
    by_fp: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        ph = str(r.get("phase", "")).upper().strip()
        res = str(r.get("resolved", "")).lower().strip()
        if ph != "SETTLED" or res not in ("win", "loss"):
            continue
        fp = str(r.get("config_fingerprint") or "").strip() or "__legacy__"
        by_fp.setdefault(fp, []).append(r)
    out: dict[str, dict[str, Any]] = {}
    for fp, settled in by_fp.items():
        wins = sum(1 for r in settled if str(r.get("resolved", "")).lower() == "win")
        t = len(settled)
        wr = wins / t if t else 0.0
        pnl_sum = 0.0
        stake_sum = 0.0
        for r in settled:
            try:
                pnl_sum += float(r.get("pnl_usdc") or 0.0)
            except (TypeError, ValueError):
                pass
            try:
                stake_sum += float(r.get("stake_usdc") or 0.0)
            except (TypeError, ValueError):
                pass
        ev = pnl_sum / t if t else 0.0
        pnl_norm = pnl_sum / max(stake_sum, 1e-9)
        slug_counts: dict[str, int] = {}
        for r in settled:
            ps = str(r.get("config_profile_slug") or "").strip() or "?"
            slug_counts[ps] = slug_counts.get(ps, 0) + 1
        out[fp] = {
            "trades": t,
            "win_rate": round(wr, 4),
            "ev_per_trade": round(ev, 6),
            "pnl_norm": round(pnl_norm, 6),
            "pnl_sum_usdc": round(pnl_sum, 6),
            "stake_sum_usdc": round(stake_sum, 6),
            "profile_slugs_seen": slug_counts,
        }
    return dict(sorted(out.items(), key=lambda kv: kv[0]))


def merge_and_validate(current: dict[str, Any], partial: dict[str, Any]) -> dict[str, Any]:
    """Fusiona solo claves conocidas, valida el resultado."""
    out = dict(current)
    for k, v in partial.items():
        if k not in _CONFIG_KEYS:
            log.warning("sixcycle_config: clave desconocida ignorada: %s", k)
            continue
        out[k] = _coerce_bool_key(k, v)
    _normalize_bool_fields(out)
    validate_config(out)
    return out


def save_config(full: dict[str, Any]) -> None:
    """Escribe JSON atómico y actualiza last_updated."""
    global _LAST_UPDATED_ISO
    validate_config(full)
    p = CONFIG_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(full, indent=2, sort_keys=True) + "\n"
    fd, tmp = tempfile.mkstemp(prefix="sixcycle_cfg_", suffix=".json", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, p)
    finally:
        if os.path.isfile(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
    with _CONFIG_LOCK:
        _LAST_UPDATED_ISO = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    append_config_version_record(full)


def build_active_filters(cfg: dict[str, Any]) -> list[str]:
    """Textos humanos para agentes / UI."""
    fm = float(cfg["fill_min"])
    fdl = float(cfg["fill_dead_zone_low"])
    fdh = float(cfg["fill_dead_zone_high"])
    fx = float(cfg["fill_max"])
    liq = float(cfg["liquidity_max"])
    sc = int(cfg["score_min_abs"])
    tmax = float(cfg["timing_max_minutes"])
    tdl = float(cfg["timing_dead_zone_low"])
    tdh = float(cfg["timing_dead_zone_high"])
    return [
        f"fill <{fm:.2f} bloqueado",
        f"fill {fdl:.2f}-{fdh:.2f} bloqueado",
        f"fill >{fx:.2f} bloqueado",
        f"liquidez >{liq:.0f} bloqueado",
        f"|score| <{sc} bloqueado",
        f"timing {tdl:.1f}-{tdh:.1f} min bloqueado",
        f"timing >{tmax:.1f} min bloqueado",
    ]


def stats_last_n_from_csv_rows(
    rows: list[dict[str, Any]],
    n: int = 100,
    *,
    config_fingerprint: str | None = None,
) -> dict[str, Any]:
    """
    Últimas n filas SETTLED (win/loss) con stake y pnl.
    pnl_norm = sum(pnl_usdc) / max(sum(stake_usdc), 1e-9) por esas filas.
    Si ``config_fingerprint`` se informa, solo cuenta filas con esa huella.
    """
    fp_filter = (config_fingerprint or "").strip() or None
    settled: list[dict[str, Any]] = []
    for r in reversed(rows):
        if len(settled) >= n:
            break
        ph = str(r.get("phase", "")).upper().strip()
        res = str(r.get("resolved", "")).lower().strip()
        if ph != "SETTLED" or res not in ("win", "loss"):
            continue
        if fp_filter is not None and str(r.get("config_fingerprint") or "").strip() != fp_filter:
            continue
        settled.append(r)
    if not settled:
        return {
            "trades": 0,
            "win_rate": 0.0,
            "ev_per_trade": 0.0,
            "pnl_norm": 0.0,
        }
    wins = sum(1 for r in settled if str(r.get("resolved", "")).lower() == "win")
    t = len(settled)
    wr = wins / t if t else 0.0
    pnl_sum = 0.0
    stake_sum = 0.0
    for r in settled:
        try:
            pnl_sum += float(r.get("pnl_usdc") or 0.0)
        except (TypeError, ValueError):
            pass
        try:
            stake_sum += float(r.get("stake_usdc") or 0.0)
        except (TypeError, ValueError):
            pass
    ev = pnl_sum / t if t else 0.0
    pnl_norm = pnl_sum / max(stake_sum, 1e-9)
    return {
        "trades": t,
        "win_rate": round(wr, 4),
        "ev_per_trade": round(ev, 6),
        "pnl_norm": round(pnl_norm, 6),
    }


def suggest_from_stats(objective: str, stats: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Heurística simple (no optimización garantizada). No aplica cambios.
    """
    wr = float(stats.get("win_rate") or 0.0)
    t = int(stats.get("trades") or 0)
    out: dict[str, Any] = {"suggested": {}, "rationale": {}, "note": "Heurística offline; validar con backtest."}
    s = out["suggested"]
    r = out["rationale"]

    if objective == "maximize_win_rate":
        if t >= 5 and wr < 0.25:
            s["fill_min"] = max(0.05, float(cfg["fill_min"]) - 0.02)
            r["fill_min"] = "Win rate bajo: relajar fill_min ligeramente"
            s["score_min_abs"] = max(1, int(cfg["score_min_abs"]) - 1)
            r["score_min_abs"] = "Permitir scores algo más débiles"
        else:
            out["note"] = "Pocos trades o WR ya aceptable; sin cambios agresivos sugeridos."
        return out

    if objective == "maximize_trades":
        s["score_min_abs"] = max(1, int(cfg["score_min_abs"]) - 1)
        r["score_min_abs"] = "Más trades: bajar |score| mínimo un punto"
        s["timing_max_minutes"] = min(4.5, float(cfg["timing_max_minutes"]) + 0.25)
        r["timing_max_minutes"] = "Ampliar ventana de tiempo marginalmente"
        return out

    if objective == "maximize_ev":
        if t >= 5 and wr > 0.45:
            s["kelly_fraction"] = min(0.5, float(cfg["kelly_fraction"]) + 0.05)
            r["kelly_fraction"] = "WR decente: subir Kelly un poco (riesgo mayor)"
        elif t >= 5 and wr < 0.35:
            s["fill_max"] = min(0.55, float(cfg["fill_max"]) + 0.02)
            r["fill_max"] = "WR bajo: permitir fills menos extremos"
        else:
            out["note"] = "Datos insuficientes o mixtos; revisar CSV manualmente."
        return out

    raise ValueError(
        f"objective inválido: {objective!r}; use maximize_win_rate | maximize_trades | maximize_ev"
    )
