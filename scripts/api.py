#!/usr/bin/env python3
"""
API FastAPI: dashboard HTML + control del proceso validate_edge (subprocess).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import csv
import hashlib
import hmac
import json
import logging
import secrets
import math
import os
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import aiohttp
import uvicorn
from fastapi import Body, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette import status as starlette_status
from starlette.middleware.base import BaseHTTPMiddleware

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lab.paths import data_dir
from scripts.debug_markets import run_polymarket_market_debug
from risk.ml_model_registry import ML_MODELS, ML_MODEL_SLUGS
from risk.model_state import ModelStateManager

log = logging.getLogger("api")
log.setLevel(logging.INFO)
if not log.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [api] %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    )
    logging.Formatter.converter = time.gmtime
    log.addHandler(_h)
log.propagate = False

DATA_DIR = data_dir()
# Entrada fija (sin .env): mismo comportamiento en local y en deploy.
API_PORT = 8080
VALIDATOR_HEALTH_PORT = 18088
AUTO_START = False
ARB_ENGINE_DRY_RUN = True

SIGNALS_CSV = DATA_DIR / "logs" / "signals.csv"
STATIC_DIR = REPO_ROOT / "static"
ML_MODELS_HTML = STATIC_DIR / "ml_models.html"
ML_MODEL_DETAIL_HTML = STATIC_DIR / "ml_model_detail.html"
HOME_HTML = STATIC_DIR / "home.html"
DEFAULT_ML_MODEL_SLUG = "crypto_5m_lgbm"
ARB_HTML = STATIC_DIR / "arb.html"
ARB_STRATEGY_DETAIL_HTML = STATIC_DIR / "arb_strategy_detail.html"
LATENCY_SPORTS_HTML = STATIC_DIR / "latency_sports.html"

STRATEGY_SLUGS = [
    "bundle_arb",
    "cross_exchange",
    "market_maker",
    "combinatorial_arb",
    "term_structure",
    "latency_arb",
    "latency_arb_sports",
    "crypto_5m_sixcycle",
]
SIXCYCLE_SLUG = "crypto_5m_sixcycle"
ARB_CSV_PATHS = {slug: DATA_DIR / "logs" / f"{slug}.csv" for slug in STRATEGY_SLUGS}
SIXCYCLE_HTML = STATIC_DIR / "sixcycle.html"
SIXCYCLE_SIGNALS_CSV = DATA_DIR / "sixcycle_signals.csv"
SIXCYCLE_ENGINE_CSV_FALLBACK = DATA_DIR / "logs" / "crypto_5m_sixcycle.csv"
BUNDLE_ARB_SCAN_JSON = DATA_DIR / "logs" / "bundle_arb_scan.json"
LATENCY_ARB_SPORTS_SNAPSHOTS_CSV = DATA_DIR / "logs" / "latency_arb_sports_snapshots.csv"
LATENCY_SPORTS_CYCLE_METRICS_JSON = DATA_DIR / "logs" / "latency_arb_sports_cycle_metrics.json"
LATENCY_SPORTS_SCHEDULE_JSON = DATA_DIR / "logs" / "latency_sports_schedule.json"
LATENCY_SPORTS_PENDING_JSON = DATA_DIR / "logs" / "latency_arb_sports_pending_matches.json"

# HTTP Basic opcional: credenciales solo por entorno (nunca en el repo).
API_HTTP_BASIC_USER = (os.getenv("API_HTTP_BASIC_USER", "admin") or "admin").strip()
API_HTTP_BASIC_PASSWORD = os.getenv("API_HTTP_BASIC_PASSWORD", "").strip()
PM_AUTH_COOKIE_NAME = "pm_dash_auth"


def _auth_session_ttl_sec() -> int:
    try:
        days = int(os.getenv("API_AUTH_SESSION_DAYS", "30").strip())
    except ValueError:
        days = 30
    return max(1, min(days, 365)) * 86400


def _sign_auth_session(username: str, password: str, ttl_sec: int) -> str:
    exp = int(time.time()) + ttl_sec
    msg = f"v1|{exp}|{username}".encode("utf-8")
    sig = hmac.new(password.encode("utf-8"), msg, hashlib.sha256).digest()
    return f"{exp}|{username}|{base64.urlsafe_b64encode(sig).decode().rstrip('=')}"


def _auth_session_cookie_ok(request: Request, username: str, password: str) -> bool:
    raw = request.cookies.get(PM_AUTH_COOKIE_NAME)
    if not raw or raw.count("|") != 2:
        return False
    exp_s, user, sig_b64 = raw.split("|", 2)
    try:
        exp = int(exp_s)
    except ValueError:
        return False
    if time.time() > exp:
        return False
    if not secrets.compare_digest(user, username):
        return False
    pad = "=" * (-len(sig_b64) % 4)
    try:
        sig_got = base64.urlsafe_b64decode(sig_b64 + pad)
    except (ValueError, binascii.Error, TypeError):
        return False
    msg = f"v1|{exp}|{username}".encode("utf-8")
    sig_exp = hmac.new(password.encode("utf-8"), msg, hashlib.sha256).digest()
    return secrets.compare_digest(sig_got, sig_exp)


def _basic_credentials_ok(request: Request, username: str, password: str) -> bool:
    raw = request.headers.get("authorization")
    if not raw or not raw.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(raw.split(" ", 1)[1].strip(), validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return False
    user, sep, pw = decoded.partition(":")
    if not sep:
        return False
    return secrets.compare_digest(user, username) and secrets.compare_digest(pw, password)


def _http_basic_challenge_response() -> JSONResponse:
    return JSONResponse(
        {"detail": "Unauthorized"},
        status_code=starlette_status.HTTP_401_UNAUTHORIZED,
        headers={"WWW-Authenticate": 'Basic realm="PredMarket"'},
    )


class _HttpBasicAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, username: str, password: str, session_ttl_sec: int):
        super().__init__(app)
        self._username = username
        self._password = password
        self._session_ttl_sec = session_ttl_sec

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        if request.method == "OPTIONS":
            return await call_next(request)
        path = request.url.path
        if path == "/health" or path.startswith("/static/") or path == "/favicon.ico":
            return await call_next(request)
        if _auth_session_cookie_ok(request, self._username, self._password):
            return await call_next(request)
        if _basic_credentials_ok(request, self._username, self._password):
            resp = await call_next(request)
            token = _sign_auth_session(self._username, self._password, self._session_ttl_sec)
            secure = request.url.scheme == "https" or (
                request.headers.get("x-forwarded-proto", "").lower() == "https"
            )
            resp.set_cookie(
                PM_AUTH_COOKIE_NAME,
                token,
                max_age=self._session_ttl_sec,
                httponly=True,
                secure=secure,
                samesite="lax",
                path="/",
            )
            return resp
        return _http_basic_challenge_response()


def _read_latency_sports_cycle_metrics() -> dict[str, Any]:
    """Último ciclo escrito por latency_arb_sports (motor); vacío si no hay archivo."""
    p = LATENCY_SPORTS_CYCLE_METRICS_JSON
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError, TypeError):
        return {}


def signals_path() -> Path:
    return SIGNALS_CSV


def read_signals_df() -> pd.DataFrame:
    from persistence.config import primary_store_postgres
    from persistence.readers import load_signals_dataframe_from_postgres

    if primary_store_postgres():
        try:
            dfp = load_signals_dataframe_from_postgres()
            if not dfp.empty:
                return dfp
        except Exception:
            log.exception("read_signals_df: fallo leyendo Postgres; fallback CSV")
    p = signals_path()
    if not p.is_file():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, dtype=str, keep_default_na=False)
    except Exception:
        return pd.DataFrame()


def _float_opt(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", "nat", "<na>"):
        return None
    try:
        v = float(s)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except ValueError:
        return None


def _json_cell(x: Any) -> Any:
    """Valor serializable a JSON (sin numpy NaN sueltos)."""
    try:
        if pd.isna(x):
            return None
    except TypeError:
        pass
    if x is None:
        return None
    if isinstance(x, (bool, np.bool_)):  # type: ignore[arg-type]
        return bool(x)
    if isinstance(x, (int, np.integer)):  # type: ignore[arg-type]
        return int(x)
    if isinstance(x, (float, np.floating)):  # type: ignore[arg-type]
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", "nat", "<na>"):
        return None
    return s


def _debug_row_dict(row: pd.Series) -> dict[str, Any]:
    rec: dict[str, Any] = {str(c): _json_cell(row[c]) for c in row.index}
    gap_nr = _float_opt(row.get("gap_nearres"))
    wr = _float_opt(row.get("window_return"))
    mins = _float_opt(row.get("minutes_elapsed"))
    pnr = _float_opt(row.get("p_nearres"))
    pmc = _float_opt(row.get("p_mercado"))
    rec["gap_nr_pct"] = (gap_nr * 100.0) if gap_nr is not None else None
    rec["window_return_pct"] = (wr * 100.0) if wr is not None else None
    rec["minutes_at_signal"] = mins
    rec["p_nearres_raw"] = pnr
    rec["p_mercado_raw"] = pmc
    return rec


def build_debug_signals_rows(last_n: int = 20) -> list[dict[str, Any]]:
    """
    Últimas N filas del CSV con columnas extra para diagnosticar NearRes / ventana / mercado.
    """
    df = read_signals_df()
    if df.empty:
        return []
    return [_debug_row_dict(r) for _, r in df.tail(int(last_n)).iterrows()]


def build_debug_last_ml_nearres_rows(signal_n: int = 10) -> list[dict[str, Any]]:
    """Últimas N filas con signal ML o NEARRES en todo el CSV (mismas columnas calculadas)."""
    df = read_signals_df()
    if df.empty or "signal" not in df.columns:
        return []
    sub = df[df["signal"].isin(("ML", "NEARRES"))]
    if sub.empty:
        return []
    return [_debug_row_dict(r) for _, r in sub.tail(int(signal_n)).iterrows()]


def _parse_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def today_mask_utc(df: pd.DataFrame) -> pd.Series:
    if df.empty or "timestamp" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    ts = _parse_ts(df["timestamp"])
    today = pd.Timestamp.now(tz="UTC").normalize().date()
    return ts.dt.date == today


def _mask_result_resolved_for_accuracy(result: pd.Series) -> pd.Series:
    """Fila usable para accuracy: result es Up/Down real, no vacío ni literal 'None'."""
    r = result.astype(str).str.strip()
    bad = r.isin(("", "none", "None", "NaT", "nan", "NaN", "<NA>"))
    return ~bad & r.isin(("Up", "Down"))


def _dedupe_by_condition_first(sub: pd.DataFrame) -> pd.DataFrame:
    if sub.empty or "condition_id" not in sub.columns:
        return sub
    out = sub.copy()
    if "timestamp" in out.columns:
        out["_ts"] = _parse_ts(out["timestamp"])
        out = out.sort_values("_ts")
    return out.groupby("condition_id", as_index=False).first()


def _accuracy_for_signal(df: pd.DataFrame, sig: str) -> Tuple[Optional[float], int]:
    """
    Precisión = aciertos / N con result resuelto (Up/Down), tras deduplicar por condition_id.
    Si N == 0 → (None, 0) para mostrar '—' en UI, no 0%% por artefacto.
    """
    need = {"signal", "direction", "result"}
    if df.empty or not need.issubset(df.columns):
        return None, 0
    m = (df["signal"] == sig) & _mask_result_resolved_for_accuracy(df["result"])
    sub = df.loc[m].copy()
    if sub.empty:
        return None, 0
    sub = _dedupe_by_condition_first(sub)
    n = len(sub)
    if n == 0:
        return None, 0
    d = sub["direction"].astype(str).str.strip()
    res = sub["result"].astype(str).str.strip()
    correct = int((d == res).sum())
    return float(correct) / float(n), n


def accuracies(df: pd.DataFrame) -> tuple[Optional[float], Optional[float], int, int]:
    ml_acc, ml_n = _accuracy_for_signal(df, "ML")
    nr_acc, nr_n = _accuracy_for_signal(df, "NEARRES")
    return ml_acc, nr_acc, ml_n, nr_n


def last_signal_dict(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    need = {"signal", "direction", "timestamp", "asset"}
    if df.empty or not need.issubset(df.columns):
        return None
    sub = df[df["signal"].isin(("ML", "NEARRES"))].copy()
    if sub.empty:
        return None
    sub["_ts"] = _parse_ts(sub["timestamp"])
    row = sub.sort_values("_ts").iloc[-1]
    gap = 0.0
    for col in ("gap_ml", "gap_nearres"):
        if col in row.index and row[col] != "":
            try:
                gap = max(gap, abs(float(row[col])))
            except ValueError:
                pass
    return {
        "asset": str(row["asset"]),
        "direction": str(row["direction"]),
        "gap": gap,
        "type": str(row["signal"]),
        "timestamp": str(row["timestamp"]),
    }


class ValidatorSupervisor:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._proc: Optional[subprocess.Popen[Any]] = None
        self._started_monotonic: Optional[float] = None

    def is_running(self) -> bool:
        with self._lock:
            if self._proc is None:
                return False
            if self._proc.poll() is not None:
                self._proc = None
                self._started_monotonic = None
                return False
            return True

    def pid(self) -> Optional[int]:
        with self._lock:
            if self._proc is None or self._proc.poll() is not None:
                return None
            return int(self._proc.pid)

    def uptime_seconds(self) -> float:
        with self._lock:
            if self._proc is None or self._started_monotonic is None:
                return 0.0
            if self._proc.poll() is not None:
                return 0.0
            return max(0.0, time.monotonic() - self._started_monotonic)

    def start(self) -> tuple[bool, Optional[int], Optional[str]]:
        with self._lock:
            if self._proc is not None and self._proc.poll() is None:
                return False, self._proc.pid, "already_running"
            cmd = [
                sys.executable,
                str(REPO_ROOT / "scripts" / "validate_edge.py"),
                "--hours",
                "168",
            ]
            try:
                child_env = os.environ.copy()
                # validate_edge también abre /health; evitar colisión con el PORT del API
                child_env["PORT"] = str(VALIDATOR_HEALTH_PORT)
                # Heredar stdout/stderr del API para que los logs de validate_edge
                # (logging + Rich) salgan en los Deploy Logs de Railway.
                self._proc = subprocess.Popen(
                    cmd,
                    cwd=str(REPO_ROOT),
                    env=child_env,
                    stdout=None,
                    stderr=None,
                    start_new_session=True,
                )
                self._started_monotonic = time.monotonic()
                return True, self._proc.pid, None
            except OSError as e:
                self._proc = None
                self._started_monotonic = None
                return False, None, str(e)

    def stop(self) -> tuple[bool, Optional[str]]:
        with self._lock:
            if self._proc is None or self._proc.poll() is not None:
                self._proc = None
                self._started_monotonic = None
                return False, "not_running"
            try:
                self._proc.terminate()
                try:
                    self._proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    self._proc.kill()
                    self._proc.wait(timeout=5)
            except OSError as e:
                return False, str(e)
            finally:
                self._proc = None
                self._started_monotonic = None
            return True, None


supervisor = ValidatorSupervisor()
_model_state_manager = ModelStateManager()


def build_status_payload() -> dict[str, Any]:
    df = read_signals_df()
    today = today_mask_utc(df)
    if df.empty:
        signals_today = 0
    elif "signal" in df.columns:
        sig_today = df["signal"].isin(("ML", "NEARRES")) & today
        signals_today = int(sig_today.sum())
    else:
        signals_today = int(today.sum())
    resolved_today = 0
    if not df.empty and "result" in df.columns:
        resolved_today = int(((df["result"].isin(("Up", "Down"))) & today).sum())
    ml_acc, nr_acc, ml_resolved_n, nr_resolved_n = accuracies(df)
    p_csv = signals_path()
    csv_rows = int(len(df))
    last_ts: Optional[str] = None
    if csv_rows and "timestamp" in df.columns:
        last_ts = str(df["timestamp"].iloc[-1])
    return {
        "running": supervisor.is_running(),
        "uptime_seconds": int(supervisor.uptime_seconds()),
        "pid": supervisor.pid(),
        "signals_today": signals_today,
        "resolved_today": resolved_today,
        "ml_accuracy": round(ml_acc, 4) if ml_acc is not None else None,
        "nearres_accuracy": round(nr_acc, 4) if nr_acc is not None else None,
        "ml_resolved_count": int(ml_resolved_n),
        "nearres_resolved_count": int(nr_resolved_n),
        "last_signal": last_signal_dict(df),
        # Diagnóstico: si csv_rows sube con el tiempo, el worker está escribiendo
        # (normalmente tras leer Gamma + mercados filtrados y precios).
        "signals_csv": str(p_csv),
        "csv_rows": csv_rows,
        "last_csv_timestamp": last_ts,
    }


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    from arb.latency_sports_schedule import scheduler_env_enabled, scheduler_poll_sec

    log.info(
        "API arrancando: DATA_DIR=%s PORT=%s AUTO_START=%s VALIDATOR_HEALTH_PORT=%s",
        DATA_DIR,
        API_PORT,
        AUTO_START,
        VALIDATOR_HEALTH_PORT,
    )
    if AUTO_START:
        ok, pid, err = supervisor.start()
        if ok:
            log.info("AUTO_START: validate_edge en marcha pid=%s (logs mezclados en stderr)", pid)
        elif err == "already_running":
            log.info("AUTO_START omitido: validate_edge ya estaba en marcha pid=%s", pid)
        elif err:
            log.error("AUTO_START falló: %s", err)
    else:
        log.info("AUTO_START desactivado: pulsa START en el dashboard o llama POST /api/start")
    try:
        (DATA_DIR / "logs").mkdir(parents=True, exist_ok=True)
    except OSError as e:
        log.warning("No se pudo asegurar DATA_DIR/logs (%s): %s", DATA_DIR / "logs", e)
    sched_task: Optional[asyncio.Task[None]] = None
    if scheduler_env_enabled():
        log.info(
            "LATENCY_SPORTS_SCHEDULER activo: ajustará enabled de latency_arb_sports cada ~%ss (POST /api/arb/start sigue siendo manual)",
            int(scheduler_poll_sec()),
        )
        sched_task = asyncio.create_task(_latency_sports_scheduler_loop(), name="latency_sports_scheduler")
    try:
        yield
    finally:
        if sched_task is not None:
            sched_task.cancel()
            try:
                await sched_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning("latency_sports_scheduler join: %s", e)
    if supervisor.is_running():
        log.info("Apagado API: deteniendo validate_edge…")
        supervisor.stop()
        log.info("validate_edge detenido")
    try:
        await _sixcycle_stop()
    except Exception as e:
        log.warning("Apagado API: sixcycle stop: %s", e)
    ok_arb, err_arb = _arb_engine_stop()
    if ok_arb:
        log.info("arb_engine detenido")
    elif err_arb and err_arb != "not_running":
        log.warning("arb_engine stop: %s", err_arb)


app = FastAPI(title="PredMarket Arb API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
if API_HTTP_BASIC_PASSWORD:
    _ttl = _auth_session_ttl_sec()
    app.add_middleware(
        _HttpBasicAuthMiddleware,
        username=API_HTTP_BASIC_USER,
        password=API_HTTP_BASIC_PASSWORD,
        session_ttl_sec=_ttl,
    )
    log.info(
        "HTTP Basic activo (usuario=%r). Tras el primer login el navegador guarda sesión ~%d días "
        "(cookie HttpOnly; EventSource incluye la cookie). Público: /health, /static/*.",
        API_HTTP_BASIC_USER,
        max(1, _ttl // 86400),
    )


@app.get("/", response_model=None)
async def root() -> Union[FileResponse, JSONResponse]:
    """Hub de inicio: enlaces a ML, Arb, resumen y seguridad."""
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    if not HOME_HTML.is_file():
        return JSONResponse({"detail": "static/home.html not found"}, status_code=404)
    return FileResponse(path=str(HOME_HTML), media_type="text/html; charset=utf-8")


@app.get("/ml", response_model=None)
async def ml_models_index_page() -> Union[FileResponse, JSONResponse]:
    """Catálogo ML (`/ml`): toggles + enlaces al monitor por slug; análogo al índice `/arb`."""
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    if not ML_MODELS_HTML.is_file():
        return JSONResponse({"detail": "static/ml_models.html not found"}, status_code=404)
    return FileResponse(path=str(ML_MODELS_HTML), media_type="text/html; charset=utf-8")


@app.get("/ml/model/{slug}", response_model=None)
async def ml_model_detail_page(slug: str) -> Union[FileResponse, JSONResponse]:
    if slug not in ML_MODEL_SLUGS:
        return JSONResponse({"detail": "unknown model slug"}, status_code=404)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    if not ML_MODEL_DETAIL_HTML.is_file():
        return JSONResponse({"detail": "static/ml_model_detail.html not found"}, status_code=404)
    return FileResponse(path=str(ML_MODEL_DETAIL_HTML), media_type="text/html; charset=utf-8")


@app.get("/arb", response_model=None)
async def arb_dashboard() -> Union[FileResponse, JSONResponse]:
    """UI del motor Arb (CLOB); separada del monitor validador ML (`/` y `/ml`)."""
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    if not ARB_HTML.is_file():
        return JSONResponse({"detail": "static/arb.html not found"}, status_code=404)
    return FileResponse(path=str(ARB_HTML), media_type="text/html; charset=utf-8")


@app.get("/arb/strategy/latency_arb_sports", response_model=None)
async def arb_latency_sports_detail_page() -> Union[FileResponse, JSONResponse]:
    """UI dedicada Latency Arb — Sports (tabla, gráfico edge, SSE)."""
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    if not LATENCY_SPORTS_HTML.is_file():
        return JSONResponse({"detail": "static/latency_sports.html not found"}, status_code=404)
    return FileResponse(path=str(LATENCY_SPORTS_HTML), media_type="text/html; charset=utf-8")


@app.get("/api/arb/latency_sports/schedule")
async def api_latency_sports_schedule(refresh: bool = Query(False)) -> JSONResponse:
    """
    Próximos partidos desde Gamma (Polymarket) + flags del planificador LATENCY_SPORTS_SCHEDULER.
    Sin odds-api.io. Caché ~50s; `refresh=true` fuerza nueva consulta Gamma.
    """
    payload = await refresh_latency_sports_schedule(force=refresh)
    return JSONResponse(
        content=payload,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


def _read_latency_sports_pending_matches() -> dict[str, Any]:
    p = LATENCY_SPORTS_PENDING_JSON
    if not p.is_file():
        return {"pending": [], "pending_count": 0, "updated_at": None}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {"pending": [], "pending_count": 0, "updated_at": None}
    if not isinstance(raw, dict):
        return {"pending": [], "pending_count": 0, "updated_at": None}
    from arb.latency_sports_ai_rejects import load_ai_rejected_condition_ids

    rejected = load_ai_rejected_condition_ids()
    pending_list = raw.get("pending")
    if not isinstance(pending_list, list) or not rejected:
        return raw
    filtered: list[Any] = []
    hidden = 0
    for row in pending_list:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("condition_id") or "").strip()
        if cid and cid in rejected:
            hidden += 1
            continue
        filtered.append(row)
    out = dict(raw)
    out["pending"] = filtered
    out["pending_count"] = len(filtered)
    if hidden:
        out["ai_rejected_hidden_in_ui"] = int(hidden)
    else:
        out.pop("ai_rejected_hidden_in_ui", None)
    return out


@app.get("/api/arb/latency_sports/pending-matches")
async def api_latency_sports_pending_matches() -> JSONResponse:
    """Snapshot del motor; oculta en cliente los ``condition_id`` en ``latency_sports_ai_rejected.json`` (IA descarte)."""
    return JSONResponse(
        content=_read_latency_sports_pending_matches(),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


@app.post("/api/arb/latency_sports/ai-match-run")
async def api_latency_sports_ai_match_run() -> JSONResponse:
    """
    OpenRouter (modelo hardcodeado en código): lee pending matches y escribe
    ``latency_sports_manual_matches.json`` (upsert si match; borra entrada si reject / sin fila IA / id inválido).
    """
    from arb.latency_sports_ai_openrouter import run_openrouter_on_pending

    log.info("POST /api/arb/latency_sports/ai-match-run (OpenRouter matching)")
    try:
        out = await run_openrouter_on_pending()
    except RuntimeError as e:
        log.warning("ai-match-run RuntimeError: %s", e)
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        log.exception("ai-match-run error")
        raise HTTPException(status_code=500, detail=str(e)) from e
    log.info(
        "ai-match-run OK skipped=%s tasks=%s matched=%s deleted_existing=%s",
        out.get("skipped"),
        out.get("tasks_count"),
        out.get("matched"),
        out.get("deleted_existing"),
    )
    return JSONResponse(
        content=out,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


@app.get("/api/arb/latency_sports/edge-history")
async def api_latency_sports_edge_history(hours: int = Query(24, ge=1, le=168)) -> JSONResponse:
    """
    Resumen de últimas N horas leyendo ``latency_arb_sports.csv``:
    señales emitidas vs descartes (LOW_EDGE / STALE / etc.) + buckets por hora UTC para gráficas.
    Pensado para el panel "Edges detectados" del dashboard latency_arb_sports.
    """
    csv_path = ARB_CSV_PATHS.get("latency_arb_sports")
    if csv_path is None or not csv_path.is_file():
        return JSONResponse(content={
            "available": False,
            "hours": int(hours),
            "signals": 0,
            "skip_low_edge": 0,
            "skip_stale_for_signal": 0,
            "skip_ref_dead": 0,
            "skip_total": 0,
            "errors": 0,
            "best_edge": None,
            "avg_edge_signal": None,
            "avg_latency_tick_to_signal_ms": None,
            "buckets": [],
            "recent_signals": [],
        })
    rows = _read_arb_csv_tail(csv_path, n=15000)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=int(hours))
    bucket_counts: dict[str, dict[str, int]] = {}
    sig_count = skip_low = skip_stale = skip_ref_dead = err_count = skip_total = 0
    edges_signal: list[float] = []
    lat_tick: list[float] = []
    best_edge: Optional[float] = None
    recent_signals: list[dict[str, Any]] = []
    for r in rows:
        ts_raw = str(r.get("ts") or "").strip()
        if not ts_raw:
            continue
        try:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if ts < cutoff:
            continue
        action = str(r.get("action") or "").strip().upper()
        bk_key = ts.strftime("%Y-%m-%dT%H:00:00Z")
        bk = bucket_counts.setdefault(bk_key, {"signal": 0, "skip_low_edge": 0, "skip_other": 0, "error": 0})
        if action == "SIGNAL":
            sig_count += 1
            bk["signal"] += 1
            try:
                e = float(r.get("edge") or r.get("edge_exec") or 0.0)
                edges_signal.append(e)
                if best_edge is None or e > best_edge:
                    best_edge = e
            except (TypeError, ValueError):
                pass
            try:
                lat = float(r.get("latency_ms_tick_to_signal") or 0.0)
                if lat > 0:
                    lat_tick.append(lat)
            except (TypeError, ValueError):
                pass
            if len(recent_signals) < 25:
                recent_signals.append(
                    {
                        "ts": ts_raw,
                        "side": r.get("side"),
                        "home_team": r.get("home_team"),
                        "away_team": r.get("away_team"),
                        "league": r.get("league"),
                        "edge": r.get("edge"),
                        "edge_exec": r.get("edge_exec"),
                        "edge_mid": r.get("edge_mid"),
                        "price_poly": r.get("price_poly"),
                        "prob_pinnacle": r.get("prob_pinnacle"),
                        "latency_ms_tick_to_signal": r.get("latency_ms_tick_to_signal"),
                        "trigger": r.get("trigger"),
                    }
                )
        elif action == "SKIP:LOW_EDGE":
            skip_low += 1
            skip_total += 1
            bk["skip_low_edge"] += 1
        elif action == "SKIP:STALE_FOR_SIGNAL":
            skip_stale += 1
            skip_total += 1
            bk["skip_other"] += 1
        elif action == "SKIP:REF_DEAD_NO_TICKS":
            skip_ref_dead += 1
            skip_total += 1
            bk["skip_other"] += 1
        elif action.startswith("SKIP"):
            skip_total += 1
            bk["skip_other"] += 1
        elif action.startswith("ERROR"):
            err_count += 1
            bk["error"] += 1
    buckets_sorted = [
        {"hour_utc": k, **bucket_counts[k]} for k in sorted(bucket_counts.keys())
    ]
    avg_edge = round(sum(edges_signal) / len(edges_signal), 6) if edges_signal else None
    avg_lat = round(sum(lat_tick) / len(lat_tick), 1) if lat_tick else None
    return JSONResponse(content={
        "available": True,
        "hours": int(hours),
        "signals": sig_count,
        "skip_low_edge": skip_low,
        "skip_stale_for_signal": skip_stale,
        "skip_ref_dead": skip_ref_dead,
        "skip_total": skip_total,
        "errors": err_count,
        "best_edge": best_edge,
        "avg_edge_signal": avg_edge,
        "avg_latency_tick_to_signal_ms": avg_lat,
        "buckets": buckets_sorted,
        "recent_signals": recent_signals,
    })


class LatencySportsManualMatchIn(BaseModel):
    condition_id: str = Field(..., min_length=1)
    odds_event_id: str = Field(..., min_length=1)
    swap_sides: bool = False
    poly_home: str = ""
    poly_away: str = ""


class SixcycleDryStakeMultiplierIn(BaseModel):
    """Factor sobre el stake Kelly en paper (``scripts/sixcycle_engine`` DRY_RUN)."""

    multiplier: float = Field(1.0, ge=0.01, le=50000.0)


@app.get("/api/arb/latency_sports/manual-matches")
async def api_latency_sports_manual_matches_list() -> JSONResponse:
    from arb.latency_sports_manual_match import list_manual_matches_for_api

    return JSONResponse(
        content={"items": list_manual_matches_for_api()},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


@app.post("/api/arb/latency_sports/manual-match")
async def api_latency_sports_manual_match_save(body: LatencySportsManualMatchIn) -> JSONResponse:
    from arb.latency_sports_manual_match import upsert_manual_match

    try:
        row = upsert_manual_match(
            body.condition_id.strip(),
            body.odds_event_id.strip(),
            swap_sides=body.swap_sides,
            poly_home=body.poly_home,
            poly_away=body.poly_away,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return JSONResponse(content={"ok": True, "item": row})


@app.delete("/api/arb/latency_sports/ai-reject/{condition_id}")
async def api_latency_sports_ai_reject_delete(condition_id: str) -> JSONResponse:
    """Quita un condition_id de la lista IA-descartada para que vuelva a aparecer como pendiente si aplica."""
    from arb.latency_sports_ai_rejects import clear_ai_reject

    ok = clear_ai_reject(condition_id.strip())
    if not ok:
        raise HTTPException(status_code=404, detail="condition_id not in ai rejects")
    return JSONResponse(content={"ok": True})


@app.delete("/api/arb/latency_sports/manual-match/{condition_id}")
async def api_latency_sports_manual_match_delete(condition_id: str) -> JSONResponse:
    from arb.latency_sports_manual_match import delete_manual_match

    ok = delete_manual_match(condition_id)
    if not ok:
        raise HTTPException(status_code=404, detail="condition_id not in manual matches")
    return JSONResponse(content={"ok": True})


@app.get("/arb/strategy/{slug}", response_model=None)
async def arb_strategy_detail_page(slug: str) -> Union[FileResponse, JSONResponse]:
    """Vista detallada de una estrategia del arb (paper / CSV / feed filtrado en cliente)."""
    if slug not in STRATEGY_SLUGS:
        return JSONResponse({"detail": "unknown strategy slug"}, status_code=404)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    if slug == SIXCYCLE_SLUG:
        if not SIXCYCLE_HTML.is_file():
            return JSONResponse({"detail": "static/sixcycle.html not found"}, status_code=404)
        return FileResponse(path=str(SIXCYCLE_HTML), media_type="text/html; charset=utf-8")
    if not ARB_STRATEGY_DETAIL_HTML.is_file():
        return JSONResponse({"detail": "static/arb_strategy_detail.html not found"}, status_code=404)
    return FileResponse(path=str(ARB_STRATEGY_DETAIL_HTML), media_type="text/html; charset=utf-8")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/status")
async def api_status() -> dict[str, Any]:
    return build_status_payload()


@app.post("/api/start")
async def api_start() -> dict[str, Any]:
    started, pid, err = supervisor.start()
    if started:
        log.info("POST /api/start -> validate_edge pid=%s", pid)
        return {"started": True, "pid": pid}
    if err == "already_running":
        log.info("POST /api/start rechazado: ya en marcha pid=%s", pid)
        return JSONResponse({"started": False, "pid": pid, "error": err}, status_code=409)
    log.error("POST /api/start error: %s", err)
    return JSONResponse({"started": False, "pid": None, "error": err or "unknown"}, status_code=500)


@app.post("/api/stop")
async def api_stop() -> dict[str, Any]:
    stopped, err = supervisor.stop()
    if stopped:
        log.info("POST /api/stop -> validate_edge detenido")
        return {"stopped": True}
    log.warning("POST /api/stop: %s", err)
    return JSONResponse({"stopped": False, "error": err or "not_running"}, status_code=400)


@app.get("/api/signals")
async def api_signals(
    limit: int = Query(100, ge=1, le=5000),
    asset: Optional[str] = Query(None),
) -> list[dict[str, Any]]:
    df = read_signals_df()
    if df.empty:
        return []
    if asset:
        df = df[df["asset"].astype(str).str.upper() == asset.strip().upper()]
    df = df.tail(int(limit))
    return df.to_dict(orient="records")


@app.get("/api/signals/download", response_model=None)
async def api_signals_download() -> Union[FileResponse, JSONResponse]:
    p = signals_path()
    if not p.is_file():
        return JSONResponse({"detail": "signals.csv not found"}, status_code=404)
    return FileResponse(
        path=str(p),
        media_type="text/csv; charset=utf-8",
        filename="signals.csv",
    )


def _csv_download_no_cache_headers() -> dict[str, str]:
    """Evita que proxies o el navegador sirvan un CSV viejo mientras la UI lee filas nuevas."""
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "Pragma": "no-cache",
    }


@app.get("/api/arb/strategy/{slug}/csv-download", response_model=None)
async def arb_strategy_csv_download(slug: str) -> Union[FileResponse, JSONResponse]:
    """Descarga el CSV completo de señales paper de la estrategia (``DATA_DIR/logs/{slug}.csv``)."""
    if slug not in STRATEGY_SLUGS:
        raise HTTPException(status_code=404, detail="Unknown strategy")
    path = ARB_CSV_PATHS[slug]
    if not path.is_file():
        return JSONResponse({"detail": f"CSV not found: {path.name}"}, status_code=404)
    return FileResponse(
        path=str(path),
        media_type="text/csv; charset=utf-8",
        filename=f"{slug}.csv",
        headers=_csv_download_no_cache_headers(),
    )


@app.get("/api/arb/strategy/{slug}/snapshots-csv-download", response_model=None)
async def arb_strategy_snapshots_csv_download(slug: str) -> Union[FileResponse, JSONResponse]:
    """Descarga ``latency_arb_sports_snapshots.csv`` completo (solo ``latency_arb_sports``)."""
    if slug != "latency_arb_sports":
        raise HTTPException(status_code=404, detail="Snapshots CSV only for latency_arb_sports")
    p = LATENCY_ARB_SPORTS_SNAPSHOTS_CSV
    if not p.is_file():
        return JSONResponse({"detail": f"CSV not found: {p.name}"}, status_code=404)
    return FileResponse(
        path=str(p),
        media_type="text/csv; charset=utf-8",
        filename="latency_arb_sports_snapshots.csv",
        headers=_csv_download_no_cache_headers(),
    )


@app.post("/api/debug/polymarket")
async def api_debug_polymarket() -> dict[str, Any]:
    """
    Misma lógica que ``scripts/debug_markets.py``: Gamma por slug (5m) + muestra REST.
    Útil en Railway si el navegador no puede llamar a Polymarket directamente.
    """
    return await asyncio.to_thread(run_polymarket_market_debug)


@app.get("/api/debug/signals")
async def api_debug_signals() -> dict[str, Any]:
    """
    Últimas 20 filas de signals.csv con columnas calculadas para diagnosticar:
    window_return ~ 0 → Bug 2; p_nearres ~ 0.5 → Bug 1; resultados vs gaps → Bug 3.

    Incluye last_signals: últimas 10 filas ML/NEARRES de todo el CSV (para el dashboard).
    """
    rows, last_sig = await asyncio.gather(
        asyncio.to_thread(build_debug_signals_rows, 20),
        asyncio.to_thread(build_debug_last_ml_nearres_rows, 10),
    )
    return {
        "source": str(signals_path()),
        "tail": 20,
        "row_count": len(rows),
        "rows": rows,
        "last_signals": last_sig,
        "last_signals_count": len(last_sig),
    }


async def _signals_sse_gen() -> AsyncIterator[str]:
    """Cada ~5s emite la última fila del CSV (JSON) para refresco en vivo."""
    last_blob: Optional[str] = None
    while True:
        df = await asyncio.to_thread(read_signals_df)
        if not df.empty:
            row = df.iloc[-1].to_dict()
            blob = json.dumps(row, default=str, sort_keys=True)
            if blob != last_blob:
                last_blob = blob
                yield f"data: {json.dumps(row, default=str)}\n\n"
        await asyncio.sleep(5)


@app.get("/api/signals/live")
async def api_signals_live() -> StreamingResponse:
    return StreamingResponse(
        _signals_sse_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/ml/models")
async def api_ml_models() -> dict[str, Any]:
    """Catálogo + estado enabled (persistido); validate_edge aún no selecciona pipeline por slug."""
    state = await _model_state_manager.get_all()
    models: list[dict[str, Any]] = []
    for meta in ML_MODELS:
        slug = str(meta["slug"])
        st = state.get(slug, {}) or {}
        row = {**meta, "enabled": bool(st.get("enabled", False))}
        models.append(row)
    return {"models": models}


@app.post("/api/ml/models/{slug}/enable")
async def api_ml_model_enable(slug: str) -> dict[str, Any]:
    if slug not in ML_MODEL_SLUGS:
        raise HTTPException(status_code=404, detail=f"Unknown model: {slug}")
    await _model_state_manager.enable(slug)
    return {"slug": slug, "enabled": True}


@app.post("/api/ml/models/{slug}/disable")
async def api_ml_model_disable(slug: str) -> dict[str, Any]:
    if slug not in ML_MODEL_SLUGS:
        raise HTTPException(status_code=404, detail=f"Unknown model: {slug}")
    await _model_state_manager.disable(slug)
    return {"slug": slug, "enabled": False}


# ---- ARB ENGINE (subprocess + control plane) ----
from risk.strategy_state import StrategyStateManager

_arb_proc: Optional[subprocess.Popen[Any]] = None
_arb_lock = threading.Lock()
_state_manager = StrategyStateManager()

_sixcycle_lock = asyncio.Lock()
_sixcycle_task: Optional[asyncio.Task[Any]] = None
_sixcycle_engine: Any = None


def _sixcycle_running() -> bool:
    return _sixcycle_task is not None and not _sixcycle_task.done()


async def _sixcycle_start() -> tuple[bool, Optional[str]]:
    """Arranca ``SixCycleEngine.run()`` en segundo plano (independiente de arb_engine)."""
    global _sixcycle_task, _sixcycle_engine
    async with _sixcycle_lock:
        if _sixcycle_task is not None and not _sixcycle_task.done():
            return False, "already_running"
        from scripts import sixcycle_engine as six_mod

        eng = six_mod.SixCycleEngine()
        _sixcycle_engine = eng
        log.info("Sixcycle engine instancia id=%d", id(eng))
        _sixcycle_task = asyncio.create_task(eng.run(), name="sixcycle_engine")
        log.info("sixcycle_engine: tarea asyncio creada")
        return True, None


async def _sixcycle_stop() -> tuple[bool, Optional[str]]:
    """Señala parada al motor y espera a la tarea (o cancela)."""
    global _sixcycle_task, _sixcycle_engine
    async with _sixcycle_lock:
        eng = _sixcycle_engine
        t = _sixcycle_task
        _sixcycle_engine = None
        _sixcycle_task = None
        if eng is not None:
            await eng.shutdown()
        if t is None:
            return False, "not_running"
        try:
            if not t.done():
                await asyncio.wait_for(t, timeout=45.0)
            else:
                await t
        except asyncio.TimeoutError:
            log.warning("sixcycle_engine: timeout esperando tarea, cancelando")
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("sixcycle_engine task: %s", e)
        log.info("sixcycle_engine: detenido")
        return True, None

_latency_schedule_lock = asyncio.Lock()
_latency_schedule_cache: dict[str, Any] = {}
_latency_schedule_mono: float = 0.0


async def refresh_latency_sports_schedule(*, force: bool = False) -> dict[str, Any]:
    """Refresca próximos eventos IO + should_run; caché ~50s salvo force."""
    global _latency_schedule_cache, _latency_schedule_mono
    from arb.latency_sports_schedule import build_schedule_payload, write_schedule_cache
    async with _latency_schedule_lock:
        now_m = time.monotonic()
        if not force and _latency_schedule_cache and (now_m - _latency_schedule_mono) < 50.0:
            return _latency_schedule_cache
        timeout = aiohttp.ClientTimeout(total=90)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            payload = await build_schedule_payload(session, upcoming_limit=10)
        _latency_schedule_cache = payload
        _latency_schedule_mono = now_m
        write_schedule_cache(LATENCY_SPORTS_SCHEDULE_JSON, payload)
        return payload


async def _latency_sports_scheduler_loop() -> None:
    from arb.latency_sports_schedule import scheduler_env_enabled, scheduler_poll_sec

    slug = "latency_arb_sports"
    while True:
        try:
            payload = await refresh_latency_sports_schedule(force=True)
            should = bool(payload.get("should_run_latency_sports"))
            cur = await _state_manager.is_enabled(slug)
            if should and not cur:
                await _state_manager.enable(slug)
                log.info("[api] LATENCY_SPORTS_SCHEDULER: estrategia %s → enabled", slug)
            elif not should and cur:
                await _state_manager.disable(slug)
                log.info("[api] LATENCY_SPORTS_SCHEDULER: estrategia %s → disabled", slug)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[api] LATENCY_SPORTS_SCHEDULER tick")
        await asyncio.sleep(scheduler_poll_sec())

def _read_arb_csv_tail(path: Path, n: int = 200) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[-n:]


_TZ_ES = ZoneInfo("Europe/Madrid")


def _csv_ts_date_spain(ts: Any) -> str:
    """YYYY-MM-DD en Europe/Madrid a partir de columna ts (ISO, típicamente UTC en servidor)."""
    s = str(ts or "").strip()
    if not s:
        return ""
    try:
        t = pd.Timestamp(s)
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        return t.tz_convert(_TZ_ES).strftime("%Y-%m-%d")
    except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
        return ""


def _csv_file_stat(path: Path) -> dict[str, Any]:
    """Existencia y tamaño en disco (útil en Railway: DATA_DIR efímero vs volumen)."""
    try:
        st = path.stat()
        return {"path": str(path), "exists": True, "bytes": int(st.st_size)}
    except OSError:
        return {"path": str(path), "exists": False, "bytes": 0}


def _csv_stats_today(path: Path) -> dict[str, Any]:
    today = pd.Timestamp.now(tz=_TZ_ES).strftime("%Y-%m-%d")
    rows = _read_arb_csv_tail(path, n=15000)
    today_rows = [r for r in rows if _csv_ts_date_spain(r.get("ts")) == today]

    signals = [r for r in today_rows if r.get("action") == "SIGNAL"]
    executed = [r for r in today_rows if r.get("action") == "EXECUTED"]
    skips = [r for r in today_rows if str(r.get("action", "")).startswith("SKIP")]
    errors = [r for r in today_rows if str(r.get("action", "")).startswith("ERROR")]
    skip_below_min = [r for r in today_rows if r.get("action") == "SKIP:BELOW_MIN_EDGE"]

    edges: list[float] = []
    for r in today_rows:
        try:
            v = float(r["edge"])
            edges.append(v)
        except (KeyError, ValueError, TypeError):
            pass

    last = today_rows[-1] if today_rows else {}
    return {
        "total_today": len(today_rows),
        "signals_today": len(signals),
        "executed_today": len(executed),
        "skips_today": len(skips),
        "skip_below_min_edge_today": len(skip_below_min),
        "errors_today": len(errors),
        "skip_rate": round(len(skips) / max(len(today_rows), 1), 3),
        "avg_edge_today": round(sum(edges) / len(edges), 4) if edges else None,
        "best_edge_today": round(max(edges), 4) if edges else None,
        "last_action": last.get("action"),
        "last_ts": last.get("ts"),
        "last_reason": last.get("reason"),
    }


def _sixcycle_csv_ts(row: dict[str, Any]) -> Any:
    """Fila sixcycle: timestamp ISO en ``timestamp_utc`` (nuevo) o ``timestamp`` (legacy)."""
    return row.get("timestamp_utc") or row.get("timestamp")


def _csv_stats_sixcycle(path: Path) -> dict[str, Any]:
    """Métricas diarias para ``crypto_5m_sixcycle.csv`` (columnas del sixcycle engine)."""
    today = pd.Timestamp.now(tz=_TZ_ES).strftime("%Y-%m-%d")
    rows = _read_arb_csv_tail(path, n=15000)
    today_rows = [r for r in rows if _csv_ts_date_spain(_sixcycle_csv_ts(r)) == today]
    settled = [
        r
        for r in today_rows
        if str(r.get("phase", "")).upper() == "SETTLED"
        or (
            str(r.get("phase", "")).strip() == ""
            and str(r.get("resolved", "")).lower() in ("win", "loss", "unknown", "error")
        )
    ]
    wins = [r for r in settled if str(r.get("resolved", "")).lower() == "win"]
    losses = [r for r in settled if str(r.get("resolved", "")).lower() == "loss"]
    pnl_vals: list[float] = []
    for r in settled:
        try:
            pnl_vals.append(float(r.get("pnl_usdc", 0) or 0))
        except (TypeError, ValueError):
            pass
    last = today_rows[-1] if today_rows else {}
    res = str(last.get("resolved") or "").strip()
    last_ts = _sixcycle_csv_ts(last) or last.get("timestamp")
    executed_fill = [
        r
        for r in today_rows
        if str(r.get("filled", "")).lower() in ("true", "1")
        or (
            str(r.get("phase", "")).upper() == "SETTLED"
            and str(r.get("resolved", "")).lower() in ("win", "loss")
        )
    ]
    return {
        "total_today": len(today_rows),
        "signals_today": len([r for r in settled if str(r.get("resolved", "")).strip()]),
        "executed_today": len(executed_fill),
        "skips_today": 0,
        "skip_below_min_edge_today": 0,
        "errors_today": len(
            [r for r in settled if str(r.get("resolved", "")).lower() in ("error", "unknown")]
        ),
        "skip_rate": 0.0,
        "avg_edge_today": None,
        "best_edge_today": None,
        "last_action": f"SETTLE:{res}" if res else None,
        "last_ts": last_ts,
        "last_reason": (
            f"wins={len(wins)} losses={len(losses)} pnl_day={sum(pnl_vals):.4f}"
            if settled
            else None
        ),
    }


def _arb_engine_start() -> tuple[bool, Optional[int], Optional[str]]:
    global _arb_proc
    with _arb_lock:
        if _arb_proc is not None and _arb_proc.poll() is None:
            return False, _arb_proc.pid, "already_running"
        cmd = [sys.executable, str(REPO_ROOT / "scripts" / "arb_engine.py")]
        try:
            child_env = os.environ.copy()
            child_env["DATA_DIR"] = str(DATA_DIR)
            _arb_proc = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                env=child_env,
                stdout=None,
                stderr=None,
                start_new_session=True,
            )
            log.info("POST /api/arb/start -> arb_engine pid=%s", _arb_proc.pid)
            return True, _arb_proc.pid, None
        except OSError as e:
            _arb_proc = None
            log.error("POST /api/arb/start error: %s", e)
            return False, None, str(e)


def _arb_engine_stop() -> tuple[bool, Optional[str]]:
    global _arb_proc
    with _arb_lock:
        if _arb_proc is None or _arb_proc.poll() is not None:
            _arb_proc = None
            return False, "not_running"
        try:
            _arb_proc.terminate()
            try:
                _arb_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                _arb_proc.kill()
                _arb_proc.wait(timeout=5)
        except OSError as e:
            return False, str(e)
        finally:
            _arb_proc = None
        log.info("POST /api/arb/stop -> arb_engine detenido")
        return True, None


def _arb_delete_data_files(include_validator: bool) -> dict[str, Any]:
    """Borra CSV/logs auxiliares del motor Arb bajo DATA_DIR (no toca strategy_state.json)."""
    paths: list[Path] = []
    paths.extend(ARB_CSV_PATHS.values())
    paths.extend(
        [
            BUNDLE_ARB_SCAN_JSON,
            LATENCY_ARB_SPORTS_SNAPSHOTS_CSV,
            LATENCY_SPORTS_CYCLE_METRICS_JSON,
            LATENCY_SPORTS_SCHEDULE_JSON,
            LATENCY_SPORTS_PENDING_JSON,
            DATA_DIR / "logs" / "latency_sports_manual_matches.json",
        ]
    )
    if include_validator:
        paths.append(SIGNALS_CSV)
    removed: list[str] = []
    errors: list[str] = []
    for p in paths:
        try:
            if p.is_file():
                p.unlink()
                removed.append(str(p))
        except OSError as e:
            errors.append(f"{p}: {e}")
    out: dict[str, Any] = {"files_removed": removed, "file_errors": errors}
    try:
        from persistence.writes import clear_postgres_on_arb_data_reset

        out.update(clear_postgres_on_arb_data_reset(include_validator_signals=include_validator))
    except Exception as e:
        log.warning("_arb_delete_data_files: postgres clear: %s", e)
        out.setdefault("postgres_errors", []).append(str(e))
    return out


def _build_arb_strategy_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Filas de estado paper por estrategia (misma forma que ``GET /api/arb/status``)."""
    strategies: list[dict[str, Any]] = []
    for slug in STRATEGY_SLUGS:
        if slug == SIXCYCLE_SLUG:
            stats = _csv_stats_sixcycle(ARB_CSV_PATHS[slug])
        else:
            stats = _csv_stats_today(ARB_CSV_PATHS[slug])
        st = state.get(slug, {}) or {}
        cap = float(st.get("fict_capital_eur") or 1000)
        cum = float(st.get("fict_pnl_cumulative_eur") or 0)
        roi = cum / cap if cap > 0 else 0.0
        enabled_val = bool(st.get("enabled", False))
        if slug == SIXCYCLE_SLUG:
            enabled_val = _sixcycle_running()
        row: dict[str, Any] = {
            "slug": slug,
            "enabled": enabled_val,
            "fict_capital_eur": cap,
            "fict_pnl_cumulative_eur": round(cum, 6),
            "fict_trades": int(st.get("fict_trades") or 0),
            "fict_roi": round(roi, 6),
            "fict_last_stake_eur": st.get("fict_last_stake_eur"),
            "fict_last_pnl_est_eur": st.get("fict_last_pnl_est_eur"),
            **stats,
        }
        if slug == "latency_arb_sports":
            m = _read_latency_sports_cycle_metrics()
            if m:
                row["open_poly_games"] = m.get("open_poly_games")
                row["reference_matched"] = m.get("reference_matched")
                row["reference_aligned_last_cycle"] = m.get("reference_aligned_last_cycle")
                row["pipeline_entered_last_cycle"] = m.get("pipeline_entered_last_cycle")
                row["csv_rows_last_cycle"] = m.get("csv_rows_last_cycle")
                row["ws_cache_size"] = m.get("ws_cache_size")
                row["ws_odds_bookmaker_slots"] = m.get("ws_odds_bookmaker_slots")
                row["betfair_first_ws_rows"] = m.get("betfair_first_ws_rows")
                row["ws_rows_usable"] = m.get("ws_rows_usable")
                row["ws_rows_missing_meta"] = m.get("ws_rows_missing_meta")
                row["ws_age_last_msg_sec"] = m.get("ws_age_last_msg_sec")
                row["ws_age_last_tick_sec"] = m.get("ws_age_last_tick_sec")
                row["bulk_backoff_left_s"] = m.get("bulk_backoff_left_s")
                row["meta_backoff_left_s"] = m.get("meta_backoff_left_s")
                row["rest_quota_used_60min"] = m.get("rest_quota_used_60min")
                row["rest_quota_per_hour"] = m.get("rest_quota_per_hour")
                row["ws_dropped_other_bookie"] = m.get("ws_dropped_other_bookie")
                row["tick_dispatch_total"] = m.get("tick_dispatch_total")
                row["tick_dispatch_signals"] = m.get("tick_dispatch_signals")
                row["tick_dispatch_low_edge"] = m.get("tick_dispatch_low_edge")
                row["avg_latency_tick_to_signal_ms"] = m.get("avg_latency_tick_to_signal_ms")
                row["capture_state"] = m.get("capture_state")
                row["rest_bootstrapped"] = m.get("rest_bootstrapped")
                row["ws_msgs_text_total"] = m.get("ws_msgs_text_total")
                row["ws_msgs_by_type"] = m.get("ws_msgs_by_type")
                row["rest_limit_notice"] = m.get("rest_limit_notice")
                row["rest_limit_reset_left_s"] = m.get("rest_limit_reset_left_s")
                row["cycle_metrics_updated_at"] = m.get("updated_at")
                row["briefing_pre_game_distant"] = m.get("briefing_pre_game_distant")
                row["briefing_within_end_window"] = m.get("briefing_within_end_window")
                row["briefing_ml_like"] = m.get("briefing_ml_like")
                row["briefing_io_name_match_cache"] = m.get("briefing_io_name_match_cache")
                row["drop_pre_game"] = m.get("drop_pre_game")
                row["drop_reference_stale"] = m.get("drop_reference_stale")
                row["drop_reference_lag"] = m.get("drop_reference_lag")
                row["drop_alignment_none"] = m.get("drop_alignment_none")
                row["drop_token_map"] = m.get("drop_token_map")
                row["drop_non_moneyline_semantic_last_cycle"] = m.get(
                    "drop_non_moneyline_semantic_last_cycle"
                )
                row["ref_health_state"] = m.get("ref_health_state")
        strategies.append(row)
    return strategies


def _dashboard_auth_info() -> dict[str, Any]:
    try:
        days = int(os.getenv("API_AUTH_SESSION_DAYS", "30").strip())
    except ValueError:
        days = 30
    days = max(1, min(days, 365))
    return {
        "http_basic_enabled": bool(API_HTTP_BASIC_PASSWORD),
        "session_days": days,
    }


def _persistence_public_status() -> dict[str, Any]:
    from clients.supabase_py import supabase_env_status
    from persistence import write_stats_snapshot
    from persistence.config import database_url, persistence_active_for_writes, primary_store_postgres

    return {
        "database_configured": bool(database_url()),
        "supabase_writes": persistence_active_for_writes(),
        "primary_store_postgres": primary_store_postgres(),
        "supabase_rest": supabase_env_status(),
        **write_stats_snapshot(),
    }


def _dashboard_account_csv_sync() -> dict[str, Any]:
    from scripts import account_metrics as am

    agg = am.aggregate_pnl_from_strategy_logs(DATA_DIR / "logs")
    agg["dry_run_env"] = _env_dry_run_global()
    return agg


@app.get("/api/dashboard/summary")
async def api_dashboard_summary() -> JSONResponse:
    """Resumen para la página de inicio: auth (sin secretos), validador, motor arb paper, PnL CSV."""
    state = await _state_manager.get_all()
    strategies = _build_arb_strategy_rows(state)
    tot_pnl = sum(float(r["fict_pnl_cumulative_eur"]) for r in strategies)
    tot_cap = sum(float(r["fict_capital_eur"]) for r in strategies)
    roi_blend = tot_pnl / tot_cap if tot_cap > 0 else 0.0
    running = False
    with _arb_lock:
        if _arb_proc is not None and _arb_proc.poll() is None:
            running = True
    vs = build_status_payload()
    acc = await asyncio.to_thread(_dashboard_account_csv_sync)
    payload: dict[str, Any] = {
        "auth": _dashboard_auth_info(),
        "default_ml_model_slug": DEFAULT_ML_MODEL_SLUG,
        "validator": {
            "running": vs["running"],
            "csv_rows": vs["csv_rows"],
            "signals_today": vs["signals_today"],
            "uptime_seconds": vs["uptime_seconds"],
        },
        "arb_engine": {
            "running": running,
            "dry_run": ARB_ENGINE_DRY_RUN,
            "poly_clob_l2_configured": _poly_clob_l2_env_configured(),
            "paper_pnl_eur_total": round(tot_pnl, 4),
            "paper_capital_eur_total": round(tot_cap, 2),
            "paper_roi_blended": round(roi_blend, 6),
            "strategies": [
                {
                    "slug": r["slug"],
                    "enabled": r["enabled"],
                    "fict_pnl_cumulative_eur": r["fict_pnl_cumulative_eur"],
                    "fict_roi": r["fict_roi"],
                    "fict_capital_eur": r["fict_capital_eur"],
                }
                for r in strategies
            ],
        },
        "account_csv": acc,
        "persistence": _persistence_public_status(),
    }
    return JSONResponse(
        content=payload,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


@app.get("/api/persistence/status")
async def api_persistence_status() -> dict[str, Any]:
    """Diagnóstico de dual-write Postgres (sin secretos)."""
    return _persistence_public_status()


@app.get("/api/supabase/status")
async def api_supabase_status() -> dict[str, Any]:
    """Cliente REST supabase-py: solo si hay ``SUPABASE_URL`` + ``SUPABASE_KEY``."""
    from clients.supabase_py import get_supabase_client, supabase_env_status

    st = supabase_env_status()
    ok = False
    if st.get("configured"):
        try:
            ok = get_supabase_client() is not None
        except Exception as e:
            return {**st, "client_ok": False, "error": type(e).__name__}
    return {**st, "client_ok": ok}


@app.get("/api/arb/status")
async def arb_status() -> JSONResponse:
    state = await _state_manager.get_all()
    strategies = _build_arb_strategy_rows(state)
    running = False
    with _arb_lock:
        if _arb_proc is not None and _arb_proc.poll() is None:
            running = True
    payload = {
        "engine_running": running,
        "dry_run": ARB_ENGINE_DRY_RUN,
        "poly_clob_l2_configured": _poly_clob_l2_env_configured(),
        "strategies": strategies,
        # Diagnóstico: CSV de arb viven bajo DATA_DIR; strategy_state sigue en data/ del repo (ver risk/strategy_state.py).
        "data_dir": str(DATA_DIR),
        "strategy_state_file": str(REPO_ROOT / "data" / "strategy_state.json"),
        "arb_csv_files": {slug: _csv_file_stat(ARB_CSV_PATHS[slug]) for slug in STRATEGY_SLUGS},
    }
    return JSONResponse(
        content=payload,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


@app.post("/api/arb/start")
async def arb_start() -> dict[str, Any]:
    started, pid, err = await asyncio.to_thread(_arb_engine_start)
    if started:
        return {"started": True, "pid": pid}
    if err == "already_running":
        return JSONResponse({"started": False, "pid": pid, "error": err}, status_code=409)
    return JSONResponse({"started": False, "pid": None, "error": err or "unknown"}, status_code=500)


@app.post("/api/arb/stop")
async def arb_stop() -> dict[str, Any]:
    stopped, err = await asyncio.to_thread(_arb_engine_stop)
    if stopped:
        return {"stopped": True}
    return JSONResponse({"stopped": False, "error": err or "not_running"}, status_code=400)


@app.post("/api/arb/reset-data", response_model=None)
async def arb_reset_data(
    include_validator: bool = Query(
        False,
        description="Si true, también borra logs/signals.csv del validador ML",
    ),
) -> Union[dict[str, Any], JSONResponse]:
    """
    Detiene arb_engine (si lo lanzó este API), borra CSV de estrategias y artefactos
    auxiliares (bundle scan, snapshots sports, métricas ciclo, caché odds-api),
    resetea contadores paper y desactiva todas las estrategias (no arranca el motor).
    """
    try:
        try:
            await _sixcycle_stop()
        except Exception as e:
            log.warning("POST /api/arb/reset-data: sixcycle_stop: %s", e)
        engine_stopped_here, _ = await asyncio.to_thread(_arb_engine_stop)
        file_result = await asyncio.to_thread(_arb_delete_data_files, include_validator)
        await _state_manager.reset_fictional_paper()
        await _state_manager.disable_all_strategies()
        log.info(
            "POST /api/arb/reset-data include_validator=%s engine_stopped=%s removed=%s",
            include_validator,
            engine_stopped_here,
            len(file_result.get("files_removed") or []),
        )
        return {
            "ok": True,
            "engine_stopped_by_reset": engine_stopped_here,
            "strategies_disabled": True,
            "include_validator": include_validator,
            **file_result,
        }
    except Exception as e:
        log.exception("POST /api/arb/reset-data failed: %s", e)
        return JSONResponse(
            status_code=500,
            content={"detail": str(e), "ok": False},
        )


@app.post("/api/arb/strategy/{slug}/enable")
async def arb_strategy_enable(slug: str) -> dict[str, Any]:
    if slug not in STRATEGY_SLUGS:
        raise HTTPException(status_code=404, detail=f"Unknown strategy: {slug}")
    if slug == SIXCYCLE_SLUG:
        started, err = await _sixcycle_start()
        if not started:
            if err == "already_running":
                return JSONResponse(
                    {"slug": slug, "enabled": True, "note": "already_running"},
                    status_code=409,
                )
            return JSONResponse(
                {"slug": slug, "enabled": False, "error": err or "start_failed"},
                status_code=500,
            )
        await _state_manager.enable(slug)
        return {"slug": slug, "enabled": True}
    await _state_manager.enable(slug)
    return {"slug": slug, "enabled": True}


@app.post("/api/arb/strategy/{slug}/disable")
async def arb_strategy_disable(slug: str) -> dict[str, Any]:
    if slug not in STRATEGY_SLUGS:
        raise HTTPException(status_code=404, detail=f"Unknown strategy: {slug}")
    if slug == SIXCYCLE_SLUG:
        await _sixcycle_stop()
        await _state_manager.disable(slug)
        return {"slug": slug, "enabled": False}
    await _state_manager.disable(slug)
    return {"slug": slug, "enabled": False}


@app.get("/api/arb/strategy/{slug}/log")
async def arb_strategy_log(
    slug: str,
    limit: int = Query(200, ge=1, le=500),
    action: str = Query("", description="Prefijo o código exacto de action"),
) -> list[dict[str, Any]]:
    if slug not in STRATEGY_SLUGS:
        raise HTTPException(status_code=404, detail="Unknown strategy")
    rows = _read_arb_csv_tail(ARB_CSV_PATHS[slug], n=max(limit, 500))
    if action:
        rows = [r for r in rows if str(r.get("action", "")).startswith(action)]
    return rows[-limit:]


@app.get("/api/arb/strategy/{slug}/snapshots")
async def arb_strategy_snapshots(
    slug: str,
    limit: int = Query(200, ge=1, le=500),
) -> list[dict[str, Any]]:
    """Últimas filas de ``latency_arb_sports_snapshots.csv`` (solo estrategia sports). Orden: más reciente primero."""
    if slug != "latency_arb_sports":
        raise HTTPException(status_code=404, detail="Snapshots only available for latency_arb_sports")
    if not LATENCY_ARB_SPORTS_SNAPSHOTS_CSV.is_file():
        return []
    rows = _read_arb_csv_tail(LATENCY_ARB_SPORTS_SNAPSHOTS_CSV, n=max(limit, 500))
    tail = rows[-limit:] if len(rows) > limit else rows
    return list(reversed(tail))


@app.get("/api/arb/strategy/{slug}/scan")
async def arb_strategy_scan(slug: str) -> dict[str, Any]:
    """
    Diagnóstico del último ciclo de escaneo CLOB (solo ``bundle_arb``).
    Generado en ``data/logs/bundle_arb_scan.json`` al terminar cada ``run_once``.
    """
    if slug != "bundle_arb":
        raise HTTPException(
            status_code=404,
            detail="scan solo disponible para bundle_arb",
        )
    if not BUNDLE_ARB_SCAN_JSON.is_file():
        return {
            "available": False,
            "path": str(BUNDLE_ARB_SCAN_JSON),
            "detail": "Sin archivo aún: activa la estrategia y espera un ciclo del motor (~poll_interval).",
        }
    try:
        data = json.loads(BUNDLE_ARB_SCAN_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return {"available": False, "detail": str(e)}
    return {"available": True, "path": str(BUNDLE_ARB_SCAN_JSON), "scan": data}


async def _arb_signals_sse_gen(request: Request) -> AsyncIterator[str]:
    prev_len = {slug: len(_read_arb_csv_tail(ARB_CSV_PATHS[slug], n=5000)) for slug in STRATEGY_SLUGS}
    while True:
        if await request.is_disconnected():
            break
        for slug in STRATEGY_SLUGS:
            rows = _read_arb_csv_tail(ARB_CSV_PATHS[slug], n=5000)
            n = len(rows)
            if n > prev_len[slug]:
                for row in rows[prev_len[slug] :]:
                    payload = {"strategy": slug, **row}
                    yield f"data: {json.dumps(payload, default=str)}\n\n"
                prev_len[slug] = n
        await asyncio.sleep(3)


@app.get("/api/arb/signals/live")
async def arb_signals_live(request: Request) -> StreamingResponse:
    return StreamingResponse(
        _arb_signals_sse_gen(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _sixcycle_history_csv_path() -> Optional[Path]:
    if SIXCYCLE_SIGNALS_CSV.is_file():
        return SIXCYCLE_SIGNALS_CSV
    if SIXCYCLE_ENGINE_CSV_FALLBACK.is_file():
        return SIXCYCLE_ENGINE_CSV_FALLBACK
    return None


def _read_sixcycle_history_tail(limit: int) -> list[dict[str, Any]]:
    p = _sixcycle_history_csv_path()
    if p is None or limit <= 0:
        return []
    try:
        with p.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except OSError:
        return []
    if not rows:
        return []
    return rows[-limit:]


def _sixcycle_kpis_from_engine_csv_tail(path: Path) -> Optional[dict[str, Any]]:
    """
    Lee la última fila con ``trades_total`` del CSV del engine (mismo fichero que ``/log``).
    Así el SSE puede mostrar KPIs alineados con el CSV tras reinicio del API o motor fuera de proceso.
    """
    if not path.is_file():
        return None
    rows = _read_arb_csv_tail(path, n=300)
    for r in reversed(rows):
        raw = str(r.get("trades_total", "") or "").strip()
        if not raw:
            continue
        try:
            trades = int(float(raw))
        except (TypeError, ValueError):
            continue
        if trades <= 0:
            continue
        wr_s = str(r.get("win_rate_pct", "") or "0").replace("%", "").strip()
        try:
            win_rate = float(wr_s)
        except (TypeError, ValueError):
            win_rate = 0.0
        try:
            pnl = float(str(r.get("pnl_cumulative_usdc", "") or "0").strip() or "0")
        except (TypeError, ValueError):
            pnl = 0.0
        try:
            streak = int(float(str(r.get("win_streak", "") or "0").strip() or "0"))
        except (TypeError, ValueError):
            streak = 0
        return {
            "trades": trades,
            "win_rate": round(win_rate, 2),
            "pnl_usdc": round(pnl, 6),
            "win_streak": streak,
        }
    return None


def _sixcycle_kpis_from_signals_csv(path: Path) -> Optional[dict[str, Any]]:
    """
    ``sixcycle_signals.csv`` (schema legacy): muchas filas por mercado; agregamos un settle
    por ``market_id`` (última fila con resolved win/loss por timestamp).
    """
    if not path.is_file():
        return None
    rows = _read_arb_csv_tail(path, n=50000)
    by_market: dict[str, dict[str, Any]] = {}
    for r in rows:
        mid = str(r.get("market_id") or "").strip()
        if not mid:
            continue
        res = str(r.get("resolved") or "").lower().strip()
        if res not in ("win", "loss"):
            continue
        ts = str(r.get("timestamp") or r.get("timestamp_utc") or "")
        prev = by_market.get(mid)
        if prev is None or str(prev.get("_ts") or "") <= ts:
            nr = dict(r)
            nr["_ts"] = ts
            by_market[mid] = nr
    if not by_market:
        return None
    settled = sorted(by_market.values(), key=lambda x: str(x.get("_ts") or ""))
    trades = len(settled)
    wins = sum(1 for r in settled if str(r.get("resolved") or "").lower() == "win")
    wr = (100.0 * wins / trades) if trades else 0.0
    pnl = 0.0
    for r in settled:
        try:
            pnl += float(r.get("pnl_usdc") or 0.0)
        except (TypeError, ValueError):
            pass
    streak_end = 0
    for r in reversed(settled):
        if str(r.get("resolved") or "").lower() == "win":
            streak_end += 1
        else:
            break
    return {
        "trades": trades,
        "win_rate": round(wr, 2),
        "pnl_usdc": round(pnl, 6),
        "win_streak": streak_end,
    }


def _sixcycle_ensure_stake_mult_in_payload(payload: dict[str, Any]) -> None:
    """Garantiza ``dry_run_stake_multiplier`` coherente con el motor (DRY_RUN)."""
    from scripts import sixcycle_engine as six_mod

    payload["dry_run_stake_multiplier"] = (
        float(six_mod.get_dry_run_stake_multiplier()) if six_mod.DRY_RUN else 1.0
    )


def _sixcycle_merge_csv_kpis_into_payload(payload: dict[str, Any]) -> None:
    """Enriquece KPIs del panel sixcycle: Postgres si ``PRIMARY_STORE=postgres``, si no CSV en disco."""
    from persistence.config import primary_store_postgres
    from persistence.readers import sixcycle_sse_kpis_from_postgres

    mem_t = int(payload.get("trades") or 0)
    if primary_store_postgres():
        pg = sixcycle_sse_kpis_from_postgres()
        if pg is not None:
            pg_t = int(pg.get("trades") or 0)
            if pg_t >= mem_t:
                payload["trades"] = pg_t
                payload["win_rate"] = float(pg.get("win_rate") or 0.0)
                payload["pnl_usdc"] = float(pg.get("pnl_usdc") or 0.0)
                payload["win_streak"] = int(pg.get("win_streak") or 0)
        _sixcycle_ensure_stake_mult_in_payload(payload)
        return

    path = ARB_CSV_PATHS.get(SIXCYCLE_SLUG)
    best: Optional[dict[str, Any]] = None
    best_t = mem_t
    if path is not None:
        eng = _sixcycle_kpis_from_engine_csv_tail(path)
        if eng and int(eng["trades"]) > best_t:
            best, best_t = eng, int(eng["trades"])
    sig = _sixcycle_kpis_from_signals_csv(SIXCYCLE_SIGNALS_CSV)
    if sig and int(sig["trades"]) > best_t:
        best, best_t = sig, int(sig["trades"])
    if best is not None:
        payload["trades"] = best["trades"]
        payload["win_rate"] = best["win_rate"]
        payload["pnl_usdc"] = best["pnl_usdc"]
        payload["win_streak"] = best["win_streak"]
    _sixcycle_ensure_stake_mult_in_payload(payload)


async def _sixcycle_live_sse_gen(request: Request) -> AsyncIterator[str]:
    from scripts import sixcycle_engine as six_mod

    while True:
        if await request.is_disconnected():
            break
        with six_mod.SIXCYCLE_STATE_LOCK:
            payload = dict(six_mod.SIXCYCLE_STATE)
        await asyncio.to_thread(_sixcycle_merge_csv_kpis_into_payload, payload)
        yield f"data: {json.dumps(payload, default=str)}\n\n"
        await asyncio.sleep(1.0)


@app.get("/api/sixcycle/live")
async def api_sixcycle_live(request: Request) -> StreamingResponse:
    return StreamingResponse(
        _sixcycle_live_sse_gen(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/sixcycle/history")
async def api_sixcycle_history(limit: int = Query(100, ge=1, le=5000)) -> JSONResponse:
    rows = await asyncio.to_thread(_read_sixcycle_history_tail, limit)
    return JSONResponse(content=rows)


@app.get("/api/sixcycle/status")
async def api_sixcycle_status() -> JSONResponse:
    from scripts import sixcycle_engine as six_mod

    with six_mod.SIXCYCLE_STATE_LOCK:
        payload = dict(six_mod.SIXCYCLE_STATE)
    await asyncio.to_thread(_sixcycle_merge_csv_kpis_into_payload, payload)
    return JSONResponse(content=payload)


def _poly_clob_l2_env_configured() -> bool:
    """True si las cuatro credenciales L2 están en env (no valida contra el CLOB)."""
    return bool(
        os.getenv("POLY_API_KEY", "").strip()
        and os.getenv("POLY_API_SECRET", "").strip()
        and os.getenv("POLY_PASSPHRASE", "").strip()
        and os.getenv("POLY_PRIVATE_KEY", "").strip()
    )


def _env_dry_run_global() -> bool:
    return os.getenv("DRY_RUN", "true").strip().lower() != "false"


@app.get("/api/account/balance")
async def api_account_balance() -> JSONResponse:
    """Balance USDC CLOB (siempre intenta cuenta real vía ``py-clob-client``)."""

    def _run() -> dict[str, Any]:
        from scripts import polymarket_client as pc

        try:
            bal = pc.get_live_account_client().get_balance()
            bal["source"] = "live"
            bal["dry_run_env"] = _env_dry_run_global()
            return bal
        except Exception as e:
            log.warning("GET /api/account/balance: %s", e)
            return {
                "usdc_available": None,
                "usdc_in_positions": None,
                "total": None,
                "source": "unavailable",
                "error": str(e),
                "credentials_path": str(pc.account_json_path()),
                "dry_run_env": _env_dry_run_global(),
            }

    return JSONResponse(content=await asyncio.to_thread(_run))


@app.get("/api/account/positions")
async def api_account_positions() -> JSONResponse:
    """Órdenes abiertas CLOB (cuenta real)."""

    def _run() -> list[dict[str, Any]]:
        from scripts import polymarket_client as pc

        try:
            return pc.get_live_account_client().get_positions()
        except Exception as e:
            log.warning("GET /api/account/positions: %s", e)
            return [{"error": str(e), "credentials_path": str(pc.account_json_path())}]

    return JSONResponse(content=await asyncio.to_thread(_run))


@app.get("/api/account/pnl")
async def api_account_pnl() -> JSONResponse:
    """PnL agregado desde CSV de logs (todas las estrategias bajo DATA_DIR/logs)."""
    from scripts import account_metrics as am

    def _run() -> dict[str, Any]:
        agg = am.aggregate_pnl_from_strategy_logs(DATA_DIR / "logs")
        agg["source"] = "csv"
        agg["dry_run_env"] = _env_dry_run_global()
        return agg

    return JSONResponse(content=await asyncio.to_thread(_run))


@app.get("/api/strategies")
async def api_strategies_list() -> JSONResponse:
    """Listado multi-estrategia: enabled, dry_run/stake (sixcycle desde JSON), PnL hoy por CSV."""
    from scripts import account_metrics as am
    from scripts import sixcycle_config_store as scs

    pnl_by = await asyncio.to_thread(am.per_slug_pnl_today, DATA_DIR / "logs")
    state = await _state_manager.get_all()
    rows: list[dict[str, Any]] = []
    sc_cfg = scs.load_config()
    for slug in STRATEGY_SLUGS:
        st = state.get(slug, {}) or {}
        enabled = bool(st.get("enabled", False))
        if slug == SIXCYCLE_SLUG:
            enabled = _sixcycle_running()
        row: dict[str, Any] = {
            "slug": slug,
            "enabled": enabled,
        }
        if slug == SIXCYCLE_SLUG:
            row["dry_run"] = bool(sc_cfg.get("dry_run", True))
            row["stake_usdc"] = float(sc_cfg.get("stake_usdc", 1.0))
            row["max_daily_loss_usdc"] = float(sc_cfg.get("max_daily_loss_usdc", 20.0))
            row["max_concurrent_trades"] = int(sc_cfg.get("max_concurrent_trades", 1))
        snap = pnl_by.get(slug)
        if snap:
            row["pnl_hoy"] = snap.get("pnl_hoy")
            row["trades_hoy"] = snap.get("trades_hoy")
            row["win_rate_hoy"] = snap.get("win_rate_hoy")
        else:
            row["pnl_hoy"] = 0.0
            row["trades_hoy"] = 0
            row["win_rate_hoy"] = 0.0
        rows.append(row)
    return JSONResponse(
        content={
            "strategies": rows,
            "dry_run_env": _env_dry_run_global(),
        }
    )


@app.post("/api/strategies/{slug}/config")
async def api_strategies_post_config(slug: str, body: dict[str, Any] = Body(...)) -> JSONResponse:
    """
    Actualiza config por estrategia. Solo ``crypto_5m_sixcycle`` está cableado a
    ``sixcycle_config.json`` (mismas reglas que ``POST /api/sixcycle/config``).
    """
    if slug not in STRATEGY_SLUGS:
        raise HTTPException(status_code=404, detail=f"Unknown strategy: {slug}")
    if slug != SIXCYCLE_SLUG:
        raise HTTPException(
            status_code=501,
            detail="Config por slug solo implementada para crypto_5m_sixcycle; use /api/sixcycle/config",
        )
    return await api_sixcycle_post_config(body)


async def _sixcycle_config_api_bundle() -> dict[str, Any]:
    """Payload enriquecido para GET /api/sixcycle/config (agentes / UI)."""
    from scripts import sixcycle_config_store as scs
    from scripts import sixcycle_engine as six_mod

    cfg_disk = scs.load_config()
    enabled_live = await _state_manager.is_enabled(SIXCYCLE_SLUG)
    cfg = dict(cfg_disk)
    cfg["enabled"] = enabled_live
    rows = _read_arb_csv_tail(ARB_CSV_PATHS[SIXCYCLE_SLUG], n=300)
    stats = scs.stats_last_n_from_csv_rows(rows, 100)
    cur_fp = scs.config_fingerprint(cfg_disk)
    stats_this_fp = scs.stats_last_n_from_csv_rows(rows, 100, config_fingerprint=cur_fp)
    return {
        "config": cfg,
        "enabled": enabled_live,
        "stats_last_100": stats,
        "stats_last_100_this_fingerprint": stats_this_fp,
        "config_fingerprint": cur_fp,
        "active_filters": scs.build_active_filters(cfg_disk),
        "last_updated": scs.get_last_updated_iso(),
        "dry_run_process_env": bool(six_mod.DRY_RUN),
        "version_log_tail": scs.load_version_tail(20),
    }


@app.get("/api/sixcycle/config")
async def api_sixcycle_get_config() -> JSONResponse:
    return JSONResponse(content=await _sixcycle_config_api_bundle())


@app.get("/api/sixcycle/config/schema")
async def api_sixcycle_config_schema() -> JSONResponse:
    from scripts import sixcycle_config_store as scs

    return JSONResponse(content=dict(scs.SCHEMA))


@app.get("/api/sixcycle/config/versions")
async def api_sixcycle_config_versions(
    limit: int = Query(50, ge=1, le=200),
    csv_rows: int = Query(8000, ge=100, le=50_000),
) -> JSONResponse:
    """Historial de configs aplicadas + agregados PnL por ``config_fingerprint`` (CSV motor)."""
    from scripts import sixcycle_config_store as scs

    tail = scs.load_version_tail(limit)
    rows = await asyncio.to_thread(_read_arb_csv_tail, ARB_CSV_PATHS[SIXCYCLE_SLUG], csv_rows)
    by_fp = scs.stats_by_fingerprint_from_rows(rows)
    return JSONResponse(
        content={
            "version_log_tail": tail,
            "stats_by_fingerprint": by_fp,
            "csv_rows_used": len(rows),
        }
    )


@app.post("/api/sixcycle/config/suggest")
async def api_sixcycle_config_suggest(body: dict[str, Any] = Body(...)) -> JSONResponse:
    from scripts import sixcycle_config_store as scs

    objective = str(body.get("objective") or "").strip()
    cfg = scs.load_config()
    rows = _read_arb_csv_tail(ARB_CSV_PATHS[SIXCYCLE_SLUG], n=300)
    stats = scs.stats_last_n_from_csv_rows(rows, 100)
    try:
        sug = scs.suggest_from_stats(objective, stats, cfg)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return JSONResponse(
        content={
            "objective": objective,
            "stats_last_100": stats,
            **sug,
        }
    )


@app.post("/api/sixcycle/config")
async def api_sixcycle_post_config(body: dict[str, Any] = Body(...)) -> JSONResponse:
    from scripts import sixcycle_config_store as scs
    from scripts import sixcycle_engine as six_mod

    reset_defaults = bool(body.get("reset_defaults"))
    confirm_live = bool(body.get("confirm_live"))
    control_keys = frozenset({"reset_defaults", "confirm_live"})
    partial = {k: v for k, v in body.items() if k not in control_keys}

    if reset_defaults:
        merged = dict(scs.DEFAULT_SIXCYCLE_CONFIG)
    else:
        cur = scs.load_config()
        merged = scs.merge_and_validate(cur, partial)

    if "dry_run" in partial and partial["dry_run"] is False and bool(six_mod.DRY_RUN):
        if not confirm_live:
            raise HTTPException(
                status_code=400,
                detail="El proceso arrancó con DRY_RUN=true: para persistir dry_run=false "
                "envía confirm_live=true y deja el motor Sixcycle parado (sin órdenes reales hasta reinicio acorde).",
            )
        if _sixcycle_running():
            raise HTTPException(
                status_code=400,
                detail="Parar Sixcycle (POST disable / Parar en UI) antes de dry_run=false con DRY_RUN en el proceso.",
            )

    if "enabled" in partial:
        if bool(partial["enabled"]):
            started, err = await _sixcycle_start()
            if not started and err != "already_running":
                raise HTTPException(
                    status_code=500,
                    detail=f"No se pudo encender Sixcycle: {err or 'start_failed'}",
                )
            await _state_manager.enable(SIXCYCLE_SLUG)
        else:
            await _sixcycle_stop()
            await _state_manager.disable(SIXCYCLE_SLUG)
    merged["enabled"] = await _state_manager.is_enabled(SIXCYCLE_SLUG)

    def _save() -> None:
        scs.save_config(merged)

    try:
        await asyncio.to_thread(_save)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    eng = _sixcycle_engine
    if eng is not None:
        await asyncio.to_thread(eng.reload_config)

    return JSONResponse(content=await _sixcycle_config_api_bundle())


@app.post("/api/sixcycle/dry-stake-multiplier")
async def api_sixcycle_dry_stake_multiplier(body: SixcycleDryStakeMultiplierIn) -> JSONResponse:
    """Ajusta el factor de stake Kelly en paper (ver ``sixcycle_engine.set_dry_run_stake_multiplier``)."""
    from scripts import sixcycle_engine as six_mod

    if not six_mod.DRY_RUN:
        raise HTTPException(
            status_code=400,
            detail="Solo disponible con DRY_RUN=true (sin órdenes reales).",
        )
    v = six_mod.set_dry_run_stake_multiplier(float(body.multiplier))
    log.info("sixcycle dry_run_stake_multiplier set to %s", v)
    return JSONResponse(content={"ok": True, "dry_run_stake_multiplier": v})


def _sixcycle_delete_data_files() -> dict[str, Any]:
    """Borra CSV del engine sixcycle y ``sixcycle_signals.csv`` bajo DATA_DIR."""
    removed: list[str] = []
    errors: list[str] = []
    for p in (ARB_CSV_PATHS[SIXCYCLE_SLUG], SIXCYCLE_SIGNALS_CSV):
        try:
            if p.is_file():
                p.unlink()
                removed.append(str(p))
        except OSError as e:
            errors.append(f"{p}: {e}")
    out: dict[str, Any] = {"files_removed": removed, "file_errors": errors}
    try:
        from persistence.writes import clear_postgres_sixcycle_rows

        out.update(clear_postgres_sixcycle_rows())
    except Exception as e:
        log.warning("_sixcycle_delete_data_files: postgres clear: %s", e)
        out.setdefault("postgres_errors", []).append(str(e))
    return out


@app.post("/api/sixcycle/reset-data")
async def api_sixcycle_reset_data() -> JSONResponse:
    """
    Para el motor Sixcycle en este API, borra ``logs/crypto_5m_sixcycle.csv`` y
    ``sixcycle_signals.csv``, y reinicia ``SIXCYCLE_STATE`` en memoria.
    No desactiva otras estrategias ni el arb_engine global.
    """
    try:
        await _sixcycle_stop()
        file_result = await asyncio.to_thread(_sixcycle_delete_data_files)
        from scripts import sixcycle_engine as six_mod

        six_mod.reset_sixcycle_live_state()
        log.info(
            "POST /api/sixcycle/reset-data removed=%s errors=%s",
            len(file_result.get("files_removed") or []),
            len(file_result.get("file_errors") or []),
        )
        return JSONResponse(
            content={"ok": True, **file_result},
        )
    except Exception as e:
        log.exception("POST /api/sixcycle/reset-data failed: %s", e)
        return JSONResponse(
            status_code=500,
            content={"ok": False, "detail": str(e)},
        )


@app.get("/sixcycle", response_model=None)
async def sixcycle_legacy_redirect() -> RedirectResponse:
    """Ruta antigua: una sola UX bajo /arb/strategy/crypto_5m_sixcycle (pestaña vista en vivo)."""
    return RedirectResponse(
        url="/arb/strategy/crypto_5m_sixcycle?view=live",
        status_code=307,
    )


STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


if __name__ == "__main__":
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    uvicorn.run(app, host="0.0.0.0", port=API_PORT, reload=False)
