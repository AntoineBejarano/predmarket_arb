#!/usr/bin/env python3
"""
API FastAPI: dashboard HTML + control del proceso validate_edge (subprocess).
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import os
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.debug_markets import run_polymarket_market_debug
from risk.ml_model_registry import ML_MODELS, ML_MODEL_SLUGS
from risk.model_state import ModelStateManager

load_dotenv(REPO_ROOT / ".env")

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

DATA_DIR = Path(os.getenv("DATA_DIR", ".")).resolve()
SIGNALS_CSV = DATA_DIR / "logs" / "signals.csv"
STATIC_DIR = REPO_ROOT / "static"
ML_MODELS_HTML = STATIC_DIR / "ml_models.html"
ML_MODEL_DETAIL_HTML = STATIC_DIR / "ml_model_detail.html"
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
]
ARB_CSV_PATHS = {slug: DATA_DIR / "logs" / f"{slug}.csv" for slug in STRATEGY_SLUGS}
BUNDLE_ARB_SCAN_JSON = DATA_DIR / "logs" / "bundle_arb_scan.json"
LATENCY_ARB_SPORTS_SNAPSHOTS_CSV = DATA_DIR / "logs" / "latency_arb_sports_snapshots.csv"
LATENCY_SPORTS_CYCLE_METRICS_JSON = DATA_DIR / "logs" / "latency_arb_sports_cycle_metrics.json"
ODDS_EVENT_META_CACHE_JSON = DATA_DIR / "logs" / "odds_event_meta_cache.json"


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
                child_env["PORT"] = str(int(os.getenv("VALIDATOR_HEALTH_PORT", "18088")))
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
    auto = os.getenv("AUTO_START", "").lower() in ("true", "1", "yes")
    log.info(
        "API arrancando: DATA_DIR=%s PORT=%s AUTO_START=%s VALIDATOR_HEALTH_PORT=%s",
        DATA_DIR,
        os.getenv("PORT", "8080"),
        auto,
        os.getenv("VALIDATOR_HEALTH_PORT", "18088"),
    )
    if auto:
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
    yield
    if supervisor.is_running():
        log.info("Apagado API: deteniendo validate_edge…")
        supervisor.stop()
        log.info("validate_edge detenido")
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


@app.get("/", response_model=None)
async def root() -> Union[FileResponse, JSONResponse]:
    """Validador ML (modelo por defecto); misma UI que /ml/model/crypto_5m_lgbm."""
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    if not ML_MODEL_DETAIL_HTML.is_file():
        return JSONResponse({"detail": "static/ml_model_detail.html not found"}, status_code=404)
    return FileResponse(path=str(ML_MODEL_DETAIL_HTML), media_type="text/html; charset=utf-8")


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


@app.get("/arb/strategy/{slug}", response_model=None)
async def arb_strategy_detail_page(slug: str) -> Union[FileResponse, JSONResponse]:
    """Vista detallada de una estrategia del arb (paper / CSV / feed filtrado en cliente)."""
    if slug not in STRATEGY_SLUGS:
        return JSONResponse({"detail": "unknown strategy slug"}, status_code=404)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
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


def _warn_if_experimental_enabled_in_state_but_env_off() -> None:
    """Evita confusión en Railway: toggles ON en JSON no cargan estrategias sin env."""
    if os.getenv("ENABLE_EXPERIMENTAL", "true").lower() in ("true", "1", "yes"):
        return
    experimental_slugs = ("combinatorial_arb", "term_structure", "latency_arb", "latency_arb_sports")
    try:
        p = REPO_ROOT / "data" / "strategy_state.json"
        if not p.is_file():
            return
        raw = json.loads(p.read_text(encoding="utf-8"))
        bad = [s for s in experimental_slugs if (raw.get(s) or {}).get("enabled")]
        if bad:
            log.warning(
                "[api] Hay estrategias enabled en strategy_state.json que el motor NO ejecutará "
                "porque ENABLE_EXPERIMENTAL=false: %s. Añade ENABLE_EXPERIMENTAL=true al servicio y redeploy.",
                ", ".join(bad),
            )
    except (OSError, json.JSONDecodeError, TypeError):
        pass


def _arb_engine_start() -> tuple[bool, Optional[int], Optional[str]]:
    global _arb_proc
    with _arb_lock:
        if _arb_proc is not None and _arb_proc.poll() is None:
            return False, _arb_proc.pid, "already_running"
        cmd = [sys.executable, str(REPO_ROOT / "scripts" / "arb_engine.py")]
        try:
            _arb_proc = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
                start_new_session=True,
            )
            log.info("POST /api/arb/start -> arb_engine pid=%s", _arb_proc.pid)
            _warn_if_experimental_enabled_in_state_but_env_off()
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
            ODDS_EVENT_META_CACHE_JSON,
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
    return {"files_removed": removed, "file_errors": errors}


@app.get("/api/arb/status")
async def arb_status() -> JSONResponse:
    state = await _state_manager.get_all()
    strategies: list[dict[str, Any]] = []
    for slug in STRATEGY_SLUGS:
        stats = _csv_stats_today(ARB_CSV_PATHS[slug])
        st = state.get(slug, {}) or {}
        cap = float(st.get("fict_capital_eur") or 1000)
        cum = float(st.get("fict_pnl_cumulative_eur") or 0)
        roi = cum / cap if cap > 0 else 0.0
        row: dict[str, Any] = {
            "slug": slug,
            "enabled": st.get("enabled", False),
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
                row["ws_cache_size"] = m.get("ws_cache_size")
                row["cycle_metrics_updated_at"] = m.get("updated_at")
        strategies.append(row)
    running = False
    with _arb_lock:
        if _arb_proc is not None and _arb_proc.poll() is None:
            running = True
    payload = {
        "engine_running": running,
        "dry_run": os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes"),
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
    await _state_manager.enable(slug)
    return {"slug": slug, "enabled": True}


@app.post("/api/arb/strategy/{slug}/disable")
async def arb_strategy_disable(slug: str) -> dict[str, Any]:
    if slug not in STRATEGY_SLUGS:
        raise HTTPException(status_code=404, detail=f"Unknown strategy: {slug}")
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


STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


if __name__ == "__main__":
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
