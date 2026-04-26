#!/usr/bin/env python3
"""
API FastAPI: dashboard HTML + control del proceso validate_edge (subprocess).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Optional, Union

import pandas as pd
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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
DASHBOARD_HTML = STATIC_DIR / "dashboard.html"


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


def _parse_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def today_mask_utc(df: pd.DataFrame) -> pd.Series:
    if df.empty or "timestamp" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    ts = _parse_ts(df["timestamp"])
    today = pd.Timestamp.now(tz="UTC").normalize().date()
    return ts.dt.date == today


def _dedupe_signal_first(df: pd.DataFrame, sig: str) -> pd.DataFrame:
    sub = df[(df["signal"] == sig) & (df["result"].isin(("Up", "Down")))].copy()
    if sub.empty:
        return sub
    sub["_ts"] = _parse_ts(sub["timestamp"])
    sub = sub.sort_values("_ts")
    return sub.groupby("condition_id", as_index=False).first()


def accuracies(df: pd.DataFrame) -> tuple[float, float]:
    if df.empty or not {"signal", "direction", "result"}.issubset(df.columns):
        return 0.0, 0.0
    resolved = df[df["result"].isin(("Up", "Down"))]
    if resolved.empty:
        return 0.0, 0.0

    def acc(sub: pd.DataFrame) -> float:
        if sub.empty:
            return 0.0
        ok = (sub["direction"] == sub["result"]).sum()
        return float(ok) / float(len(sub))

    ml = _dedupe_signal_first(resolved, "ML")
    nr = _dedupe_signal_first(resolved, "NEARRES")
    return acc(ml), acc(nr)


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
    ml_acc, nr_acc = accuracies(df)
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
        "ml_accuracy": round(ml_acc, 4),
        "nearres_accuracy": round(nr_acc, 4),
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
    yield
    if supervisor.is_running():
        log.info("Apagado API: deteniendo validate_edge…")
        supervisor.stop()
        log.info("validate_edge detenido")


app = FastAPI(title="PredMarket Arb API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_model=None)
async def root() -> Union[FileResponse, JSONResponse]:
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    if not DASHBOARD_HTML.is_file():
        return JSONResponse({"detail": "static/dashboard.html not found"}, status_code=404)
    return FileResponse(path=str(DASHBOARD_HTML), media_type="text/html; charset=utf-8")


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


if __name__ == "__main__":
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
