#!/usr/bin/env python3
"""
Validador de edge en vivo: Polymarket (Gamma) vs modelo ML + heurística NearRes.
Solo lectura — sin trades ni wallet.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from scipy.stats import norm

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.healthcheck import start_health_server  # noqa: E402
from scripts import polymarket_feed  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

import os  # noqa: E402

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SIGNAL_THRESHOLD = float(os.getenv("SIGNAL_THRESHOLD", "0.08"))
BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
GAMMA_API_URL = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com").rstrip("/")
DATA_DIR = Path(os.getenv("DATA_DIR", ".")).resolve()
SIGNALS_CSV = DATA_DIR / "logs" / "signals.csv"
REPORTS_DIR = DATA_DIR / "reports"

FEATURES = [
    "ret_1",
    "ret_3",
    "ret_6",
    "ret_12",
    "ret_24",
    "ret_48",
    "vol_10",
    "vol_ratio",
    "atr_5",
    "vol_zscore",
    "vol_trend",
    "hour",
    "dow",
    "is_ny_open",
]

ASSET_MAP: dict[str, dict[str, str]] = {
    "BTC": {"binance": "BTCUSDT", "model": "BTCUSDT"},
    "ETH": {"binance": "ETHUSDT", "model": "ETHUSDT"},
    "SOL": {"binance": "SOLUSDT", "model": "SOLUSDT"},
    "XRP": {"binance": "XRPUSDT", "model": "XRPUSDT"},
    "BNB": {"binance": "BNBUSDT", "model": "BNBUSDT"},
}

CSV_COLUMNS = [
    "timestamp",
    "asset",
    "condition_id",
    "minutes_elapsed",
    "p_mercado",
    "p_modelo",
    "p_nearres",
    "gap_ml",
    "gap_nearres",
    "window_return",
    "vol_regime",
    "signal",
    "direction",
    "confidence",
    "volume_usdc",
    "price_source",
    "result",
]

RAW_TICK_MAXLEN = 2000
CLOSES_5M_MAXLEN = 250
AGGREGATE_EVERY_N_TICKS = 10
BINANCE_POLL_SEC = 30.0
MAIN_LOOP_SEC = 30.0
GAMMA_CACHE_SEC = 60.0
GAMMA_PAGE_SLEEP = 1.0
GAMMA_MAX_PAGES = int(os.getenv("GAMMA_MAX_PAGES", "120"))
# slug: GET /markets/slug/{btc|eth|...}-updown-5m-{unix_5m_utc} (recomendado). scan: paginación legacy.
GAMMA_FETCH_MODE = os.getenv("GAMMA_FETCH_MODE", "slug").strip().lower()
GAMMA_SLUG_CACHE_SEC = float(os.getenv("GAMMA_SLUG_CACHE_SEC", "15"))
POLYMARKET_WS_URL = os.getenv("POLYMARKET_WS_URL", "wss://ws-live-data.polymarket.com").strip()
# polymarket: RTDS crypto_prices (btc/eth/sol/xrp) + Binance REST solo BNB. binance: los 5 por REST.
PRICE_FEED = os.getenv("PRICE_FEED", "polymarket").strip().lower()
# Ventana 5m: no procesar mercado fuera de [0, WINDOW_DURATION_MIN) minutos.
WINDOW_DURATION_MIN = float(os.getenv("WINDOW_DURATION_MIN", "5"))
CSV_WRITE_RETRIES = 3

log = logging.getLogger("validate_edge")
MARKET_PRICES: dict[str, float] = {}
OHLCV_BUFFER: dict[str, deque[dict[str, float]]] = {
    asset: deque(maxlen=60) for asset in ASSET_MAP
}


def setup_logging() -> None:
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [validate_edge] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[logging.StreamHandler(sys.stderr)],
    )
    logging.Formatter.converter = time.gmtime


def calibrate_prob(calibrator: Any, raw: float) -> float:
    arr = np.asarray([raw], dtype=np.float64)
    if hasattr(calibrator, "predict"):
        return float(np.asarray(calibrator.predict(arr.reshape(-1)))[0])
    if hasattr(calibrator, "transform"):
        return float(np.asarray(calibrator.transform(arr.reshape(-1, 1)))[0])
    raise TypeError("Calibrator must support predict or transform")


@dataclass
class LoadedModels:
    symbol: str
    model_stem: str
    clf: Any
    cal: Any
    clf_high: Any | None
    cal_high: Any | None
    clf_low: Any | None
    cal_low: Any | None


@dataclass
class SessionStats:
    started_monotonic: float = field(default_factory=time.monotonic)
    observations: int = 0
    signals_fired: int = 0
    markets_resolved: int = 0
    last_signal_text: str = "—"
    last_signal_time: str = ""


def _models_dir() -> Path:
    return REPO_ROOT / "models" / "saved"


def load_models_for_assets() -> dict[str, LoadedModels]:
    out: dict[str, LoadedModels] = {}
    mdir = _models_dir()
    for sym, cfg in ASSET_MAP.items():
        stem = cfg["model"]
        p_model = mdir / f"{stem}_model.pkl"
        p_cal = mdir / f"{stem}_calibrator.pkl"
        if not p_model.is_file() or not p_cal.is_file():
            log.warning("Omitiendo %s: faltan %s_model.pkl o %s_calibrator.pkl", sym, stem, stem)
            continue
        try:
            clf = joblib.load(p_model)
            cal = joblib.load(p_cal)
        except Exception as e:
            log.error("No se pudo cargar modelo base %s: %s", stem, e)
            continue
        ch, cah, cl, calo = None, None, None, None
        ph, pl = mdir / f"{stem}_model_high.pkl", mdir / f"{stem}_model_low.pkl"
        qh, ql = mdir / f"{stem}_calibrator_high.pkl", mdir / f"{stem}_calibrator_low.pkl"
        if ph.is_file() and qh.is_file():
            try:
                ch, cah = joblib.load(ph), joblib.load(qh)
            except Exception as e:
                log.warning("Régimen high %s no cargado: %s", stem, e)
        if pl.is_file() and ql.is_file():
            try:
                cl, calo = joblib.load(pl), joblib.load(ql)
            except Exception as e:
                log.warning("Régimen low %s no cargado: %s", stem, e)
        if ch is None or cl is None:
            log.info(
                "Activo %s: modelo base OK; régimen high/low %s",
                sym,
                "completo" if ch and cl else "parcial o ausente — se usará base donde falte",
            )
        out[sym] = LoadedModels(
            symbol=sym,
            model_stem=stem,
            clf=clf,
            cal=cal,
            clf_high=ch,
            cal_high=cah,
            clf_low=cl,
            cal_low=calo,
        )
        log.info("Modelo cargado para %s (%s)", sym, stem)
    return out


def pick_regime_models(
    lm: LoadedModels, vol_regime: str
) -> tuple[Any, Any]:
    if vol_regime == "high" and lm.clf_high is not None and lm.cal_high is not None:
        return lm.clf_high, lm.cal_high
    if vol_regime == "low" and lm.clf_low is not None and lm.cal_low is not None:
        return lm.clf_low, lm.cal_low
    return lm.clf, lm.cal


def build_feature_row_from_ohlcv(
    candles: list[dict[str, float]],
    now: datetime,
) -> tuple[dict[str, float], str] | None:
    """Features sobre velas 5m OHLCV reales de Binance."""
    if len(candles) < 55:
        return None
    df = pd.DataFrame(candles)
    df["ret_1"] = df["close"].pct_change(1)
    df["ret_3"] = df["close"].pct_change(3)
    df["ret_6"] = df["close"].pct_change(6)
    df["ret_12"] = df["close"].pct_change(12)
    df["ret_24"] = df["close"].pct_change(24)
    df["ret_48"] = df["close"].pct_change(48)
    df["vol_10"] = df["ret_1"].rolling(10).std()
    df["vol_20"] = df["ret_1"].rolling(20).std()
    df["vol_ratio"] = df["vol_10"] / df["vol_20"]
    df["atr_5"] = (df["high"] - df["low"]).rolling(5).mean()
    vm = df["volume"].rolling(20).mean()
    vs = df["volume"].rolling(20).std()
    df["vol_zscore"] = (df["volume"] - vm) / vs.replace(0, np.nan)
    df["vol_zscore"] = df["vol_zscore"].fillna(0.0)
    df["vol_trend"] = df["volume"] / df["volume"].shift(3)
    df["hour"] = now.hour
    df["dow"] = now.weekday()
    df["is_ny_open"] = 1 if 13 <= now.hour <= 16 else 0
    roll_mean_vol20 = df["vol_20"].rolling(100, min_periods=50).mean()
    df["vol_regime"] = np.where(df["vol_20"] > roll_mean_vol20, "high", "low")
    last = df.iloc[-1]
    row: dict[str, float] = {}
    for f in FEATURES:
        v = last.get(f)
        if pd.isna(v):
            return None
        row[f] = float(v)
    vr = last.get("vol_regime")
    if isinstance(vr, str) and vr in ("high", "low"):
        regime = vr
    else:
        regime = "low"
    return row, regime


def predict_p_model(lm: LoadedModels, feats: dict[str, float], vol_regime: str) -> float:
    clf, cal = pick_regime_models(lm, vol_regime)
    X = pd.DataFrame([feats], columns=FEATURES)
    raw = float(clf.predict_proba(X)[0, 1])
    return calibrate_prob(cal, raw)


def parse_asset_from_question(q: str) -> str | None:
    u = q.upper()
    for sym in ASSET_MAP:
        if sym in u or f"{sym} " in u:
            return sym
    return None


def parse_json_maybe(val: Any) -> Any:
    if isinstance(val, str) and val.strip().startswith("["):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return val
    return val


def parse_outcome_prices(m: dict[str, Any]) -> list[float] | None:
    raw = m.get("outcomePrices") or m.get("outcome_prices")
    raw = parse_json_maybe(raw)
    if not isinstance(raw, (list, tuple)) or len(raw) < 1:
        return None
    try:
        return [float(x) for x in raw]
    except (TypeError, ValueError):
        return None


def parse_clob_token_ids(m: dict[str, Any]) -> list[str]:
    raw = m.get("clobTokenIds")
    parsed = parse_json_maybe(raw)
    if not isinstance(parsed, (list, tuple)):
        return []
    out: list[str] = []
    for x in parsed:
        sx = str(x).strip()
        if sx:
            out.append(sx)
    return out


def get_clob_midpoint(token_id: str, session: requests.Session) -> float | None:
    try:
        r = session.get(
            "https://clob.polymarket.com/midpoint",
            params={"token_id": token_id},
            timeout=5,
        )
        r.raise_for_status()
        return float(r.json()["mid"])
    except Exception:
        return None


def get_condition_id(m: dict[str, Any]) -> str | None:
    cid = m.get("conditionId") or m.get("condition_id")
    if cid is None:
        return None
    return str(cid)


def fetch_binance_price(symbol: str, session: requests.Session, last_good: dict[str, float]) -> float | None:
    url = f"{BINANCE_BASE_URL}/api/v3/ticker/price"
    try:
        r = session.get(url, params={"symbol": symbol}, timeout=15)
        r.raise_for_status()
        p = float(r.json()["price"])
        last_good[symbol] = p
        return p
    except Exception as e:
        log.warning("Binance %s: %s — usando último precio conocido si existe", symbol, e)
        return last_good.get(symbol)


def fetch_real_5m_ohlcv(symbol: str, session: requests.Session) -> list[dict[str, float]] | None:
    try:
        r = session.get(
            f"{BINANCE_BASE_URL}/api/v3/klines",
            params={"symbol": symbol, "interval": "5m", "limit": 60},
            timeout=10,
        )
        r.raise_for_status()
        raw = r.json()
        if not isinstance(raw, list):
            return None
        out: list[dict[str, float]] = []
        for c in raw:
            out.append(
                {
                    "open_time": float(c[0]),  # ms epoch — used to anchor window_open
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                }
            )
        return out
    except Exception as e:
        log.warning("fetch_real_5m_ohlcv %s: %s", symbol, e)
        return None


def ohlcv_worker(
    display_sym: str,
    binance_sym: str,
    stop_event: threading.Event,
    ohlcv_buffer: dict[str, deque[dict[str, float]]],
    ohlcv_lock: threading.Lock,
    session: requests.Session,
) -> None:
    while not stop_event.is_set():
        candles = fetch_real_5m_ohlcv(binance_sym, session)
        if candles:
            with ohlcv_lock:
                ohlcv_buffer[display_sym] = deque(candles, maxlen=60)
            log.debug("OHLCV %s: %s candles", display_sym, len(candles))
        stop_event.wait(300)


def price_worker(
    display_sym: str,
    binance_sym: str,
    stop_event: threading.Event,
    raw_ticks: dict[str, deque[float]],
    tick_lock: threading.Lock,
    tick_counters: dict[str, int],
    closes_5m: dict[str, deque[float]],
    last_price: dict[str, float],
    session: requests.Session,
) -> None:
    last_local: dict[str, float] = {}
    while not stop_event.is_set():
        try:
            px = fetch_binance_price(binance_sym, session, last_local)
            if px is not None:
                with tick_lock:
                    last_price[display_sym] = px
                    dq = raw_ticks[display_sym]
                    dq.append(px)
                    tick_counters[display_sym] = tick_counters.get(display_sym, 0) + 1
                    if tick_counters[display_sym] % AGGREGATE_EVERY_N_TICKS == 0:
                        closes_5m[display_sym].append(px)
                        log.debug("5m close %s: %s (n=%s)", display_sym, px, len(closes_5m[display_sym]))
        except Exception as e:
            log.error("Hilo precio %s: %s", display_sym, e)
        stop_event.wait(BINANCE_POLL_SEC)


@dataclass
class GammaCache:
    fetched_at: float = 0.0
    markets: list[dict[str, Any]] = field(default_factory=list)
    slug_window_ts: int | None = None


def fetch_gamma_markets(session: requests.Session, cache: GammaCache) -> list[dict[str, Any]]:
    if GAMMA_FETCH_MODE == "scan":
        return fetch_gamma_markets_scan(session, cache)
    return fetch_gamma_markets_slug(session, cache)


def fetch_gamma_markets_slug(session: requests.Session, cache: GammaCache) -> list[dict[str, Any]]:
    now_dt = datetime.now(timezone.utc)
    win_ts = polymarket_feed.floor_5m_window_ts_utc(now_dt)
    now_m = time.monotonic()
    if (
        cache.markets
        and cache.slug_window_ts == win_ts
        and (now_m - cache.fetched_at) < GAMMA_SLUG_CACHE_SEC
    ):
        log.debug("Gamma slug: caché (%s mercados ventana=%s)", len(cache.markets), win_ts)
        return cache.markets
    markets, wts = polymarket_feed.fetch_5m_markets_by_slug(
        session, GAMMA_API_URL, list(ASSET_MAP.keys()), now_dt
    )
    cache.markets = markets
    cache.slug_window_ts = wts
    cache.fetched_at = now_m
    log.info("Gamma slug: %s mercados (ventana UTC ts=%s)", len(markets), wts)
    if not markets:
        log.warning("Gamma slug: 0 mercados — red, convención de slug o ventana sin mercado aún.")
    return markets


def fetch_gamma_markets_scan(session: requests.Session, cache: GammaCache) -> list[dict[str, Any]]:
    now = time.monotonic()
    if cache.markets and (now - cache.fetched_at) < GAMMA_CACHE_SEC:
        log.debug("Gamma: usando caché (%s mercados)", len(cache.markets))
        return cache.markets
    all_rows: list[dict[str, Any]] = []
    offset = 0
    limit = 100
    base = f"{GAMMA_API_URL}/markets"
    page = 0
    while page < GAMMA_MAX_PAGES:
        page += 1
        try:
            r = session.get(
                base,
                params={"active": "true", "limit": limit, "offset": offset},
                timeout=30,
            )
            r.raise_for_status()
            chunk = r.json()
        except Exception as e:
            log.warning("Gamma API error (offset=%s): %s — reintentando en 60s", offset, e)
            time.sleep(60)
            all_rows = []
            offset = 0
            page = 0
            continue
        if not isinstance(chunk, list):
            log.warning("Gamma: respuesta inesperada, se ignora")
            break
        all_rows.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit
        time.sleep(GAMMA_PAGE_SLEEP)
    else:
        log.warning(
            "Gamma: paginación truncada tras %s páginas (offset=%s). "
            "Si siempre ves 0 mercados filtrados, sube GAMMA_MAX_PAGES en .env o revisa el filtro.",
            GAMMA_MAX_PAGES,
            offset,
        )
    filtered = [m for m in all_rows if passes_market_filter(m)]
    cache.markets = filtered
    cache.slug_window_ts = None
    cache.fetched_at = time.monotonic()
    log.info("Gamma scan: %s mercados activos tras filtro 5m crypto Up/Down", len(filtered))
    if not filtered:
        if all_rows:
            samples = [str(m.get("question", ""))[:120] for m in all_rows[:3]]
            log.warning(
                "Gamma: ningún mercado pasó el filtro entre %s descargados (no es fallo de Binance). "
                "Ejemplos de preguntas en ese bloque: %s",
                len(all_rows),
                samples,
            )
        else:
            log.warning("Gamma: la API devolvió 0 mercados en las páginas consultadas.")
    return filtered


def passes_market_filter(m: dict[str, Any]) -> bool:
    if m.get("closed") is True or str(m.get("closed")).lower() == "true":
        return False
    q = str(m.get("question", ""))
    ql = q.lower()
    if "up or down" not in ql:
        return False
    has_5m = ("5m" in ql) or ("5-minute" in ql) or ("5 min" in ql) or ("5M" in q)
    if not has_5m:
        return False
    uq = q.upper()
    if not any(a in uq for a in ("BTC", "ETH", "SOL", "XRP", "BNB")):
        return False
    return True


def fmt_csv_val(x: Any) -> str:
    if x is None or (isinstance(x, float) and (np.isnan(x))):
        return ""
    if isinstance(x, float):
        return f"{x:.8f}".rstrip("0").rstrip(".")
    return str(x)


def ensure_signals_csv_header() -> None:
    SIGNALS_CSV.parent.mkdir(parents=True, exist_ok=True)
    if not SIGNALS_CSV.is_file() or SIGNALS_CSV.stat().st_size == 0:
        with open(SIGNALS_CSV, "w", encoding="utf-8", newline="") as f:
            f.write(",".join(CSV_COLUMNS) + "\n")


def append_signals_row(row: dict[str, Any]) -> None:
    SIGNALS_CSV.parent.mkdir(parents=True, exist_ok=True)
    header = not SIGNALS_CSV.is_file() or SIGNALS_CSV.stat().st_size == 0
    line = ",".join(fmt_csv_val(row.get(c)) for c in CSV_COLUMNS) + "\n"
    for attempt in range(CSV_WRITE_RETRIES):
        try:
            mode = "w" if header else "a"
            with open(SIGNALS_CSV, mode, encoding="utf-8", newline="") as f:
                if header:
                    f.write(",".join(CSV_COLUMNS) + "\n")
                f.write(line)
            return
        except OSError as e:
            log.error("CSV write intento %s/%s: %s", attempt + 1, CSV_WRITE_RETRIES, e)
            time.sleep(0.3 * (attempt + 1))
    print(f"FATAL: no se pudo escribir {SIGNALS_CSV}", file=sys.stderr)


def flush_pending_resolutions(
    now: datetime,
    pending_end: dict[str, tuple[str, float, datetime]],
    last_price: dict[str, float],
    tick_lock: threading.Lock,
    session: requests.Session,
    stats: SessionStats,
) -> None:
    """Resuelve mercados cuya endDate ya pasó (aunque no vengan en la última respuesta Gamma)."""
    last_good: dict[str, float] = {}
    for cid in list(pending_end.keys()):
        asset, w_open, end_dt = pending_end[cid]
        if now < end_dt:
            continue
        with tick_lock:
            final_p = last_price.get(asset)
        if final_p is None:
            sym = ASSET_MAP[asset]["binance"]
            final_p = fetch_binance_price(sym, session, last_good)
        if final_p is None:
            log.warning("Resolución %s: sin precio Binance, se usa window_open", cid)
            final_p = w_open
        res = "Up" if float(final_p) >= float(w_open) else "Down"
        update_csv_results_for_condition(cid, res)
        stats.markets_resolved += 1
        del pending_end[cid]
        log.info("Resuelto %s → %s (final=%s open=%s)", cid, res, final_p, w_open)


def update_csv_results_for_condition(condition_id: str, result: str) -> None:
    if not SIGNALS_CSV.is_file():
        return
    for attempt in range(CSV_WRITE_RETRIES):
        try:
            df = pd.read_csv(SIGNALS_CSV, dtype=str, keep_default_na=False)
            if "condition_id" not in df.columns or "result" not in df.columns:
                return
            m = df["condition_id"].astype(str) == str(condition_id)
            df.loc[m, "result"] = result
            tmp = SIGNALS_CSV.with_suffix(".tmp")
            df.to_csv(tmp, index=False)
            tmp.replace(SIGNALS_CSV)
            return
        except Exception as e:
            log.error("update_csv_results intento %s: %s", attempt + 1, e)
            time.sleep(0.3 * (attempt + 1))


def _dedupe_signal_rows(df: pd.DataFrame, sig: str) -> pd.DataFrame:
    """Una fila por condition_id: primera aparición temporal con esa señal (mercado resuelto)."""
    sub = df[(df["signal"] == sig) & (df["result"].isin(("Up", "Down")))].copy()
    if sub.empty:
        return sub
    sub["_ts"] = pd.to_datetime(sub["timestamp"], utc=True, errors="coerce")
    sub = sub.sort_values("_ts")
    return sub.groupby("condition_id", as_index=False).first()


def accuracy_from_csv() -> tuple[int, int, int, int, int, int]:
    """Returns ml_correct, ml_total, nr_correct, nr_total, comb_correct, comb_total."""
    if not SIGNALS_CSV.is_file():
        return 0, 0, 0, 0, 0, 0
    try:
        df = pd.read_csv(SIGNALS_CSV, dtype=str, keep_default_na=False)
    except Exception:
        return 0, 0, 0, 0, 0, 0
    need = {"signal", "direction", "result", "condition_id", "timestamp"}
    if not need.issubset(df.columns):
        return 0, 0, 0, 0, 0, 0
    df = df[df["result"].isin(("Up", "Down"))]
    if df.empty:
        return 0, 0, 0, 0, 0, 0

    def counts(sub: pd.DataFrame) -> tuple[int, int]:
        if sub.empty:
            return 0, 0
        ok = (sub["direction"] == sub["result"]).sum()
        return int(ok), len(sub)

    ml = _dedupe_signal_rows(df, "ML")
    nr = _dedupe_signal_rows(df, "NEARRES")
    both = df[df["signal"].isin(("ML", "NEARRES")) & (df["result"].isin(("Up", "Down")))].copy()
    both["_ts"] = pd.to_datetime(both["timestamp"], utc=True, errors="coerce")
    both = both.sort_values("_ts")
    sig = both.groupby("condition_id", as_index=False).first()
    mc, mt = counts(ml)
    nc, nt = counts(nr)
    cc, ct = counts(sig)
    return mc, mt, nc, nt, cc, ct


def compute_accuracy_rates() -> tuple[float, float, float, int, int, int, int, int, int, int]:
    mc, mt, nc, nt, cc, ct = accuracy_from_csv()
    ml_pct = (100.0 * mc / mt) if mt else 0.0
    nr_pct = (100.0 * nc / nt) if nt else 0.0
    cb_pct = (100.0 * cc / ct) if ct else 0.0
    return ml_pct, nr_pct, cb_pct, mc, mt, nc, nt, cc, ct


def analyze_csv_only() -> None:
    setup_logging()
    if not SIGNALS_CSV.is_file():
        print(f"No existe {SIGNALS_CSV}", file=sys.stderr)
        sys.exit(1)
    text = build_report_text_for_csv()
    print(text)


def build_report_text_for_csv() -> str:
    ml_pct, nr_pct, cb_pct, mc, mt, nc, nt, cc, ct = compute_accuracy_rates()
    lines = [
        f"Signals in CSV (resolved rows with signal): ML {mt}, NearRes {nt}, combined {ct}",
        f"ML accuracy: {ml_pct:.1f}% ({mc}/{mt})" if mt else "ML accuracy: n/a",
        f"NearRes accuracy: {nr_pct:.1f}% ({nc}/{nt})" if nt else "NearRes accuracy: n/a",
        f"Combined: {cb_pct:.1f}% ({cc}/{ct})" if ct else "Combined: n/a",
        f"VERDICT: {verdict_from_rates(cb_pct, ct)}",
    ]
    return "\n".join(lines)


def verdict_from_rates(combined_pct: float, n: int) -> str:
    if n < 8:
        return "INSUFFICIENT_DATA"
    if combined_pct >= 58.0:
        return "EDGE EXISTS"
    if combined_pct >= 52.0:
        return "MARGINAL"
    return "NO EDGE"


def daily_report_yesterday(now: datetime) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    yday = (now - timedelta(days=1)).date()
    path = REPORTS_DIR / f"edge_analysis_{yday.isoformat()}.txt"
    if path.is_file():
        return
    if not SIGNALS_CSV.is_file():
        body = f"No signals.csv for {yday}\n"
        path.write_text(body, encoding="utf-8")
        log.info("Informe diario escrito (vacío): %s", path)
        return
    try:
        df = pd.read_csv(SIGNALS_CSV, dtype=str, keep_default_na=False)
        df["ts"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        day_df = df[(df["ts"].dt.date == yday) & (df["result"].isin(("Up", "Down")))]
        fired = df[df["ts"].dt.date == yday]
        n_fired = len(fired[fired["signal"].isin(("ML", "NEARRES"))])
        n_res = len(day_df)
        body_lines = [
            f"Date (UTC): {yday}",
            f"Signals fired: {n_fired} | Resolved rows that day: {n_res}",
        ]
        if n_res == 0:
            body_lines.append("VERDICT: INSUFFICIENT_DATA")
            path.write_text("\n".join(body_lines) + "\n", encoding="utf-8")
            log.info("Informe diario: %s", path)
            return
        sig_df = day_df[day_df["signal"].isin(("ML", "NEARRES"))]
        ml = sig_df[sig_df["signal"] == "ML"]
        nr = sig_df[sig_df["signal"] == "NEARRES"]

        def acc(sub: pd.DataFrame) -> tuple[int, int, float]:
            if sub.empty:
                return 0, 0, 0.0
            ok = (sub["direction"] == sub["result"]).sum()
            tot = len(sub)
            return int(ok), tot, 100.0 * float(ok) / tot if tot else 0.0

        m_ok, m_tot, m_pct = acc(ml)
        n_ok, n_tot, n_pct = acc(nr)
        c_ok, c_tot, c_pct = acc(sig_df)
        body_lines.extend(
            [
                f"ML accuracy: {m_pct:.1f}% ({m_ok}/{m_tot})" if m_tot else "ML accuracy: n/a",
                f"NearRes accuracy: {n_pct:.1f}% ({n_ok}/{n_tot})" if n_tot else "NearRes accuracy: n/a",
                f"Combined: {c_pct:.1f}% ({c_ok}/{c_tot})" if c_tot else "Combined: n/a",
            ]
        )
        by_asset: dict[str, list[float]] = {}
        for asset, g in sig_df.groupby("asset"):
            ok = (g["direction"] == g["result"]).sum()
            tot = len(g)
            gaps = []
            if "gap_ml" in g.columns:
                for _, r in g.iterrows():
                    for col in ("gap_ml", "gap_nearres"):
                        if col in r and r[col] != "":
                            try:
                                gaps.append(abs(float(r[col])))
                            except ValueError:
                                pass
            avg_gap = (100.0 * float(np.mean(gaps))) if gaps else 0.0
            by_asset[str(asset)] = [float(tot), 100.0 * float(ok) / tot if tot else 0.0, avg_gap]
        body_lines.append("By asset: " + json.dumps(by_asset))
        hour_perf: dict[int, list[int]] = {}
        for _, r in sig_df.iterrows():
            try:
                h = int(pd.Timestamp(r["timestamp"]).hour)
            except Exception:
                continue
            ok = 1 if r["direction"] == r["result"] else 0
            hour_perf.setdefault(h, [0, 0])
            hour_perf[h][0] += ok
            hour_perf[h][1] += 1
        best_h, worst_h = -1, -1
        best_r, worst_r = -1.0, 200.0
        for h, (oks, tot) in hour_perf.items():
            if tot < 2:
                continue
            rp = float(oks) / tot
            if rp > best_r:
                best_r, best_h = rp, h
            if rp < worst_r:
                worst_r, worst_h = rp, h
        body_lines.append(
            f"Best hour UTC: {best_h:02d} | Worst hour UTC: {worst_h:02d}"
            if best_h >= 0
            else "Best/Worst hour: n/a"
        )
        correct_gaps: list[float] = []
        wrong_gaps: list[float] = []
        for _, r in sig_df.iterrows():
            ok = r["direction"] == r["result"]
            for col in ("gap_ml", "gap_nearres"):
                if col in r.index and r[col] != "":
                    try:
                        g = abs(float(r[col]))
                        (correct_gaps if ok else wrong_gaps).append(g)
                    except ValueError:
                        pass
        ag_c = 100.0 * float(np.mean(correct_gaps)) if correct_gaps else 0.0
        ag_w = 100.0 * float(np.mean(wrong_gaps)) if wrong_gaps else 0.0
        body_lines.append(f"Avg gap when correct: {ag_c:.2f}% | Avg gap when wrong: {ag_w:.2f}%")
        vd = verdict_from_rates(c_pct, c_tot)
        body_lines.append(f"VERDICT: {vd}")
        path.write_text("\n".join(body_lines) + "\n", encoding="utf-8")
        log.info("Informe diario escrito: %s", path)
    except Exception as e:
        log.error("Informe diario falló: %s", e)


def format_runtime(seconds: float) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m:02d}m"


def gap_icon(gap_abs: float) -> str:
    if gap_abs > 0.10:
        return "🚨"
    if gap_abs >= 0.08:
        return "⚠️"
    return "·"


def build_dashboard_text(
    stats: SessionStats,
    rows_preview: list[str],
) -> Text:
    elapsed = time.monotonic() - stats.started_monotonic
    ml_pct, nr_pct, cb_pct, mc, mt, nc, nt, cc, ct = compute_accuracy_rates()
    lines = [
        "╔══════════════════════════════════════════════════╗",
        "║  PredMarket Arb — Edge Validator  🔍 PAPER ONLY  ║",
        "╠══════════════════════════════════════════════════╣",
        f"║  Runtime: {format_runtime(elapsed):<12} |  Markets watched: {stats.observations:<6}║",
        f"║  Signals detected: {stats.signals_fired:<4} |  Resolved: {stats.markets_resolved:<14}║",
        "╠══════════════════════════════════════════════════╣",
        "║  ACTIVE MARKETS                                  ║",
    ]
    lines.extend(rows_preview[:8] or ["║  (sin mercados filtrados en esta iteración)      ║"])
    lines.extend(
        [
            "╠══════════════════════════════════════════════════╣",
            "║  SIGNAL ACCURACY (resolved only)                 ║",
            f"║  ML signals:      {mc}/{mt} correct  ({ml_pct:.1f}%)".ljust(52) + "║",
            f"║  NearRes signals: {nc}/{nt} correct  ({nr_pct:.1f}%)".ljust(52) + "║",
            f"║  Combined:        {cc}/{ct} correct ({cb_pct:.1f}%)".ljust(52) + "║",
            "╠══════════════════════════════════════════════════╣",
            f"║  LAST SIGNAL: {stats.last_signal_text[:44]:<44}║",
            "╚══════════════════════════════════════════════════╝",
        ]
    )
    return Text("\n".join(lines))


def run_main(hours: float | None, threshold: float) -> None:
    setup_logging()
    global SIGNAL_THRESHOLD
    SIGNAL_THRESHOLD = threshold

    models = load_models_for_assets()
    if not models:
        log.warning(
            "Ningún modelo cargado desde models/saved — se omiten predicciones ML. "
            "Ejecuta `python models/train.py` para generar los .pkl. NearRes y logging siguen activos."
        )
    else:
        log.info(
            "Modelos ML listos (%s activos): %s",
            len(models),
            ", ".join(sorted(models.keys())),
        )

    port = int(os.getenv("PORT", "8080"))
    start_health_server(port)
    log.info("Health server en 0.0.0.0:%s /health", port)
    ensure_signals_csv_header()

    stop_event = threading.Event()
    tick_lock = threading.Lock()
    ohlcv_lock = threading.Lock()
    price_symbols = list(ASSET_MAP.keys())
    raw_ticks: dict[str, deque[float]] = {s: deque(maxlen=RAW_TICK_MAXLEN) for s in price_symbols}
    closes_5m: dict[str, deque[float]] = {s: deque(maxlen=CLOSES_5M_MAXLEN) for s in price_symbols}
    ohlcv_buffer = OHLCV_BUFFER
    with ohlcv_lock:
        for asset in ASSET_MAP:
            ohlcv_buffer[asset] = deque(maxlen=60)
    tick_counters: dict[str, int] = {s: 0 for s in price_symbols}
    last_price: dict[str, float] = {}
    session = requests.Session()
    session.headers.update({"User-Agent": "predmarket-arb-validate-edge/0.1"})

    threads: list[threading.Thread] = []
    feed = PRICE_FEED
    if feed == "both":
        log.info("PRICE_FEED=both: se usa solo Binance para no duplicar ticks en velas 5m.")
        feed = "binance"
    if feed not in ("binance", "polymarket"):
        log.warning("PRICE_FEED=%s no reconocido; uso polymarket", PRICE_FEED)
        feed = "polymarket"

    log.info(
        "Arranque validate_edge: GAMMA_FETCH_MODE=%s PRICE_FEED=%s GAMMA_API=%s%s",
        GAMMA_FETCH_MODE,
        feed,
        GAMMA_API_URL,
        f" POLYMARKET_WS={POLYMARKET_WS_URL}" if feed == "polymarket" else "",
    )

    if feed == "binance":
        for sym in price_symbols:
            t = threading.Thread(
                target=price_worker,
                args=(
                    sym,
                    ASSET_MAP[sym]["binance"],
                    stop_event,
                    raw_ticks,
                    tick_lock,
                    tick_counters,
                    closes_5m,
                    last_price,
                    session,
                ),
                name=f"price-{sym}",
                daemon=True,
            )
            t.start()
            threads.append(t)
        log.info(
            "Precio Binance: hilos=%s (poll cada %ss símbolos %s)",
            len(threads),
            int(BINANCE_POLL_SEC),
            ", ".join(price_symbols),
        )
    elif feed == "polymarket":
        poly_syms = [s for s in price_symbols if s in polymarket_feed.POLY_BINANCE_SYMBOL]
        try:
            poly_ts = polymarket_feed.polymarket_crypto_ws_bundle(
                POLYMARKET_WS_URL,
                stop_event,
                tick_lock,
                raw_ticks,
                tick_counters,
                closes_5m,
                last_price,
                poly_syms,
                BINANCE_POLL_SEC,
                AGGREGATE_EVERY_N_TICKS,
            )
            threads.extend(poly_ts)
        except RuntimeError as e:
            log.error("%s — instala websocket-client o usa PRICE_FEED=binance", e)
            raise
        if "BNB" in price_symbols:
            t_bnb = threading.Thread(
                target=price_worker,
                args=(
                    "BNB",
                    ASSET_MAP["BNB"]["binance"],
                    stop_event,
                    raw_ticks,
                    tick_lock,
                    tick_counters,
                    closes_5m,
                    last_price,
                    session,
                ),
                name="price-BNB",
                daemon=True,
            )
            t_bnb.start()
            threads.append(t_bnb)
        log.info(
            "Precio Polymarket RTDS (%s) + Binance REST solo BNB; muestreo velas cada %ss",
            ",".join(poly_syms),
            int(BINANCE_POLL_SEC),
        )

    for sym in price_symbols:
        t_ohlcv = threading.Thread(
            target=ohlcv_worker,
            args=(
                sym,
                ASSET_MAP[sym]["binance"],
                stop_event,
                ohlcv_buffer,
                ohlcv_lock,
                session,
            ),
            name=f"ohlcv-{sym}",
            daemon=True,
        )
        t_ohlcv.start()
        threads.append(t_ohlcv)
    log.info("OHLCV 5m real: hilos=%s (refresh cada 300s)", len(price_symbols))

    # Pre-fetch bloqueante: garantiza que ohlcv_buffer esté poblado antes del primer
    # tick del bucle, evitando que window_open caiga a float(px) por buffer vacío.
    log.info("Pre-cargando OHLCV 5m inicial (bloqueante)…")
    for _sym in price_symbols:
        _pre = fetch_real_5m_ohlcv(ASSET_MAP[_sym]["binance"], session)
        if _pre:
            with ohlcv_lock:
                ohlcv_buffer[_sym] = deque(_pre, maxlen=60)
            log.info("OHLCV pre-cargado %s: %s velas", _sym, len(_pre))

    gamma_cache = GammaCache()
    stats = SessionStats()
    fired_signals: set[tuple[str, str, str]] = set()
    # Format: (asset, window_timestamp_str, signal_type), e.g. ("BTC", "1777274700", "ML")
    market_prices = MARKET_PRICES
    market_prices.clear()
    clob_token_ids: list[str] = []
    last_clob_token_set: set[str] = set()
    clob_thread: threading.Thread | None = None
    clob_ws_disabled = False
    window_open: dict[str, float] = {}
    pending_end: dict[str, tuple[str, float, datetime]] = {}
    daily_written_for: date | None = None
    deadline: float | None = None
    if hours is not None:
        deadline = time.monotonic() + hours * 3600.0

    loop_i = 0

    def shutdown(_sig: int | None = None, _frame: Any = None) -> None:
        stop_event.set()
        log.info("Apagado: stop_event activado")

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    tty = sys.stderr.isatty()
    console = Console(stderr=True, force_terminal=tty, no_color=not tty)

    try:
        with Live(
            console=console,
            refresh_per_second=0.5,
            redirect_stdout=False,
            redirect_stderr=False,
        ) as live:
            while not stop_event.is_set():
                loop_i += 1
                try:
                    now = datetime.now(timezone.utc)
                    if deadline is not None and time.monotonic() >= deadline:
                        log.info("Tiempo límite --hours alcanzado, saliendo")
                        break
                    if now.hour == 0 and now.minute < 30:
                        yday = (now - timedelta(days=1)).date()
                        if daily_written_for != yday:
                            daily_report_yesterday(now)
                            daily_written_for = yday

                    markets = fetch_gamma_markets(session, gamma_cache)
                    token_ids_now: list[str] = []
                    for m in markets:
                        clob_ids = parse_clob_token_ids(m)
                        if clob_ids:
                            token_ids_now.append(clob_ids[0])  # Up token
                    token_set_now = set(token_ids_now)
                    if token_set_now != last_clob_token_set:
                        clob_token_ids[:] = sorted(token_set_now)
                        last_clob_token_set = token_set_now
                        log.info("CLOB tokens activos: %s", len(clob_token_ids))
                    if clob_thread is None and not clob_ws_disabled:
                        try:
                            clob_thread = polymarket_feed.clob_market_ws_bundle(
                                stop_event,
                                tick_lock,
                                market_prices,
                                clob_token_ids,
                            )
                            threads.append(clob_thread)
                        except RuntimeError as e:
                            log.error("%s — CLOB WS desactivado, se usará fallback", e)
                            clob_ws_disabled = True

                    preview_lines: list[tuple[float, str]] = []

                    for m in markets:
                        try:
                            cid = get_condition_id(m)
                            if not cid:
                                continue
                            q = str(m.get("question", ""))
                            va = m.get("_validator_asset")
                            asset = va if isinstance(va, str) and va in ASSET_MAP else parse_asset_from_question(q)
                            if asset is None or asset not in ASSET_MAP:
                                continue
                            # endDate = window close (correct, has full datetime)
                            end_s = m.get("endDate")

                            # eventStartTime = window open (top-level, most reliable)
                            # Fallback: events[0]["startTime"]
                            start_s = m.get("eventStartTime")
                            if not start_s:
                                events_list = m.get("events") or []
                                if isinstance(events_list, list) and events_list:
                                    start_s = events_list[0].get("startTime")

                            if not start_s or not end_s:
                                continue
                            start_dt = pd.Timestamp(start_s).to_pydatetime()
                            if start_dt.tzinfo is None:
                                start_dt = start_dt.replace(tzinfo=timezone.utc)
                            else:
                                start_dt = start_dt.astimezone(timezone.utc)
                            end_dt = pd.Timestamp(end_s).to_pydatetime()
                            if end_dt.tzinfo is None:
                                end_dt = end_dt.replace(tzinfo=timezone.utc)
                            else:
                                end_dt = end_dt.astimezone(timezone.utc)

                            # Siempre total_seconds (no .seconds): incluye días y fracción correcta.
                            window_ts = int(start_dt.timestamp())
                            minutes_elapsed = (now - start_dt).total_seconds() / 60.0
                            if minutes_elapsed < 0 or minutes_elapsed >= WINDOW_DURATION_MIN:
                                continue

                            clob_ids = parse_clob_token_ids(m)
                            token_id_up = clob_ids[0] if clob_ids else None
                            price_source = "gamma_fallback"
                            clob_price: float | None = None
                            if token_id_up:
                                with tick_lock:
                                    clob_price = market_prices.get(token_id_up)
                                if clob_price is None:
                                    # Warm-up/fallback rápido mientras llega WS.
                                    clob_price = get_clob_midpoint(token_id_up, session)
                            if clob_price is not None:
                                p_mercado = float(clob_price)
                                price_source = "clob"
                            else:
                                prices = parse_outcome_prices(m)
                                p_mercado = float(prices[0]) if prices else 0.5
                                log.debug("No CLOB price for %s, using Gamma fallback", token_id_up)
                            vol_usd = m.get("volume") or m.get("volumeNum") or ""
                            with tick_lock:
                                px = last_price.get(asset)
                            with ohlcv_lock:
                                candles = list(ohlcv_buffer.get(asset, []))
                            if px is None:
                                log.debug("Sin precio aún para %s", asset)
                                continue
                            if cid not in window_open:
                                # Anchor to the 5m candle that opened this window so that
                                # window_return is correct even when first observed at min 3+.
                                win_open_ms = float(start_dt.timestamp() * 1000)
                                anchor: float | None = None
                                for _c in reversed(candles):
                                    if _c.get("open_time") == win_open_ms:
                                        anchor = float(_c["open"])
                                        break
                                window_open[cid] = anchor if anchor is not None else float(px)
                                if anchor is None:
                                    log.debug(
                                        "window_open %s: sin vela OHLCV para ts=%.0f, usando spot",
                                        cid,
                                        win_open_ms,
                                    )
                            w0 = window_open[cid]
                            window_return = (float(px) - w0) / w0 if w0 else float("nan")
                            VOL_PER_MIN: dict[str, float] = {
                                "BTC": 0.0008,
                                "ETH": 0.0010,
                                "SOL": 0.0018,
                                "XRP": 0.0015,
                                "BNB": 0.0010,
                            }
                            asset_vol = VOL_PER_MIN.get(asset, 0.0010)
                            vol_remaining = asset_vol * np.sqrt(max(5.0 - minutes_elapsed, 0.1))
                            if np.isfinite(window_return) and vol_remaining > 0:
                                p_nearres = float(norm.cdf(window_return / vol_remaining))
                            else:
                                p_nearres = float("nan")

                            p_modelo: float | None = None
                            gap_ml = float("nan")
                            vol_regime = ""
                            built = build_feature_row_from_ohlcv(candles, now)
                            if asset not in models:
                                log.debug("Sin modelo ML para %s", asset)
                            elif built is None:
                                log.debug("Features no listas %s (ohlcv_5m=%s)", asset, len(candles))
                            else:
                                feats, vol_regime = built
                                try:
                                    p_modelo = predict_p_model(models[asset], feats, vol_regime)
                                    gap_ml = float(p_modelo) - p_mercado
                                except Exception as e:
                                    log.error("Modelo falló %s: %s", asset, e)
                                    p_modelo = None
                                    gap_ml = float("nan")

                            gap_nearres = (
                                float(p_nearres) - p_mercado if np.isfinite(p_nearres) else float("nan")
                            )
                            sig_label = ""
                            direction = ""
                            confidence = ""
                            ml_key = (asset, str(window_ts), "ML")
                            nr_key = (asset, str(window_ts), "NR")
                            if (
                                p_modelo is not None
                                and np.isfinite(gap_ml)
                                and abs(gap_ml) > SIGNAL_THRESHOLD
                                and minutes_elapsed <= 2
                                and ml_key not in fired_signals
                            ):
                                fired_signals.add(ml_key)
                                sig_label = "ML"
                                direction = "Up" if gap_ml > 0 else "Down"
                                confidence = str(abs(gap_ml))
                                stats.signals_fired += 1
                                stats.last_signal_text = f"{asset} {direction} ML gap {abs(gap_ml)*100:.1f}% @ min {minutes_elapsed:.1f}"
                                console.print(
                                    f"[bold red on black]SIGNAL ML[/] {asset} {direction} conf={float(confidence):.4f} "
                                    f"p_model={p_modelo:.4f} p_mkt={p_mercado:.4f}"
                                )
                            _nr_dir = "Up" if gap_nearres > 0 else "Down"
                            # Solo disparar NR cuando la señal concuerda con el movimiento
                            # realizado en la ventana: evita apostar contra un mercado eficiente
                            # que ya incorporó el movimiento (gap_nr negativo + precio subiendo
                            # → CLOB correcto, fórmula browniana se queda corta → no señal).
                            _nr_consistent = (
                                np.isfinite(window_return)
                                and window_return != 0.0
                                and (
                                    (_nr_dir == "Up" and window_return > 0)
                                    or (_nr_dir == "Down" and window_return < 0)
                                )
                            )
                            if (
                                np.isfinite(gap_nearres)
                                and abs(gap_nearres) > SIGNAL_THRESHOLD
                                and minutes_elapsed >= 1
                                and _nr_consistent
                                and nr_key not in fired_signals
                            ):
                                fired_signals.add(nr_key)
                                sig_label = "NEARRES"
                                direction = _nr_dir
                                confidence = str(abs(gap_nearres))
                                stats.signals_fired += 1
                                stats.last_signal_text = (
                                    f"{asset} {direction} NearRes gap {abs(gap_nearres)*100:.1f}% @ min {minutes_elapsed:.1f}"
                                )
                                console.print(
                                    f"[bold yellow on black]SIGNAL NEARRES[/] {asset} {direction} conf={float(confidence):.4f}"
                                )

                            result = ""
                            row = {
                                "timestamp": now.isoformat(),
                                "asset": asset,
                                "condition_id": cid,
                                "minutes_elapsed": f"{minutes_elapsed:.4f}",
                                "p_mercado": p_mercado,
                                "p_modelo": p_modelo if p_modelo is not None else "",
                                "p_nearres": p_nearres if np.isfinite(p_nearres) else "",
                                "gap_ml": gap_ml if np.isfinite(gap_ml) else "",
                                "gap_nearres": gap_nearres if np.isfinite(gap_nearres) else "",
                                "window_return": window_return if np.isfinite(window_return) else "",
                                "vol_regime": vol_regime,
                                "signal": sig_label,
                                "direction": direction,
                                "confidence": confidence,
                                "volume_usdc": vol_usd,
                                "price_source": price_source,
                                "result": result,
                            }
                            append_signals_row(row)
                            stats.observations += 1

                            if cid not in pending_end:
                                pending_end[cid] = (asset, float(window_open[cid]), end_dt)

                            gdisp = abs(gap_ml) if np.isfinite(gap_ml) else 0.0
                            if not np.isfinite(gdisp) and np.isfinite(gap_nearres):
                                gdisp = abs(gap_nearres)
                            ic = gap_icon(gdisp if np.isfinite(gdisp) else 0.0)
                            pm = f"{p_mercado*100:.0f}%"
                            pmod = f"{p_modelo*100:.0f}%" if p_modelo is not None else "n/a"
                            gap_s = f"{(gap_ml*100):+.0f}%" if np.isfinite(gap_ml) else "n/a"
                            sort_key = gdisp if np.isfinite(gdisp) else (abs(gap_nearres) if np.isfinite(gap_nearres) else 0.0)
                            preview_lines.append(
                                (sort_key, f"║  {asset} 5m │ {minutes_elapsed:.1f}min │ Up {pm} │ model {pmod} │ {gap_s} {ic}   ║")
                            )

                        except Exception as e:
                            log.exception("Error procesando mercado: %s", e)

                    flush_pending_resolutions(now, pending_end, last_price, tick_lock, session, stats)
                    preview_sorted = [ln for _, ln in sorted(preview_lines, key=lambda x: x[0], reverse=True)]
                    live.update(
                        Panel(
                            build_dashboard_text(stats, preview_sorted),
                            title="validate_edge",
                            border_style="cyan",
                        )
                    )
                    with tick_lock:
                        n5 = {s: len(closes_5m[s]) for s in price_symbols}
                        pri = {
                            s: (round(float(last_price[s]), 4) if s in last_price else None)
                            for s in price_symbols
                        }
                    log.info(
                        "Iteración #%s: gamma_mercados_filtrados=%s obs_total=%s señales=%s "
                        "resueltos=%s pendientes_csv=%s velas_5m_por_activo=%s precio_spot=%s",
                        loop_i,
                        len(markets),
                        stats.observations,
                        stats.signals_fired,
                        stats.markets_resolved,
                        len(pending_end),
                        n5,
                        pri,
                    )
                    if len(fired_signals) > 100:
                        fired_signals = set(
                            sorted(
                                fired_signals,
                                key=lambda k: int(k[1]) if k[1].isdigit() else 0,
                            )[-100:]
                        )
                except Exception as e:
                    log.exception("Error en bucle principal (continuando): %s", e)

                if deadline is not None:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        log.info("Tiempo límite --hours alcanzado (entre iteraciones), saliendo")
                        break
                    sleep_s = min(MAIN_LOOP_SEC, max(0.05, left))
                else:
                    sleep_s = MAIN_LOOP_SEC
                stop_event.wait(sleep_s)
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=2.0)
        ml_pct, nr_pct, cb_pct, mc, mt, nc, nt, cc, ct = compute_accuracy_rates()
        console.print(
            f"\n[bold]Resumen sesión[/]: observaciones={stats.observations} señales={stats.signals_fired} "
            f"resueltos_mercados={stats.markets_resolved} ML {mc}/{mt} NearRes {nc}/{nt} Combined {cc}/{ct}\n"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Edge validator (paper only)")
    p.add_argument("--hours", type=float, default=None, help="Detener tras N horas (default: sin límite)")
    p.add_argument("--analyze", action="store_true", help="Solo analizar signals.csv")
    p.add_argument("--threshold", type=float, default=None, help="Sobrescribe SIGNAL_THRESHOLD")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    thr = float(args.threshold) if args.threshold is not None else SIGNAL_THRESHOLD
    env_hours = os.getenv("RUN_HOURS")
    hours_limit = args.hours
    if hours_limit is None and env_hours:
        try:
            hours_limit = float(env_hours)
        except ValueError:
            pass
    if args.analyze:
        analyze_csv_only()
        return
    run_main(hours_limit, thr)


if __name__ == "__main__":
    main()
