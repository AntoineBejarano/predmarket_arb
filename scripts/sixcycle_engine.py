"""Loop principal de 6 ciclos (Scan→Predict→Validate→Size→Fill→Settle) para BTC 5m Up/Down en Polymarket."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    """
    Raíz del repo para imports (`clients`, `lab`, …).
    Orden: PM_REPO_ROOT → padre de este script → ascendiendo desde cwd.
    """
    raw = os.environ.get("PM_REPO_ROOT", "").strip()
    if raw:
        p = Path(raw).expanduser().resolve()
        if (p / "clients").is_dir():
            return p
    here = Path(__file__).resolve()
    cand = here.parent.parent
    if (cand / "clients").is_dir():
        return cand
    cur = Path.cwd().resolve()
    for d in [cur, *cur.parents]:
        if (d / "clients" / "poly_clob.py").is_file():
            return d
    return cand


REPO_ROOT = _repo_root()
_root_s = str(REPO_ROOT)
if _root_s in sys.path:
    try:
        sys.path.remove(_root_s)
    except ValueError:
        pass
sys.path.insert(0, _root_s)

import asyncio
import csv
import json
import logging
import signal as sig_module
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Literal

import aiohttp
import numpy as np
import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from clients.poly_clob import PolyCLOBClient, _level_price, _level_size
from clients.poly_parse import api_bool_true, extract_yes_token_id, parse_json_maybe

from lab.market_scorer import MarketScorer  # noqa: E402
from lab.paths import data_dir  # noqa: E402
from scripts.clob_signal_filter import CLOBSignalFilter  # noqa: E402
from scripts import sixcycle_config_store as _sixcycle_cfg  # noqa: E402
from scripts.polymarket_client import get_polymarket_client  # noqa: E402

DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"
# En DRY_RUN: factor sobre el stake Kelly (p. ej. 100 → apuestas y PnL ×100 en paper). No aplica en LIVE.
_DRY_RUN_STAKE_MULT = 1.0
_DRY_RUN_STAKE_MULT_LOCK = threading.Lock()
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.05"))
MIN_LIQUIDITY_USDC = float(os.getenv("MIN_LIQUIDITY_USDC", "50.0"))
MAX_STAKE_USDC = float(os.getenv("MAX_STAKE_USDC", "10.0"))
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))
SCAN_INTERVAL_SECONDS = float(os.getenv("SCAN_INTERVAL_SECONDS", "30.0"))
PREPARE_LAST_SECONDS = int(os.getenv("SIXCYCLE_PREPARE_LAST_SECONDS", "90"))
# Evita intentar órdenes LIVE cuando el mercado está prácticamente cerrado.
LIVE_MIN_TTE_SEC = float(os.getenv("SIXCYCLE_LIVE_MIN_TTE_SEC", "6.0"))
GAMMA_API_URL = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com").rstrip("/")
BINANCE_REST_URL = os.getenv("BINANCE_REST_URL", "https://api.binance.com").rstrip("/")

_SIXCYCLE_CSV_BASE_COLUMNS = [
    "timestamp_utc",
    "phase",
    "market_id",
    "market_slug",
    "minutes_elapsed",
    "clob_yes_price",
    "clob_no_price",
    "clob_extreme",
    "liquidity_usdc",
    "score",
    "scorer_direction",
    "scorer_confirms",
    "signal",
    "direction",
    "edge",
    "stake_usdc",
    "fill_price",
    "simulated",
    "resolved",
    "resolution_up",
    "pnl_usdc",
    "win",
    "win_streak",
    "trades_total",
    "win_rate_pct",
    "pnl_cumulative_usdc",
    "reason",
    "dry_run",
]
SIXCYCLE_CSV_LEGACY_HEADER = ",".join(_SIXCYCLE_CSV_BASE_COLUMNS)
CSV_COLUMNS = _SIXCYCLE_CSV_BASE_COLUMNS + ["config_profile_slug", "config_fingerprint"]

_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/sixcycle-engine (+https://github.com)",
    "Accept": "application/json",
}

log = logging.getLogger("sixcycle_engine")

SIXCYCLE_STATE_LOCK = threading.Lock()
SIXCYCLE_STATE: dict[str, Any] = {
    "timestamp_utc": "",
    "phase": "OFFLINE",
    "next_slug": "",
    "secs_until_open": 0,
    "price_to_beat": None,
    "spot_price": None,
    "ptb_gap": None,
    "score": 0,
    "direction": None,
    "confidence": 0.0,
    "ready": False,
    "components": {},
    "clob_yes_price": None,
    "liquidity_usdc": None,
    "signal": False,
    "edge": 0.0,
    "trades": 0,
    "win_rate": 0.0,
    "pnl_usdc": 0.0,
    "win_streak": 0,
    "best_streak": 0,
    "dry_run": DRY_RUN,
    "clob_extreme": "NEUTRAL",
    "clob_no_price": None,
    "clob_bar": "",
    "dry_run_stake_multiplier": 1.0,
    "live_error": None,
    "live_error_ts": None,
    "live_error_reason": None,
}


def get_dry_run_stake_multiplier() -> float:
    """Factor de stake en paper (solo tiene efecto si ``DRY_RUN``)."""
    with _DRY_RUN_STAKE_MULT_LOCK:
        return float(_DRY_RUN_STAKE_MULT)


def set_dry_run_stake_multiplier(m: float) -> float:
    """
    Ajusta el multiplicador de stake Kelly en DRY_RUN. Clamp [0.01, 50_000].
    Devuelve el valor efectivo.
    """
    global _DRY_RUN_STAKE_MULT
    v = float(m)
    if v < 0.01:
        v = 0.01
    elif v > 50000.0:
        v = 50000.0
    with _DRY_RUN_STAKE_MULT_LOCK:
        _DRY_RUN_STAKE_MULT = v
    with SIXCYCLE_STATE_LOCK:
        SIXCYCLE_STATE["dry_run_stake_multiplier"] = v
    return v


def reset_sixcycle_live_state() -> None:
    """Restablece ``SIXCYCLE_STATE`` (p. ej. tras borrar CSV desde el API)."""
    with SIXCYCLE_STATE_LOCK:
        SIXCYCLE_STATE.clear()
        SIXCYCLE_STATE.update(
            {
                "timestamp_utc": "",
                "phase": "OFFLINE",
                "next_slug": "",
                "secs_until_open": 0,
                "price_to_beat": None,
                "spot_price": None,
                "ptb_gap": None,
                "score": 0,
                "direction": None,
                "confidence": 0.0,
                "ready": False,
                "components": {},
                "clob_yes_price": None,
                "liquidity_usdc": None,
                "signal": False,
                "edge": 0.0,
                "trades": 0,
                "win_rate": 0.0,
                "pnl_usdc": 0.0,
                "win_streak": 0,
                "best_streak": 0,
                "dry_run": DRY_RUN,
                "clob_extreme": "NEUTRAL",
                "clob_no_price": None,
                "clob_bar": "",
                "dry_run_stake_multiplier": get_dry_run_stake_multiplier(),
                "live_error": None,
                "live_error_ts": None,
                "live_error_reason": None,
            }
        )


class LiveOrderFailure(RuntimeError):
    """Fallo operativo en orden LIVE que debe disparar stop de estrategia."""

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.detail = dict(detail or {})


def _floor_5m_window_ts_utc(now: datetime) -> int:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    minute = (now.minute // 5) * 5
    floored = now.replace(minute=minute, second=0, microsecond=0)
    return int(floored.timestamp())


def _slug_btc_5m(window_ts: int) -> str:
    return f"btc-updown-5m-{window_ts}"


def _window_ts_from_btc_slug(slug: str) -> int | None:
    """``btc-updown-5m-<unix>`` → inicio UTC de la ventana 5m en segundos."""
    pref = "btc-updown-5m-"
    if not slug.startswith(pref):
        return None
    try:
        return int(slug[len(pref) :])
    except ValueError:
        return None


def next_market_slug_ts_for_prepare(now: datetime | None = None) -> int:
    """Inicio UTC de la ventana 5m siguiente (slug del mercado que está por abrir)."""
    n = now or datetime.now(timezone.utc)
    if n.tzinfo is None:
        n = n.replace(tzinfo=timezone.utc)
    else:
        n = n.astimezone(timezone.utc)
    t = int(n.timestamp())
    win = (t // 300) * 300
    return win + 300


def _fmt_usd(v: float | None) -> str:
    if v is None:
        return "—"
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "—"


def _parse_gamma_end(m: dict[str, Any]) -> datetime | None:
    raw = m.get("endDate") or m.get("end_date") or m.get("endDateIso")
    if raw is None:
        return None
    try:
        t = pd.Timestamp(raw)
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        return t.to_pydatetime()
    except (ValueError, TypeError):
        return None


def _gamma_no_token_id(m: dict[str, Any]) -> str | None:
    raw_tok = m.get("clobTokenIds") or m.get("clob_token_ids")
    outcomes = m.get("outcomes")
    # Mismo criterio que cycle_scan (Up/Down → primer token = lado “YES/Up”).
    yid, _, _ = extract_yes_token_id(outcomes, raw_tok, assume_first=True)
    parsed = parse_json_maybe(raw_tok)
    if not isinstance(parsed, (list, tuple)) or len(parsed) < 2:
        return None
    if yid:
        for i, tid in enumerate(parsed):
            if str(tid).strip() == str(yid).strip():
                for j, t2 in enumerate(parsed):
                    if j != i:
                        s = str(t2).strip()
                        if s:
                            return s
    return str(parsed[-1]).strip() if parsed else None


def _sum_ask_notional_top_n(book: dict[str, Any], n: int = 5) -> float:
    """Liquidez USDC aproximada: suma price*size en los n mejores asks (comprar YES/NO)."""
    asks = book.get("asks") or []
    if not isinstance(asks, list):
        return 0.0
    rows: list[tuple[float, float]] = []
    for lvl in asks:
        p = _level_price(lvl)
        s = _level_size(lvl)
        if p is not None and s is not None:
            rows.append((float(p), float(s)))
    rows.sort(key=lambda x: x[0])
    total = 0.0
    for p, s in rows[:n]:
        total += p * s
    return float(total)


def _parse_outcome_prices(m: dict[str, Any]) -> list[float] | None:
    raw = m.get("outcomePrices") or m.get("outcome_prices")
    raw = parse_json_maybe(raw)
    if not isinstance(raw, (list, tuple)) or len(raw) < 1:
        return None
    try:
        return [float(x) for x in raw]
    except (TypeError, ValueError):
        return None


def _settle_parse_up_won(data: dict[str, Any]) -> bool | None:
    """
    True = ganó UP/Yes, False = ganó DOWN/No, None = aún no hay resolución clara (seguir polling).
    Orden: outcomePrices → winningOutcome → resolution → resolutionSource (mercado cerrado).
    """
    prices_raw = data.get("outcomePrices") or data.get("outcome_prices")
    prices = parse_json_maybe(prices_raw)
    if isinstance(prices, (list, tuple)) and len(prices) >= 2:
        try:
            p_up = float(prices[0])
            p_dn = float(prices[1])
        except (TypeError, ValueError):
            pass
        else:
            mx = max(p_up, p_dn)
            if mx >= 0.95:
                return bool(p_up > 0.5)
    wo = data.get("winningOutcome") or data.get("winning_outcome")
    if wo is not None and str(wo).strip():
        s = str(wo).strip().lower()
        if s in ("up", "yes", "y"):
            return True
        if s in ("down", "no", "n"):
            return False
    res = data.get("resolution")
    if res is not None and str(res).strip():
        s = str(res).strip().lower()
        if s in ("yes", "y", "up"):
            return True
        if s in ("no", "n", "down"):
            return False
    if api_bool_true(data.get("closed")) or api_bool_true(data.get("isClosed")):
        src = str(data.get("resolutionSource") or data.get("resolution_source") or "").lower()
        if src:
            has_up = "up" in src or "yes" in src
            has_down = "down" in src
            if has_down and not has_up:
                return False
            if has_up and not has_down:
                return True
    return None


def _close_ts_from_btc_slug_market(m: dict[str, Any]) -> datetime | None:
    """
    Cierre aproximado de ventana 5m: slug ``btc-updown-5m-{ts_apertura_utc}`` + 300 s.
    ``market_id`` numérico no contiene el ts; usar siempre ``market_slug``.
    """
    slug = str(m.get("market_slug", "") or "").strip()
    if not slug.startswith("btc-updown-5m-"):
        return None
    try:
        ts_open = int(slug.rsplit("-", 1)[-1])
    except (ValueError, TypeError, IndexError):
        return None
    try:
        return datetime.fromtimestamp(int(ts_open) + 300, tz=timezone.utc)
    except (ValueError, OSError):
        return None


def _gamma_price_to_beat(data: dict[str, Any]) -> float | None:
    for key in ("openPrice", "open_price", "startPrice", "start_price"):
        if key not in data:
            continue
        v = data.get(key)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


async def _fetch_binance_5m_open(
    http: aiohttp.ClientSession, window_start_ts: int, symbol: str = "BTCUSDT"
) -> float | None:
    """
    OPEN de la vela 5m en Binance cuyo *open time* coincide con ``window_start_ts`` (UTC, segundos).

    Los mercados Gamma ``btc-updown-5m-<ts>`` suelen no traer ``openPrice``; el PTB on-chain
    es Chainlink al inicio de ventana — aquí usamos el open Binance 5m como proxy de UI/scorer.
    """
    st_ms = int(window_start_ts) * 1000
    url = f"{BINANCE_REST_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": "5m", "startTime": st_ms, "limit": 1}
    try:
        async with http.get(url, params=params) as resp:
            if resp.status != 200:
                return None
            arr = json.loads(await resp.text())
        if not isinstance(arr, list) or not arr:
            return None
        row0 = arr[0]
        if not isinstance(row0, (list, tuple)) or len(row0) < 2:
            return None
        return float(row0[1])
    except Exception as e:  # noqa: BLE001
        log.debug("binance 5m open ts=%s: %s", window_start_ts, e)
        return None


async def _resolve_price_to_beat(
    http: aiohttp.ClientSession, data: dict[str, Any], slug: str
) -> float | None:
    ptb = _gamma_price_to_beat(data)
    if ptb is not None:
        return ptb
    wts = _window_ts_from_btc_slug(slug)
    if wts is None:
        return None
    return await _fetch_binance_5m_open(http, wts)


def _model_prob_from_signal(signal: dict[str, Any]) -> float:
    """Proxy P(UP) para CLOBSignalFilter a partir de dirección y confianza del scorer."""
    d = signal.get("direction")
    conf = float(signal.get("confidence") or 0.0)
    if d == "UP":
        return 0.5 + conf * 0.5
    if d == "DOWN":
        return 0.5 - conf * 0.5
    return 0.5


def clob_extreme_from_levels(
    clob_yes: float | None, elow: float, ehigh: float
) -> Literal["UP", "DOWN", "NEUTRAL"]:
    """YES extremo UP si precio bajo ``elow``, DOWN si por encima de ``ehigh``."""
    if clob_yes is None:
        return "NEUTRAL"
    try:
        y = float(clob_yes)
    except (TypeError, ValueError):
        return "NEUTRAL"
    if y != y:
        return "NEUTRAL"
    if y < float(elow):
        return "UP"
    if y > float(ehigh):
        return "DOWN"
    return "NEUTRAL"


def _minutes_elapsed_from_tte(tte_sec: float | None) -> str:
    if tte_sec is None:
        return ""
    try:
        elapsed = max(0.0, min(300.0, 300.0 - float(tte_sec)))
        return f"{elapsed / 60.0:.2f}"
    except (TypeError, ValueError):
        return ""


def _minutes_elapsed_float_for_filter(val: dict[str, Any], market: dict[str, Any] | None) -> float | None:
    """Minutos desde apertura ventana 5m: ``val`` / ``market`` o derivado de ``tte_sec``."""
    for src in (val, market or {}):
        if not isinstance(src, dict):
            continue
        raw = src.get("minutes_elapsed")
        if raw is not None and str(raw).strip() != "":
            try:
                return float(str(raw).strip().replace(",", "."))
            except (TypeError, ValueError):
                continue
    mkt = market or {}
    tte = mkt.get("tte_sec")
    if tte is None:
        return None
    try:
        elapsed_sec = max(0.0, min(300.0, 300.0 - float(tte)))
        return elapsed_sec / 60.0
    except (TypeError, ValueError):
        return None


def _scorer_confirms_extreme(signal: dict[str, Any], extreme: str) -> bool:
    d = signal.get("direction")
    if d is None or extreme == "NEUTRAL":
        return False
    d = str(d).upper()
    if extreme == "UP" and d == "UP":
        return True
    if extreme == "DOWN" and d == "DOWN":
        return True
    return False


class SixCycleEngine:
    """Loop principal 6 ciclos: Scan→Predict→Validate→Size→Fill→Settle."""

    def __init__(
        self,
        on_live_failure: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> None:
        print(f"NEW ENGINE INSTANCE id={id(self)}")
        self.scorer = MarketScorer()
        self._scorer_token: str | None = None
        self._filter = CLOBSignalFilter()
        self.trades = 0
        self.wins = 0
        self.losses = 0
        self.pnl_usdc = 0.0
        self.win_streak = 0
        self.best_streak = 0
        self._console = Console()
        self._csv_path = data_dir() / "logs" / "crypto_5m_sixcycle.csv"
        self._csv_lock = asyncio.Lock()
        self._last_signal_line = "Sin señal — edge insuficiente o sin mercados activos"
        self._settle_tasks: set[asyncio.Task[Any]] = set()
        self._http_session: aiohttp.ClientSession | None = None
        self._panel_extra_lines: list[str] = []
        self._stop_run = asyncio.Event()
        self._running = False
        self._prepare_ptb: float | None = None
        self._prepare_prev: float | None = None
        self._prepare_next_ts: int = 0
        self._prepare_slug: str = ""
        self._pending_signal: dict[str, Any] = {}
        self._last_val_snapshot: dict[str, Any] = {}
        self._current_market_id: str | None = None
        self._current_market_filled: bool = False
        self._filled_market_ids: set[str] = set()
        self._last_scan_focus_market_id: str | None = None
        self._config: dict[str, Any] = dict(_sixcycle_cfg.DEFAULT_SIXCYCLE_CONFIG)
        self._dry_run_effective = bool(DRY_RUN)
        self._daily_pnl_usdc: float = 0.0
        self._daily_pnl_date_utc: datetime | None = None
        self._on_live_failure = on_live_failure
        self.reload_config()
        self._ensure_csv_header()

    def reload_config(self) -> None:
        """Recarga ``sixcycle_config.json`` (llamar desde API tras POST o al inicio de cada ciclo)."""
        self._config = _sixcycle_cfg.load_config()
        if DRY_RUN:
            self._dry_run_effective = True
        else:
            self._dry_run_effective = bool(self._config.get("dry_run", False))

    def _row_profile_meta(self) -> tuple[str, str]:
        """Etiqueta legible + huella de parámetros (para CSV/Postgres y análisis por versión)."""
        slug = str(self._config.get("profile_slug") or "default").strip() or "default"
        fp = _sixcycle_cfg.config_fingerprint(self._config)
        return slug, fp

    def _maybe_reset_daily_pnl_utc(self) -> None:
        """Reinicia contador de PnL diario (UTC) para circuit breaker."""
        now = datetime.now(timezone.utc).date()
        if self._daily_pnl_date_utc != now:
            self._daily_pnl_date_utc = now
            self._daily_pnl_usdc = 0.0

    def _record_settled_daily_pnl(self, pnl: float, resolved: str) -> None:
        if resolved not in ("win", "loss"):
            return
        self._maybe_reset_daily_pnl_utc()
        self._daily_pnl_usdc += float(pnl)

    def _open_settle_task_count(self) -> int:
        return sum(1 for t in self._settle_tasks if not t.done())

    def _extreme_yes(self, clob_yes: float | None) -> Literal["UP", "DOWN", "NEUTRAL"]:
        return clob_extreme_from_levels(
            clob_yes,
            float(self._config["clob_extreme_low"]),
            float(self._config["clob_extreme_high"]),
        )

    def _clob_bar_plain(self, clob_yes: float | None) -> str:
        ex = self._extreme_yes(clob_yes)
        el = float(self._config["clob_extreme_low"])
        eh = float(self._config["clob_extreme_high"])
        if ex == "UP":
            return "CLOB " + "█" * 12 + f" UP   (YES<{el})"
        if ex == "DOWN":
            return "CLOB " + "█" * 12 + f" DOWN (YES>{eh})"
        return "CLOB " + "░" * 12 + " neutro"

    def _clob_bar_rich(self, clob_yes: float | None) -> Text:
        ex = self._extreme_yes(clob_yes)
        el = float(self._config["clob_extreme_low"])
        eh = float(self._config["clob_extreme_high"])
        if ex == "UP":
            return Text.from_markup(
                f"[bold green]CLOB ████████████[/] [green]UP[/] [dim](YES<{el})[/]"
            )
        if ex == "DOWN":
            return Text.from_markup(
                f"[bold red]CLOB ████████████[/] [red]DOWN[/] [dim](YES>{eh})[/]"
            )
        return Text.from_markup("[dim]CLOB ░░░░░░░░░░░░ neutro[/]")

    def _validate_with_clob_extreme_override(
        self,
        signal: dict[str, Any],
        clob_yes: float,
        liquidity: float,
        market: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Primero CLOBSignalFilter; si no hay señal y el CLOB está en extremo con liquidez OK,
        re-evalúa con model_prob sintético (misma clase, sin tocar clob_signal_filter.py).
        """
        val = self.cycle_validate(signal, clob_yes, liquidity, market)
        if val.get("signal"):
            return self._apply_empirical_post_filters(val, signal, clob_yes, liquidity, market)
        if liquidity < MIN_LIQUIDITY_USDC:
            return val
        eps = 1e-5
        c_hi = float(self._config["clob_threshold_high"])
        c_lo = float(self._config["clob_threshold_low"])
        meff = float(self._config["min_edge"])
        if clob_yes > c_hi:
            m_try = min(0.5, float(clob_yes) - meff - eps)
            m_try = max(0.01, m_try)
            direction = "NO"
            edge = float(clob_yes) - m_try
            log.info(
                "CLOB extremo detectado: yes=%.3f → %s edge=%.3f",
                clob_yes,
                direction,
                edge,
            )
            v2 = self._filter.evaluate(
                m_try,
                float(clob_yes),
                min_edge=meff,
                min_liquidity_usdc=MIN_LIQUIDITY_USDC,
                liquidity=float(liquidity),
            )
            if v2.get("signal") and str(v2.get("direction")) == "NO":
                r = dict(v2)
                r["reason"] = str(r.get("reason", "")) + f" [clob_extreme>{c_hi}]"
                return self._apply_empirical_post_filters(r, signal, clob_yes, liquidity, market)
        elif clob_yes < c_lo:
            # Debe quedar estrictamente > 0.5 para rama YES del filtro.
            m_try = max(0.5 + 2e-4, float(clob_yes) + meff + eps)
            m_try = min(0.99, m_try)
            direction = "YES"
            edge = m_try - float(clob_yes)
            log.info(
                "CLOB extremo detectado: yes=%.3f → %s edge=%.3f",
                clob_yes,
                direction,
                edge,
            )
            v2 = self._filter.evaluate(
                m_try,
                float(clob_yes),
                min_edge=meff,
                min_liquidity_usdc=MIN_LIQUIDITY_USDC,
                liquidity=float(liquidity),
            )
            if v2.get("signal") and str(v2.get("direction")) == "YES":
                r = dict(v2)
                r["reason"] = str(r.get("reason", "")) + f" [clob_extreme<{c_lo}]"
                return self._apply_empirical_post_filters(r, signal, clob_yes, liquidity, market)
        return val

    def _sync_sixcycle_state(
        self,
        phase: str,
        *,
        signal: dict[str, Any] | None = None,
        markets: list[dict[str, Any]] | None = None,
    ) -> None:
        """Actualiza ``SIXCYCLE_STATE`` para SSE /api/sixcycle/live (API en otro proceso)."""
        now = datetime.now(timezone.utc)
        sig = dict(signal) if signal else {}
        markets = markets or []
        m0 = markets[0] if markets else {}
        comp_raw = sig.get("components") or {}
        comp: dict[str, float] = {}
        if isinstance(comp_raw, dict):
            for k, v in comp_raw.items():
                try:
                    comp[str(k)] = float(v)
                except (TypeError, ValueError):
                    pass
        ptb_gap = comp.get("ptb_gap")
        if ptb_gap is None:
            try:
                sp = float(sig["spot_price"]) if sig.get("spot_price") is not None else None
            except (TypeError, ValueError):
                sp = None
            ptb_e: float | None = None
            if m0.get("price_to_beat") is not None:
                try:
                    ptb_e = float(m0["price_to_beat"])
                except (TypeError, ValueError):
                    ptb_e = None
            if ptb_e is None and self._prepare_ptb is not None:
                try:
                    ptb_e = float(self._prepare_ptb)
                except (TypeError, ValueError):
                    ptb_e = None
            if sp is not None and ptb_e is not None:
                ptb_gap = float(sp) - float(ptb_e)

        eff_phase = phase
        if phase == "EJECUTANDO" and len(self._settle_tasks) > 0:
            eff_phase = "SETTLING"
        elif phase == "EJECUTANDO" and not markets:
            eff_phase = "SKIPPED"

        next_slug = ""
        secs_open = 0
        if phase == "PREPARANDO":
            next_slug = self._prepare_slug or ""
            secs_open = max(0, int(self._prepare_next_ts) - int(time.time()))
        else:
            next_slug = _slug_btc_5m(_floor_5m_window_ts_utc(now))
            nxt = next_market_slug_ts_for_prepare(now)
            secs_open = max(0, int(nxt) - int(time.time()))

        ptb: float | None = None
        if phase == "PREPARANDO":
            if self._prepare_ptb is not None:
                try:
                    ptb = float(self._prepare_ptb)
                except (TypeError, ValueError):
                    ptb = None
        elif m0.get("price_to_beat") is not None:
            try:
                ptb = float(m0["price_to_beat"])
            except (TypeError, ValueError):
                ptb = None
        elif self._prepare_ptb is not None:
            try:
                ptb = float(self._prepare_ptb)
            except (TypeError, ValueError):
                ptb = None

        clob_yes: float | None = None
        if sig.get("clob_yes_price") is not None:
            try:
                clob_yes = float(sig["clob_yes_price"])
            except (TypeError, ValueError):
                clob_yes = None
        elif m0.get("clob_yes_price") is not None:
            try:
                clob_yes = float(m0["clob_yes_price"])
            except (TypeError, ValueError):
                clob_yes = None

        clob_no: float | None = None
        if clob_yes is not None:
            try:
                clob_no = max(0.0, min(1.0, 1.0 - float(clob_yes)))
            except (TypeError, ValueError):
                clob_no = None
        clob_ex_s = self._extreme_yes(clob_yes)
        clob_bar_s = self._clob_bar_plain(clob_yes)

        liq: float | None = None
        if sig.get("liquidity_usdc") is not None:
            try:
                liq = float(sig["liquidity_usdc"])
            except (TypeError, ValueError):
                liq = None
        elif m0.get("liquidity_usdc") is not None:
            try:
                liq = float(m0["liquidity_usdc"])
            except (TypeError, ValueError):
                liq = None

        val = self._last_val_snapshot or {}
        try:
            edge_f = float(val.get("edge", 0.0) or 0.0)
        except (TypeError, ValueError):
            edge_f = 0.0
        sig_bool = bool(val.get("signal"))

        try:
            spot = float(sig["spot_price"]) if sig.get("spot_price") is not None else None
        except (TypeError, ValueError):
            spot = None

        try:
            score_i = int(sig.get("score", 0) or 0)
        except (TypeError, ValueError):
            score_i = 0
        try:
            conf_f = float(sig.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            conf_f = 0.0

        direction = sig.get("direction")
        if direction is not None:
            direction = str(direction)

        wr = (100.0 * self.wins / self.trades) if self.trades else 0.0
        prev_live_error: Any = None
        prev_live_error_ts: Any = None
        prev_live_error_reason: Any = None
        with SIXCYCLE_STATE_LOCK:
            prev_live_error = SIXCYCLE_STATE.get("live_error")
            prev_live_error_ts = SIXCYCLE_STATE.get("live_error_ts")
            prev_live_error_reason = SIXCYCLE_STATE.get("live_error_reason")

        payload: dict[str, Any] = {
            "timestamp_utc": now.isoformat(),
            "phase": eff_phase,
            "next_slug": next_slug,
            "secs_until_open": secs_open,
            "price_to_beat": ptb,
            "spot_price": spot,
            "ptb_gap": float(ptb_gap) if ptb_gap is not None else None,
            "score": score_i,
            "direction": direction,
            "confidence": conf_f,
            "ready": bool(sig.get("ready")),
            "components": comp,
            "clob_yes_price": clob_yes,
            "liquidity_usdc": liq,
            "signal": sig_bool,
            "edge": edge_f,
            "trades": int(self.trades),
            "win_rate": round(wr, 2),
            "pnl_usdc": round(float(self.pnl_usdc), 6),
            "win_streak": int(self.win_streak),
            "best_streak": int(self.best_streak),
            "dry_run": bool(self._dry_run_effective),
            "dry_run_stake_multiplier": float(get_dry_run_stake_multiplier()) if DRY_RUN else 1.0,
            "clob_extreme": clob_ex_s,
            "clob_no_price": clob_no,
            "clob_bar": clob_bar_s,
            "live_error": prev_live_error,
            "live_error_ts": prev_live_error_ts,
            "live_error_reason": prev_live_error_reason,
        }
        with SIXCYCLE_STATE_LOCK:
            SIXCYCLE_STATE.clear()
            SIXCYCLE_STATE.update(payload)

    def _publish_trade_counters_to_state(self) -> None:
        """Actualiza solo KPIs de carrera en ``SIXCYCLE_STATE`` (p. ej. tras settle en tarea aparte)."""
        now = datetime.now(timezone.utc).isoformat()
        wr = (100.0 * self.wins / self.trades) if self.trades else 0.0
        with SIXCYCLE_STATE_LOCK:
            SIXCYCLE_STATE["timestamp_utc"] = now
            SIXCYCLE_STATE["trades"] = int(self.trades)
            SIXCYCLE_STATE["win_rate"] = round(wr, 2)
            SIXCYCLE_STATE["pnl_usdc"] = round(float(self.pnl_usdc), 6)
            SIXCYCLE_STATE["win_streak"] = int(self.win_streak)
            SIXCYCLE_STATE["best_streak"] = int(self.best_streak)
            if DRY_RUN:
                SIXCYCLE_STATE["dry_run_stake_multiplier"] = float(get_dry_run_stake_multiplier())

    def _clear_live_error_state(self) -> None:
        with SIXCYCLE_STATE_LOCK:
            SIXCYCLE_STATE["live_error"] = None
            SIXCYCLE_STATE["live_error_ts"] = None
            SIXCYCLE_STATE["live_error_reason"] = None

    async def _mark_live_failure_and_stop(
        self,
        *,
        reason: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "reason": str(reason or "live_order_failed"),
            "detail": dict(detail or {}),
            "ts": now_iso,
        }
        with SIXCYCLE_STATE_LOCK:
            SIXCYCLE_STATE["timestamp_utc"] = now_iso
            SIXCYCLE_STATE["phase"] = "FAILED_STOPPED"
            SIXCYCLE_STATE["signal"] = False
            SIXCYCLE_STATE["live_error"] = payload["detail"]
            SIXCYCLE_STATE["live_error_ts"] = now_iso
            SIXCYCLE_STATE["live_error_reason"] = payload["reason"]
        self._stop_run.set()
        if self._on_live_failure is not None:
            try:
                await self._on_live_failure(payload)
            except Exception:
                log.exception("on_live_failure callback falló")

    async def shutdown(self) -> None:
        """Solicita salida del bucle principal; ``run()`` hace ``scorer.stop()`` en ``finally``."""
        self._stop_run.set()

    async def _sleep_scan_interval_or_stop(self) -> bool:
        """Espera SCAN_INTERVAL o hasta stop. Devuelve True si hay que salir del bucle principal."""
        sleep_t = asyncio.create_task(asyncio.sleep(float(SCAN_INTERVAL_SECONDS)))
        stop_t = asyncio.create_task(self._stop_run.wait())
        done, pending = await asyncio.wait(
            {sleep_t, stop_t},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        return self._stop_run.is_set()

    async def _sleep_prepare_tick_or_stop(self, seconds: float) -> bool:
        """Igual que scan sleep pero con intervalos cortos (PREPARANDO)."""
        sleep_t = asyncio.create_task(asyncio.sleep(float(seconds)))
        stop_t = asyncio.create_task(self._stop_run.wait())
        done, pending = await asyncio.wait(
            {sleep_t, stop_t},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        return self._stop_run.is_set()

    async def _fetch_prev_market_result(
        self, http: aiohttp.ClientSession, next_ts: int
    ) -> float | None:
        """% UP del mercado anterior (slug con next_ts - 300)."""
        prev_ts = int(next_ts) - 300
        slug = _slug_btc_5m(prev_ts)
        url = f"{GAMMA_API_URL}/markets/slug/{slug}"
        try:
            async with http.get(url) as resp:
                if resp.status != 200:
                    return None
                text = await resp.text()
                data = json.loads(text)
            if isinstance(data, list):
                data = data[0] if data else {}
            if not isinstance(data, dict):
                return None
            prices = _parse_outcome_prices(data)
            if prices and len(prices) >= 1:
                return float(prices[0])
        except Exception as e:  # noqa: BLE001
            log.debug("prev_market_result %s: %s", slug, e)
        return None

    async def _fetch_next_market_price_to_beat(
        self, http: aiohttp.ClientSession, next_ts: int
    ) -> float | None:
        """openPrice / startPrice del mercado que abre en next_ts."""
        slug = _slug_btc_5m(int(next_ts))
        url = f"{GAMMA_API_URL}/markets/slug/{slug}"
        try:
            async with http.get(url) as resp:
                if resp.status != 200:
                    return None
                text = await resp.text()
                data = json.loads(text)
            if isinstance(data, list):
                data = data[0] if data else {}
            if not isinstance(data, dict):
                return None
            return await _resolve_price_to_beat(http, data, slug)
        except Exception as e:  # noqa: BLE001
            log.debug("next_market_ptb %s: %s", slug, e)
        return None

    async def _prepare_phase(self, http: aiohttp.ClientSession) -> None:
        next_ts = next_market_slug_ts_for_prepare()
        prev_result = await self._fetch_prev_market_result(http, next_ts)
        tsec = int(time.time())
        current_ts = (tsec // 300) * 300
        price_to_beat = await self._fetch_next_market_price_to_beat(http, current_ts)
        self._prepare_ptb = price_to_beat
        self._prepare_prev = prev_result
        self._prepare_next_ts = int(next_ts)
        self._prepare_slug = _slug_btc_5m(int(next_ts))
        self._pending_signal = self.cycle_predict(
            price_to_beat=price_to_beat,
            prev_market_up_pct=prev_result,
        )

    def _render_preparing_panel(self, mode: str) -> None:
        sig = self._pending_signal or {}
        now_ts = int(time.time())
        sec_left = max(0, int(self._prepare_next_ts) - now_ts)
        ptb = self._prepare_ptb
        prev = self._prepare_prev
        spot = sig.get("spot_price")
        score = sig.get("score")
        direction = sig.get("direction") or "—"
        conf = float(sig.get("confidence") or 0.0)
        ready = sig.get("ready")
        cb = sig.get("candles_below_ptb", 0)
        comp = sig.get("components") or {}
        comp_parts = []
        for k in sorted(comp.keys()):
            v = comp[k]
            try:
                fv = float(v)
                comp_parts.append(f"{k}={fv:+.2f}" if abs(fv) < 1000 else f"{k}={fv:.4g}")
            except (TypeError, ValueError):
                comp_parts.append(f"{k}={v}")
        comp_s = " ".join(comp_parts) if comp_parts else "—"
        prev_line = "Mercado anterior: —"
        if prev is not None:
            prev_line = f"Mercado anterior: {prev * 100:.0f}% UP"
            cel = float(self._config["clob_extreme_low"])
            ceh = float(self._config["clob_extreme_high"])
            if prev < cel:
                prev_line += " (sesgo DOWN)"
            elif prev > ceh:
                prev_line += " (sesgo UP)"
        wr = (100.0 * self.wins / self.trades) if self.trades else 0.0
        body = (
            f"Abre en: {sec_left}s  Price to Beat: {_fmt_usd(ptb)}\n"
            f"{prev_line}\n"
            f"Score: {score} → {direction} (conf={conf:.2f})\n"
            f"{comp_s}\n"
            f"Spot: {_fmt_usd(float(spot) if spot is not None else None)}  "
            f"ready={ready}  ptb_below(5)={cb}\n"
            f"trades={self.trades} win_rate={wr:.1f}% pnl_usdc={self.pnl_usdc:+.4f} "
            f"streak={self.win_streak}"
        )
        title = f"{mode} PREPARANDO {self._prepare_slug}"
        self._console.print(Panel.fit(body, title=title, border_style="yellow"))

    def _migrate_sixcycle_csv_add_profile_columns(self) -> None:
        """Añade columnas de versión de config sin perder filas históricas."""
        src = self._csv_path
        rows: list[dict[str, str]] = []
        try:
            with src.open("r", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                for row in rdr:
                    row = dict(row)
                    row.setdefault("config_profile_slug", "")
                    row.setdefault("config_fingerprint", "")
                    rows.append({k: str(row.get(k, "") or "") for k in CSV_COLUMNS})
        except OSError as e:
            log.warning("migrate sixcycle csv read failed: %s", e)
            return
        tmp = src.with_suffix(".csv.migrating")
        try:
            with tmp.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
                w.writeheader()
                for row in rows:
                    w.writerow(row)
            tmp.replace(src)
            log.info(
                "CSV sixcycle migrado a cabecera con config_profile_slug/config_fingerprint (%d filas)",
                len(rows),
            )
        except OSError as e:
            log.warning("migrate sixcycle csv write failed: %s", e)
            try:
                if tmp.is_file():
                    tmp.unlink()
            except OSError:
                pass

    def _ensure_csv_header(self) -> None:
        self._csv_path.parent.mkdir(parents=True, exist_ok=True)
        expected = ",".join(CSV_COLUMNS)
        if not self._csv_path.is_file() or self._csv_path.stat().st_size == 0:
            with self._csv_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                w.writeheader()
            return
        try:
            with self._csv_path.open("r", encoding="utf-8") as f:
                first = f.readline().strip()
        except OSError:
            first = ""
        if first == expected:
            return
        if first == SIXCYCLE_CSV_LEGACY_HEADER:
            self._migrate_sixcycle_csv_add_profile_columns()
            return
        bak = self._csv_path.with_name(self._csv_path.name + ".bak_" + str(int(time.time())))
        try:
            self._csv_path.rename(bak)
            log.warning("Cabecera CSV sixcycle distinta: archivo anterior → %s", bak.name)
        except OSError as e:
            log.warning("No se pudo rotar CSV anterior (%s); se trunca cabecera nueva.", e)
        with self._csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()

    async def _append_csv(self, row: dict[str, Any]) -> None:
        async with self._csv_lock:
            out_row = {k: row.get(k, "") for k in CSV_COLUMNS}
            with self._csv_path.open("a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
                w.writerow(out_row)
            try:
                from persistence.writes import append_sixcycle_row

                append_sixcycle_row(dict(out_row))
            except Exception:
                pass

    def _csv_bool(self, v: Any) -> str:
        if v is True or str(v).lower() in ("true", "1", "yes"):
            return "true"
        if v is False or str(v).lower() in ("false", "0", "no"):
            return "false"
        return ""

    def _csv_row_scan(
        self,
        *,
        phase: str,
        market: dict[str, Any] | None,
        signal: dict[str, Any],
        val: dict[str, Any] | None,
        stake_usdc: float | None = None,
        fill_price: float | None = None,
        simulated: bool | None = None,
    ) -> dict[str, Any]:
        ts = datetime.now(timezone.utc).isoformat()
        wr = (100.0 * self.wins / self.trades) if self.trades else 0.0
        m = market or {}
        mid = str(m.get("market_id", "") or "")
        slug = str(m.get("market_slug", "") or "")
        tte = m.get("tte_sec")
        yes = m.get("clob_yes_price")
        liq = m.get("liquidity_usdc")
        try:
            yes_f = float(yes) if yes is not None else None
        except (TypeError, ValueError):
            yes_f = None
        no_f = max(0.0, min(1.0, 1.0 - yes_f)) if yes_f is not None else None
        extreme = self._extreme_yes(yes_f)
        try:
            liq_f = float(liq) if liq is not None else 0.0
        except (TypeError, ValueError):
            liq_f = 0.0
        try:
            score_i = int(signal.get("score", 0) or 0)
        except (TypeError, ValueError):
            score_i = 0
        sdir = signal.get("direction")
        sdir_s = str(sdir) if sdir is not None else ""
        confirms = _scorer_confirms_extreme(signal, extreme)
        v = val or {}
        sig_b = bool(v.get("signal"))
        edge_v = v.get("edge", "")
        dir_v = str(v.get("direction", "") or "")
        reason_v = str(v.get("reason", "") or "")
        pslug, pfp = self._row_profile_meta()
        return {
            "timestamp_utc": ts,
            "phase": phase,
            "market_id": mid,
            "market_slug": slug,
            "minutes_elapsed": _minutes_elapsed_from_tte(float(tte) if tte is not None else None),
            "clob_yes_price": f"{yes_f:.6f}" if yes_f is not None else "",
            "clob_no_price": f"{no_f:.6f}" if no_f is not None else "",
            "clob_extreme": extreme,
            "liquidity_usdc": f"{liq_f:.4f}" if liq is not None else "",
            "score": str(score_i),
            "scorer_direction": sdir_s,
            "scorer_confirms": self._csv_bool(confirms),
            "signal": self._csv_bool(sig_b),
            "direction": dir_v,
            "edge": f"{float(edge_v):.6f}" if edge_v != "" and edge_v is not None else "",
            "stake_usdc": f"{float(stake_usdc):.4f}" if stake_usdc is not None else "",
            "fill_price": f"{float(fill_price):.6f}" if fill_price is not None else "",
            "simulated": self._csv_bool(simulated) if simulated is not None else "",
            "resolved": "",
            "resolution_up": "",
            "pnl_usdc": "",
            "win": "",
            "win_streak": str(self.win_streak),
            "trades_total": str(self.trades),
            "win_rate_pct": f"{wr:.2f}",
            "pnl_cumulative_usdc": f"{self.pnl_usdc:.6f}",
            "reason": reason_v,
            "dry_run": self._csv_bool(self._dry_run_effective),
            "config_profile_slug": pslug,
            "config_fingerprint": pfp,
        }

    async def run(self) -> None:
        """Loop infinito con SCAN_INTERVAL_SECONDS entre iteraciones."""
        if self._running:
            log.warning("Engine ya corriendo — ignorando segunda llamada a run()")
            return
        self._running = True
        try:
            await self._run_main_loop()
        finally:
            self._running = False

    async def _run_main_loop(self) -> None:
        mode = "[DRY_RUN]" if self._dry_run_effective else "[LIVE]"
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers=_DEFAULT_HEADERS,
        ) as http:
            self._http_session = http
            self._clear_live_error_state()
            async with PolyCLOBClient(
                api_key=os.getenv("POLY_API_KEY", ""),
                private_key=os.getenv("POLY_PRIVATE_KEY", ""),
                dry_run=DRY_RUN,
            ) as clob:
                try:
                    asyncio.create_task(self.scorer.start(token_id=None))
                    await asyncio.sleep(5.0)
                    while not self._stop_run.is_set():
                        self.reload_config()
                        mode = "[DRY_RUN]" if self._dry_run_effective else "[LIVE]"
                        now = datetime.now(timezone.utc)
                        tsec = int(now.timestamp())
                        win = (tsec // 300) * 300
                        phase_sec = tsec - win
                        preparing = phase_sec >= (300 - PREPARE_LAST_SECONDS)

                        if preparing:
                            try:
                                await self._prepare_phase(http)
                            except Exception as e:  # noqa: BLE001
                                log.warning("_prepare_phase error", extra={"error": str(e)})
                            self._sync_sixcycle_state(
                                "PREPARANDO",
                                signal=self._pending_signal or {},
                            )
                            self._render_preparing_panel(mode)
                            if await self._sleep_prepare_tick_or_stop(1.0):
                                break
                            continue

                        t0 = time.perf_counter()
                        self._panel_extra_lines = []
                        self._last_val_snapshot = {}
                        cycle_scan_ms = cycle_predict_ms = cycle_validate_ms = 0.0
                        cycle_size_ms = cycle_fill_ms = 0.0
                        model_prob = 0.5
                        signal: dict[str, Any] = {}
                        try:
                            t_scan = time.perf_counter()
                            markets, scan_rejections = await self.cycle_scan(http, clob)
                            cycle_scan_ms = (time.perf_counter() - t_scan) * 1000.0
                            log.debug(
                                "scan: %s mercado(s) tras filtros 60-270s",
                                len(markets),
                            )
                            for r in scan_rejections:
                                log.debug("scan_reject %s", r)
                        except Exception as e:  # noqa: BLE001
                            log.warning("cycle_scan error", extra={"error": str(e)})
                            markets = []
                            scan_rejections = []
                            cycle_scan_ms = (time.perf_counter() - t0) * 1000.0

                        if markets:
                            tid = str(markets[0].get("token_yes_id") or "").strip()
                            if tid and tid != self._scorer_token:
                                await self.scorer.start(token_id=tid)
                                self._scorer_token = tid

                        if not markets:
                            self._panel_extra_lines.append(
                                "Scan: 0 mercados activos en ventana 60-270s"
                            )
                        else:
                            self._panel_extra_lines.append(
                                f"Scan: {len(markets)} mercado(s) en ventana 60-270s"
                            )
                            for m in markets:
                                self._panel_extra_lines.append(
                                    "  • "
                                    f"id={m['market_id']} TTE={float(m['tte_sec']):.0f}s "
                                    f"YES={float(m['clob_yes_price']):.4f} "
                                    f"liq={float(m['liquidity_usdc']):.1f} USDC"
                                )

                        try:
                            t_pred = time.perf_counter()
                            ptb_exec: float | None = self._prepare_ptb
                            if markets:
                                raw_ptb = markets[0].get("price_to_beat")
                                if raw_ptb is not None:
                                    try:
                                        ptb_exec = float(raw_ptb)
                                    except (TypeError, ValueError):
                                        pass
                            signal = self.cycle_predict(
                                price_to_beat=ptb_exec,
                                prev_market_up_pct=self._prepare_prev,
                            )
                            model_prob = _model_prob_from_signal(signal)
                            cycle_predict_ms = (time.perf_counter() - t_pred) * 1000.0
                        except Exception as e:  # noqa: BLE001
                            log.warning("predict/scorer error", extra={"error": str(e)})
                            self._sync_sixcycle_state(
                                "EJECUTANDO",
                                signal={},
                                markets=markets,
                            )
                            self._render_dashboard(
                                mode,
                                cycle_scan_ms,
                                cycle_predict_ms,
                                cycle_validate_ms,
                                cycle_size_ms,
                                cycle_fill_ms,
                            )
                            if await self._sleep_scan_interval_or_stop():
                                break
                            continue

                        if markets:
                            comp = signal.get("components") or {}
                            comp_s = " ".join(
                                f"{k}={v:+.0f}"
                                for k, v in sorted(comp.items())
                                if float(v) != 0.0
                            )
                            self._panel_extra_lines.append(
                                f"  Scorer: score={signal.get('score')} dir={signal.get('direction')} "
                                f"conf={float(signal.get('confidence') or 0):.3f} "
                                f"ready={signal.get('ready')} spot={signal.get('spot_price')} "
                                f"ptb_below={signal.get('candles_below_ptb')}"
                            )
                            if comp_s:
                                self._panel_extra_lines.append(f"    componentes: {comp_s}")
                            self._panel_extra_lines.append(f"  P(up) proxy={model_prob:.4f}")

                        signal_taken = False
                        executed_row: dict[str, Any] | None = None
                        for m in markets:
                            mid = str(m.get("market_id", "")).strip()
                            if mid in self._filled_market_ids:
                                continue
                            if (
                                self._current_market_filled
                                and self._current_market_id is not None
                                and mid != self._current_market_id
                            ):
                                continue
                            if self._current_market_id == mid and self._current_market_filled:
                                continue
                            if self._current_market_id != mid:
                                self._current_market_id = mid
                                self._current_market_filled = False
                            try:
                                t_val = time.perf_counter()
                                clob_book = float(m["clob_yes_price"])
                                clob_px = signal.get("clob_yes_price")
                                if clob_px is None:
                                    clob_px = clob_book
                                else:
                                    clob_px = float(clob_px)
                                liq_ws = signal.get("liquidity_usdc")
                                liq = (
                                    float(liq_ws)
                                    if liq_ws is not None
                                    else float(m["liquidity_usdc"])
                                )
                                # Extremos CLOB vs precio YES del scan (evita desalineación con WS del scorer).
                                val = self._validate_with_clob_extreme_override(signal, clob_book, liq, m)
                                self._last_val_snapshot = dict(val)
                                cycle_validate_ms = (time.perf_counter() - t_val) * 1000.0
                            except Exception as e:  # noqa: BLE001
                                log.warning("cycle_validate error", extra={"error": str(e)})
                                continue

                            log.debug(
                                "mercado id=%s TTE=%.0fs YES=%.4f liq=%.1f model_prob=%.4f edge=%.4f signal=%s",
                                m["market_id"],
                                float(m["tte_sec"]),
                                clob_px,
                                liq,
                                model_prob,
                                float(val.get("edge", 0.0)),
                                val.get("signal"),
                            )

                            self._panel_extra_lines.append(
                                "  • "
                                f"id={m['market_id']} model={model_prob:.4f} "
                                f"edge={float(val.get('edge', 0.0)):.4f} "
                                f"signal={val.get('signal')} — {val.get('reason', '')}"
                            )

                            if not val.get("signal"):
                                self._last_signal_line = (
                                    f"Sin señal — {val.get('reason', 'edge insuficiente o sin liquidez')}"
                                )
                                continue

                            self._maybe_reset_daily_pnl_utc()
                            loss_limit = float(self._config["max_daily_loss_usdc"])
                            if self._daily_pnl_usdc < -loss_limit:
                                log.warning(
                                    "Circuit breaker: pérdida diaria %.2f USDC supera límite %.2f — no fill",
                                    self._daily_pnl_usdc,
                                    loss_limit,
                                )
                                self._last_signal_line = (
                                    f"Circuit breaker: pérdida diaria {self._daily_pnl_usdc:.2f} > límite {loss_limit:.2f}"
                                )
                                continue
                            max_ct = int(self._config["max_concurrent_trades"])
                            open_settles = self._open_settle_task_count()
                            if open_settles >= max_ct:
                                log.warning(
                                    "max_concurrent_trades=%s con settles abiertos=%s — no fill",
                                    max_ct,
                                    open_settles,
                                )
                                self._last_signal_line = (
                                    f"Límite posiciones abiertas ({open_settles}/{max_ct})"
                                )
                                continue

                            try:
                                t_sz = time.perf_counter()
                                stake = float(self._config["stake_usdc"])
                                if DRY_RUN:
                                    mult = float(get_dry_run_stake_multiplier())
                                    stake = float(stake) * mult
                                    stake = max(0.01, min(float(stake), 1.0e7))
                                cycle_size_ms = (time.perf_counter() - t_sz) * 1000.0
                            except Exception as e:  # noqa: BLE001
                                log.warning("cycle_size error", extra={"error": str(e)})
                                continue

                            if val.get("direction") == "NO":
                                token_no = str(m.get("token_no_id") or "").strip()
                                if not token_no:
                                    log.warning(
                                        "mercado %s: dirección NO sin token_no_id, se omite fill",
                                        m.get("market_id"),
                                    )
                                    continue
                                token_fill = token_no
                            else:
                                token_fill = str(m["token_yes_id"])
                            fill_price = float(m["clob_yes_price"])
                            if val.get("direction") == "NO":
                                px_no = await clob.get_price(str(token_fill), side="buy")
                                if px_no is not None:
                                    fill_price = float(px_no)

                            tte_now = float(m.get("tte_sec") or 0.0)
                            if not self._dry_run_effective and tte_now <= LIVE_MIN_TTE_SEC:
                                log.warning(
                                    "LIVE fill omitido por cierre inminente "
                                    "market=%s dir=%s tte=%.2fs guard=%.2fs",
                                    mid,
                                    str(val.get("direction") or "YES"),
                                    tte_now,
                                    LIVE_MIN_TTE_SEC,
                                )
                                self._last_signal_line = (
                                    f"LIVE omitido: cierre inminente (tte={tte_now:.2f}s <= {LIVE_MIN_TTE_SEC:.2f}s)"
                                )
                                continue

                            try:
                                t_fill = time.perf_counter()
                                fill_out = await self.cycle_fill(
                                    mid,
                                    str(token_fill),
                                    str(val.get("direction") or "YES"),
                                    stake,
                                    fill_price,
                                    clob,
                                )
                                cycle_fill_ms = (time.perf_counter() - t_fill) * 1000.0
                                log.info(
                                    "FILL: market=%s dir=%s stake=%.2f price=%.4f simulated=%s",
                                    mid,
                                    str(val.get("direction") or "YES"),
                                    stake,
                                    float(fill_out.get("price", fill_price)),
                                    fill_out.get("simulated", True),
                                )
                            except Exception as e:  # noqa: BLE001
                                log.exception(
                                    "cycle_fill error market=%s dir=%s token=%s stake=%.4f price=%.4f tte=%.2fs",
                                    mid,
                                    str(val.get("direction") or "YES"),
                                    str(token_fill)[:16],
                                    float(stake),
                                    float(fill_price),
                                    float(tte_now),
                                )
                                if not self._dry_run_effective:
                                    fail_detail: dict[str, Any] = {
                                        "market_id": mid,
                                        "direction": str(val.get("direction") or "YES"),
                                        "token_id": str(token_fill),
                                        "stake_usdc": float(stake),
                                        "price": float(fill_price),
                                        "tte_sec": float(tte_now),
                                        "error": str(e),
                                    }
                                    if isinstance(e, LiveOrderFailure):
                                        fail_detail.update(e.detail)
                                    log.critical(
                                        "LIVE fill falló — no reintento automático "
                                        "market=%s dir=%s tte=%.2fs",
                                        mid,
                                        str(val.get("direction") or "YES"),
                                        float(tte_now),
                                    )
                                    await self._mark_live_failure_and_stop(
                                        reason="live_order_failed",
                                        detail=fail_detail,
                                    )
                                    return
                                continue

                            fill_result = fill_out
                            if not bool(fill_result.get("filled")):
                                continue

                            self._current_market_filled = True
                            self._filled_market_ids.add(mid)

                            self._last_signal_line = (
                                f"{val.get('direction')} edge={val.get('edge'):.4f} stake={stake:.2f} "
                                f"filled={fill_result.get('filled')} sim={fill_result.get('simulated')}"
                            )
                            signal_taken = True

                            dir_s = str(val.get("direction") or "YES")
                            fill_px = float(fill_result.get("price", fill_price))
                            close_ts_eff: datetime | None = None
                            raw_ct = m.get("close_ts")
                            if isinstance(raw_ct, datetime):
                                close_ts_eff = raw_ct
                                if close_ts_eff.tzinfo is None:
                                    close_ts_eff = close_ts_eff.replace(tzinfo=timezone.utc)
                                else:
                                    close_ts_eff = close_ts_eff.astimezone(timezone.utc)
                            if close_ts_eff is None:
                                close_ts_eff = _close_ts_from_btc_slug_market(m)

                            log.info("FILL OK — lanzando settle para market_id=%s", mid)
                            executed_row = self._csv_row_scan(
                                phase="EJECUTANDO",
                                market=m,
                                signal=signal,
                                val=dict(val),
                                stake_usdc=stake,
                                fill_price=fill_px,
                                simulated=bool(fill_result.get("simulated", True)),
                            )
                            pslug, pfp = self._row_profile_meta()

                            settle_payload = {
                                "market_id": mid,
                                "market_slug": str(m.get("market_slug", "")),
                                "direction": dir_s,
                                "stake_usdc": stake,
                                "fill_price": fill_px,
                                "model_prob": model_prob,
                                "clob_yes_price": float(m["clob_yes_price"]),
                                "liquidity_usdc": float(m["liquidity_usdc"]),
                                "tte_sec": float(m["tte_sec"]),
                                "edge": float(val["edge"]),
                                "filled": bool(fill_result.get("filled")),
                                "simulated": bool(fill_result.get("simulated", True)),
                                "close_ts": close_ts_eff,
                                "cycle_ms_scan": round(cycle_scan_ms, 2),
                                "cycle_ms_predict": round(cycle_predict_ms, 2),
                                "cycle_ms_validate": round(cycle_validate_ms, 2),
                                "cycle_ms_size": round(cycle_size_ms, 2),
                                "cycle_ms_fill": round(cycle_fill_ms, 2),
                                "signal_snapshot": dict(signal),
                                "val_snapshot": dict(val),
                                "config_profile_slug": pslug,
                                "config_fingerprint": pfp,
                            }
                            task = asyncio.create_task(
                                self.cycle_settle(
                                    dict(settle_payload),
                                ),
                                name=f"settle-{mid}",
                            )
                            self._settle_tasks.add(task)
                            task.add_done_callback(self._settle_tasks.discard)
                            break

                        if not signal_taken and markets:
                            self._last_signal_line = (
                                "Sin señal — edge insuficiente o sin liquidez para los mercados escaneados"
                            )
                        elif not markets:
                            self._last_signal_line = (
                                "Sin señal — edge insuficiente o sin mercados activos"
                            )

                        try:
                            if executed_row is not None:
                                scan_row = executed_row
                            else:
                                m0 = markets[0] if markets else None
                                scan_row = self._csv_row_scan(
                                    phase="EJECUTANDO",
                                    market=m0,
                                    signal=signal,
                                    val=dict(self._last_val_snapshot) if self._last_val_snapshot else None,
                                )
                            await self._append_csv(scan_row)
                        except Exception as e:  # noqa: BLE001
                            log.debug("scan csv row: %s", e)

                        self._sync_sixcycle_state(
                            "EJECUTANDO",
                            signal=signal,
                            markets=markets,
                        )
                        if markets:
                            try:
                                self._console.print(self._clob_bar_rich(float(markets[0]["clob_yes_price"])))
                            except (KeyError, TypeError, ValueError):
                                self._console.print(self._clob_bar_rich(None))
                        self._render_dashboard(
                            mode,
                            cycle_scan_ms,
                            cycle_predict_ms,
                            cycle_validate_ms,
                            cycle_size_ms,
                            cycle_fill_ms,
                        )
                        if await self._sleep_scan_interval_or_stop():
                            break
                finally:
                    try:
                        await self.scorer.stop()
                    except Exception:  # noqa: BLE001
                        log.warning("scorer.stop en salida de run()", exc_info=True)

    def _render_dashboard(
        self,
        mode: str,
        ms_scan: float,
        ms_pred: float,
        ms_val: float,
        ms_sz: float,
        ms_fill: float,
    ) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        wr = (100.0 * self.wins / self.trades) if self.trades else 0.0
        header = f"{mode} {ts}"
        extra = ""
        if self._panel_extra_lines:
            extra = "\n" + "\n".join(self._panel_extra_lines)
        body = (
            f"trades={self.trades}  win_rate={wr:.1f}%  pnl_usdc={self.pnl_usdc:.4f}  "
            f"win_streak={self.win_streak}  best_streak={self.best_streak}\n"
            f"Última señal: {self._last_signal_line}{extra}\n"
            f"cycle_ms: scan={ms_scan:.1f} predict={ms_pred:.1f} validate={ms_val:.1f} "
            f"size={ms_sz:.1f} fill={ms_fill:.1f}"
        )
        self._console.print(Panel.fit(body, title=header, border_style="cyan"))

    async def cycle_scan(
        self, http: aiohttp.ClientSession, clob: PolyCLOBClient
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """
        Busca mercados BTC 5m activos en Gamma API.
        Retorna lista de mercados con: market_id, question,
        token_yes_id, close_ts, clob_yes_price, liquidity_usdc.
        Filtros: solo mercados con time_to_expiry > 60s y < 270s.
        """
        now = datetime.now(timezone.utc)
        win_ts = _floor_5m_window_ts_utc(now)
        out: list[dict[str, Any]] = []
        rejections: list[str] = []
        for delta in (0, -300, 300, -600, 600):
            slug = _slug_btc_5m(win_ts + delta)
            url = f"{GAMMA_API_URL}/markets/slug/{slug}"
            max_attempts = 10 if delta == 0 else 1
            data: dict[str, Any] | None = None
            for attempt in range(max_attempts):
                try:
                    async with http.get(url) as resp:
                        if resp.status == 404:
                            msg = f"{slug}: HTTP 404 (sin mercado)"
                            if attempt == max_attempts - 1:
                                rejections.append(msg)
                            log.debug(msg)
                            if attempt < max_attempts - 1:
                                await asyncio.sleep(1.0)
                            continue
                        text = await resp.text()
                        if resp.status != 200:
                            msg = f"{slug}: HTTP {resp.status}"
                            rejections.append(msg)
                            log.warning(
                                "gamma slug http",
                                extra={"status": resp.status, "slug": slug},
                            )
                            break
                        parsed = json.loads(text)
                except Exception as e:  # noqa: BLE001
                    msg = f"{slug}: error {e!s}"
                    rejections.append(msg)
                    log.warning("gamma slug error", extra={"slug": slug, "error": str(e)})
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(1.0)
                    continue
                else:
                    if not isinstance(parsed, dict):
                        msg = f"{slug}: respuesta no es dict"
                        rejections.append(msg)
                        log.debug(msg)
                        break
                    data = parsed
                    break
            if data is None:
                continue
            end_dt = _parse_gamma_end(data)
            if end_dt is None:
                msg = f"{slug}: sin endDate"
                rejections.append(msg)
                log.debug(msg)
                continue
            tte = (end_dt - now).total_seconds()
            mid_guess = str(
                data.get("id") or data.get("conditionId") or data.get("condition_id") or slug
            )
            if not (60.0 < tte < 270.0):
                msg = (
                    f"id={mid_guess} TTE fuera de rango 60-270s "
                    f"(tte={tte:.0f}s, slug={slug})"
                )
                rejections.append(msg)
                log.debug(msg)
                continue
            yid, _, rej = extract_yes_token_id(
                data.get("outcomes"),
                data.get("clobTokenIds") or data.get("clob_token_ids"),
                assume_first=True,
            )
            if not yid:
                msg = f"id={mid_guess} sin token YES/Up ({rej})"
                rejections.append(msg)
                log.debug("scan: sin token YES/Up", extra={"reject": rej, "slug": slug})
                continue
            nid = _gamma_no_token_id(data)
            mid = str(data.get("id") or data.get("conditionId") or data.get("condition_id") or "")
            if not mid:
                msg = f"{slug}: sin market_id"
                rejections.append(msg)
                log.debug(msg)
                continue
            book = await clob.get_orderbook(str(yid))
            if book.get("error"):
                msg = f"id={mid} CLOB book: {book.get('error')}"
                rejections.append(msg)
                log.warning("clob book yes", extra={"error": book.get("error")})
                continue
            px = await clob.get_price(str(yid), side="buy")
            if px is None:
                px = book.get("best_ask")
            if px is None:
                msg = f"id={mid} sin precio YES (buy/mid)"
                rejections.append(msg)
                log.debug(msg)
                continue
            liq = _sum_ask_notional_top_n(book, 5)
            # PTB siempre del Gamma ``data`` / slug de este mercado; al cambiar de mercado no arrastrar _prepare_ptb.
            if (
                self._last_scan_focus_market_id
                and mid
                and str(self._last_scan_focus_market_id).strip() != str(mid).strip()
            ):
                self._prepare_ptb = None
            self._last_scan_focus_market_id = str(mid).strip()
            ptb_row = await _resolve_price_to_beat(http, data, slug)
            out.append(
                {
                    "market_id": mid,
                    "market_slug": slug,
                    "question": str(data.get("question", "")),
                    "token_yes_id": str(yid),
                    "token_no_id": str(nid) if nid else "",
                    "close_ts": end_dt,
                    "tte_sec": float(tte),
                    "clob_yes_price": float(px),
                    "liquidity_usdc": float(liq),
                    "price_to_beat": ptb_row,
                }
            )
            break
        return out, rejections

    def cycle_predict(
        self,
        price_to_beat: float | None = None,
        prev_market_up_pct: float | None = None,
    ) -> dict[str, Any]:
        """Señal de confluencia vía WebSockets (Binance 1m + CLOB)."""
        return self.scorer.get_signal(
            price_to_beat=price_to_beat,
            prev_market_up_pct=prev_market_up_pct,
            min_abs_score=5,
        )

    def _apply_empirical_post_filters(
        self,
        val: dict[str, Any],
        signal: dict[str, Any],
        clob_yes_price: float,
        liquidity: float,
        market: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Tras ``CLOBSignalFilter.evaluate`` (u override de extremo): vetos empíricos (67 trades).
        Solo actúa si ``val['signal']`` ya es True; el ``reason`` del veto va a CSV y log.
        """
        if not val.get("signal"):
            return val
        minutes_elapsed = _minutes_elapsed_float_for_filter(val, market)
        cfg = self._config
        t_dlo = float(cfg["timing_dead_zone_low"])
        t_dhi = float(cfg["timing_dead_zone_high"])
        t_max = float(cfg["timing_max_minutes"])
        if minutes_elapsed is not None and t_dlo <= minutes_elapsed <= t_dhi:
            reason_tm = (
                f"timing: minuto {minutes_elapsed:.1f} en zona muerta "
                f"{t_dlo:.1f}-{t_dhi:.1f} (WR 0% empírico)"
            )
            log.info("Señal rechazada (filtro empírico): %s", reason_tm)
            return {**val, "signal": False, "reason": reason_tm}
        if minutes_elapsed is not None and minutes_elapsed > t_max:
            reason_tm = f"timing: minuto {minutes_elapsed:.1f} >{t_max:.1f} (WR 0% empírico)"
            log.info("Señal rechazada (filtro empírico): %s", reason_tm)
            return {**val, "signal": False, "reason": reason_tm}
        direction = str(val.get("direction") or "YES").upper()
        cy = float(clob_yes_price)
        if direction == "YES":
            fill = cy
        else:
            fill = max(0.0, min(1.0, 1.0 - cy))
        liq = float(liquidity)
        try:
            score = int(round(float(signal.get("score", 0) or 0)))
        except (TypeError, ValueError):
            score = 0

        f_min = float(cfg["fill_min"])
        f_dlo = float(cfg["fill_dead_zone_low"])
        f_dhi = float(cfg["fill_dead_zone_high"])
        f_max = float(cfg["fill_max"])
        liq_max = float(cfg["liquidity_max"])
        sc_min = int(cfg["score_min_abs"])

        reason_rej = ""
        if fill < f_min:
            reason_rej = f"fill {fill:.2f} <{f_min:.2f}: CLOB demasiado extremo, mercado tiene info"
        elif f_dlo <= fill < f_dhi:
            reason_rej = f"fill {fill:.2f} en zona muerta {f_dlo:.2f}-{f_dhi:.2f} (WR 8% empírico)"
        elif fill > f_max:
            reason_rej = f"fill {fill:.2f} >{f_max:.2f}: CLOB no suficientemente extremo"
        elif liq > liq_max:
            reason_rej = f"liquidez {liq:.0f} >{liq_max:.0f}: mercado demasiado eficiente"
        elif abs(score) < sc_min:
            reason_rej = (
                f"score {score} demasiado neutral (WR 13% empírico cuando |score|<{sc_min})"
            )

        if reason_rej:
            log.info("Señal rechazada (filtro empírico): %s", reason_rej)
            return {
                "signal": False,
                "edge": float(val.get("edge", 0) or 0),
                "direction": direction if direction in ("YES", "NO") else "YES",
                "reason": reason_rej,
            }
        return val

    def cycle_validate(
        self,
        signal: dict[str, Any],
        clob_yes_price: float,
        liquidity: float,
        market: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Llamar CLOBSignalFilter.evaluate() con P(up) derivada del scorer."""
        if not signal.get("ready") or signal.get("direction") is None:
            return {
                "signal": False,
                "edge": 0.0,
                "direction": "YES",
                "reason": "scorer sin dirección o datos insuficientes (buffer/CLOB)",
            }
        model_prob = _model_prob_from_signal(signal)
        meff = float(self._config["min_edge"])
        val = self._filter.evaluate(
            model_prob=model_prob,
            clob_yes_price=clob_yes_price,
            min_edge=meff,
            min_liquidity_usdc=MIN_LIQUIDITY_USDC,
            liquidity=liquidity,
        )
        if val.get("signal"):
            return self._apply_empirical_post_filters(val, signal, clob_yes_price, liquidity, market)
        return val

    def cycle_size(self, edge: float) -> float:
        """
        Kelly fraccionario: stake = kelly_fraction * edge * capital_disponible.
        Capital = max_stake_usdc desde config. Clamp [1.0, cap].
        """
        cap = float(self._config["max_stake_usdc"])
        kf = float(self._config["kelly_fraction"])
        stake = kf * float(edge) * cap
        stake = max(1.0, min(cap, stake))
        return float(stake)

    async def cycle_fill(
        self,
        market_id: str,
        token_id: str,
        direction: str,
        stake_usdc: float,
        clob_yes_price: float,
        clob: PolyCLOBClient,
    ) -> dict[str, Any]:
        """
        DRY_RUN: simular fill. LIVE: ``py-clob-client`` (cuenta centralizada JSON).
        """
        _ = token_id
        _ = clob
        pm = get_polymarket_client(bool(self._config.get("dry_run", True)))
        fill_px = float(clob_yes_price)
        dir_s = str(direction or "YES").upper()

        if self._dry_run_effective:
            log.info(
                "[DRY_RUN] fill simulado",
                extra={"token": token_id[:16], "stake": stake_usdc, "price": fill_px},
            )
            return {
                "filled": True,
                "price": fill_px,
                "order_id": "",
                "simulated": True,
            }
        try:

            def _place() -> dict[str, Any]:
                return pm.place_order(
                    market_id=str(market_id).strip(),
                    side=dir_s,
                    amount_usdc=float(stake_usdc),
                    price=fill_px,
                    token_id=str(token_id).strip(),
                )

            out = await asyncio.to_thread(_place)
            ok = bool(out.get("success"))
            oid = str(out.get("order_id") or "")
            if not ok:
                err = str(out.get("error") or "").strip()
                raw = out.get("raw")
                raw_s = ""
                if raw is not None:
                    raw_s = str(raw)[:300]
                log.warning(
                    "LIVE place_order rechazado market=%s dir=%s stake=%.4f price=%.4f error=%s raw=%s",
                    str(market_id).strip(),
                    dir_s,
                    float(stake_usdc),
                    float(fill_px),
                    err or "-",
                    raw_s or "-",
                )
                raise LiveOrderFailure(
                    f"LIVE place_order rechazado: {err or 'unknown_error'}",
                    detail={
                        "place_order_error": err or "unknown_error",
                        "order_raw": raw_s or None,
                        "market_id": str(market_id).strip(),
                        "direction": dir_s,
                        "stake_usdc": float(stake_usdc),
                        "price": float(fill_px),
                    },
                )
            return {
                "filled": ok,
                "price": float(out.get("price", fill_px)),
                "order_id": oid,
                "simulated": False,
            }
        except Exception as e:  # noqa: BLE001
            log.exception(
                "LIVE place_order falló market=%s dir=%s stake=%.4f price=%.4f",
                str(market_id).strip(),
                dir_s,
                float(stake_usdc),
                float(fill_px),
            )
            raise

    async def cycle_settle(self, settle_payload: dict[str, Any]) -> None:
        """
        Polling cada 30s hasta que el mercado cierre; resolución Gamma; PnL y CSV.
        Usar vía asyncio.create_task (no bloquea el scan loop).
        """
        http = self._http_session
        payload = dict(settle_payload)
        market_id = str(payload.get("market_id", "")).strip()
        direction = str(payload.get("direction", "YES"))
        stake_usdc = float(payload.get("stake_usdc", 0.0) or 0.0)
        close_ts_log = payload.get("close_ts")
        log.info(
            "SETTLE iniciado: market_id=%s direction=%s stake=%.2f close_ts=%s",
            market_id,
            direction,
            float(stake_usdc),
            close_ts_log,
        )
        if http is None:
            log.warning("cycle_settle sin sesión o payload")
            if str(self._current_market_id or "") == str(market_id).strip():
                self._current_market_id = None
                self._current_market_filled = False
            return
        if str(payload.get("market_id")) != str(market_id) or abs(
            float(payload.get("stake_usdc", 0)) - float(stake_usdc)
        ) > 1e-6:
            log.warning("cycle_settle: payload no coincide con argumentos")
        if str(payload.get("direction")) != str(direction):
            log.warning("cycle_settle: dirección payload vs arg")
        mid_settle = str(payload["market_id"]).strip()
        resolved_end, pnl_end = "", 0.0
        try:
            resolved_end, pnl_end = await self._settle_trade_loop(http, payload)
        finally:
            if str(self._current_market_id or "") == str(market_id).strip():
                self._current_market_id = None
                self._current_market_filled = False
        log.info(
            "SETTLE completado: market_id=%s resolved=%s pnl=%.4f",
            mid_settle,
            resolved_end,
            pnl_end,
        )

    async def _settle_trade_loop(
        self, http: aiohttp.ClientSession, payload: dict[str, Any]
    ) -> tuple[str, float]:
        """Polling hasta resolución; actualiza métricas y escribe CSV."""
        market_id = str(payload["market_id"]).strip()
        direction = str(payload["direction"])
        stake = float(payload["stake_usdc"])
        fill_price = float(payload["fill_price"])
        url = f"{GAMMA_API_URL}/markets/{market_id}"
        resolved = ""
        pnl = 0.0
        up_won_final: bool | None = None
        won = False

        raw_close = payload.get("close_ts")
        close_ts_dt: datetime | None = None
        if isinstance(raw_close, datetime):
            close_ts_dt = raw_close
            if close_ts_dt.tzinfo is None:
                close_ts_dt = close_ts_dt.replace(tzinfo=timezone.utc)
            else:
                close_ts_dt = close_ts_dt.astimezone(timezone.utc)
        elif isinstance(raw_close, str) and raw_close.strip():
            try:
                close_ts_dt = datetime.fromisoformat(raw_close.replace("Z", "+00:00"))
                if close_ts_dt.tzinfo is None:
                    close_ts_dt = close_ts_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                close_ts_dt = None

        attempt = 0
        try:
            while True:
                attempt += 1
                log.info("SETTLE polling: market_id=%s intento=%d", market_id, attempt)
                now = datetime.now(timezone.utc)
                if close_ts_dt is not None and now > close_ts_dt + timedelta(minutes=10):
                    log.warning(
                        "SETTLE timeout market_id=%s — resolviendo como unknown",
                        market_id,
                    )
                    resolved = "unknown"
                    pnl = 0.0
                    up_won_final = None
                    won = False
                    break

                try:
                    async with http.get(url) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            await asyncio.sleep(30.0)
                            continue
                        m = json.loads(text)
                except Exception as e:  # noqa: BLE001
                    log.warning("settle fetch", extra={"error": str(e)})
                    await asyncio.sleep(30.0)
                    continue
                if not isinstance(m, dict):
                    await asyncio.sleep(30.0)
                    continue

                log.debug("SETTLE gamma response: %s", str(m)[:500])

                end_dt = _parse_gamma_end(m)
                if end_dt is not None and now < end_dt:
                    await asyncio.sleep(30.0)
                    continue
                if not api_bool_true(m.get("closed")) and not api_bool_true(m.get("isClosed")):
                    await asyncio.sleep(30.0)
                    continue

                up_won = _settle_parse_up_won(m)
                if up_won is None:
                    await asyncio.sleep(30.0)
                    continue

                up_won_final = bool(up_won)
                bet_up = direction == "YES"
                won = (bet_up and up_won) or ((not bet_up) and (not up_won))
                if won:
                    pnl = stake * (1.0 / max(fill_price, 1e-9) - 1.0)
                    resolved = "win"
                    self.wins += 1
                    self.win_streak += 1
                    self.best_streak = max(self.best_streak, self.win_streak)
                else:
                    pnl = -stake
                    resolved = "loss"
                    self.losses += 1
                    self.win_streak = 0
                self.trades += 1
                self.pnl_usdc += pnl
                self._record_settled_daily_pnl(float(pnl), str(resolved))
                self._console.print(
                    Panel.fit(
                        f"[settle] {market_id} {resolved} pnl={pnl:.4f} USDC",
                        title="Settle",
                        border_style="green" if won else "red",
                    )
                )
                break
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("settle loop error", extra={"market_id": market_id})
            resolved = resolved or "error"

        log.info(
            "SETTLE: market=%s resolved=%s pnl=%.4f win=%s",
            market_id,
            resolved,
            pnl,
            won,
        )

        ts = datetime.now(timezone.utc).isoformat()
        wr = (100.0 * self.wins / self.trades) if self.trades else 0.0
        try:
            cy_f = float(payload.get("clob_yes_price", 0))
        except (TypeError, ValueError):
            cy_f = 0.0
        no_f = max(0.0, min(1.0, 1.0 - cy_f)) if cy_f else 0.0
        extreme = self._extreme_yes(cy_f if cy_f else None)
        sig_snap = payload.get("signal_snapshot") or {}
        val_snap = payload.get("val_snapshot") or {}
        try:
            score_i = int(sig_snap.get("score", 0) or 0)
        except (TypeError, ValueError):
            score_i = 0
        sdir = sig_snap.get("direction")
        sdir_s = str(sdir) if sdir is not None else ""
        confirms = _scorer_confirms_extreme(sig_snap, extreme)
        try:
            liq_p = float(payload.get("liquidity_usdc", 0) or 0)
        except (TypeError, ValueError):
            liq_p = 0.0
        tte_p = payload.get("tte_sec")
        try:
            tte_f = float(tte_p) if tte_p is not None else None
        except (TypeError, ValueError):
            tte_f = None

        win_cell = ""
        res_up_cell = ""
        if resolved == "win":
            win_cell = "true"
        elif resolved == "loss":
            win_cell = "false"
        if up_won_final is not None:
            res_up_cell = self._csv_bool(up_won_final)

        pslug = str(payload.get("config_profile_slug") or "").strip() or "default"
        pfp = str(payload.get("config_fingerprint") or "").strip()

        row = {
            "timestamp_utc": ts,
            "phase": "SETTLED",
            "market_id": market_id,
            "market_slug": str(payload.get("market_slug", "") or ""),
            "minutes_elapsed": _minutes_elapsed_from_tte(tte_f),
            "clob_yes_price": f"{cy_f:.6f}" if payload.get("clob_yes_price") is not None else "",
            "clob_no_price": f"{no_f:.6f}" if payload.get("clob_yes_price") is not None else "",
            "clob_extreme": extreme,
            "liquidity_usdc": f"{liq_p:.4f}" if payload.get("liquidity_usdc") is not None else "",
            "score": str(score_i),
            "scorer_direction": sdir_s,
            "scorer_confirms": self._csv_bool(confirms),
            "signal": self._csv_bool(bool(val_snap.get("signal"))),
            "direction": direction,
            "edge": f"{float(payload.get('edge', 0)):.6f}",
            "stake_usdc": f"{stake:.4f}",
            "fill_price": f"{float(payload.get('fill_price', fill_price)):.6f}",
            "simulated": self._csv_bool(bool(payload.get("simulated", True))),
            "resolved": resolved,
            "resolution_up": res_up_cell,
            "pnl_usdc": f"{pnl:.6f}",
            "win": win_cell,
            "win_streak": str(self.win_streak),
            "trades_total": str(self.trades),
            "win_rate_pct": f"{wr:.2f}",
            "pnl_cumulative_usdc": f"{self.pnl_usdc:.6f}",
            "reason": "settle",
            "dry_run": self._csv_bool(self._dry_run_effective),
            "config_profile_slug": pslug,
            "config_fingerprint": pfp,
        }
        try:
            log.info(
                "SETTLE escribiendo CSV: market_id=%s resolved=%s pnl=%.4f",
                market_id,
                resolved,
                float(pnl),
            )
            await self._append_csv(row)
        finally:
            self._publish_trade_counters_to_state()
        return resolved, float(pnl)


async def _shutdown(engine: SixCycleEngine) -> None:
    log.info("Shutdown — guardando estado...")
    engine._stop_run.set()
    try:
        await engine.scorer.stop()
    except Exception:  # noqa: BLE001
        log.exception("scorer.stop durante shutdown")
    # No sys.exit aquí: al salir la tarea, engine.run() termina y asyncio.run() acaba con código 0.


async def main() -> None:
    if not logging.root.handlers:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO"),
            format="%(message)s",
        )
    engine = SixCycleEngine()
    loop = asyncio.get_running_loop()
    try:
        for s in (sig_module.SIGINT, sig_module.SIGTERM):
            loop.add_signal_handler(
                s,
                lambda: asyncio.create_task(_shutdown(engine)),
            )
    except (NotImplementedError, RuntimeError):
        pass
    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
