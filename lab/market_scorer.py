"""Scoring de confluencia en tiempo real: velas Binance 1m (WS) + libro Polymarket CLOB (WS)."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from typing import Any

import numpy as np
import pandas as pd
import websockets

from lab.paths import REPO_ROOT

log = logging.getLogger("market_scorer")

BUFFER_PATH = REPO_ROOT / "data" / "scorer_state" / "candles_buffer.parquet"
_CANDLE_BUFFER_COLS = ("open_time", "open", "high", "low", "close", "volume")

BINANCE_KLINE_WS = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_AGENT = "predmarket-arb/market-scorer (+https://github.com)"

# Suma de cotas superiores de contribución absoluta en una dirección (sin contar spread negativo).
_MAX_ABS_SCORE = (
    2   # RSI
    + 3  # EMA cruce + tendencia
    + 1  # VWAP
    + 2  # Bollinger
    + 1  # momentum 3 velas
    + 2  # hammer / shooting star
    + 3  # engulfing
    + 1  # número redondo
    + 3  # OFI (2+1)
    + 1  # spread (penalización)
    + 7  # regla 12: gap PTB + velas (máx 4+3)
    + 3  # regla 13: mercado anterior
)


def _utc_midnight_ms(ts_ms: int) -> int:
    """Medianoche UTC del día que contiene ts_ms (ms)."""
    t = pd.Timestamp(ts_ms, unit="ms", tz="UTC")
    day_start = t.normalize()
    return int(day_start.value // 10**6)


def _rsi_wilder(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def _nearest_round_1000(price: float) -> float:
    return round(price / 1000.0) * 1000.0


class MarketScorer:
    """
    Escucha Binance kline 1m y (opcional) Polymarket CLOB market WS;
    expone ``get_signal`` con score de confluencia continuo.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._candles: deque[dict[str, Any]] = deque(maxlen=100)
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._clob_token_target: str | None = None
        self._ofi_buffer: deque[float] = deque(maxlen=10)
        self._binance_connected = False
        self._clob_connected = False
        self._stop = threading.Event()
        self._binance_task: asyncio.Task[Any] | None = None
        self._clob_task: asyncio.Task[Any] | None = None

    def get_signal(
        self,
        price_to_beat: float | None = None,
        prev_market_up_pct: float | None = None,
        min_abs_score: int = 5,
    ) -> dict[str, Any]:
        """
        Retorna:
        - score: int — score total de confluencia
        - direction: "UP" | "DOWN" | None
        - confidence: float — abs(score) / max_possible_score (0-1)
        - components: dict — desglose por indicador
        - clob_yes_price: float | None — último mid del CLOB
        - liquidity_usdc: float | None — liquidez top 5 niveles (bids+asks)
        - spot_price: float | None
        - candles_below_ptb: int — últimas 5 velas cerradas con close < PTB (0 si N/A)
        - ready: bool — True si buffer >= 20 velas y WS conectados
        """
        with self._lock:
            candles = list(self._candles)
            bids = dict(self._bids)
            asks = dict(self._asks)
            token_tgt = self._clob_token_target
            bn_ok = self._binance_connected
            cl_ok = self._clob_connected
            n_candles = len(candles)
            ready = n_candles >= 20 and bn_ok and (token_tgt is None or cl_ok)

        spot: float | None = None
        clob_mid: float | None = None
        liquidity: float | None = None
        score = 0
        components: dict[str, float] = {}

        if candles:
            spot = float(candles[-1]["close"])

        use_clob_scores = token_tgt is None or cl_ok

        # --- CLOB: mid, spread, OFI, liquidez (solo si no exigimos libro o ya conectó) ---
        if use_clob_scores and bids and asks:
            bid_prices = sorted(bids.keys(), reverse=True)[:5]
            ask_prices = sorted(asks.keys())[:5]
            bid_vol = sum(float(bids[p]) * float(p) for p in bid_prices)
            ask_vol = sum(float(asks[p]) * float(p) for p in ask_prices)
            denom = bid_vol + ask_vol
            if denom > 0:
                imbalance = (bid_vol - ask_vol) / denom
                ofi = 0.0
                if imbalance > 0.2:
                    ofi += 2.0
                if imbalance < -0.2:
                    ofi -= 2.0
                if imbalance > 0.4:
                    ofi += 1.0
                if imbalance < -0.4:
                    ofi -= 1.0
                self._ofi_buffer.append(float(ofi))
                ofi_smooth = (
                    float(sum(self._ofi_buffer)) / float(len(self._ofi_buffer))
                    if self._ofi_buffer
                    else float(ofi)
                )
                if ofi_smooth != 0.0:
                    score += int(round(ofi_smooth))
                    components["ofi"] = float(ofi_smooth)

            bb = max(bids.keys()) if bids else None
            ba = min(asks.keys()) if asks else None
            if bb is not None and ba is not None:
                spread = float(ba) - float(bb)
                clob_mid = (float(bb) + float(ba)) / 2.0
                sp_comp = 0.0
                if spread <= 0.01:
                    sp_comp = 0.0
                elif spread > 0.03:
                    sp_comp = -1.0
                    score -= 1
                components["spread"] = sp_comp

            liq_b = sum(float(bids[p]) * float(p) for p in bid_prices)
            liq_a = sum(float(asks[p]) * float(p) for p in ask_prices)
            liquidity = float(liq_b + liq_a)

        if n_candles < 20:
            score, components = self._apply_rules_12_13(
                score,
                components,
                price_to_beat,
                spot,
                candles,
                prev_market_up_pct,
            )
            cb_ptb = int(float(components.get("ptb_below", 0)))
            conf = min(1.0, abs(score) / _MAX_ABS_SCORE) if _MAX_ABS_SCORE else 0.0
            return {
                "score": int(score),
                "direction": None,
                "confidence": float(conf),
                "components": components,
                "clob_yes_price": clob_mid,
                "liquidity_usdc": liquidity,
                "spot_price": spot,
                "candles_below_ptb": cb_ptb,
                "ready": bool(ready),
            }

        df = pd.DataFrame(candles)
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)

        # 1 RSI 14
        rsi_s = _rsi_wilder(df["close"], 14)
        rsi_last = float(rsi_s.iloc[-1]) if not pd.isna(rsi_s.iloc[-1]) else 50.0
        rsi_pts = 0.0
        if rsi_last < 30.0:
            rsi_pts = 2.0
        elif rsi_last > 70.0:
            rsi_pts = -2.0
        elif 45.0 < rsi_last < 55.0:
            rsi_pts = 0.0
        if rsi_pts != 0.0:
            score += int(rsi_pts)
            components["rsi"] = rsi_pts

        # 2 EMA9 vs EMA21
        ema9 = df["close"].ewm(span=9, adjust=False).mean()
        ema21 = df["close"].ewm(span=21, adjust=False).mean()
        d_last = float(ema9.iloc[-1] - ema21.iloc[-1])
        d_prev = float(ema9.iloc[-2] - ema21.iloc[-2]) if len(df) >= 2 else 0.0
        ema_pts = 0.0
        if d_last > 0 and d_prev < 0:
            ema_pts += 2.0
        if d_last < 0 and d_prev > 0:
            ema_pts -= 2.0
        if d_last > 0:
            ema_pts += 1.0
        elif d_last < 0:
            ema_pts -= 1.0
        if ema_pts != 0.0:
            score += int(ema_pts)
            components["ema"] = ema_pts

        # 3 VWAP día UTC
        last_ot = int(df["open_time"].iloc[-1])
        day0 = _utc_midnight_ms(last_ot)
        mask = df["open_time"].astype(np.int64) >= day0
        sub = df.loc[mask]
        tp = (sub["high"] + sub["low"] + sub["close"]) / 3.0
        vol = sub["volume"]
        if float(vol.sum()) > 0:
            vwap = float((tp * vol).sum() / vol.sum())
            c = float(df["close"].iloc[-1])
            vw = 0.0
            if c > vwap:
                vw = 1.0
            elif c < vwap:
                vw = -1.0
            if vw != 0.0:
                score += int(vw)
                components["vwap"] = vw

        # 4 Bollinger 20, 2 std
        if len(df) >= 20:
            mid = df["close"].rolling(20).mean()
            std = df["close"].rolling(20).std()
            lower = mid - 2 * std
            upper = mid + 2 * std
            lo = float(lower.iloc[-1])
            hi = float(upper.iloc[-1])
            lo_p = float(lower.iloc[-2]) if len(df) >= 2 else lo
            hi_p = float(upper.iloc[-2]) if len(df) >= 2 else hi
            low_c = float(df["low"].iloc[-1])
            high_c = float(df["high"].iloc[-1])
            low_p = float(df["low"].iloc[-2])
            high_p = float(df["high"].iloc[-2])
            touch_low_now = low_c <= lo
            touch_low_prev = low_p <= lo_p
            touch_hi_now = high_c >= hi
            touch_hi_prev = high_p >= hi_p
            bb_pts = 0.0
            if touch_low_now and touch_low_prev:
                bb_pts += 2.0
            if touch_hi_now and touch_hi_prev:
                bb_pts -= 2.0
            if bb_pts != 0.0:
                score += int(bb_pts)
                components["bollinger"] = bb_pts

        # 5 Momentum últimas 3
        if len(df) >= 3:
            tail = df.iloc[-3:]
            all_up = bool(
                (tail["close"] > tail["open"]).all()
            )
            all_dn = bool(
                (tail["close"] < tail["open"]).all()
            )
            mo = 0.0
            if all_up:
                mo = 1.0
            elif all_dn:
                mo = -1.0
            if mo != 0.0:
                score += int(mo)
                components["momentum"] = mo

        # 6 Hammer / Shooting star (última vela)
        o = float(df["open"].iloc[-1])
        h = float(df["high"].iloc[-1])
        l = float(df["low"].iloc[-1])
        c = float(df["close"].iloc[-1])
        body = abs(c - o)
        rng = h - l
        if rng > 0 and body > 1e-12:
            lower_wick = min(o, c) - l
            upper_wick = h - max(o, c)
            third_top = h - rng / 3.0
            third_bot = l + rng / 3.0
            pat = 0.0
            if lower_wick > 2 * body and c >= third_top:
                pat = 2.0
            elif upper_wick > 2 * body and c <= third_bot:
                pat = -2.0
            if pat != 0.0:
                score += int(pat)
                components["hammer_star"] = pat

        # 7 Engulfing (últimas 2)
        if len(df) >= 2:
            o1, c1 = float(df["open"].iloc[-2]), float(df["close"].iloc[-2])
            o2, c2 = float(df["open"].iloc[-1]), float(df["close"].iloc[-1])
            bull1 = c1 < o1
            bear1 = c1 > o1
            bull2 = c2 > o2
            bear2 = c2 < o2
            eng = 0.0
            if bull2 and bear1 and c2 >= o1 and o2 <= c1:
                eng = 3.0
            elif bear2 and bull1 and o2 >= c1 and c2 <= o1:
                eng = -3.0
            if eng != 0.0:
                score += int(eng)
                components["engulfing"] = eng

        # 8 Número redondo (1000)
        if spot is not None and spot > 0:
            nr = _nearest_round_1000(spot)
            if nr > 0 and abs(spot - nr) / nr < 0.001:
                rnd = 0.0
                if spot < nr:
                    rnd = -1.0
                elif spot > nr:
                    rnd = 1.0
                if rnd != 0.0:
                    score += int(rnd)
                    components["round_1000"] = rnd

        # 9–10 ya sumados arriba desde bids/asks snapshot

        score, components = self._apply_rules_12_13(
            score,
            components,
            price_to_beat,
            spot,
            candles,
            prev_market_up_pct,
        )
        candles_below_ptb = int(float(components.get("ptb_below", 0)))

        conf = min(1.0, abs(score) / float(_MAX_ABS_SCORE)) if _MAX_ABS_SCORE else 0.0
        direction: str | None
        if abs(score) < min_abs_score or not ready:
            direction = None
        elif score > 0:
            direction = "UP"
        else:
            direction = "DOWN"

        return {
            "score": int(score),
            "direction": direction,
            "confidence": float(conf),
            "components": components,
            "clob_yes_price": clob_mid,
            "liquidity_usdc": liquidity,
            "spot_price": spot,
            "candles_below_ptb": int(candles_below_ptb),
            "ready": bool(ready),
        }

    def _apply_rules_12_13(
        self,
        score: int,
        components: dict[str, float],
        price_to_beat: float | None,
        spot: float | None,
        candles: list[dict[str, Any]],
        prev_market_up_pct: float | None,
    ) -> tuple[int, dict[str, float]]:
        """Reglas 12 (gap + velas vs PTB) y 13 (mercado anterior)."""
        ptb_ok = price_to_beat is not None and float(price_to_beat) > 0 and spot is not None
        if ptb_ok:
            gap = float(spot) - float(price_to_beat)  # type: ignore[arg-type]
            if gap > 100:
                score += 4
            elif gap > 50:
                score += 3
            elif gap > 20:
                score += 1
            elif gap < -100:
                score -= 4
            elif gap < -50:
                score -= 3
            elif gap < -20:
                score -= 1
            components["ptb_gap"] = float(gap)

            below = 0
            if len(candles) >= 5:
                last5_closes = [float(c["close"]) for c in candles[-5:]]
                below = sum(1 for c in last5_closes if c < float(price_to_beat))
                above = 5 - below
                if below >= 4:
                    score -= 3
                elif below >= 3:
                    score -= 1
                if above >= 4:
                    score += 3
                elif above >= 3:
                    score += 1
            components["ptb_below"] = float(below)

        if prev_market_up_pct is not None:
            p = float(prev_market_up_pct)
            if p < 0.20:
                score -= 3
            elif p < 0.35:
                score -= 1
            elif p > 0.80:
                score += 3
            elif p > 0.65:
                score += 1
            components["prev_market"] = float(p)

        return score, components

    def _load_buffer(self) -> None:
        if not BUFFER_PATH.exists():
            return
        with self._lock:
            if len(self._candles) > 0:
                return
        try:
            df = pd.read_parquet(BUFFER_PATH)
            missing = [c for c in _CANDLE_BUFFER_COLS if c not in df.columns]
            if missing:
                log.warning("Buffer parquet incompleto (faltan %s)", missing)
                return
            df = df[list(_CANDLE_BUFFER_COLS)].sort_values("open_time")
            records = df.to_dict("records")
            with self._lock:
                if len(self._candles) > 0:
                    return
                for row in records:
                    self._candles.append(
                        {c: row[c] for c in _CANDLE_BUFFER_COLS}
                    )
                nbuf = len(self._candles)
                first_ot = self._candles[0]["open_time"] if self._candles else "—"
                last_ot = self._candles[-1]["open_time"] if self._candles else "—"
            log.info(
                "Buffer restaurado: %s velas (desde %s hasta %s)",
                nbuf,
                first_ot,
                last_ot,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("No se pudo restaurar buffer: %s", e)

    def _save_buffer(self) -> None:
        with self._lock:
            if not self._candles:
                return
            snap = [{c: row[c] for c in _CANDLE_BUFFER_COLS} for row in self._candles]
            n = len(snap)
        try:
            BUFFER_PATH.parent.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame(snap)
            df.to_parquet(BUFFER_PATH, index=False)
            log.info("Buffer guardado: %s velas", n)
        except Exception as e:  # noqa: BLE001
            log.warning("Error guardando buffer: %s", e)

    def _ingest_binance_closed(self, k: dict[str, Any]) -> None:
        if not k.get("x"):
            return
        row = {
            "open_time": int(k["t"]),
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
        }
        with self._lock:
            self._candles.append(row)
            n = len(self._candles)
        if n > 0 and n % 5 == 0:
            self._save_buffer()

    def _ingest_clob_message(self, data: dict[str, Any]) -> None:
        et_raw = data.get("event_type") or data.get("type")
        et = str(et_raw).lower() if et_raw is not None else ""

        if et == "price_change":
            for ch in data.get("price_changes") or []:
                if not isinstance(ch, dict):
                    continue
                bb = ch.get("best_bid")
                ba = ch.get("best_ask")
                if bb is not None and ba is not None:
                    with self._lock:
                        try:
                            p_b = float(bb)
                            p_a = float(ba)
                            sb = ch.get("best_bid_size") or ch.get("bid_size")
                            sa = ch.get("best_ask_size") or ch.get("ask_size")
                            if sb is not None:
                                self._bids[p_b] = float(sb)
                            if sa is not None:
                                self._asks[p_a] = float(sa)
                        except (TypeError, ValueError):
                            pass
            return

        bids_raw = data.get("bids")
        asks_raw = data.get("asks")
        bk = data.get("book")
        if isinstance(bk, dict):
            if not bids_raw:
                bids_raw = bk.get("bids")
            if not asks_raw:
                asks_raw = bk.get("asks")
        if not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
            return

        def merge_levels(levels: list[Any], side: str) -> dict[float, float]:
            out: dict[float, float] = {}
            for lvl in levels:
                p_raw = None
                s_raw = None
                if isinstance(lvl, dict):
                    p_raw = lvl.get("price")
                    s_raw = lvl.get("size")
                elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    p_raw, s_raw = lvl[0], lvl[1]
                try:
                    p = float(p_raw)
                    s = float(s_raw)
                except (TypeError, ValueError):
                    continue
                if s <= 0:
                    out.pop(p, None)
                else:
                    out[p] = s
            return out

        with self._lock:
            self._bids = merge_levels(bids_raw, "bid")
            self._asks = merge_levels(asks_raw, "ask")

    async def start(self, token_id: str | None = None) -> None:
        """Lanza tareas WS; ``token_id`` None desactiva solo el CLOB."""
        self._stop.clear()
        tid = str(token_id).strip() if token_id else None

        with self._lock:
            buf_empty = len(self._candles) == 0
        if buf_empty:
            self._load_buffer()

        if self._clob_task and not self._clob_task.done():
            self._clob_task.cancel()
            try:
                await self._clob_task
            except asyncio.CancelledError:
                pass
            self._clob_task = None

        with self._lock:
            self._clob_token_target = tid
            if tid is None:
                self._bids.clear()
                self._asks.clear()
                self._clob_connected = False

        if self._binance_task is None or self._binance_task.done():
            self._binance_task = asyncio.create_task(self._binance_runner(), name="ms_binance")

        if tid:
            self._clob_task = asyncio.create_task(self._clob_runner(), name="ms_clob")

    async def stop(self) -> None:
        self._save_buffer()
        self._stop.set()
        for t in (self._binance_task, self._clob_task):
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        self._binance_task = None
        self._clob_task = None
        with self._lock:
            self._binance_connected = False
            self._clob_connected = False

    async def _binance_runner(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    BINANCE_KLINE_WS,
                    ping_interval=20,
                    ping_timeout=20,
                    additional_headers={"User-Agent": USER_AGENT},
                ) as ws:
                    with self._lock:
                        self._binance_connected = True
                    backoff = 1.0
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            if isinstance(raw, bytes):
                                raw = raw.decode("utf-8")
                            msg = json.loads(raw)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            continue
                        if not isinstance(msg, dict):
                            continue
                        k = msg.get("k")
                        if isinstance(k, dict):
                            self._ingest_binance_closed(k)
            except asyncio.CancelledError:
                with self._lock:
                    self._binance_connected = False
                raise
            except Exception as e:
                log.warning("Binance WS: %s — reconexión en %.1fs", e, backoff)
                with self._lock:
                    self._binance_connected = False
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
        with self._lock:
            self._binance_connected = False

    async def _clob_runner(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            with self._lock:
                token = self._clob_token_target
            if not token:
                await asyncio.sleep(0.15)
                continue
            try:
                sub = json.dumps(
                    {
                        "assets_ids": [token],
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                )
                async with websockets.connect(
                    CLOB_WS,
                    ping_interval=20,
                    ping_timeout=20,
                    additional_headers={"User-Agent": USER_AGENT},
                ) as ws:
                    await ws.send(sub)
                    with self._lock:
                        self._clob_connected = True
                    backoff = 1.0
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        with self._lock:
                            want = self._clob_token_target
                        if want != token:
                            break
                        try:
                            if isinstance(raw, bytes):
                                raw = raw.decode("utf-8")
                            data = json.loads(raw)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            continue
                        if isinstance(data, dict):
                            self._ingest_clob_message(data)
            except asyncio.CancelledError:
                with self._lock:
                    self._bids.clear()
                    self._asks.clear()
                    self._clob_connected = False
                raise
            except Exception as e:
                log.warning("CLOB WS: %s — reconexión en %.1fs", e, backoff)
                with self._lock:
                    self._bids.clear()
                    self._asks.clear()
                    self._clob_connected = False
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
        with self._lock:
            self._clob_connected = False


async def _demo_main() -> None:
    from rich.console import Console
    from rich.table import Table

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    console = Console()
    scorer = MarketScorer()
    await scorer.start(token_id=None)
    t_end = time.monotonic() + 60.0
    while time.monotonic() < t_end:
        sig = scorer.get_signal(price_to_beat=None, min_abs_score=5)
        table = Table(title="MarketScorer (Binance only)")
        table.add_column("Campo", style="cyan")
        table.add_column("Valor", style="white")
        for k, v in sig.items():
            if k == "components":
                table.add_row(k, repr(v))
            else:
                table.add_row(k, str(v))
        console.print(table)
        await asyncio.sleep(5.0)
    await scorer.stop()


if __name__ == "__main__":
    asyncio.run(_demo_main())
