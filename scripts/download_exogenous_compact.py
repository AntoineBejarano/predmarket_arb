#!/usr/bin/env python3
"""
Descarga compacta de datos exógenos para PredMarket Arb.

Objetivo:
- Evitar guardar ticks crudos masivos.
- Agregar en 5m lo antes posible.
- Persistir solo columnas útiles para features.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "raw" / "exogenous"
FUTURES_DIR = DATA_DIR / "futures"
POLY_DIR = DATA_DIR / "polymarket"

ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
ASSET_SHORT = {a: a.replace("USDT", "") for a in ASSETS}
SLUG_PREFIX = {"BTCUSDT": "btc", "ETHUSDT": "eth", "SOLUSDT": "sol", "XRPUSDT": "xrp", "BNBUSDT": "bnb"}

BINANCE_DATA = "https://data.binance.vision/data/futures/um"
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

log = logging.getLogger("download_exogenous_compact")


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [compact] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logging.Formatter.converter = time.gmtime


def iter_days(start: date, end: date) -> Iterator[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def iter_month_starts(start: date, end: date) -> Iterator[date]:
    cur = date(start.year, start.month, 1)
    while cur <= end:
        yield cur
        y = cur.year + (1 if cur.month == 12 else 0)
        m = 1 if cur.month == 12 else cur.month + 1
        cur = date(y, m, 1)


def resolve_assets(raw_assets: list[str] | None) -> list[str]:
    if not raw_assets:
        return list(ASSETS)
    out: list[str] = []
    for a in raw_assets:
        x = a.strip().upper()
        if not x.endswith("USDT"):
            x = f"{x}USDT"
        if x not in ASSETS:
            raise SystemExit(f"Activo no soportado: {a} -> {x}")
        out.append(x)
    return list(dict.fromkeys(out))


def get_bytes(session: requests.Session, url: str, timeout: int = 45) -> bytes | None:
    r = session.get(url, timeout=timeout)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.content


def read_csv_from_zip_bytes(payload: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(payload), "r") as zf:
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            raise ValueError("zip sin CSV")
        with zf.open(names[0]) as f:
            return pd.read_csv(f)


def _agg_aggtrades_5m(df: pd.DataFrame) -> pd.DataFrame:
    # Columns spec de aggTrades: a,p,q,f,l,T,m,M (sin header)
    if "T" in df.columns:
        ren = {"T": "ts_ms", "p": "price", "q": "qty", "m": "is_buyer_maker"}
        df = df.rename(columns=ren)
    elif df.shape[1] >= 8:
        df = df.iloc[:, :8].copy()
        df.columns = ["agg_id", "price", "qty", "first_id", "last_id", "ts_ms", "is_buyer_maker", "ignore"]
    elif df.shape[1] == 7:
        # Algunos dumps vienen sin última columna M/ignore.
        df = df.iloc[:, :7].copy()
        df.columns = ["agg_id", "price", "qty", "first_id", "last_id", "ts_ms", "is_buyer_maker"]
    else:
        raise ValueError(f"aggTrades schema inesperado: {df.columns.tolist()}")
    needed = {"ts_ms", "price", "qty", "is_buyer_maker"}
    if not needed.issubset(set(df.columns)):
        raise ValueError(f"aggTrades schema inesperado: {df.columns.tolist()}")

    ts_num = pd.to_numeric(df["ts_ms"], errors="coerce")
    d = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(ts_num, unit="ms", utc=True, errors="coerce"),
            "price": pd.to_numeric(df["price"], errors="coerce"),
            "qty": pd.to_numeric(df["qty"], errors="coerce"),
            "is_buyer_maker": df["is_buyer_maker"].astype(str).str.lower().isin(("true", "1")),
        }
    ).dropna()
    d["notional"] = d["price"] * d["qty"]
    d["taker_buy_qty"] = np.where(~d["is_buyer_maker"], d["qty"], 0.0)
    d["taker_sell_qty"] = np.where(d["is_buyer_maker"], d["qty"], 0.0)
    d["taker_buy_notional"] = np.where(~d["is_buyer_maker"], d["notional"], 0.0)
    d["taker_sell_notional"] = np.where(d["is_buyer_maker"], d["notional"], 0.0)
    d["n_trades"] = 1

    out = (
        d.set_index("timestamp")
        .resample("5min")
        .agg(
            {
                "taker_buy_qty": "sum",
                "taker_sell_qty": "sum",
                "taker_buy_notional": "sum",
                "taker_sell_notional": "sum",
                "n_trades": "sum",
            }
        )
        .dropna()
        .reset_index()
    )
    vol = out["taker_buy_qty"] + out["taker_sell_qty"]
    out["taker_imbalance_5m"] = np.where(vol > 0, (out["taker_buy_qty"] - out["taker_sell_qty"]) / vol, 0.0)
    out["buy_sell_ratio_5m"] = np.where(out["taker_sell_qty"] > 0, out["taker_buy_qty"] / out["taker_sell_qty"], np.nan)
    return out


def _download_aggtrades_5m(session: requests.Session, symbol: str, start: date, end: date) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    total = (end - start).days + 1
    miss = 0
    for i, d in enumerate(iter_days(start, end), start=1):
        url = f"{BINANCE_DATA}/daily/aggTrades/{symbol}/{symbol}-aggTrades-{d.isoformat()}.zip"
        payload = get_bytes(session, url)
        if payload is None:
            miss += 1
            continue
        with zipfile.ZipFile(io.BytesIO(payload), "r") as zf:
            names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not names:
                continue
            with zf.open(names[0]) as f:
                raw = pd.read_csv(f, header=None, low_memory=False)
        parts.append(_agg_aggtrades_5m(raw))
        if i % 30 == 0:
            log.info("%s aggTrades: %s/%s días procesados", symbol, i, total)
    if not parts:
        log.warning("%s aggTrades: sin datos en rango", symbol)
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    log.info("%s aggTrades: filas_5m=%s días_missing=%s", symbol, len(out), miss)
    return out


def _download_metrics_oi_5m(session: requests.Session, symbol: str, start: date, end: date) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for d in iter_days(start, end):
        url = f"{BINANCE_DATA}/daily/metrics/{symbol}/{symbol}-metrics-{d.isoformat()}.zip"
        payload = get_bytes(session, url)
        if payload is None:
            continue
        raw = read_csv_from_zip_bytes(payload)
        if raw.empty:
            continue
        cols = {
            "create_time": "timestamp",
            "sum_open_interest": "sum_open_interest",
            "sum_open_interest_value": "sum_open_interest_value",
            "sum_taker_long_short_vol_ratio": "sum_taker_long_short_vol_ratio",
        }
        keep = [c for c in cols if c in raw.columns]
        if "create_time" not in keep:
            continue
        dfi = raw[keep].rename(columns=cols).copy()
        dfi["timestamp"] = pd.to_datetime(dfi["timestamp"], utc=True, errors="coerce")
        for c in ("sum_open_interest", "sum_open_interest_value", "sum_taker_long_short_vol_ratio"):
            if c in dfi.columns:
                dfi[c] = pd.to_numeric(dfi[c], errors="coerce")
        dfi = dfi.dropna(subset=["timestamp"]).sort_values("timestamp")
        parts.append(dfi)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    if "sum_open_interest_value" in out.columns:
        out["oi_change_5m"] = out["sum_open_interest_value"].pct_change(1)
    elif "sum_open_interest" in out.columns:
        out["oi_change_5m"] = out["sum_open_interest"].pct_change(1)
    return out


def _download_funding_monthly(session: requests.Session, symbol: str, start: date, end: date) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for m in iter_month_starts(start, end):
        url = f"{BINANCE_DATA}/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{m.year}-{m.month:02d}.zip"
        payload = get_bytes(session, url)
        if payload is None:
            continue
        raw = read_csv_from_zip_bytes(payload)
        if raw.empty:
            continue
        if "calc_time" not in raw.columns:
            continue
        dfi = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(raw["calc_time"], unit="ms", utc=True, errors="coerce"),
                "funding_rate": pd.to_numeric(raw.get("last_funding_rate"), errors="coerce"),
            }
        ).dropna(subset=["timestamp"])
        dfi = dfi.sort_values("timestamp")
        parts.append(dfi)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    out["funding_change"] = out["funding_rate"].diff()
    return out


def _merge_persist(symbol: str, kind: str, df_new: pd.DataFrame) -> Path | None:
    if df_new.empty:
        return None
    out_dir = FUTURES_DIR / kind
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{symbol}.parquet"
    if out_path.is_file():
        old = pd.read_parquet(out_path)
        merged = pd.concat([old, df_new], ignore_index=True)
    else:
        merged = df_new
    merged = merged.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    merged.to_parquet(out_path, index=False)
    size_mb = out_path.stat().st_size / (1024 * 1024)
    log.info("Guardado %s (%s filas, %.1f MB)", out_path.relative_to(REPO_ROOT), len(merged), size_mb)
    return out_path


def run_futures(session: requests.Session, symbols: list[str], start: date, end: date) -> None:
    for symbol in symbols:
        log.info("=== Futures compacto %s (%s → %s) ===", symbol, start, end)
        agg = _download_aggtrades_5m(session, symbol, start, end)
        _merge_persist(symbol, "agg_5m", agg)

        metrics = _download_metrics_oi_5m(session, symbol, start, end)
        _merge_persist(symbol, "metrics_5m", metrics)

        funding = _download_funding_monthly(session, symbol, start, end)
        _merge_persist(symbol, "funding", funding)


def _slug_for(asset_symbol: str, ts_5m: int) -> str:
    return f"{SLUG_PREFIX[asset_symbol]}-updown-5m-{ts_5m}"


def _fetch_market_by_slug(session: requests.Session, slug: str) -> dict | None:
    r = session.get(f"{GAMMA_API}/markets/slug/{slug}", timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else None


def _fetch_prices_history(session: requests.Session, token_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    r = session.get(
        f"{CLOB_API}/prices-history",
        params={"market": token_id, "startTs": start_ts, "endTs": end_ts, "interval": "1m", "fidelity": 10},
        timeout=30,
    )
    if r.status_code == 404:
        return pd.DataFrame()
    r.raise_for_status()
    j = r.json()
    history = j.get("history", []) if isinstance(j, dict) else []
    if not history:
        return pd.DataFrame()
    out = pd.DataFrame(history)
    if "t" not in out.columns or "p" not in out.columns:
        return pd.DataFrame()
    out = out.rename(columns={"t": "ts", "p": "poly_mid"})
    out["timestamp"] = pd.to_datetime(out["ts"], unit="s", utc=True)
    out["poly_mid"] = pd.to_numeric(out["poly_mid"], errors="coerce")
    out = out.dropna(subset=["timestamp", "poly_mid"])[["timestamp", "poly_mid"]]
    return out


def run_polymarket(session: requests.Session, symbols: list[str], start: date, end: date, max_markets_per_asset: int) -> None:
    POLY_DIR.mkdir(parents=True, exist_ok=True)
    meta_rows: list[dict] = []
    series_parts: dict[str, list[pd.DataFrame]] = {s: [] for s in symbols}

    start_ts = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    end_dt = datetime(end.year, end.month, end.day, tzinfo=timezone.utc) + timedelta(days=1)
    end_ts = int(end_dt.timestamp())

    for symbol in symbols:
        log.info("=== Polymarket compacto %s (%s → %s) ===", symbol, start, end)
        # Exploración por slugs cada 5m: es costoso, por eso permitimos límite.
        # Usamos esta vía porque cada mercado 5m tiene token distinto.
        cur = start_ts - (start_ts % 300)
        fetched = 0
        while cur < end_ts and fetched < max_markets_per_asset:
            slug = _slug_for(symbol, cur)
            m = _fetch_market_by_slug(session, slug)
            if m is not None:
                raw_ids = m.get("clobTokenIds")
                try:
                    token_ids = json.loads(raw_ids) if isinstance(raw_ids, str) else list(raw_ids or [])
                except Exception:
                    token_ids = []
                up_token = str(token_ids[0]) if token_ids else ""
                if up_token:
                    hist = _fetch_prices_history(session, up_token, cur, cur + 5 * 60)
                    if not hist.empty:
                        hist["asset"] = symbol
                        hist["window_ts"] = cur
                        series_parts[symbol].append(hist)
                meta_rows.append(
                    {
                        "asset": symbol,
                        "window_ts": cur,
                        "slug": slug,
                        "market_id": str(m.get("id", "")),
                        "condition_id": str(m.get("conditionId", "")),
                        "up_token_id": up_token,
                    }
                )
                fetched += 1
            cur += 300
        log.info("%s: mercados capturados=%s (límite=%s)", symbol, fetched, max_markets_per_asset)

    if meta_rows:
        meta = pd.DataFrame(meta_rows).drop_duplicates(subset=["asset", "window_ts"], keep="last")
        p_meta = POLY_DIR / "markets_index.parquet"
        if p_meta.is_file():
            old = pd.read_parquet(p_meta)
            meta = pd.concat([old, meta], ignore_index=True).drop_duplicates(subset=["asset", "window_ts"], keep="last")
        meta.to_parquet(p_meta, index=False)
        log.info("Guardado %s (%s filas)", p_meta.relative_to(REPO_ROOT), len(meta))

    for symbol, parts in series_parts.items():
        if not parts:
            continue
        s = pd.concat(parts, ignore_index=True)
        s = (
            s.sort_values("timestamp")
            .drop_duplicates(subset=["timestamp", "window_ts"], keep="last")
            .groupby("timestamp", as_index=False)
            .agg({"poly_mid": "mean"})
        )
        p = POLY_DIR / "prices_1m" / f"{symbol}.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.is_file():
            old = pd.read_parquet(p)
            s = pd.concat([old, s], ignore_index=True).drop_duplicates(subset=["timestamp"], keep="last")
        s = s.sort_values("timestamp").reset_index(drop=True)
        s.to_parquet(p, index=False)
        log.info("Guardado %s (%s filas)", p.relative_to(REPO_ROOT), len(s))


@dataclass
class RunConfig:
    assets: list[str]
    start: date
    end: date
    skip_futures: bool
    skip_polymarket: bool
    poly_max_markets: int
    verbose: bool


def parse_args() -> RunConfig:
    p = argparse.ArgumentParser(description="Descarga compacta de exógenas (futures + polymarket).")
    p.add_argument("--assets", nargs="+", help="Ej: BTC ETH SOL")
    p.add_argument("--start", default="2024-01-01", help="YYYY-MM-DD")
    p.add_argument("--end", default=datetime.now(timezone.utc).date().isoformat(), help="YYYY-MM-DD")
    p.add_argument("--skip-futures", action="store_true")
    p.add_argument("--skip-polymarket", action="store_true")
    p.add_argument("--poly-max-markets", type=int, default=2000, help="Límite por activo para no explotar requests.")
    p.add_argument("--verbose", action="store_true")
    a = p.parse_args()

    start = datetime.strptime(a.start, "%Y-%m-%d").date()
    end = datetime.strptime(a.end, "%Y-%m-%d").date()
    if end < start:
        raise SystemExit("end < start")
    return RunConfig(
        assets=resolve_assets(a.assets),
        start=start,
        end=end,
        skip_futures=bool(a.skip_futures),
        skip_polymarket=bool(a.skip_polymarket),
        poly_max_markets=max(1, int(a.poly_max_markets)),
        verbose=bool(a.verbose),
    )


def main() -> None:
    cfg = parse_args()
    setup_logging(cfg.verbose)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": "predmarket-arb-compact-exogenous/1.0"})

    log.info(
        "Inicio descarga compacta assets=%s rango=%s→%s skip_futures=%s skip_poly=%s",
        cfg.assets,
        cfg.start,
        cfg.end,
        cfg.skip_futures,
        cfg.skip_polymarket,
    )
    if not cfg.skip_futures:
        run_futures(session, cfg.assets, cfg.start, cfg.end)
    if not cfg.skip_polymarket:
        run_polymarket(session, cfg.assets, cfg.start, cfg.end, cfg.poly_max_markets)
    log.info("Descarga compacta finalizada.")


if __name__ == "__main__":
    main()
