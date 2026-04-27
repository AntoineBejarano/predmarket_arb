#!/usr/bin/env python3
"""
Construye dataset 5m compacto con features exógenas unidas a spot.
No reentrena modelos; prepara tablas para evaluación.
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
EXO_DIR = RAW_DIR / "exogenous"
OUT_DIR = EXO_DIR / "compact_5m"

ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]

log = logging.getLogger("build_exogenous_features")


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [compact-build] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logging.Formatter.converter = time.gmtime


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


def _load_spot_5m(symbol: str, start: str) -> pd.DataFrame:
    p = RAW_DIR / f"{symbol}_1min.parquet"
    if not p.is_file():
        raise FileNotFoundError(f"No existe {p}")
    df = pd.read_parquet(p, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.set_index("timestamp").sort_index()
    df = df[df.index >= pd.Timestamp(start, tz="UTC")]
    out = (
        df.resample("5min")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .reset_index()
    )
    return out


def _load_exo(symbol: str, subpath: str) -> pd.DataFrame:
    p = EXO_DIR / "futures" / subpath / f"{symbol}.parquet"
    if not p.is_file():
        return pd.DataFrame()
    return pd.read_parquet(p)


def _load_poly(symbol: str) -> pd.DataFrame:
    p = EXO_DIR / "polymarket" / "prices_1m" / f"{symbol}.parquet"
    if not p.is_file():
        return pd.DataFrame()
    d = pd.read_parquet(p)
    if d.empty:
        return d
    d = d.set_index("timestamp").sort_index()
    d = d.resample("5min").agg({"poly_mid": "last"}).dropna().reset_index()
    return d


def _make_features(base: pd.DataFrame) -> pd.DataFrame:
    df = base.copy()
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
    df["hour"] = df["timestamp"].dt.hour
    df["dow"] = df["timestamp"].dt.dayofweek
    df["is_ny_open"] = ((df["hour"] >= 13) & (df["hour"] <= 16)).astype(int)
    return df


def _join_exogenous(df: pd.DataFrame, agg: pd.DataFrame, metrics: pd.DataFrame, funding: pd.DataFrame, poly: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for ex in (agg, metrics, funding, poly):
        if ex.empty:
            continue
        ex = ex.copy()
        ex["timestamp"] = pd.to_datetime(ex["timestamp"], utc=True, errors="coerce")
        ex = ex.dropna(subset=["timestamp"]).sort_values("timestamp")
        out = out.merge(ex, on="timestamp", how="left")

    # Funding es cada 8h: carry forward temporal.
    if "funding_rate" in out.columns:
        out["funding_rate"] = out["funding_rate"].ffill()
    if "funding_change" in out.columns:
        out["funding_change"] = out["funding_change"].fillna(0.0)

    # Polymarket puede faltar en tramos; feature robusta a missing.
    if "poly_mid" in out.columns:
        out["poly_mid"] = out["poly_mid"].ffill()
        out["poly_mid_change_1"] = out["poly_mid"].pct_change(1, fill_method=None)
        out["spot_vs_poly_gap"] = out["close"] - out["poly_mid"]
    else:
        out["poly_mid"] = np.nan
        out["poly_mid_change_1"] = np.nan
        out["spot_vs_poly_gap"] = np.nan

    if "sum_open_interest_value" in out.columns:
        out["oi_level"] = out["sum_open_interest_value"]
    elif "sum_open_interest" in out.columns:
        out["oi_level"] = out["sum_open_interest"]
    else:
        out["oi_level"] = np.nan
    if "oi_change_5m" not in out.columns:
        out["oi_change_5m"] = np.nan

    # target del pipeline actual: vela siguiente verde.
    out["target"] = (out["close"].shift(-1) >= out["open"].shift(-1)).astype(int)
    return out


@dataclass
class BuildCfg:
    assets: list[str]
    start: str
    verbose: bool


def parse_args() -> BuildCfg:
    p = argparse.ArgumentParser(description="Construye dataset exógeno compacto 5m.")
    p.add_argument("--assets", nargs="+", help="Ej: BTC ETH")
    p.add_argument("--start", default="2021-01-01")
    p.add_argument("--verbose", action="store_true")
    a = p.parse_args()
    return BuildCfg(
        assets=resolve_assets(a.assets),
        start=str(a.start),
        verbose=bool(a.verbose),
    )


def main() -> None:
    cfg = parse_args()
    setup_logging(cfg.verbose)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for symbol in cfg.assets:
        log.info("Construyendo compact_5m para %s", symbol)
        base = _load_spot_5m(symbol, cfg.start)
        base = _make_features(base)

        agg = _load_exo(symbol, "agg_5m")
        metrics = _load_exo(symbol, "metrics_5m")
        funding = _load_exo(symbol, "funding")
        poly = _load_poly(symbol)

        merged = _join_exogenous(base, agg, metrics, funding, poly)
        merged = merged.sort_values("timestamp").reset_index(drop=True)

        out_path = OUT_DIR / f"{symbol}.parquet"
        merged.to_parquet(out_path, index=False)
        size_mb = out_path.stat().st_size / (1024 * 1024)
        cov_poly = float(merged["poly_mid"].notna().mean()) if "poly_mid" in merged.columns else 0.0
        cov_oi = float(merged["oi_level"].notna().mean()) if "oi_level" in merged.columns else 0.0
        cov_funding = float(merged["funding_rate"].notna().mean()) if "funding_rate" in merged.columns else 0.0
        log.info(
            "%s listo: filas=%s size=%.1fMB cov_poly=%.1f%% cov_oi=%.1f%% cov_funding=%.1f%%",
            symbol,
            len(merged),
            size_mb,
            100 * cov_poly,
            100 * cov_oi,
            100 * cov_funding,
        )


if __name__ == "__main__":
    main()
