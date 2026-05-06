"""Une mercados Polymarket resueltos con velas Binance 5m y genera parquet alineado por activo para entrenamiento."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
POLY_MARKETS = RAW_DIR / "poly_markets_resolved.parquet"
ALIGNED_DIR = RAW_DIR / "poly_aligned"

ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
ALLOWED_ASSETS = frozenset(ASSETS)
START_DATE = "2023-01-01"
RESAMPLE = "5min"
TOL = pd.Timedelta("5min")

# Columnas de features clásicas (mismo orden que models/train.FEATURES antes de las 3 poly).
FEATURES_CLASSIC = [
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

console = Console()


def _ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC")


def resample_and_features_5m(df: pd.DataFrame) -> pd.DataFrame:
    """Mantener alineado con models/train.load_and_featurize (resample + features, sin target ni poly)."""
    df5 = df.resample(RESAMPLE).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    ).dropna()

    df5["ret_1"] = df5["close"].pct_change(1)
    df5["ret_3"] = df5["close"].pct_change(3)
    df5["ret_6"] = df5["close"].pct_change(6)
    df5["ret_12"] = df5["close"].pct_change(12)
    df5["ret_24"] = df5["close"].pct_change(24)
    df5["ret_48"] = df5["close"].pct_change(48)

    df5["vol_10"] = df5["ret_1"].rolling(10).std()
    df5["vol_20"] = df5["ret_1"].rolling(20).std()
    df5["vol_ratio"] = df5["vol_10"] / df5["vol_20"]
    df5["atr_5"] = (df5["high"] - df5["low"]).rolling(5).mean()

    vm = df5["volume"].rolling(20).mean()
    vs = df5["volume"].rolling(20).std()
    df5["vol_zscore"] = (df5["volume"] - vm) / vs
    df5["vol_trend"] = df5["volume"] / df5["volume"].shift(3)

    df5["hour"] = df5.index.hour
    df5["dow"] = df5.index.dayofweek
    df5["is_ny_open"] = ((df5["hour"] >= 13) & (df5["hour"] <= 16)).astype(int)
    return df5


def _ts_epoch_ms(ts: pd.Timestamp) -> np.int64:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return np.int64(int(np.floor(t.timestamp() * 1000)))


def _index_epoch_ms_array(idx: pd.DatetimeIndex) -> np.ndarray:
    """Enteros epoch-ms alineados con ``_ts_epoch_ms`` (idx puede ser datetime64[ms|ns|us])."""
    raw = np.asarray(idx.astype(np.int64), dtype=np.int64)
    unit = getattr(idx.dtype, "unit", None) or "ns"
    if unit == "ns":
        return raw // 1_000_000
    if unit == "us":
        return raw // 1_000
    if unit == "ms":
        return raw
    if unit == "s":
        return raw * 1000
    return raw // 1_000_000


def nearest_bar_index(idx: pd.DatetimeIndex, ts: pd.Timestamp) -> int | None:
    if len(idx) == 0:
        return None
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    idx_ms = _index_epoch_ms_array(idx)
    ts_ms = int(_ts_epoch_ms(ts))
    i = int(np.searchsorted(idx_ms, ts_ms))
    candidates: list[int] = []
    if 0 <= i < len(idx):
        candidates.append(i)
    if i - 1 >= 0:
        candidates.append(i - 1)
    if i + 1 < len(idx):
        candidates.append(i + 1)
    best_j: int | None = None
    best_d = None
    for j in candidates:
        d = abs((idx[j] - ts).total_seconds())
        if d <= TOL.total_seconds():
            if best_d is None or d < best_d:
                best_d = d
                best_j = j
    return best_j


def build_for_asset(asset: str, markets: pd.DataFrame) -> pd.DataFrame:
    path_1m = RAW_DIR / f"{asset}_1min.parquet"
    if not path_1m.is_file():
        raise FileNotFoundError(f"No existe {path_1m}")

    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    df = pd.read_parquet(path_1m, columns=cols).set_index("timestamp").sort_index()
    df = df[df.index >= _ts(START_DATE)]
    df5 = resample_and_features_5m(df)
    idx = df5.index

    sub = markets[markets["asset"] == asset].copy()
    rows: list[dict] = []
    for _, m in sub.iterrows():
        open_ts = pd.Timestamp(m["open_ts"])
        if open_ts.tzinfo is None:
            open_ts = open_ts.tz_localize("UTC")
        else:
            open_ts = open_ts.tz_convert("UTC")
        close_ts = pd.Timestamp(m["close_ts"])
        if close_ts.tzinfo is None:
            close_ts = close_ts.tz_localize("UTC")
        else:
            close_ts = close_ts.tz_convert("UTC")

        io = nearest_bar_index(idx, open_ts)
        ic = nearest_bar_index(idx, close_ts)
        if io is None or ic is None:
            continue
        open_bar = idx[io]
        # siguiente vela a la barra de apertura (mismo criterio que target Binance en train)
        if io + 1 >= len(df5):
            continue
        next_i = io + 1
        o = float(df5.iloc[next_i]["open"])
        c = float(df5.iloc[next_i]["close"])
        target_binance = int(c >= o)

        feat_row = df5.loc[open_bar]
        resolved = bool(m["resolved_yes"])
        target_poly = int(resolved)
        ddir = str(m["direction"]).lower()
        if ddir == "both":
            direction_up = 0.5
        else:
            direction_up = 1 if ddir == "up" else 0
        yp = m.get("yes_price_at_close")
        if yp is None or pd.isna(yp):
            poly_yes = np.nan
        else:
            poly_yes = float(yp)

        rec: dict = {
            "market_id": str(m["market_id"]),
            # open_ts = inicio de vela 5m (índice df5) para join con models/train.py
            "open_ts": open_bar,
            "poly_market_open_ts": open_ts,
            "close_ts": close_ts,
            "target_poly": target_poly,
            "target_binance": target_binance,
            "poly_yes_price": poly_yes,
            "time_to_expiry_pct": 1.0,
            "direction_up": direction_up,
        }
        for f in FEATURES_CLASSIC:
            rec[f] = feat_row[f]
        rows.append(rec)

    return pd.DataFrame(rows)


def _parse_assets_arg(names: list[str] | None) -> frozenset[str] | None:
    if not names:
        return None
    out: set[str] = set()
    for raw in names:
        u = raw.strip().upper()
        if not u.endswith("USDT"):
            u = f"{u}USDT"
        if u not in ALLOWED_ASSETS:
            console.print(f"[red]Activo no soportado: {raw} (usa BTC, ETH, …)[/red]")
            sys.exit(1)
        out.add(u)
    return frozenset(out)


def _print_overlap_hint(asset: str, markets: pd.DataFrame, path_1m: Path) -> None:
    sub = markets[markets["asset"].astype(str) == asset]
    if sub.empty:
        return
    omin = pd.Timestamp(sub["open_ts"].min())
    omax = pd.Timestamp(sub["open_ts"].max())
    cols = ["timestamp"]
    tdf = pd.read_parquet(path_1m, columns=cols)
    tmin = pd.Timestamp(tdf["timestamp"].min())
    tmax = pd.Timestamp(tdf["timestamp"].max())
    console.print(
        f"[dim]Solape fechas: Poly open_ts [{omin} … {omax}] vs Binance 1m [{tmin} … {tmax}]. "
        "Hace falta que las velas cubran las ventanas Poly.[/dim]"
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Une poly_markets_resolved con velas 5m (desde 1m) por activo.",
    )
    p.add_argument(
        "--assets",
        nargs="+",
        metavar="SYM",
        default=None,
        help="Solo estos activos (ej. BTC). Por defecto: solo activos que aparezcan en el parquet Poly.",
    )
    args = p.parse_args()
    asset_cli = _parse_assets_arg(args.assets)

    if not POLY_MARKETS.is_file():
        console.print(f"[red]Falta {POLY_MARKETS}. Ejecuta scripts/download_poly_history.py[/red]")
        sys.exit(1)

    markets = pd.read_parquet(POLY_MARKETS)
    ALIGNED_DIR.mkdir(parents=True, exist_ok=True)

    poly_assets = set(markets["asset"].dropna().astype(str).str.strip().unique())
    candidates = sorted(poly_assets & ALLOWED_ASSETS)
    if asset_cli is not None:
        candidates = sorted(set(candidates) & set(asset_cli))
    if not candidates:
        console.print(
            "[red]Ningún activo a construir: el parquet Poly no tiene filas en ASSETS conocidos, "
            "o --assets no coincide.[/red]",
        )
        sys.exit(1)

    summary: list[tuple[str, int, float, float]] = []

    for asset in candidates:
        path_1m = RAW_DIR / f"{asset}_1min.parquet"
        if not path_1m.is_file():
            console.print(f"[yellow]Omitido {asset}: no existe {path_1m}[/yellow]")
            continue
        try:
            df_out = build_for_asset(asset, markets)
        except FileNotFoundError as e:
            console.print(f"[yellow]{e}[/yellow]")
            continue
        out_path = ALIGNED_DIR / f"{asset}.parquet"
        df_out.to_parquet(out_path, index=False, engine="pyarrow")
        n = len(df_out)
        pct_poly = 100.0 * float(df_out["target_poly"].mean()) if n else 0.0
        if n >= 2 and df_out["target_poly"].nunique() > 1 and df_out["target_binance"].nunique() > 1:
            corr = float(df_out["target_poly"].corr(df_out["target_binance"]))
        else:
            corr = float("nan")
        summary.append((asset, n, pct_poly, corr))
        console.print(f"[green]{asset}[/green] → {out_path.relative_to(REPO_ROOT)} ({n} filas)")
        if n == 0:
            _print_overlap_hint(asset, markets, path_1m)

    t = Table(title="Resumen build_poly_target")
    t.add_column("asset")
    t.add_column("filas", justify="right")
    t.add_column("% target_poly=1", justify="right")
    t.add_column("corr(poly,binance)", justify="right")
    for asset, n, pct, corr in summary:
        t.add_row(asset, str(n), f"{pct:.1f}%", f"{corr:.4f}" if np.isfinite(corr) else "nan")
    console.print(t)


if __name__ == "__main__":
    main()
