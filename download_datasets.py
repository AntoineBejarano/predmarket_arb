#!/usr/bin/env python3
"""Descarga OHLCV 1m desde Binance Data Vision (bulk ZIP), no API."""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import re
import sys
import threading
import time
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
ZIPS_DIR = DATA_DIR / "zips"
RAW_DIR = DATA_DIR / "raw"

BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

ASSETS: dict[str, dict[str, int]] = {
    "BTCUSDT": {"start_year": 2019, "start_month": 1},
    "ETHUSDT": {"start_year": 2019, "start_month": 1},
    "SOLUSDT": {"start_year": 2020, "start_month": 9},
    "XRPUSDT": {"start_year": 2019, "start_month": 1},
    "DOGEUSDT": {"start_year": 2019, "start_month": 7},
    "BNBUSDT": {"start_year": 2019, "start_month": 1},
    "HYPEUSDT": {"start_year": 2024, "start_month": 11},
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "predmarket-arb-download/1.0"})


def month_end_exclusive_utc(year: int, month: int) -> pd.Timestamp:
    start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
    return start + pd.DateOffset(months=1)


def iter_months(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> Iterator[tuple[int, int]]:
    cur = pd.Timestamp(year=start_year, month=start_month, day=1, tz="UTC")
    end = pd.Timestamp(year=end_year, month=end_month, day=1, tz="UTC")
    while cur <= end:
        yield int(cur.year), int(cur.month)
        cur = cur + pd.DateOffset(months=1)


def zip_url(symbol: str, year: int, month: int) -> str:
    return (
        f"{BASE_URL}/{symbol}/1m/"
        f"{symbol}-1m-{year}-{month:02d}.zip"
    )


def checksum_url(symbol: str, year: int, month: int) -> str:
    return zip_url(symbol, year, month).replace(".zip", ".CHECKSUM")


def short_label(symbol: str) -> str:
    return symbol.replace("USDT", "")


class ZipRequestRateLimiter:
    """Al menos 1 segundo entre inicios de descarga de ZIP (y su verificación)."""

    def __init__(self, min_interval_s: float = 1.0) -> None:
        self._min_interval_s = min_interval_s
        self._lock = threading.Lock()
        self._last: float | None = None

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if self._last is not None:
                wait = self._min_interval_s - (now - self._last)
                if wait > 0:
                    time.sleep(wait)
                    now = time.monotonic()
            self._last = now


def parse_checksum_file(content: str, expected_filename: str) -> str | None:
    for line in content.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) >= 2:
            digest, name = parts[0], parts[1]
            if name == expected_filename or name.endswith(expected_filename):
                return digest.lower()
        m = re.match(r"^([a-fA-F0-9]{64})\s+\*?(.+)$", line)
        if m:
            digest, name = m.group(1), m.group(2)
            if Path(name).name == expected_filename:
                return digest.lower()
    return None


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def download_to_file(url: str, dest: Path, timeout: int = 120) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = SESSION.get(url, stream=True, timeout=timeout)
    if r.status_code == 404:
        return False
    r.raise_for_status()
    with dest.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return True


def read_klines_from_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            raise ValueError(f"No CSV in {zip_path}")
        name = names[0]
        with zf.open(name) as f:
            raw = f.read()
    buf = io.BytesIO(raw)
    df = pd.read_csv(
        buf,
        header=None,
        names=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )
    return df


def klines_to_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(df["open_time"], unit="ms", utc=True),
            "open": df["open"].astype("float64"),
            "high": df["high"].astype("float64"),
            "low": df["low"].astype("float64"),
            "close": df["close"].astype("float64"),
            "volume": df["volume"].astype("float64"),
        }
    )
    return out


@dataclass
class CleanStats:
    gaps_over_5min: int


def clean_ohlcv(df: pd.DataFrame) -> tuple[pd.DataFrame, CleanStats]:
    if df.empty:
        return df, CleanStats(gaps_over_5min=0)

    d = df.drop_duplicates(subset=["timestamp"], keep="last")
    d = d.sort_values("timestamp").reset_index(drop=True)

    diffs = d["timestamp"].diff()
    gaps_over_5 = int((diffs > pd.Timedelta("5min")).sum())
    stats = CleanStats(gaps_over_5min=gaps_over_5)

    d = d.set_index("timestamp").sort_index()
    full_idx = pd.date_range(d.index.min(), d.index.max(), freq="1min", tz="UTC")
    d = d.reindex(full_idx)
    for c in ("open", "high", "low", "close"):
        d[c] = d[c].ffill(limit=5)
    d["volume"] = d["volume"].fillna(0.0)

    dead = (d["volume"] == 0) & (
        (d["open"] == d["high"])
        & (d["high"] == d["low"])
        & (d["low"] == d["close"])
    )
    d = d.loc[~dead]
    d = d.dropna(subset=["open", "high", "low", "close", "volume"])
    d = d.reset_index()
    d = d.rename(columns={d.columns[0]: "timestamp"})
    d = d.reset_index(drop=True)
    return d, stats


def parquet_path(symbol: str) -> Path:
    return RAW_DIR / f"{symbol}_1min.parquet"


def load_last_timestamp(symbol: str) -> pd.Timestamp | None:
    path = parquet_path(symbol)
    if not path.is_file():
        return None
    s = pd.read_parquet(path, columns=["timestamp"])
    if s.empty:
        return None
    ts = s["timestamp"].max()
    if getattr(ts, "tzinfo", None) is None:
        ts = pd.Timestamp(ts).tz_localize("UTC")
    else:
        ts = pd.Timestamp(ts).tz_convert("UTC")
    return ts


def months_to_fetch(
    symbol: str,
    start_year: int,
    start_month: int,
    last_ts: pd.Timestamp | None,
    end_y: int,
    end_m: int,
) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for y, m in iter_months(start_year, start_month, end_y, end_m):
        if last_ts is not None:
            boundary = month_end_exclusive_utc(y, m)
            if last_ts >= boundary:
                continue
        out.append((y, m))
    return out


def process_one_month(
    symbol: str,
    year: int,
    month: int,
    limiter: ZipRequestRateLimiter,
) -> tuple[tuple[int, int], pd.DataFrame | None, Path | None, str | None]:
    """Descarga ZIP + checksum, verifica, extrae. Devuelve (ym, df|None, zip_path|None, error)."""
    zip_name = f"{symbol}-1m-{year}-{month:02d}.zip"
    dest = ZIPS_DIR / zip_name
    expected_checksum_name = zip_name

    try:
        ck_url = checksum_url(symbol, year, month)
        cr = SESSION.get(ck_url, timeout=60)
        expected_hash: str | None = None
        if cr.status_code == 404:
            warnings.warn(f"{symbol} {year}-{month:02d}: CHECKSUM 404, se descarga sin verificar.")
        else:
            cr.raise_for_status()
            expected_hash = parse_checksum_file(cr.text, expected_checksum_name)
            if not expected_hash:
                warnings.warn(
                    f"{symbol} {year}-{month:02d}: CHECKSUM ilegible, se descarga sin verificar."
                )

        limiter.wait()
        ok = download_to_file(zip_url(symbol, year, month), dest)
        if not ok:
            if dest.is_file():
                dest.unlink(missing_ok=True)
            return (year, month), None, None, "zip 404"

        if expected_hash is not None:
            actual = sha256_file(dest)
            if actual != expected_hash:
                warnings.warn(
                    f"{symbol} {year}-{month:02d}: checksum SHA256 no coincide, se omite."
                )
                dest.unlink(missing_ok=True)
                return (year, month), None, None, "checksum mismatch"

        raw = read_klines_from_zip(dest)
        ohlcv = klines_to_ohlcv(raw)
        return (year, month), ohlcv, dest, None
    except Exception as e:  # noqa: BLE001
        if dest.is_file():
            dest.unlink(missing_ok=True)
        return (year, month), None, None, str(e)


def merge_and_save(
    symbol: str,
    new_parts: list[pd.DataFrame],
    existing: pd.DataFrame | None,
) -> pd.DataFrame:
    parts = [p for p in new_parts if p is not None and not p.empty]
    if not parts:
        if existing is not None and not existing.empty:
            return existing
        raise ValueError(f"{symbol}: sin datos para guardar")
    if existing is not None and not existing.empty:
        parts.append(existing)
    merged = pd.concat(parts, ignore_index=True)
    merged = merged.sort_values("timestamp").reset_index(drop=True)
    merged = merged.drop_duplicates(subset=["timestamp"], keep="last")
    merged, stats = clean_ohlcv(merged)
    logger.info("%s: intervalos entre velas > 5 min: %d", symbol, stats.gaps_over_5min)
    out = parquet_path(symbol)
    merged.to_parquet(out, engine="pyarrow", index=False)
    return merged


def asset_summary(symbol: str, df: pd.DataFrame) -> dict[str, object]:
    path = parquet_path(symbol)
    size_mb = path.stat().st_size / (1024 * 1024) if path.is_file() else 0.0
    n = len(df)
    t0, t1 = df["timestamp"].min(), df["timestamp"].max()
    span_min = (t1 - t0).total_seconds() / 60.0 + 1.0 if n else 0.0
    coverage = (100.0 * n / span_min) if span_min > 0 else 0.0
    return {
        "rows": n,
        "start": t0,
        "end": t1,
        "coverage_pct": coverage,
        "size_mb": size_mb,
    }


def resolve_assets_filter(names: list[str] | None) -> list[str]:
    if not names:
        return list(ASSETS.keys())
    resolved: list[str] = []
    for raw in names:
        u = raw.strip().upper()
        if u.endswith("USDT"):
            key = u
        else:
            key = f"{u}USDT"
        if key not in ASSETS:
            raise SystemExit(f"Activo desconocido: {raw} (resuelto como {key})")
        resolved.append(key)
    return list(dict.fromkeys(resolved))


def run_asset(
    symbol: str,
    start_year: int,
    start_month: int,
    update_only: bool,
    limiter: ZipRequestRateLimiter,
) -> pd.DataFrame | None:
    path = parquet_path(symbol)
    if update_only and not path.is_file():
        logger.warning("--update: omitiendo %s (no existe parquet)", symbol)
        return None

    last_ts = load_last_timestamp(symbol)
    existing: pd.DataFrame | None = None
    if path.is_file():
        existing = pd.read_parquet(path)

    now = datetime.now(timezone.utc)
    end_y, end_m = now.year, now.month

    months = months_to_fetch(symbol, start_year, start_month, last_ts, end_y, end_m)
    if not months:
        logger.info("%s ya está al día.", symbol)
        return pd.read_parquet(path) if path.is_file() else None

    label = short_label(symbol)
    total = len(months)
    new_frames: list[pd.DataFrame] = []
    zip_paths_to_delete: list[Path] = []

    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [
            ex.submit(process_one_month, symbol, y, m, limiter) for y, m in months
        ]
        pbar = tqdm(
            as_completed(futures),
            total=total,
            desc=f"{label}",
            unit="months",
            bar_format="{desc}: {postfix} |{bar}| {n_fmt}/{total_fmt} months",
            dynamic_ncols=True,
        )
        for fut in pbar:
            (y, m), part, zpath, err = fut.result()
            pbar.set_postfix_str(f"{y}-{m:02d}", refresh=True)
            if err and err not in ("zip 404",):
                if err != "checksum mismatch":
                    logger.warning("%s %04d-%02d: %s", symbol, y, m, err)
            if part is not None and not part.empty:
                new_frames.append(part)
            if zpath is not None and zpath.is_file():
                zip_paths_to_delete.append(zpath)
        pbar.close()

    if not new_frames:
        if existing is not None and not existing.empty:
            return existing
        logger.warning("%s: no se descargó ningún mes nuevo.", symbol)
        return None

    final_df = merge_and_save(symbol, new_frames, existing)

    for zp in zip_paths_to_delete:
        try:
            if zp.is_file():
                zp.unlink()
        except OSError:
            pass

    return final_df


def print_final_summary(results: dict[str, pd.DataFrame | None]) -> None:
    for symbol, df in results.items():
        label = short_label(symbol)
        if df is None or df.empty:
            print(f"\n⚠️  {symbol}\n   Sin datos guardados.\n")
            continue
        info = asset_summary(symbol, df)
        rows = int(info["rows"])
        cov = float(info["coverage_pct"])
        mb = float(info["size_mb"])
        t0 = pd.Timestamp(info["start"])
        t1 = pd.Timestamp(info["end"])
        note = ""
        months_span = (t1.to_period("M") - t0.to_period("M")).n + 1
        if months_span < 12 or symbol == "HYPEUSDT":
            note = f"\n   Note: Only ~{months_span} month(s) — ML not recommended"
        icon = "✅" if cov >= 95.0 and rows > 0 else "⚠️ "
        print(
            f"\n{icon} {symbol}\n"
            f"   Rows: {rows:,}\n"
            f"   Range: {t0.strftime('%Y-%m-%d')} → {t1.strftime('%Y-%m-%d')}\n"
            f"   Coverage: {cov:.1f}%\n"
            f"   Size: {mb:.0f}MB{note}\n"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Descarga OHLCV 1m desde Binance Data Vision.")
    p.add_argument(
        "--assets",
        nargs="+",
        help="Subconjunto, ej. BTC ETH SOL (se resuelven a *USDT)",
    )
    p.add_argument(
        "--update",
        action="store_true",
        help="Solo activos que ya tengan parquet en data/raw/",
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    ZIPS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    symbols = resolve_assets_filter(args.assets)
    limiter = ZipRequestRateLimiter(1.0)

    results: dict[str, pd.DataFrame | None] = {}
    for sym in symbols:
        meta = ASSETS[sym]
        df = run_asset(
            sym,
            int(meta["start_year"]),
            int(meta["start_month"]),
            bool(args.update),
            limiter,
        )
        if df is not None:
            results[sym] = df
        elif parquet_path(sym).is_file():
            results[sym] = pd.read_parquet(parquet_path(sym))
        else:
            results[sym] = None

    print_final_summary(results)


if __name__ == "__main__":
    main()
