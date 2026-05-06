#!/usr/bin/env python3
"""Descarga OHLCV 1m desde Binance Data Vision (bulk ZIP), no API."""

from __future__ import annotations

import argparse
import gc
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
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
ZIPS_DIR = DATA_DIR / "zips"
RAW_DIR = DATA_DIR / "raw"

# Piso UTC para descarga incremental: no pedir meses ZIP anteriores a esta fecha.
START_DATE = "2023-01-01"

BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"
DAILY_KLINES_BASE = "https://data.binance.vision/data/spot/daily/klines"

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


def daily_zip_url(symbol: str, day: pd.Timestamp) -> str:
    d = pd.Timestamp(day).tz_convert("UTC").strftime("%Y-%m-%d")
    return f"{DAILY_KLINES_BASE}/{symbol}/1m/{symbol}-1m-{d}.zip"


def fetch_daily_klines_inclusive_range(
    symbol: str,
    t_lo: pd.Timestamp,
    t_hi: pd.Timestamp,
    limiter: ZipRequestRateLimiter,
) -> pd.DataFrame | None:
    """Velas 1m en [t_lo, t_hi] (UTC) con ZIP **diarios** (p. ej. mes en curso sin ZIP mensual)."""
    t_lo = pd.Timestamp(t_lo).tz_convert("UTC")
    t_hi = pd.Timestamp(t_hi).tz_convert("UTC")
    if t_lo > t_hi:
        return None
    day0 = t_lo.normalize()
    day1 = t_hi.normalize()
    parts: list[pd.DataFrame] = []
    cur = day0
    while cur <= day1:
        dest = ZIPS_DIR / f"{symbol}-1m-{cur.strftime('%Y-%m-%d')}.zip"
        try:
            limiter.wait()
            ok = download_to_file(daily_zip_url(symbol, cur), dest)
            if ok:
                raw = read_klines_from_zip(dest)
                ohlcv = klines_to_ohlcv(raw)
                if not ohlcv.empty:
                    parts.append(ohlcv)
        except Exception as e:  # noqa: BLE001
            logger.warning("%s daily %s: %s", symbol, cur.strftime("%Y-%m-%d"), e)
        finally:
            if dest.is_file():
                try:
                    dest.unlink(missing_ok=True)
                except OSError:
                    pass
        cur = cur + pd.Timedelta(days=1)
    if not parts:
        return None
    out = pd.concat(parts, ignore_index=True)
    ts = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out = out.loc[ts.notna() & (ts >= t_lo) & (ts <= t_hi)].reset_index(drop=True)
    return out if not out.empty else None


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
    """CSV Binance → OHLCV 1m; descarta open_time no numérico o fuera de rango (evita timestamps absurdos)."""
    if df.empty:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
    ot = pd.to_numeric(df["open_time"], errors="coerce")
    m = ot.notna()
    if not m.any():
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
    # Binance Data Vision: ZIP viejos usan open_time en ms (~1.2e12); a partir de ~2025-12 muchos CSV vienen en µs
    # (~1.7e15). Sin normalizar, el filtro ms_ok descarta todas las filas.
    for _ in range(4):
        mx = float(ot[m].max())
        if mx <= 5e12:  # ms Unix hasta ~año 2128
            break
        ot = ot / 1000.0
    sub = df.loc[m].copy()
    ot_i = ot[m].astype("int64")
    ms_ok = (ot_i >= _OPEN_TIME_MS_LO) & (ot_i <= _OPEN_TIME_MS_HI)
    n_bad_ms = int((~ms_ok).sum())
    if n_bad_ms:
        logger.debug("klines: descartadas %d filas (open_time ms fuera de rango)", n_bad_ms)
    sub = sub.loc[ms_ok].reset_index(drop=True)
    ot_i = ot_i.loc[ms_ok].reset_index(drop=True)
    if sub.empty:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
    out = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(ot_i, unit="ms", utc=True),
            "open": sub["open"].astype("float64"),
            "high": sub["high"].astype("float64"),
            "low": sub["low"].astype("float64"),
            "close": sub["close"].astype("float64"),
            "volume": sub["volume"].astype("float64"),
        }
    )
    return out


@dataclass
class CleanStats:
    gaps_over_5min: int


def clean_ohlcv(
    df: pd.DataFrame,
    *,
    dense_reindex: bool = True,
) -> tuple[pd.DataFrame, CleanStats]:
    if df.empty:
        return df, CleanStats(gaps_over_5min=0)

    d = df.drop_duplicates(subset=["timestamp"], keep="last")
    d = d.sort_values("timestamp").reset_index(drop=True)

    diffs = d["timestamp"].diff()
    gaps_over_5 = int((diffs > pd.Timedelta("5min")).sum())
    stats = CleanStats(gaps_over_5min=gaps_over_5)

    # Sin reindex denso: evita date_range(1min) en todo el histórico (OOM con años de datos).
    if not dense_reindex:
        return d, stats

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


# Binance Vision no debería devolver velas fuera de este rango; filas raras rompen strftime y stats.
_TS_VALID_LO = pd.Timestamp("2017-01-01", tz="UTC")
_TS_VALID_HI = pd.Timestamp("2040-12-31", tz="UTC")
# Límites en open_time (ms) antes de to_datetime — coherente con _TS_VALID_*.
_OPEN_TIME_MS_LO = int(_TS_VALID_LO.timestamp() * 1000)
_OPEN_TIME_MS_HI = int((_TS_VALID_HI + pd.Timedelta(days=1)).timestamp() * 1000) - 1


def _drop_timestamp_outliers(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty or "timestamp" not in df.columns:
        return df
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    ok = ts.notna() & (ts >= _TS_VALID_LO) & (ts <= _TS_VALID_HI)
    n_bad = int((~ok).sum())
    if n_bad:
        logger.warning("%s: descartando %d filas con timestamp inválido o fuera de rango", symbol, n_bad)
    out = df.loc[ok].copy()
    out["timestamp"] = ts[ok]
    return out.reset_index(drop=True)


def _safe_iso_date(ts: object) -> str:
    """strftime falla en Timestamp extremos (p. ej. año > 9999)."""
    try:
        t = pd.Timestamp(ts)
        if getattr(t, "tzinfo", None) is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
    except (ValueError, TypeError, OSError):
        return "?"
    try:
        return t.strftime("%Y-%m-%d")
    except (ValueError, OSError, NotImplementedError):
        return f"{int(t.year):04d}-{int(t.month):02d}-{int(t.day):02d}"


def _months_span_calendar(t0: pd.Timestamp, t1: pd.Timestamp) -> int:
    if pd.isna(t0) or pd.isna(t1) or t0 > t1:
        return 0
    return (t1.year - t0.year) * 12 + (t1.month - t0.month) + 1


def load_last_timestamp(symbol: str) -> pd.Timestamp | None:
    path = parquet_path(symbol)
    if not path.is_file():
        return None
    s = pd.read_parquet(path, columns=["timestamp"])
    if s.empty:
        return None
    tcol = pd.to_datetime(s["timestamp"], utc=True, errors="coerce")
    ok = tcol.notna() & (tcol >= _TS_VALID_LO) & (tcol <= _TS_VALID_HI)
    if not ok.any():
        return None
    ts = tcol[ok].max()
    if getattr(ts, "tzinfo", None) is None:
        ts = pd.Timestamp(ts).tz_localize("UTC")
    else:
        ts = pd.Timestamp(ts).tz_convert("UTC")
    return ts


def months_spanning_range(window_start: pd.Timestamp, window_end: pd.Timestamp) -> list[tuple[int, int]]:
    """Meses calendario UTC que pueden contener velas en [window_start, window_end] (ambos inclusive)."""
    ws = pd.Timestamp(window_start).tz_convert("UTC")
    we = pd.Timestamp(window_end).tz_convert("UTC")
    cur = pd.Timestamp(year=ws.year, month=ws.month, day=1, tz="UTC")
    end_m = pd.Timestamp(year=we.year, month=we.month, day=1, tz="UTC")
    out: list[tuple[int, int]] = []
    while cur <= end_m:
        out.append((int(cur.year), int(cur.month)))
        cur = cur + pd.DateOffset(months=1)
    return out


def cleanup_zips_outside_months(symbol: str, keep_months: set[tuple[int, int]]) -> None:
    """Borra ZIPs locales del símbolo cuyo (año, mes) no está en keep_months."""
    if not ZIPS_DIR.is_dir():
        return
    pat = re.compile(rf"^{re.escape(symbol)}-1m-(\d{{4}})-(\d{{2}})\.zip$")
    for zp in ZIPS_DIR.glob(f"{symbol}-1m-*.zip"):
        mo = pat.match(zp.name)
        if not mo:
            continue
        ym = (int(mo.group(1)), int(mo.group(2)))
        if ym in keep_months:
            continue
        try:
            zp.unlink(missing_ok=True)
        except OSError:
            pass


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
            logger.debug("%s %04d-%02d: CHECKSUM 404, se descarga sin verificar.", symbol, year, month)
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
        ohlcv = _drop_timestamp_outliers(ohlcv, f"{symbol} {year:04d}-{month:02d}")
        return (year, month), ohlcv, dest, None
    except Exception as e:  # noqa: BLE001
        if dest.is_file():
            dest.unlink(missing_ok=True)
        return (year, month), None, None, str(e)


def merge_and_save(
    symbol: str,
    new_parts: list[pd.DataFrame],
    existing: pd.DataFrame | None,
    *,
    dense_reindex: bool = True,
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
    merged = _drop_timestamp_outliers(merged, symbol)
    merged, stats = clean_ohlcv(merged, dense_reindex=dense_reindex)
    logger.info("%s: intervalos entre velas > 5 min: %d", symbol, stats.gaps_over_5min)
    out = parquet_path(symbol)
    merged.to_parquet(out, engine="pyarrow", index=False)
    return merged


def asset_summary(symbol: str, df: pd.DataFrame) -> dict[str, object]:
    path = parquet_path(symbol)
    size_mb = path.stat().st_size / (1024 * 1024) if path.is_file() else 0.0
    n = len(df)
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    ok = ts.notna() & (ts >= _TS_VALID_LO) & (ts <= _TS_VALID_HI)
    ts_ok = ts[ok] if ok.any() else ts.dropna()
    t0 = ts_ok.min() if len(ts_ok) else pd.Timestamp("NaT", tz="UTC")
    t1 = ts_ok.max() if len(ts_ok) else pd.Timestamp("NaT", tz="UTC")
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
    *,
    full: bool = False,
) -> pd.DataFrame | None:
    path = parquet_path(symbol)
    if update_only and not path.is_file():
        logger.warning("--update: omitiendo %s (no existe parquet)", symbol)
        return None

    if full:
        logger.info(
            "%s: --full: descarga desde %04d-%02d (ignorando parquet existente y último timestamp).",
            symbol,
            start_year,
            start_month,
        )
        last_ts = None
        existing: pd.DataFrame | None = None
    else:
        last_ts = load_last_timestamp(symbol)
        existing = None
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
    # Pocos meses por merge + sin reindex denso en clean_ohlcv evita OOM con años de 1m.
    flush_every_n = 3

    # Un solo worker reduce picos de RAM (ZIP + parse en paralelo saturaba OOM con flush cada 6 meses).
    with ThreadPoolExecutor(max_workers=1) as ex:
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
            if len(new_frames) >= flush_every_n:
                existing = merge_and_save(
                    symbol, new_frames, existing, dense_reindex=False
                )
                new_frames.clear()
                gc.collect()
            if zpath is not None and zpath.is_file():
                zip_paths_to_delete.append(zpath)
        pbar.close()

    if new_frames:
        existing = merge_and_save(symbol, new_frames, existing, dense_reindex=False)
        gc.collect()

    if existing is not None and not existing.empty:
        for zp in zip_paths_to_delete:
            try:
                if zp.is_file():
                    zp.unlink()
            except OSError:
                pass
        return existing

    logger.warning("%s: no se descargó ningún mes nuevo.", symbol)
    return None


def run_asset_window(
    symbol: str,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
    limiter: ZipRequestRateLimiter,
) -> pd.DataFrame | None:
    """
    Descarga solo los ZIP mensuales que solapan la ventana y escribe ``{symbol}_1min.parquet``
    **solo** con filas UTC en [window_start, window_end] (inclusive). No fusiona un parquet previo.
    """
    ws = pd.Timestamp(window_start).tz_convert("UTC")
    we = pd.Timestamp(window_end).tz_convert("UTC")
    if ws > we:
        logger.error("%s: window_start > window_end", symbol)
        return None

    months = months_spanning_range(ws, we)
    if not months:
        logger.warning("%s: sin meses en ventana %s … %s", symbol, ws, we)
        return None

    new_frames: list[pd.DataFrame] = []
    zip_paths_to_delete: list[Path] = []
    keep_months = set(months)

    with ThreadPoolExecutor(max_workers=1) as ex:
        futures = [ex.submit(process_one_month, symbol, y, m, limiter) for y, m in months]
        label = short_label(symbol)
        total = len(futures)
        pbar = tqdm(
            as_completed(futures),
            total=total,
            desc=f"{label} (ventana)",
            unit="months",
            bar_format="{desc}: {postfix} |{bar}| {n_fmt}/{total_fmt} months",
            dynamic_ncols=True,
        )
        for fut in pbar:
            (y, m), part, zpath, err = fut.result()
            pbar.set_postfix_str(f"{y}-{m:02d}", refresh=True)
            if err and err not in ("zip 404",) and err != "checksum mismatch":
                logger.warning("%s %04d-%02d: %s", symbol, y, m, err)
            if part is not None and not part.empty:
                new_frames.append(part)
            if zpath is not None and zpath.is_file():
                zip_paths_to_delete.append(zpath)
        pbar.close()

    if not new_frames:
        logger.error("%s: ventana %s … %s sin datos (ZIP 404 o vacíos).", symbol, ws, we)
        return None

    merged = merge_and_save(symbol, new_frames, None, dense_reindex=False)
    ts_all = pd.to_datetime(merged["timestamp"], utc=True, errors="coerce")
    tmax = ts_all.max()
    gap_lo = ws
    if pd.notna(tmax) and tmax + pd.Timedelta(minutes=1) <= we:
        gap_lo = max(ws, tmax + pd.Timedelta(minutes=1))
    if gap_lo <= we:
        extra = fetch_daily_klines_inclusive_range(symbol, gap_lo, we, limiter)
        if extra is not None and not extra.empty:
            merged = pd.concat([merged, extra], ignore_index=True)
            merged = merged.sort_values("timestamp").reset_index(drop=True)
            merged = merged.drop_duplicates(subset=["timestamp"], keep="last")
            merged = _drop_timestamp_outliers(merged, symbol)
            merged, _ = clean_ohlcv(merged, dense_reindex=False)

    ts = pd.to_datetime(merged["timestamp"], utc=True, errors="coerce")
    mask = ts.notna() & (ts >= ws) & (ts <= we)
    merged = merged.loc[mask].reset_index(drop=True)
    merged, stats = clean_ohlcv(merged, dense_reindex=False)
    logger.info("%s (ventana): velas=%d, gaps>5min=%d", symbol, len(merged), stats.gaps_over_5min)
    merged.to_parquet(parquet_path(symbol), engine="pyarrow", index=False)

    for zp in zip_paths_to_delete:
        try:
            if zp.is_file():
                zp.unlink()
        except OSError:
            pass

    cleanup_zips_outside_months(symbol, keep_months)
    gc.collect()
    return merged


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
        months_span = _months_span_calendar(t0, t1)
        if months_span < 12 or symbol == "HYPEUSDT":
            note = f"\n   Note: Only ~{months_span} month(s) — ML not recommended"
        icon = "✅" if cov >= 95.0 and rows > 0 else "⚠️ "
        print(
            f"\n{icon} {symbol}\n"
            f"   Rows: {rows:,}\n"
            f"   Range: {_safe_iso_date(t0)} → {_safe_iso_date(t1)}\n"
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
    p.add_argument(
        "--full",
        action="store_true",
        help="Ignorar parquet existente: descargar desde START_DATE (sin incremental desde último timestamp).",
    )
    p.add_argument(
        "--window-start",
        metavar="YYYY-MM-DD",
        default=None,
        help="Con --window-end: solo descarga meses que solapan y guarda parquet recortado a esa ventana (UTC).",
    )
    p.add_argument(
        "--window-end",
        metavar="YYYY-MM-DD",
        default=None,
        help="Fin inclusive (todo el día UTC del calendario indicado).",
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    ZIPS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    symbols = resolve_assets_filter(args.assets)
    limiter = ZipRequestRateLimiter(1.0)

    ws_raw = args.window_start
    we_raw = args.window_end
    if (ws_raw is None) ^ (we_raw is None):
        raise SystemExit("Usa --window-start y --window-end juntos (YYYY-MM-DD), o ninguno.")
    window_mode = ws_raw is not None and we_raw is not None
    if window_mode and args.update:
        logger.warning("--update se ignora en modo ventana (--window-*).")
    if window_mode and args.full:
        logger.warning("--full se ignora en modo ventana (--window-*).")

    floor = pd.Timestamp(START_DATE, tz="UTC")
    floor_ym = (int(floor.year), int(floor.month))
    results: dict[str, pd.DataFrame | None] = {}
    for sym in symbols:
        if window_mode:
            ws = pd.Timestamp(ws_raw, tz="UTC")
            we_day = pd.Timestamp(we_raw, tz="UTC")
            we = we_day + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            df = run_asset_window(sym, ws, we, limiter)
            results[sym] = df
            continue
        meta = ASSETS[sym]
        st_y, st_m = max(
            (int(meta["start_year"]), int(meta["start_month"])),
            floor_ym,
        )
        df = run_asset(
            sym,
            st_y,
            st_m,
            bool(args.update),
            limiter,
            full=bool(args.full),
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
