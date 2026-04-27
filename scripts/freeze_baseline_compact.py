#!/usr/bin/env python3
"""
Congela baseline actual para plan compacto.

Mide, por activo:
- base rate target actual
- baseline mayoría
- rendimiento del modelo guardado (si existe) en últimos 180 días
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lab.paths import default_compact_experiment_dir

RAW_DIR = REPO_ROOT / "data" / "raw"
MODELS_DIR = REPO_ROOT / "models" / "saved"

ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
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


def load_df(asset: str) -> pd.DataFrame:
    p = RAW_DIR / f"{asset}_1min.parquet"
    df = pd.read_parquet(p, columns=["timestamp", "open", "high", "low", "close", "volume"]).set_index("timestamp").sort_index()
    df = df[df.index >= pd.Timestamp("2021-01-01", tz="UTC")]
    df5 = (
        df.resample("5min")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
    )
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
    df5["vol_zscore"] = (df5["volume"] - vm) / vs.replace(0, np.nan)
    df5["vol_zscore"] = df5["vol_zscore"].fillna(0.0)
    df5["vol_trend"] = df5["volume"] / df5["volume"].shift(3)
    df5["hour"] = df5.index.hour
    df5["dow"] = df5.index.dayofweek
    df5["is_ny_open"] = ((df5["hour"] >= 13) & (df5["hour"] <= 16)).astype(int)
    df5["target"] = (df5["close"].shift(-1) >= df5["open"].shift(-1)).astype(int)
    return df5.dropna(subset=FEATURES + ["target"]).reset_index()


def run_one(asset: str) -> dict:
    df = load_df(asset)
    cutoff = df["timestamp"].max() - pd.Timedelta(days=180)
    eval_df = df[df["timestamp"] >= cutoff].copy()
    y = eval_df["target"].astype(int).values
    majority = max(float(y.mean()), 1.0 - float(y.mean()))
    out = {
        "asset": asset,
        "rows": int(len(eval_df)),
        "target_up_rate": float(y.mean()),
        "majority_baseline_acc": float(majority),
        "saved_model_acc": None,
        "saved_model_edge_vs_majority": None,
    }
    mp = MODELS_DIR / f"{asset}_model.pkl"
    cp = MODELS_DIR / f"{asset}_calibrator.pkl"
    if mp.is_file() and cp.is_file():
        model = joblib.load(mp)
        cal = joblib.load(cp)
        raw = model.predict_proba(eval_df[FEATURES].values)[:, 1]
        proba = np.asarray(cal.predict(raw))
        pred = (proba >= 0.5).astype(int)
        acc = float((pred == y).mean())
        out["saved_model_acc"] = acc
        out["saved_model_edge_vs_majority"] = float(acc - majority)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Congela baseline vs modelo guardado.")
    ap.add_argument(
        "--experiment-dir",
        type=Path,
        default=None,
        help="Directorio de salida (default: lab path crypto exogenous_compact o PM_STRATEGY_EXPERIMENT_DIR).",
    )
    a = ap.parse_args()
    if a.experiment_dir is not None:
        out_dir = a.experiment_dir.resolve() if a.experiment_dir.is_absolute() else (REPO_ROOT / a.experiment_dir).resolve()
    else:
        out_dir = default_compact_experiment_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = [run_one(a) for a in ASSETS]
    report = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "baseline_target": "close[t+1] >= open[t+1]",
        "assets": rows,
    }
    p = out_dir / "compact_baseline_freeze.json"
    p.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
