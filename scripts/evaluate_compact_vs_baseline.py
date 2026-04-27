#!/usr/bin/env python3
"""
Comparación walk-forward simple:
- baseline (features actuales)
- baseline + exógenas compactas

Sin guardar modelos; sólo métricas y reporte JSON/Markdown.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lab.paths import default_compact_experiment_dir

COMPACT_DIR = REPO_ROOT / "data" / "raw" / "exogenous" / "compact_5m"

BASELINE_FEATURES = [
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

EXO_FEATURES = [
    "taker_imbalance_5m",
    "buy_sell_ratio_5m",
    "n_trades",
    "funding_rate",
    "funding_change",
    "oi_level",
    "oi_change_5m",
    "sum_taker_long_short_vol_ratio",
    "poly_mid",
    "poly_mid_change_1",
    "spot_vs_poly_gap",
]

log = logging.getLogger("evaluate_compact")


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [compact-eval] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logging.Formatter.converter = time.gmtime


def _time_cv_accuracy(df: pd.DataFrame, features: list[str], target: str, n_splits: int) -> tuple[float, float, list[float]]:
    sub = df.copy()
    sub = sub.replace([np.inf, -np.inf], np.nan)
    # Imputación robusta para evitar perder todo el rango por una feature parcial.
    for c in features:
        if c not in sub.columns:
            return float("nan"), float("nan"), []
        med = sub[c].median(skipna=True)
        if pd.isna(med):
            return float("nan"), float("nan"), []
        sub[c] = sub[c].fillna(float(med))
        lo = sub[c].quantile(0.01)
        hi = sub[c].quantile(0.99)
        if np.isfinite(lo) and np.isfinite(hi) and lo < hi:
            sub[c] = sub[c].clip(lo, hi)
    sub = sub.dropna(subset=[target]).copy()
    if len(sub) < 5000:
        return float("nan"), float("nan"), []

    X = sub[features].values
    y = sub[target].astype(int).values

    tscv = TimeSeriesSplit(n_splits=n_splits)
    scores: list[float] = []
    for tr, va in tscv.split(X):
        scaler = StandardScaler()
        Xtr = scaler.fit_transform(X[tr])
        Xva = scaler.transform(X[va])
        clf = LogisticRegression(max_iter=2000, C=1.0)
        clf.fit(Xtr, y[tr])
        pred = clf.predict(Xva)
        scores.append(float(accuracy_score(y[va], pred)))
    return float(np.mean(scores)), float(np.std(scores)), scores


def _majority_baseline(y: pd.Series) -> float:
    p = float(y.mean())
    return max(p, 1 - p)


def _build_markdown(report: dict) -> str:
    rows = report["assets"]
    lines = [
        "# Compact Plan Evaluation",
        "",
        "Comparación baseline vs baseline+exógenas (LogReg + TimeSeriesSplit).",
        "",
        "| Asset | Rows | Majority | Baseline mean±std | Compact mean±std | Delta vs baseline |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in rows:
        lines.append(
            f"| {r['asset']} | {r['rows']:,} | {r['majority']:.4f} | "
            f"{r['baseline_mean']:.4f}±{r['baseline_std']:.4f} | "
            f"{r['compact_mean']:.4f}±{r['compact_std']:.4f} | "
            f"{r['delta_vs_baseline']:+.4f} |"
        )
    lines.extend(
        [
            "",
            f"## Decision",
            f"{report['decision']}",
            "",
            "## Success Criteria",
            "- Mejora consistente y robusta sobre baseline.",
            "- Edge medio objetivo >= 0.02 sobre mayoría en validación temporal.",
        ]
    )
    return "\n".join(lines) + "\n"


@dataclass
class EvalCfg:
    assets: list[str]
    start: str
    n_splits: int
    verbose: bool
    experiment_dir: Path


def parse_args() -> EvalCfg:
    p = argparse.ArgumentParser(description="Evalúa compact vs baseline.")
    p.add_argument("--assets", nargs="+", default=["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"])
    p.add_argument("--start", default="2021-01-01")
    p.add_argument("--n-splits", type=int, default=5)
    p.add_argument("--verbose", action="store_true")
    p.add_argument(
        "--experiment-dir",
        type=Path,
        default=None,
        help="Salida JSON/MD (default: strategies/crypto_5m_updown/experiments/exogenous_compact o PM_STRATEGY_EXPERIMENT_DIR).",
    )
    a = p.parse_args()
    assets = []
    for x in a.assets:
        s = x.strip().upper()
        if not s.endswith("USDT"):
            s = f"{s}USDT"
        assets.append(s)
    if a.experiment_dir is not None:
        exp = a.experiment_dir.resolve() if a.experiment_dir.is_absolute() else (REPO_ROOT / a.experiment_dir).resolve()
    else:
        exp = default_compact_experiment_dir()
    return EvalCfg(
        assets=list(dict.fromkeys(assets)),
        start=str(a.start),
        n_splits=max(3, int(a.n_splits)),
        verbose=bool(a.verbose),
        experiment_dir=exp,
    )


def main() -> None:
    cfg = parse_args()
    setup_logging(cfg.verbose)
    out_dir = cfg.experiment_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    asset_rows: list[dict] = []
    compact_wins = 0
    valid_assets = 0

    for asset in cfg.assets:
        p = COMPACT_DIR / f"{asset}.parquet"
        if not p.is_file():
            log.warning("No existe %s, se omite", p)
            continue
        df = pd.read_parquet(p)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df[df["timestamp"] >= pd.Timestamp(cfg.start, tz="UTC")].sort_values("timestamp")
        if df.empty:
            continue

        majority = _majority_baseline(df["target"])
        b_mean, b_std, _ = _time_cv_accuracy(df, BASELINE_FEATURES, "target", cfg.n_splits)
        exo_active: list[str] = []
        for f in EXO_FEATURES:
            if f not in df.columns:
                continue
            cov = float(df[f].replace([np.inf, -np.inf], np.nan).notna().mean())
            if cov >= 0.20:
                exo_active.append(f)
        c_feats = BASELINE_FEATURES + exo_active
        c_mean, c_std, _ = _time_cv_accuracy(df, c_feats, "target", cfg.n_splits)
        if np.isfinite(b_mean) and np.isfinite(c_mean):
            valid_assets += 1
            if c_mean > b_mean:
                compact_wins += 1

        row = {
            "asset": asset,
            "rows": int(len(df)),
            "majority": float(majority),
            "baseline_mean": float(b_mean) if np.isfinite(b_mean) else float("nan"),
            "baseline_std": float(b_std) if np.isfinite(b_std) else float("nan"),
            "compact_mean": float(c_mean) if np.isfinite(c_mean) else float("nan"),
            "compact_std": float(c_std) if np.isfinite(c_std) else float("nan"),
            "delta_vs_baseline": (float(c_mean) - float(b_mean)) if np.isfinite(c_mean) and np.isfinite(b_mean) else float("nan"),
        }
        asset_rows.append(row)
        log.info(
            "%s rows=%s majority=%.4f baseline=%.4f compact=%.4f delta=%+.4f",
            asset,
            row["rows"],
            row["majority"],
            row["baseline_mean"],
            row["compact_mean"],
            row["delta_vs_baseline"],
        )

    decision = "NO-GO"
    if valid_assets > 0 and compact_wins >= max(1, int(np.ceil(0.6 * valid_assets))):
        decision = "GO_CANDIDATE"

    report = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "assets": asset_rows,
        "valid_assets": valid_assets,
        "compact_wins": compact_wins,
        "decision": decision,
    }

    json_path = out_dir / "compact_eval_report.json"
    md_path = out_dir / "compact_eval_report.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_path.write_text(_build_markdown(report), encoding="utf-8")
    log.info("Reporte JSON: %s", json_path.relative_to(REPO_ROOT))
    log.info("Reporte MD: %s", md_path.relative_to(REPO_ROOT))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
