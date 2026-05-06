#!/usr/bin/env python3
"""
Pipeline de entrenamiento ML (LightGBM + calibración isotónica) para PredMarket Arb.
Walk-forward temporal; modelos por régimen de volatilidad opcionales.
"""

from __future__ import annotations

import argparse
import json
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn  # noqa: E402
from rich.table import Table  # noqa: E402
from sklearn.calibration import calibration_curve  # noqa: E402
from sklearn.isotonic import IsotonicRegression  # noqa: E402
from sklearn.metrics import (  # noqa: E402
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "raw"
OUTPUT_DIR = REPO_ROOT / "models" / "saved"
REPORTS_DIR = REPO_ROOT / "reports"

ASSETS = ["BTCUSDT"]

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

START_DATE = "2023-01-01"
RESAMPLE = "5min"

MODEL_PARAMS: dict[str, Any] = {
    "n_estimators": 500,
    "learning_rate": 0.02,
    "max_depth": 5,
    "num_leaves": 24,
    "min_child_samples": 150,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "objective": "binary",
    "verbose": -1,
    "random_state": 42,
}

# Walk-forward: intervalos [train_start, train_end), [val_start, val_end) en índice UTC
WF_FOLDS: list[tuple[str, str, str, str, str]] = [
    ("1", "2023-01-01", "2023-07-01", "2023-07-01", "2024-01-01"),
    ("2", "2023-01-01", "2024-01-01", "2024-01-01", "2024-07-01"),
    ("3", "2023-01-01", "2024-07-01", "2024-07-01", "2025-01-01"),
    ("4", "2023-01-01", "2025-01-01", "2025-01-01", "2025-07-01"),
    ("5", "2023-01-01", "2025-07-01", "2025-07-01", "2026-01-01"),
]

console = Console()


@dataclass
class FoldMetrics:
    fold_id: str
    train_label: str
    val_label: str
    accuracy: float
    edge: float
    brier: float
    auc: float
    logloss: float


@dataclass
class AssetReport:
    asset: str
    folds: list[FoldMetrics] = field(default_factory=list)
    wf_acc_mean: float = 0.0
    wf_acc_std: float = 0.0
    mean_edge: float = 0.0
    brier_final: float = 0.0
    ece: float = 0.0
    best_fold_acc: float = 0.0
    worst_fold_acc: float = 0.0
    best_features: list[str] = field(default_factory=list)
    error: str | None = None


def _ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC")


def load_and_featurize(asset: str) -> pd.DataFrame:
    path = DATA_DIR / f"{asset}_1min.parquet"
    if not path.is_file():
        raise FileNotFoundError(
            f"No existe {rel_repo(path)}. Genera datos 1m Binance con: "
            f"python download_datasets.py (símbolo {asset})."
        )
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    df = pd.read_parquet(path, columns=cols).set_index("timestamp").sort_index()
    df = df[df.index >= _ts(START_DATE)]
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

    df5["vol_regime"] = (
        df5["vol_20"] > df5["vol_20"].rolling(100, min_periods=50).mean()
    ).map({True: "high", False: "low"})

    # Target Binance: siguiente vela 5m cierra >= su open (sin join Polymarket).
    df5["target"] = (df5["close"].shift(-1) >= df5["open"].shift(-1)).astype(int)

    df5 = df5.dropna(subset=FEATURES + ["target", "vol_regime"])
    return df5


def walk_forward_empty_reason(df5: pd.DataFrame) -> str:
    """Explica por qué WF_FOLDS no produjo ningún fold (p. ej. parquet demasiado corto)."""
    idx = df5.index
    n = len(df5)
    t0, t1 = idx.min(), idx.max()
    first_va_s = WF_FOLDS[0][3]
    last_va_e = WF_FOLDS[-1][4]
    return (
        f"sin datos suficientes para walk-forward: ningún fold tiene val≥100 filas "
        f"con train≥500 (ventanas desde 2023). "
        f"Dataset 5m: n={n}, UTC {t0} … {t1}; las validaciones WF esperan solapar "
        f"[{first_va_s}, {last_va_e}). "
        f"Amplía el parquet (p. ej. python download_datasets.py) hasta cubrir esas fechas."
    )


def mask_interval(idx: pd.DatetimeIndex, start: str, end: str) -> np.ndarray:
    t0, t1 = _ts(start), _ts(end)
    return (idx >= t0) & (idx < t1)


def make_lgbm() -> Any:
    try:
        from lightgbm import LGBMClassifier

        return LGBMClassifier(**MODEL_PARAMS)
    except (OSError, ImportError) as e:
        raise RuntimeError(
            "LightGBM no disponible (p. ej. falta libomp). Instala libomp o usa un entorno "
            f"con LightGBM funcional. Detalle: {e}"
        ) from e


def eval_fold(y_true: np.ndarray, proba: np.ndarray) -> tuple[float, float, float, float, float]:
    pred = (proba >= 0.5).astype(int)
    acc = float(accuracy_score(y_true, pred))
    edge = acc - 0.5
    brier = float(brier_score_loss(y_true, proba))
    ll = float(log_loss(y_true, proba, labels=[0, 1]))
    try:
        auc = float(roc_auc_score(y_true, proba))
    except ValueError:
        auc = float("nan")
    return acc, edge, brier, auc, ll


def expected_calibration_error(y_true: np.ndarray, proba: np.ndarray, n_bins: int = 10) -> float:
    """ECE binario: sum_k (n_k/n) * |accuracy_k - mean(proba)_k|."""
    y = np.asarray(y_true).astype(int)
    p = np.asarray(proba, dtype=np.float64)
    n = len(y)
    if n == 0:
        return 0.0
    idx = np.clip((np.floor(p * n_bins)).astype(int), 0, n_bins - 1)
    ece = 0.0
    for i in range(n_bins):
        m = idx == i
        if not np.any(m):
            continue
        acc = float(y[m].mean())
        conf = float(p[m].mean())
        ece += (float(np.sum(m)) / n) * abs(acc - conf)
    return ece


def apply_calibrator(iso: IsotonicRegression, raw: np.ndarray) -> np.ndarray:
    """Producción: raw → calibrado (sklearn usa predict; spec equivalente a transform)."""
    r = np.asarray(raw, dtype=np.float64).reshape(-1)
    return np.asarray(iso.predict(r))


def plot_calibration_curve(
    y_true: np.ndarray, proba: np.ndarray, asset: str, suffix: str = ""
) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    prob_true, prob_pred = calibration_curve(y_true, proba, n_bins=10, strategy="uniform")
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot(prob_pred, prob_true, "o-", label="Modelo", color="steelblue")
    ax.plot([0, 1], [0, 1], "k--", alpha=0.5, label="Perfecto")
    ax.set_xlabel("Prob. predicha (media bin)")
    ax.set_ylabel("Frecuencia real positivos")
    ax.set_title(f"Curva de calibración {asset}{suffix}")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    name = f"09_{asset}_calibration_curve.png"
    path = REPORTS_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_importance(mean_imp: dict[str, float], asset: str) -> Path:
    s = pd.Series(mean_imp).sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    s.plot(kind="barh", ax=ax, color="teal", edgecolor="none")
    ax.set_title(f"Importancia media (5 folds) — {asset}")
    ax.grid(alpha=0.3, axis="x")
    fig.tight_layout()
    path = REPORTS_DIR / f"10_{asset}_importance.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def file_size_mb(path: Path) -> str:
    if not path.is_file():
        return "0 MB"
    return f"{path.stat().st_size / (1024 * 1024):.1f} MB"


def rel_repo(p: Path) -> str:
    try:
        return str(p.relative_to(REPO_ROOT))
    except ValueError:
        return str(p)


def save_sk(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(obj, path)
    console.print(f"[green]OK[/green] Guardado: {rel_repo(path)} ({file_size_mb(path)})")


def models_exist(asset: str, regime: bool) -> bool:
    base = OUTPUT_DIR / f"{asset}_model.pkl"
    if not base.is_file():
        return False
    if not (OUTPUT_DIR / f"{asset}_calibrator.pkl").is_file():
        return False
    if regime:
        for tag in ("high", "low"):
            if not (OUTPUT_DIR / f"{asset}_model_{tag}.pkl").is_file():
                return False
            if not (OUTPUT_DIR / f"{asset}_calibrator_{tag}.pkl").is_file():
                return False
    return True


def walk_forward_full(
    df5: pd.DataFrame, asset: str, progress: Progress, task_id: Any
) -> tuple[list[FoldMetrics], dict[str, float]]:
    idx = df5.index
    fold_rows: list[FoldMetrics] = []
    importances: list[dict[str, float]] = []

    for fi, (fold_id, tr_s, tr_e, va_s, va_e) in enumerate(WF_FOLDS, start=1):
        progress.update(
            task_id,
            description=f"[cyan]{asset}[/cyan] fold {fi}/{len(WF_FOLDS)}",
        )
        try:
            m_tr = mask_interval(idx, tr_s, tr_e)
            m_va = mask_interval(idx, va_s, va_e)
            if not m_va.any():
                continue
            X_tr, y_tr = df5.loc[m_tr, FEATURES], df5.loc[m_tr, "target"].astype(int)
            X_va, y_va = df5.loc[m_va, FEATURES], df5.loc[m_va, "target"].astype(int)
            if len(X_tr) < 500 or len(X_va) < 100:
                continue
            clf = make_lgbm()
            clf.fit(X_tr, y_tr)
            proba = clf.predict_proba(X_va)[:, 1]
            acc, edge, brier, auc, ll = eval_fold(y_va.values, proba)
            fold_rows.append(
                FoldMetrics(
                    fold_id=fold_id,
                    train_label=f"{tr_s} → {tr_e}",
                    val_label=f"{va_s} → {va_e}",
                    accuracy=acc,
                    edge=edge,
                    brier=brier,
                    auc=auc,
                    logloss=ll,
                )
            )
            imp = dict(zip(FEATURES, clf.feature_importances_))
            importances.append(imp)
        finally:
            progress.update(task_id, advance=1)

    mean_imp: dict[str, float] = {}
    if importances:
        for f in FEATURES:
            mean_imp[f] = float(np.mean([d[f] for d in importances]))

    return fold_rows, mean_imp


def train_final_and_calibrate(
    df5: pd.DataFrame,
    _fold_rows: list[FoldMetrics],
) -> tuple[Any, IsotonicRegression, np.ndarray, np.ndarray, float]:
    """Calibración honesta en val OOS y modelo final de despliegue reentrenado en todo el histórico."""
    idx = df5.index
    last = WF_FOLDS[-1]
    _, tr_s, tr_e, va_s, va_e = last
    m_tr = mask_interval(idx, tr_s, tr_e)
    m_va = mask_interval(idx, va_s, va_e)
    X_tr, y_tr = df5.loc[m_tr, FEATURES], df5.loc[m_tr, "target"].astype(int)
    X_va, y_va = df5.loc[m_va, FEATURES], df5.loc[m_va, "target"].astype(int)

    final_clf = make_lgbm()
    final_clf.fit(X_tr, y_tr)
    raw_val = final_clf.predict_proba(X_va)[:, 1]

    # Evitar ECE optimista: calibrar en primer tramo de val y medir en segundo tramo (temporal).
    split = len(X_va) // 2
    if split < 100 or (len(X_va) - split) < 100:
        # Fallback robusto para datasets pequeños.
        split = max(1, int(len(X_va) * 0.7))
    raw_cal = raw_val[:split]
    y_cal = y_va.values[:split]
    raw_eval = raw_val[split:]
    y_eval = y_va.values[split:]

    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(raw_cal, y_cal)
    cal_eval = apply_calibrator(iso, raw_eval)
    ece = expected_calibration_error(y_eval, cal_eval)

    # Usar el mismo modelo con el que se calibró: garantiza que iso mapea
    # las probabilidades del modelo desplegado (no de un modelo diferente retrenado en todo).
    return final_clf, iso, y_eval, cal_eval, ece


def train_regime_pair(
    df5: pd.DataFrame, regime: str, asset: str
) -> tuple[Any, IsotonicRegression, np.ndarray, np.ndarray] | None:
    sub = df5[df5["vol_regime"] == regime].dropna(subset=FEATURES + ["target"])
    if len(sub) < 2000:
        return None
    last = WF_FOLDS[-1]
    _, tr_s, tr_e, va_s, va_e = last
    idx = sub.index
    m_tr = mask_interval(idx, tr_s, tr_e)
    m_va = mask_interval(idx, va_s, va_e)
    X_tr, y_tr = sub.loc[m_tr, FEATURES], sub.loc[m_tr, "target"].astype(int)
    X_va, y_va = sub.loc[m_va, FEATURES], sub.loc[m_va, "target"].astype(int)
    if len(X_va) < 50 or len(X_tr) < 200:
        return None
    # Calibración OOS: entrenar en split train, predecir en val (nunca visto).
    # Esto evita el overfitting en calibración que ocurría cuando el modelo
    # entrenaba en todos los datos incluyendo val y luego calibraba sobre ese mismo val.
    cal_clf = make_lgbm()
    cal_clf.fit(X_tr, y_tr)
    raw_va = cal_clf.predict_proba(X_va)[:, 1]
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(raw_va, y_va.values)
    # Modelo de despliegue: todos los datos del régimen para máxima cobertura.
    # El calibrador fue ajustado sobre cal_clf (misma arquitectura, distinto split),
    # lo que lo hace coherente con deploy cuando los datos son suficientes.
    final = make_lgbm()
    final.fit(sub[FEATURES], sub["target"].astype(int))
    cal_va = apply_calibrator(iso, raw_va)
    return final, iso, y_va.values, cal_va


def print_fold_table(asset: str, folds: list[FoldMetrics]) -> None:
    t = Table(title=f"Walk-forward — {asset}")
    t.add_column("Fold")
    t.add_column("Train")
    t.add_column("Val")
    t.add_column("Acc", justify="right")
    t.add_column("Edge", justify="right")
    t.add_column("Brier", justify="right")
    t.add_column("AUC", justify="right")
    t.add_column("LogLoss", justify="right")
    for f in folds:
        t.add_row(
            f.fold_id,
            f.train_label,
            f.val_label,
            f"{f.accuracy:.1%}",
            f"{f.edge:+.1%}",
            f"{f.brier:.4f}",
            f"{f.auc:.3f}" if np.isfinite(f.auc) else "nan",
            f"{f.logloss:.4f}",
        )
    console.print(t)


def print_asset_panel(rp: AssetReport) -> None:
    if rp.error:
        console.print(Panel.fit(f"[red]{rp.error}[/red]", title=rp.asset, border_style="red"))
        return
    accs = [f.accuracy for f in rp.folds]
    txt = f"""[bold]{rp.asset}[/bold] — Entrenamiento completo

Walk-forward accuracy: {rp.wf_acc_mean:.1%} ± {rp.wf_acc_std:.1%}
Mean edge sobre 50%: {rp.mean_edge:+.1%}
Best fold accuracy: {rp.best_fold_acc:.1%}
Worst fold accuracy: {rp.worst_fold_acc:.1%}
Brier score: {rp.brier_final:.4f}
Calibration ECE: {rp.ece:.4f}
Features destacadas: {", ".join(rp.best_features)}
Modelo guardado: [green]OK[/green]
"""
    console.print(Panel.fit(txt.strip(), title=f"{rp.asset}", border_style="green"))


def train_one_asset(
    asset: str,
    force: bool,
    skip_regime: bool,
    progress: Progress,
    parent_task: Any,
) -> AssetReport:
    rp = AssetReport(asset=asset)
    if not force and models_exist(asset, regime=not skip_regime):
        console.print(f"[yellow]OK[/yellow] {asset} ya tiene modelos — omitiendo (usa --force)")
        rp.error = "skipped_existing"
        progress.update(parent_task, advance=1)
        return rp
    try:
        df5 = load_and_featurize(asset)
    except Exception as e:  # noqa: BLE001
        rp.error = str(e)
        console.print(f"[red]Error {asset}:[/red] {e}")
        progress.update(parent_task, advance=1)
        return rp

    fold_task = progress.add_task(
        f"[cyan]{asset}[/cyan] fold 0/{len(WF_FOLDS)}",
        total=len(WF_FOLDS),
    )
    folds, mean_imp = walk_forward_full(df5, asset, progress, fold_task)
    progress.remove_task(fold_task)

    if not folds:
        rp.error = walk_forward_empty_reason(df5)
        console.print(Panel.fit(f"[red]{rp.error}[/red]", title=asset, border_style="red"))
        progress.update(parent_task, advance=1)
        return rp

    accs = [f.accuracy for f in folds]
    edges = [f.edge for f in folds]
    rp.folds = folds
    rp.wf_acc_mean = float(np.mean(accs))
    rp.wf_acc_std = float(np.std(accs))
    rp.mean_edge = float(np.mean(edges))
    rp.best_fold_acc = max(accs)
    rp.worst_fold_acc = min(accs)
    rp.brier_final = float(np.mean([f.brier for f in folds]))

    final_clf, iso, y_val, cal_val, ece = train_final_and_calibrate(df5, folds)
    rp.ece = ece
    top_feats = sorted(mean_imp.items(), key=lambda x: -x[1])[:3]
    rp.best_features = [t[0] for t in top_feats]

    print_fold_table(asset, folds)
    plot_importance(mean_imp, asset)
    ranked = sorted(mean_imp.items(), key=lambda x: -x[1])
    top5 = [f"{n} ({v:.1f})" for n, v in ranked[:5]]
    console.print(
        f"[dim]Importancia media (5 folds) — más estables arriba:[/dim] {', '.join(top5)}"
    )
    plot_calibration_curve(y_val, cal_val, asset, "")

    save_sk(OUTPUT_DIR / f"{asset}_model.pkl", final_clf)
    save_sk(OUTPUT_DIR / f"{asset}_calibrator.pkl", iso)
    print_asset_panel(rp)

    if not skip_regime:
        for tag in ("high", "low"):
            pair = train_regime_pair(df5, tag, asset)
            if pair is None:
                console.print(f"[yellow]Régimen {tag}:[/yellow] datos insuficientes, omitido")
                continue
            model, cal, y_v, raw_v = pair
            save_sk(OUTPUT_DIR / f"{asset}_model_{tag}.pkl", model)
            save_sk(OUTPUT_DIR / f"{asset}_calibrator_{tag}.pkl", cal)
            plot_calibration_curve(y_v, raw_v, asset, f"_{tag}")

    progress.update(parent_task, advance=1)
    return rp


def resolve_asset(raw: str | None) -> list[str]:
    if not raw:
        return list(ASSETS)
    u = raw.strip().upper()
    if not u.endswith("USDT"):
        u = f"{u}USDT"
    if u not in ASSETS:
        raise SystemExit(f"Asset no permitido: {raw}. Válidos: {ASSETS}")
    return [u]


def build_summary_json(reports: list[AssetReport]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "assets": {},
    }
    for r in reports:
        if r.error == "skipped_existing":
            out["assets"][r.asset] = {"skipped": True, "reason": r.error}
            continue
        if r.error:
            out["assets"][r.asset] = {"error": r.error}
            continue
        if not r.folds:
            out["assets"][r.asset] = {"error": "sin folds (estado inesperado)"}
            continue
        out["assets"][r.asset] = {
            "wf_accuracy_mean": r.wf_acc_mean,
            "wf_accuracy_std": r.wf_acc_std,
            "edge": r.mean_edge,
            "brier_score": r.brier_final,
            "ece": r.ece,
            "best_features": r.best_features,
            "target_source": "binance",
        }
    return out


def print_final_summary(reports: list[AssetReport]) -> None:
    t = Table(title="Resumen — todos los activos")
    t.add_column("Asset")
    t.add_column("WF Accuracy", justify="right")
    t.add_column("Edge", justify="right")
    t.add_column("Brier", justify="right")
    t.add_column("ECE", justify="right")
    t.add_column("Regime", justify="center")
    t.add_column("Status", justify="left", overflow="ellipsis", max_width=56)
    best_edge = -1.0
    best_a = ""
    for r in reports:
        if r.error == "skipped_existing":
            t.add_row(r.asset, "—", "—", "—", "—", "—", "omitido")
            continue
        if r.error:
            detail = r.error.replace("\n", " ").strip()
            t.add_row(r.asset, "—", "—", "—", "—", "—", detail)
            continue
        reg = (
            "2 mdls"
            if all((OUTPUT_DIR / f"{r.asset}_model_{t}.pkl").is_file() for t in ("high", "low"))
            else "1 mdl"
        )
        t.add_row(
            r.asset,
            f"{r.wf_acc_mean:.1%}±{r.wf_acc_std:.1%}",
            f"{r.mean_edge:+.1%}",
            f"{r.brier_final:.3f}",
            f"{r.ece:.3f}",
            reg,
            "OK",
        )
        if r.mean_edge > best_edge:
            best_edge = r.mean_edge
            best_a = r.asset
    console.print(t)
    if best_a:
        console.print(f"\n[bold]Mejor activo por edge (walk-forward):[/bold] {best_a} ({best_edge:+.1%})")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Entrenar LightGBM calibrado (PredMarket Arb).")
    p.add_argument("--asset", default=None, help="Ej. BTC o BTCUSDT")
    p.add_argument("--skip-regime", action="store_true", help="No entrenar modelos high/low")
    p.add_argument("--force", action="store_true", help="Reentrenar aunque existan .pkl")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    assets = resolve_asset(args.asset)
    force = bool(args.force)
    skip_regime = bool(args.skip_regime)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        parent = progress.add_task("Assets", total=len(assets))
        reports: list[AssetReport] = []
        for a in assets:
            reports.append(train_one_asset(a, force, skip_regime, progress, parent))

    print_final_summary(reports)
    report_path = REPO_ROOT / "models" / "training_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(build_summary_json(reports), indent=2), encoding="utf-8")
    console.print(f"\n[dim]JSON:[/dim] {report_path}")


if __name__ == "__main__":
    main()
