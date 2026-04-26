#!/usr/bin/env python3
"""Ingeniería de features y detección de señal (equivalente a 02_features.ipynb)."""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402
from scipy import stats  # noqa: E402
from sklearn.ensemble import HistGradientBoostingClassifier  # noqa: E402
from sklearn.inspection import permutation_importance  # noqa: E402
from sklearn.model_selection import train_test_split  # noqa: E402

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "raw"
REPORTS_DIR = REPO_ROOT / "reports"

START = "2021-01-01"
TARGET_CANDLES_AHEAD = 3
KS_MIN = 0.002
CORR_MAX = 0.85

FEATURES = [
    "ret_1", "ret_3", "ret_6", "ret_12",
    "ret_24", "ret_48",
    "vol_10", "vol_ratio", "atr_5",
    "vol_zscore", "vol_trend",
    "hour", "dow", "is_ny_open",
]

console = Console()


def compute_ks_scores(df5: pd.DataFrame, feats: list[str]) -> dict[str, float]:
    up = df5[df5.target == 1]
    down = df5[df5.target == 0]
    return {
        f: float(stats.ks_2samp(up[f].dropna(), down[f].dropna())[0]) for f in feats
    }


def remove_high_correlation(
    feats: list[str], df5: pd.DataFrame, scores: dict[str, float], max_abs: float = CORR_MAX
) -> list[str]:
    out = list(feats)
    while len(out) >= 2:
        c = df5[out].corr()
        worst_pair: tuple[str, str] | None = None
        worst_val = 0.0
        for i, a in enumerate(out):
            for b in out[i + 1 :]:
                v = abs(float(c.loc[a, b]))
                if v > max_abs and v > worst_val:
                    worst_val = v
                    worst_pair = (a, b)
        if worst_pair is None:
            break
        a, b = worst_pair
        if scores[a] < scores[b]:
            out.remove(a)
        elif scores[b] < scores[a]:
            out.remove(b)
        else:
            out.remove(b)
    return out


def max_offdiag_abs_corr(feats: list[str], df5: pd.DataFrame) -> float:
    if len(feats) < 2:
        return 0.0
    c = df5[feats].corr()
    m = 0.0
    for i, a in enumerate(feats):
        for b in feats[i + 1 :]:
            m = max(m, abs(float(c.loc[a, b])))
    return m


def _subplot_grid(n: int) -> tuple[plt.Figure, list[plt.Axes]]:
    ncols = 5
    nrows = max(1, (n + ncols - 1) // ncols)
    fig, axes_arr = plt.subplots(nrows, ncols, figsize=(4 * ncols, 3.2 * nrows))
    axes = list(np.atleast_1d(axes_arr).ravel())
    for j in range(n, len(axes)):
        axes[j].set_visible(False)
    return fig, axes


def _save(fig: plt.Figure, name: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def load_df5(asset: str) -> pd.DataFrame:
    path = DATA_DIR / f"{asset}_1min.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"No existe {path} — ejecuta download_datasets.py")
    df = pd.read_parquet(path).set_index("timestamp").sort_index()
    df = df[df.index >= START]
    df5 = df.resample("5T").agg(
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
    df5["ret_24"] = df5["close"].pct_change(24)  # 2hr momentum
    df5["ret_48"] = df5["close"].pct_change(48)  # 4hr momentum
    df5["mom_accel"] = df5["ret_1"] - df5["ret_3"]

    df5["vol_10"] = df5["ret_1"].rolling(10).std()
    df5["vol_20"] = df5["ret_1"].rolling(20).std()
    df5["vol_ratio"] = df5["vol_10"] / df5["vol_20"]
    df5["atr_5"] = (df5["high"] - df5["low"]).rolling(5).mean()
    df5["parkinson"] = np.sqrt(
        (np.log(df5["high"] / df5["low"]) ** 2).rolling(10).mean() / (4 * np.log(2))
    )

    vol_mean = df5["volume"].rolling(20).mean()
    vol_std = df5["volume"].rolling(20).std()
    df5["vol_zscore"] = (df5["volume"] - vol_mean) / vol_std
    df5["vol_trend"] = df5["volume"] / df5["volume"].shift(3)

    df5["hour"] = df5.index.hour
    df5["dow"] = df5.index.dayofweek
    df5["is_ny_open"] = ((df5["hour"] >= 13) & (df5["hour"] <= 16)).astype(int)

    df5["target"] = (
        df5["close"].shift(-TARGET_CANDLES_AHEAD) >= df5["close"]
    ).astype(int)
    df5 = df5.dropna(subset=FEATURES + ["target"])
    return df5


def plot_feature_distributions(df5: pd.DataFrame, feats: list[str]) -> Path:
    fig, axes = _subplot_grid(len(feats))
    plt.style.use("dark_background")
    sns.set_palette("husl")
    for i, feat in enumerate(feats):
        lo, hi = df5[feat].quantile([0.01, 0.99])
        data = df5[feat].clip(lo, hi)
        axes[i].hist(data, bins=80, alpha=0.85, edgecolor="none", color="steelblue")
        axes[i].set_title(feat, fontsize=10, fontweight="bold")
        axes[i].set_yticks([])
        axes[i].grid(alpha=0.2)
        axes[i].axvline(data.mean(), color="yellow", linewidth=1.5, linestyle="--", alpha=0.8)
    fig.suptitle(
        "Feature Distributions | Yellow = mean",
        fontsize=13,
        y=1.02,
    )
    fig.tight_layout()
    return _save(fig, "05_feature_distributions.png")


def plot_feature_correlation(df5: pd.DataFrame, feats: list[str]) -> Path:
    corr = df5[feats].corr()
    fig, ax = plt.subplots(figsize=(13, 10))
    plt.style.use("dark_background")
    mask = np.zeros_like(corr, dtype=bool)
    np.fill_diagonal(mask, True)
    sns.heatmap(
        corr,
        annot=True,
        fmt=".2f",
        cmap="RdYlGn",
        vmin=-1,
        vmax=1,
        ax=ax,
        linewidths=0.3,
        square=True,
        annot_kws={"size": 7},
        mask=mask,
    )
    ax.set_title(
        "Feature correlation (off-diagonal masked)",
        fontsize=13,
        pad=20,
    )
    fig.tight_layout()
    return _save(fig, "06_feature_correlation.png")


def plot_up_vs_down(df5: pd.DataFrame, feats: list[str]) -> Path:
    up = df5[df5.target == 1]
    down = df5[df5.target == 0]
    fig, axes = _subplot_grid(len(feats))
    plt.style.use("dark_background")
    for i, feat in enumerate(feats):
        lo, hi = df5[feat].quantile([0.01, 0.99])
        bins = np.linspace(lo, hi, 60)
        axes[i].hist(
            up[feat].clip(lo, hi),
            bins=bins,
            alpha=0.55,
            color="lime",
            label="UP",
            density=True,
        )
        axes[i].hist(
            down[feat].clip(lo, hi),
            bins=bins,
            alpha=0.55,
            color="red",
            label="DOWN",
            density=True,
        )
        ks_stat, _ = stats.ks_2samp(up[feat].dropna(), down[feat].dropna())
        ks_stat = float(ks_stat)
        title_color = "lime" if ks_stat > 0.05 else ("yellow" if ks_stat > 0.02 else "tomato")
        sig_label = "STRONG" if ks_stat > 0.05 else ("weak" if ks_stat > 0.02 else "none")
        axes[i].set_title(
            f"{feat}\nKS={ks_stat:.3f} {sig_label}",
            fontsize=8.5,
            color=title_color,
        )
        axes[i].set_yticks([])
        axes[i].grid(alpha=0.15)
    axes[0].legend(fontsize=9, loc="upper right")
    fig.suptitle("UP vs DOWN (15m horizon)", fontsize=13, y=1.03)
    fig.tight_layout()
    return _save(fig, "07_up_vs_down.png")


def train_boosting(df5: pd.DataFrame, feats: list[str]) -> tuple[float, dict[str, float], str]:
    """LightGBM si está disponible; si no (p. ej. falta libomp), HistGradientBoosting."""
    X = df5[feats]
    y = df5["target"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    try:
        import lightgbm as lgb  # type: ignore[import-untyped]

        train_set = lgb.Dataset(X_train, label=y_train, feature_name=feats)
        test_set = lgb.Dataset(X_test, label=y_test, reference=train_set, feature_name=feats)
        params = {
            "objective": "binary",
            "metric": "binary_logloss",
            "verbosity": -1,
            "learning_rate": 0.05,
            "num_leaves": 31,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
            "seed": 42,
        }
        bst = lgb.train(
            params,
            train_set,
            num_boost_round=400,
            valid_sets=[test_set],
            callbacks=[lgb.early_stopping(40, verbose=False)],
        )
        proba = bst.predict(X_test, num_iteration=bst.best_iteration)
        pred = (proba >= 0.5).astype(int)
        acc = float((pred == y_test).mean())
        imp_raw = bst.feature_importance(importance_type="gain")
        imp = {feats[i]: float(imp_raw[i]) for i in range(len(feats))}
        return acc, imp, f"lightgbm (best_iter={bst.best_iteration})"
    except (OSError, ImportError):
        clf = HistGradientBoostingClassifier(
            max_iter=300,
            learning_rate=0.05,
            random_state=42,
            early_stopping=True,
            validation_fraction=0.1,
            n_iter_no_change=20,
        )
        clf.fit(X_train, y_train)
        proba = clf.predict_proba(X_test)[:, 1]
        pred = (proba >= 0.5).astype(int)
        acc = float((pred == y_test).mean())
        n_sub = min(8000, len(X_test))
        y_sub = y_test.iloc[:n_sub] if hasattr(y_test, "iloc") else y_test[:n_sub]
        r = permutation_importance(
            clf,
            X_test.iloc[:n_sub],
            y_sub,
            n_repeats=3,
            random_state=42,
            scoring="roc_auc",
        )
        imp = {feats[i]: float(r.importances_mean[i]) for i in range(len(feats))}
        return acc, imp, "sklearn HistGradientBoosting (fallback — instala libomp para LightGBM)"


def plot_feature_importance(imp: dict[str, float]) -> Path:
    s = pd.Series(imp).sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    plt.style.use("dark_background")
    s.plot(kind="barh", ax=ax, color="steelblue", edgecolor="none")
    ax.set_title("LightGBM feature importance (gain)", fontsize=12)
    ax.grid(alpha=0.2, axis="x")
    fig.tight_layout()
    return _save(fig, "08_feature_importance.png")


def best_hours(df5: pd.DataFrame) -> tuple[list[int], int]:
    hourly = df5.groupby("hour").agg(
        predictability=("target", lambda x: abs(float(x.mean()) - 0.5) * 2),
    )
    best = (
        hourly.nlargest(6, "predictability")
        .sort_index()
        .index.astype(int)
        .tolist()
    )
    worst_h = int(hourly["predictability"].idxmin())
    return best, worst_h


def print_ks_table(signal_scores: dict[str, float], title: str = "KS por feature (UP vs DOWN)") -> None:
    table = Table(title=title)
    table.add_column("Feature")
    table.add_column("KS", justify="right")
    table.add_column("Nota", justify="left")
    for feat, ks in sorted(signal_scores.items(), key=lambda x: -x[1]):
        if ks > 0.05:
            note = "fuerte"
        elif ks > 0.02:
            note = "débil"
        else:
            note = "—"
        table.add_row(feat, f"{ks:.4f}", note)
    console.print(table)


def final_verdict_panel(
    asset: str,
    acc: float,
    signal_scores: dict[str, float],
    best_h: list[int],
    worst_h: int,
) -> None:
    edge = acc - 0.5
    n_strong = sum(1 for v in signal_scores.values() if v > 0.05)
    top_feats = sorted(signal_scores.items(), key=lambda x: -x[1])[:5]
    top_txt = "\n".join(f"  • {k}: KS={v:.4f}" for k, v in top_feats)

    if acc <= 0.50:
        label = "STOP"
        emoji = "❌"
        style = "red"
        body = "Accuracy ≤ 50%: no hay ventaja sobre el azar."
    elif acc > 0.51 and n_strong >= 3:
        label = "PROCEED"
        emoji = "✅"
        style = "green"
        body = "Accuracy > 51% y ≥3 features con KS>0.05 — razonable pasar a train.py."
    else:
        label = "MARGINAL"
        emoji = "⚠️"
        style = "yellow"
        parts: list[str] = []
        if acc <= 0.51:
            parts.append("el accuracy no supera el 51%")
        if n_strong < 3:
            parts.append(f"solo {n_strong} feature(s) con KS>0.05 (objetivo ≥3)")
        body = ("; ".join(parts) + " — rediseñar features o validación antes de train.").strip()

    text = f"""[bold]{emoji} {label}[/bold]

[bold]Activo:[/bold] {asset}
[bold]Holdout accuracy:[/bold] {acc:.2%}
[bold]Edge (acc − 50%):[/bold] {edge:+.2%}
[bold]Features KS>0.05:[/bold] {n_strong}

[bold]Mejores features (KS):[/bold]
{top_txt}

[bold]Mejores horas UTC (predictability):[/bold] {best_h}
[bold]Peor hora UTC:[/bold] {worst_h}:00

{body}
"""
    console.print(Panel.fit(text.strip(), title="Veredicto señal / modelo", border_style=style))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Análisis de features y LightGBM.")
    p.add_argument(
        "--asset",
        default="BTCUSDT",
        help="Símbolo, ej. BTCUSDT o ETHUSDT",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    asset = str(args.asset).strip().upper()
    if not asset.endswith("USDT"):
        asset = f"{asset}USDT"

    plt.style.use("dark_background")
    sns.set_palette("husl")
    pd.set_option("display.float_format", "{:.6f}".format)

    console.print(f"[cyan]Asset[/cyan] {asset} | horizonte {TARGET_CANDLES_AHEAD * 5} min")

    df5 = load_df5(asset)
    console.print(f"Filas 5m tras features: {len(df5):,} | target UP: {df5['target'].mean():.1%}")

    signal_scores = compute_ks_scores(df5, FEATURES)
    assert signal_scores["ret_24"] > signal_scores["ret_1"], (
        f"ret_24 KS ({signal_scores['ret_24']:.4f}) debe ser > ret_1 ({signal_scores['ret_1']:.4f})"
    )
    assert signal_scores["ret_48"] > signal_scores["ret_1"], (
        f"ret_48 KS ({signal_scores['ret_48']:.4f}) debe ser > ret_1 ({signal_scores['ret_1']:.4f})"
    )
    console.print("[green]OK[/green] ret_24 y ret_48 tienen KS mayor que ret_1")

    dropped_ks = [f for f in FEATURES if signal_scores[f] < KS_MIN]
    feats_active = [f for f in FEATURES if signal_scores[f] >= KS_MIN]
    if dropped_ks:
        console.print(f"[yellow]Excluidas por KS < {KS_MIN}:[/yellow] {', '.join(dropped_ks)}")
    assert all(signal_scores[f] >= KS_MIN for f in feats_active)
    assert not any(signal_scores[f] < KS_MIN for f in feats_active)

    feats_final = remove_high_correlation(feats_active, df5, signal_scores)
    max_c = max_offdiag_abs_corr(feats_final, df5)
    assert max_c <= CORR_MAX, f"|corr| máx entre pares = {max_c:.3f} > {CORR_MAX}"
    console.print(f"[green]OK[/green] |corr| máximo off-diagonal en lista final: {max_c:.3f} (≤ {CORR_MAX})")

    p5 = plot_feature_distributions(df5, feats_final)
    console.print(f"[green]OK[/green] {p5}")
    p6 = plot_feature_correlation(df5, feats_final)
    console.print(f"[green]OK[/green] {p6}")
    p7 = plot_up_vs_down(df5, feats_final)
    console.print(f"[green]OK[/green] {p7}")

    scores_final = {f: signal_scores[f] for f in feats_final}
    acc, imp, backend_note = train_boosting(df5, feats_final)
    p8 = plot_feature_importance(imp)
    console.print(f"[green]OK[/green] {p8}")

    print_ks_table(
        scores_final,
        title=f"Signal ranking — final (KS≥{KS_MIN}, |corr|≤{CORR_MAX})",
    )
    best_h, worst_h = best_hours(df5)

    console.print(f"\n[bold]Accuracy holdout:[/bold] {acc:.2%} | [dim]{backend_note}[/dim]")
    console.print(f"[bold]Mejores horas UTC:[/bold] {best_h}")

    final_verdict_panel(asset, acc, scores_final, best_h, worst_h)
    console.print(
        f"\n[dim]Gráficos en {REPORTS_DIR} — listo.[/dim]"
    )


if __name__ == "__main__":
    main()
