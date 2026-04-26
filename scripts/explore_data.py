#!/usr/bin/env python3
"""Exploración de parquets OHLCV (equivalente a 01_explore_data.ipynb)."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import duckdb  # noqa: E402
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "raw"
REPORTS_DIR = REPO_ROOT / "reports"

ASSETS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "BNBUSDT",
    "HYPEUSDT",
]

console = Console()


def _save(fig: plt.Figure, name: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def build_summary() -> tuple[pd.DataFrame, list[str]]:
    rows: list[dict[str, object]] = []
    missing: list[str] = []
    for asset in ASSETS:
        path = DATA_DIR / f"{asset}_1min.parquet"
        if not path.is_file():
            missing.append(asset)
            continue
        df = pd.read_parquet(path, columns=["timestamp", "close", "volume"])
        rows.append(
            {
                "Asset": asset.replace("USDT", ""),
                "Rows": len(df),
                "Start": df["timestamp"].min().strftime("%Y-%m-%d"),
                "End": df["timestamp"].max().strftime("%Y-%m-%d"),
                "Size MB": path.stat().st_size / 1e6,
                "Nulls": int(df.isnull().sum().sum()),
            }
        )
    if not rows:
        return pd.DataFrame(), missing
    return pd.DataFrame(rows).set_index("Asset"), missing


def print_summary_rich(summary: pd.DataFrame, missing: list[str]) -> None:
    if missing:
        console.print(
            f"[yellow]Parquets no encontrados ({len(missing)}):[/yellow] "
            + ", ".join(missing)
        )
    if summary.empty:
        console.print("[red]No hay datos en data/raw/. Ejecuta download_datasets.py[/red]")
        return
    table = Table(title="Resumen OHLCV (1m)")
    for col in summary.reset_index().columns:
        table.add_column(col, justify="right" if col != "Asset" else "left")
    for idx, row in summary.reset_index().iterrows():
        table.add_row(
            str(row["Asset"]),
            f"{int(row['Rows']):,}",
            str(row["Start"]),
            str(row["End"]),
            f"{row['Size MB']:.0f}",
            str(int(row["Nulls"])),
        )
    console.print(table)


def plot_price_history(available: list[str]) -> Path | None:
    if not available:
        return None
    use = [a for a in ASSETS[:6] if a in available]
    if not use:
        return None
    fig, axes = plt.subplots(3, 2, figsize=(16, 12))
    axes = axes.flatten()
    plt.style.use("dark_background")
    sns.set_palette("husl")
    for i, asset in enumerate(use):
        path = DATA_DIR / f"{asset}_1min.parquet"
        df = pd.read_parquet(path, columns=["timestamp", "close"])
        daily = df.set_index("timestamp")["close"].resample("1D").last().dropna()
        if daily.empty:
            continue
        normalized = (daily / daily.iloc[0]) * 100
        axes[i].plot(normalized.index, normalized.values, linewidth=1)
        axes[i].set_title(asset.replace("USDT", ""), fontsize=14, fontweight="bold")
        axes[i].set_ylabel("Indexed (start=100)")
        axes[i].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        axes[i].grid(alpha=0.3)
    for j in range(len(use), 6):
        axes[j].set_visible(False)
    fig.suptitle("Price History — Normalized to 100 at Start", fontsize=16, y=1.02)
    fig.tight_layout()
    return _save(fig, "01_price_history.png")


def collect_gap_stats(available: list[str]) -> pd.DataFrame:
    rec: list[dict[str, object]] = []
    for asset in available:
        path = DATA_DIR / f"{asset}_1min.parquet"
        df = pd.read_parquet(path, columns=["timestamp"])
        gaps = df["timestamp"].diff()
        rec.append(
            {
                "asset": asset.replace("USDT", ""),
                "gap_5min": int((gaps > pd.Timedelta("5min")).sum()),
                "gap_1h": int((gaps > pd.Timedelta("1h")).sum()),
                "gap_1day": int((gaps > pd.Timedelta("1D")).sum()),
            }
        )
    return pd.DataFrame(rec)


def plot_gap_analysis(gap_df: pd.DataFrame) -> Path | None:
    if gap_df.empty:
        return None
    plt.style.use("dark_background")
    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(gap_df))
    w = 0.25
    ax.bar(x - w, gap_df["gap_5min"], width=w, label="> 5 min", color="steelblue")
    ax.bar(x, gap_df["gap_1h"], width=w, label="> 1 h", color="darkorange")
    ax.bar(x + w, gap_df["gap_1day"], width=w, label="> 1 day", color="crimson")
    ax.set_xticks(x)
    ax.set_xticklabels(gap_df["asset"])
    ax.set_ylabel("Count")
    ax.set_title("Gap analysis — timestamp diffs")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    return _save(fig, "02_gap_analysis.png")


def plot_volume_by_hour(available: list[str]) -> Path | None:
    use = [a for a in ASSETS[:6] if a in available]
    if not use:
        return None
    plt.style.use("dark_background")
    fig, ax = plt.subplots(figsize=(12, 6))
    for asset in use:
        path = DATA_DIR / f"{asset}_1min.parquet"
        df = pd.read_parquet(path, columns=["timestamp", "volume"])
        df["hour"] = pd.to_datetime(df["timestamp"], utc=True).dt.hour
        hourly = df.groupby("hour")["volume"].mean()
        ax.plot(hourly.index, hourly.values, marker="o", markersize=3, label=asset.replace("USDT", ""))
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlabel("Hour UTC")
    ax.set_ylabel("Mean volume (1m bars)")
    ax.set_title("Mean volume by hour UTC")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    return _save(fig, "03_volume_by_hour.png")


def plot_correlation(available: list[str]) -> Path | None:
    """Correlación de retornos diarios entre activos (si hay 2+); si no, correlación OHLC diarios de un solo activo."""
    plt.style.use("dark_background")
    daily_rets: dict[str, pd.Series] = {}
    for asset in available:
        path = DATA_DIR / f"{asset}_1min.parquet"
        df = pd.read_parquet(path, columns=["timestamp", "close"])
        daily = df.set_index("timestamp")["close"].resample("1D").last().dropna()
        daily_rets[asset.replace("USDT", "")] = np.log(daily / daily.shift(1)).dropna()
    if len(daily_rets) >= 2:
        wide = pd.DataFrame(daily_rets).dropna()
        corr = wide.corr()
    elif len(daily_rets) == 1:
        path = DATA_DIR / f"{next(a for a in available)}_1min.parquet"
        df = pd.read_parquet(path, columns=["timestamp", "open", "high", "low", "close"])
        d = (
            df.set_index("timestamp")
            .resample("1D")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
            .dropna()
        )
        corr = np.log(d / d.shift(1)).dropna().corr()
        name = f"{name} daily log-returns (OHLC)"
    else:
        return None
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="RdYlGn", vmin=-1, vmax=1, ax=ax, square=True)
    ax.set_title("Correlation matrix")
    fig.tight_layout()
    return _save(fig, "04_correlation.png")


def volatile_days_table(asset: str = "BTCUSDT") -> pd.DataFrame | None:
    """Top días volátiles (misma lógica que el notebook DuckDB)."""
    path = DATA_DIR / f"{asset}_1min.parquet"
    if not path.is_file():
        return None
    p = path.resolve().as_posix().replace("'", "''")
    q = f"""
    SELECT
        strftime(timestamp, '%Y-%m-%d') AS day,
        round(max(high) - min(low), 2) AS range_usd,
        round(max(high)/min(low) - 1, 4) AS range_pct,
        round(sum(volume), 0) AS total_volume
    FROM read_parquet('{p}')
    GROUP BY day
    ORDER BY range_pct DESC
    LIMIT 10
    """
    return duckdb.query(q).df()


def print_volatile_rich(df: pd.DataFrame | None, asset: str) -> None:
    if df is None:
        return
    table = Table(title=f"Top 10 días más volátiles ({asset})")
    for c in df.columns:
        table.add_column(str(c))
    for _, row in df.iterrows():
        table.add_row(*[str(v) for v in row])
    console.print(table)


def verdict_panel(summary: pd.DataFrame, gap_df: pd.DataFrame, missing: list[str]) -> None:
    if summary.empty:
        console.print(
            Panel.fit(
                "[bold red]NO DATA[/bold red]\nNo hay parquets en data/raw/.",
                title="Calidad de datos",
                border_style="red",
            )
        )
        return
    n_assets = len(summary)
    bad_gaps = 0
    if not gap_df.empty:
        bad_gaps = int((gap_df["gap_5min"] >= 100).sum())
    if missing:
        status_txt = "PARCIAL"
        detail = f"Faltan {len(missing)} activos. Completar descarga para cobertura plena."
        border_style = "yellow"
    elif bad_gaps == 0:
        status_txt = "BUENA"
        detail = "Todos los activos presentes; pocos huecos >5 min en el umbral del notebook."
        border_style = "green"
    else:
        status_txt = "REVISAR"
        detail = f"{bad_gaps} activo(s) con ≥100 huecos >5 min — revisar calidad o ventanas de mantenimiento."
        border_style = "yellow"

    text = f"""[bold]{status_txt}[/bold]

Activos con datos: {n_assets}
Filas totales (suma): {int(summary['Rows'].sum()):,}
{detail}

Gráficos guardados en [cyan]{REPORTS_DIR}[/cyan]
"""
    console.print(
        Panel.fit(text.strip(), title="Veredicto — calidad global", border_style=border_style)
    )


def main() -> None:
    plt.style.use("dark_background")
    sns.set_palette("husl")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    summary, missing = build_summary()
    print_summary_rich(summary, missing)

    available = [a for a in ASSETS if (DATA_DIR / f"{a}_1min.parquet").is_file()]
    p1 = plot_price_history(available)
    gap_df = collect_gap_stats(available) if available else pd.DataFrame()
    p2 = plot_gap_analysis(gap_df)
    p3 = plot_volume_by_hour(available)
    p4 = plot_correlation(available)

    for label, p in [
        ("01_price_history.png", p1),
        ("02_gap_analysis.png", p2),
        ("03_volume_by_hour.png", p3),
        ("04_correlation.png", p4),
    ]:
        if p:
            console.print(f"[green]OK[/green] {p}")
        else:
            console.print(f"[dim]skip[/dim] {label} (sin datos)")

    primary = "BTCUSDT" if "BTCUSDT" in available else (available[0] if available else "BTCUSDT")
    vol_df = volatile_days_table(primary)
    if vol_df is not None and available:
        print_volatile_rich(vol_df, primary)

    verdict_panel(summary, gap_df, missing)

    console.print(
        "\n[bold]Resumen:[/bold] exploración terminada. Revisa PNG en reports/ y tablas arriba."
    )


if __name__ == "__main__":
    main()
