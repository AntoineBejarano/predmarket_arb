#!/usr/bin/env python3
"""Análisis agregado del CSV sixcycle (crypto_5m_sixcycle)."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    raw = os.environ.get("PM_REPO_ROOT", "").strip()
    if raw:
        p = Path(raw).expanduser().resolve()
        if (p / "clients").is_dir():
            return p
    here = Path(__file__).resolve()
    cand = here.parent.parent
    if (cand / "clients").is_dir():
        return cand
    return cand


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
from rich.console import Console
from rich.table import Table

from lab.paths import data_dir  # noqa: E402

REQUIRED_COLS = {
    "timestamp_utc",
    "phase",
    "clob_yes_price",
    "resolved",
    "pnl_usdc",
    "win",
    "scorer_confirms",
    "minutes_elapsed",
}


def _parse_bool_series(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.lower().isin(("true", "1", "yes"))
    return out


def _win_loss_streaks(win_bool: list[bool]) -> tuple[int, int]:
    max_w = max_l = w = l = 0
    for ok in win_bool:
        if ok:
            w += 1
            l = 0
            max_w = max(max_w, w)
        else:
            l += 1
            w = 0
            max_l = max(max_l, l)
    return max_w, max_l


def main() -> None:
    default_csv = data_dir() / "logs" / "crypto_5m_sixcycle.csv"
    ap = argparse.ArgumentParser(description="Estadísticas sixcycle desde CSV.")
    ap.add_argument(
        "csv_path",
        nargs="?",
        type=Path,
        default=default_csv,
        help=f"Ruta al CSV (default: {default_csv})",
    )
    args = ap.parse_args()
    path: Path = args.csv_path.expanduser().resolve()
    console = Console()

    if not path.is_file():
        console.print(f"[red]No existe el fichero:[/] {path}")
        sys.exit(1)

    df = pd.read_csv(path)
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        console.print(
            "[yellow]CSV antiguo o incompleto: faltan columnas[/] "
            + ", ".join(sorted(missing))
            + "\nSe necesita el esquema nuevo del sixcycle_engine (CSV_COLUMNS)."
        )
        sys.exit(2)

    settled = df[df["resolved"].astype(str).str.lower().isin(("win", "loss"))].copy()
    settled["win_bool"] = settled["resolved"].astype(str).str.lower() == "win"
    settled["ts"] = pd.to_datetime(settled["timestamp_utc"], utc=True, errors="coerce")
    settled = settled.sort_values("ts")

    if settled.empty:
        console.print("[yellow]No hay filas SETTLED con resolved win/loss.[/]")
        sys.exit(0)

    yes = pd.to_numeric(settled["clob_yes_price"], errors="coerce")

    def bucket(y: float) -> str:
        if pd.isna(y):
            return "NA"
        if 0.65 <= y < 0.70:
            return "65-70%"
        if 0.70 <= y < 0.75:
            return "70-75%"
        if 0.75 <= y < 0.80:
            return "75-80%"
        if y >= 0.80:
            return ">80%"
        return "other"

    settled["clob_bucket"] = yes.map(bucket)
    confirms = _parse_bool_series(settled["scorer_confirms"])
    settled["confirms"] = confirms

    console.print(f"[bold]Sixcycle signals[/]  n_settled={len(settled)}  fichero={path}\n")

    # Win rate por rango CLOB (solo filas con YES en tramo alto; "other" agrupa el resto)
    t1 = Table(title="Win rate por CLOB YES (entrada)", show_lines=True)
    t1.add_column("Rango YES")
    t1.add_column("n")
    t1.add_column("win_rate")
    for label in ("65-70%", "70-75%", "75-80%", ">80%"):
        sub = settled[settled["clob_bucket"] == label]
        n = len(sub)
        wr = 100.0 * sub["win_bool"].mean() if n else 0.0
        t1.add_row(label, str(n), f"{wr:.1f}%" if n else "—")
    console.print(t1)

    t2 = Table(title="Win rate scorer_confirms", show_lines=True)
    t2.add_column("Grupo")
    t2.add_column("n")
    t2.add_column("win_rate")
    for lab, mask in (
        ("con confirmación", settled["confirms"]),
        ("sin confirmación", ~settled["confirms"]),
    ):
        sub = settled[mask]
        n = len(sub)
        wr = 100.0 * sub["win_bool"].mean() if n else 0.0
        t2.add_row(lab, str(n), f"{wr:.1f}%" if n else "—")
    console.print(t2)

    mins = pd.to_numeric(settled["minutes_elapsed"], errors="coerce").fillna(-1).astype(int).clip(0, 4)
    settled["min_bin"] = mins
    t3 = Table(title="Win rate por minuto (floor minutes_elapsed)", show_lines=True)
    t3.add_column("min")
    t3.add_column("n")
    t3.add_column("win_rate")
    for m in range(0, 5):
        sub = settled[settled["min_bin"] == m]
        n = len(sub)
        wr = 100.0 * sub["win_bool"].mean() if n else 0.0
        t3.add_row(str(m), str(n), f"{wr:.1f}%" if n else "—")
    console.print(t3)

    settled["day_utc"] = settled["ts"].dt.strftime("%Y-%m-%d")
    pnl = pd.to_numeric(settled["pnl_usdc"], errors="coerce").fillna(0.0)
    settled["_pnl"] = pnl
    day_pnl = settled.groupby("day_utc", as_index=False)["_pnl"].sum()
    t4 = Table(title="PnL por día (UTC)", show_lines=True)
    t4.add_column("día")
    t4.add_column("PnL USDC")
    for _, r in day_pnl.iterrows():
        t4.add_row(str(r["day_utc"]), f"{float(r['_pnl']):+.4f}")
    console.print(t4)

    seq = settled["win_bool"].tolist()
    best_w, worst_l = _win_loss_streaks(seq)
    console.print(f"[bold]Mejor racha W:[/] {best_w}  [bold]Peor racha L:[/] {worst_l}\n")

    t5 = Table(title="Últimos 20 trades (settled)", show_lines=True)
    for c in ("ts", "market_slug", "direction", "clob_yes_price", "edge", "resolved", "pnl_usdc", "win"):
        t5.add_column(c)
    tail = settled.tail(20).iloc[::-1]
    for _, r in tail.iterrows():
        wv = r.get("win", "")
        t5.add_row(
            str(r["timestamp_utc"])[:19],
            str(r.get("market_slug", "") or "")[:24],
            str(r.get("direction", "")),
            str(r.get("clob_yes_price", "")),
            str(r.get("edge", "")),
            str(r.get("resolved", "")),
            str(r.get("pnl_usdc", "")),
            str(wv),
        )
    console.print(t5)


if __name__ == "__main__":
    main()
