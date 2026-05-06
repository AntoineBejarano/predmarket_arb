#!/usr/bin/env python3
"""
Alinea datos BTC: Poly (5m Up/Down) + Binance 1m en la misma ventana temporal.

1) Refresca ``poly_markets_resolved.parquet`` (solo BTC).
2) Calcula ventana [min(open_ts), max(close_ts)] con margen de 2 días.
3) Reemplaza ``BTCUSDT_1min.parquet`` con solo esos meses (modo ventana de download_datasets).
4) Borra parquets ``poly_aligned`` de otros activos (sobran si solo usas BTC).
5) Ejecuta ``build_poly_target.py --assets BTC``.

Los mercados ``btc-updown-5m-*`` suelen existir en Gamma solo desde ~finales de 2025;
no es un fallo del script si el histórico Poly es corto.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd
from rich.console import Console

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW = REPO_ROOT / "data" / "raw"
POLY = RAW / "poly_markets_resolved.parquet"
ALIGNED = RAW / "poly_aligned"

console = Console()


def run(cmd: list[str]) -> None:
    console.print("[dim]+[/dim]", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-poly", action="store_true", help="No ejecutar download_poly_history.py")
    p.add_argument("--skip-binance", action="store_true", help="No ejecutar download_datasets en modo ventana")
    p.add_argument("--skip-build", action="store_true", help="No ejecutar build_poly_target.py")
    p.add_argument(
        "--padding-days",
        type=int,
        default=2,
        help="Días extra antes del primer open_ts y después del último close_ts (default 2).",
    )
    args = p.parse_args()

    py = sys.executable

    if not args.skip_poly:
        run([py, str(REPO_ROOT / "scripts" / "download_poly_history.py"), "--assets", "BTC"])

    if not POLY.is_file():
        console.print(f"[red]Falta {POLY}[/red]")
        sys.exit(1)

    markets = pd.read_parquet(POLY)
    if markets.empty:
        console.print("[red]poly_markets_resolved.parquet está vacío.[/red]")
        sys.exit(1)

    ots = pd.to_datetime(markets["open_ts"], utc=True, errors="coerce")
    cts = pd.to_datetime(markets["close_ts"], utc=True, errors="coerce")
    if ots.isna().all() or cts.isna().all():
        console.print("[red]No hay open_ts/close_ts válidos en Poly.[/red]")
        sys.exit(1)

    om = ots.min()
    cm = cts.max()
    pad = pd.Timedelta(days=max(0, int(args.padding_days)))
    # om/cm ya vienen con tz (utc=True); no usar pd.Timestamp(..., tz=...) sobre un aware.
    s0 = (om - pad).tz_convert("UTC").strftime("%Y-%m-%d")
    s1 = (cm + pad).tz_convert("UTC").strftime("%Y-%m-%d")
    console.print(f"[bold]Ventana Binance (UTC)[/bold]: {s0} … {s1}  (Poly open min {om}, close max {cm})")

    if not args.skip_binance:
        run(
            [
                py,
                str(REPO_ROOT / "download_datasets.py"),
                "--assets",
                "BTC",
                "--window-start",
                s0,
                "--window-end",
                s1,
            ]
        )

    if ALIGNED.is_dir():
        for pth in ALIGNED.glob("*.parquet"):
            if pth.name != "BTCUSDT.parquet":
                try:
                    pth.unlink()
                    console.print(f"[dim]Eliminado[/dim] {pth.relative_to(REPO_ROOT)}")
                except OSError as e:
                    console.print(f"[yellow]No se pudo borrar {pth}: {e}[/yellow]")

    if not args.skip_build:
        run([py, str(REPO_ROOT / "scripts" / "build_poly_target.py"), "--assets", "BTC"])

    console.print("[green]Listo.[/green] Revisa data/raw/poly_aligned/BTCUSDT.parquet y filas > 0.")


if __name__ == "__main__":
    main()
