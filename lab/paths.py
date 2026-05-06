"""Repository paths for strategy experiments (scalable layout)."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def data_dir() -> Path:
    """
    Datos runtime (CSV arb/validador, logs, cachés bajo ``…/data/logs``).

    - Si existe la variable de entorno ``DATA_DIR`` (ruta absoluta o relativa al repo), se usa
      tal cual en documentación (.env.example / Railway con volumen).
    - **Railway / imagen Docker** (`WORKDIR=/app`) sin ``DATA_DIR``: ``/app/data`` (montar volumen ahí).
    - **Local** sin ``DATA_DIR``: ``<repo>/data``.
    """
    override = os.environ.get("DATA_DIR", "").strip()
    if override:
        p = Path(override).expanduser()
        if not p.is_absolute():
            p = (REPO_ROOT / p).resolve()
    elif str(REPO_ROOT) == "/app":
        p = Path("/app/data")
    else:
        p = REPO_ROOT / "data"
    p.mkdir(parents=True, exist_ok=True)
    return p.resolve()


def strategy_experiment_dir(strategy_id: str, experiment_slug: str) -> Path:
    """Resolved path: strategies/<strategy_id>/experiments/<experiment_slug>/"""
    return REPO_ROOT / "strategies" / strategy_id / "experiments" / experiment_slug


def default_compact_experiment_dir() -> Path:
    """
    Default output directory for the crypto 5m compact exogenous pipeline.

    Override with env PM_STRATEGY_EXPERIMENT_DIR (absolute or repo-relative path).
    """
    override = os.environ.get("PM_STRATEGY_EXPERIMENT_DIR", "").strip()
    if override:
        p = Path(override)
        return p if p.is_absolute() else (REPO_ROOT / p).resolve()
    return strategy_experiment_dir("crypto_5m_updown", "exogenous_compact")
