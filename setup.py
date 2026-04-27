#!/usr/bin/env python3
"""
Bootstrap del entorno: NO es setuptools. No usar `pip install .` con este archivo como paquete.
Ejecutar: python setup.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
VENV_DIR = REPO_ROOT / ".venv"

PACKAGES = [
    "rich",
    "pandas",
    "pyarrow",
    "tqdm",
    "requests",
    "python-dotenv",
    "jupyterlab",
    "matplotlib",
    "seaborn",
    "duckdb",
    "lightgbm",
    "scikit-learn",
    "scipy",
    "numpy",
]


def require_python_version() -> None:
    """Alineado con pyproject.toml (>=3.11): deps del arb y py-order-utils no soportan 3.9.x antiguo."""
    if sys.version_info < (3, 11):
        print(
            f"❌ Este repo requiere Python >= 3.11 (tienes {sys.version_info.major}."
            f"{sys.version_info.minor}.{sys.version_info.micro}).\n"
            "   Instala 3.11+ (pyenv, asdf, brew, uv) y vuelve a ejecutar: python setup.py\n",
            file=sys.stderr,
        )
        sys.exit(1)


def venv_pip_python() -> tuple[Path, Path]:
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "pip.exe", VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "pip", VENV_DIR / "bin" / "python"


def run(cmd: list[str]) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=REPO_ROOT)


def main() -> None:
    require_python_version()

    if not VENV_DIR.is_dir():
        run([sys.executable, "-m", "venv", str(VENV_DIR)])
        print(f"✅ Entorno virtual creado en {VENV_DIR}\n")
    else:
        print(f"✅ Entorno virtual ya existe: {VENV_DIR}\n")

    pip_exe, _py_exe = venv_pip_python()
    if not pip_exe.is_file():
        print(f"❌ No se encontró pip en {pip_exe}", file=sys.stderr)
        sys.exit(1)

    run([str(pip_exe), "install", "--upgrade", "pip"])
    run([str(pip_exe), "install", *PACKAGES])

    req_path = REPO_ROOT / "requirements.txt"
    with req_path.open("w", encoding="utf-8") as f:
        subprocess.run([str(pip_exe), "freeze"], check=True, stdout=f, cwd=REPO_ROOT)
    print(f"\n✅ Dependencias congeladas en {req_path}\n")

    print(
        "✅ Setup complete!\n\n"
        "To activate your environment:\n"
        "  source .venv/bin/activate   (Mac/Linux)\n"
        "  .venv\\Scripts\\activate     (Windows)\n\n"
        "To download data:\n"
        "  python download_datasets.py\n\n"
        "To explore data in Jupyter:\n"
        "  jupyter lab\n"
    )


if __name__ == "__main__":
    main()
