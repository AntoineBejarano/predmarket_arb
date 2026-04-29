#!/usr/bin/env python3
"""
Bootstrap del entorno: NO es setuptools. No usar `pip install .` con este archivo como paquete.

Lee dependencias desde pyproject.toml (runtime + tool.uv dev-dependencies) y las instala en .venv.

Ejecutar: python3.12 setup.py
Opcional: python3.12 setup.py --no-dev   (solo [project].dependencies, sin Jupyter/etc.)
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
VENV_DIR = REPO_ROOT / ".venv"
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"


def load_pip_requirements_from_pyproject(*, include_dev: bool) -> list[str]:
    """Requisitos alineados con pyproject.toml (una sola fuente de verdad)."""
    import tomllib  # stdlib >=3.11; setup ya exige intérprete >=3.11 antes de llamar aquí

    with PYPROJECT_PATH.open("rb") as f:
        data = tomllib.load(f)
    out: list[str] = []
    project = data.get("project") or {}
    for x in project.get("dependencies") or []:
        if isinstance(x, str) and x.strip():
            out.append(x.strip())
    if include_dev:
        uv = (data.get("tool") or {}).get("uv") or {}
        for x in uv.get("dev-dependencies") or []:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
    return out


def require_python_version() -> None:
    """Alineado con pyproject.toml (>=3.11): deps del arb y py-order-utils no soportan 3.9.x antiguo."""
    if sys.version_info < (3, 11):
        print(
            f"❌ Este repo requiere Python >= 3.11 (tienes {sys.version_info.major}."
            f"{sys.version_info.minor}.{sys.version_info.micro}).\n"
            "   Instala 3.12 (recomendado) o 3.11, luego ejecuta por ejemplo:\n"
            "     python3.12 setup.py\n"
            "   (pyenv/asdf: `pyenv install` según .python-version; Homebrew: brew install python@3.12)\n",
            file=sys.stderr,
        )
        sys.exit(1)


def _interpreter_version_tuple(python_exe: str) -> tuple[int, int, int]:
    out = subprocess.run(
        [python_exe, "-c", "import sys; print(sys.version_info[0], sys.version_info[1], sys.version_info[2])"],
        capture_output=True,
        text=True,
        check=True,
        cwd=REPO_ROOT,
    )
    a, b, c = (int(x) for x in out.stdout.strip().split())
    return a, b, c


def pick_venv_python_executable() -> str:
    """Prefiere 3.12 > 3.11 > intérprete actual (si ya es >= 3.11).

    Incluye rutas típicas de Homebrew por si `python3.12` no está en PATH (zsh nuevo, SSH, etc.).
    """
    candidates: list[str] = []
    for fixed in (
        "/opt/homebrew/bin/python3.12",
        "/opt/homebrew/bin/python3.11",
        "/usr/local/bin/python3.12",
        "/usr/local/bin/python3.11",
    ):
        if Path(fixed).is_file():
            candidates.append(fixed)
    for cmd in ("python3.12", "python3.11"):
        path = shutil.which(cmd)
        if path and path not in candidates:
            candidates.append(path)
    for path in candidates:
        try:
            if _interpreter_version_tuple(path) >= (3, 11, 0):
                return path
        except (subprocess.CalledProcessError, OSError, ValueError):
            continue
    if sys.version_info >= (3, 11):
        return sys.executable
    return ""


def ensure_venv_python_ok() -> None:
    """Si .venv existe pero es < 3.11, obligar a recrear (evita .venv creado con python3.9)."""
    _, py_exe = venv_pip_python()
    if not py_exe.is_file():
        return
    try:
        maj, min_, _ = _interpreter_version_tuple(str(py_exe))
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        print(f"❌ No se pudo leer la versión de {py_exe}: {e}", file=sys.stderr)
        sys.exit(1)
    if (maj, min_) < (3, 11):
        print(
            f"❌ El .venv actual usa Python {maj}.{min_} (< 3.11). Elimínalo y vuelve a ejecutar setup con 3.12+:\n"
            f"   rm -rf {VENV_DIR}\n"
            "   python3.12 setup.py\n",
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
    parser = argparse.ArgumentParser(description="Crea .venv e instala deps desde pyproject.toml")
    parser.add_argument(
        "--no-dev",
        action="store_true",
        help="Omitir tool.uv dev-dependencies (jupyterlab, matplotlib, seaborn, duckdb)",
    )
    args = parser.parse_args()

    require_python_version()

    if VENV_DIR.is_dir():
        ensure_venv_python_ok()
        print(f"✅ Entorno virtual ya existe: {VENV_DIR}\n")
    else:
        py_for_venv = pick_venv_python_executable()
        if not py_for_venv:
            print(
                "❌ No se encontró python3.12 ni python3.11 en PATH y el intérprete actual no sirve.\n"
                "   Instala Python 3.12 y ejecuta: python3.12 setup.py\n",
                file=sys.stderr,
            )
            sys.exit(1)
        run([py_for_venv, "-m", "venv", str(VENV_DIR)])
        print(f"✅ Entorno virtual creado con `{py_for_venv}` en {VENV_DIR}\n")

    pip_exe, _py_exe = venv_pip_python()
    if not pip_exe.is_file():
        print(f"❌ No se encontró pip en {pip_exe}", file=sys.stderr)
        sys.exit(1)

    if not PYPROJECT_PATH.is_file():
        print(f"❌ No se encuentra {PYPROJECT_PATH}", file=sys.stderr)
        sys.exit(1)

    reqs = load_pip_requirements_from_pyproject(include_dev=not args.no_dev)
    if not reqs:
        print("❌ pyproject.toml no define dependencies instalables.", file=sys.stderr)
        sys.exit(1)

    run([str(pip_exe), "install", "--upgrade", "pip"])
    print(f"   → {len(reqs)} paquetes desde pyproject.toml" + (" (sin dev)" if args.no_dev else " + dev") + "\n")
    run([str(pip_exe), "install", *reqs])

    req_path = REPO_ROOT / "requirements.txt"
    with req_path.open("w", encoding="utf-8") as f:
        subprocess.run([str(pip_exe), "freeze"], check=True, stdout=f, cwd=REPO_ROOT)
    print(f"\n✅ requirements.txt actualizado ({req_path})\n")

    print(
        "✅ Listo.\n\n"
        "Activar entorno:\n"
        "  source .venv/bin/activate    # Mac/Linux\n"
        "  .venv\\Scripts\\activate       # Windows\n\n"
        "API + dashboards:\n"
        "  ENABLE_EXPERIMENTAL=true AUTO_START=false PORT=8080 python scripts/api.py\n\n"
        "Datos de ejemplo:\n"
        "  python download_datasets.py\n\n"
        + ("" if args.no_dev else "Jupyter:\n  jupyter lab\n\n")
    )


if __name__ == "__main__":
    main()
