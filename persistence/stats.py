"""Contadores simples de escrituras Postgres (observabilidad)."""

from __future__ import annotations

import threading
from typing import Any

_lock = threading.Lock()
_writes_ok = 0
_writes_fail = 0
_last_error: str | None = None


def record_write_ok() -> None:
    global _writes_ok
    with _lock:
        _writes_ok += 1


def record_write_fail(exc: BaseException) -> None:
    global _writes_fail, _last_error
    with _lock:
        _writes_fail += 1
        _last_error = f"{type(exc).__name__}: {exc}"[:500]


def snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "postgres_writes_ok": _writes_ok,
            "postgres_writes_fail": _writes_fail,
            "postgres_last_write_error": _last_error,
        }
