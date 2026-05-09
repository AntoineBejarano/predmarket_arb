"""Pool de conexiones Postgres (un pool por proceso)."""

from __future__ import annotations

import atexit
import logging
import threading
from typing import Any, Optional

from persistence.config import database_url

log = logging.getLogger("persistence.pool")

_pool: Optional[Any] = None
_pool_lock = threading.Lock()


def _close_pool() -> None:
    global _pool
    with _pool_lock:
        if _pool is not None:
            try:
                _pool.close()
            except Exception as e:
                log.warning("persistence pool close: %s", e)
            _pool = None


def pool_needed() -> bool:
    """Pool si hay ``DATABASE_URL``: escrituras, lecturas primarias Postgres, o tareas como reset (DELETE)."""
    if not database_url():
        return False
    return True


def get_pool() -> Any | None:
    """Pool compartido si hay ``DATABASE_URL`` (escrituras, lecturas Postgres o reset)."""
    global _pool
    if not pool_needed():
        with _pool_lock:
            if _pool is not None:
                try:
                    _pool.close()
                except Exception:
                    pass
                _pool = None
        return None
    with _pool_lock:
        if _pool is None:
            from psycopg_pool import ConnectionPool

            _pool = ConnectionPool(
                conninfo=database_url(),
                min_size=0,
                max_size=6,
                timeout=10.0,
                max_waiting=20,
                num_workers=2,
                kwargs={
                    "connect_timeout": 8,
                    # Supabase transaction pool (:6543) / PgBouncer: evita prepared statements en servidor.
                    "prepare_threshold": None,
                },
            )
            log.info("Postgres ConnectionPool inicializado (max_size=6)")
            atexit.register(_close_pool)
        return _pool
