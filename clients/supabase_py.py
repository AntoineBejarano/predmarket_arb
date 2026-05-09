"""Cliente opcional de Supabase (PostgREST vía supabase-py).

Requiere ``SUPABASE_URL`` y ``SUPABASE_KEY`` (anon o ``service_role`` según RLS).
La persistencia principal del repo usa ``DATABASE_URL`` + psycopg; este módulo
sirve para llamadas REST/Realtime cuando convenga.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

_client: Any = None
_client_fingerprint: tuple[str, str] | None = None


def supabase_env_status() -> dict[str, Any]:
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()
    host: str | None = None
    if url:
        try:
            host = urlparse(url).netloc or None
        except ValueError:
            host = None
    return {
        "configured": bool(url and key),
        "url_host": host,
    }


def get_supabase_client():
    """Devuelve ``Client`` de supabase-py o ``None`` si faltan env vars."""
    global _client, _client_fingerprint
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()
    if not url or not key:
        _client = None
        _client_fingerprint = None
        return None
    fp = (url, key)
    if _client is not None and _client_fingerprint == fp:
        return _client
    from supabase import create_client

    _client = create_client(url, key)
    _client_fingerprint = fp
    return _client
