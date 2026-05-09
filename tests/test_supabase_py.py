"""Cliente opcional supabase-py (env)."""

import os

import pytest


def test_supabase_env_status_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    from clients import supabase_py

    supabase_py._client = None  # type: ignore[attr-defined]
    supabase_py._client_fingerprint = None  # type: ignore[attr-defined]
    st = supabase_py.supabase_env_status()
    assert st["configured"] is False
    assert st["url_host"] is None
    assert supabase_py.get_supabase_client() is None


def test_supabase_env_status_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_URL", "https://abc.supabase.co")
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    from clients import supabase_py

    supabase_py._client = None  # type: ignore[attr-defined]
    supabase_py._client_fingerprint = None  # type: ignore[attr-defined]
    st = supabase_py.supabase_env_status()
    assert st["configured"] is False
    assert st["url_host"] == "abc.supabase.co"
