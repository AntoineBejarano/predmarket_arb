"""Persistencia enlaces manuales latency sports."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import mock

from arb.latency_sports_manual_match import delete_manual_match, load_manual_matches, upsert_manual_match


def test_upsert_load_delete_roundtrip(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "latency_sports_manual_matches.json"

        def fake_path() -> Path:
            return p

        monkeypatch.setattr("arb.latency_sports_manual_match.manual_matches_path", fake_path)
        assert load_manual_matches() == {}
        upsert_manual_match("0xcid1", "ev99", swap_sides=True, poly_home="A", poly_away="B")
        m = load_manual_matches()
        assert "0xcid1" in m
        assert m["0xcid1"]["odds_event_id"] == "ev99"
        assert m["0xcid1"]["swap_sides"] is True
        assert delete_manual_match("0xcid1") is True
        assert load_manual_matches() == {}
        raw = json.loads(p.read_text(encoding="utf-8"))
        assert raw.get("version") == 1
        assert raw.get("items") == {}
