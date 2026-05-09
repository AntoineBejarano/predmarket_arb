"""Versionado de config Sixcycle (huella + agregados por fingerprint)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_config_fingerprint_changes_with_params() -> None:
    from scripts import sixcycle_config_store as scs

    a = dict(scs.DEFAULT_SIXCYCLE_CONFIG)
    b = scs.merge_and_validate(a, {"timing_max_minutes": 5.0})
    assert scs.config_fingerprint(a) != scs.config_fingerprint(b)


def test_stats_by_fingerprint_from_rows() -> None:
    from scripts import sixcycle_config_store as scs

    rows = [
        {
            "phase": "SETTLED",
            "resolved": "win",
            "pnl_usdc": "1",
            "stake_usdc": "1",
            "config_fingerprint": "aaaabbbbccccdddd",
            "config_profile_slug": "v1",
        },
        {
            "phase": "SETTLED",
            "resolved": "loss",
            "pnl_usdc": "-1",
            "stake_usdc": "1",
            "config_fingerprint": "aaaabbbbccccdddd",
            "config_profile_slug": "v1",
        },
        {
            "phase": "SETTLED",
            "resolved": "win",
            "pnl_usdc": "2",
            "stake_usdc": "1",
            "config_fingerprint": "0000111122223333",
            "config_profile_slug": "v2",
        },
    ]
    out = scs.stats_by_fingerprint_from_rows(rows)
    assert out["aaaabbbbccccdddd"]["trades"] == 2
    assert out["0000111122223333"]["trades"] == 1


def test_stats_last_n_filter_fingerprint() -> None:
    from scripts import sixcycle_config_store as scs

    rows = [
        {
            "phase": "SETTLED",
            "resolved": "win",
            "pnl_usdc": "5",
            "stake_usdc": "1",
            "config_fingerprint": "onlyme",
        },
        {
            "phase": "SETTLED",
            "resolved": "win",
            "pnl_usdc": "99",
            "stake_usdc": "1",
            "config_fingerprint": "other",
        },
    ]
    s = scs.stats_last_n_from_csv_rows(rows, 10, config_fingerprint="onlyme")
    assert s["trades"] == 1
    assert abs(float(s["ev_per_trade"]) - 5.0) < 1e-6


def test_append_version_dedupe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import sixcycle_config_store as scs

    logp = tmp_path / "sixcycle_config_versions.jsonl"
    monkeypatch.setattr(scs, "VERSION_LOG_PATH", logp)
    cfg = dict(scs.DEFAULT_SIXCYCLE_CONFIG)
    scs.append_config_version_record(cfg)
    scs.append_config_version_record(dict(cfg))
    assert len(logp.read_text().strip().splitlines()) == 1


def test_append_version_new_note(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import sixcycle_config_store as scs

    logp = tmp_path / "v.jsonl"
    monkeypatch.setattr(scs, "VERSION_LOG_PATH", logp)
    a = dict(scs.DEFAULT_SIXCYCLE_CONFIG)
    scs.append_config_version_record(a)
    b = scs.merge_and_validate(a, {"profile_note": "timing experimento"})
    scs.append_config_version_record(b)
    lines = [json.loads(ln) for ln in logp.read_text().splitlines() if ln.strip()]
    assert len(lines) == 2
    assert lines[1]["profile_note"] == "timing experimento"
