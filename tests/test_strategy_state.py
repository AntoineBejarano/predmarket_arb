import asyncio
import json

from risk import strategy_state as ss


def test_disable_with_error_records_structured_reason(tmp_path, monkeypatch):
    state_file = tmp_path / "strategy_state.json"
    monkeypatch.setattr(ss, "STATE_FILE", state_file)
    mgr = ss.StrategyStateManager()

    async def _run():
        await mgr.enable("crypto_5m_sixcycle")
        await mgr.disable_with_error(
            "crypto_5m_sixcycle",
            reason="live_order_failed",
            detail={"market_id": "m1", "error": "boom"},
        )
        return await mgr.get_all()

    all_state = asyncio.run(_run())
    ent = all_state["crypto_5m_sixcycle"]
    assert ent["enabled"] is False
    assert ent["stopped_by_failure"] is True
    assert ent["last_stop_reason"] == "live_order_failed"
    assert ent["last_stop_detail"]["market_id"] == "m1"
    assert ent["last_stop_ts"]


def test_enable_clears_previous_failure_state(tmp_path, monkeypatch):
    state_file = tmp_path / "strategy_state.json"
    monkeypatch.setattr(ss, "STATE_FILE", state_file)
    mgr = ss.StrategyStateManager()

    async def _run():
        await mgr.disable_with_error("bundle_arb", reason="test_failure", detail={"x": 1})
        await mgr.enable("bundle_arb")
        return await mgr.get_all()

    all_state = asyncio.run(_run())
    ent = all_state["bundle_arb"]
    assert ent["enabled"] is True
    assert ent["stopped_by_failure"] is False
    assert ent["last_stop_reason"] is None
    assert ent["last_stop_detail"] is None
    assert ent["last_stop_ts"] is None


def test_reset_fictional_for_slug_only_affects_target(tmp_path, monkeypatch):
    state_file = tmp_path / "strategy_state.json"
    monkeypatch.setattr(ss, "STATE_FILE", state_file)
    mgr = ss.StrategyStateManager()

    async def _run():
        await mgr.disable_with_error(
            "crypto_5m_sixcycle",
            reason="live_order_failed",
            detail={"market_id": "m2"},
        )
        await mgr.enable("bundle_arb")
        all_before = await mgr.get_all()
        all_before["bundle_arb"]["fict_pnl_cumulative_eur"] = 12.5
        state_file.write_text(json.dumps(all_before), encoding="utf-8")
        ok = await mgr.reset_fictional_for_slug("crypto_5m_sixcycle")
        return ok, await mgr.get_all()

    ok, all_state = asyncio.run(_run())
    assert ok is True
    six = all_state["crypto_5m_sixcycle"]
    bun = all_state["bundle_arb"]
    assert six["fict_pnl_cumulative_eur"] == 0.0
    assert six["fict_trades"] == 0
    assert six["stopped_by_failure"] is False
    assert six["last_stop_reason"] is None
    assert bun["fict_pnl_cumulative_eur"] == 12.5
