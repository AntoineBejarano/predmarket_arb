from __future__ import annotations

import asyncio
import json
import time
import unittest

from clients.odds_api_io import (
    _EVENT_META_TTL_SEC,
    CatalogRow,
    OddsApiIo,
    OddsEvent,
)


class TestOddsApiIoMerge(unittest.TestCase):
    def _build_ws_event(self) -> OddsEvent:
        return OddsEvent(
            home="",
            away="",
            home_odds=2.15,
            away_odds=1.92,
            draw_odds=None,
            bookie="Betfair Exchange",
            updated_at="",
            event_id="evt-1",
            sport="tennis",
        )

    def test_get_cached_odds_merges_home_away_from_rest_meta_cache(self) -> None:
        c = OddsApiIo()
        c._ws_odds_cache = {"evt-1": {"Betfair Exchange": self._build_ws_event()}}
        c._set_event_meta("evt-1", "Novak Djokovic", "Rafael Nadal")

        rows = c.get_cached_odds("tennis")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].home, "novak djokovic")
        self.assertEqual(rows[0].away, "rafael nadal")
        self.assertAlmostEqual(rows[0].home_odds, 2.15)
        self.assertAlmostEqual(rows[0].away_odds, 1.92)

    def test_has_ws_incomplete_for_io_sport_becomes_false_after_meta_fill(self) -> None:
        c = OddsApiIo()
        c._ws_odds_cache = {"evt-1": {"Betfair Exchange": self._build_ws_event()}}

        self.assertTrue(c.has_ws_incomplete_for_io_sport("tennis"))
        c._set_event_meta("evt-1", "Iga Swiatek", "Aryna Sabalenka")
        self.assertFalse(c.has_ws_incomplete_for_io_sport("tennis"))

    def test_meta_cache_ttl_expiry_does_not_fill_ws_event(self) -> None:
        c = OddsApiIo()
        c._ws_odds_cache = {"evt-1": {"Betfair Exchange": self._build_ws_event()}}
        c._event_meta_by_id["evt-1"] = (
            time.monotonic() - _EVENT_META_TTL_SEC - 1.0,
            "old home",
            "old away",
        )

        rows = c.get_cached_odds("tennis")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].home, "")
        self.assertEqual(rows[0].away, "")

    def test_get_matching_candidate_events_adds_catalog_stub(self) -> None:
        c = OddsApiIo()
        io = "tennis"
        c._events_catalog[io] = (
            time.monotonic(),
            [
                CatalogRow(
                    event_id="999991",
                    home="Alpha Team",
                    away="Beta Team",
                    event_date_s="2099-01-01T00:00:00Z",
                    io_sport=io,
                )
            ],
        )
        c.events_catalog_ttl_sec = 99999.0
        evs, st = c.get_matching_candidate_events_with_stats("tennis")
        self.assertIn("999991", [str(x.event_id) for x in evs])
        hit = [x for x in evs if str(x.event_id) == "999991"]
        self.assertEqual(len(hit), 1)
        self.assertTrue(hit[0].catalog_metadata_only)
        self.assertEqual(st["catalog_events_total"], 1)
        self.assertGreaterEqual(st["catalog_candidates_added"], 1)

    def test_get_matching_dedupes_catalog_when_ws_has_same_event_id(self) -> None:
        c = OddsApiIo()
        io = "tennis"
        c._ws_odds_cache = {
            "999992": {
                "Betfair Exchange": OddsEvent(
                    home="a",
                    away="b",
                    home_odds=2.0,
                    away_odds=2.0,
                    draw_odds=None,
                    bookie="Betfair Exchange",
                    updated_at="",
                    event_id="999992",
                    sport=io,
                )
            }
        }
        c._events_catalog[io] = (
            time.monotonic(),
            [CatalogRow("999992", "A", "B", "", io)],
        )
        c.events_catalog_ttl_sec = 99999.0
        evs, st = c.get_matching_candidate_events_with_stats("tennis")
        rows = [x for x in evs if str(x.event_id) == "999992"]
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0].catalog_metadata_only)
        self.assertEqual(st["catalog_candidates_added"], 0)

    def test_hydrate_event_odds_ok(self) -> None:
        payload = [
            {
                "id": "424242",
                "home": "H",
                "away": "A",
                "bookmakers": {
                    "Betfair Exchange": [
                        {
                            "name": "ML",
                            "updatedAt": "2026-01-01T00:00:00Z",
                            "odds": [{"home": "2.5", "away": "1.55"}],
                        }
                    ]
                },
            }
        ]

        class _Resp:
            def __init__(self, body: str) -> None:
                self.status = 200
                self._body = body

            async def __aenter__(self) -> "_Resp":
                return self

            async def __aexit__(self, *args: object) -> None:
                return None

            async def text(self) -> str:
                return self._body

        class _Sess:
            def get(self, *args: object, **kwargs: object) -> _Resp:
                return _Resp(json.dumps(payload))

        async def _run() -> None:
            c = OddsApiIo()
            c._rest_quota_ts.clear()
            ev, st = await c.hydrate_event_odds(_Sess(), "424242", "tennis")
            assert st == "ok"
            assert ev is not None
            assert ev.catalog_metadata_only is False
            assert abs(ev.home_odds - 2.5) < 1e-9
            assert abs(ev.away_odds - 1.55) < 1e-9

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
