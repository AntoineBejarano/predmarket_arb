from __future__ import annotations

import time
import unittest

from clients.odds_api_io import _EVENT_META_TTL_SEC, OddsApiIo, OddsEvent


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


if __name__ == "__main__":
    unittest.main()
