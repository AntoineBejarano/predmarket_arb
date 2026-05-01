from __future__ import annotations

import json
import unittest
import unittest.mock
from pathlib import Path

from arb.latency_sports_ai_rejects import clear_ai_reject, load_ai_rejected_condition_ids, record_ai_reject


class TestLatencySportsAiRejects(unittest.TestCase):
    def test_roundtrip(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "latency_sports_ai_rejected.json"

            def fake_path() -> Path:
                return p

            import arb.latency_sports_ai_rejects as m

            with unittest.mock.patch.object(m, "ai_rejects_path", fake_path):
                self.assertEqual(load_ai_rejected_condition_ids(), set())
                record_ai_reject("0xc1", reason="test")
                self.assertIn("0xc1", load_ai_rejected_condition_ids())
                self.assertTrue(clear_ai_reject("0xc1"))
                self.assertEqual(load_ai_rejected_condition_ids(), set())
                raw = json.loads(p.read_text(encoding="utf-8"))
                self.assertEqual(raw.get("items"), {})


if __name__ == "__main__":
    unittest.main()
