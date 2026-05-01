from __future__ import annotations

import unittest

from arb.latency_sports_ai_openrouter import (
    _assistant_message_text,
    _build_ai_tasks,
    _extract_decisions_list,
    _extract_json_array,
)


class TestLatencySportsAiOpenrouter(unittest.TestCase):
    def test_extract_json_array_plain(self) -> None:
        out = _extract_json_array('[{"condition_id":"a","decision":"reject"}]')
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].get("condition_id"), "a")

    def test_extract_json_array_fenced(self) -> None:
        raw = '```json\n[{"x":1}]\n```'
        out = _extract_json_array(raw)
        self.assertEqual(out, [{"x": 1}])

    def test_extract_decisions_list_wrapped_object(self) -> None:
        raw = '{"matches":[{"condition_id":"x","decision":"reject"}]}'
        out = _extract_decisions_list(raw)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].get("condition_id"), "x")

    def test_assistant_message_text_list_parts(self) -> None:
        msg = {"content": [{"type": "text", "text": "[{\"a\":1}]"}]}
        self.assertIn("[", _assistant_message_text(msg))

    def test_build_ai_tasks_truncates_candidates(self) -> None:
        many = [{"event_id": str(i), "home": "h", "away": "a", "bookie": "b"} for i in range(100)]
        pending = [
            {
                "condition_id": "0xc1",
                "poly_home": "A",
                "poly_away": "B",
                "sport_slug": "tennis",
                "io_candidates": many,
            }
        ]
        tasks = _build_ai_tasks(pending)
        self.assertEqual(len(tasks), 1)
        self.assertLessEqual(len(tasks[0]["candidate_event_ids"]), 35)


if __name__ == "__main__":
    unittest.main()
