from __future__ import annotations

import unittest

from clients.odds_api_io import _consume_ws_json_buffer


class TestWsJsonBufferParser(unittest.TestCase):
    def test_single_json_per_frame(self) -> None:
        objs, buf, errs, overflow = _consume_ws_json_buffer(buffer="", chunk='{"type":"a"}')
        self.assertEqual(len(objs), 1)
        self.assertEqual(objs[0]["type"], "a")
        self.assertEqual(buf, "")
        self.assertEqual(errs, 0)
        self.assertIsNone(overflow)

    def test_concatenated_json_objects(self) -> None:
        objs, buf, errs, overflow = _consume_ws_json_buffer(
            buffer="",
            chunk='{"type":"a"}{"type":"b"}',
        )
        self.assertEqual([o.get("type") for o in objs], ["a", "b"])
        self.assertEqual(buf, "")
        self.assertEqual(errs, 0)
        self.assertIsNone(overflow)

    def test_json_with_newline_separator(self) -> None:
        objs, buf, errs, overflow = _consume_ws_json_buffer(
            buffer="",
            chunk='{"type":"a"}\n{"type":"b"}',
        )
        self.assertEqual([o.get("type") for o in objs], ["a", "b"])
        self.assertEqual(buf, "")
        self.assertEqual(errs, 0)
        self.assertIsNone(overflow)

    def test_partial_json_across_chunks(self) -> None:
        objs1, buf1, errs1, overflow1 = _consume_ws_json_buffer(
            buffer="",
            chunk='{"type":"a"',
        )
        self.assertEqual(objs1, [])
        self.assertNotEqual(buf1, "")
        self.assertEqual(errs1, 0)
        self.assertIsNone(overflow1)

        objs2, buf2, errs2, overflow2 = _consume_ws_json_buffer(
            buffer=buf1,
            chunk="}",
        )
        self.assertEqual(len(objs2), 1)
        self.assertEqual(objs2[0]["type"], "a")
        self.assertEqual(buf2, "")
        self.assertEqual(errs2, 0)
        self.assertIsNone(overflow2)

    def test_irrecoverable_garbage_and_overflow(self) -> None:
        # Basura + JSON válido concatenado: se recupera objeto y cuenta error.
        objs, buf, errs, overflow = _consume_ws_json_buffer(
            buffer="",
            chunk='xxx{"type":"ok"}',
        )
        self.assertEqual([o.get("type") for o in objs], ["ok"])
        self.assertEqual(buf, "")
        self.assertGreaterEqual(errs, 1)
        self.assertIsNone(overflow)

        # Overflow defensivo: buffer parcial demasiado grande.
        big_partial = "{" + ("a" * 128)
        objs2, buf2, errs2, overflow2 = _consume_ws_json_buffer(
            buffer="",
            chunk=big_partial,
            max_buffer_size=32,
        )
        self.assertEqual(objs2, [])
        self.assertEqual(buf2, "")
        self.assertGreaterEqual(errs2, 1)
        self.assertIsNotNone(overflow2)


if __name__ == "__main__":
    unittest.main()
