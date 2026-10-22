"""Проверки ошибок записи и чтения checkpoint без запуска Ray."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from examples.counter_checkpoint import load_checkpoint, save_checkpoint


class CheckpointFailureTests(unittest.TestCase):
    def test_failed_save_keeps_previous_checkpoint(self):
        for failing_call in ("fsync", "replace"):
            with (
                self.subTest(failing_call=failing_call),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "state.json"
                save_checkpoint(path, 7)
                with patch(
                    f"examples.counter_checkpoint.os.{failing_call}",
                    side_effect=OSError("simulated storage failure"),
                ):
                    with self.assertRaises(OSError):
                        save_checkpoint(path, 8)
                self.assertEqual(load_checkpoint(path), 7)
                # Остаток временного файла не мешает следующей успешной записи.
                save_checkpoint(path, 8)
                self.assertEqual(load_checkpoint(path), 8)

    def test_incomplete_temporary_file_is_not_restored(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            save_checkpoint(path, 7)
            path.with_suffix(".tmp").write_text('{"value":', encoding="utf-8")
            self.assertEqual(load_checkpoint(path), 7)

    def test_corrupt_checkpoint_is_not_silently_reset(self):
        invalid_contents = [
            "", '{"value":', "{}", "[]", '{"value": true}',
            '{"value": -1}', '{"value": 1.5}', '{"value": "7"}',
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            for contents in invalid_contents:
                with self.subTest(contents=contents):
                    path.write_text(contents, encoding="utf-8")
                    with self.assertRaises(ValueError):
                        load_checkpoint(path)


if __name__ == "__main__":
    unittest.main()
