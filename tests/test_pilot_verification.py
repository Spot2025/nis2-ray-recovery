"""Проверяющий инструмент должен отвергать пропуски, дубли и неверные батчи."""

import copy
import unittest

from ray_ps.verify import expected_indices, validate_manifest


class ManifestVerificationTests(unittest.TestCase):
    def setUp(self):
        self.config = dict(steps=3, workers=2, batch_size=2, dataset_size=10, seed=7)
        self.rows = []
        for step in range(1, 4):
            self.rows.append({"step": step, "operation_id": f"train-{step}",
                              "base_version": step - 1, "result_version": step,
                              "batches": [{"step": step, "shard": shard,
                                           "batch_id": f"step-{step}:shard-{shard}",
                                           "indices": expected_indices(10, 2, shard, step, 2, 7)}
                                          for shard in range(2)]})

    def test_missing_and_duplicate_updates_are_rejected(self):
        validate_manifest(self.rows, self.config)
        with self.assertRaises(ValueError):
            validate_manifest(self.rows[:-1], self.config)
        duplicate = copy.deepcopy(self.rows)
        duplicate[1] = copy.deepcopy(duplicate[0])
        with self.assertRaises(ValueError):
            validate_manifest(duplicate, self.config)

    def test_wrong_examples_and_shards_are_rejected(self):
        for field, value in [("indices", [999, 998]), ("shard", 1)]:
            with self.subTest(field=field):
                corrupt = copy.deepcopy(self.rows)
                corrupt[1]["batches"][0][field] = value
                with self.assertRaises(ValueError):
                    validate_manifest(corrupt, self.config)

    def test_reference_crosses_epochs_without_dropping_examples(self):
        # 5 элементов в каждом shard, батчи по 2 пересекают границу эпохи.
        for shard in range(2):
            stream = [index for step in range(1, 6)
                      for index in expected_indices(10, 2, shard, step, 2, 7)]
            self.assertEqual(sorted(stream[:5]), list(range(shard, 10, 2)))
            self.assertEqual(sorted(stream[5:]), list(range(shard, 10, 2)))


if __name__ == "__main__":
    unittest.main()
