"""Независимый последовательный проход: общий батч вместо распределённой агрегации."""

import argparse
import json
from pathlib import Path

import torch
from torch.nn import functional as F
from torchvision.datasets import MNIST

from .model import ConvNet, configure_torch, copy_weights, images_and_labels, momentum_buffers, tensor_digest


def expected_indices(dataset_size, workers, shard, step, batch_size, seed):
    # Независимо вычисляем позиции из номера шага, без DataShard и его cursor.
    partition = torch.arange(shard, dataset_size, workers)
    cache, indices = {}, []
    for absolute_position in range((step - 1) * batch_size, step * batch_size):
        epoch, offset = divmod(absolute_position, len(partition))
        if epoch not in cache:
            generator = torch.Generator().manual_seed(seed + 1009 * shard + 1000003 * epoch)
            cache[epoch] = partition[torch.randperm(len(partition), generator=generator)]
        indices.append(int(cache[epoch][offset]))
    return indices


def validate_manifest(rows, config):
    if len(rows) != config["steps"]:
        raise ValueError("Missing or extra training step")
    for step, row in enumerate(rows, start=1):
        if (row["step"], row["operation_id"], row["base_version"], row["result_version"]) != (step, f"train-{step}", step - 1, step):
            raise ValueError("Missing, duplicate or out-of-order update")
        if len(row["batches"]) != config["workers"]:
            raise ValueError("Missing or extra batch")
        for shard, batch in enumerate(row["batches"]):
            expected = expected_indices(config["dataset_size"], config["workers"], shard, step, config["batch_size"], config["seed"])
            if (batch["step"], batch["shard"], batch["batch_id"], batch["indices"]) != (step, shard, f"step-{step}:shard-{shard}", expected):
                raise ValueError("Wrong batch or data order")


def compare_tensors(actual, expected, atol, rtol):
    if set(actual) != set(expected):
        raise ValueError("Tensor names differ")
    max_error = 0.0
    for name in expected:
        torch.testing.assert_close(actual[name], expected[name], atol=atol, rtol=rtol)
        max_error = max(max_error, float((actual[name] - expected[name]).abs().max()))
    return max_error


def _verify(run_dir, data_root=None, atol=1e-6, rtol=1e-5):
    config = json.loads((run_dir / "config.json").read_text())
    rows = [json.loads(line) for line in (run_dir / "steps.jsonl").read_text().splitlines()]
    validate_manifest(rows, config)
    configure_torch(config["seed"])
    dataset = MNIST(str(data_root or config["data_root"]), train=True, download=False)
    if len(dataset) != config["dataset_size"] or tensor_digest({"images": dataset.data, "labels": dataset.targets}) != config["dataset_sha256"]:
        raise ValueError("Dataset differs from the pilot")
    model = ConvNet()
    optimizer = torch.optim.SGD(model.parameters(), lr=config["learning_rate"], momentum=config["momentum"])
    for step in range(1, config["steps"] + 1):
        indices = [index for shard in range(config["workers"])
                   for index in expected_indices(len(dataset), config["workers"], shard, step, config["batch_size"], config["seed"])]
        images, labels = images_and_labels(dataset, indices)
        optimizer.zero_grad(set_to_none=True)
        # Один общий батч: не используем actor.apply, worker и код агрегации пилота.
        F.cross_entropy(model(images), labels).backward()
        optimizer.step()
    final = torch.load(run_dir / "final-state.pt", map_location="cpu", weights_only=True)
    if final["step"] != config["steps"] or len(final["sources"]) != config["workers"]:
        raise ValueError("Wrong final progress")
    for shard, source in enumerate(final["sources"]):
        if (source["shard"], source["position"], source["committed_step"]) != (shard, config["steps"] * config["batch_size"], config["steps"]):
            raise ValueError("Wrong source progress")
    weights_error = compare_tensors(final["weights"], copy_weights(model), atol, rtol)
    momentum_error = compare_tensors(final["momentum"], momentum_buffers(model, optimizer), atol, rtol)
    if rows[-1]["weights_sha256"] != tensor_digest(final["weights"]):
        raise ValueError("Final state does not match the log")
    report = {"status": "passed", "reference": "sequential combined-batch SGD with momentum",
              "steps": config["steps"], "missing_or_duplicate_step_ids": 0,
              "weights_max_abs_error": weights_error, "momentum_max_abs_error": momentum_error,
              "atol": atol, "rtol": rtol, "ray_commit_of_pilot": config["ray_commit"],
              "limitations": "No injected failures; shared model/math library; same host/library versions"}
    (run_dir / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def verify(run_dir, data_root=None, atol=1e-6, rtol=1e-5):
    try:
        return _verify(run_dir, data_root, atol, rtol)
    except Exception as error:
        # Повторная проверка испорченного результата не оставляет старый passed.
        (run_dir / "verification.json").write_text(json.dumps({
            "status": "failed", "error_type": type(error).__name__, "error": str(error),
        }, indent=2) + "\n")
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--data-root", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.run_dir, args.data_root), indent=2))


if __name__ == "__main__":
    main()
