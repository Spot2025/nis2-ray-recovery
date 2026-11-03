"""Повторяемость двух пилотов и отказ проверки при повреждении momentum."""

import argparse
import json
import shutil
import tempfile
from pathlib import Path

import torch

from .model import tensor_digest
from .verify import verify


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first", type=Path, required=True)
    parser.add_argument("--second", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    configs = [json.loads((path / "config.json").read_text()) for path in (args.first, args.second)]
    keys = ("steps", "workers", "batch_size", "seed", "learning_rate", "momentum",
            "dataset_sha256", "ray_commit", "torch", "numpy")
    if any(configs[0][key] != configs[1][key] for key in keys):
        raise ValueError("Runs have different configurations")
    verification = [verify(path) for path in (args.first, args.second)]
    states = [torch.load(path / "final-state.pt", weights_only=True) for path in (args.first, args.second)]
    hashes = [{key: tensor_digest(state[key]) for key in ("weights", "momentum")} for state in states]
    if hashes[0] != hashes[1]:
        raise ValueError("Repeated runs did not produce identical tensor bytes")
    # Меняем копию; исходные результаты сохраняются.
    with tempfile.TemporaryDirectory(prefix="negative-check-", dir=args.first.parent) as directory:
        target = Path(directory)
        for name in ("config.json", "steps.jsonl", "final-state.pt", "verification.json"):
            shutil.copy2(args.first / name, target / name)
        state = torch.load(target / "final-state.pt", weights_only=True)
        name = next(iter(state["momentum"]))
        state["momentum"][name].flatten()[0] += 1
        torch.save(state, target / "final-state.pt")
        try:
            verify(target)
        except AssertionError:
            rejected = json.loads((target / "verification.json").read_text())
            if rejected["status"] != "failed":
                raise RuntimeError("Stale verification report")
        else:
            raise RuntimeError("Corrupted optimizer state was accepted")
    report = {"status": "passed", "ray_commit": configs[0]["ray_commit"],
              "steps_per_run": configs[0]["steps"], "identical_tensor_hashes": hashes[0],
              "verification": verification, "corrupt_momentum_rejected": True,
              "stale_passed_report_replaced": True, "corruption_delta": 1.0}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
