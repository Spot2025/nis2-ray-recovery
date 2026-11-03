"""Синхронное обучение на CPU без сбоев; журнал и состояние для внешней проверки."""

import argparse
import hashlib
import json
import os
import platform
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import ray
import torch
import torchvision
from torch.nn import functional as F
from torchvision.datasets import MNIST

from .model import (
    ConvNet, configure_torch, copy_weights, images_and_labels,
    momentum_buffers, tensor_digest,
)

TARGET_RAY_COMMIT = "2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f"


@ray.remote(num_cpus=0, max_restarts=0, max_task_retries=0)
class DataShard:
    def __init__(self, root, shard, workers, batch_size, seed):
        configure_torch(seed)
        self.dataset = MNIST(root, train=True, download=False)
        self.partition = list(range(shard, len(self.dataset), workers))
        self.shard, self.batch_size, self.seed = shard, batch_size, seed
        self.position, self.committed_step, self.pending = 0, 0, None

    def reserve(self, step):
        if self.pending is not None:
            if self.pending["step"] != step:
                raise ValueError("Previous batch has not been committed")
            return self.pending
        if step != self.committed_step + 1:
            raise ValueError("Out-of-order batch request")
        indices, orders = [], {}
        for offset in range(self.batch_size):
            epoch, position = divmod(self.position + offset, len(self.partition))
            if epoch not in orders:
                generator = torch.Generator().manual_seed(self.seed + 1009 * self.shard + 1000003 * epoch)
                orders[epoch] = torch.randperm(len(self.partition), generator=generator)
            indices.append(self.partition[int(orders[epoch][position])])
        self.pending = {
            "step": step, "shard": self.shard,
            "batch_id": f"step-{step}:shard-{self.shard}", "indices": indices,
            "indices_sha256": hashlib.sha256(json.dumps(indices).encode()).hexdigest(),
        }
        return self.pending

    def batch(self, step):
        if self.pending is None or self.pending["step"] != step:
            raise ValueError("Batch must be reserved first")
        images, labels = images_and_labels(self.dataset, self.pending["indices"])
        return images, labels, self.pending

    def commit(self, step):
        if step == self.committed_step:
            return self.snapshot()
        if self.pending is None or self.pending["step"] != step:
            raise ValueError("No matching reservation")
        self.position += self.batch_size
        self.committed_step = step
        self.pending = None
        return self.snapshot()

    def snapshot(self):
        return {"shard": self.shard, "position": self.position,
                "committed_step": self.committed_step, "pid": os.getpid()}


@ray.remote(num_cpus=1, max_retries=0)
def compute_gradient(weights, source, step, seed):
    configure_torch(seed)
    started = time.perf_counter_ns()
    images, labels, batch = ray.get(source.batch.remote(step), timeout=60)
    model = ConvNet()
    model.load_state_dict(weights)
    loss = F.cross_entropy(model(images), labels)
    loss.backward()
    return {
        "step": step, "base_version": step - 1, "batch": batch,
        "attempt": 1, "pid": os.getpid(), "loss": float(loss.detach()),
        "gradients": {name: parameter.grad.detach().clone()
                      for name, parameter in model.named_parameters()},
        "duration_ns": time.perf_counter_ns() - started,
    }


@ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
class ParameterServer:
    def __init__(self, seed, learning_rate, momentum, workers):
        configure_torch(seed)
        self.model = ConvNet()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=learning_rate, momentum=momentum)
        self.step, self.workers = 0, workers

    def apply(self, step, results):
        if step != self.step + 1 or len(results) != self.workers:
            raise ValueError("Unexpected step or number of gradients")
        results = sorted(results, key=lambda result: result["batch"]["shard"])
        if [result["batch"]["shard"] for result in results] != list(range(self.workers)):
            raise ValueError("Missing or duplicate shard")
        if any(result["step"] != step or result["base_version"] != self.step for result in results):
            raise ValueError("Stale gradient")
        self.optimizer.zero_grad(set_to_none=True)
        for name, parameter in self.model.named_parameters():
            parameter.grad = torch.stack([result["gradients"][name] for result in results]).mean(0)
        self.optimizer.step()
        self.step = step
        return {"step": self.step, "weights": copy_weights(self.model)}

    def snapshot(self):
        return {"step": self.step, "weights": copy_weights(self.model),
                "momentum": momentum_buffers(self.model, self.optimizer), "pid": os.getpid()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--steps", type=int, default=20)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--learning-rate", type=float, default=0.01)
    parser.add_argument("--momentum", type=float, default=0.9)
    parser.add_argument("--data-root", type=Path, default=Path("runs/data"))
    parser.add_argument("--output", type=Path, required=True, help="New directory for this run")
    parser.add_argument("--allow-other-ray", action="store_true", help="Development only; recorded in report")
    args = parser.parse_args()
    if min(args.steps, args.workers, args.batch_size) < 1 or args.workers > 16:
        parser.error("steps/batch-size must be positive; workers must be in 1..16")
    if args.learning_rate <= 0 or not 0 < args.momentum < 1:
        parser.error("learning-rate must be positive; momentum must be in (0, 1)")
    if ray.__commit__ != TARGET_RAY_COMMIT and not args.allow_other_ray:
        parser.error(f"Expected Ray commit {TARGET_RAY_COMMIT}; got {ray.__commit__}")
    configure_torch(args.seed)
    data_root = args.data_root.resolve()
    dataset = MNIST(str(data_root), train=True, download=True)
    args.output.mkdir(parents=True, exist_ok=False)
    config = {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()}
    config.update({"data_root": str(args.data_root), "dataset": "MNIST/train", "dataset_size": len(dataset),
                   "dataset_sha256": tensor_digest({"images": dataset.data, "labels": dataset.targets}),
                   "python": platform.python_version(), "platform": platform.platform(),
                   "ray_version": ray.__version__, "ray_commit": ray.__commit__,
                   "torch": torch.__version__, "torchvision": torchvision.__version__,
                   "numpy": np.__version__, "torch_threads": torch.get_num_threads(),
                   "source_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                     for path in sorted(Path(__file__).parent.glob("*.py"))},
                   "started_at": datetime.now(timezone.utc).isoformat()})
    (args.output / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    os.environ.setdefault("RAY_USAGE_STATS_ENABLED", "0")
    os.environ.setdefault("RAY_AUTH_TOKEN", secrets.token_urlsafe(32))
    rows = []
    try:
        ray.init(address="local", num_cpus=args.workers + 1, include_dashboard=False,
                 log_to_driver=False, object_store_memory=128 * 1024 * 1024)
        server = ParameterServer.remote(args.seed, args.learning_rate, args.momentum, args.workers)
        sources = [DataShard.remote(str(data_root), shard, args.workers, args.batch_size, args.seed)
                   for shard in range(args.workers)]
        initial = ray.get(server.snapshot.remote(), timeout=120)
        ray.get([source.snapshot.remote() for source in sources], timeout=120)
        weights = initial["weights"]
        with (args.output / "steps.jsonl").open("w") as log:
            for step in range(1, args.steps + 1):
                sent_ns = time.perf_counter_ns()
                reservations = ray.get([source.reserve.remote(step) for source in sources], timeout=60)
                weights_ref = ray.put(weights)
                results = ray.get([compute_gradient.remote(weights_ref, source, step, args.seed)
                                   for source in sources], timeout=120)
                update = ray.get(server.apply.remote(step, results), timeout=60)
                positions = ray.get([source.commit.remote(step) for source in sources], timeout=60)
                ack_ns = time.perf_counter_ns()
                weights = update["weights"]
                row = {"step": step, "operation_id": f"train-{step}", "attempt": 1,
                       "base_version": step - 1, "result_version": update["step"],
                       "sent_ns": sent_ns, "ack_ns": ack_ns, "duration_ns": ack_ns - sent_ns,
                       "batches": reservations, "sources": positions,
                       "computations": [{key: value for key, value in result.items()
                                         if key not in ("gradients", "batch")} for result in results],
                       "mean_loss": sum(result["loss"] for result in results) / args.workers,
                       "weights_sha256": tensor_digest(weights)}
                log.write(json.dumps(row) + "\n")
                log.flush()
                rows.append(row)
                print(f"step={step} loss={row['mean_loss']:.4f} duration_ms={row['duration_ns']/1e6:.2f}", flush=True)
        final = ray.get(server.snapshot.remote(), timeout=60)
        final["sources"] = ray.get([source.snapshot.remote() for source in sources], timeout=60)
        if final["step"] != args.steps or any(source["position"] != args.steps * args.batch_size for source in final["sources"]):
            raise RuntimeError("Final version or data positions do not match the requested run")
        actor_pids = {final["pid"], *(source["pid"] for source in final["sources"])}
        if len(actor_pids) != args.workers + 1 or os.getpid() in actor_pids:
            raise RuntimeError("Expected separate actor processes")
        torch.save(final, args.output / "final-state.pt")
    finally:
        ray.shutdown()
    durations = np.array([row["duration_ns"] / 1e6 for row in rows])
    summary = {"status": "completed_pending_independent_verification", "steps": len(rows),
               "gradient_computations": args.steps * args.workers,
               "p95_step_ms": float(np.quantile(durations, 0.95, method="linear")),
               "p99_step_ms": float(np.quantile(durations, 0.99, method="linear")),
               "timing_scope": "all pilot steps, including cold worker startup; descriptive only",
               "weights_sha256": tensor_digest(final["weights"]),
               "momentum_sha256": tensor_digest(final["momentum"]),
               "ray_commit": ray.__commit__, "failures_injected": 0}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Run saved: {args.output}; verify with: python -m ray_ps.verify --run-dir {args.output}")


if __name__ == "__main__":
    main()
