"""Короткий замер доступности метрик сбоя на уже проверенном счётчике."""

import argparse
import json
import os
import platform
import secrets
import tempfile
import time
from pathlib import Path

import ray

from examples.counter_checkpoint import Counter as SavedCounter
from examples.counter_restart import Counter as VolatileCounter


def quantile(values, probability):
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["restart", "checkpoint"], required=True)
    parser.add_argument("--before", type=int, default=8)
    parser.add_argument("--after", type=int, default=8)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.before, args.after) < 1:
        parser.error("before and after must be positive")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        parser.error("output already exists; choose a new path")
    Path("runs").mkdir(exist_ok=True)
    state_dir = Path(tempfile.mkdtemp(prefix="recovery-probe-", dir="runs")).resolve()
    os.environ.setdefault("RAY_USAGE_STATS_ENABLED", "0")
    os.environ.setdefault("RAY_AUTH_TOKEN", secrets.token_urlsafe(32))
    events = []

    def increment(counter, number):
        sent = time.perf_counter_ns()
        result = ray.get(counter.increment.remote(), timeout=60)
        ack = time.perf_counter_ns()
        events.append({"event": "increment_ack", "operation_id": f"inc-{number}", "attempt": 1,
                       "sent_ns": sent, "ack_ns": ack, "value": result, "duration_ns": ack - sent})
        return result

    try:
        ray.init(address="local", num_cpus=1, include_dashboard=False,
                 log_to_driver=False, object_store_memory=80 * 1024 * 1024)
        counter = (SavedCounter.remote(str(state_dir / "state.json"))
                   if args.mode == "checkpoint" else VolatileCounter.remote())
        initial = ray.get(counter.snapshot.remote(), timeout=60)
        for number in range(1, args.before + 1):
            if increment(counter, number) != number:
                raise RuntimeError("Unexpected pre-crash state")
        before = ray.get(counter.snapshot.remote(), timeout=60)
        crash_requested = time.perf_counter_ns()
        events.append({"event": "crash_requested", "time_ns": crash_requested, "pid": before["pid"]})
        try:
            ray.get(counter.crash.remote(), timeout=60)
        except ray.exceptions.RayActorError as error:
            events.append({"event": "crash_observed", "time_ns": time.perf_counter_ns(), "error": type(error).__name__})
        else:
            raise RuntimeError("Crash did not fail the actor call")
        restored = ray.get(counter.snapshot.options(max_task_retries=-1).remote(), timeout=60)
        events.append({"event": "actor_ready", "time_ns": time.perf_counter_ns(), **restored})
        expected_restored = args.before if args.mode == "checkpoint" else 0
        if initial["value"] != 0 or restored["pid"] == before["pid"] or restored["value"] != expected_restored:
            raise RuntimeError("Unexpected restart outcome")
        for offset in range(1, args.after + 1):
            if increment(counter, args.before + offset) != expected_restored + offset:
                raise RuntimeError("Unexpected post-crash state")
        final = ray.get(counter.snapshot.remote(), timeout=60)
    finally:
        ray.shutdown()
    operations = [event for event in events if event["event"] == "increment_ack"]
    latencies_ms = [event["duration_ns"] / 1e6 for event in operations]
    first_after = operations[args.before]
    report = {
        "status": "passed", "mode": args.mode, "python": platform.python_version(),
        "ray_version": ray.__version__, "ray_commit": ray.__commit__,
        "before": before, "restored": restored, "final": final,
        "acknowledged_operations": len(operations),
        "acknowledged_effects_lost": len(operations) - final["value"],
        "client_attempts": len(operations), "repeated_increment_attempts": 0,
        "recovery_to_first_ack_ms": (first_after["ack_ns"] - crash_requested) / 1e6,
        "p95_increment_ms": quantile(latencies_ms, 0.95),
        "p99_increment_ms": quantile(latencies_ms, 0.99),
        "events": events,
        "limitations": [
            "Recovery interval starts at the driver's crash request, not the exact process death",
            "Increment latency excludes the readiness wait; recovery interval includes it",
            "Automatic read retries are not counted as increment attempts",
            "Small pilot proves data availability, not a stable tail-latency estimate",
            "No in-flight increment at crash; ambiguous outcomes and replay not implemented",
        ],
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key not in ("events", "limitations")}, indent=2))


if __name__ == "__main__":
    main()
