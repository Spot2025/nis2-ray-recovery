"""Первый пример: один актор Ray выполняет заданное число прибавлений."""

import argparse
import json
import os
import platform
import secrets
from datetime import datetime, timezone
from pathlib import Path

import ray


@ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

    def snapshot(self):
        return {"value": self.value, "pid": os.getpid()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--increments", type=int, default=100)
    parser.add_argument("--output", type=Path, default=Path("runs/counter.json"))
    args = parser.parse_args()
    if args.increments < 1:
        parser.error("--increments должен быть положительным целым числом")

    os.environ.setdefault("RAY_USAGE_STATS_ENABLED", "0")
    # Токен живёт только в окружении этого запуска, без записи в домашнюю папку.
    os.environ.setdefault("RAY_AUTH_TOKEN", secrets.token_urlsafe(32))
    ray.init(
        address="local",
        num_cpus=1,
        include_dashboard=False,
        log_to_driver=False,
        object_store_memory=80 * 1024 * 1024,
    )
    try:
        counter = Counter.remote()
        initial = ray.get(counter.snapshot.remote(), timeout=60)
        # .remote() отправляет вызовы актору; ray.get() ждёт их результаты.
        calls = [counter.increment.remote() for _ in range(args.increments)]
        returned_values = ray.get(calls, timeout=60)
        final = ray.get(counter.snapshot.remote(), timeout=60)

        expected_values = list(range(1, args.increments + 1))
        if initial["value"] != 0:
            raise RuntimeError("Начальное значение счётчика отличается от нуля")
        if returned_values != expected_values:
            raise RuntimeError("Результаты операций отличаются от ожидаемых")
        if final["value"] != args.increments:
            raise RuntimeError("Итоговое значение счётчика отличается от ожидаемого")
        if final["pid"] == os.getpid():
            raise RuntimeError("Актор должен работать в отдельном процессе")

        result = {
            "scenario": "counter_without_failures",
            "status": "passed",
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "operations": args.increments,
            "initial_value": initial["value"],
            "expected_value": args.increments,
            "actual_value": final["value"],
            "operation_results_match": True,
            "driver_pid": os.getpid(),
            "actor_pid": final["pid"],
            "environment": {
                "python": platform.python_version(),
                "ray": ray.__version__,
                "ray_commit": ray.__commit__,
                "system": platform.system(),
                "architecture": platform.machine(),
            },
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Отправлено операций: {args.increments}")
        print(f"Ожидаемое значение: {args.increments}")
        print(f"Полученное значение: {final['value']}")
        print("Проверка пройдена: актор выполнил все операции в отдельном процессе.")
        print(f"Результат сохранён: {args.output}")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
