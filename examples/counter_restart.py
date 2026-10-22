"""Падение актора: процесс перезапускается, а счётчик начинается с нуля."""

import argparse
import json
import os
import platform
import secrets
from datetime import datetime, timezone
from pathlib import Path

import ray


@ray.remote(num_cpus=1, max_restarts=1, max_task_retries=0)
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

    def snapshot(self):
        return {"value": self.value, "pid": os.getpid()}

    def crash(self):
        # Завершаем только процесс этого актора. Ответ клиенту не вернётся.
        os._exit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-crash", type=int, default=50)
    parser.add_argument("--after-restart", type=int, default=50)
    parser.add_argument(
        "--output", type=Path, default=Path("runs/counter-restart.json")
    )
    args = parser.parse_args()
    if args.before_crash < 1 or args.after_restart < 1:
        parser.error("число операций до и после падения должно быть положительным")

    os.environ.setdefault("RAY_USAGE_STATS_ENABLED", "0")
    os.environ.setdefault("RAY_AUTH_TOKEN", secrets.token_urlsafe(32))
    try:
        ray.init(
            address="local",
            num_cpus=1,
            include_dashboard=False,
            log_to_driver=False,
            object_store_memory=80 * 1024 * 1024,
        )
        counter = Counter.remote()
        initial = ray.get(counter.snapshot.remote(), timeout=60)
        if initial["value"] != 0 or initial["pid"] == os.getpid():
            raise RuntimeError("Ожидался новый счётчик в отдельном процессе")

        # Сначала получаем подтверждения всех операций, затем вызываем падение.
        before_calls = [counter.increment.remote() for _ in range(args.before_crash)]
        before_results = ray.get(before_calls, timeout=60)
        before_crash = ray.get(counter.snapshot.remote(), timeout=60)
        if before_results != list(range(1, args.before_crash + 1)):
            raise RuntimeError("Ответы до падения отличаются от ожидаемых")
        if before_crash["value"] != args.before_crash:
            raise RuntimeError("Неверное значение перед падением")
        if before_crash["pid"] != initial["pid"]:
            raise RuntimeError("Процесс актора сменился до запланированного падения")
        print(f"До падения: {before_crash['value']}; PID: {before_crash['pid']}")

        # У этого вызова нет повторов: второй процесс не должен упасть ещё раз.
        print("Вызываем падение процесса актора...", flush=True)
        try:
            ray.get(counter.crash.remote(), timeout=60)
        except ray.exceptions.RayActorError as error:
            crash_error = type(error).__name__
            print(f"Падение подтверждено: {crash_error}")
        else:
            raise RuntimeError("Вызов crash неожиданно завершился успешно")

        # Повторять разрешаем только чтение: актор ещё может перезапускаться.
        # Весь период ожидания ограничен timeout=60 у ray.get.
        ready = counter.snapshot.options(max_task_retries=-1).remote()
        after_restart = ray.get(ready, timeout=60)
        if after_restart["pid"] in (before_crash["pid"], os.getpid()):
            raise RuntimeError("Ожидался новый отдельный процесс актора")
        if after_restart["value"] != 0:
            raise RuntimeError("После перезапуска без сохранения ожидался ноль")
        print(
            f"После перезапуска: {after_restart['value']}; "
            f"PID: {after_restart['pid']}"
        )

        # Используем прежний handle counter: Ray сам перезапустил актор.
        after_calls = [counter.increment.remote() for _ in range(args.after_restart)]
        after_results = ray.get(after_calls, timeout=60)
        final = ray.get(counter.snapshot.remote(), timeout=60)
        if after_results != list(range(1, args.after_restart + 1)):
            raise RuntimeError("Ответы после перезапуска отличаются от ожидаемых")
        if final["value"] != args.after_restart:
            raise RuntimeError("Итоговое значение отличается от ожидаемого")
        if final["pid"] != after_restart["pid"]:
            raise RuntimeError("Процесс актора неожиданно сменился ещё раз")

        acknowledged = len(before_results) + len(after_results)
        lost_increments = acknowledged - final["value"]
        result = {
            "scenario": "actor_restart_without_persistence",
            "status": "passed",
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "operations_before_crash": args.before_crash,
            "operations_after_restart": args.after_restart,
            "acknowledged_increment_operations": acknowledged,
            "expected_value_if_state_preserved": acknowledged,
            "expected_value_after_fresh_restart": args.after_restart,
            "actual_value": final["value"],
            "lost_acknowledged_increments": lost_increments,
            "initial": initial,
            "before_crash": before_crash,
            "after_restart": after_restart,
            "final": final,
            "crash_error": crash_error,
            "operation_results_match": True,
            "actor_pid_changed": True,
            "actor_state_reset": True,
            "driver_pid": os.getpid(),
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
        print(f"После ещё {args.after_restart} операций: {final['value']}")
        print(f"Всего подтверждено прибавлений: {acknowledged}")
        print(f"Потеряно в состоянии после сбоя: {lost_increments}")
        print("Проверка пройдена: процесс перезапущен, прежнее состояние потеряно.")
        print(f"Результат сохранён: {args.output}")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
