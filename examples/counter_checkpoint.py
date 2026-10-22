"""Падение актора: счётчик восстанавливается из файла после перезапуска."""

import argparse
import json
import os
import platform
import secrets
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import ray


def load_checkpoint(path):
    try:
        contents = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return 0
    # Повреждённый файл — ошибка, а не повод незаметно начать с нуля.
    state = json.loads(contents)
    if (
        not isinstance(state, dict)
        or type(state.get("value")) is not int
        or state["value"] < 0
    ):
        raise ValueError("Checkpoint должен содержать целое неотрицательное value")
    return state["value"]


def save_checkpoint(path, value):
    # Один актор пишет в свой файл. Временный файл находится на том же диске.
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump({"value": value}, file)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    # Читатель увидит либо прежний, либо полностью записанный новый checkpoint.
    os.replace(temporary, path)


@ray.remote(num_cpus=1, max_restarts=1, max_task_retries=0)
class Counter:
    def __init__(self, checkpoint_path):
        self.checkpoint_path = Path(checkpoint_path)
        self.value = load_checkpoint(self.checkpoint_path)

    def increment(self):
        next_value = self.value + 1
        save_checkpoint(self.checkpoint_path, next_value)
        # Если запись не удалась, не меняем память и не подтверждаем прибавление.
        self.value = next_value
        return self.value

    def snapshot(self):
        return {"value": self.value, "pid": os.getpid()}

    def crash(self):
        os._exit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-crash", type=int, default=50)
    parser.add_argument("--after-restart", type=int, default=50)
    parser.add_argument(
        "--output", type=Path, default=Path("runs/counter-checkpoint.json")
    )
    args = parser.parse_args()
    if args.before_crash < 1 or args.after_restart < 1:
        parser.error("число операций до и после падения должно быть положительным")

    # Каждый запуск сценария начинает с чистого каталога; рестарт актора
    # внутри этого запуска использует прежний путь и сохранённый файл.
    runs_dir = Path("runs").resolve()
    runs_dir.mkdir(parents=True, exist_ok=True)
    state_dir = Path(tempfile.mkdtemp(prefix="counter-checkpoint-", dir=runs_dir))
    checkpoint_path = state_dir / "state.json"
    checkpoint_relative = str(checkpoint_path.relative_to(Path.cwd()))
    print(f"Файл состояния: {checkpoint_relative}", flush=True)

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
        counter = Counter.remote(str(checkpoint_path))
        initial = ray.get(counter.snapshot.remote(), timeout=60)
        if initial["value"] != 0 or initial["pid"] == os.getpid():
            raise RuntimeError("Ожидался новый счётчик в отдельном процессе")

        before_calls = [counter.increment.remote() for _ in range(args.before_crash)]
        before_results = ray.get(before_calls, timeout=60)
        before_crash = ray.get(counter.snapshot.remote(), timeout=60)
        saved_before = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if before_results != list(range(1, args.before_crash + 1)):
            raise RuntimeError("Ответы до падения отличаются от ожидаемых")
        if before_crash["value"] != args.before_crash:
            raise RuntimeError("Неверное значение перед падением")
        if saved_before != {"value": args.before_crash}:
            raise RuntimeError("Checkpoint перед падением не совпал со счётчиком")
        if before_crash["pid"] != initial["pid"]:
            raise RuntimeError("Процесс актора сменился до запланированного падения")
        print(f"До падения: {before_crash['value']}; PID: {before_crash['pid']}")

        print("Вызываем падение процесса актора...", flush=True)
        try:
            ray.get(counter.crash.remote(), timeout=60)
        except ray.exceptions.RayActorError as error:
            crash_error = type(error).__name__
            print(f"Падение подтверждено: {crash_error}")
        else:
            raise RuntimeError("Вызов crash неожиданно завершился успешно")

        # Повторы разрешены только чтению, пока запускается новый процесс.
        ready = counter.snapshot.options(max_task_retries=-1).remote()
        after_restart = ray.get(ready, timeout=60)
        saved_after_restart = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if after_restart["pid"] in (before_crash["pid"], os.getpid()):
            raise RuntimeError("Ожидался новый отдельный процесс актора")
        if after_restart["value"] != args.before_crash:
            raise RuntimeError("После перезапуска не восстановлено прежнее значение")
        if saved_after_restart != saved_before:
            raise RuntimeError("Checkpoint неожиданно изменился при перезапуске")
        print(
            f"После перезапуска: {after_restart['value']}; "
            f"PID: {after_restart['pid']}"
        )

        # Прежний handle теперь обращается к новому процессу с загруженным состоянием.
        after_calls = [counter.increment.remote() for _ in range(args.after_restart)]
        after_results = ray.get(after_calls, timeout=60)
        final = ray.get(counter.snapshot.remote(), timeout=60)
        expected = args.before_crash + args.after_restart
        if after_results != list(range(args.before_crash + 1, expected + 1)):
            raise RuntimeError("Ответы после перезапуска отличаются от ожидаемых")
        if final["value"] != expected:
            raise RuntimeError("Итоговое значение отличается от ожидаемого")
        if final["pid"] != after_restart["pid"]:
            raise RuntimeError("Процесс актора неожиданно сменился ещё раз")
        saved_final = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if saved_final != {"value": expected}:
            raise RuntimeError("Итоговый checkpoint не совпал со счётчиком")

        acknowledged = len(before_results) + len(after_results)
        lost_increments = acknowledged - final["value"]
        result = {
            "scenario": "actor_restart_with_file_checkpoint",
            "status": "passed",
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "operations_before_crash": args.before_crash,
            "operations_after_restart": args.after_restart,
            "acknowledged_increment_operations": acknowledged,
            "expected_value": expected,
            "actual_value": final["value"],
            "lost_acknowledged_increments": lost_increments,
            "initial": initial,
            "before_crash": before_crash,
            "after_restart": after_restart,
            "final": final,
            "checkpoint": {
                "path": checkpoint_relative,
                "storage": "local_filesystem_same_machine",
                "saved_after_each_increment": True,
                "before_crash": saved_before,
                "after_restart": saved_after_restart,
                "final": saved_final,
            },
            "crash_error": crash_error,
            "operation_results_match": True,
            "actor_pid_changed": True,
            "actor_state_restored": True,
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
        print("Проверка пройдена: новый процесс восстановил счётчик из файла.")
        print(f"Результат сохранён: {args.output}")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
