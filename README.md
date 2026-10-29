# Ray: акторы и восстановление

Проект команды №10 по НИС «Распределённые системы 2».
[Задание P25-B](https://distsys.ru/osukhoroslov/nis2/src/branch/main/project-tasks/p25-b-ray-actors-recovery.md).
Участники: Зодоров Адам, Царапкин Максим, Шиляев Иван.

Будем сравнивать способы восстановления обучения после падения процесса.
Для этого используем небольшую нейросеть на MNIST: акторы Ray хранят модель
и позиции в данных, отдельные задачи считают градиенты.
Сейчас работает обучение без сбоев и проверка его результата. Перезапуск
с сохранением состояния проверен на простом счётчике; восстановление обучения
и вариант на обычных Python-процессах ещё предстоит написать.

- [Описание проекта](nis/project.md) — устройство программы и что хотим получить.
- [План эксперимента](nis/experiment-plan.md) — с чем сравниваем и что измеряем.
- [КТ1](nis/checkpoints/01.md) — проведённые проверки, результаты и оставшиеся задачи.
- [Код обучения](ray_ps/), [примеры со счётчиком](examples/), [тесты](tests/), [результаты КТ1](nis/evidence/kt1/).

## Среда и запуск

Все команды ниже выполняются из корня репозитория. Инструкция рассчитана
на Mac с Apple Silicon и Python 3.12. Для сборки нужны Git, curl, интернет,
место под исходники и кэш, а также Xcode Command Line Tools
(`xcode-select --install`). Версии проверенной среды указаны в [КТ1](nis/checkpoints/01.md#среда-проверок).

### Сборка Ray

В задании закреплён коммит `2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f`.
[Скрипт](scripts/build_ray_macos.sh) скачивает исходники и Bazel 7.5.0,
проверяет их SHA и контрольную сумму, собирает Ray и устанавливает его
в отдельное окружение:

```bash
bash scripts/build_ray_macos.sh
source work/venvs/ray-pinned/bin/activate
python -m pip install -r requirements-pilot-deps.txt
python -m pip check
```

Если Python 3.12 не находится в `PATH` или нужно ограничить ресурсы сборки:

```bash
PYTHON_BIN=/path/to/python3.12 RAY_BUILD_JOBS=4 RAY_BUILD_MEMORY_MB=8192 \
  RAY_BUILD_ROOT=/private/tmp/nis2-ray-build bash scripts/build_ray_macos.sh
```

По умолчанию Bazel использует 6 параллельных заданий и две трети RAM.
Это настройки планировщика, а не ограничения памяти со стороны ОС.
Каталог сборки должен иметь ASCII-путь без пробелов: в проверенном окружении
Bazel не запускался из пути с кириллицей. После установки этот каталог
нужен только для повторной сборки с кэшем.

Wheel сохраняется в `work/build/wheels/`, окружение — в `work/venvs/ray-pinned/`.
В `work/setup/` лежат отчёт `ray-pinned-macos-build.json`, список пакетов
`ray-pinned-macos-freeze.txt` и лог `build-logs/build-ray-macos.log`.
Каталог `work/` не входит в Git.

Сборка Ray по отдельным командам прошла. Скрипт, который их объединяет,
проверен через `bash -n`, но полностью ещё не запускался.
Полученный wheel рассчитан на CPython 3.12, macOS 15+ и arm64;
более старую macOS не проверяли. Подробности — в [отчёте сборки](nis/checkpoints/01.md#1-сборка-ray).

Для КТ1 используйте `requirements-pilot-deps.txt`: в нём закреплены зависимости
пилота без самого Ray. `requirements-ml.txt` содержит только ML-библиотеки,
а `requirements.txt` относится к старым примерам на Ray 2.59.0.
Не устанавливайте последний поверх среды КТ1.

### Запуск и проверка пилота

В активированном окружении:

```bash
python -m ray_ps.pilot --steps 20 --workers 2 --batch-size 32 --output runs/ps-01
python -m ray_ps.verify --run-dir runs/ps-01
python -m ray_ps.pilot --steps 20 --workers 2 --batch-size 32 --output runs/ps-02
python -m ray_ps.verify --run-dir runs/ps-02
python -m ray_ps.check_results --first runs/ps-01 --second runs/ps-02 --output runs/pilot-checks.json
python -m unittest discover -s tests -v
```

Первый запуск скачает MNIST в `runs/data/`. Для каждого запуска нужен новый
каталог `--output`. Параметры `--steps`, `--workers`, `--batch-size` и `--seed`
меняют конфигурацию опыта. Пилот проверяет SHA Ray; флаг `--allow-other-ray`
отключает эту проверку для разработки.

| Файл результата | Содержимое |
| --- | --- |
| `config.json` | Параметры, версии, seed, хеши данных и исходников |
| `steps.jsonl` | Батчи, номера шагов и версий модели, позиции источников, время и хеши весов |
| `summary.json` | Итог запуска и p95/p99 времени шага |
| `final-state.pt` | Веса, momentum, номер шага и позиции источников |
| `verification.json` | Результат проверки по последовательному эталону |

`completed_pending_independent_verification` в `summary.json` означает, что
программа завершилась. Результат проверки нужно смотреть в `verification.json`:
`verify` перезаписывает его и при успехе, и при ошибке.
`check_results` сравнивает два запуска и проверяет, что намеренно испорченный
momentum обнаруживается. Исходные результаты при этом не меняются.

### Примеры со счётчиком

```bash
python examples/counter.py --output runs/counter.json
python examples/counter_restart.py --output runs/counter-restart.json
python examples/counter_checkpoint.py --output runs/counter-checkpoint.json
```

Первый пример выполняет 100 прибавлений. Второй показывает потерю состояния
после перезапуска: `50 → сбой → 0 → 50`. Третий сохраняет счётчик в файл:
`50 → сбой → 50 → 100`. Для короткого опыта в последних двух командах можно
добавить `--before-crash 7 --after-restart 3`, для первого — задать `--increments`.
Устройство checkpoint и команды замера восстановления описаны в [КТ1](nis/checkpoints/01.md#технические-проверки).

## Источники и лицензии

Архитектура [ConvNet](ray_ps/model.py) взята из [примера Parameter Server](https://github.com/ray-project/ray/blob/2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f/doc/source/ray-core/examples/plot_parameter_server.ipynb)
проекта Ray (The Ray Authors), Apache-2.0; [текст лицензии](LICENSES/Ray.txt).
В модели изменён выход на logits для cross entropy, добавлены функции
копирования и хеширования состояния и настройки воспроизводимости на CPU.
Код источников батчей, координации шагов, журналирования и проверки написан
для этого проекта.

[MNIST](https://yann.lecun.org/exdb/mnist/) скачивается через
[torchvision](https://docs.pytorch.org/vision/stable/generated/torchvision.datasets.MNIST.html)
и не хранится в репозитории. Лицензии библиотек поставляются с пакетами.
Для сборки использованы [инструкция Ray](https://github.com/ray-project/ray/blob/2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f/doc/source/ray-contribute/development.md)
и [релиз Bazel 7.5.0](https://github.com/bazelbuild/bazel/releases/tag/7.5.0).
Описание использования Codex — в [отчёте КТ1](nis/checkpoints/01.md#применение-генеративной-модели).
