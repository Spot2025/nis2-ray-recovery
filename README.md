# НИС РС-2 · P25-B: Ray — акторы и восстановление

Команда №10: Зодоров Адам, Царапкин Максим, Шиляев Иван.

Исследуем восстановление состояния при сбоях процессов на примере синхронного
обучения небольшой ConvNet на MNIST. Ray выполняет задачи и акторы;
команда разрабатывает приложение, стратегии сохранения, процессный baseline
и проверку правильности.

Документы КТ1:

- [Описание проекта](nis/project.md) — задание, устройство пилота, ожидаемые результаты и ограничения.
- [План эксперимента](nis/experiment-plan.md) — варианты сравнения, нагрузки, метрики и серии.
- [Отчёт КТ1](nis/checkpoints/01.md) — команды и результаты проверок, ресурсы, риски и вклад участников.

На назначенном коммите Ray выполнены два пилота по 20 шагов с независимой
последовательной проверкой. Восстановление обучения и процессный baseline
ещё не реализованы; сохранение состояния пока проверено на счётчике.

## Среда и запуск

Все команды выполняются в корне репозитория. Для нативной сборки нужны Mac
с Apple Silicon, нативный Python 3.12 с `venv`/`pip`, Xcode Command Line Tools,
Git, curl, интернет и место под исходники и кэш сборки. При отсутствии CLT
выполнить `xcode-select --install`; полный Xcode.app не требуется.
Версии и характеристики проверенной машины приведены в [отчёте КТ1](nis/checkpoints/01.md#среда-проверок).

### Сборка Ray

Основная ревизия — `2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f`.
[Скрипт сборки](scripts/build_ray_macos.sh) создаёт отдельную среду,
проверяет checksum Bazel 7.5.0 и SHA исходников, собирает и устанавливает
wheel, затем проверяет `ray.__commit__` и зависимости:

```bash
bash scripts/build_ray_macos.sh
source work/venvs/ray-pinned/bin/activate
python -m pip install -r requirements-pilot-deps.txt
python -m pip check
python -c 'import ray; print(ray.__version__, ray.__commit__); assert ray.__commit__ == "2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f"'
```

Скрипт не устанавливает системные пакеты и не меняет учебную `.venv/`.
По умолчанию используются 6 параллельных заданий и две трети RAM — это
настройки планировщика Bazel, а не строгие лимиты ОС. При необходимости
задать Python, ресурсы и постоянный каталог сборки можно так:

```bash
PYTHON_BIN=/path/to/python3.12 RAY_BUILD_JOBS=4 RAY_BUILD_MEMORY_MB=8192 \
  RAY_BUILD_ROOT=/private/tmp/nis2-ray-build bash scripts/build_ray_macos.sh
```

Bazel не запустился из пути с кириллицей, поэтому дерево сборки и кэш находятся
в ASCII-каталоге `/private/tmp`; `RAY_BUILD_ROOT` должен быть без пробелов.
Существующие исходники проверяются на ревизию и посторонние изменения.
Для работы установленного Ray временное дерево не нужно; его можно оставить
для повторной сборки с кэшем.

| Локальный результат сборки | Содержание |
| --- | --- |
| `work/build/wheels/` | Wheel Ray |
| `work/venvs/ray-pinned/` | Окружение Python |
| `work/setup/build-logs/build-ray-macos.log` | Лог сборки |
| `work/setup/ray-pinned-macos-build.json` | Версия, SHA и checksum пакета |
| `work/setup/ray-pinned-macos-freeze.txt` | Установленные зависимости |

Каталог `work/` исключён из Git. Версии инструментов закреплены в скрипте;
часть зависимостей upstream разрешается при сборке, побитовая повторяемость
wheel не предполагается. Собирается Python Ray Core без отдельного C++ API,
Redis для upstream-тестов, Java API и frontend dashboard.

Сборка по отдельным командам проверена; объединяющий их скрипт прошёл только
`bash -n`, повторная полная сборка через него ещё не выполнялась.
Проверенный wheel предназначен для CPython 3.12 / macOS 15+ / arm64;
совместимость с более старой macOS не подтверждена.
[Свидетельство сборки и ограничения](nis/checkpoints/01.md#1-сборка-и-запуск-артефакта).

`requirements-pilot-deps.txt` фиксирует зависимости пилота без Ray;
`requirements-ml.txt` содержит только прямые ML-зависимости.
Корневой `requirements.txt` относится к исторической среде с Ray 2.59.0 —
не устанавливать его поверх среды КТ1. Исторические отчёты находятся
в `nis/evidence/`, результаты назначенной ревизии — в `nis/evidence/kt1/`.

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

Первый запуск скачивает MNIST в `runs/data/`; каждый каталог `--output`
должен быть новым. По умолчанию пилот проверяет SHA Ray. Флаг
`--allow-other-ray` пригоден только для разработки: такой запуск
не подтверждает работу назначенной ревизии. Параметры `--seed`, `--workers`,
`--steps` и `--batch-size` задают конфигурацию опыта.
[Устройство и файлы пилота](nis/project.md#устройство-приложения).

### Результаты запусков

| Файл в каталоге запуска | Что содержит |
| --- | --- |
| `config.json` | Параметры, версии, seed, хеши данных и исходников |
| `steps.jsonl` | ID/версии шагов, батчи, прогресс источников, времена и хеши весов |
| `summary.json` | Итог запуска и описательные p95/p99 |
| `final-state.pt` | Веса, momentum, номер шага и позиции источников |
| `verification.json` | Результат независимой проверки, ошибки и допуски |

Статус `completed_pending_independent_verification` означает завершение пилота;
корректность подтверждает `verification.json`. Проверяющий инструмент обновляет
этот файл, в том числе при ошибке. `check_results` сравнивает повторные запуски
побитово и проверяет обнаружение испорченного momentum во временной копии.
Квантили короткого пилота включают холодный старт и не служат сравнением
производительности. Правила эталона и измерений — в [плане](nis/experiment-plan.md).

## Учебные примеры счётчика

```bash
python examples/counter.py --output runs/counter.json
python examples/counter_restart.py --output runs/counter-restart.json
python examples/counter_checkpoint.py --output runs/counter-checkpoint.json
```

Первый пример проверяет 100 прибавлений в отдельном процессе. Во втором
`os._exit(1)` вызывает один перезапуск: `50 → сбой → 0 → 50`. В третьем
сохранение после каждой операции даёт `50 → сбой → 50 → 100`.
Параметры: `--increments 250` для первого примера, `--before-crash 7
--after-restart 3` для двух остальных. Команды проб времени восстановления,
ожидания и результаты всех проверок — в [отчёте КТ1](nis/checkpoints/01.md#технические-проверки).

`@ray.remote` создаёт актор, `.remote()` отправляет вызов, `ray.get()` ждёт
ответ. В сценариях сбоя разрешён один рестарт; повторы изменяющих методов
отключены. Чтение состояния после сбоя повторяется с ожиданием до 60 секунд.
Смена PID подтверждает запуск нового процесса; `status: passed` у restart
означает подтверждение ожидаемой потери состояния.

Checkpoint хранится в `runs/counter-checkpoint-<идентификатор>/state.json`
как `{"value": 50}`. Перед ответом новое значение записывается в соседний
`state.tmp`, синхронизируется через `flush`/`fsync` и публикуется `os.replace`.
Новый процесс читает тот же checkpoint; незавершённый временный файл
игнорируется, повреждённый основной файл вызывает ошибку. Итоговый JSON-отчёт
клиента — отдельный файл, он не используется для восстановления.

Сбои происходят между группами полностью подтверждённых операций на одной
машине с сохранным диском. `fsync` каталога не выполняется; отказ питания,
диска, узла, перенос на другую машину и потеря ответа не проверены.
Защиты от повторного прибавления при неизвестном исходе нет.
Тесты ошибок записи используют подмену вызовов ОС, а не выключение машины.

## Источники и лицензии

[ConvNet](ray_ps/model.py) адаптирована из официального примера
[Parameter Server](https://github.com/ray-project/ray/blob/2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f/doc/source/ray-core/examples/plot_parameter_server.ipynb)
Ray (The Ray Authors), Apache-2.0; [текст лицензии](LICENSES/Ray.txt).
Изменены выход на logits для cross entropy, функции состояния, настройки CPU
и контрольные суммы. Источники батчей, ID, журналирование, последовательная
проверка и измерения разработаны для проекта.

[MNIST](https://yann.lecun.org/exdb/mnist/) загружается через
[torchvision.datasets.MNIST](https://docs.pytorch.org/vision/stable/generated/torchvision.datasets.MNIST.html)
и не включён в Git. Лицензии зависимостей поставляются с пакетами.
Сборка основана на [инструкции назначенной ревизии Ray](https://github.com/ray-project/ray/blob/2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f/doc/source/ray-contribute/development.md);
Bazel и checksum — из [релиза 7.5.0](https://github.com/bazelbuild/bazel/releases/tag/7.5.0).
Сохранение акторов описано в [документации Ray](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html#actor-checkpointing),
атомарная замена файла — в [документации Python](https://docs.python.org/3.12/library/os.html#os.replace).
Применение Codex и ответственность команды раскрыты в [отчёте КТ1](nis/checkpoints/01.md#применение-генеративной-модели).
