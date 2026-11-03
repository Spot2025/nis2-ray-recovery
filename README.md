# Ray: акторы и восстановление

Проект команды №10 по НИС «Распределённые системы 2».
[Задание P25-B](https://distsys.ru/osukhoroslov/nis2/src/branch/main/project-tasks/p25-b-ray-actors-recovery.md).
Участники: Зодоров Адам, Царапкин Максим, Шиляев Иван.

Сравним обычный перезапуск процесса и восстановление из checkpoint
на примере обучения нейросети. Небольшая сеть уже обучается на MNIST,
её результат сверяем с последовательным эталоном. Восстановление пока
проверили на счётчике; дальше добавим его в обучение и сделаем вариант
на обычных Python-процессах.

- [Описание проекта](nis/project.md) — устройство программы и что хотим получить.
- [План эксперимента](nis/experiment-plan.md) — с чем сравниваем и что измеряем.
- [КТ1](nis/checkpoints/01.md) — проведённые проверки, результаты и оставшиеся задачи.
- [Код обучения](ray_ps/), [примеры со счётчиком](examples/), [тесты](tests/), [результаты КТ1](nis/evidence/kt1/).

## Запуск

Команды выполняются из корня репозитория на Mac с Apple Silicon и Python 3.12.
Для сборки нужны Git, curl, интернет, место под исходники и кэш,
Xcode Command Line Tools (`xcode-select --install`).
[Проверенные версии и машина](nis/checkpoints/01.md#ресурсы).

### Сборка Ray

[Скрипт](scripts/build_ray_macos.sh) скачивает исходники Ray и Bazel 7.5.0,
проверяет SHA ревизии и контрольную сумму Bazel, затем собирает Ray
и устанавливает его в отдельное окружение:

```bash
bash scripts/build_ray_macos.sh
source work/venvs/ray-pinned/bin/activate
python -m pip install -r requirements-pilot-deps.txt
python -m pip check
```

Для этого запуска нужен `requirements-pilot-deps.txt` с зависимостями без Ray.
`requirements-ml.txt` содержит только ML-библиотеки. `requirements.txt`
используется в старых примерах на Ray 2.59.0; его установка заменит нужную
версию Ray. Настройки и результат сборки — в [КТ1](nis/checkpoints/01.md#1-сборка-ray).

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

При первом запуске MNIST скачается в `runs/data/`. Для каждого запуска укажите
новый каталог `--output`. Конфигурацию можно менять параметрами `--steps`,
`--workers`, `--batch-size` и `--seed`. Пилот проверяет SHA Ray; для разработки
проверку можно отключить флагом `--allow-other-ray`.

Форматы результатов и работа проверки описаны в [проекте](nis/project.md#файлы-запуска).

### Примеры со счётчиком

```bash
python examples/counter.py --output runs/counter.json
python examples/counter_restart.py --output runs/counter-restart.json
python examples/counter_checkpoint.py --output runs/counter-checkpoint.json
```

Первый пример делает 100 прибавлений (`--increments` меняет их число).
Остальные сравнивают перезапуск с потерей состояния и с checkpoint;
`--before-crash 7 --after-restart 3` сокращает опыт.
[Результаты и устройство checkpoint](nis/checkpoints/01.md#2-счётчик-и-перезапуск-актора),
[замер времени восстановления](nis/checkpoints/01.md#5-данные-для-метрик).

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
Описание использования Codex — в [отчёте КТ1](nis/checkpoints/01.md#описание-применения-генеративной-модели).
