# Использованные материалы

`ray_ps/model.py`: архитектура ConvNet адаптирована из официального примера
[Parameter Server](https://github.com/ray-project/ray/blob/2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f/doc/source/ray-core/examples/plot_parameter_server.ipynb)
проекта Ray (The Ray Authors), Apache-2.0; [текст лицензии](LICENSES/Ray.txt).
Изменения: выход — logits для cross entropy, отдельные функции состояния,
детерминированные настройки CPU и контрольные суммы.

Схема parameter server опирается на этот пример. Код источников батчей,
идентификаторов, журналирования, последовательной проверки и измерений
разработан для данного проекта. Пример не реализует полное восстановление обучения.

MNIST загружается через `torchvision.datasets.MNIST` и не включается в Git.
Источник: [описание MNIST](https://yann.lecun.org/exdb/mnist/),
[загрузчик torchvision](https://docs.pytorch.org/vision/stable/generated/torchvision.datasets.MNIST.html).
Лицензии зависимостей поставляются с соответствующими пакетами.
