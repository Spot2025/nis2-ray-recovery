# Среда первого примера

- Дата подготовки: 22.10.2026.
- Машина проверки: macOS 15.7.9, arm64.
- Python: 3.12.11, отдельное окружение `.venv/`.
- Ray первого примера: 2.59.0 из PyPI.
- Зависимости: `requirements.txt`.
- Фактическую ревизию установленного Ray программа записывает в JSON результата.

## Основная ревизия из задания

В [карточке задания](https://distsys.ru/osukhoroslov/nis2/src/branch/main/project-tasks/p25-b-ray-actors-recovery.md)
закреплён Ray `2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f`.
В `_version.py` этой ревизии указана версия `3.0.0.dev0`.

По [официальной инструкции установки конкретного коммита](https://docs.ray.io/en/latest/ray-overview/installation.html#installing-from-a-specific-commit)
проверен адрес:

```text
https://s3-us-west-2.amazonaws.com/ray-wheels/master/2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f/ray-3.0.0.dev0-cp312-cp312-macosx_12_0_arm64.whl
```

Команда проверки:

```bash
curl -I --fail --max-time 25 https://s3-us-west-2.amazonaws.com/ray-wheels/master/2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f/ray-3.0.0.dev0-cp312-cp312-macosx_12_0_arm64.whl
```

Полученный ответ: **HTTP 404 Not Found**. Это свидетельство отсутствия сборки
по данному адресу, а не доказательство невозможности работы указанной ревизии.

Для первого примера использован доступный релиз 2.59.0. Сохранённый результат
получен на этой версии; запуск основной ревизии задания не проверен.

## Границы первого результата

Пример проверяет работу одного обычного синхронного актора и одного клиента
без падений. В нём ещё нет эксперимента по восстановлению, пользовательского
журнала операций, checkpoint/replay, процессного baseline и p95/p99.
