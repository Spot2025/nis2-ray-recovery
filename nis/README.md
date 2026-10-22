# Технические материалы

- [Среда и версии](environment.md).
- [Результат проверки счётчика](evidence/counter.json).

## Выполненная проверка

```bash
python examples/counter.py --output nis/evidence/counter.json
```

Ожидалось: начальное значение 0, результаты операций от 1 до 100,
итоговое значение 100, отдельный процесс актора.

Получено: все проверки пройдены. Среда и результат сохранены в
[`evidence/counter.json`](evidence/counter.json).
