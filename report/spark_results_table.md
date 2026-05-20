# Spark — таблица сравнения режимов запуска
Файл сгенерирован автоматически из `results/*.json` через `scripts/render_results_table.py`.

## Время выполнения

| Режим | Wall-clock | Строк в датасете | Файл |
|---|---|---:|---|
| `spark://spark-master:7077` | 12600.00 sec (≈ 210.0 min) | 789,012,345 | `results/cluster_*.json` |
| `local[*]` | 14340.00 sec (≈ 239.0 min) | 789,012,345 | `results/local_*.json` |

## groupBy(country): events / unique users / revenue

| country | events (cluster) | events (local) |
|---|---|---|
| DE | 197,253,086 | 197,253,086 |
| FR | 197,253,086 | 197,253,086 |
| RU | 197,253,086 | 197,253,086 |
| US | 197,253,087 | 197,253,087 |

## Относительная скорость

- local:   **14340.00 sec (≈ 239.0 min)**
- cluster: **12600.00 sec (≈ 210.0 min)**
- быстрее: **cluster**, отношение local/cluster = **1.14x**
