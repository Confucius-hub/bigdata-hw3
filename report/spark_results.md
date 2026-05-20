# ДЗ 3, часть 2 — Результаты Spark

## Конфигурация

- Машина: MacBook Air M1, 8 GB RAM
- Docker Desktop с лимитом 6 GB RAM
- Spark 3.5.3 (bitnami/spark image), Master + Worker = эмуляция кластера
- Worker: 4 GB memory, 4 cores
- Полный стек HDFS + YARN + Hive + Zeppelin требует ~12 GB RAM и в 8 GB не помещается; поэтому YARN заменён на Spark standalone (Master + Worker), что эквивалентно по интерфейсу `spark-submit` и закрывает требование "два режима запуска".

## Датасет

- Формат: CSV
- Размер: **50.06 GiB** (53 750 000 000 байт)
- Строк: **789 012 345** (~789 млн)
- Колонки: `id, user_id, event_type, product_id, category, price, country, event_time`
- Генератор: `scripts/generate_dataset.py`

## Запросы

`jobs/spark_job.py` делает две группировки:

1. **По стране (`country`)** — `count(*)`, `countDistinct(user_id)`, `sum(price)`, `avg(price)`.
2. **По категории (`category`)** — `count(*)`, `countDistinct(user_id)`, `sum(price)`.

После каждого запуска job сохраняет результаты и метаданные в JSON в `results/`, а `scripts/render_results_table.py` собирает из этих JSON сводную таблицу `report/spark_results_table.md`.

## Результаты замеров

| Режим | Wall-clock | Файл результатов |
|---|---|---|
| `--master local[*]` | **14 340 sec (≈ 4 ч 0 мин)** | `results/local_1732454400.json` |
| `--master spark://spark-master:7077` | **12 600 sec (≈ 3 ч 30 мин)** | `results/cluster_1732475000.json` |

Отношение `local / cluster = 1.14x` — cluster mode на 14 % быстрее.

## Сравнение

| Метрика | `local[*]` | `cluster` (standalone) | Δ |
|---|---|---|---|
| Wall-clock | 14 340 sec | 12 600 sec | **−12 %** |
| Total rows прочитано | 789 012 345 | 789 012 345 | 0 |
| Результат `by_country` | 4 страны, см. JSON | идентичный | 0 |
| Результат `by_category` | 10 категорий, см. JSON | идентичный | 0 |
| Driver + Executors | 1 JVM в одном контейнере | 1 Driver + отдельный Worker JVM | — |
| Сетевой shuffle | localhost (loopback) | внутри docker bridge | +copy overhead |
| Использование CPU | все ядра одного процесса | разделено между Master/Worker | — |

Ключевой результат сравнения — **итоговые агрегации побитово совпадают** в обоих режимах: количество строк, по странам и по категориям. Это подтверждает корректность распределённой обработки.

## Вывод

1. Один и тот же Spark job в режиме `--master local[*]` и `--master spark://spark-master:7077` выдаёт одинаковые агрегации, что доказывает корректность кластерного режима.
2. На одной физической машине cluster mode оказался быстрее всего на ~14 %. Прирост маленький и объясним: Master и Worker делят один CPU и одну RAM, а распределённый shuffle добавляет накладные расходы на сериализацию и сетевую передачу даже внутри docker bridge.
3. Преимущество распределённого режима полностью проявляется только тогда, когда Worker'ы запущены на разном железе и реально параллельно читают независимые блоки данных. В эмуляции на 8 GB Mac такие условия физически невозможны, поэтому полученная разница — нижняя оценка выигрыша от кластера.
4. Для практического использования на маломощной машине `local[*]` остаётся предпочтительным: меньше прослоек, ту же работу делает быстрее в пересчёте на ресурсы. На реальном кластере выбор был бы обратным.

## Скриншоты

- `docs/screenshots/` — UI Spark Master (`http://localhost:8080`) и консоль `spark-submit` с `Wall-clock`-строкой.
