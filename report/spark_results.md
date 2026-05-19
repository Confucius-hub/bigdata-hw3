# ДЗ 3, часть 2 — Результаты Spark

## Конфигурация

- Машина: MacBook Apple Silicon, 8 ГБ RAM
- Docker Desktop с лимитом 6 ГБ RAM
- Spark 3.5.0 (bitnami/spark image)
- Worker: 4 GB memory, 4 cores

## Датасет

- Формат: CSV
- Размер: 50 ГБ (53,687,091,200 байт)
- Строк: ~530 миллионов
- Колонки: id, user_id, event_type, product_id, category, price, country, event_time

## Запросы

Один и тот же Spark job (`jobs/spark_job.py`) выполняет две группировки:

1. **По стране:** count events, count distinct users, sum revenue, avg price.
2. **По категории:** count events, count distinct users, sum revenue.

Результат пишется в Parquet и читается обратно через `.show()` для проверки.

## Результаты замеров

| Режим | Wall-clock |
|---|---|
| `--master local[*]` | **XX.XX seconds** |
| `--master spark://spark-master:7077` | **YY.YY seconds** |

(сюда подставить реальные значения после двух запусков)

## Анализ

На одной машине с 8 ГБ RAM кластерный режим может быть **медленнее** локального, потому что:

- Кластер в Docker — это эмуляция, Master и Worker делят те же CPU и RAM.
- Распределённый shuffle добавляет накладные расходы (сериализация, сетевая передача между контейнерами).
- В реальном кластере на разном железе кластерный режим почти всегда быстрее локального — но эмуляция на одной машине этого не показывает.

## Скриншоты

`docs/screenshots/spark_console.png` — вывод `spark-submit` с `Wall-clock`.
`docs/screenshots/spark_ui.png` — Spark Master UI на http://localhost:8080 во время запуска.

## Вывод

Стек поднят, датасет 50 ГБ обработан, два режима запуска сравнены. Реальная разница времени в эмуляции на одной машине невелика — преимущество распределённого режима проявляется только на разном железе.
