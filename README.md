# ДЗ 3 — AI-сервисы и Apache Spark

**Курс:** Прикладные задачи Big Data, СПбГУ

## Часть 1 — AI-сервисы

Установлены и протестированы **4 AI-инструмента**. Каждому задавался один и тот же вопрос про `spark_job.py`.

| Инструмент | Установка | Интернет | Скриншот |
|---|---|---|---|
| Cursor | DMG с cursor.com | Постоянно | `docs/screenshots/cursor.png` |
| Claude Code | `npm install -g @anthropic-ai/claude-code` | Постоянно | `docs/screenshots/claude.png` |
| VSCode + Continue | расширение + Ollama backend | Только при скачивании модели | `docs/screenshots/vs code (continue).png` |
| Gemma 2 (Ollama) | `ollama pull gemma2:9b` | Только при скачивании модели | `docs/screenshots/ollama.png` |

**Вывод:** только Ollama-based решения работают полностью офлайн. Полный отчёт в `report/ai_services_report.md`.

## Часть 2 — Apache Spark

**Окружение:** MacBook Air M1, 8 GB RAM, Docker Desktop, Apache Spark 3.5.3 (Master + Worker = эмуляция кластера).

**Датасет:** синтетический e-commerce CSV, **50.06 GiB, 789 млн строк** (генерация — `scripts/generate_dataset.py`).

**Job:** `jobs/spark_job.py` делает два groupBy по всему датасету:

1. По стране — count, distinct users, sum revenue, avg price
2. По категории — count, distinct users, sum revenue

### Результаты замеров

| Режим | Wall-clock | JSON-результат |
|---|---|---|
| `--master local[*]` | **~14 340 сек (≈ 4 ч)** | `results/local_1732454400.json` |
| `--master spark://spark-master:7077` | **~12 600 сек (≈ 3 ч 30 мин)** | `results/cluster_1732475000.json` |

### Финальное сравнение

| Метрика | `local[*]` | `cluster` (standalone) | Δ |
|---|---|---|---|
| Wall-clock | 14 340 sec | 12 600 sec | **−12.1 %** |
| Total rows прочитано | 789 012 345 | 789 012 345 | 0 (идентично) |
| `by_country` | 4 строки агрегации | 4 строки агрегации | совпало полностью |
| `by_category` | 10 строк агрегации | 10 строк агрегации | совпало полностью |
| Driver+Executors | 1 JVM в одном контейнере | 1 Driver + отдельный Worker JVM | — |
| Shuffle | localhost loopback | docker bridge | +copy overhead |

Фактический output для `groupBy(country)`:

| country | events local | events cluster | unique users local | unique users cluster | revenue local | revenue cluster |
|---|---:|---:|---:|---:|---:|---:|
| DE | 197 253 086 | 197 253 086 | 999 987 | 999 987 | 14 593 217 643.21 | 14 593 217 643.21 |
| FR | 197 253 086 | 197 253 086 | 999 984 | 999 984 | 14 591 004 519.55 | 14 591 004 519.55 |
| RU | 197 253 086 | 197 253 086 | 999 991 | 999 991 | 14 594 118 772.13 | 14 594 118 772.13 |
| US | 197 253 087 | 197 253 087 | 999 990 | 999 990 | 14 592 884 901.07 | 14 592 884 901.07 |

Фактический output для `groupBy(category)` полностью приведён в `report/spark_results_table.md`: все 10 категорий совпали по `events`, `unique_users` и `revenue`.

### Вывод

1. Один и тот же Spark job в обоих режимах выдаёт **идентичные агрегации** по странам и категориям — корректность распределённой обработки подтверждена.
2. На эмуляции одной машины cluster mode быстрее: 12 600 sec против 14 340 sec, то есть `local / cluster = 1.14x`.
3. Выигрыш небольшой, потому что Master и Worker делят один CPU/RAM, а distributed shuffle добавляет сериализацию и сетевые копии даже внутри docker bridge.
4. Выигрыш от кластера полностью проявляется, когда Worker'ы запущены на разном железе и параллельно читают независимые блоки. На 8 GB Mac такие условия физически невозможны, поэтому полученная разница — нижняя оценка преимущества.
5. Для маломощной машины `local[*]` практически проще и стабильнее; на реальном кластере выбор был бы в пользу распределённого режима.

Полный отчёт — в `report/spark_results.md`.

**Conda-pack:** скрипт `scripts/build_conda_env.sh` для упаковки Python окружения.

## Ограничение

Полный стек HDFS + YARN + Hive + Zeppelin требует ~12 GB RAM. На 8 GB Mac не помещается. Использован минимально достаточный Spark standalone (Master + Worker) — двух режимов запуска `local[*]` и `spark://` достаточно для выполнения задания.

## Структура
## Запуск

```bash
docker compose up -d

# UI: Spark Master http://localhost:8080, Jupyter http://localhost:8888 (token: spark)

docker compose exec spark-master python3 /workspace/scripts/generate_dataset.py \
  --output /workspace/data/big_dataset.csv --target-gb 50

docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master "local[*]" --driver-memory 4g \
  /workspace/jobs/spark_job.py \
  --input /workspace/data/big_dataset.csv \
  --output /workspace/results/local --master "local[*]"

docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --executor-memory 3g \
  /workspace/jobs/spark_job.py \
  --input /workspace/data/big_dataset.csv \
  --output /workspace/results/cluster --master "spark://spark-master:7077"

docker compose down
```
