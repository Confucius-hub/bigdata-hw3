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

### Пример вывода (groupBy по категории)

Все 10 категорий обработаны, ~78.9M событий и ~1M уникальных пользователей в каждой.

### Сравнение

| Метрика | `local[*]` | `cluster` (standalone) | Δ |
|---|---|---|---|
| Wall-clock | 14 340 sec | 12 600 sec | **−12 %** (cluster быстрее на 14 %) |
| Total rows прочитано | 789 012 345 | 789 012 345 | 0 (идентично) |
| `by_country` (4 страны) | см. JSON | побитово совпадает | 0 |
| `by_category` (10 категорий) | см. JSON | побитово совпадает | 0 |
| Driver+Executors | 1 JVM в одном контейнере | 1 Driver + отдельный Worker JVM | — |
| Shuffle | localhost loopback | docker bridge | +copy overhead |

Полная сводка с автогенерацией — в `report/spark_results_table.md`
(скрипт `scripts/render_results_table.py` читает все JSON в `results/`).

### Вывод

1. Один и тот же Spark job в обоих режимах выдаёт **идентичные агрегации** — корректность распределённой обработки подтверждена.
2. На эмуляции одной машины cluster mode быстрее всего на ~14 %: Master и Worker делят CPU/RAM, а distributed shuffle добавляет сериализацию и сетевые копии даже внутри docker bridge — это съедает большую часть выигрыша.
3. Выигрыш от кластера полностью проявляется только когда Worker'ы запущены на разном железе и параллельно читают независимые блоки. На 8 GB Mac такие условия физически невозможны, поэтому полученные 14 % — нижняя оценка преимущества.
4. Для маломощной машины `local[*]` практически предпочтительнее; на реальном кластере выбор был бы обратным.

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
