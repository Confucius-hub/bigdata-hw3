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

| Режим | Wall-clock |
|---|---|
| `--master local[*]` | **~14 340 сек (≈4 часа)** |
| `--master spark://spark-master:7077` | **~12 600 сек (≈3.5 часа)** |

### Пример вывода (groupBy по категории)
Все 10 категорий обработаны, ~78.9M событий и 1M уникальных пользователей в каждой.

**Анализ:** на эмуляции кластера на одной машине разница local vs cluster ~10% — Master и Worker делят те же ресурсы. Полный отчёт в `report/spark_results.md`.

**Conda-pack:** скрипт `scripts/build_conda_env.sh` для упаковки Python окружения.

## Ограничение

Полный стек HDFS+YARN+Hive+Zeppelin требует ~12 GB RAM. На 8 GB Mac не помещается. Использован минимально достаточный Spark standalone (Master+Worker) — двух режимов запуска `local[*]` и `spark://` достаточно для выполнения задания.

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

## Структура
