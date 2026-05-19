# ДЗ 3 — AI-сервисы и Apache Spark в Docker

**Big Data course, ДЗ 3.**

Состоит из двух частей:

1. **AI-сервисы** — установить и попробовать Cursor, Claude Code, VSCode с AI-плагинами и локальную Gemma через Ollama. Сравнить, ходят ли модели в интернет. См. `report/ai_services_report.md`.
2. **Spark** — поднять Apache Spark в Docker, сгенерировать 50 ГБ датасет, обработать его в двух режимах (`local[*]` и `spark://`), замерить время.

## Структура

```
hw3/
├── docker-compose.yml          # Spark Master + Worker + Jupyter
├── scripts/
│   └── generate_dataset.py     # генерация 50 ГБ синтетического лога
├── jobs/
│   └── spark_job.py            # PySpark агрегация (запускается в обоих режимах)
├── notebooks/                  # Jupyter notebook со Spark
├── data/                       # сюда падает датасет (gitignored)
├── results/                    # результаты в Parquet
├── report/
│   ├── ai_services_report.md   # часть 1 — отчёт по AI
│   └── spark_results.md        # часть 2 — числа замеров
└── docs/
    └── screenshots/            # скриншоты для отчётов
```

## Требования

- Docker Desktop с поддержкой Apple Silicon
- 80+ ГБ свободного диска
- Python 3.10+ (для генерации датасета на хосте, опционально)

## Быстрый старт

### 1. Поднять стек

```bash
docker compose up -d
docker compose ps        # все 3 контейнера должны быть Up
```

UI:
- Spark Master: http://localhost:8080
- Jupyter: http://localhost:8888 (token: `spark`)

### 2. Сгенерировать датасет 50 ГБ

```bash
docker compose exec spark-master \
  python /workspace/scripts/generate_dataset.py \
  --output /workspace/data/big_dataset.csv \
  --target-gb 50
```

Занимает ~30 минут. Файл появляется в `./data/big_dataset.csv` на хосте.

Проверь размер:

```bash
ls -lh data/big_dataset.csv
# должно быть ~50G
```

### 3. Запуск в режиме `local[*]`

```bash
docker compose exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master local[*] \
  /workspace/jobs/spark_job.py \
  --input  /workspace/data/big_dataset.csv \
  --output /workspace/results/local \
  --master "local[*]"
```

В выводе будет `Wall-clock: XX.XX seconds` — это локальное время.

### 4. Запуск в режиме `spark://spark-master:7077` (кластер)

```bash
docker compose exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/jobs/spark_job.py \
  --input  /workspace/data/big_dataset.csv \
  --output /workspace/results/cluster \
  --master "spark://spark-master:7077"
```

В выводе будет `Wall-clock: YY.YY seconds` — это кластерное время.

### 5. Conda-pack для распространения окружения (опционально)

В Apache Spark на YARN обычно используется `conda-pack` чтобы упаковать Python-окружение и отправить вместе с job. На упрощённом standalone-кластере это не обязательно (Worker уже содержит pyspark), но команда есть в `scripts/build_conda_env.sh`.

## Записать результаты

В `report/spark_results.md` подставь свои числа из двух запусков и сделай скриншот вывода `spark-submit` в `docs/screenshots/spark_console.png`.

## Что НЕ включено и почему

Полный стек **HDFS + YARN + Hive + Zeppelin** требует около 12 ГБ оперативной памяти у Docker Desktop. На MacBook с 8 ГБ это физически не помещается — контейнеры начинают падать по OOM. Поэтому стек упрощён до Spark Master + Worker + Jupyter, что достаточно для демонстрации двух режимов выполнения (`local[*]` и распределённый через `spark://`). Условие «обработка в двух режимах с замером времени» выполнено.

## Остановить

```bash
docker compose down
```

## Полная очистка

```bash
docker compose down -v
```
