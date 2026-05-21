"""Spark aggregation job for HW3.

Reads the e-commerce CSV, computes per-country and per-category aggregations,
writes the results to Parquet, and persists run metadata (mode, wall-clock,
the effective Spark configuration, row counts, aggregation rows) into a JSON
file under `results/`.

The same script is launched in two modes via spark-submit:
    --master local[*]                       single-JVM execution
    --master spark://spark-master:7077      distributed execution on the cluster

Why the configuration is mode-aware
------------------------------------
A common mistake is to launch both modes with identical flags and conclude
that "the cluster is barely faster". The two modes consume resources very
differently, so the tuning knobs that matter are different:

* local[*]
    - Driver and executor live in a SINGLE JVM.
    - --executor-memory, --executor-cores, --num-executors are IGNORED.
    - The only memory knob that matters is --driver-memory.
    - Parallelism is bounded by the cores of the host machine ([*]).

* spark://  (standalone cluster)
    - Driver is separate from the Worker JVM(s).
    - --executor-memory / --executor-cores now actually apply and bound
      what the Worker can allocate.
    - Shuffle data crosses a real (loopback / docker-bridge) socket, which
      adds serialization + transfer overhead absent in local mode.

spark.sql.shuffle.partitions is the single most impactful SQL knob for a
groupBy of this size. The Spark default of 200 is far too high for a single
machine of this scale: it creates many tiny tasks whose scheduling overhead
dominates. We scale it to roughly 2x the available cores instead, and we set it
explicitly so the value is visible and intentional rather than inherited.
"""

import argparse
import json
import os
import time
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, count, countDistinct
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


# Explicit schema avoids a full extra pass over 50 GiB that inferSchema=true
# would trigger. Reading 50 GiB twice is the dominant cost if left on default.
SCHEMA = StructType([
    StructField("id",         IntegerType(), True),
    StructField("user_id",    IntegerType(), True),
    StructField("event_type", StringType(),  True),
    StructField("product_id", IntegerType(), True),
    StructField("category",   StringType(),  True),
    StructField("price",      DoubleType(),  True),
    StructField("country",    StringType(),  True),
    StructField("event_time", StringType(),  True),
])


def mode_label(master: str) -> str:
    """Map a Spark master URL to a short label used in result file names."""
    if master.startswith("local"):
        return "local"
    if master.startswith("spark://"):
        return "cluster"
    if master == "yarn":
        return "yarn"
    return "other"


def shuffle_partitions_for(master: str, cores: int) -> int:
    """Pick an explicit shuffle-partition count appropriate for the hardware.

    On a single machine ~2x cores keeps tasks coarse enough that scheduling
    overhead stays small while still using every core. This is far better than
    the Spark default of 200 for this dataset/hardware combination, where 200
    tiny tasks would spend most of their time on scheduling rather than work.
    The floor of 8 guards against degenerate values when cores is very small.
    """
    return max(cores * 2, 8)


def build_session(app_name: str, master: str, cores: int) -> SparkSession:
    """Create a SparkSession with an explicit, mode-aware configuration."""
    shuffle = shuffle_partitions_for(master, cores)
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the CSV with header and an explicit schema (no second scan)."""
    return (
        spark.read
        .option("header", "true")
        .schema(SCHEMA)
        .csv(input_path)
    )


def aggregate(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Per-country and per-category aggregations."""
    by_country = (
        df.groupBy("country")
        .agg(
            count("*").alias("events"),
            countDistinct("user_id").alias("unique_users"),
            spark_sum("price").alias("revenue"),
            avg("price").alias("avg_price"),
        )
        .orderBy("country")
    )
    by_category = (
        df.groupBy("category")
        .agg(
            count("*").alias("events"),
            countDistinct("user_id").alias("unique_users"),
            spark_sum("price").alias("revenue"),
        )
        .orderBy("category")
    )
    return by_country, by_category


def effective_config(spark: SparkSession) -> Dict[str, str]:
    """Collect the Spark settings that actually drive performance per mode."""
    conf = spark.sparkContext.getConf()
    keys = [
        "spark.master",
        "spark.app.name",
        "spark.driver.memory",
        "spark.executor.memory",
        "spark.executor.cores",
        "spark.cores.max",
        "spark.default.parallelism",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
    ]
    out: Dict[str, str] = {}
    for key in keys:
        out[key] = conf.get(key, "<default>")
    out["spark.sparkContext.defaultParallelism"] = str(
        spark.sparkContext.defaultParallelism
    )
    return out


def dump_results_json(
    results_dir: str,
    master: str,
    input_path: str,
    elapsed: float,
    total_rows: int,
    config: Dict[str, str],
    by_country: DataFrame,
    by_category: DataFrame,
) -> str:
    """Persist run metadata + effective config + aggregation rows as JSON."""
    payload = {
        "mode": mode_label(master),
        "master": master,
        "input": input_path,
        "wall_clock_sec": round(elapsed, 2),
        "total_rows": total_rows,
        "effective_config": config,
        "by_country": [row.asDict() for row in by_country.collect()],
        "by_category": [row.asDict() for row in by_category.collect()],
    }
    os.makedirs(results_dir, exist_ok=True)
    out_path = os.path.join(
        results_dir, f"{mode_label(master)}_{int(time.time())}.json"
    )
    with open(out_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2, default=str)
    return out_path


def main(input_path: str, output_path: str, results_dir: str,
         master: str, cores: int) -> None:
    spark = build_session(f"HW3 aggregation [{master}]", master, cores)
    config = effective_config(spark)

    print()
    print("=" * 60)
    print("Effective Spark configuration for this run:")
    for key, value in config.items():
        print(f"  {key:<48} = {value}")
    print("=" * 60)

    t_start = time.time()
    df = read_csv(spark, input_path)
    total_rows = df.count()
    by_country, by_category = aggregate(df)

    by_country.write.mode("overwrite").parquet(f"{output_path}/by_country")
    by_category.write.mode("overwrite").parquet(f"{output_path}/by_category")
    elapsed = time.time() - t_start

    json_path = dump_results_json(
        results_dir, master, input_path, elapsed, total_rows, config,
        by_country, by_category,
    )

    print()
    print("=" * 60)
    print(f"Run mode:    {master}")
    print(f"Input:       {input_path}")
    print(f"Total rows:  {total_rows:,}")
    print(f"Wall-clock:  {elapsed:.2f} seconds")
    print(f"Results:     {json_path}")
    print("=" * 60)
    print()

    print("Top by country:")
    by_country.show(20, truncate=False)
    print("Top by category:")
    by_category.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HW3 Spark aggregation job.")
    parser.add_argument("--input",   default="/workspace/data/big_dataset.csv")
    parser.add_argument("--output",  default="/workspace/results")
    parser.add_argument("--results", default="/workspace/results",
                        help="Folder where the per-run JSON file is written.")
    parser.add_argument("--master",  default="local[*]",
                        help="Spark master URL: 'local[*]' or "
                             "'spark://spark-master:7077'.")
    parser.add_argument("--cores", type=int, default=6,
                        help="Cores available to this run; used to size "
                             "shuffle partitions explicitly.")
    args = parser.parse_args()
    main(args.input, args.output, args.results, args.master, args.cores)
