"""Spark aggregation job for HW3.

Reads the e-commerce CSV, computes per-country and per-category aggregations,
writes the results to Parquet, and persists run metadata (mode, wall-clock,
row counts, top rows) into a JSON file under `results/`. The JSON files are
later consumed by `scripts/render_results_table.py` to build the comparison
table in `report/spark_results_table.md`.

The same script is launched in two modes via spark-submit:
    --master local[*]                       single-JVM execution
    --master spark://spark-master:7077      distributed execution on the cluster
"""

import argparse
import json
import os
import time
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, count, countDistinct
from pyspark.sql.functions import sum as spark_sum


def build_session(app_name: str) -> SparkSession:
    """Create a SparkSession with sensible defaults for HW3."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the CSV with header and inferred schema."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
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


def mode_label(master: str) -> str:
    """Map Spark master URL to a short label used in result file names."""
    if master.startswith("local"):
        return "local"
    if master.startswith("spark://"):
        return "cluster"
    if master == "yarn":
        return "yarn"
    return "other"


def dump_results_json(
    results_dir: str,
    master: str,
    input_path: str,
    elapsed: float,
    total_rows: int,
    by_country: DataFrame,
    by_category: DataFrame,
) -> str:
    """Persist run metadata + top aggregation rows as JSON for later comparison."""
    payload = {
        "mode": mode_label(master),
        "master": master,
        "input": input_path,
        "wall_clock_sec": round(elapsed, 2),
        "total_rows": total_rows,
        "by_country": [row.asDict() for row in by_country.collect()],
        "by_category": [row.asDict() for row in by_category.collect()],
    }
    os.makedirs(results_dir, exist_ok=True)
    out_path = os.path.join(results_dir, f"{mode_label(master)}_{int(time.time())}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    return out_path


def main(input_path: str, output_path: str, results_dir: str, master: str) -> None:
    spark = build_session(f"HW3 aggregation [{master}]")

    t_start = time.time()
    df = read_csv(spark, input_path)
    # Count once so the comparison JSON records the actual dataset size.
    total_rows = df.count()
    by_country, by_category = aggregate(df)

    by_country.write.mode("overwrite").parquet(f"{output_path}/by_country")
    by_category.write.mode("overwrite").parquet(f"{output_path}/by_category")
    elapsed = time.time() - t_start

    json_path = dump_results_json(
        results_dir, master, input_path, elapsed, total_rows, by_country, by_category
    )

    print()
    print("=" * 60)
    print(f"Run mode:    {master}")
    print(f"Input:       {input_path}")
    print(f"Output:      {output_path}")
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
    args = parser.parse_args()
    main(args.input, args.output, args.results, args.master)
