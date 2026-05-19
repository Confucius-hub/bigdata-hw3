"""Spark aggregation job for HW3.

Reads the e-commerce CSV, computes per-country and per-category aggregations,
and writes the results to Parquet. Wall-clock time is measured and printed.

The same script is launched in two modes via spark-submit:
    --master local[*]                       single-JVM execution
    --master spark://spark-master:7077      distributed execution on the cluster
"""

import argparse
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


def main(input_path: str, output_path: str, master: str) -> None:
    spark = build_session(f"HW3 aggregation [{master}]")

    t_start = time.time()
    df = read_csv(spark, input_path)
    by_country, by_category = aggregate(df)

    by_country.write.mode("overwrite").parquet(f"{output_path}/by_country")
    by_category.write.mode("overwrite").parquet(f"{output_path}/by_category")
    elapsed = time.time() - t_start

    print()
    print("=" * 60)
    print(f"Run mode:    {master}")
    print(f"Input:       {input_path}")
    print(f"Output:      {output_path}")
    print(f"Wall-clock:  {elapsed:.2f} seconds")
    print("=" * 60)
    print()

    print("Top by country:")
    by_country.show(20, truncate=False)
    print("Top by category:")
    by_category.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HW3 Spark aggregation job.")
    parser.add_argument("--input",  default="/workspace/data/big_dataset.csv")
    parser.add_argument("--output", default="/workspace/results")
    parser.add_argument("--master", default="local[*]",
                        help="Spark master URL: 'local[*]' or "
                             "'spark://spark-master:7077'.")
    args = parser.parse_args()
    main(args.input, args.output, args.master)
