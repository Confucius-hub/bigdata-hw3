"""Generate a synthetic e-commerce event log CSV of the target size.

Schema:
    id, user_id, event_type, product_id, category, price, country, event_time
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


CATEGORIES = ["electronics", "books", "clothing", "home", "sport",
              "beauty", "toys", "auto", "food", "garden"]
COUNTRIES   = ["RU", "US", "DE", "CN", "BR"]
EVENT_TYPES = ["view", "click", "add_to_cart", "purchase"]
START_DATE  = datetime(2024, 1, 1)


def write_chunk(writer: Any, start_id: int, chunk_rows: int) -> int:
    """Write chunk_rows rows starting at start_id. Returns next id."""
    rnd = random.Random(start_id)
    for i in range(chunk_rows):
        row_id   = start_id + i
        user_id  = rnd.randint(1, 1_000_000)
        event    = rnd.choice(EVENT_TYPES)
        product  = rnd.randint(1, 100_000)
        category = rnd.choice(CATEGORIES)
        price    = round(rnd.uniform(0.01, 1000.0), 2)
        country  = rnd.choice(COUNTRIES)
        ts       = (START_DATE + timedelta(seconds=rnd.randint(0, 365 * 24 * 3600))
                    ).isoformat()
        writer.writerow([row_id, user_id, event, product, category, price,
                         country, ts])
    return start_id + chunk_rows


def generate_dataset(output: Path, target_gb: float,
                     chunk_rows: int = 1_000_000) -> None:
    """Generate a CSV file of the requested size."""
    output.parent.mkdir(parents=True, exist_ok=True)
    target_bytes = int(target_gb * 1024 ** 3)

    print(f"Target: {target_gb} GiB ({target_bytes:,} bytes)")
    print(f"Writing to: {output}")

    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "user_id", "event_type", "product_id",
                         "category", "price", "country", "event_time"])

        next_id = 1
        while file.tell() < target_bytes:
            next_id = write_chunk(writer, next_id, chunk_rows)
            if (next_id - 1) % (chunk_rows * 10) == 1:
                size_gb = file.tell() / 1024 ** 3
                print(f"  rows: {next_id - 1:>12,}  size: {size_gb:6.2f} GiB")

    final_size_gb = output.stat().st_size / 1024 ** 3
    print(f"Done: {next_id - 1:,} rows, {final_size_gb:.2f} GiB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a synthetic e-commerce log CSV for HW3."
    )
    parser.add_argument("--output", default="data/big_dataset.csv", type=Path,
                        help="Output CSV path.")
    parser.add_argument("--target-gb", default=50.0, type=float,
                        help="Target file size in GiB (default: 50).")
    args = parser.parse_args()
    generate_dataset(args.output, args.target_gb)
