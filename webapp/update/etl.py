import sys
from pathlib import Path
import datetime
import polars as pl

from webapp.read import get_project_root, schema
from webapp.update.convert import csv_to_parquet
from webapp.update.extract import extract, get_timestamps


def update_data(subdir: str = "Resale Flat Prices"):
    """Executes ETL process"""
    csv_file_glob: Path = get_project_root() / "data" / subdir / "20*.csv"
    df = pl.read_csv(csv_file_glob, schema=schema)

    start, end = get_timestamps(df)
    has_changed = extract([start, end, "-f"])
    if has_changed:
        csv_to_parquet(subdir)
        print("Changes detected")

        with open(get_project_root() / "data" / subdir / "metadata", "w") as f:
            f.write(f"{int(datetime.datetime.now().timestamp())}")
    sys.exit(0)


if __name__ == "__main__":
    update_data()
