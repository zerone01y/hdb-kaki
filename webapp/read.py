from datetime import datetime
from pathlib import Path

import polars as pl
import streamlit as st
from pybadges import badge

from webapp.utils import get_project_root


def get_last_updated_badge(subdir: str = "Resale Flat Prices"):
    data_dir = get_project_root() / "data" / subdir
    with open(data_dir / "metadata") as file:
        content = int(file.read())
        last_updated = datetime.fromtimestamp(content)

    return badge(
        left_text="last updated",
        right_text=last_updated.isoformat()[:10],
        left_color="#555",
        right_color="#007ec6",
    )


def convert_lease(x):
    if 0 < x <= 60:
        result = "0-60 years"
    elif 60 < x <= 80:
        result = "61-80 years"
    elif 80 < x <= 99:
        result = "81-99 years"
    return result


def get_dataframe_from_csv(
    subdir: str = "Resale Flat Prices", file_pattern="20*.csv"
) -> pl.DataFrame:
    """Combine all CSV files in the specified directory into a single DataFrame."""
    data_dir: Path = get_project_root() / "data"

    df = pl.read_csv(data_dir / subdir / file_pattern, schema=schema)
    return df


def get_dataframe_from_parquet(
    subdir: str = "Resale Flat Prices", filename="df.parquet"
) -> pl.DataFrame:
    """Combine all CSV files in the specified directory into a single DataFrame."""
    data_dir: Path = get_project_root() / "data"

    df = pl.read_parquet(data_dir / subdir / filename)
    return df.sort(by="town")


def add_time_filters(df: pl.DataFrame):
    df = df.with_columns(pl.col("month").str.strptime(pl.Date, "%Y-%m"))
    df = df.with_columns(
        pl.col("month").dt.quarter().alias("quarter"),
        pl.col("month").dt.year().alias("year"),
    )

    df = df.with_columns(
        (
            pl.concat_str(
                [
                    pl.col("year").cast(str),
                    pl.col("quarter")
                    .cast(str)
                    .map_elements(lambda x: f" Q{x}", return_dtype=str),
                ]
            ).alias("quarter_label")
        )
    )
    return df


@st.cache_data(ttl=3600)
def load_dataframe() -> pl.DataFrame:
    """Wrapper for get_dataframe that provides a cache"""
    df = get_dataframe_from_parquet()
    df = add_time_filters(df)
    return df


@st.cache_data
def get_annual_new_units():
    try:
        data_dir = get_project_root() / "data" / "Processed Data"
        file_path = data_dir / "annual_new_units.csv"
        if not file_path.exists():
            return pl.DataFrame()
        return pl.read_csv(file_path)
    except Exception:
        return pl.DataFrame()


schema = {
    "_id": pl.Int64,
    "month": pl.Utf8,
    "town": pl.Utf8,
    "flat_type": pl.Utf8,
    "block": pl.Utf8,
    "street_name": pl.Utf8,
    "storey_range": pl.Utf8,
    "floor_area_sqm": pl.Float32,
    "flat_model": pl.Utf8,
    "lease_commence_date": pl.Int16,
    "remaining_lease": pl.Utf8,
    "resale_price": pl.Float32,
    "address": pl.Utf8,
    "postal": pl.Int32,
    "latitude": pl.Float32,
    "longitude": pl.Float32,
    "_ts": pl.Utf8,
}
