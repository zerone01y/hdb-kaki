from argparse import ArgumentParser
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pandas as pd
import json
from dateutil.relativedelta import relativedelta
from webapp.utils import get_project_root
from webapp.update.property_info import update_property_info
from webapp.update.geocoding import get_map_results
from webapp.update.datagov import fetch_data_gov_sg
from webapp.read import schema
from numpy import nan


@lru_cache
def extract_hdb_data(year_month):
    dataset_id = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
    query_params = {
        "filters": json.dumps({"month": year_month}),
        # "limit": 14000 # fetch_data_gov_sg handles pagination automatically
    }

    # Use the shared function
    records = fetch_data_gov_sg(dataset_id, query_params)
    return records


def get_data(start_date="2019-01", end_date=pd.Timestamp.now().strftime("%Y-%m")):
    dates = (
        pd.date_range(start=start_date, end=end_date, freq="MS")
        .strftime("%Y-%m")
        .tolist()
    )

    all_items = []
    for date in dates:
        res = extract_hdb_data(date)
        all_items.extend(res)

    df = pd.DataFrame(all_items)
    if df.empty:
        return pd.DataFrame()

    df["address"] = df["block"] + " " + df["street_name"]
    return df.reset_index(drop=True)


def load_existing_data(file_path: Path) -> pd.DataFrame:
    """Load existing data from a CSV file if it exists, otherwise return an empty DataFrame."""
    if file_path.exists():
        df = pd.read_csv(
            file_path,
            dtype={
                "_id": "Int64",
                "month": str,
                "town": str,
                "flat_type": str,
                "block": str,
                "street_name": str,
                "storey_range": str,
                "floor_area_sqm": float,
                "flat_model": str,
                "lease_commence_date": "Int64",
                "remaining_lease": str,
                "resale_price": float,
                "address": str,
                "postal": "Int64",
                "latitude": float,
                "longitude": float,
            },
        )

        return df

    return pd.DataFrame()


def skip_process(file_path: Path, should_process: bool) -> bool:
    """Determine if the file should be processed based on its existence and if it's the current month."""
    if file_path.exists() and not should_process:
        print(f"File {file_path} exists and is not the current month. Skipping.")
        return False
    return True


def get_coordinate_map(new_data: pd.DataFrame, force=False) -> pd.DataFrame:
    """
    Retrieve coordinate map (address -> lat, lon, postal).
    Updates property_info if addresses are missing.
    Falls back to manual fetching for stubborn missing addresses.
    """
    property_info_path = (
        get_project_root()
        / "data"
        / "HDB Property Information"
        / "HDB Property Information.CSV"
    )
    coord_cols = ["address", "postal", "latitude", "longitude"]

    # 1. Load existing property info
    if property_info_path.exists():
        property_info = pd.read_csv(property_info_path)
    else:
        property_info = pd.DataFrame()

    # 2. Check for missing addresses
    known_addresses = set(property_info["address"]) if not property_info.empty else set()
    missing_mask = ~new_data["address"].isin(known_addresses)

    if missing_mask.any():
        print(
            f"Found {missing_mask.sum()} addresses not in property info. Updating property info..."
        )
        # Update property info ONCE
        property_info = update_property_info(property_info_path, force=force)

        # Refresh known addresses
        if not property_info.empty:
            known_addresses = set(property_info["address"])
        else:
            known_addresses = set()

        still_missing_mask = ~new_data["address"].isin(known_addresses)

        # 3. Fallback: Manual Fetch for still missing addresses
        if still_missing_mask.any():
            print(
                f"Still {still_missing_mask.sum()} addresses missing after update. Fetching manually..."
            )
            missing_addresses_df = new_data.loc[
                still_missing_mask, ["address"]
            ].drop_duplicates()
            fresh_map_data = get_map_results(missing_addresses_df)

            if not fresh_map_data.empty:
                # Append fresh results to property_info for this session only (to be returned)
                fresh_coords = (
                    fresh_map_data[coord_cols]
                    if set(coord_cols).issubset(fresh_map_data.columns)
                    else fresh_map_data
                )
                property_info = pd.concat(
                    [property_info, fresh_coords], ignore_index=True
                )

                # Save the updated property info with manually fetched addresses
                # Deduplicate by address to keep the file clean
                property_info = property_info.drop_duplicates(
                    subset=["address"], keep="last"
                )
                property_info.to_csv(property_info_path, index=False)
                print(
                    f"Saved updated property info with {len(fresh_coords)} new addresses to {property_info_path}"
                )

    # 4. Return unique coordinate map
    if not property_info.empty:
        # Ensure we have the columns we need
        available_cols = [c for c in coord_cols if c in property_info.columns]
        return property_info[available_cols].drop_duplicates(subset=["address"])

    return pd.DataFrame(columns=coord_cols)


def process_month(month: str, data_dir: Path, should_process: bool = False):
    """Process and save data for a given month."""
    file_path = data_dir / f"{month}.csv"

    if not skip_process(file_path, should_process):
        return False

    # 1. Download new data for the month
    new_data = get_data(start_date=month, end_date=month)
    if new_data.empty:
        print(f"No data found for {month}")
        return False

    existing_data = load_existing_data(file_path)
    if not existing_data.empty and len(new_data) == len(existing_data):
        print(f"{month}: Same data size, skip...")
        return

    # 2. Get Geocoding Info
    property_coords = get_coordinate_map(new_data)

    # 3. Merge Coordinates
    # Remove existing coord cols from new_data if present to avoid overlap
    new_data = new_data.drop(
        columns=[c for c in ["postal", "latitude", "longitude"] if c in new_data.columns]
    )
    merged_df = new_data.merge(property_coords, on="address", how="left")

    # 4. Combine with existing data and deduplicate
    if not existing_data.empty:
        ts_map = existing_data.set_index("_id")["_ts"]
        merged_df["_ts"] = merged_df["_id"].map(ts_map)

    df = merged_df
    print(f"Total number of observations for {month}: {df.shape[0]}")

    # 5. Add/Fill Timestamp
    today = datetime.today().strftime("%Y-%m-%d")
    df["_ts"] = df.get("_ts", nan)
    df["_ts"] = df["_ts"].fillna(today)

    for col, dtype in schema.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception:
                pass

    df.to_csv(file_path, index=False)
    return True


def get_timestamps(df=None) -> tuple[str, str]:
    current_timestamp = datetime.now()
    current_month = current_timestamp.strftime("%Y-%m")

    if df is not None and not df.is_empty():
        last_month = df["month"].max()
    else:
        last_month = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m")

    return last_month, current_month


def extract(raw_args=None, subdir: str = "Resale Flat Prices"):
    parser = ArgumentParser(description="Fetch HDB and map data.")
    parser.add_argument("start_date", type=str, help="Start date in YYYY-MM format")
    parser.add_argument("end_date", type=str, help="End date in YYYY-MM format")
    parser.add_argument("-f", "--force", action="store_true")
    args = parser.parse_args(raw_args)

    data_dir = Path("data") / subdir
    data_dir.mkdir(exist_ok=True)

    start_date = pd.to_datetime(args.start_date, format="%Y-%m")
    end_date = pd.to_datetime(args.end_date, format="%Y-%m")
    months = (
        pd.date_range(start=start_date, end=end_date, freq="MS")
        .strftime("%Y-%m")
        .tolist()
    )
    last_month, current_month = get_timestamps()

    all_changed = False
    for month in months:
        should_process = args.force or month in (last_month, current_month)
        month_changed = process_month(month, data_dir, should_process)
        if month_changed:
            all_changed = True
    return all_changed
