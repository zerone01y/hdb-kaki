import pandas as pd
from pathlib import Path
from numpy import nan
from webapp.utils import get_project_root
from webapp.update.geocoding import get_map_results
from webapp.update.datagov import download_collection

DEFAULT_COLLECTION_ID = "150"

# Mapping for town names to match standard format
LOCATION_DICT = {
    "AMK": "ANG MO KIO",
    "BB": "BUKIT BATOK",
    "BD": "BEDOK",
    "BH": "BISHAN",
    "BM": "BUKIT MERAH",
    "BP": "BUKIT PANJANG",
    "BT": "BUKIT TIMAH",
    "CCK": "CHOA CHU KANG",
    "CL": "CLEMENTI",
    "CT": "CENTRAL AREA",
    "GL": "GEYLANG",
    "HG": "HOUGANG",
    "JE": "JURONG EAST",
    "JW": "JURONG WEST",
    "KWN": "KALLANG/WHAMPOA",
    "MP": "MARINE PARADE",
    "PG": "PUNGGOL",
    "PRC": "PASIR RIS",
    "QT": "QUEENSTOWN",
    "SB": "SEMBAWANG",
    "SGN": "SERANGOON",
    "SK": "SENGKANG",
    "TAP": "TAMPINES",
    "TG": "TENGAH",
    "TP": "TOA PAYOH",
    "WL": "WOODLANDS",
    "YS": "YISHUN",
}

PROPERTY_INFO_SCHEMA = {
    "max_floor_lvl": "Int64",
    "year_completed": "Int64",
    "total_dwelling_units": "Int64",
    "1room_sold": "Int64",
    "2room_sold": "Int64",
    "3room_sold": "Int64",
    "4room_sold": "Int64",
    "5room_sold": "Int64",
    "exec_sold": "Int64",
    "multigen_sold": "Int64",
    "studio_apartment_sold": "Int64",
    "1room_rental": "Int64",
    "2room_rental": "Int64",
    "3room_rental": "Int64",
    "other_room_rental": "Int64",
    "latitude": "float",
    "longitude": "float",
    "postal": "str",
}


def update_property_info(
    file_path: Path = None, subdir="HDB Property Information", force=False
) -> pd.DataFrame:
    """
    Main function to update property information.
    Downloads new data, merges with existing coordinates, and geocodes new addresses.
    """
    if file_path is None:
        file_path = get_project_root() / "data" / subdir / "HDB Property Information.CSV"

    file_path.parent.mkdir(parents=True, exist_ok=True)

    # 1. Load existing data to preserve coordinates and old records
    known_coords = pd.DataFrame(columns=["address", "postal", "latitude", "longitude"])
    existing_df = pd.DataFrame()
    if file_path.exists():
        existing_df = pd.read_csv(file_path)
        cols = ["address", "postal", "latitude", "longitude"]
        if all(c in existing_df.columns for c in cols):
            known_coords = existing_df[
                ["address", "postal", "latitude", "longitude"]
            ].drop_duplicates(subset=["address"])

    # 2. Download new data
    download_collection(DEFAULT_COLLECTION_ID)
    new_data = pd.read_csv(file_path)
    if new_data.empty:
        print("No data downloaded.")
        return pd.DataFrame()

    # 3. Preprocess new data
    if "blk_no" not in new_data.columns or "street" not in new_data.columns:
        print("Error: Missing 'blk_no' or 'street' columns.")
        return pd.DataFrame()

    new_data["address"] = new_data["blk_no"] + " " + new_data["street"]
    if "bldg_contract_town" in new_data.columns:
        new_data["town"] = new_data["bldg_contract_town"].map(LOCATION_DICT)

    # 4. Merge and Geocode
    # Merge new data with existing data, do not overwrite (as old data could be deleted in future.)
    # Strategy: Concat both, drop duplicates keeping new (for attributes), then re-attach known coordinates.
    merged_data = pd.concat([existing_df, new_data], ignore_index=True)
    merged_data = merged_data.drop_duplicates(subset=["address"], keep="last")
    merged_data = merged_data.drop(
        columns=["postal", "latitude", "longitude"], errors="ignore"
    )
    merged_data = merged_data.merge(known_coords, on="address", how="left")

    # Identify missing coordinates
    missing_mask = merged_data["latitude"].isna()
    addresses_to_geocode = (
        merged_data.loc[missing_mask, ["address"]]
        .drop_duplicates()
        .dropna(subset=["address"])
    )

    if not addresses_to_geocode.empty:
        print(f"Geocoding {len(addresses_to_geocode)} new addresses...")
        fresh_map_data = get_map_results(addresses_to_geocode)
        if not fresh_map_data.empty:
            # Update using map to fill NaNs
            for col in ["latitude", "longitude", "postal"]:
                mapper = fresh_map_data.set_index("address")[col].dropna()
                merged_data.loc[missing_mask, col] = merged_data.loc[
                    missing_mask, "address"
                ].map(mapper)

    # 5. Save
    # Define column types

    for col, dtype in PROPERTY_INFO_SCHEMA.items():
        if col in merged_data.columns:
            try:
                merged_data[col] = merged_data[col].astype(dtype)
            except Exception as e:
                print(f"Warning: Could not convert column {col} to {dtype}: {e}")

    merged_data.to_csv(file_path, index=False)
    print(f"Saved {len(merged_data)} records to {file_path}")

    return merged_data


def summarize_hdb_units():
    data_dir = get_project_root() / "data" / "HDB Property Information"
    input_file = data_dir / "HDB Property Information.CSV"
    output_file = get_project_root() / "data" / "Processed Data" / "annual_new_units.csv"

    # Read the CSV
    df = pd.read_csv(input_file)

    # Filter out rows with invalid year_completed (e.g. 0 or NaN)
    df = df[df["year_completed"] > 0]

    # Define columns to sum
    type_columns = [
        "1room_sold",
        "2room_sold",
        "3room_sold",
        "4room_sold",
        "5room_sold",
        "exec_sold",
        "multigen_sold",
        "studio_apartment_sold",
    ]

    # Group by year_completed and sum the unit types
    annual_units = df.groupby("year_completed")[type_columns].sum().reset_index()

    # Pre-calculate MOP year (Built Year + 5)
    annual_units["mop_year"] = (annual_units["year_completed"] + 5).astype(int)

    # Pre-calculate quarter label for plotting (e.g. "2023 Q1")
    # This aligns with the chart's x-axis format
    annual_units["quarter_label"] = annual_units["mop_year"].astype(str) + " Q1"

    # Calculate total new units
    annual_units["total_new_units"] = annual_units[type_columns].sum(axis=1)

    # Sort by year
    annual_units = annual_units.sort_values("year_completed")

    # Save to CSV (keeping original column names for type_columns to avoid unnecessary rename)
    annual_units.to_csv(output_file, index=False)
    print(f"Summary saved to {output_file}")
    print(annual_units.tail())


if __name__ == "__main__":
    update_property_info()
    summarize_hdb_units()
