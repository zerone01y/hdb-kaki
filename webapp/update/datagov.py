import requests
import json
import datetime
import pathlib
from webapp.utils import get_project_root
from typing import Optional, Dict, Any

BASE_SEARCH_URL = "https://data.gov.sg/api/action/datastore_search"
COLLECTION_METADATA_URL = (
    "https://api-production.data.gov.sg/v2/public/api/collections/{}/metadata"
)
DATASET_METADATA_URL = (
    "https://api-production.data.gov.sg/v2/public/api/datasets/{}/metadata"
)
DATASET_DOWNLOAD_URL = (
    "https://api-open.data.gov.sg/v1/public/api/datasets/{}/poll-download"
)


def fetch_data_gov_sg(dataset_id: str, query_params: dict = None) -> list:
    """
    Fetch all records from data.gov.sg for a given dataset ID (datastore API).
    Handles pagination automatically.
    """
    params = {
        "resource_id": dataset_id,
        "limit": 4000,
        "offset": 0,
    }  # Maximize batch size (approx 5k limit on API)
    if query_params:
        params.update(query_params)

    all_records = []

    with requests.Session() as session:
        session.headers.update({"User-Agent": "HDB Kaki/1.0"})

        while True:
            try:
                resp = session.get(BASE_SEARCH_URL, params=params)
                resp.raise_for_status()

                data = resp.json().get("result", {})
                records = data.get("records", [])

                if not records:
                    break

                all_records.extend(records)

                # If we received fewer records than the limit,
                # we have reached the end of the dataset.
                if len(records) < params["limit"]:
                    break

                params["offset"] += len(records)

            except Exception as e:
                print(f"Error downloading data: {e}")
                break

    return all_records


def download_dataset(
    dataset_id: str,
    base_path: Optional[pathlib.Path] = None,
    collection_name: Optional[str] = None,
    existing_metadata_dict: Optional[Dict[str, Any]] = None,
    session: Optional[requests.Session] = None,
) -> Optional[Dict[str, Any]]:
    """
    Download a single dataset from data.gov.sg API.

    Parameters:
    -----------
    dataset_id : str
        The dataset ID to download
    base_path : pathlib.Path, optional
        Base directory path. If None, uses 'data/Standalone Datasets/'
    collection_name : str, optional
        If provided, saves to collection subfolder. Otherwise saves to standalone folder.
    existing_metadata_dict : dict, optional
        For collections, pass the existing collection metadata dict to check for updates.
        If None, attempts to load metadata.json from the base_path.
    session : requests.Session, optional
        Existing requests session to use for API calls.

    Returns:
    --------
    dict : Dictionary with download status and metadata, or None if failed.
           Format: {"status": "success"|"skipped"|"error", "metadata": ...}
    """
    local_session = session or requests.Session()
    should_close_session = session is None

    try:
        # 1. Determine Paths
        if collection_name is None:
            collection_name = "Standalone Datasets"

        if base_path is None:
            base_path = get_project_root() / "data" / collection_name

        base_path.mkdir(parents=True, exist_ok=True)

        metadata_file = base_path / "metadata.json"

        # 2. Load Existing Metadata if not provided
        if existing_metadata_dict is None:
            existing_metadata_dict = {}
            if metadata_file.exists():
                try:
                    with open(metadata_file, "r") as f:
                        existing_metadata_dict = json.load(f)
                except (json.JSONDecodeError, IOError) as e:
                    print(
                        f"Warning: Could not load existing metadata from {metadata_file}: {e}"
                    )

        # 3. Fetch Current Metadata
        meta_url = DATASET_METADATA_URL.format(dataset_id)
        try:
            resp = local_session.get(meta_url, timeout=30)
            resp.raise_for_status()
            current_meta = resp.json().get("data", {})
        except requests.RequestException as e:
            print(f"Warning: Failed to fetch metadata for dataset {dataset_id}: {e}")
            return {"status": "error", "error": str(e)}

        dataset_name = current_meta.get("name", dataset_id)
        file_format = current_meta.get("format", "csv")  # Default to csv if missing

        # 4. Check if Update is Needed
        existing_dataset_meta = existing_metadata_dict.get(dataset_id, {})
        existing_ts = existing_dataset_meta.get("lastUpdatedAt")
        current_ts = current_meta.get("lastUpdatedAt")

        print(f"Dataset: {dataset_name}")
        update_needed = True
        if existing_ts and current_ts:
            try:
                # Handle potential timezone differences if necessary, currently assuming ISO format matches
                dt_existing = datetime.datetime.fromisoformat(existing_ts)
                dt_current = datetime.datetime.fromisoformat(current_ts)

                print(f"  Local version:   {dt_existing}")
                print(f"  Remote version:  {dt_current}")

                if dt_existing >= dt_current:
                    print("  Status: Up to date. Skipping download.")
                    return {"status": "skipped", "metadata": existing_dataset_meta}

            except ValueError as e:
                print(f"Warning: Could not parse timestamp: {e}. Forcing update.")

        # 5. Initiate Download
        print(f"  Status: Update available. Downloading...")
        dl_url_api = DATASET_DOWNLOAD_URL.format(dataset_id)

        try:
            dl_resp = local_session.get(dl_url_api, timeout=30)
            dl_resp.raise_for_status()
            download_info = dl_resp.json().get("data", {})
            file_url = download_info.get("url")

            if not file_url:
                print(f"Warning: No download URL returned for dataset {dataset_id}")
                return {"status": "error", "error": "No download URL"}

            # Download the actual file
            file_resp = local_session.get(file_url, timeout=300)
            file_resp.raise_for_status()

            output_filename = f"{dataset_name}.{file_format}"
            # Sanitize filename if needed (basic)
            output_filename = "".join(
                c for c in output_filename if c.isalnum() or c in (" ", ".", "_", "-")
            ).strip()
            output_file = base_path / output_filename

            with open(output_file, "wb") as f:
                f.write(file_resp.content)
            print(f"  Saved to: {output_file.name}")

        except requests.RequestException as e:
            print(f"Error downloading file for dataset {dataset_id}: {e}")
            return {"status": "error", "error": str(e)}

        # 7. Update Metadata (Standalone Mode)
        # We update the dict object regardless
        current_meta["lastAccessedAt"] = datetime.datetime.now().isoformat()
        existing_metadata_dict[dataset_id] = current_meta

        if collection_name == "Standalone Datasets":
            try:
                with open(metadata_file, "w") as f:
                    json.dump(existing_metadata_dict, f, indent=2)
            except IOError as e:
                print(f"Warning: Could not save metadata to {metadata_file}: {e}")

        return {"status": "success", "metadata": current_meta}

    except Exception as e:
        print(f"Unexpected error in download_dataset for {dataset_id}: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        if should_close_session:
            local_session.close()


def download_collection(collection_id: str):
    """
    Download a collection and all its child datasets from data.gov.sg API.

    Parameters:
    -----------
    collection_id : str
        The collection ID to download
    """
    print(f"Starting download for collection: {collection_id}")

    with requests.Session() as session:
        # 1. Fetch Collection Metadata
        coll_url = COLLECTION_METADATA_URL.format(collection_id)
        try:
            resp = session.get(coll_url, timeout=30)
            resp.raise_for_status()
            coll_data = resp.json().get("data", {})
            coll_meta = coll_data.get("collectionMetadata", {})
        except requests.RequestException as e:
            print(f"Error fetching collection metadata: {e}")
            return

        child_datasets = coll_meta.get("childDatasets", [])
        collection_name = coll_meta.get("name", f"Collection_{collection_id}")

        # Summary
        print(f"Collection: {collection_name}")
        print(f"Last Updated: {coll_meta.get('lastUpdatedAt', 'Unknown')}")
        print(f"Datasets found: {len(child_datasets)}")

        if not child_datasets:
            print("No datasets to download.")
            return

        # 2. Setup Directory
        # Sanitize collection name for folder
        safe_coll_name = "".join(
            c for c in collection_name if c.isalnum() or c in (" ", "-", "_")
        ).strip()
        # Ensure unique-ish folder or just use name
        folder_name = f"{safe_coll_name}"

        base_path = get_project_root() / "data" / folder_name
        base_path.mkdir(parents=True, exist_ok=True)

        # 3. Load Existing Metadata
        metadata_file = base_path / "metadata.json"
        collection_metadata_store = {}

        if metadata_file.exists():
            try:
                with open(metadata_file, "r") as f:
                    collection_metadata_store = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load existing metadata: {e}")

        # 4. Download Each Dataset
        success_count = 0
        skipped_count = 0
        fail_count = 0

        for dataset_id in child_datasets:
            result = download_dataset(
                dataset_id,
                base_path=base_path,
                collection_name=folder_name,
                existing_metadata_dict=collection_metadata_store,
                session=session,
            )

            if result:
                status = result.get("status")
                if status == "success":
                    success_count += 1
                elif status == "skipped":
                    skipped_count += 1
                else:
                    fail_count += 1

                # Update our in-memory store if we got valid metadata back
                # (download_dataset updates the dict passed to it, but let's be explicit)
                if result.get("metadata"):
                    collection_metadata_store[dataset_id] = result["metadata"]
            else:
                fail_count += 1

        # 5. Save Final Metadata
        try:
            with open(metadata_file, "w") as f:
                json.dump(collection_metadata_store, f, indent=2)
        except IOError as e:
            print(f"Error saving metadata: {e}")

        print(
            f"Collection {collection_name}: Success: {success_count} | Skipped: {skipped_count} | Failed: {fail_count}"
        )


if __name__ == "__main__":
    download_collection(150)
    download_dataset("d_3f172c6feb3f4f92a2f47d93eed2908a")
    download_dataset("d_b39d3a0871985372d7e1637193335da5")
    download_dataset("d_0542d48f0991541706b58059381a6eca")
    download_dataset("d_4a086da0a5553be1d89383cd90d07ecd")
    download_dataset("d_688b934f82c1059ed0a6993d2a829089")
    download_dataset("d_8d886e3a83934d7447acdf5bc6959999")
    download_dataset("d_cac2c32f01960a3ad7202a99c27268a0")
    pass
