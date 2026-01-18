import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


def fetch_osm_postal(query_address, session: requests.Session):
    query_string = f"https://nominatim.openstreetmap.org/search?q={query_address}&format=json&addressdetails=1"

    response = session.get(query_string).json()

    if not response:
        return None

    return response[0]["address"].get("postcode", None)


def fetch_map_data(query_address, session: requests.Session):
    query_string = (
        "https://www.onemap.gov.sg/api/common/elastic/search?&searchVal="
        + query_address
        + "&returnGeom=Y&getAddrDetails=Y"
    )

    response = session.get(query_string).json()["results"]

    if len(response):
        postal_code = response["POSTAL"]
        # use open street map if postal code is null or invalid
        if len(str(postal_code)) < 6:
            postal_code = fetch_osm_postal(query_address, session)
        return {
            "address": query_address,
            "postal": postal_code,
            "latitude": response["LATITUDE"],
            "longitude": response["LONGITUDE"],
        }

    else:
        return {
            "address": query_address,
            "postal": None,
            "latitude": None,
            "longitude": None,
        }


def get_map_results(data):
    headers = {
        "User-Agent": "HDB Kaki/1.0 (https://hdb-kaki.streamlit.app/)",
        "Referer": "https://hdb-kaki.streamlit.app/",
    }

    unique_address = list(dict.fromkeys(data["address"]))
    with requests.Session() as session:
        session.headers = headers
        with ThreadPoolExecutor() as executor:
            results = list(
                tqdm(
                    executor.map(
                        lambda addr: fetch_map_data(addr, session), unique_address
                    ),
                    total=len(unique_address),
                )
            )

    return pd.DataFrame(results)
