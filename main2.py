import requests
import pandas as pd
import time
import math

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Multiple Overpass API endpoints (fallback list)
OVERPASS_SERVERS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter"
]

USER_AGENT = "MyBusinessSearcher/1.0 (d.favato@outlook.com)"


# -------------------------------------------------------
# 1) Geocode address → Get a point (lat, lon)
# -------------------------------------------------------
def geocode(address):
    params = {
        "q": address,
        "format": "json",
        "limit": 1
    }
    headers = {"User-Agent": USER_AGENT}

    r = requests.get(NOMINATIM_URL, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()

    if not data:
        print("Address not found.")
        return None

    return data[0]  # includes lat, lon, and a big bbox (which we will NOT use)


# -------------------------------------------------------
# 2) Create a small bounding box around the lat/lon
# -------------------------------------------------------
def create_small_bbox(lat, lon, meters=350):
    """
    Create a bounding box approx. X meters around a point.
    This solves the Overpass 504 timeout problem.
    """
    # 1 degree latitude = 111,320 meters
    dlat = meters / 111320.0

    # Longitude degrees shrink with latitude
    dlon = meters / (40075000 * math.cos(math.radians(lat)) / 360)

    # Return: south, west, north, east
    return (lat - dlat, lon - dlon, lat + dlat, lon + dlon)


# -------------------------------------------------------
# 3) Query Overpass (with retry & fallback)
# -------------------------------------------------------
def search_osm_pois(bbox_tuple, tag_key, tag_value, max_results):
    lat1, lon1, lat2, lon2 = bbox_tuple

    query = f"""
    [out:json][timeout:25];
    (
      node["{tag_key}"="{tag_value}"]({lat1},{lon1},{lat2},{lon2});
      way["{tag_key}"="{tag_value}"]({lat1},{lon1},{lat2},{lon2});
      relation["{tag_key}"="{tag_value}"]({lat1},{lon1},{lat2},{lon2});
    );
    out center {max_results};
    """

    headers = {"User-Agent": USER_AGENT}

    # Try servers in order until one responds
    for server in OVERPASS_SERVERS:
        try:
            print(f"Trying Overpass server: {server}")
            r = requests.post(server, data={"data": query}, headers=headers, timeout=30)
            r.raise_for_status()
            print("Success with server:", server)
            return r.json()

        except requests.exceptions.Timeout:
            print(f"⚠️ Timeout on {server}, trying next server...")
        except requests.exceptions.HTTPError as e:
            print(f"⚠️ HTTP error on {server}: {e}")
        except Exception as e:
            print(f"⚠️ Unknown error on {server}: {e}")

    print("❌ All Overpass servers failed.")
    return {"elements": []}


# -------------------------------------------------------
# 4) Extract business info from OSM element
# -------------------------------------------------------
def extract_record(el):
    tags = el.get("tags", {})
    center = el.get("center", {})

    return {
        "osm_id": f"{el.get('type')}/{el.get('id')}",
        "name": tags.get("name"),
        "street": tags.get("addr:street"),
        "housenumber": tags.get("addr:housenumber"),
        "postcode": tags.get("addr:postcode"),
        "city": tags.get("addr:city"),
        "email": tags.get("contact:email") or tags.get("email"),
        "phone": tags.get("phone"),
        "website": tags.get("website") or tags.get("contact:website"),
        "lat": el.get("lat") or center.get("lat"),
        "lon": el.get("lon") or center.get("lon"),
        "date_scraped": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }


# -------------------------------------------------------
# 5) Main workflow
# -------------------------------------------------------
def run(address, category_key, category_value, max_results):
    print("Geocoding…")
    geo = geocode(address)
    if not geo:
        return

    lat = float(geo["lat"]) # lat from the big box
    lon = float(geo["lon"]) # lon from the big box

    # Create small (350 m) bounding box
    bbox = create_small_bbox(lat, lon, meters=350)
    print("Using small bounding box:", bbox)

    print("Querying Overpass…")
    data = search_osm_pois(bbox, category_key, category_value, max_results)

    results = [extract_record(el) for el in data.get("elements", [])]

    df = pd.DataFrame(results)
    df.to_csv("results.csv", index=False, encoding="utf-8")

    print(f"Saved results.csv with {len(df)} records!")


# -------------------------------------------------------
# Run example
# -------------------------------------------------------
if __name__ == "__main__":
    run(
        address="Hermannstraße 100, Berlin",
        category_key="amenity",     # IMPORTANT: cafe == amenity=cafe
        category_value="cafe",
        max_results=10
    )
