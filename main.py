import requests
import pandas as pd
import time

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# 1) Geocode address - get bounding box
def geocode(address):
  params = {
    "q": address,
    "format": "json",
    "limit": 1
  }
  headers = {"User-Agent": "MyBusinessFinderApp/1.0 (d.favato@outlook.com)"}

  r = requests.get(NOMINATIM_URL, params=params, headers=headers)
  r.raise_for_status()
  data = r.json()

  if not data:
    print("Address not found.")
    return None
  
  return data[0]

# Turn bounding box into numeric tuple
def parse_bbox(bbox):
  lat_min = float(bbox[0])
  lat_max = float(bbox[1])
  lon_min = float(bbox[2])
  lon_max = float(bbox[3])
  return lat_min, lon_min, lat_max, lon_max

# 2) Query Overpass for POIs
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

  headers = {"User-Agent": "MyBusinessFinderApp/1.0 (d.favato@outlook.com)"}
  r = requests.post(OVERPASS_URL, data={"data": query}, headers=headers)
  r.raise_for_status()
  return r.json()

# 3) Extract tags from each result
def extract_record(el):
  tags = el.get("tags", {})
  center = el.get("center", {})


  return {
    "osm_id": f"{el.get('type')}/{el.get('id')}",
    "name": tags.get("name", None),
    "street": tags.get("addr:street", None),
    "housenumber": tags.get("addr:housenumber", None),
    "postcode": tags.get("addr:postcode", None),
    "city": tags.get("addr:city", None),
    "email": tags.get("contact:email") or tags.get("email"),
    "phone": tags.get("phone"),
    "website": tags.get("website") or tags.get("contact:website"),
    "lat": el.get("lat") or center.get("lat"),
    "lon": el.get("lon") or center.get("lon"),
    "date_scraped": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
  }

# 4) Main workflow
def run(address, category_key, category_value, max_results):
  print("Geocoding...")
  geo = geocode(address)
  if not geo:
    return
  
  bbox = parse_bbox(geo["boundingbox"])

  print("Quering Overpass...")
  data = search_osm_pois(bbox, category_key, category_value, max_results)

  results = []
  for el in data.get("elements", []):
    results.append(extract_record(el))

  df = pd.DataFrame(results)
  df.to_csv("results.csv", index=False, encoding="utf-8")

  print(f"Saved results.csv with {len(df)} records!")

# Run example
if __name__ == "__main__":
  # Customize these values
  run(
    address="Hermannstraße 100, Berlin",
    category_key="amenity",    # can be "amenity" or "shop"
    category_value="cafe", # e.g. bakery, restaurant, bar, cafe
    max_results=10
  )