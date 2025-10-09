import os, time, json, hashlib, requests, threading
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT_FILE  = Path("data/tweets_raw.csv")
OUTPUT_FILE = Path("data/gps_points_geocoded.csv")

CACHE_DIR = Path("data/.geo_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# export GOOGLE_API_KEY="key"
GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")

class RateLimiter:
    """Simple serialized limiter: enforces min period between calls."""
    def __init__(self, rps: float):
        assert rps > 0
        self.period = 1.0 / rps
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            if delay:
                time.sleep(delay)
                
            # reserve the next slot exactly one period from NOW
            self._next = time.monotonic() + self.period

# Nominatim recommends ~1 rps per IP/project
NOMINATIM_LIMITER = RateLimiter(rps=1.0)

# allowed QPS (per IP/key)
GOOGLE_LIMITER = RateLimiter(rps=10.0)

def cache_path(lat, lon, service):
    key = f"{service}:{round(lat,5)},{round(lon,5)}"
    h = hashlib.sha1(key.encode()).hexdigest()
    return CACHE_DIR / f"{h}.json"

def load_cache(lat, lon, service):
    p = cache_path(lat, lon, service)
    if p.exists():
        return json.loads(p.read_text())

    return None

def save_cache(lat, lon, service, data):
    p = cache_path(lat, lon, service)
    p.write_text(json.dumps(data))

def google_revgeo(lat, lon):
    if not GOOGLE_KEY:
        raise RuntimeError("GOOGLE_API_KEY is not set in the environment")

    cached = load_cache(lat, lon, "google")
    if cached is not None:
        return cached

    GOOGLE_LIMITER.wait()

    def comp(query):
        for c in components:
            if query in c.get("types", []):
                return c.get("long_name")
                
        return None

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"latlng": f"{lat},{lon}", "key": GOOGLE_KEY, "result_type":"street_address|premise|subpremise|route"}
    r = requests.get(url, params=params, timeout=20)
    data = r.json()
    if data.get("status") == "OK" and data.get("results"):
        top = data["results"][0]
        components = top.get("address_components", [])

        out = {
            "google_formatted_address": top.get("formatted_address"),
            "google_zip": comp("postal_code"),
            "google_city": comp("locality") or comp("postal_town") or comp("sublocality") or comp("administrative_area_level_2"),
            "google_state": comp("administrative_area_level_1"),
            "google_country": comp("country"),
            "google_poi": None
        }

        # Try to find a nearby POI by inspecting place types in this or next result
        poi_name = None
        for res in data["results"]:
            types = res.get("types", [])
            if any(t in types for t in ["point_of_interest","establishment","premise"]):
                poi_name = res.get("formatted_address")
                break

        out["google_poi"] = poi_name

    else:
        print("status not OK")
        out = {"google_formatted_address": None, "google_zip": None, "google_city": None,
               "google_state": None, "google_country": None, "google_poi": None}

    save_cache(lat, lon, "google", out)
    return out

def nominatim_revgeo(lat, lon):
    cached = load_cache(lat, lon, "nominatim")
    if cached is not None:
        return cached

    NOMINATIM_LIMITER.wait()

    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "lat": lat, "lon": lon, "format": "jsonv2", "addressdetails": 1,
        # include a real email if allowed; required per usage policy for heavy use
        # "email": "your.email@domain.edu"
    }
    headers = {"User-Agent": "USC-CAR-GeoScripts/1.0"}
    r = requests.get(url, params=params, headers=headers, timeout=30)
    if r.status_code == 200:
        data = r.json()
        addr = data.get("address", {})
        out = {
            "nominatim_address": data.get("display_name"),
            "nominatim_zip": addr.get("postcode"),
            "nominatim_city": addr.get("city") or addr.get("town") or addr.get("village") or addr.get("hamlet"),
            "nominatim_state": addr.get("state"),
            "nominatim_country": addr.get("country"),
        }
    else:
        out = {"nominatim_address": None, "nominatim_zip": None, "nominatim_city": None,
               "nominatim_state": None, "nominatim_country": None}

    save_cache(lat, lon, "nominatim", out)
    return out

def revgeo_worker(row_id, lat, lon, sentiment, timestamp):
    g = google_revgeo(lat, lon)
    n = nominatim_revgeo(lat, lon)
    return {
        "row_id": row_id,
        "timestamp": timestamp,
        "lat": lat, "lon": lon,
        "latlon": f"{round(lat,6)},{round(lon,6)}",
        "google_address": g.get("google_formatted_address"),
        "google_zip_code": g.get("google_zip"),
        "google_city": g.get("google_city"),
        "google_country": g.get("google_country"),
        "google_state": g.get("google_state"),
        "google_nearest_poi": g.get("google_poi"),
        "nominatim_address": n.get("nominatim_address"),
        "nominatim_zip_code": n.get("nominatim_zip"),
        "nominatim_city": n.get("nominatim_city"),
        "nominatim_country": n.get("nominatim_country"),
        "nominatim_state": n.get("nominatim_state"),
        "sentiment": sentiment,
    }

def main(max_workers=None):
    start = time.perf_counter()
    if max_workers is None:
        max_workers = min(8, (os.cpu_count() or 2) * 4)

    # timestamp	location_type	longitude	latitude	sentiment
    df = pd.read_csv(INPUT_FILE, low_memory=False)

    df = df[
        pd.to_numeric(df["latitude"], errors="coerce").between(-90, 90)
        & pd.to_numeric(df["longitude"], errors="coerce").between(-180, 180)
    ].reset_index(drop=True)

    df.insert(0, "row_id", range(1, len(df)+1))

    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        # FUTURES VARIABLE (dict): {Future: row_id}
        futures = {
            ex.submit(
                revgeo_worker,
                int(row.row_id),
                float(row.latitude),
                float(row.longitude),
                getattr(row, "sentiment", None),
                getattr(row, "timestamp", None),
            ): int(row.row_id)
            for row in df.itertuples(index=False)
        }
        for fut in as_completed(futures):
            row_id = futures[fut]
            rows.append(fut.result())

    out = pd.DataFrame(rows).sort_values("row_id")
    out.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote {len(out)} rows to {OUTPUT_FILE}")

    # quick summary
    s = {
        "rows": len(out),
        "google_address_coverage": int(out["google_address"].notna().sum()),
        "nominatim_address_coverage": int(out["nominatim_address"].notna().sum()),
        "unique_google_zip": out["google_zip_code"].nunique(dropna=True),
        "unique_google_city": out["google_city"].nunique(dropna=True)
    }
    print("Summary:", s)

    end = time.perf_counter()
    print("Total time:", end-start)

if __name__ == "__main__":
    main()