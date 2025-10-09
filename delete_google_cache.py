from pathlib import Path
import json

CACHE_DIR = Path("data/.geo_cache")
deleted = 0
kept = 0

for p in CACHE_DIR.glob("*.json"):
    try:
        obj = json.loads(p.read_text())
    except Exception:
        # corrupt or non-JSON: skip
        kept += 1
        continue

    # Heuristic: google cache objects have google_* fields
    if any(k.startswith("google_") for k in obj.keys()):
        p.unlink()
        deleted += 1
    else:
        kept += 1

print(f"Deleted {deleted} Google cache files; kept {kept} others.")
