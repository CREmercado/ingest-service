import requests
from typing import List, Dict, Any
from config import QDRANT_URL

def upsert_points(points: List[Dict[str, Any]], collection: str):
    url = f"{QDRANT_URL.rstrip('/')}/collections/{collection}/points"
    body = {"points": points}
    resp = requests.put(url, json=body, timeout=60)
    resp.raise_for_status()
    return resp.json()