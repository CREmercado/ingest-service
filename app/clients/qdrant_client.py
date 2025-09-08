import requests
from typing import List, Dict, Any
from ..config import QDRANT_URL

def upsert_points(points: List[Dict[str, Any]], collection: str):
    url = f"{QDRANT_URL.rstrip('/')}/collections/{collection}/points"
    body = {"points": points}
    resp = requests.put(url, json=body, timeout=60)
    resp.raise_for_status()
    return resp.json()

def create_collection(collection: str, vector_size: int, distance: str = "Cosine", timeout: int = 30) -> Dict[str, Any]:
    """
    PUT /collections/{collection}
    Create a collection with the given vector size and distance metric.
    """
    url = f"{QDRANT_URL.rstrip('/')}/collections/{collection}"
    body = {"vectors": {"size": vector_size, "distance": distance}}
    resp = requests.put(url, json=body, timeout=timeout)
    resp.raise_for_status()
    return resp.json()