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
    If the collection already exists, do nothing.
    """
    url = f"{QDRANT_URL.rstrip('/')}/collections/{collection}"
    body = {"vectors": {"size": vector_size, "distance": distance}}

    try:
        resp = requests.put(url, json=body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        # Check if the error is a 409 Conflict, which means the collection already exists
        if resp.status_code == 409:
            print(f"Collection '{collection}' already exists. Skipping creation.")
            # Return a neutral response or None, depending on what's expected downstream
            # For example, returning an empty dictionary or a specific message
            return {"status": "already_exists", "collection": collection}
        else:
            # If it's another HTTP error, re-raise it
            raise e
    except requests.exceptions.RequestException as e:
        # Handle other potential errors like connection issues, timeouts, etc.
        print(f"An error occurred during collection creation: {e}")
        raise e