import requests
from typing import List, Dict, Any
from .http_client import create_session
from ..config import QDRANT_URL, HTTP_RETRIES, HTTP_BACKOFF_FACTOR, CONNECT_TIMEOUT, READ_TIMEOUT

session, timeout = create_session(
    retries=HTTP_RETRIES,
    backoff_factor=HTTP_BACKOFF_FACTOR,
    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
)

def upsert_points(points: List[Dict[str, Any]], collection: str) -> Dict[str, Any]:
    """
    Upsert points into a Qdrant collection.
    """
    url = f"{QDRANT_URL.rstrip('/')}/collections/{collection}/points"
    body = {"points": points}
    resp = session.put(url, json=body, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def create_collection(
    collection: str,
    vector_size: int,
    distance: str = "Cosine"
) -> Dict[str, Any]:
    """
    PUT /collections/{collection}
    Create a collection with the given vector size and distance metric.
    If the collection already exists, return a neutral response.
    """
    url = f"{QDRANT_URL.rstrip('/')}/collections/{collection}"
    body = {"vectors": {"size": vector_size, "distance": distance}}

    try:
        resp = session.put(url, json=body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    except requests.exceptions.HTTPError as e:
        if resp.status_code == 409:
            # 409 = collection already exists
            return {"status": "already_exists", "collection": collection}
        else:
            raise e

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"An error occurred during collection creation: {e}") from e