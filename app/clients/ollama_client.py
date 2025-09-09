from typing import Any, List, Tuple
import requests
from .http_client import create_session
from ..config import OLLAMA_URL, HTTP_RETRIES, HTTP_BACKOFF_FACTOR, CONNECT_TIMEOUT, READ_TIMEOUT
from ..logger import setup_logging

log = setup_logging()
session, timeout = create_session(
    retries=HTTP_RETRIES,
    backoff_factor=HTTP_BACKOFF_FACTOR,
    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
)

def extract_embedding_from_ollama_response(ollama_res: Any) -> List[float]:
    """
    Normalize different Ollama embedding response formats into a flat list of floats.
    """
    resp_item = ollama_res[0] if isinstance(ollama_res, list) and len(ollama_res) > 0 else ollama_res

    if isinstance(resp_item, dict):
        embs = resp_item.get("embeddings") or resp_item.get("embedding")
        if embs is None:
            for res in (ollama_res if isinstance(ollama_res, list) else [ollama_res]):
                if isinstance(res, dict) and "embeddings" in res:
                    embs = res["embeddings"]
                    break
        if embs is None:
            raise ValueError("No embeddings key found in Ollama response")

        if isinstance(embs, list) and len(embs) > 0 and isinstance(embs[0], list):
            return embs[0]
        if isinstance(embs, list):
            return embs

    if isinstance(ollama_res, list) and all(isinstance(x, (int, float)) for x in ollama_res):
        return ollama_res

    raise ValueError("Unrecognized embedding shape from Ollama")


def embed_text(text: str, model: str) -> List[float]:
    """
    Call Ollama's /api/embed to create embeddings for the given text.
    """
    url = f"{OLLAMA_URL.rstrip('/')}/api/embed"
    payload = {"model": model, "input": text}

    resp = session.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return extract_embedding_from_ollama_response(resp.json())


def ensure_ollama_model(model: str):
    """
    Ensure Ollama model is available. If not, trigger a pull.
    """
    # 1) Check if the model is already available
    try:
        resp = session.get(f"{OLLAMA_URL.rstrip('/')}/api/tags", timeout=timeout)
        resp.raise_for_status()
        tags = resp.json().get("models", [])
        if any(m.get("name") == model or m.get("name", "").startswith(model) for m in tags):
            log.info(f"Ollama model '{model}' is already available.")
            return
    except Exception as e:
        log.warning(f"Could not list Ollama models: {e}")

    # 2) Pull the model
    log.info(f"Pulling Ollama model '{model}' ... this may take several minutes")
    resp = session.post(
        f"{OLLAMA_URL.rstrip('/')}/api/pull",
        json={"model": model},
        timeout=timeout,
    )
    resp.raise_for_status()
    log.info(f"Ollama model '{model}' pull triggered successfully")