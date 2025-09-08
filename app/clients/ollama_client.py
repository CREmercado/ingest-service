import requests
from typing import Any, List
from ..config import OLLAMA_URL
from ..logger import setup_logging

log = setup_logging()

def extract_embedding_from_ollama_response(ollama_res: Any) -> List[float]:
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

def embed_text(text: str, model: str, timeout: int = 60) -> List[float]:
    url = f"{OLLAMA_URL.rstrip('/')}/api/embed"
    payload = {"model": model, "input": text}
    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return extract_embedding_from_ollama_response(resp.json())

def ensure_ollama_model(model: str, timeout: int = 300):
    """
    Ensure Ollama model is available. If not, trigger pull.
    """
    # 1) check if model already available
    try:
        resp = requests.get(f"{OLLAMA_URL.rstrip('/')}/api/tags", timeout=10)
        resp.raise_for_status()
        tags = resp.json().get("models", [])
        if any(m.get("name") == model for m in tags):
            log.info(f"Ollama model '{model}' is already available.")
            return
    except Exception as e:
        log.warning(f"Could not list Ollama models: {e}")

    # 2) pull model
    log.info(f"Pulling Ollama model '{model}' ... this may take several minutes")
    resp = requests.post(
        f"{OLLAMA_URL.rstrip('/')}/api/pull",
        json={"model": model},
        timeout=timeout,
    )
    resp.raise_for_status()
    log.info(f"Ollama model '{model}' pull triggered successfully")