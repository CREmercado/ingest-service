import requests
from typing import Optional
from config import TIKA_URL

def extract_text(file_bytes: bytes, timeout: int = 60) -> str:
    headers = {"Accept": "text/plain"}
    resp = requests.put(TIKA_URL, headers=headers, data=file_bytes, timeout=timeout)
    resp.raise_for_status()
    return resp.text