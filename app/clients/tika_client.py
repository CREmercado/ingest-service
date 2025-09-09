from typing import Optional
from charset_normalizer import from_bytes
from .http_client import create_session
from ..config import TIKA_URL, HTTP_RETRIES, HTTP_BACKOFF_FACTOR, CONNECT_TIMEOUT, READ_TIMEOUT

session, timeout = create_session(
    retries=HTTP_RETRIES,
    backoff_factor=HTTP_BACKOFF_FACTOR,
    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
)

def extract_text(file_bytes: bytes, prefer_utf8: bool = True) -> str:
    """
    PUT bytes to Tika and return cleaned unicode text.

    Steps:
    1) Call Tika server with Accept=text/plain
    2) Decode bytes -> try UTF-8 first
    3) Fallback to charset-normalizer detection
    4) Cleanup text with ftfy + regex normalization
    """
    headers = {"Accept": "text/plain"}
    resp = session.put(TIKA_URL, headers=headers, data=file_bytes, timeout=timeout)
    resp.raise_for_status()

    data = resp.content  # raw bytes

    # 1) Try UTF-8 directly
    text: Optional[str]
    if prefer_utf8:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = None
    else:
        text = None

    # 2) Fallback: charset detection
    if text is None:
        try:
            detection = from_bytes(data)  # returns CharsetMatch objects
            best = detection.best()
            if best:
                text = best.read()  # decoded string
            else:
                text = data.decode("utf-8", errors="replace")
        except Exception:
            text = data.decode("utf-8", errors="replace")

    # 3) Normalize/fix text
    return text