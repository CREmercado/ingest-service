import re
import ftfy
import requests
import unicodedata
from typing import Optional
from ..config import TIKA_URL
from charset_normalizer import from_bytes

def _cleanup_unicode(s: str) -> str:
    # Normalize unicode form
    s = unicodedata.normalize("NFC", s)

    # ftfy fixes mojibake, weird escapes, smart quotes, etc.
    s = ftfy.fix_text(s)

    # Remove control chars except newline/tab
    s = re.sub(r"[^\S\r\n\t]+", " ", s)   # collapse other whitespace to single space
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+", "", s)

    # Normalize multiple blank lines to two
    s = re.sub(r"\n{3,}", "\n\n", s)

    # Trim
    return s.strip()

def extract_text(file_bytes: bytes, timeout: int = 60, prefer_utf8: bool = True) -> str:
    """
    PUT bytes to Tika and return cleaned unicode text.
    This function:
      - gets resp.content (bytes),
      - tries UTF-8, else uses charset-normalizer to detect,
      - uses ftfy + unicodedata to normalize and fix mojibake,
      - strips control chars and collapses whitespace.
    """
    headers = {"Accept": "text/plain"}
    resp = requests.put(TIKA_URL, headers=headers, data=file_bytes, timeout=timeout)
    resp.raise_for_status()

    data = resp.content  # bytes

    # 1) fast path: try utf-8
    if prefer_utf8:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = None
    else:
        text = None

    # 2) detection with charset-normalizer
    if text is None:
        try:
            detection = from_bytes(data)  # returns CharsetMatch objects
            best = detection.best()
            if best:
                text = best.read()  # returns str decoded with detected encoding
            else:
                # fallback to utf-8 with replacement
                text = data.decode("utf-8", errors="replace")
        except Exception:
            text = data.decode("utf-8", errors="replace")

    # 3) Fix common mojibake and normalize
    text = _cleanup_unicode(text)

    return text