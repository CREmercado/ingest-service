from typing import List

def _find_safe_break(s: str, start: int, end: int, max_chars: int) -> int:
    slice_ = s[start:end]
    idx = max(
        slice_.rfind(". "),
        slice_.rfind("? "),
        slice_.rfind("! "),
        slice_.rfind("\n\n"),
    )
    if idx > int(max_chars * 0.3):
        return start + idx + 1
    return end

def chunk_text(raw: str, max_chars: int, overlap: int) -> List[str]:
    text = raw.replace("\r\n", "\n")
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    text = text.strip()
    if not text:
        return []
    chunks: List[str] = []
    start = 0
    L = len(text)
    while start < L:
        end = min(start + max_chars, L)
        if end < L:
            end = _find_safe_break(text, start, end, max_chars)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = max(0, end - overlap)
        if end == L:
            break
    return chunks