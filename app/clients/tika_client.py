import re
import ftfy
import unicodedata
from typing import Optional, Tuple
from charset_normalizer import from_bytes
from .http_client import create_session
from ..config import TIKA_URL, HTTP_RETRIES, HTTP_BACKOFF_FACTOR, CONNECT_TIMEOUT, READ_TIMEOUT
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

stop_words_es = set(stopwords.words('spanish'))
lemmatizer = WordNetLemmatizer()

session, timeout = create_session(
    retries=HTTP_RETRIES,
    backoff_factor=HTTP_BACKOFF_FACTOR,
    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
)

def _cleanup_unicode(s: str) -> str:
    """
    Normaliza, limpia y sanitiza texto jurídico.
    Incluye normalización Unicode, corrección, lowercasing,
    eliminación de stopwords y lematización.
    """
    # Normalización y corrección Unicode
    s = unicodedata.normalize("NFC", s)
    s = ftfy.fix_text(s)

    # Convertir a minúsculas
    s = s.lower()

    # Colapsar espacios y eliminar caracteres de control
    s = re.sub(r"[^\S\r\n\t]+", " ", s)
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+", "", s)

    # Eliminar URLs y cualquier cosa que se parezca a una URL
    s = re.sub(r'http\S+|www.\S+', '', s)

    # Tokenizar para lematización y eliminación de stopwords
    tokens = nltk.word_tokenize(s)
    
    # Eliminar stopwords
    # Es crucial usar una lista de stopwords apropiada para español
    stop_words_es = set(stopwords.words('spanish'))
    filtered_tokens = [word for word in tokens if word not in stop_words_es]

    # Lematización
    # Para textos jurídicos, la lematización es útil pero a veces puede ser
    # demasiado agresiva. Se puede usar stemming como alternativa si es necesario
    # nltk.stem.snowball.SpanishStemmer().stem()
    lemmatizer = WordNetLemmatizer()
    lemmas = [lemmatizer.lemmatize(word) for word in filtered_tokens]

    # Reconstruir la cadena
    s = ' '.join(lemmas)

    # Normalizar múltiples saltos de línea a un máximo de 2
    s = re.sub(r"\n{3,}", "\n\n", s)

    return s.strip()


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
    return _cleanup_unicode(text)