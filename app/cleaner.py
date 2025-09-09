import re
import ftfy
import unicodedata
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

stop_words_es = set(stopwords.words('spanish'))
lemmatizer = WordNetLemmatizer()

def clean_text(s: str) -> str:
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