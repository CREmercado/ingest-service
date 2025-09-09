import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Tuple

def create_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
    allowed_methods=None,
    timeout: Tuple[int, int] = (5, 300)  # (connect_timeout, read_timeout)
) -> Tuple[requests.Session, Tuple[int, int]]:
    """
    Creates a requests.Session with retry logic and a default timeout tuple.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods or {"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"},
        raise_on_status=False,
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session, timeout
