
from urllib.parse import urlparse

def canonicalize(url: str) -> str:
    pu = urlparse(url)
    return f"{pu.scheme}://{pu.netloc}{pu.path}".rstrip("/")

