# src/db/supabase_io.py
import os
from typing import Iterable, Dict, Any, List
from supabase import create_client, Client

def get_sb(service: bool = False) -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"] if service else os.environ["SUPABASE_ANON_KEY"]
    return create_client(url, key)

def chunked(iterable: Iterable, size: int):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def upsert_listings(rows: List[Dict[str, Any]], table: str = "listings", conflict: str = "url"):
    sb = get_sb(service=True)  # service key for writes
    for batch in chunked(rows, 500):
        # Supabase upsert: on_conflict="url" assumes unique index on url
        sb.table(table).upsert(batch, on_conflict=conflict).execute()
