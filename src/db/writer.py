from __future__ import annotations
from typing import Any, Dict, Iterable, List
from .supabase_client import SupabaseClient

def chunked(seq: List[Dict[str, Any]], size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]

def upsert_listings_batched(
    client: SupabaseClient,
    rows: List[Dict[str, Any]],
    *,
    batch_size: int = 200,
    on_conflict: str = "url",   # we created a unique index on url
    require_2xx: bool = True,
) -> None:
    if not rows:
        return

    total = len(rows)
    sent = 0
    for batch in chunked(rows, batch_size):
        status, body = client.upsert_rows(
            "listings",
            batch,
            on_conflict=on_conflict,
            return_representation=False,
        )
        if require_2xx and (status < 200 or status >= 300):
            # raise with body for quick debugging in logs
            raise RuntimeError(f"Supabase upsert failed (status={status}): {body.decode('utf-8', 'ignore')}")
        sent += len(batch)
        print(f"[supabase] upserted {sent}/{total}")
