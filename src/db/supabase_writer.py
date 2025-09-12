from typing import List, Dict, Any
import time
import os
from datetime import datetime, timezone
from .supabase_client import get_client

# --- helpers -------------------------------------------------------

def _prune_nulls(d: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only keys that have real values. Allow 0/False.
    Always keep 'url' and 'scraped_at'."""
    keep: Dict[str, Any] = {}
    for k, v in d.items():
        if k in ("url", "scraped_at"):
            keep[k] = v
            continue
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        keep[k] = v
    return keep


# --- writer --------------------------------------------------------

class SupabaseWriter:
    """Buffers rows and upserts them via a Supabase RPC that is NULL-safe.

    Requires the SQL function upsert_listings_batch(rows jsonb) existing in your DB.
    """

    def __init__(self, batch_size: int = 200, retries: int = 3):
        self.client = get_client()
        self.batch_size = batch_size
        self.retries = retries
        self.buffer: List[Dict[str, Any]] = []
        self.source_env = os.getenv("SOURCE_ENV", "prod")

    def add(self, row: Dict[str, Any]) -> None:
        # never pass id — Postgres will gen UUID
        row.pop("id", None)

        # ensure scraped_at is set (ISO8601 w/ tz)
        row["scraped_at"] = row.get("scraped_at") or datetime.now(timezone.utc).isoformat()

        # compute derived numeric fields if both available
        price = row.get("price_php")
        area = row.get("area_sqm")
        try:
            if price is not None and area not in (None, 0, 0.0) and float(area) > 0:
                row["price_per_sqm"] = float(price) / float(area)
        except Exception:
            # best-effort; leave unset if parsing fails
            pass

        # tag rows with environment (useful for QA vs prod)
        row.setdefault("source_env", self.source_env)

        # prune null/empty so we don’t overwrite good data with NULLs
        clean = _prune_nulls(row)

        self.buffer.append(clean)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return

        # Take the batch and clear buffer optimistically
        payload = self.buffer
        self.buffer = []

        # Chunk very large batches to be safe on payload size
        CHUNK = 250
        for i in range(0, len(payload), CHUNK):
            chunk = payload[i:i + CHUNK]

            for attempt in range(1, self.retries + 1):
                try:
                    # NULL-safe upsert via RPC (uses upsert_listings_batch)
                    self.client.rpc('upsert_listings_batch', {'rows': chunk}).execute()
                    break
                except Exception as e:
                    if attempt == self.retries:
                        # If it still fails on the last attempt, re-raise
                        raise
                    time.sleep(2 ** attempt)  # simple exponential backoff

    def close(self) -> None:
        self.flush()
