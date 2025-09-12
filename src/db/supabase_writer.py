from typing import List, Dict, Any
import os
import time
from datetime import datetime, timezone
from .supabase_client import get_client


def _prune_nulls(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only keys that have real values.
    Always keep 'url', 'scraped_at', and 'source_env'.
    Allow 0/False.
    """
    keep = {}
    for k, v in d.items():
        if k in ("url", "scraped_at", "source_env"):
            keep[k] = v
            continue
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        keep[k] = v
    return keep


class SupabaseWriter:
    def __init__(self, batch_size: int = 200, retries: int = 3):
        self.client = get_client()
        self.batch_size = batch_size
        self.retries = retries
        self.buffer: List[Dict[str, Any]] = []

    def add(self, row: Dict[str, Any]):
        # Never send an id (DB will generate)
        row.pop("id", None)

        # Always tag environment
        row["source_env"] = os.getenv("SOURCE_ENV", "prod")

        # Ensure scraped_at is ISO8601 with timezone
        row["scraped_at"] = row.get("scraped_at") or datetime.now(timezone.utc).isoformat()

        # Compute derived price_per_sqm if possible
        price = row.get("price_php")
        area = row.get("area_sqm")
        try:
            if price is not None and area not in (None, 0) and float(area) > 0:
                row["price_per_sqm"] = float(price) / float(area)
        except Exception:
            # if parsing fails, just skip the derived field
            row.pop("price_per_sqm", None)

        clean = _prune_nulls(row)  # don’t overwrite good DB values with NULLs
        self.buffer.append(clean)

        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return

        payload = self.buffer  # capture current buffer
        # NOTE: clear only after success
        for attempt in range(1, self.retries + 1):
            try:
                # Requires UNIQUE INDEX on public.listings(url)
                # returning="minimal" cuts payload overhead
                self.client.table("listings") \
                    .upsert(payload, on_conflict="url", returning="minimal") \
                    .execute()
                # success -> clear buffer
                self.buffer.clear()
                return
            except Exception as e:
                if attempt == self.retries:
                    raise
                time.sleep(2 ** attempt)  # simple backoff

    def close(self):
        self.flush()
