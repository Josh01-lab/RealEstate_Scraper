from typing import List, Dict, Any
import time
from datetime import datetime, timezone
from .supabase_client import get_client

def _prune_nulls(d: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only keys that have real values. Allow 0/False. Always keep 'url' and 'scraped_at'."""
    keep = {}
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

class SupabaseWriter:
    def __init__(self, batch_size: int = 200, retries: int = 3):
        self.client = get_client()
        self.batch_size = batch_size
        self.retries = retries
        self.buffer: List[Dict] = []

    def add(self, row: Dict):
        row.pop("id", None)  # let DB default gen uuid
        # ensure scraped_at is set and ISO8601 with timezone
        row["scraped_at"] = row.get("scraped_at") or datetime.now(timezone.utc).isoformat()
        # compute derived numeric if both available
        price = row.get("price_php")
        area = row.get("area_sqm")
        if price is not None and area not in (None, 0):
            row["price_per_sqm"] = float(price) / float(area)
        # prune null/empty keys so we don’t overwrite good data with NULLs
        clean = _prune_nulls(row)
        self.buffer.append(clean)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        payload = self.buffer
        self.buffer = []
        for attempt in range(1, self.retries + 1):
            try:
                # requires UNIQUE INDEX on listings(url)
                self.client.table("listings") \
                    .upsert(payload, on_conflict="url", returning="minimal") \
                    .execute()
                return
            except Exception as e:
                if attempt == self.retries:
                    raise
                time.sleep(2 ** attempt)

    def close(self):
        self.flush()