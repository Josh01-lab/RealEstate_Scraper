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
        if k in ("description", "published_at_text"):
            # keep only if non-empty string
            if isinstance(v, str) and v.strip():
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
    
        # take ownership of payload and clear buffer optimistically
        payload = self.buffer
        self.buffer = []
    
        # chunk size configurable via env var
        try:
            CHUNK = int(os.getenv("SUPABASE_UPSERT_CHUNK", "250"))
        except Exception:
            CHUNK = 250
    
        # helper for logging
        def _log(msg):
            if hasattr(self, "logger") and getattr(self, "logger"):
                self.logger.info(msg)
            else:
                print(msg)
    
        total = len(payload)
        _log(f"[supabase_writer] flushing {total} rows in chunks of {CHUNK}")
    
        # iterate chunks
        for start in range(0, total, CHUNK):
            chunk = payload[start : start + CHUNK]
    
            # try attempts with exponential backoff
            for attempt in range(1, self.retries + 1):
                try:
                    # Primary path: call RPC (recommended if implemented in DB to be NULL-safe)
                    try:
                        self.client.rpc("upsert_listings_batch", {"rows": chunk}).execute()
                        _log(f"[supabase_writer] RPC upsert ok: {len(chunk)} rows")
                        break
                    except Exception as rpc_exc:
                        # if RPC appears to be missing or failing, fall back to direct upsert
                        rpc_msg = str(rpc_exc).lower()
                        if "upsert_listings_batch" in rpc_msg or "could not find" in rpc_msg or "rpc" in rpc_msg:
                            _log("[supabase_writer] RPC upsert failed or not available; attempting fallback to table upsert")
                            # fallback to normal upsert
                            self.client.table("listings").upsert(chunk, on_conflict="url", returning="minimal").execute()
                            _log(f"[supabase_writer] Fallback upsert ok: {len(chunk)} rows")
                            break
                        else:
                            # unknown RPC error: re-raise into outer except to handle retry/backoff
                            raise
    
                except Exception as exc:
                    # last attempt -> restore unsent rows and re-raise
                    if attempt == self.retries:
                        # restore remaining payload (current chunk + rest)
                        remaining = chunk + payload[start + CHUNK 
                        # prepend remaining so next flush will attempt them first
                        self.buffer = remaining + self.buffer
                        _log(f"[supabase_writer] FAILED after {self.retries} attempts; restored {len(remaining)} rows to buffer")
                        raise
                    else:
                        wait = 2 ** attempt
                        _log(f"[supabase_writer] attempt {attempt} failed: {exc}. retrying in {wait}s")
                        time.sleep(wait)
    
        _log("[supabase_writer] flush complete")



    def close(self) -> None:
        self.flush()
