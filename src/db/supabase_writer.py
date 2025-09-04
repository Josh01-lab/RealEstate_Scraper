from _future_ import annotations
from typing import List, Dict, Optional
from datetime import datetime, timezone
import time
import jsonlines

from .supabase_client import get_client  # your minimal client

def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # handles '...Z' and with offset
        s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _coerce_num(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _normalize_scrape_record(rec: Dict, source: str) -> Dict:
    """
    Map one scraped record (from JSONL) to the listings schema.
    Input rec is expected to look like your ListingData dict:
      { url, title, price={value,currency,period,raw}, area={sqm,raw}, address, property_type, scraped_at, ... }
    """
    url = (rec.get("url") or "").strip()
    title = rec.get("title")
    address = rec.get("address")
    property_type = rec.get("property_type")

    price = rec.get("price") or {}
    area = rec.get("area") or {}

    price_php = None
    if (price.get("currency") == "PHP") and (price.get("period") in (None, "month")):
        price_php = _coerce_num(price.get("value"))

    area_sqm = _coerce_num(area.get("sqm"))
    ppsqm = (price_php / area_sqm) if (price_php and area_sqm and area_sqm > 0) else None

    scraped_dt = _parse_iso(rec.get("scraped_at")) or datetime.now(timezone.utc)

    return {
        # listings columns
        "url": url,
        "listing_title": title,
        "property_type": property_type,
        "address": address,
        "price_php": price_php,
        "area_sqm": area_sqm,
        "price_per_sqm": ppsqm,
        "price_json": price or None,
        "area_json": area or None,
        "scraped_at": _to_iso(scraped_dt),
        "source": source,
    }, scraped_dt  # also return parsed dt for snapshot

class SupabaseWriter:
    """
    - Upserts each listing by URL (returns listing UUID).
    - Upserts a daily snapshot into listing_daily (listing_id + scraped_date).
    - Keeps a small buffer for listings upsert (optional); snapshots are written per-row because
      we need the listing_id immediately after upsert.
    """
    def __init__(self, batch_size: int = 100, retries: int = 3):
        self.client = get_client()
        self.batch_size = batch_size
        self.retries = retries
        self.buffer: List[Dict] = []          # listings buffer (normalized rows)
        self.buffer_meta: List[dict] = []      # aligns with buffer: holds scraped_dt + source per row

    def add_scrape_record(self, rec: Dict, source: str):
        listing_row, scraped_dt = _normalize_scrape_record(rec, source)
        self.buffer.append(listing_row)
        self.buffer_meta.append({"scraped_dt": scraped_dt, "source": source})
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def add_many_jsonl(self, jsonl_path: str, source: str):
        with jsonlines.open(jsonl_path) as reader:
            for rec in reader:
                # skip if URL missing
                if not rec.get("url"):
                    continue
                self.add_scrape_record(rec, source)
        self.flush()

    def flush(self):
        if not self.buffer:
            return

        rows = self.buffer
        metas = self.buffer_meta

        # attempt upsert listings with exponential backoff
        for attempt in range(1, self.retries + 1):
            try:
                # Upsert by URL and return representation so we get IDs
                resp = self.client.table("listings") \
                    .upsert(rows, on_conflict="url") \
                    .select("id,url") \
                    .execute()

                # Build a quick lookup: url -> id
                got = resp.data or []
                url_to_id = {r["url"]: r["id"] for r in got if r.get("id") and r.get("url")}

                # Insert/Upsert daily snapshots per returned row
                # (We only snapshot rows that were returned this flush.)
                snapshot_payload = []
                for row, meta in zip(rows, metas):
                    url = row["url"]
                    listing_id = url_to_id.get(url)
                    if not listing_id:
                        continue
                    scraped_date = (meta["scraped_dt"].date() if meta.get("scraped_dt") else None)
                    if not scraped_date:
                        continue

                    snapshot_payload.append({
                        "listing_id": listing_id,
                        "scraped_date": str(scraped_date),
                        "seen_at": row.get("scraped_at"),
                        "price_php": row.get("price_php"),
                        "area_sqm": row.get("area_sqm"),
                        "price_per_sqm": row.get("price_per_sqm"),
                        "is_active": True,
                        "property_type": row.get("property_type"),
                        "source": row.get("source"),
                    })

                if snapshot_payload:
                    # composite conflict target
                    self.client.table("listing_daily") \
                        .upsert(snapshot_payload, on_conflict="listing_id,scraped_date") \
                        .execute()

                # clear buffers after success
                self.buffer.clear()
                self.buffer_meta.clear()
                return

            except Exception as e:
                if attempt == self.retries:
                    # last attempt—rethrow
                    raise
                time.sleep(2 ** attempt)  # simple backoff

    def close(self):
        self.flush()
