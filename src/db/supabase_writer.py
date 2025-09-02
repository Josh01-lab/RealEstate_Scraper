import logging
from supabase import create_client

logger = logging.getLogger("supabase")

class SupabaseClient:
    def __init__(self, url: str, key: str):
        if not url or not key:
            raise RuntimeError("Supabase URL/Key missing")
        self.client = create_client(url, key)

    def upsert_listings(self, rows: list[dict]):
        """Upsert normalized listing rows into listings (conflict on URL)."""
        if not rows:
            return
        try:
            resp = (
                self.client
                .table("listings")
                .upsert(rows, on_conflict=["url"])
                .execute()
            )
            logger.info(f"Upserted {len(rows)} rows → listings")
            return resp.data
        except Exception as e:
            logger.error(f"Supabase upsert failed: {e}")
            raise
