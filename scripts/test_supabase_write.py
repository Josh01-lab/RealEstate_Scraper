from datetime import datetime, timezone
from src.supabase.writer import upsert_listing_by_url, insert_daily_snapshot

rec = {
    "url": "http://test/local/manual",
    "listing_title": "Manual Smoke",
    "property_type": "Office",
    "address": "Cebu IT Park",
    "price_php": 25000,
    "area_sqm": 50,
    "price_per_sqm": 500,
    "price_json": {"currency":"PHP","value":25000,"period":"month"},
    "area_json": {"sqm":50},
    "scraped_at": datetime.now(timezone.utc),
    "source": "manual",
}
lid = upsert_listing_by_url(rec)
insert_daily_snapshot(lid, {
    "seen_at": datetime.now(timezone.utc),
    "price_php": rec["price_php"],
    "area_sqm": rec["area_sqm"],
    "price_per_sqm": rec["price_per_sqm"],
    "is_active": True,
    "property_type": rec["property_type"],
    "source": rec["source"],
})
print("✅ wrote listing:", lid)
