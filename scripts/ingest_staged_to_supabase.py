import os, jsonlines, logging
from pathlib import Path
from datetime import datetime
from supabase_client import SupabaseClient  # your minimal client wrapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest")

def ingest_portal(portal_name: str, staged_dir: Path, supabase: SupabaseClient):
    jsonl_file = staged_dir / f"{portal_name}_listings.jsonl"
    if not jsonl_file.exists():
        logger.warning(f"No staged file found for {portal_name}")
        return

    rows = []
    with jsonlines.open(jsonl_file) as reader:
        for rec in reader:
            # normalize schema for Supabase listings
            rows.append({
                "url": rec.get("url"),
                "listing_title": rec.get("title"),
                "property_type": rec.get("property_type"),
                "address": rec.get("address"),
                "price_php": rec.get("price", {}).get("value"),
                "area_sqm": rec.get("area", {}).get("sqm"),
                "price_per_sqm": None,  # let Supabase compute or update later
                "price_json": rec.get("price"),
                "area_json": rec.get("area"),
                "scraped_at": rec.get("scraped_at") or datetime.utcnow().isoformat(),
                "source": portal_name,
            })

    if rows:
        logger.info(f"Inserting {len(rows)} rows for {portal_name}")
        supabase.upsert_listings(rows)  # clean writer

def main():
    staged_dir = Path("scraper_output/latest/staged")  # or adapt to your run pattern
    portals = [f.stem.replace("_listings", "") for f in staged_dir.glob("*_listings.jsonl")]

    supabase = SupabaseClient(
        url=os.getenv("SUPABASE_URL"),
        key=os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
    )

    for portal in portals:
        ingest_portal(portal, staged_dir, supabase)

if _name_ == "_main_":
    main()
