import argparse
from pathlib import Path
import json
import jsonlines
from typing import Dict, Any

# make src importable
import sys
ROOT = Path(_file_).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.writers.supabase_writer import SupabaseWriter  # you already have this
from src.config import PORTALS_CONFIG

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", required=True, help="portal name (must match your portals.json)")
    ap.add_argument("--run-dir", required=True, help="path to scraper_output/run_YYYYMMDD_HHMMSS")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    staged = run_dir / "staged"
    in_file = staged / f"{args.portal}_listings.jsonl"

    if not in_file.exists():
        raise FileNotFoundError(f"Not found: {in_file}")

    # optional: validate that portal exists in portals.json
    portals = json.loads(Path(PORTALS_CONFIG).read_text(encoding="utf-8")).get("portals", [])
    names = {p["portal_name"] for p in portals}
    if args.portal not in names:
        raise SystemExit(f"Portal '{args.portal}' not found in {PORTALS_CONFIG}. Have: {sorted(names)}")

    writer = SupabaseWriter(batch_size=200)
    # minimal: add rows from jsonl and push
    with jsonlines.open(str(in_file), "r") as r:
        for row in r:
            # ensure the fields the DB expects exist (defensively)
            payload: Dict[str, Any] = {
                "url": row.get("url"),
                "listing_title": row.get("title"),
                "property_type": row.get("property_type"),
                "address": row.get("address"),
                "price_php": None,
                "area_sqm": None,
                "price_per_sqm": None,
                "price_json": row.get("price"),
                "area_json": row.get("area"),
                "scraped_at": row.get("scraped_at"),
                "source": args.portal,
            }
            # best-effort derive numeric fields if present
            price = row.get("price") or {}
            if price and isinstance(price, dict) and (price.get("currency") in (None, "PHP")):
                payload["price_php"] = price.get("value")

            area = row.get("area") or {}
            if area and isinstance(area, dict) and area.get("sqm"):
                payload["area_sqm"] = area.get("sqm")

            if payload["price_php"] and payload["area_sqm"] and payload["area_sqm"] > 0:
                payload["price_per_sqm"] = float(payload["price_php"]) / float(payload["area_sqm"])

            writer.add(payload)

    writer.close()
    print(f"✅ Published {args.portal} dump to Supabase from {in_file}")

if _name_ == "_main_":
    main()
