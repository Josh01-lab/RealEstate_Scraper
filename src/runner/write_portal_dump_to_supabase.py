
import argparse
from pathlib import Path
import jsonlines
from typing import Any, Dict

from src.db.supabase_writer import SupabaseWriter

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", required=True)
    ap.add_argument("--run-dir", required=True)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    staged_file = run_dir / "staged" / f"{args.portal}_listings.jsonl"

    if not staged_file.exists():
        raise FileNotFoundError(f"Not found: {staged_file}")

    writer = SupabaseWriter(batch_size=200)

    allowed = {
        "url", "listing_title", "property_type", "address",
        "price_php", "area_sqm", "price_per_sqm",
        "price_json", "area_json", "scraped_at", "source",
    }

    sent = 0
    with jsonlines.open(str(staged_file), "r") as r:
        for row in r:
            price = (row.get("price") or {})
            area  = (row.get("area")  or {})

            payload: Dict[str, Any] = {
                "url": row.get("url"),
                "listing_title": row.get("title"),
                "property_type": row.get("property_type"),
                "address": row.get("address"),
                "price_php": price.get("value") if isinstance(price, dict) and (price.get("currency") in (None, "PHP")) else None,
                "area_sqm": area.get("sqm") if isinstance(area, dict) else None,
                "price_per_sqm": None,
                "price_json": price if isinstance(price, dict) else None,
                "area_json": area if isinstance(area, dict) else None,
                "scraped_at": row.get("scraped_at"),
                "source": args.portal,
            }

            if payload["price_php"] and payload["area_sqm"]:
                try:
                    a = float(payload["area_sqm"])
                    p = float(payload["price_php"])
                    if a > 0:
                        payload["price_per_sqm"] = p / a
                except Exception:
                    payload["price_per_sqm"] = None

            payload = {k: v for k, v in payload.items() if k in allowed}

            writer.add(payload)
            sent += 1

    writer.close()
    print(f"✅ Published {sent} rows to Supabase from {staged_file}")

if __name__ == "__main__":
    main()