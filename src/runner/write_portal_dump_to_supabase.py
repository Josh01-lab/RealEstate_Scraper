import argparse, sys, json
from pathlib import Path
from typing import Dict, Any
import jsonlines

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.writers.supabase_writer import SupabaseWriter
from src.config import PORTALS_CONFIG

def _auto_latest_run() -> Path:
    latest = Path("scraper_output") / "latest_run.txt"
    if latest.exists():
        p = Path(latest.read_text(encoding="utf-8").strip())
        if p.exists():
            return p
    runs = sorted(Path("scraper_output").glob("run_*"))
    if runs:
        return runs[-1]
    raise FileNotFoundError("No run dir found. Did discovery & details run?")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", required=True)
    ap.add_argument("--run-dir", help="path to scraper_output/run_YYYYMMDD_HHMMSS (optional)")
    args = ap.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else _auto_latest_run()
    staged = run_dir / "staged"
    in_file = staged / f"{args.portal}_listings.jsonl"
    if not in_file.exists():
        raise FileNotFoundError(f"Not found: {in_file}")

    portals = json.loads(Path(PORTALS_CONFIG).read_text(encoding="utf-8")).get("portals", [])
    names = {p["portal_name"] for p in portals}
    if args.portal not in names:
        raise SystemExit(f"Portal '{args.portal}' not found in {PORTALS_CONFIG}. Have: {sorted(names)}")

    writer = SupabaseWriter(batch_size=200)
    with jsonlines.open(str(in_file), "r") as r:
        for row in r:
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
            price = row.get("price") or {}
            if isinstance(price, dict) and (price.get("currency") in (None, "PHP")):
                payload["price_php"] = price.get("value")

            area = row.get("area") or {}
            if isinstance(area, dict) and area.get("sqm"):
                payload["area_sqm"] = area.get("sqm")

            if payload["price_php"] and payload["area_sqm"] and payload["area_sqm"] > 0:
                try:
                    payload["price_per_sqm"] = float(payload["price_php"]) / float(payload["area_sqm"])
                except Exception:
                    pass

            writer.add(payload)

    writer.close()
    print(f"✅ Published {args.portal} dump to Supabase from {in_file}")

if __name__ == "_main_":
    main()
