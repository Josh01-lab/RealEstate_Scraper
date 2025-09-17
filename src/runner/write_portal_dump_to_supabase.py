import argparse
from pathlib import Path
import jsonlines
from dateutil import parser as dtparse
from datetime import timezone
from src.db.supabase_writer import SupabaseWriter

def _norm(s):
    if s is None: return None
    return " ".join(str(s).replace("\u00a0"," ").split())

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--portal", required=True)
    p.add_argument("--run-dir", required=True)
    args = p.parse_args()

    run_dir = Path(args.run_dir)
    portal = args.portal

    staged_file = run_dir / "staged" / f"{portal}_listings.jsonl"
    if not staged_file.exists():
        raise FileNotFoundError(f"Not found: {staged_file}")

    writer = SupabaseWriter(batch_size=200)

    allowed = {
        "url", "listing_title", "property_type", "address",
        "price_php", "area_sqm", "price_per_sqm",
        "price_json", "area_json", "scraped_at", "source",
        "published_at_text", "published_at", "description",
    }

    with jsonlines.open(str(staged_file), "r") as r:
        for row in r:
            price = (row.get("price") or {})
            area = (row.get("area") or {})

            # currency guard (accept PHP or peso symbol)
            cur = (price.get("currency") or "")
            cur_up = cur.upper() if isinstance(cur, str) else ""
            is_php = cur_up in ("", "PHP", "₱", "PHP₱")

            payload = {
                "url": row.get("url"),
                "listing_title": _norm(row.get("title")),
                "property_type": _norm(row.get("property_type")),
                "address": _norm(row.get("address")),
                "price_php": price.get("value") if isinstance(price, dict) and is_php else None,
                "area_sqm": area.get("sqm") if isinstance(area, dict) else None,
                "price_per_sqm": None,
                "price_json": price if isinstance(price, dict) else None,
                "area_json": area if isinstance(area, dict) else None,
                "scraped_at": row.get("scraped_at"),
                "source": portal,
                "published_at_text": _norm(row.get("published_at_text")),
                "published_at": row.get("published_at"),
                "description": row.get("description"), # keep line breaks
            }

            # normalize published_at → ISO with tz if present
            if payload["published_at"]:
                try:
                    payload["published_at"] = dtparse.parse(payload["published_at"]).astimezone(timezone.utc).isoformat()
                except Exception:
                    payload["published_at"] = None

            # compute price_per_sqm
            if payload["price_php"] and payload["area_sqm"]:
                try:
                    if float(payload["area_sqm"]) > 0:
                        payload["price_per_sqm"] = float(payload["price_php"]) / float(payload["area_sqm"])
                except Exception:
                    payload["price_per_sqm"] = None

            payload = {k: v for k, v in payload.items() if k in allowed}
            writer.add(payload)

    writer.close()
    print(f"✅ Published to Supabase from {staged_file}")

if __name__ == "__main__":
    main()