import argparse
from pathlib import Path
import jsonlines
import logging
from dateutil import parser as dtparse
from datetime import timezone
from src.db.supabase_writer import SupabaseWriter


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("write_portal")


def _norm(s):
    if s is None:
        return None
    s2 = str(s).replace("\u00a0", " ")
    s2 = " ".join(s2.split())
    return s2 if s2 else None


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


    count_in = 0
    count_written = 0
    stats = {"has_description": 0, "has_published_at": 0, "price_and_area": 0, "skipped_invalid_url": 0}


    with jsonlines.open(str(staged_file), "r") as r:
        for row in r:
            count_in += 1


            # tolerate either "title" or "listing_title" coming from the parser
            title = row.get("listing_title") or row.get("title") or row.get("listingTitle")


            price = (row.get("price") or {})
            area = (row.get("area") or {})


            # currency guard (accept PHP or peso symbol)
            cur = (price.get("currency") or "")
            cur_up = cur.upper() if isinstance(cur, str) else ""
            is_php = cur_up in ("", "PHP", "₱", "PHP₱")


            payload = {
                "url": row.get("url"),
                "listing_title": _norm(title),
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
                "description": (row.get("description") or None), # keep line breaks but None if empty
            }


            # verify URL is present
            if not payload["url"]:
                logger.warning("Skipping row with no url (row #%d)", count_in)
                stats["skipped_invalid_url"] += 1
                continue


            # normalize published_at -> ISO/UTC if present
            if payload["published_at"]:
                try:
                    payload["published_at"] = dtparse.parse(payload["published_at"]).astimezone(timezone.utc).isoformat()
                    stats["has_published_at"] += 1
                except Exception:
                    logger.debug("Could not parse published_at: %r (url=%s)", payload["published_at"], payload["url"])
                    payload["published_at"] = None


            if payload["description"]:
                stats["has_description"] += 1


            # compute price_per_sqm if possible
            if payload["price_php"] and payload["area_sqm"]:
                try:
                    a = float(payload["area_sqm"])
                    if a > 0:
                        payload["price_per_sqm"] = float(payload["price_php"]) / a
                        stats["price_and_area"] += 1
                except Exception:
                    payload["price_per_sqm"] = None


            # only keep allowed keys (protect against schema drift)
            payload = {k: v for k, v in payload.items() if k in allowed}


            writer.add(payload)
            count_written += 1


    writer.close()


    logger.info("Published to Supabase from %s", staged_file)
    logger.info("Rows read: %d added(%s): %d", count_in, portal, count_written)
    logger.info("Stats: %s", stats)


if __name__ == "__main__":
    main()

