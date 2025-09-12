from pathlib import Path
import json, sys
from bs4 import BeautifulSoup
from datetime import datetime, timezone

from src.scrapers.property_scraper import ScrapingConfig, PropertyScraper

PORTALS = Path("config/portals.json")

def main():
    if len(sys.argv) < 3:
        print("Usage: python -m src.tools.test_selectors <portal_name> <listing_url>")
        sys.exit(1)

    portal, url = sys.argv[1], sys.argv[2]
    cfg_json = json.loads(PORTALS.read_text(encoding="utf-8"))
    cfg_obj = None
    for p in cfg_json.get("portals", []):
        if p["portal_name"] == portal:
            cfg_obj = ScrapingConfig(**p)
            break
    if not cfg_obj:
        raise SystemExit(f"Portal '{portal}' not found in {PORTALS}")

    scraper = PropertyScraper(config_path=str(PORTALS))
    html = scraper._get_page_content(url, cfg_obj)
    if not html:
        print("Failed to fetch HTML")
        sys.exit(1)

    soup = BeautifulSoup(html, "lxml")
    print(f"Testing selectors for: {url}\n")

    for field, sel in cfg_obj.detail_selectors.items():
        if field.startswith("_"):  # skip meta
            continue
        el = soup.select_one(sel)
        text = el.get_text(" ", strip=True) if el else None
        print(f"{field:<18} ->", repr(text))

    # show how the parser would normalize
    from src.scrapers.property_scraper import ListingData
    listing = ListingData(url=url, scraped_at=datetime.now(timezone.utc).isoformat())
    for field, sel in cfg_obj.detail_selectors.items():
        if field.startswith("_"): 
            continue
        el = soup.select_one(sel)
        if not el:
            continue
        text = el.get_text(" ", strip=True)
        if field == "area":
            listing.area = scraper._normalize_area(text)
        elif field == "price":
            listing.price = scraper._normalize_price(text)
        else:
            setattr(listing, field, text)

    print("\nNormalized preview:")
    print("title:", listing.title)
    print("address:", listing.address)
    print("price:", listing.price)
    print("area:", listing.area)
    print("description:", (listing.description[:120] + "…") if listing.description else None)

if __name__ == "__main__":
    main()