import sys, os, json
from pathlib import Path
from datetime import datetime

# Make 'src' importable
ROOT = Path(_file_).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import PORTALS_CONFIG, SCRAPING_MODE, MAX_PAGES, RATE_LIMIT_DELAY  # your config module
from src.scrapers.scraper import PropertyScraper  # your scraper class

def main():
    # Keep it tiny & fast for CI/local checks
    max_pages_override = int(os.getenv("DRYRUN_MAX_PAGES", "2"))
    mode_override      = os.getenv("DRYRUN_MODE", "").strip() or SCRAPING_MODE
    rate_delay         = float(os.getenv("DRYRUN_RATE_DELAY", "0.5"))

    cfg_path = Path(PORTALS_CONFIG)
    if not cfg_path.exists():
        print(f"[dry-run] portals config not found: {cfg_path}")
        sys.exit(2)

    print(f"[dry-run] config: {cfg_path}")
    print(f"[dry-run] overrides: MAX_PAGES={max_pages_override}, MODE={mode_override or '(as-config)'}")

    scraper = PropertyScraper(str(cfg_path))

    total_urls = 0
    per_portal = []

    for cfg in scraper.configs:
        # apply overrides safely
        if mode_override:
            cfg.scraping_mode = mode_override
        cfg.max_pages = min(cfg.max_pages, max_pages_override)
        cfg.rate_limit_delay = max(cfg.rate_limit_delay, rate_delay)

        print(f"\n[dry-run] Portal: {cfg.portal_name}")
        print(f"[dry-run]  seed_urls: {len(cfg.seed_urls)} | mode: {cfg.scraping_mode} | max_pages: {cfg.max_pages}")

        try:
            urls = scraper.url_discovery_routine(cfg)  # this should only write jsonl under scraper_output
        except Exception as e:
            print(f"[dry-run]  ERROR: discovery crashed: {e}")
            sys.exit(3)

        n = len(urls)
        total_urls += n
        per_portal.append((cfg.portal_name, n))
        print(f"[dry-run]  discovered: {n} url(s)")

    # Summary
    print("\n[dry-run] SUMMARY")
    for name, n in per_portal:
        print(f"  - {name}: {n} url(s)")
    print(f"  TOTAL: {total_urls} url(s)")

    # Hard fail if nothing found
    if total_urls == 0:
        print("\n[dry-run] FAIL: selectors/pagination likely broken (0 URLs).")
        sys.exit(4)

    print("\n[dry-run] OK")
    sys.exit(0)

if _name_ == "_main_":
    main()
