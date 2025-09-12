import argparse, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]     # <-- _file_
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.scrapers.property_scraper import PropertyScraper
from src.config import PORTALS_CONFIG

def _select_cfg(scraper, portal_name: str):
    for cfg in scraper.configs:
        if cfg.portal_name == portal_name:
            return cfg
    raise SystemExit(f"Portal '{portal_name}' not found in {PORTALS_CONFIG}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", required=True)
    args = ap.parse_args()

    print(f"[run_discovery] Using portals config: {PORTALS_CONFIG}")
    scraper = PropertyScraper(config_path=str(PORTALS_CONFIG))
    cfg = _select_cfg(scraper, args.portal)

    print(f"[run_discovery] Run dir: {scraper.base_dir}")
    urls = scraper.url_discovery_routine(cfg)
    print(f"[run_discovery] ✅ Discovery complete: {len(urls)} urls")
    print(f"[run_discovery] Staged file: {scraper.dirs['staged'] / f'{args.portal}_urls.jsonl'}")

if __name__ == "__main__":                     # <-- _name_
    main()