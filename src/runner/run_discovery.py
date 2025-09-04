import argparse, json, sys, time
from pathlib import Path

# import path
ROOT = Path(_file_).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.scrapers.scraper import PropertyScraper
from src.config import PORTALS_CONFIG

def _select_cfg(scraper, portal_name: str):
    for cfg in scraper.configs:
        if cfg.portal_name == portal_name:
            return cfg
    raise SystemExit(f"Portal '{portal_name}' not found in {PORTALS_CONFIG}")

def _maybe_force_run_dir(scraper, run_dir: Path):
    # Re-point the scraper to a known run dir so discovery & details share files
    scraper.base_dir = run_dir
    scraper.dirs = {
        "raw_html": run_dir / "raw_html",
        "staged": run_dir / "staged",
        "logs": run_dir / "logs",
    }
    for d in scraper.dirs.values():
        d.mkdir(parents=True, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", required=True)
    ap.add_argument("--run-dir", help="reuse a specific run directory")
    args = ap.parse_args()

    scraper = PropertyScraper(config_path=str(PORTALS_CONFIG))

    if args.run_dir:
        _maybe_force_run_dir(scraper, Path(args.run_dir))

    cfg = _select_cfg(scraper, args.portal)

    urls = scraper.url_discovery_routine(cfg)
    print(f"✅ Discovery complete: {len(urls)} urls")
    print(f"Run dir: {scraper.base_dir}")

if _name_ == "_main_":
    main()
