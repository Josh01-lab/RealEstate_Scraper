import argparse, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
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
    scraper.base_dir = run_dir
    scraper.dirs = {
        "raw_html": run_dir / "raw_html",
        "staged":   run_dir / "staged",
        "logs":     run_dir / "logs",
    }
    for d in scraper.dirs.values():
        d.mkdir(parents=True, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", required=True)
    ap.add_argument("--run-dir", help="Reuse a specific run directory (optional)")
    args = ap.parse_args()

    scraper = PropertyScraper(config_path=str(PORTALS_CONFIG))
    if args.run_dir:
        _maybe_force_run_dir(scraper, Path(args.run_dir))

    cfg = _select_cfg(scraper, args.portal)
    urls = scraper.url_discovery_routine(cfg)

    # Persist the chosen run dir so later steps can find it
    latest = Path("scraper_output") / "latest_run.txt"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(str(scraper.base_dir), encoding="utf-8")

    print(f"✅ Discovery complete: {len(urls)} urls")
    print(f"Run dir: {scraper.base_dir}")

if __name__ == "_main_":
    main()
