import argparse, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.scrapers.property_scraper import PropertyScraper      # <- fix
from src.config import PORTALS_CONFIG
import jsonlines

def _select_cfg(scraper, portal_name: str):
    for cfg in scraper.configs:
        if cfg.portal_name == portal_name:
            return cfg
    raise SystemExit(f"Portal '{portal_name}' not found in {PORTALS_CONFIG}")

def _force_run_dir(scraper, run_dir: Path):
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
    ap.add_argument("--run-dir", required=True, help="same run dir used during discovery")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    staged = run_dir / "staged"
    urls_file = staged / f"{args.portal}_urls.jsonl"
    if not urls_file.exists():
        raise FileNotFoundError(f"Not found: {urls_file}")

    scraper = PropertyScraper(config_path=str(PORTALS_CONFIG))
    _force_run_dir(scraper, run_dir)
    cfg = _select_cfg(scraper, args.portal)

    urls = []
    with jsonlines.open(str(urls_file), "r") as r:
        for rec in r:
            if rec.get("url"):
                urls.append(rec["url"])

    n = scraper.detail_extraction_stage(urls, cfg)
    print(f"✅ Details complete: {n} listings")
    print(f"Wrote: {staged / f'{args.portal}_listings.jsonl'}")

if __name__ == "__main__":
    main()