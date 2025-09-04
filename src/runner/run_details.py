import argparse, sys
from pathlib import Path
import jsonlines

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

def _force_run_dir(scraper, run_dir: Path):
    scraper.base_dir = run_dir
    scraper.dirs = {
        "raw_html": run_dir / "raw_html",
        "staged":   run_dir / "staged",
        "logs":     run_dir / "logs",
    }
    for d in scraper.dirs.values():
        d.mkdir(parents=True, exist_ok=True)

def _auto_latest_run() -> Path:
    latest = Path("scraper_output") / "latest_run.txt"
    if latest.exists():
        p = Path(latest.read_text(encoding="utf-8").strip())
        if p.exists():
            return p
    # fallback: newest run_* directory
    runs = sorted(Path("scraper_output").glob("run_*"))
    if runs:
        return runs[-1]
    raise FileNotFoundError("No run dir found. Did discovery run?")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", required=True)
    ap.add_argument("--run-dir", help="same run dir used during discovery (optional)")
    args = ap.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else _auto_latest_run()
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
            u = rec.get("url")
            if u:
                urls.append(u)

    n = scraper.detail_extraction_stage(urls, cfg)
    print(f"✅ Details complete: {n} listings")
    print(f"Wrote: {staged / f'{args.portal}_listings.jsonl'}")

if __name__ == "_main_":
    main()
