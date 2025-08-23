import argparse, glob, json
from src.scrapers.property_scraper import PropertyScraper
import jsonlines, os, logging

def load_urls_for(scraper: PropertyScraper, portal_name: str):
    # prefer current run’s file
    curr = scraper.dirs["staged"] / f"{portal_name}_urls.jsonl"
    if curr.exists():
        with jsonlines.open(curr) as r:
            for rec in r:
                yield rec["url"]
        return
    # fallback to any run
    pattern = os.path.join("scraper_output", "run_*", "staged", f"{portal_name}_urls.jsonl")
    for path in sorted(glob.glob(pattern)):
        with jsonlines.open(path) as r:
            for rec in r:
                yield rec["url"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/portals.json")
    ap.add_argument("--phase", choices=["discover","details","all"], default="all")
    ap.add_argument("--portals", default="")
    args = ap.parse_args()

    s = PropertyScraper(args.config)

    # filter to requested portals (comma-separated)
    names = [p.strip() for p in args.portals.split(",") if p.strip()]
    configs = [c for c in s.configs if (not names or c.portal_name in names)]
    if not configs:
        print(f"No matching portals for: {names}")
        return

    if args.phase in ("discover","all"):
        for cfg in configs:
            s.url_discovery_routine(cfg)

    if args.phase in ("details","all"):
        for cfg in configs:
            urls = list(load_urls_for(s, cfg.portal_name))
            logging.getLogger("scraper").info(f"Details start {cfg.portal_name}: {len(urls)} URLs")
            s.detail_extraction_stage(urls, cfg)
            logging.getLogger("scraper").info(f"Details done {cfg.portal_name}")
            
if __name__ == "__main__":
    main()
