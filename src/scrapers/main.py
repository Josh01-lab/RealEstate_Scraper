import sys, json
from pathlib import Path
from src.config import PORTALS_CONFIG, ENV, validate_prod
from src.utils.logging import get_logger
from src.scrapers.property_scraper import PropertyScraper

logger = get_logger(_name_)

def main() -> int:
    try:
        if ENV == "prod":
            validate_prod()
        scraper = PropertyScraper(PORTALS_CONFIG)
        # run all configs inside
        count = scraper.run_all()   # implement inside your class
        logger.info(f"Done. URLs processed={count}")
        return 0
    except Exception as e:
        logger.exception("Fatal error")
        return 1
