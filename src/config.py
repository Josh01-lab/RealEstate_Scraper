import os
import json
from pathlib import Path
from typing import Any, Dict

# project root = parent of src/
ROOT = Path(__file__).resolve().parents[1]

# runtime mode
ENV = os.getenv("ENV", "dev")  # dev|prod

# DB
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
SQLITE_DB    = os.getenv("SQLITE_DB", str(ROOT / "data" / "db" / "central.db"))

# scraper tunables
MAX_PAGES        = int(os.getenv("MAX_PAGES", "200"))
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "1.0"))
SCRAPING_MODE    = os.getenv("SCRAPING_MODE", "").strip()  # optional override
PORTALS_CONFIG   = os.getenv("PORTALS_CONFIG", str(ROOT / "config" / "portals.json"))

# logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

def load_portals_config() -> Dict[str, Any]:
    """Read the portals.json (or custom path from env)."""
    p = Path(PORTALS_CONFIG)
    if not p.exists():
        raise FileNotFoundError(f"PORTALS_CONFIG not found at {p}")
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def validate_prod() -> None:
    if ENV == "prod":
        missing = [k for k, v in {
            "SUPABASE_URL": SUPABASE_URL,
            "SUPABASE_SERVICE_ROLE_KEY": SUPABASE_KEY,
        }.items() if not v]
        if missing:
            raise RuntimeError(f"Missing required env vars in prod: {missing}")
