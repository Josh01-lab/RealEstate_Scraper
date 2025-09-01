from _future_ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

# allow: python scripts/validate_portals.py from repo root
REPO_ROOT = Path(_file_).resolve().parents[1]
DEFAULT_CFG = REPO_ROOT / "config" / "portals.json"

ALLOWED_MODES = {"requests", "selenium", "playwright"}

URL_RE = re.compile(r"^https?://", re.I)

REQUIRED_KEYS = [
    "portal_name",
    "seed_urls",
    "scraping_mode",
    "listing_selector",
]

def _err(msg: str) -> None:
    print(f"❌ {msg}")

def _ok(msg: str) -> None:
    print(f"✅ {msg}")

def validate_portal(p: Dict[str, Any], idx: int) -> List[str]:
    errors: List[str] = []
    name = p.get("portal_name") or f"(index {idx})"

    # Required keys present
    for k in REQUIRED_KEYS:
        if k not in p:
            errors.append(f"[{name}] missing required key: {k}")

    # Types / values
    # portal_name
    if "portal_name" in p and not isinstance(p["portal_name"], str):
        errors.append(f"[{name}] portal_name must be string")

    # seed_urls
    if "seed_urls" in p:
        su = p["seed_urls"]
        if not isinstance(su, list) or not su:
            errors.append(f"[{name}] seed_urls must be a non-empty list")
        else:
            for u in su:
                if not isinstance(u, str) or not URL_RE.match(u.strip()):
                    errors.append(f"[{name}] invalid seed URL: {u!r}")

    # scraping_mode
    if "scraping_mode" in p:
        mode = str(p["scraping_mode"]).strip().lower()
        if mode not in ALLOWED_MODES:
            errors.append(
                f"[{name}] scraping_mode must be one of {sorted(ALLOWED_MODES)} (got {p['scraping_mode']!r})"
            )

    # selectors
    if "listing_selector" in p and not str(p["listing_selector"]).strip():
        errors.append(f"[{name}] listing_selector cannot be empty")

    # pagination_selector is optional but if present should be non-empty string
    if "pagination_selector" in p and p["pagination_selector"] is not None:
        if not isinstance(p["pagination_selector"], str) or not p["pagination_selector"].strip():
            errors.append(f"[{name}] pagination_selector provided but empty/invalid")

    # detail_selectors optional; if present must be dict of str->str
    if "detail_selectors" in p and p["detail_selectors"] is not None:
        ds = p["detail_selectors"]
        if not isinstance(ds, dict):
            errors.append(f"[{name}] detail_selectors must be an object/dict")
        else:
            for k, v in ds.items():
                # underscore keys are control hints; allow empty (we won't enforce)
                if k.startswith("_"):
                    continue
                if not isinstance(v, str) or not v.strip():
                    errors.append(f"[{name}] detail_selectors[{k}] must be a non-empty CSS selector")

    # optional numeric knobs
    for nk in ("max_pages", "rate_limit_delay", "timeout", "max_retries"):
        if nk in p and p[nk] is not None:
            if not isinstance(p[nk], (int, float)):
                errors.append(f"[{name}] {nk} must be numeric")

    return errors

def main(path: Path) -> int:
    if not path.exists():
        _err(f"config file not found: {path}")
        return 2

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        _err(f"failed to load JSON: {e}")
        return 2

    portals = data.get("portals")
    if not isinstance(portals, list) or not portals:
        _err("root key 'portals' must be a non-empty array")
        return 2

    print(f"🔎 Validating {len(portals)} portal config(s) in {path} …\n")

    all_errors: List[str] = []
    for i, p in enumerate(portals):
        name = p.get("portal_name", f"(index {i})")
        errs = validate_portal(p, i)
        if errs:
            print(f"— {name}: ❗ {len(errs)} issue(s)")
            for e in errs:
                print(f"   - {e}")
        else:
            print(
                f"— {name}: OK | mode={p['scraping_mode']} | seeds={len(p['seed_urls'])} "
                f"| listing_selector='{p['listing_selector']}'"
                + (f" | pagination='{p['pagination_selector']}'" if p.get("pagination_selector") else "")
            )
        all_errors.extend(errs)

    print()
    if all_errors:
        _err(f"Validation failed with {len(all_errors)} issue(s).")
        return 1

    _ok("All portal configs look good.")
    return 0

if _name_ == "_main_":
    cfg_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CFG
    sys.exit(main(cfg_path))
