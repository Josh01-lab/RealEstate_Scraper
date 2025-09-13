import os, re, json, time, random, hashlib, logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qsl, urlencode
from dataclasses import dataclass, asdict, is_dataclass
import re
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparse
import requests, jsonlines
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from src.config import ENV_SCRAPE_MODE, ENV_RATE_DELAY, MAX_LISTINGS, MAX_PAGES






REL_RE = re.compile(
    r"(?:(?P<years>\d+)\s*year[s]?)?\s*,?\s*"
    r"(?:(?P<months>\d+)\s*month[s]?)?\s*,?\s*"
    r"(?:(?P<weeks>\d+)\s*week[s]?)?\s*,?\s*"
    r"(?:(?P<days>\d+)\s*day[s]?)?\s*,?\s*"
    r"(?:(?P<hours>\d+)\s*hour[s]?)?\s*,?\s*"
    r"(?:(?P<minutes>\d+)\s*minute[s]?)?\s*ago",
    flags=re.I,
)



# Optional heavy tools
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


@dataclass
class ScrapingConfig:
    portal_name: str
    seed_urls: List[str]
    scraping_mode: str  # 'requests', 'selenium', 'playwright'
    listing_selector: str
    pagination_selector: Optional[str] = None
    detail_selectors: Dict[str, str] = None
    rate_limit_delay: float = 1.0
    max_retries: int = 3
    timeout: int = 30
    headers: Dict[str, str] = None
    max_pages: int = 200
    wait_for_selector: Optional[str] = None
    respect_robots: bool = False

    def __post_init__(self):
        if self.detail_selectors is None:
            self.detail_selectors = {}
        if self.headers is None:
            self.headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0 Safari/537.36"
                )
            }


@dataclass
class ListingData:
    url: str
    title: Optional[str] = None
    price: Optional[dict] = None
    area: Optional[dict] = None
    address: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    description: Optional[str] = None
    scraped_at: Optional[str] = None
    


class PropertyScraper:
    def __init__(self, config_path: str):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.base_dir = Path("scraper_output") / f"run_{self.run_id}"
        self.dirs = {
            "raw_html": self.base_dir / "raw_html",
            "staged": self.base_dir / "staged",
            "logs": self.base_dir / "logs",
        }
        for cfg in self.configs:
            # apply env overrides
            cfg.scraping_mode = get_env_scrape_mode(cfg.scraping_mode)
            cfg.rate_limit_delay = get_env_rate_limit_delay(cfg.rate_limit_delay)
        self.max_listings = get_env_max_listings()

        for d in self.dirs.values():
            d.mkdir(parents=True, exist_ok=True)

        # logging
        log_file = self.dirs["logs"] / f"scraper_{self.run_id}.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        self.logger = logging.getLogger("scraper")

        # load configs
        with open(config_path, "r", encoding="utf-8") as f:
            cfg_json = json.load(f)
        
        self.configs = [ScrapingConfig(**pc) for pc in cfg_json.get("portals", [])]
        
        # --- apply env overrides (if any)
        try:
            from src.utils.config import (
                get_env_scrape_mode,
                get_env_rate_limit_delay,
                get_env_max_listings,  # used later by discovery, not here
            )
        except ModuleNotFoundError:
            import os
            def get_env_scrape_mode(default="requests"): return os.getenv("SCRAPING_MODE", default).lower()
            def get_env_rate_limit_delay(default=1.0):
                try: return float(os.getenv("RATE_LIMIT_DELAY", default))
                except ValueError: return default
            def get_env_max_listings(default=0):
                try: return int(os.getenv("MAX_LISTINGS", str(default)))
                except ValueError: return default
        
        env_mode = get_env_scrape_mode(None)
        env_delay = get_env_rate_limit_delay(None)
        
        if env_mode:
            for c in self.configs:
                c.scraping_mode = env_mode
        
        if env_delay is not None:
            for c in self.configs:
                c.rate_limit_delay = env_delay
                
        self.logger.info("Loaded %d portal configs: %s",
                 len(self.configs),
                 [c.portal_name for c in self.configs])
        


        # requests session with retries
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        # browser handles
        self._pw = None
        self._pw_browser = None
        self._selenium_driver = None

        self.seen_urls: Set[str] = set()

    # --- Helpers ---------------------------------------------------
    def _canonicalize_url(self, url: str) -> str:
        pu = urlparse(url)
        return f"{pu.scheme}://{pu.netloc}{pu.path}".rstrip("/")

    def _fetch_with_requests(self, url: str, cfg: ScrapingConfig) -> Optional[str]:
        try:
            r = self.session.get(url, headers=cfg.headers, timeout=cfg.timeout)
            if r.status_code == 429 and "Retry-After" in r.headers:
                delay = int(r.headers["Retry-After"])
                self.logger.info(f"429 Retry-After {delay}s for {url}")
                time.sleep(delay)
                return None
            r.raise_for_status()
            return r.text
        except Exception as e:
            self.logger.warning(f"Requests error {url}: {e}")
            return None
        
    def url_discovery_routine(self, cfg: "ScrapingConfig") -> List[str]:
        """Discover listing URLs for a portal, save to staged/<portal>_urls.jsonl (append)."""
        import os
    
        self.logger.info(f"Discovery start {cfg.portal_name}")
        urls_out = self.dirs["staged"] / f"{cfg.portal_name}_urls.jsonl"
        seen_file = self.dirs["staged"] / f"{cfg.portal_name}_seen.txt"
    
        # Hard cap by count (0 = unlimited)
        MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "0"))
    
        # Load prior seen for cross-run dedupe (optional but handy)
        if seen_file.exists():
            try:
                self.seen_urls |= {line.strip() for line in seen_file.read_text(encoding="utf-8").splitlines() if line.strip()}
            except Exception as e:
                self.logger.warning(f"Could not read seen file: {seen_file} ({e})")
    
        all_urls: List[str] = []
        pages = 0
        current = cfg.seed_urls[0]
    
        # Optional: only accept links from the same hostname as the seed
        seed_host = urlparse(cfg.seed_urls[0]).netloc
    
        while current and pages < cfg.max_pages:
            html = self._get_page_content(current, cfg)
            if not html:
                self.logger.warning(f"No HTML for {current}; stopping pagination.")
                break
    
            soup = BeautifulSoup(html, "lxml")
    
            # Collect listing URLs on this page
            page_urls = set()
            for a in soup.select(cfg.listing_selector):
                href = (a.get("href") or "").strip()
                if not href or href.startswith(("javascript:", "mailto:")):
                    continue
                full = self._canonicalize_url(urljoin(current, href))
                if not full.startswith(("http://", "https://")):
                    continue
                # same-domain guard
                if urlparse(full).netloc != seed_host:
                    continue
                page_urls.add(full)
    
            # Write new ones
            new_count = 0
            for full in sorted(page_urls):
                if full in self.seen_urls:
                    continue
                if MAX_LISTINGS and len(all_urls) >= MAX_LISTINGS:
                    self.logger.info(f"Hit MAX_LISTINGS={MAX_LISTINGS}; stopping discovery.")
                    current = None  # stop outer loop
                    break
                with jsonlines.open(urls_out, "a") as w:
                    w.write({"url": full, "discovered_at": datetime.now(timezone.utc).isoformat()})
                self.seen_urls.add(full)
                all_urls.append(full)
                new_count += 1
    
            # Find "next" page
            nxt_url = None
            if current and cfg.pagination_selector:
                nxt = soup.select_one(cfg.pagination_selector)
                if nxt and nxt.get("href"):
                    nxt_url = urljoin(current, nxt.get("href"))
    
            pages += 1
            self.logger.info(f"Page {pages}: {new_count} listings | next={bool(nxt_url)}")
            current = nxt_url
            time.sleep(cfg.rate_limit_delay + random.uniform(0, 0.8))
    
            if MAX_LISTINGS and len(all_urls) >= MAX_LISTINGS:
                break
    
        # Persist seen set (so future runs don’t re-emit same URLs)
        try:
            with open(seen_file, "w", encoding="utf-8") as f:
                for u in sorted(self.seen_urls):
                    f.write(u + "\n")
        except Exception as e:
            self.logger.warning(f"Could not write seen file: {seen_file} ({e})")
    
        self.logger.info(f"Discovery done {cfg.portal_name}: {len(all_urls)} urls")
        return all_urls


    def _ensure_playwright(self, cfg: "ScrapingConfig"):
        if not PLAYWRIGHT_AVAILABLE:
            return None
        if getattr(self, "_pw_ctx", None):
            return self._pw_ctx

        self._pw = sync_playwright().start()
        self._pw_browser = self._pw.chromium.launch(headless=True)
        self._pw_ctx = self._pw_browser.new_context(user_agent=cfg.headers.get("User-Agent"))
        return self._pw_ctx

    def _fetch_with_playwright(self, url: str, cfg: "ScrapingConfig") -> Optional[str]:
        if not PLAYWRIGHT_AVAILABLE:
            return None
        ctx = self._ensure_playwright(cfg)
        page = ctx.new_page()
        page.set_default_navigation_timeout(cfg.timeout * 1000)
        try:
            page.goto(url, wait_until="domcontentloaded")
            # try to accept cookies
            try:
                page.locator(
                    "button:has-text('Accept'), button:has-text('I agree'), "
                    "#onetrust-accept-btn-handler, button[aria-label*='accept' i]"
                ).first.click(timeout=2500)
            except Exception:
                pass
            # wait for something meaningful
            wait_for = cfg.wait_for_selector
            if "/property/" in url:
                wait_for = (cfg.detail_selectors or {}).get("_detail_wait_for_selector") or wait_for
            if wait_for:
                page.wait_for_selector(wait_for, timeout=cfg.timeout * 1000, state="attached")
            try:
                page.wait_for_load_state("networkidle", timeout=3000)
            except Exception:
                pass
            return page.content()
        except Exception as e:
            self.logger.warning(f"Playwright error {url}: {e}")
            return None
        finally:
            try:
                page.close()
            except Exception:
                pass

    def _get_page_content(self, url: str, cfg: "ScrapingConfig") -> Optional[str]:
        mode = (cfg.scraping_mode or "requests").lower()
        if mode == "requests":
            return self._fetch_with_requests(url, cfg)
        if mode == "playwright":
            return self._fetch_with_playwright(url, cfg)
        if mode == "selenium":
            self.logger.warning("Selenium mode not implemented in this build.")
            return None
        self.logger.warning(f"Unknown scraping mode '{cfg.scraping_mode}', defaulting to requests().")
        return self._fetch_with_requests(url, cfg)

    # --- Normalizers & helpers ------------------------------------
    def _normalize_price(self, raw: Optional[str]) -> Optional[dict]:
        if not raw: return None
        txt = self._clean_text(raw)
        # accept "₱ 95,200 /month" or "Php 95,200 / month"
        m = re.search(r"(?:₱|Php)\s*([\d,]+)(?:\s*/\s*(month|mo|year|yr|day))?", txt, re.I)
        if not m: return {"raw": txt}
        value = float(m.group(1).replace(",", ""))
        period = m.group(2).lower() if m.group(2) else None
        if period in {"mo"}: period = "month"
        if period in {"yr"}: period = "year"
        return {"raw": txt, "currency": "PHP", "value": value, "period": period}

    def _normalize_area(self, raw: Optional[str]) -> Optional[dict]:
        if not raw: return None
        txt = self._clean_text(raw)
        m = re.search(r"([\d,.]+)\s*(sqm|m²|sq\.? m)", txt, re.I)
        if not m: return {"raw": txt}
        sqm = float(m.group(1).replace(",", ""))
        return {"raw": txt, "sqm": sqm}

    def _extract_number(self, txt: str) -> Optional[int]:
        m = re.search(r"\d+", txt or "")
        return int(m.group()) if m else None

    def _parse_published_at(self, text: Optional[str]) -> Optional[str]:
        # relative like '1 day, 6 hours ago' OR absolute like '12 Sep 2023'
        if not text:
            return None
        t = text.strip()

        m = REL_RE.search(t)
        if m:
            now = datetime.now(timezone.utc)
            parts = {k: int(v) for k, v in m.groupdict(default="0").items()}
            delta = timedelta(
                days=parts["days"] + parts["weeks"] * 7 + parts["months"] * 30 + parts["years"] * 365,
                hours=parts["hours"],
                minutes=parts["minutes"],
            )
            return (now - delta).isoformat()

        try:
            dt = dtparse.parse(t)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return None

    # --- Listing detail parse -------------------------------------
    def _parse_listing(self, html: str, url: str, cfg: ScrapingConfig) -> Dict[str, any]:
        soup = BeautifulSoup(html, "lxml")
        sel = cfg.detail_selectors or {}
    
        def tx(css: str) -> Optional[str]:
            if not css:
                return None
            el = soup.select_one(css)
            if not el:
                return None
            # prefer datetime attribute for <time> when present
            if el.name == "time" and el.get("datetime"):
                return el.get("datetime")
            return (el.get_text(" ", strip=True) or None)
    
        title = tx(sel.get("title"))
        address = tx(sel.get("address"))
        property_type = tx(sel.get("property_type"))
        description = tx(sel.get("description"))
    
        # price parsing (keep your current logic if you already have it)
        price_raw = tx(sel.get("price"))
        price_dict = None
        if price_raw:
            # super simple normalization; keep your richer version if you have one
            m = re.search(r"([\d,]+(?:\.\d+)?)", price_raw)
            val = float(m.group(1).replace(",", "")) if m else None
            cur = "PHP" if "₱" in price_raw or "PHP" in price_raw.upper() else None
            period = "month" if "month" in price_raw.lower() else None
            price_dict = {"raw": price_raw, "currency": cur, "value": val, "period": period}
    
        # area parsing
        area_raw = tx(sel.get("area"))
        area_dict = None
        if area_raw:
            m2 = re.search(r"([\d,]+(?:\.\d+)?)\s*sq?m", area_raw, re.I)
            sqm = float(m2.group(1).replace(",", "")) if m2 else None
            area_dict = {"raw": area_raw, "sqm": sqm}
    
        # --- published_at parsing ---
        published_at_iso: Optional[str] = None
    
        # 1) first: try a proper <time datetime="...">
        t_el = soup.select_one("time[itemprop='datePublished'], time[datetime]")
        if t_el and t_el.get("datetime"):
            try:
                dt = dtparse.parse(t_el["datetime"])
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                published_at_iso = self._dt_to_iso(dt)
            except Exception:
                published_at_iso = None
    
        # 2) otherwise: try your configured text container and parse relative phrase
        if not published_at_iso:
            rel_text = tx(sel.get("published_at_text"))
            # If it looks like a full date, parse directly; else try relative
            if rel_text:
                parsed = None
                try:
                    # Attempt absolute date first (e.g., "12 Sep 2023")
                    parsed_dt = dtparse.parse(rel_text, fuzzy=True, dayfirst=False)
                    if parsed_dt:
                        if not parsed_dt.tzinfo:
                            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                        parsed = self._dt_to_iso(parsed_dt)
                except Exception:
                    parsed = None
                if not parsed:
                    parsed = self._parse_relative_published(rel_text)
                published_at_iso = parsed
    
        return {
            "url": url,
            "title": title,
            "address": address,
            "property_type": property_type,
            "description": description,
            "price": price_dict,
            "area": area_dict,
            "published_at": published_at_iso,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }


    # --- Details runner --------------------------------------------
    def detail_extraction_stage(self, listing_urls: List[str], cfg: "ScrapingConfig") -> int:
        self.logger.info(f"Starting detail extraction for {cfg.portal_name} with {len(listing_urls)} URLs")
        if not listing_urls:
            self.logger.warning("No URLs to process; did discovery run?")
            return 0

        processed_file = self.dirs["staged"] / f"{cfg.portal_name}_processed.txt"
        processed = set()
        if processed_file.exists():
            processed |= {line.strip() for line in open(processed_file, encoding="utf-8")}

        out_file = self.dirs["staged"] / f"{cfg.portal_name}_listings.jsonl"
        failed_file = self.dirs["staged"] / f"{cfg.portal_name}_failed.txt"

        success = fail = 0
        with jsonlines.open(out_file, "a") as writer, open(processed_file, "a", encoding="utf-8") as done:
            for idx, u in enumerate(listing_urls, 1):
                if u in processed:
                    continue
                self.logger.info(f"[{idx}/{len(listing_urls)}] detail -> {u}")

                html = None
                for attempt in range(1, cfg.max_retries + 1):
                    html = self._get_page_content(u, cfg)
                    if html:
                        break
                    time.sleep(min(2 ** attempt, 8))

                if not html:
                    fail += 1
                    with open(failed_file, "a", encoding="utf-8") as ff:
                        ff.write(u + "\n")
                    continue

                try:
                    listing = self._parse_listing(html, u, cfg)
                    if not is_dataclass(listing):
                        raise ValueError("parse returned non-dataclass")
                    writer.write(asdict(listing))
                    done.write(u + "\n")
                    success += 1
                except Exception as e:
                    self.logger.warning(f"Parse error for {u}: {e}")
                    fail += 1
                    with open(failed_file, "a", encoding="utf-8") as ff:
                        ff.write(u + "\n")

                time.sleep(cfg.rate_limit_delay + random.uniform(0, 0.7))

        self.logger.info(f"Details done {cfg.portal_name}: {success} rows (fail {fail})")
        return success

    # --- Cleanup ---------------------------------------------------
    def __del__(self):
        try:
            if getattr(self, "_pw_browser", None):
                self._pw_browser.close()
            if getattr(self, "_pw", None):
                self._pw.stop()
        except Exception:
            pass
    def _page_url(self, seed: str, page: int) -> str:
        """Return seed with its page query param set to page (add or replace)."""
        parts = urlsplit(seed)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q["page"] = str(page)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))
    
    def _clean_text(self, s: Optional[str]) -> Optional[str]:
        if not s:
            return s
        s = s.strip()
        # common mojibake fix (₱ sign)
        s = s.replace("â‚±", "₱")
        # normalize whitespace
        s = re.sub(r"\s+", " ", s)
        return s

    def _dt_to_iso(self, dt: datetime) -> str:
        return dt.astimezone(timezone.utc).isoformat()
    
    def _parse_relative_published(self, text: str) -> Optional[str]:
        if not text:
            return None
        m = _REL_RE.search(text)
        if not m:
            return None
        parts = {k: int(v) for k, v in m.groupdict().items() if v}
        if not parts:
            return None
        # build a timedelta (years/months are approximated)
        days = parts.get("days", 0) + parts.get("weeks", 0) * 7 + parts.get("months", 0) * 30 + parts.get("years", 0) * 365
        td = timedelta(
            days=days,
            hours=parts.get("hours", 0),
            minutes=parts.get("minutes", 0),
        )
        published = datetime.now(timezone.utc) - td
        return self._dt_to_iso(published)

   
        













