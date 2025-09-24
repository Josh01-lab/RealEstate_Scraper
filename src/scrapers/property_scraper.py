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
from src.utils.jsonld import _jsonld_iter, extract_jsonld_blocks, find_first
import jsonlines
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout       





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
    property_type: Optional[str] = None
    published_at: Optional[str] = None
    published_at_text: Optional[str] = None
    


class PropertyScraper:
    def __init__(self, config_path: str):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.base_dir = Path("scraper_output") / f"run_{self.run_id}"
        self.dirs = {
            "raw_html": self.base_dir / "raw_html",
            "staged": self.base_dir / "staged",
            "logs": self.base_dir / "logs",
        }
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

        # ---------------- load portal configs FIRST ----------------
        with open(config_path, "r", encoding="utf-8") as f:
            cfg_json = json.load(f)

        portals = cfg_json.get("portals", cfg_json)
        if isinstance(portals, dict):
            portals = [portals]
        if not isinstance(portals, list):
            raise ValueError("Invalid portals.json: expected list or {'portals': [...]}")

        self.configs = [ScrapingConfig(**pc) for pc in cfg_json.get("portals", [])]

        # --------------- env helpers (fallbacks if module missing) ---------------
        try:
            from src.config import (
                get_env_scrape_mode,
                get_env_rate_limit_delay,
                get_env_max_listings,
            )
        except ModuleNotFoundError:
            
            def get_env_scrape_mode(default="requests") -> str:
                val = os.getenv("SCRAPING_MODE")
                return (val or default).lower()
            
            def get_env_rate_limit_delay(default=1.0) -> float:
                val = os.getenv("RATE_LIMIT_DELAY")
                if val is None:
                    return float(default)
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return float(default)
            
            def get_env_max_listings(default=0) -> int:
                val = os.getenv("MAX_LISTINGS")
                if val is None:
                    return int(default)
                try:
                    return int(val)
                except (TypeError, ValueError):
                    return int(default)


        # --------------- apply env overrides to each config ----------------
        env_mode  = get_env_scrape_mode()
        env_delay = get_env_rate_limit_delay()
        env_max = get_env_max_listings()
        
        if env_mode:
            for c in self.configs:
                c.scraping_mode = env_mode
        
        if env_delay:
            for c in self.configs:
                c.rate_limit_delay = env_delay
                
        self.max_listings = env_max       

        self.logger.info(
            "Loaded %d portal configs: %s",
            len(self.configs), [c.portal_name for c in self.configs]
        )

        # ---- (rest of your init: requests session, playwright/selenium, etc.) ----
        


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
        """
        Paginate through ALL result pages, collect all listing URLs on each page,
        de-duplicate, and write to staged/<portal>_urls.jsonl.
        - Respects cfg.max_pages (0 or None means 'no cap', stop only when no next).
        - Uses cfg.listing_selector for per-listing anchors.
        - Uses cfg.pagination_selector to find the "next page" link (e.g., a[rel='next']).
        """
        import random, time, jsonlines
        from urllib.parse import urljoin


        self.logger.info(f"Discovery start {cfg.portal_name}")


        urls_out = self.dirs["staged"] / f"{cfg.portal_name}_urls.jsonl"
        # (re)create the file so re-runs don’t append duplicates from previous runs
        if urls_out.exists():
            urls_out.unlink()


        # hard caps via env (optional)
        try:
            from src.config import get_env_max_listings
            MAX_LISTINGS = get_env_max_listings(0)
        except Exception:
            import os
            try:
                MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "0"))
            except Exception:
                MAX_LISTINGS = 0


        # “0” or None for unlimited pages until no next
        max_pages = cfg.max_pages if cfg.max_pages is not None else 0
        if isinstance(max_pages, bool):
            max_pages = 0


        all_urls: List[str] = []
        pages = 0
        current = cfg.seed_urls[0]
        seen_local = set()


        with jsonlines.open(urls_out, "w") as w:
            while current and (max_pages == 0 or pages < max_pages):
                html = self._get_page_content(current, cfg)
                if not html:
                    self.logger.warning(f"No HTML for {current}; stopping pagination.")
                    break


                soup = BeautifulSoup(html, "lxml")


                # --- collect listing URLs on this page
                found_this_page = 0
                for a in soup.select(cfg.listing_selector):
                    href = a.get("href")
                    if not href:
                        continue
                    full = self._canonicalize_url(urljoin(current, href))
                    if full in seen_local:
                        continue
                    seen_local.add(full)
                    all_urls.append(full)
                    w.write({
                        "url": full,
                        "discovered_at": datetime.now(timezone.utc).isoformat()
                    })
                    found_this_page += 1


                    # optional global hard cap by count (mostly for tests)
                    if MAX_LISTINGS and len(all_urls) >= MAX_LISTINGS:
                        self.logger.info(f"Hit MAX_LISTINGS={MAX_LISTINGS}; stopping.")
                        current = None
                        break


                # --- next page (via pagination link)
                next_url = None
                if cfg.pagination_selector:
                    nxt = soup.select_one(cfg.pagination_selector)
                    if nxt and nxt.get("href"):
                        next_url = urljoin(current, nxt.get("href"))


                pages += 1
                self.logger.info(
                    f"Page {pages}: {found_this_page} listings | total={len(all_urls)} | next={bool(next_url)}"
                )


                # stop if no more pages
                if not next_url:
                    break


                # rate limit
                time.sleep(cfg.rate_limit_delay + random.uniform(0, 0.5))
                current = next_url


        self.logger.info(f"Discovery done {cfg.portal_name}: {len(all_urls)} urls")
        return all_urls

    def _ensure_playwright(self, cfg):
        if self._pw:
            return self._pw_browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/121.0.0.0 Safari/537.36"),
                locale="en-PH",
                timezone_id="Asia/Manila",
                viewport={"width": 1366, "height": 864},
                device_scale_factor=1.0,
                extra_http_headers={
                    "Accept-Language": "en-PH,en;q=0.9",
                    "DNT": "1",
                    "Upgrade-Insecure-Requests": "1",
                },
            )

        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._pw_browser = self._pw.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ])
        return self._pw_browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/121.0.0.0 Safari/537.36"),
            locale="en-PH",
            timezone_id="Asia/Manila",
            viewport={"width": 1366, "height": 864},
            device_scale_factor=1.0,
            extra_http_headers={
                "Accept-Language": "en-PH,en;q=0.9",
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
            },
        )
   

    def _fetch_with_playwright(self, url, cfg):
        """Return fully-rendered HTML via Playwright with sensible defaults for Lamudi."""
        # Start Playwright & browser once
        if not self._pw:
            self._pw = sync_playwright().start()
        if not self._pw_browser:
            # A couple args help with anti-automation heuristics
            self._pw_browser = self._pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

        # Retry a couple of times for flaky loads
        attempts = 0
        last_err = None
        while attempts < 3:
            attempts += 1
            ctx = None
            page = None
            try:
                ctx = self._pw_browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    locale="en-PH",
                    timezone_id="Asia/Manila",
                    viewport={"width": 1366, "height": 850},
                    java_script_enabled=True,
                    bypass_csp=True,
                    # Block big assets to speed up
                    # (keeping CSS and fonts; images/video blocked)
                )

                # Block heavy assets
                def _route(route):
                    req = route.request
                    if req.resource_type in ("image", "media"):
                        return route.abort()
                    return route.continue_()
                ctx.route("*/", _route)

                page = ctx.new_page()
                page.set_default_timeout(max(20000, int(cfg.timeout * 1000)))

                # Go to page
                page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # Try to dismiss cookie/consent banners (best-effort)
                for sel in [
                    "button:has-text('Accept')",
                    "button:has-text('I Accept')",
                    "button#onetrust-accept-btn-handler",
                    "button[aria-label='Accept all']",
                ]:
                    try:
                        btn = page.locator(sel)
                        if btn.count() > 0:
                            btn.first.click(timeout=1500)
                            break
                    except Exception:
                        pass

                # Proof-of-content waits (any one is enough)
                # Use a short chain of candidates to be resilient
                proof_selectors = [
                    "h1[data-testid='ad-title']",
                    "[data-testid='description']",
                    "[data-testid='publish-date']",
                    ".ListingDetail__Title",
                    "main [class*='Listing']",   # very loose fallback
                ]
                got_content = False
                for sel in proof_selectors:
                    try:
                        page.wait_for_selector(sel, timeout=10000)
                        got_content = True
                        break
                    except PwTimeout:
                        continue

                # Gentle scroll to trigger lazy content
                try:
                    page.evaluate("""
                        (async () => {
                        for (let y=0; y<=1500; y+=300) {
                            window.scrollTo(0, y);
                            await new Promise(r => setTimeout(r, 200));
                        }
                        })();
                    """)
                except Exception:
                    pass

                # If we didn’t see proof of content, try one last wait on <time> or .meta
                if not got_content:
                    try:
                        page.wait_for_selector("time, .meta, .posted-date", timeout=6000)
                    except PwTimeout:
                        pass

                html = page.content()
                return html

            except Exception as e:
                last_err = e
                # brief backoff
                import time as _t
                _t.sleep(1.5 * attempts)
            finally:
                # Always cleanup
                try:
                    if page:
                        page.close()
                except Exception:
                    pass
                try:
                    if ctx:
                        ctx.close()
                except Exception:
                    pass

        # If we get here, all attempts failed
        self.logger.warning(f"Playwright error {url}: {last_err}")
        return None


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
        
    def _parse_relative_published(self, text: str) -> Optional[str]:
        """Turn '2 months ago' etc. into ISO8601 (UTC)."""
        if not text:
            return None
        m = REL_RE.search(text)
        if not m:
            return None
        parts = {k: int(v) for k, v in m.groupdict().items() if v}
        if not parts:
            return None

        # crude month/year to days approximation
        days = (
            parts.get("days", 0)
            + parts.get("weeks", 0) * 7
            + parts.get("months", 0) * 30
            + parts.get("years", 0) * 365
        )
        td = timedelta(
            days=days,
            hours=parts.get("hours", 0),
            minutes=parts.get("minutes", 0),
        )
        published = datetime.now(timezone.utc) - td
        return published.isoformat()

    def _probe_selectors(self, soup, selectors: dict) -> dict:
        """Debug which selectors matched (for logging)."""
        hits = {}
        for name, sel in selectors.items():
            try:
                hits[name] = bool(soup.select_one(sel))
            except Exception:
                hits[name] = False
        return hits    
        

    # --- Listing detail parse -------------------------------------
    def _parse_listing(self, html: str, url: str, cfg) -> Optional[dict]:
        """Parse a Lamudi listing page into a dict. DOM first, then JSON-LD fallback."""
        soup = BeautifulSoup(html or "", "lxml")

        # ---------- tiny local helpers ----------
        def _norm_txt(x):
            return (x or "").replace("\u00a0", " ").strip()

        def _first_text(soup, selectors):
            for sel in selectors:
                el = soup.select_one(sel)
                if not el:
                    continue
                v = el.get("content") if el.name == "meta" else el.get_text(" ", strip=True)
                v = _norm_txt(v)
                if v:
                    return v
            return None

        def _first_attr_or_text(soup, selectors, attr="datetime"):
            for sel in selectors:
                el = soup.select_one(sel)
                if not el:
                    continue
                v = el.get(attr) or el.get_text(" ", strip=True)
                v = _norm_txt(v)
                if v:
                    return v
            return None

        def _parse_rel_to_iso(text):
            # works with "X days/weeks/months/years/hours/minutes ago"
            REL_RE = re.compile(
                r"(?:(?P<years>\d+)\s*year[s]?)?\s*,?\s*"
                r"(?:(?P<months>\d+)\s*month[s]?)?\s*,?\s*"
                r"(?:(?P<weeks>\d+)\s*week[s]?)?\s*,?\s*"
                r"(?:(?P<days>\d+)\s*day[s]?)?\s*,?\s*"
                r"(?:(?P<hours>\d+)\s*hour[s]?)?\s*,?\s*"
                r"(?:(?P<minutes>\d+)\s*minute[s]?)?\s*ago",
                flags=re.I,
            )
            m = REL_RE.search(text or "")
            if not m:
                return None
            parts = {k: int(v) for k, v in m.groupdict().items() if v}
            if not parts:
                return None
            days = (
                parts.get("days", 0)
                + parts.get("weeks", 0) * 7
                + parts.get("months", 0) * 30
                + parts.get("years", 0) * 365
            )
            ts = datetime.now(timezone.utc) - timedelta(
                days=days,
                hours=parts.get("hours", 0),
                minutes=parts.get("minutes", 0),
            )
            return ts.isoformat()

        def _jsonld_blocks(soup):
            blocks = []
            for s in soup.select("script[type='application/ld+json']"):
                raw = (s.get_text() or "").strip()
                if not raw:
                    continue
                try:
                    blocks.append(json.loads(raw))
                except Exception:
                    # salvage multiple json objects glued together
                    try:
                        parts = raw.replace("}\n{", "}\n\n{").split("\n\n")
                        for p in parts:
                            blocks.append(json.loads(p))
                    except Exception:
                        pass
            return blocks

        def _iter_nodes(obj):
            if isinstance(obj, dict):
                yield obj
                for v in obj.values():
                    yield from _iter_nodes(v)
            elif isinstance(obj, list):
                for it in obj:
                    yield from _iter_nodes(it)

        def _find_first(nodes, *types):
            for root in nodes:
                for node in _iter_nodes(root):
                    t = node.get("@type")
                    if not t:
                        continue
                    if isinstance(t, list):
                        if any(tt in t for tt in types):
                            return node
                    elif t in types:
                        return node
            return None

        # ---------- title ----------
        title = _first_text(soup, [
            "h1[data-testid='ad-title']",
            "h1.ListingDetail__Title, h1.listing-title, h1",
            "meta[property='og:title']",
        ])

        # ---------- price (DOM) ----------
        price_text = _first_text(soup, [
            "[data-testid='ad-price']",
            ".ListingDetail__Price, .price, .Price__Value",
            "meta[property='product:price:amount']",
        ])
        price = None
        if price_text:
            txt = _norm_txt(price_text).replace(",", "")
            m = re.search(r"(\d+(?:\.\d+)?)", txt)
            val = float(m.group(1)) if m else None
            per = "month" if "month" in txt.lower() else None
            cur = "PHP" if ("₱" in price_text or "php" in price_text.lower()) else None
            price = {"raw": price_text, "currency": cur, "value": val, "period": per}

        # ---------- area ----------
        area = None
        full_text = soup.get_text(" ", strip=True)
        m = re.search(r"(\d[\d,\.]*)\s*(sqm|m2|m²)", full_text, flags=re.I)
        if m:
            try:
                area = {"raw": m.group(0), "sqm": float(m.group(1).replace(",", ""))}
            except Exception:
                area = None

        # ---------- address ----------
        address = _first_text(soup, [
            "[data-testid='address'], .ListingDetail__Address, .address",
            "span[itemprop='address'], meta[property='og:street-address']",
            ".Breadcrumbs, nav[aria-label='breadcrumb']",
        ])

        # ---------- description ----------
        description = _first_text(soup, [
            "[data-testid='description'], .ListingDetail__Description, .description",
            "section[data-testid='description']",
        ])

        # ---------- published_at (DOM first) ----------
        published_text = _first_attr_or_text(soup, [
            "[data-testid='publish-date']",
            "time[datetime]",
            ".ListingDetail__Meta time",
            ".posted-date time, .posted_date time",
            ".meta time"
        ], attr="datetime")

        if not published_text:
            published_text = _first_text(soup, [
                "[data-testid='publish-date']",
                ".ListingDetail__Meta, .posted-date, .posted_date, .meta",
                ".date",   # <-- add this line
            ])


        published_at = None
        if published_text and not published_at:
            # try to extract the leading date token
            m = re.search(r"(\d{1,2}\s+[A-Za-z]{3,}\s+\d{4})", published_text)
            if m:
                try:
                    dt = dtparse.parse(m.group(1), dayfirst=True).astimezone(timezone.utc)
                    published_at = dt.isoformat()
                except Exception:
                    pass

        # ---------- JSON-LD fallback (dates, type, price if missing) ----------
        blocks = _jsonld_blocks(soup)

        # published_* via JSON-LD if still missing
        if not published_text:
            node = _find_first(blocks, "Offer", "Product", "NewsArticle", "Article", "CreativeWork") or {}
            for key in ("datePublished", "datePosted", "dateCreated", "uploadDate", "pubDate"):
                v = node.get(key)
                if v:
                    try:
                        published_at = dtparse.parse(v).astimezone(timezone.utc).isoformat()
                        published_text = v
                        break
                    except Exception:
                        pass

        # property_type
        property_type = None
        product = _find_first(blocks, "Product", "Offer", "RealEstateAgent") or {}
        property_type = product.get("category") or product.get("@type")
        if not property_type and re.search(r"\boffice|serviced office|commercial\b", full_text, re.I):
            property_type = "Offices"

        # price via JSON-LD if DOM missing
        if (not price) or (price and price.get("value") is None):
            offer = _find_first(blocks, "Offer") or {}
            jd_price = offer.get("price") or (offer.get("offers") or {}).get("price") if isinstance(offer.get("offers"), dict) else offer.get("price")
            jd_currency = offer.get("priceCurrency") or (offer.get("offers") or {}).get("priceCurrency") if isinstance(offer.get("offers"), dict) else offer.get("priceCurrency")
            if jd_price:
                try:
                    val = float(str(jd_price).replace(",", ""))
                except Exception:
                    val = None
                price = {
                    "raw": (price or {}).get("raw") or str(jd_price),
                    "currency": (price or {}).get("currency") or jd_currency,
                    "value": val,
                    "period": (price or {}).get("period"),
                }

        # ---------- final payload ----------
        return {
            "url": url,
            "title": title,
            "address": address,
            "property_type": property_type,
            "description": description,
            "price": price,
            "area": area,
            "published_at_text": published_text,
            "published_at": published_at,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }



    # --- Details runner --------------------------------------------
    def detail_extraction_stage(self, urls: list[str], cfg) -> int:
        """Fetch each URL, parse details, write staged listings.jsonl, and log failures."""

        out_file = self.dirs["staged"] / f"{cfg.portal_name}_listings.jsonl"
        fail_file = self.dirs["staged"] / f"{cfg.portal_name}_failures.jsonl"

        ok = 0
        fails = []

        self.logger.info(f"Starting detail extraction for {cfg.portal_name} with {len(urls)} URLs")

        with jsonlines.open(out_file, "w") as w:
            for i, u in enumerate(urls, 1):
                self.logger.info(f"[{i}/{len(urls)}] detail -> {u}")
                try:
                    html = self._get_page_content(u, cfg)
                    if not html:
                        fails.append({"url": u, "reason": "no_html"})
                        self.logger.warning(f"No HTML fetched, skipping: {u}")
                        continue

                    listing = self._parse_listing(html, u, cfg)
                    if not listing:
                        fails.append({"url": u, "reason": "parse_returned_none"})
                        self.logger.warning(f"Parse returned None, skipping: {u}")
                        continue

                    # hard guards so we know WHY we skipped
                    if not listing.get("title"):
                        fails.append({"url": u, "reason": "missing_title"})
                        self.logger.warning(f"Missing title, skipping: {u}")
                        continue

                    # If you require address/price etc., add more required-key checks here:
                    # for req in ("address", "price"):
                    #     if not listing.get(req):
                    #         fails.append({"url": u, "reason": f"missing_{req}"})
                    #         self.logger.warning(f"Missing {req}, skipping: {u}")
                    #         break
                    # else:
                    #     w.write(listing); ok += 1; continue

                    w.write(listing)
                    ok += 1

                except Exception as e:
                    # include exception name + message for root-cause analysis
                    fails.append({
                        "url": u,
                        "reason": f"exception:{type(e).__name__}",
                        "detail": str(e)[:1000],
                    })
                    # log full traceback in console/log file
                    self.logger.warning(f"Parse error {u}: {e}", exc_info=True)

        # write failures to a separate JSONL for inspection
        if fails:
            with jsonlines.open(fail_file, "w") as fw:
                for row in fails:
                    row["logged_at"] = datetime.now(timezone.utc).isoformat()
                    fw.write(row)

        self.logger.info(f"Details done {cfg.portal_name}: {ok} rows (fail {len(fails)})")
        if fails:
            self.logger.info(f"Failure log written: {fail_file}")
        self.logger.info(f"✅ Details complete: {ok} listings")
        self.logger.info(f"Wrote: {out_file}")
        return ok


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
    
    def extract_jsonld_blocks(scripts: list) -> list:
        """Parse a list of JSON strings (script tags) into a flat list of dict nodes."""
        nodes = []
        for s in scripts:
            if not s:
                continue
            try:
                data = json.loads(s)
                if isinstance(data, dict):
                    nodes.append(data)
                elif isinstance(data, list):
                    nodes.extend([d for d in data if isinstance(d, dict)])
            except Exception:
                # ignore malformed JSON-LD
                continue
        return nodes

    def find_first(nodes: list, type_name: str) -> Optional[dict]:
        """Return first JSON-LD node with matching @type."""
        for node in nodes:
            t = node.get("@type")
            if t == type_name:
                return node
            # sometimes @type is a list
            if isinstance(t, list) and type_name in t:
                return node
        return None

    
    
    
    



















