import os, re, json, time, random, hashlib, logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, asdict

import requests, jsonlines
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

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
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                              " AppleWebKit/537.36 (KHTML, like Gecko)"
                              " Chrome/115.0 Safari/537.36"
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
    def _iter_discovered_urls(self, portal_name: str):
        import glob, jsonlines, os

    # 1) Current run
        curr = self.dirs["staged"] / f"{portal_name}_urls.jsonl"
        if curr.exists():
            with jsonlines.open(str(curr)) as r:
             for rec in r:
                yield rec
        return

    # 2) Any previous run(s)
        pattern = os.path.join("scraper_output", "run_*", "staged", f"{portal_name}_urls.jsonl")
        for path in sorted(glob.glob(pattern)):
            with jsonlines.open(path) as r:
                for rec in r:
                    yield rec

    def __init__(self, config_path: str):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.base_dir = Path("scraper_output") / f"run_{self.run_id}"
        self.dirs = {
            "raw_html": self.base_dir / "raw_html",
            "staged": self.base_dir / "staged",
            "logs": self.base_dir / "logs"
        }
        for d in self.dirs.values():
            d.mkdir(parents=True, exist_ok=True)

        # logging
        log_file = self.dirs["logs"] / f"scraper_{self.run_id}.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
        )
        self.logger = logging.getLogger("scraper")

        # load configs
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        self.configs = [ScrapingConfig(**pc) for pc in cfg.get("portals", [])]

        # requests session with retries
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5,
                        status_forcelist=[429, 500, 502, 503, 504],
                        allowed_methods=["GET", "HEAD"])
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        # browser handles
        self._pw = None
        self._pw_browser = None
        self._selenium_driver = None

        self.seen_urls: Set[str] = set()

    # --- Fetchers ---------------------------------------------------
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

    def _ensure_selenium(self, cfg):
        if not SELENIUM_AVAILABLE:
            return None
        if self._selenium_driver is None:
            options = ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            self._selenium_driver = webdriver.Chrome(options=options)
            self._selenium_wait = WebDriverWait(self._selenium_driver, cfg.timeout)
        return self._selenium_driver

    def _fetch_with_selenium(self, url: str, cfg: ScrapingConfig) -> Optional[str]:
        driver = self._ensure_selenium(cfg)
        try:
            driver.get(url)
            self._selenium_wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            if cfg.wait_for_selector:
                self._selenium_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, cfg.wait_for_selector)))
            return driver.page_source
        except Exception as e:
            self.logger.warning(f"Selenium error {url}: {e}")
            return None

    def _ensure_playwright(self, cfg):
        if not PLAYWRIGHT_AVAILABLE:
            return None
        if self._pw is None:
            self._pw = sync_playwright().start()
            self._pw_browser = self._pw.chromium.launch(headless=True)
            self._pw_context = self._pw_browser.new_context(user_agent=cfg.headers.get("User-Agent"))
        return self._pw_context

    def _fetch_with_playwright(self, url: str, cfg: ScrapingConfig) -> Optional[str]:
        ctx = self._ensure_playwright(cfg)
        page = ctx.new_page()
        page.set_default_navigation_timeout(cfg.timeout * 1000)

        nav_attempts = 3
        for i in range(1, nav_attempts + 1):
            try:
                page.goto(url, wait_until="domcontentloaded")
                break
            except Exception as e:
                if "ERR_NETWORK_CHANGED" in str(e) and i < nav_attempts:
                    self.logger.warning(f"goto retry {i}/{nav_attempts} for {url} due to {e}")
                    page.wait_for_timeout(500 * i)
                    continue
                raise

        try:
        # Cookie/consent best-effort
            try:
                page.locator(
                    "button:has-text('Accept'), button:has-text('I agree'), "
                    "#onetrust-accept-btn-handler, "
                    "button[aria-label*='accept' i]"
                ).first.click(timeout=3000)
            except Exception:
                pass

        # Decide wait targets by URL type
            is_detail = "/property/" in url
            candidates = []
            if is_detail:
                detail_wait = cfg.detail_selectors.get("_detail_wait_for_selector")
                candidates.extend([
                    detail_wait,
                    ".left-details .main-title h1",
                    ".prices-and-fees__price",
                    "#view-map__text",
                    "h1"
                ])
            else:
                candidates.extend([
                    cfg.wait_for_selector,
                    cfg.detail_selectors.get("_wait_for_selector"),
                    "a[href*='/property/']",
                    ".ListingCell",
                    "main"
                ])

        # Wait for first selector that attaches in DOM
            for sel in [c for c in candidates if c]:
                try:
                    page.wait_for_selector(sel, timeout=cfg.timeout * 1000, state="attached")
                    break
                except Exception:
                    continue

        # Let XHR/lazy load settle a bit
            try:
                page.wait_for_load_state("networkidle", timeout=4000)
            except Exception:
                pass

        # Nudge lazy content
            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(800)
            except Exception:
                pass

            html = page.content()
            return html

        except Exception as e:
            self.logger.warning(f"Playwright error {url}: {e}")
            try:
                page.screenshot(path=str(self.dirs["logs"] / "debug_last.png"))
                with open(self.dirs["logs"] / "debug_last.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
            except Exception:
                pass
            return None
        finally:
            page.close()



    def _get_page_content(self, url: str, cfg: ScrapingConfig) -> Optional[str]:
        if cfg.scraping_mode == "requests":
            return self._fetch_with_requests(url, cfg)
        elif cfg.scraping_mode == "selenium":
            return self._fetch_with_selenium(url, cfg)
        elif cfg.scraping_mode == "playwright":
            return self._fetch_with_playwright(url, cfg)
        else:
            self.logger.error(f"Unknown scraping_mode {cfg.scraping_mode}")
            return None

    # --- Helpers ---------------------------------------------------
    def _canonicalize_url(self, url: str) -> str:
        pu = urlparse(url)
        return f"{pu.scheme}://{pu.netloc}{pu.path}".rstrip("/")

    def _html_cache_path(self, url: str, portal: str):
        uhash = hashlib.sha1(self._canonicalize_url(url).encode()).hexdigest()
        return self.dirs["raw_html"] / f"{portal}_{uhash}.html"

    def _fetch_or_cache(self, url: str, portal: str, cfg: ScrapingConfig):
        p = self._html_cache_path(url, portal)
        if p.exists():
            return p.read_text(encoding="utf-8")
        html = self._get_page_content(url, cfg)
        if html:
            p.write_text(html, encoding="utf-8")
        return html

    # --- Discovery -------------------------------------------------
    # in src/scrapers/property_scraper.py
def url_discovery_routine(self, cfg: ScrapingConfig) -> List[str]:
    self.logger.info(f"Discovery start {cfg.portal_name}")
    urls_out = self.dirs["staged"] / f"{cfg.portal_name}_urls.jsonl"
    seen_file = self.dirs["staged"] / f"{cfg.portal_name}_seen.txt"
    if seen_file.exists():
        self.seen_urls |= set(line.strip() for line in open(seen_file, encoding="utf-8"))

    all_urls = []
    for seed in cfg.seed_urls:
        current = seed
        pages = 0

        while current and pages < cfg.max_pages:
            html = self._with_watchdog(
                lambda: self._fetch_or_cache(current, f"{cfg.portal_name}_list", cfg),
                timeout_s=70,
                mode=cfg.scraping_mode.lower()
            )
            if not html:
                self.logger.warning(f"No HTML for {current}; stopping pagination.")
                break

            soup = BeautifulSoup(html, "lxml")

            # 1) collect listing URLs on this page
            found = 0
            for a in soup.select(cfg.listing_selector):
                href = a.get("href")
                if not href:
                    continue
                full = self._canonicalize_url(urljoin(current, href))
                if full not in self.seen_urls:
                    with jsonlines.open(urls_out, "a") as w:
                        w.write({"url": full, "discovered_at": datetime.now().isoformat()})
                    self.seen_urls.add(full)
                    all_urls.append(full)
                    found += 1

            # 2) find the "Next" page
            nxt_url = None
            if cfg.pagination_selector:
                nxt = soup.select_one(cfg.pagination_selector)
                if nxt and nxt.get("href"):
                    nxt_url = urljoin(current, nxt.get("href"))

            self.logger.info(f"Page {pages+1}: {found} listings | next={bool(nxt_url)}")
            current = nxt_url
            pages += 1
            time.sleep(cfg.rate_limit_delay + random.uniform(0, 0.8))

    with open(seen_file, "w", encoding="utf-8") as f:
        for u in sorted(self.seen_urls):
            f.write(u + "\n")

    self.logger.info(f"Discovery done {cfg.portal_name}: {len(all_urls)} new URLs")
    return all_urls



    

    # --- Details ---------------------------------------------------
    def _parse_listing(self, html: str, url: str, cfg: ScrapingConfig) -> ListingData:
        soup = BeautifulSoup(html, "lxml")
        listing = ListingData(url=url, scraped_at=datetime.now().isoformat())

    # 1) Extract using selectors
        for field, sel in cfg.detail_selectors.items():
            if field.startswith("_"):
                continue
            el = soup.select_one(sel)
            if not el:
                continue
            text = el.get_text(strip=True)

            if field == "area":
                listing.area = self._normalize_area(text)
            elif field == "price":
                listing.price = self._normalize_price(text)
            elif field in ["bedrooms", "bathrooms"]:
                listing.__dict__[field] = self._extract_number(text)
            else:
                listing.__dict__[field] = text

    # 2) Fallbacks for area if still missing
        if (not listing.area) or (listing.area.get("sqm") in (None, 0)):
            area_guess = self._extract_area_from_soup(soup)
            if area_guess:
                listing.area = area_guess

    # 3) Last-resort regex from description
        if (not listing.area or not listing.area.get("sqm")) and listing.description:
            m = re.search(r"(\d+\.?\d*)\s*sqm", listing.description, re.IGNORECASE)
            if m:
                sqm_val = float(m.group(1))
                listing.area = {"raw": f"{sqm_val} sqm", "sqm": sqm_val}

        return listing


    def detail_extraction_stage(self, listing_urls, cfg):
        self.logger.info(f"Starting detail extraction for {cfg.portal_name} with {len(listing_urls)} URLs")
        if not listing_urls:
            self.logger.warning("No URLs to process; did discovery run?")
            return 0

        processed_file = self.dirs["staged"] / f"{cfg.portal_name}_processed.txt"
        processed = set()
        if processed_file.exists():
            processed |= set(line.strip() for line in open(processed_file, encoding="utf-8"))

        out_file = self.dirs["staged"] / f"{cfg.portal_name}_listings.jsonl"
        failed_file = self.dirs["staged"] / f"{cfg.portal_name}_failed.txt"

        success = fail = 0
        mode = cfg.scraping_mode.lower()

        with jsonlines.open(out_file, "a") as writer, open(processed_file, "a", encoding="utf-8") as done:
            for idx, u in enumerate(listing_urls, 1):
                if u in processed:
                    continue

            # --- per-URL retry loop ---
                max_attempts = 3
                attempt = 0
                html = None

                while attempt < max_attempts and html is None:
                    attempt += 1
                    self.logger.info(f"[{idx}/{len(listing_urls)}] Fetch {u} (attempt {attempt}/{max_attempts})")
                    html = self._with_watchdog(
                        lambda: self._fetch_or_cache(u, f"{cfg.portal_name}_detail", cfg),
                        timeout_s=70,
                        mode=mode
                    )
                    if html is None:
                    # brief backoff for transient hiccups like ERR_NETWORK_CHANGED
                        time.sleep(1.0 * attempt)

                if not html:
                    fail += 1
                    with open(failed_file, "a", encoding="utf-8") as ff:
                        ff.write(u + "\n")
                    continue
            # --- end per-URL retry loop ---

                from dataclasses import is_dataclass, asdict

                try:
                    listing = self._parse_listing(html, u, cfg)
                except Exception as e:
                    self.logger.warning(f"Parse error for {u}: {e}")
                    fail += 1
                    with open(failed_file, "a", encoding="utf-8") as ff:
                        ff.write(u + "\n")
                    continue

                if not is_dataclass(listing):
                    self.logger.warning(
                        f"_parse_listing did not return a dataclass for {u} "
                        f"(got {type(listing).__name__}); skipping."
                    )
                    fail += 1
                    with open(failed_file, "a", encoding="utf-8") as ff:
                        ff.write(u + "\n")
                    continue

                writer.write(asdict(listing))
                done.write(u + "\n")
                success += 1


            # politeness / anti-fingerprint jitter
                time.sleep(cfg.rate_limit_delay + random.uniform(0, 0.8))

        self.logger.info(f"Details complete {cfg.portal_name} — success:{success} fail:{fail}")
        return success


    # --- Normalization --------------------------------------------
    def _normalize_price(self, txt: str) -> dict:
    
        raw = (txt or "").strip()
        lower = raw.lower()

    # currency detection
        currency = None
        if "₱" in raw or "php" in lower or "ph₱" in lower:
            currency = "PHP"
        elif "usd" in lower or "$" in raw:
            currency = "USD"
        elif "eur" in lower or "€" in raw:
            currency = "EUR"

    # amount extraction: keep digits and dot, drop commas/spaces
        digits = re.sub(r"[^\d.]", "", raw.replace(",", ""))
        value = None
        if digits:
            try:
                value = float(digits)
            except ValueError:
                value = None

    # period detection
        period = None
        if any(k in lower for k in ["per month", "/ month", "/month", "monthly", "/ mo", "/mo"]):
            period = "month"
        elif any(k in lower for k in ["per year", "/ year", "/year", "yearly", "annum", "p.a"]):
            period = "year"
        elif any(k in lower for k in ["per week", "/ week", "/week", "weekly"]):
            period = "week"
        elif any(k in lower for k in ["per day", "/ day", "/day", "daily"]):
            period = "day"

        return {"raw": raw, "currency": currency, "value": value, "period": period}


    def _normalize_area(self, txt: str) -> dict:
        """
        Normalize to sqm where possible.
        Handles: '184 sqm', '184 m²', '184 m2', '184 square meters', '1,000 sq ft'
        """
        raw = (txt or "").strip()
        if not raw:
            return {"raw": raw, "sqm": None}

        lower = raw.lower()

    # Prefer explicit sqm/m² first
        m = re.search(r"(\d+(?:[\.,]\d+)?)\s*(m²|m2|sqm|sq\.?\s*m(?:eters?)?)", lower, flags=re.I)
        if m:
            val = float(m.group(1).replace(",", ""))
            return {"raw": raw, "sqm": val}

    # sq ft → sqm
        ft = re.search(r"(\d+(?:[\.,]\d+)?)\s*(sq\.?\s*ft|ft²|ft2|square\s*feet)", lower, flags=re.I)
        if ft:
            ft_val = float(ft.group(1).replace(",", ""))
            sqm = round(ft_val * 0.092903, 2)
            return {"raw": raw, "sqm": sqm}

    # Fallback: first number, assume sqm (conservative)
        n = re.search(r"(\d+(?:[\.,]\d+)?)", lower)
        if n:
            try:
                val = float(n.group(1).replace(",", ""))
                return {"raw": raw, "sqm": val}
            except Exception:
                pass

        return {"raw": raw, "sqm": None}


    def _extract_number(self, txt: str) -> Optional[int]:
        m = re.search(r"\d+", txt)
        return int(m.group()) if m else None

    # --- Cleanup ---------------------------------------------------
    def __del__(self):
        try:
            if self._selenium_driver: self._selenium_driver.quit()
            if self._pw_browser: self._pw_browser.close()
            if self._pw: self._pw.stop()
        except: pass

    def _with_watchdog(self, fn, timeout_s=60, mode: str = "requests"):
        if mode == "playwright":
        # rely on Playwright's own timeouts set in _fetch_with_playwright
            try:
                return fn()
            except Exception:
                return None
        
        import threading
        result = {"val": None}; err = {"exc": None}
        def runner():
            try: result["val"] = fn()
            except Exception as e: err["exc"] = e
        t = threading.Thread(target=runner, daemon=True)
        t.start(); t.join(timeout_s)
        if t.is_alive():
            self.logger.warning("Watchdog timeout; skipping URL")
            return None
        if err["exc"]:
            self.logger.warning(f"Worker error: {err['exc']}")
            return None
        return result["val"]
    def _extract_area_from_soup(self, soup: BeautifulSoup) -> Optional[dict]:
        """
        Try multiple strategies to get area:
        - explicit Lamudi attributes (data-test="area-value", "floor-area-value")
        - generic area labels in spec blocks
        - JSON-LD (floorSize.value)
        - free-text fallback regex anywhere in left-details / description
        Returns dict: {"raw": <str>, "sqm": <float>} or None
        """
        # A) Strong, site-specific selectors
        candidates = [
            "[data-test='area-value']",
            "[data-test='floor-area-value']",
            ".details-item-value[data-test*='area']",
            ".place-features__values[data-test*='area']",
            ".place-details .details-item-value",           # sometimes area appears here
            ".left-details .place-features .floor-area .place-features__values",
        ]
        for sel in candidates:
            el = soup.select_one(sel)
            if el:
                norm = self._normalize_area(el.get_text(" ", strip=True))
                if norm and norm.get("sqm"):
                    return norm

    # B) Label:value pattern inside features cards
    #   e.g., "Usable area: 184 sqm" / "Floor area: 100 m²"
        for block in soup.select(".place-features .spec, .place-details .details-item"):
            text = block.get_text(" ", strip=True)
            if not text:
                continue
        # quick filter
            if "area" in text.lower():
                norm = self._normalize_area(text)
                if norm and norm.get("sqm"):
                    return norm

    # C) JSON-LD <script type="application/ld+json">
    #    Look for floorSize.value or area, depending on publisher
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                import json as _json
                data = _json.loads(script.string or "")
                # sometimes it's a list
                objs = data if isinstance(data, list) else [data]
                for obj in objs:
                    floor = obj.get("floorSize") or obj.get("area")
                    if isinstance(floor, dict):
                        val = floor.get("value")
                        unit = (floor.get("unitCode") or floor.get("unitText") or "").lower()
                        if isinstance(val, (int, float)):
                            # assume m2 if unspecified
                            sqm = float(val)
                            # basic ft2 to m2 conversion if indicated
                            if "ft" in unit or "sq ft" in unit or unit == "ftk":
                                sqm = sqm * 0.092903
                            return {"raw": f"{val} {unit}".strip(), "sqm": round(sqm, 2)}
            except Exception:
                pass

    # D) Free-text fallback in the main content
        host = soup.select_one(".left-details") or soup.select_one("main") or soup
        txt = host.get_text(" ", strip=True) if host else soup.get_text(" ", strip=True)
        m = re.search(r"(\d+(?:[\.,]\d+)?)\s*(m²|m2|sqm|sq\.?\s*m(?:eters)?)", txt, flags=re.I)
        if m:
            num = float(m.group(1).replace(",", ""))
            return {"raw": f"{num} {m.group(2)}", "sqm": num}

    # E) As a last resort, look for square feet and convert
        m2 = re.search(r"(\d+(?:[\.,]\d+)?)\s*(sq\.?\s*ft|ft²|ft2)", txt, flags=re.I)
        if m2:
            num = float(m2.group(1).replace(",", ""))
            sqm = round(num * 0.092903, 2)
            return {"raw": f"{num} sq ft", "sqm": sqm}

        return None



