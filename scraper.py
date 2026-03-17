#!/usr/bin/env python3
"""
Dental Products Price Benchmarking Scraper
===========================================

Strategy per site:
  dmi.ie        → LD+JSON from product URL; search via /categories.html?type=simple&name=QUERY
  dmi.co.uk     → same platform as dmi.ie
  dentalsky.com → LD+JSON from product URL; search via Playwright (Magento with Ajax search)
  dontalia.com  → LD+JSON from product URL; search via Playwright (JS platform)
  henryschein.ie → LD+JSON from product URL; search via Playwright (Angular SPA)

Usage:
  # Basic run (uses existing URLs, HTML search for DMI sites)
  python scraper.py --input "Price Benchmarking...csv" --output output.csv

  # With Playwright for JS-heavy sites (required for search on dentalsky/dontalia/henryschein)
  python scraper.py --input ... --output ... --playwright

  # Test first N rows only
  python scraper.py --input ... --output ... --limit 10

  # Only specific sites
  python scraper.py --input ... --output ... --sites dmi_ie dmi_uk
"""

import argparse
import csv
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

MIN_DELAY = 1.5
MAX_DELAY = 4.0


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------
@dataclass
class ScrapedPrice:
    price: Optional[str] = None
    url: Optional[str] = None
    found: bool = False
    product_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _extract_ld_json_price(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract price, currency, and product name from schema.org Product LD+JSON.
    Returns (price_str, currency_code, product_name) e.g. ("27.39", "EUR", "Septoject XL Box100")
    """
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            raw = sc.string or ""
            data = json.loads(raw)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") == "Product":
                    offers = item.get("offers", {})
                    if isinstance(offers, list):
                        offers = offers[0]
                    price = str(offers.get("price", "")).strip()
                    currency = str(offers.get("priceCurrency", "")).strip()
                    product_name = str(item.get("name", "")).strip() or None
                    if price and re.match(r"^\d+\.?\d*$", price):
                        return price, currency, product_name
        except Exception:
            pass
    return None, None, None


def _format_price(price_str: str, currency_code: str) -> str:
    """Format raw price value to display string e.g. '€27.39' or '£16.74'."""
    symbol_map = {"EUR": "€", "GBP": "£", "USD": "$"}
    symbol = symbol_map.get(currency_code.upper(), currency_code)
    try:
        return f"{symbol}{float(price_str):.2f}"
    except ValueError:
        return f"{symbol}{price_str}"


def _clean_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    url = url.strip().split("?")[0]  # strip tracking params
    if url.lower() in ("n/a", "na", "", "-"):
        return None
    return url if url.startswith("http") else None


def _build_queries(part_number: str, product_name: str, manufacturer: str) -> list[str]:
    queries = []
    pn = (part_number or "").strip()
    name = (product_name or "").strip()
    mfr = (manufacturer or "").strip()

    if pn:
        queries.append(pn)
    if mfr and pn:
        queries.append(f"{mfr} {pn}")
    if name:
        short = name[:60].rsplit(" ", 1)[0]
        queries.append(short)
    if mfr and name:
        short = name[:40].rsplit(" ", 1)[0]
        queries.append(f"{mfr} {short}")

    seen: set[str] = set()
    result = []
    for q in queries:
        if q and q not in seen:
            seen.add(q)
            result.append(q)
    return result


# ---------------------------------------------------------------------------
# Base scraper
# ---------------------------------------------------------------------------
class BaseScraper:
    SITE_NAME = "base"
    BASE_URL = ""
    CURRENCY = "EUR"

    def __init__(self, session: requests.Session):
        self.session = session
        self._last_request: dict[str, float] = {}

    def _throttle(self):
        domain = urlparse(self.BASE_URL).netloc
        now = time.time()
        elapsed = now - self._last_request.get(domain, 0)
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request[domain] = time.time()

    def _get(self, url: str, params: dict = None, retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(retries):
            self._throttle()
            try:
                resp = self.session.get(
                    url, headers=BROWSER_HEADERS, params=params, timeout=30
                )
                if resp.status_code == 404:
                    return None
                if resp.status_code == 429:
                    logger.warning(f"[{self.SITE_NAME}] Rate limited, sleeping 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else "?"
                logger.warning(f"[{self.SITE_NAME}] HTTP {code} for {url} (attempt {attempt+1})")
                if code in (403, 404):
                    return None
            except Exception as e:
                logger.warning(f"[{self.SITE_NAME}] Request error for {url} (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
        return None

    def _soup(self, url: str, params: dict = None) -> Optional[BeautifulSoup]:
        resp = self._get(url, params=params)
        if resp:
            return BeautifulSoup(resp.content, "lxml")
        return None

    def scrape_price_from_url(self, url: str) -> tuple[Optional[str], Optional[str]]:
        """Scrape price and product name from a known product URL using LD+JSON.
        Returns (price_str, product_name)."""
        soup = self._soup(url)
        if not soup:
            return None, None
        price_val, currency, product_name = _extract_ld_json_price(soup)
        if price_val:
            return _format_price(price_val, currency or self.CURRENCY), product_name
        return None, None

    def search(self, part_number: str, product_name: str, manufacturer: str) -> ScrapedPrice:
        raise NotImplementedError

    def scrape_product(self, row: dict) -> ScrapedPrice:
        """Try direct URL first, then fall back to search."""
        existing_url = _clean_url(self._get_existing_url(row))
        if existing_url:
            price, product_name = self.scrape_price_from_url(existing_url)
            if price:
                logger.info(f"  [{self.SITE_NAME}] Direct URL ✓ {price}")
                return ScrapedPrice(price=price, url=existing_url, found=True, product_name=product_name)
            logger.info(f"  [{self.SITE_NAME}] Direct URL gave no price, trying search")

        result = self.search(
            part_number=row.get("Part Number", ""),
            product_name=row.get("Name", ""),
            manufacturer=row.get("Manufacturer", ""),
        )
        return result

    def _get_existing_url(self, row: dict) -> Optional[str]:
        return None


# ---------------------------------------------------------------------------
# DMI scraper (dmi.ie and dmi.co.uk share the same platform)
# ---------------------------------------------------------------------------
class DMIScraper(BaseScraper):
    """
    dmi.ie / dmi.co.uk — custom commerce platform.
    Search: /categories.html?type=simple&name=QUERY
    Product pages: have LD+JSON with schema.org/Product
    """

    def search(self, part_number: str, product_name: str, manufacturer: str) -> ScrapedPrice:
        for query in _build_queries(part_number, product_name, manufacturer):
            result = self._html_search(query)
            if result.found:
                return result
        return ScrapedPrice()

    def _html_search(self, query: str) -> ScrapedPrice:
        url = urljoin(self.BASE_URL, "/categories.html")
        soup = self._soup(url, params={"type": "simple", "name": query})
        if not soup:
            return ScrapedPrice()
        # Find product links on search results page
        for a in soup.select("a[href*='/products/']"):
            href = a.get("href", "")
            product_url = urljoin(self.BASE_URL, href.split("?")[0])
            price, product_name = self.scrape_price_from_url(product_url)
            if price:
                logger.info(f"  [{self.SITE_NAME}] Search '{query}' ✓ {price}")
                return ScrapedPrice(price=price, url=product_url, found=True, product_name=product_name)
        return ScrapedPrice()


class DMIIEScraper(DMIScraper):
    SITE_NAME = "dmi.ie"
    BASE_URL = "https://www.dmi.ie"
    CURRENCY = "EUR"

    def _get_existing_url(self, row: dict) -> Optional[str]:
        return row.get("DMI URL (IE)", "")


class DMIUKScraper(DMIScraper):
    SITE_NAME = "dmi.co.uk"
    BASE_URL = "https://www.dmi.co.uk"
    CURRENCY = "GBP"

    def _get_existing_url(self, row: dict) -> Optional[str]:
        return row.get("DMI URL (UK)", "")


# ---------------------------------------------------------------------------
# DentalSky scraper (Magento with Ajax search — search needs Playwright)
# ---------------------------------------------------------------------------
class DentalSkyScraper(BaseScraper):
    SITE_NAME = "dentalsky.com"
    BASE_URL = "https://www.dentalsky.com"
    CURRENCY = "GBP"

    def _get_existing_url(self, row: dict) -> Optional[str]:
        return row.get("DentalSky URL", "")

    def scrape_price_from_url(self, url: str) -> tuple[Optional[str], Optional[str]]:
        """DentalSky: try LD+JSON then HTML price selectors.
        Returns (price_str, product_name)."""
        soup = self._soup(url)
        if not soup:
            return None, None
        # 1. LD+JSON (most reliable)
        price_val, currency, product_name = _extract_ld_json_price(soup)
        if price_val:
            return _format_price(price_val, currency or self.CURRENCY), product_name
        # 2. Magento HTML price selectors
        for sel in [
            "[data-price-type='finalPrice'] .price",
            ".special-price .price",
            ".regular-price .price",
            ".price-box .price",
            "[itemprop='price']",
        ]:
            el = soup.select_one(sel)
            if el:
                # DentalSky price might be in content attr or text
                content = el.get("content") or el.get_text(" ", strip=True)
                # Extract numeric value (handles garbled currency symbols)
                m = re.search(r"(\d[\d,]*\.\d{2})", content)
                if m:
                    val = m.group(1).replace(",", "")
                    return f"£{float(val):.2f}", None
        return None, None

    def search(self, part_number: str, product_name: str, manufacturer: str) -> ScrapedPrice:
        # DentalSky Magento search requires JavaScript — handled by PlaywrightScraper
        # Fallback: try constructing URL from product name slug
        return self._slug_search(part_number, product_name, manufacturer)

    def _slug_search(self, part_number: str, product_name: str, manufacturer: str) -> ScrapedPrice:
        """Try to construct a URL from the product name slug pattern."""
        name = product_name.lower()
        slug = re.sub(r"[^a-z0-9]+", "-", name).strip("-")
        words = slug.split("-")
        tried: set[str] = set()
        for length in [len(words), min(len(words), 6), min(len(words), 4)]:
            candidate_slug = "-".join(words[:length])
            if candidate_slug in tried:
                continue
            tried.add(candidate_slug)
            price, comp_name = self.scrape_price_from_url(f"{self.BASE_URL}/{candidate_slug}.html")
            if price:
                url = f"{self.BASE_URL}/{candidate_slug}.html"
                logger.info(f"  [{self.SITE_NAME}] Slug match '{candidate_slug}' ✓ {price}")
                return ScrapedPrice(price=price, url=url, found=True, product_name=comp_name)
        return ScrapedPrice()


# ---------------------------------------------------------------------------
# Dontalia scraper (custom JS platform — search needs Playwright)
# ---------------------------------------------------------------------------
class DontaliaScraper(BaseScraper):
    SITE_NAME = "dontalia.com"
    BASE_URL = "https://www.dontalia.com"
    CURRENCY = "EUR"

    def _get_existing_url(self, row: dict) -> Optional[str]:
        return row.get("Dontalia URL", "")

    def search(self, part_number: str, product_name: str, manufacturer: str) -> ScrapedPrice:
        # Dontalia's search is JS-rendered — handled by PlaywrightScraper
        # Fallback: try constructing a URL from manufacturer + product slug
        return self._slug_search(part_number, product_name, manufacturer)

    def _slug_search(self, part_number: str, product_name: str, manufacturer: str) -> ScrapedPrice:
        name = product_name.lower()
        slug = re.sub(r"[^a-z0-9]+", "-", name).strip("-")
        words = slug.split("-")
        # Dontalia URL pattern: /manufacturer-product-name.html
        for length in [min(len(words), 5), min(len(words), 4), min(len(words), 3)]:
            candidate_slug = "-".join(words[:length])
            url = f"{self.BASE_URL}/{candidate_slug}.html"
            resp = self._get(url)
            if resp and resp.status_code == 200:
                soup = BeautifulSoup(resp.content, "lxml")
                price_val, currency, product_name = _extract_ld_json_price(soup)
                if price_val:
                    price = _format_price(price_val, currency or self.CURRENCY)
                    logger.info(f"  [{self.SITE_NAME}] Slug match '{candidate_slug}' ✓ {price}")
                    return ScrapedPrice(price=price, url=url, found=True, product_name=product_name)
        return ScrapedPrice()


# ---------------------------------------------------------------------------
# Henry Schein scraper (Angular SPA — all scraping needs Playwright)
# ---------------------------------------------------------------------------
class HenryScheinScraper(BaseScraper):
    SITE_NAME = "henryschein.ie"
    BASE_URL = "https://www.henryschein.ie"
    CURRENCY = "EUR"
    SEARCH_URL = "https://www.henryschein.ie/en-ie/search/searchresults.aspx"

    def _get_existing_url(self, row: dict) -> Optional[str]:
        return row.get("Henry Schein URL", "")

    def scrape_price_from_url(self, url: str) -> Optional[str]:
        # Henry Schein is an Angular SPA; static HTML has no price data
        # Must use Playwright
        return None

    def search(self, part_number: str, product_name: str, manufacturer: str) -> ScrapedPrice:
        # Must use Playwright
        return ScrapedPrice()


# ---------------------------------------------------------------------------
# Playwright-based scraper (handles JS-heavy sites)
# ---------------------------------------------------------------------------
class PlaywrightScraper:
    """
    Uses Playwright to render JS-heavy pages.
    Falls back gracefully if Playwright is not installed.
    """

    def __init__(self):
        self._pw = None
        self._browser = None
        self._context = None
        self._available = False
        try:
            from playwright.sync_api import sync_playwright
            self._sync_playwright = sync_playwright
            self._available = True
        except ImportError:
            logger.warning("Playwright not installed. Run: playwright install chromium")

    def __enter__(self):
        if self._available:
            self._pw = self._sync_playwright().__enter__()
            self._browser = self._pw.chromium.launch(headless=True)
            self._context = self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-GB",
            )
        return self

    def __exit__(self, *args):
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.__exit__(*args)

    def get_page_html(self, url: str, wait_selector: str = None, timeout: int = 15000) -> Optional[str]:
        if not self._available:
            return None
        try:
            page = self._context.new_page()
            page.goto(url, timeout=timeout, wait_until="networkidle")
            if wait_selector:
                page.wait_for_selector(wait_selector, timeout=timeout)
            html = page.content()
            page.close()
            return html
        except Exception as e:
            logger.warning(f"[Playwright] Error loading {url}: {e}")
            try:
                page.close()
            except Exception:
                pass
            return None

    def search_dentalsky(self, query: str) -> ScrapedPrice:
        if not self._available:
            return ScrapedPrice()
        logger.info(f"  [dentalsky.com] Playwright search: {query}")
        url = f"https://www.dentalsky.com/catalogsearch/result/?q={quote_plus(query)}"
        html = self.get_page_html(url, wait_selector=".product-item-link", timeout=20000)
        if not html:
            return ScrapedPrice()
        soup = BeautifulSoup(html, "lxml")
        for link in soup.select(".product-item-link")[:3]:
            href = link.get("href", "")
            if not href:
                continue
            # Scrape the product page for LD+JSON price
            product_html = self.get_page_html(href)
            if product_html:
                product_soup = BeautifulSoup(product_html, "lxml")
                price_val, currency, product_name = _extract_ld_json_price(product_soup)
                if price_val:
                    price = _format_price(price_val, currency or "GBP")
                    logger.info(f"  [dentalsky.com] Playwright ✓ {price}")
                    return ScrapedPrice(price=price, url=href, found=True, product_name=product_name)
        return ScrapedPrice()

    def search_dontalia(self, query: str) -> ScrapedPrice:
        if not self._available:
            return ScrapedPrice()
        logger.info(f"  [dontalia.com] Playwright search: {query}")
        # Dontalia doesn't have a working search URL, try navigating to homepage and using JS search
        home_html = self.get_page_html("https://www.dontalia.com/", wait_selector=".product-card")
        if not home_html:
            return ScrapedPrice()
        # Try to find a search input and submit
        # For now fall back to slug matching via Playwright navigation
        return ScrapedPrice()

    def scrape_henryschein(self, query: str, part_number: str = "") -> ScrapedPrice:
        if not self._available:
            return ScrapedPrice()
        logger.info(f"  [henryschein.ie] Playwright search: {query}")
        url = f"https://www.henryschein.ie/en-ie/search/searchresults.aspx?searchkeyword={quote_plus(query)}"
        html = self.get_page_html(
            url,
            wait_selector="[class*='product-tile'], [class*='product-card'], [class*='product-item']",
            timeout=25000,
        )
        if not html:
            return ScrapedPrice()
        soup = BeautifulSoup(html, "lxml")
        # Try LD+JSON first
        price_val, currency, product_name = _extract_ld_json_price(soup)
        if price_val:
            price = _format_price(price_val, currency or "EUR")
            return ScrapedPrice(price=price, url=url, found=True, product_name=product_name)
        # Try to find product link then scrape it
        for link in soup.select("[class*='product'] a[href]")[:3]:
            href = link.get("href", "")
            if not href:
                continue
            product_url = href if href.startswith("http") else urljoin("https://www.henryschein.ie", href)
            product_html = self.get_page_html(product_url, wait_selector="[class*='price']", timeout=20000)
            if product_html:
                product_soup = BeautifulSoup(product_html, "lxml")
                pv, curr, product_name = _extract_ld_json_price(product_soup)
                if pv:
                    price = _format_price(pv, curr or "EUR")
                    logger.info(f"  [henryschein.ie] Playwright ✓ {price}")
                    return ScrapedPrice(price=price, url=product_url, found=True, product_name=product_name)
                # Fallback: look for price in text
                for sel in ["[class*='price']", "[itemprop='price']"]:
                    el = product_soup.select_one(sel)
                    if el:
                        text = el.get_text(" ", strip=True)
                        m = re.search(r"€\s*[\d,]+\.?\d*|[\d,]+\.?\d*\s*€", text)
                        if m:
                            price_str = re.sub(r"[€\s,]", "", m.group()).strip(".")
                            try:
                                price = f"€{float(price_str):.2f}"
                                logger.info(f"  [henryschein.ie] Playwright ✓ {price}")
                                return ScrapedPrice(price=price, url=product_url, found=True)
                            except ValueError:
                                pass
        return ScrapedPrice()

    def scrape_henryschein_url(self, url: str) -> Optional[str]:
        """Scrape a specific Henry Schein product URL using Playwright."""
        if not self._available:
            return None
        html = self.get_page_html(url, wait_selector="[class*='price'], [itemprop='price']", timeout=20000)
        if not html:
            return None
        soup = BeautifulSoup(html, "lxml")
        price_val, currency, _ = _extract_ld_json_price(soup)
        if price_val:
            return _format_price(price_val, currency or "EUR")
        return None


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------
def read_csv(path: str) -> tuple[list[dict], list[str]]:
    """
    Read the benchmarking CSV. Handles duplicate/blank column headers.
    Returns (rows_as_dicts, clean_fieldnames)
    """
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        raw_headers = next(reader)
        rows_raw = list(reader)

    # Rename blank and duplicate columns
    headers = []
    blank_count = 0
    dmi_url_count = 0
    variance_count = 0

    VARIANCE_NAMES = ["Variance (DMI IE)", "Variance (DentalSky)", "Variance (Dontalia)", "Variance (DMI UK)"]

    for h in raw_headers:
        h = h.strip()
        if h == "":
            blank_count += 1
            headers.append(f"_blank_{blank_count}")
        elif h == "DMI URL":
            dmi_url_count += 1
            headers.append("DMI URL (IE)" if dmi_url_count == 1 else "DMI URL (UK)")
        elif h == "Variance":
            variance_count += 1
            if variance_count <= len(VARIANCE_NAMES):
                headers.append(VARIANCE_NAMES[variance_count - 1])
            else:
                headers.append(f"Variance_{variance_count}")
        else:
            headers.append(h)

    rows = []
    for raw_row in rows_raw:
        padded = raw_row + [""] * max(0, len(headers) - len(raw_row))
        rows.append(dict(zip(headers, padded)))

    return rows, headers


def build_output_headers(existing_headers: list[str]) -> list[str]:
    """Add Henry Schein columns and per-site Notes columns if missing."""
    headers = list(existing_headers)
    new_cols = [
        "Henry Schein Sales Price (€)",
        "Variance (Henry Schein)",
        "Henry Schein URL",
        # Notes columns — one per site
        "DMI IE Notes",
        "DMI UK Notes",
        "DentalSky Notes",
        "Dontalia Notes",
        "Henry Schein Notes",
        # Competitor product name columns — for pack size verification
        "DMI IE Product",
        "DMI UK Product",
        "DentalSky Product",
        "Dontalia Product",
        "Henry Schein Product",
        # Pack size flag columns
        "DMI IE Pack Flag",
        "DMI UK Pack Flag",
        "DentalSky Pack Flag",
        "Dontalia Pack Flag",
        "Henry Schein Pack Flag",
        # Adjusted per-unit variance columns (only meaningful when pack sizes differ)
        "DMI IE Adjusted Variance",
        "DMI UK Adjusted Variance",
        "DentalSky Adjusted Variance",
        "Dontalia Adjusted Variance",
        "Henry Schein Adjusted Variance",
    ]
    for col in new_cols:
        if col not in headers:
            headers.append(col)
    # Remove blank spacer columns from output
    return [h for h in headers if not h.startswith("_blank_")]


# ---------------------------------------------------------------------------
# Variance calculation
# ---------------------------------------------------------------------------
def _parse_price_value(price_str: str) -> Optional[float]:
    if not price_str:
        return None
    cleaned = re.sub(r"[€£$,\s]", "", price_str)
    try:
        return float(cleaned)
    except ValueError:
        return None


def _calc_variance(own_price_str: str, competitor_price_str: str) -> str:
    """
    Variance = (our_price - competitor_price) / competitor_price * 100
    Positive = we are cheaper than competitor, Negative = we are more expensive.
    """
    own = _parse_price_value(own_price_str)
    comp = _parse_price_value(competitor_price_str)
    if own and comp and comp > 0:
        variance = ((own - comp) / comp) * 100
        return f"{variance:.1f}%"
    return "N/A"


def _extract_pack_size(name: str) -> Optional[int]:
    """
    Extract pack/box quantity from a product name string.
    e.g. "Needles 27g - Box100" → 100
         "TePe Brushes - Pack36" → 36
         "Syringe 4x3ml" → 4
         "Each" → 1
    Returns None if quantity cannot be determined.
    """
    if not name:
        return None
    name_lower = name.lower()
    # "Each" or "single" = 1
    if re.search(r'\beach\b|\bsingle\b', name_lower):
        return 1
    # "Box100", "Pack 500", "Pk25", "Bx50"
    m = re.search(r'(?:box|pack|pk|bx)\s*(\d+)', name_lower)
    if m:
        return int(m.group(1))
    # "100 pack", "50 box"
    m = re.search(r'(\d+)\s*(?:pack|box|pk|bx)\b', name_lower)
    if m:
        return int(m.group(1))
    # "4x3ml", "12x5ml" — multiplier pattern (e.g. Boutique kits)
    m = re.search(r'(\d+)\s*x\s*\d', name_lower)
    if m:
        return int(m.group(1))
    # Trailing "- 500" or "- 80" at end of name (some products use bare numbers)
    m = re.search(r'-\s*(\d{2,4})\s*$', name.strip())
    if m:
        return int(m.group(1))
    return None


def _pack_size_flag(our_name: str, comp_name: str) -> tuple[str, Optional[int], Optional[int]]:
    """
    Compare pack sizes from our product name and competitor product name.
    Returns (flag, our_qty, comp_qty) where flag is MATCH / MISMATCH / UNKNOWN.
    """
    our_qty = _extract_pack_size(our_name)
    comp_qty = _extract_pack_size(comp_name)
    if our_qty is None or comp_qty is None:
        return "UNKNOWN", our_qty, comp_qty
    if our_qty == comp_qty:
        return "MATCH", our_qty, comp_qty
    return f"MISMATCH (ours:{our_qty} theirs:{comp_qty})", our_qty, comp_qty


def _calc_adjusted_variance(own_price_str: str, comp_price_str: str, own_qty: int, comp_qty: int) -> str:
    """Per-unit variance after adjusting for pack size difference."""
    own = _parse_price_value(own_price_str)
    comp = _parse_price_value(comp_price_str)
    if own and comp and comp > 0 and own_qty > 0 and comp_qty > 0:
        own_unit = own / own_qty
        comp_unit = comp / comp_qty
        variance = ((own_unit - comp_unit) / comp_unit) * 100
        return f"{variance:.1f}%"
    return "N/A"


# ---------------------------------------------------------------------------
# Site configuration
# ---------------------------------------------------------------------------
SITE_CONFIG: dict[str, dict] = {
    "dmi_ie": {
        "scraper_class": DMIIEScraper,
        "price_col": "DMI Sales Price (€)",
        "variance_col": "Variance (DMI IE)",
        "url_col": "DMI URL (IE)",
        "notes_col": "DMI IE Notes",
        "own_price_col": "Sales Price (€)",
        "product_name_col": "DMI IE Product",
        "pack_flag_col": "DMI IE Pack Flag",
        "adj_variance_col": "DMI IE Adjusted Variance",
    },
    "dmi_uk": {
        "scraper_class": DMIUKScraper,
        "price_col": "DMI Sales Price (£)",
        "variance_col": "Variance (DMI UK)",
        "url_col": "DMI URL (UK)",
        "notes_col": "DMI UK Notes",
        "own_price_col": "Sales Price (£)",
        "product_name_col": "DMI UK Product",
        "pack_flag_col": "DMI UK Pack Flag",
        "adj_variance_col": "DMI UK Adjusted Variance",
    },
    "dentalsky": {
        "scraper_class": DentalSkyScraper,
        "price_col": "DentalSky Sales Price (£)",
        "variance_col": "Variance (DentalSky)",
        "url_col": "DentalSky URL",
        "notes_col": "DentalSky Notes",
        "own_price_col": "Sales Price (£)",
        "product_name_col": "DentalSky Product",
        "pack_flag_col": "DentalSky Pack Flag",
        "adj_variance_col": "DentalSky Adjusted Variance",
    },
    "dontalia": {
        "scraper_class": DontaliaScraper,
        "price_col": "Dontalia Sales Price (€)",
        "variance_col": "Variance (Dontalia)",
        "url_col": "Dontalia URL",
        "notes_col": "Dontalia Notes",
        "own_price_col": "Sales Price (€)",
        "product_name_col": "Dontalia Product",
        "pack_flag_col": "Dontalia Pack Flag",
        "adj_variance_col": "Dontalia Adjusted Variance",
    },
    "henryschein": {
        "scraper_class": HenryScheinScraper,
        "price_col": "Henry Schein Sales Price (€)",
        "variance_col": "Variance (Henry Schein)",
        "url_col": "Henry Schein URL",
        "notes_col": "Henry Schein Notes",
        "own_price_col": "Sales Price (€)",
        "product_name_col": "Henry Schein Product",
        "pack_flag_col": "Henry Schein Pack Flag",
        "adj_variance_col": "Henry Schein Adjusted Variance",
    },
}


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
def run(
    input_path: str,
    output_path: str,
    sites: list[str],
    limit: Optional[int] = None,
    skip_existing: bool = True,
    use_playwright: bool = False,
):
    logger.info(f"Reading input: {input_path}")
    rows, headers = read_csv(input_path)
    output_headers = build_output_headers(headers)

    if limit:
        rows = rows[:limit]
        logger.info(f"Limited to first {limit} rows")

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    # Build scraper instances
    scrapers: dict[str, BaseScraper] = {}
    for key in sites:
        cfg = SITE_CONFIG.get(key)
        if not cfg:
            logger.warning(f"Unknown site key '{key}', skipping")
            continue
        scrapers[key] = cfg["scraper_class"](session)
        logger.info(f"Registered scraper: {key}")

    # Playwright context (for JS-heavy search)
    pw_scraper = PlaywrightScraper() if use_playwright else None
    pw_ctx = pw_scraper.__enter__() if pw_scraper else None

    try:
        total = len(rows)
        for idx, row in enumerate(rows, 1):
            code = row.get("Code", "?")
            name = row.get("Name", "?")
            logger.info(f"[{idx}/{total}] {code} — {name[:70]}")

            for site_key, scraper in scrapers.items():
                cfg = SITE_CONFIG[site_key]
                price_col = cfg["price_col"]
                url_col = cfg["url_col"]
                variance_col = cfg["variance_col"]
                notes_col = cfg["notes_col"]
                own_price_col = cfg["own_price_col"]
                product_name_col = cfg["product_name_col"]
                pack_flag_col = cfg["pack_flag_col"]
                adj_variance_col = cfg["adj_variance_col"]

                existing_price = (row.get(price_col) or "").strip()
                if skip_existing and existing_price and existing_price.lower() not in ("n/a", ""):
                    logger.info(f"  [{site_key}] Already has price: {existing_price}, skipping")
                    continue

                result = ScrapedPrice()
                try:
                    # --- Henry Schein needs Playwright ---
                    if site_key == "henryschein" and pw_ctx:
                        existing_url = _clean_url(row.get(url_col, ""))
                        if existing_url:
                            price = pw_ctx.scrape_henryschein_url(existing_url)
                            if price:
                                result = ScrapedPrice(price=price, url=existing_url, found=True)
                        if not result.found:
                            for q in _build_queries(
                                row.get("Part Number", ""),
                                row.get("Name", ""),
                                row.get("Manufacturer", ""),
                            ):
                                result = pw_ctx.scrape_henryschein(q, row.get("Part Number", ""))
                                if result.found:
                                    break

                    # --- DentalSky Playwright search for missing URLs ---
                    elif site_key == "dentalsky" and pw_ctx and not _clean_url(row.get(url_col, "")):
                        result = scraper.scrape_product(row)  # try slug first
                        if not result.found:
                            for q in _build_queries(
                                row.get("Part Number", ""),
                                row.get("Name", ""),
                                row.get("Manufacturer", ""),
                            ):
                                result = pw_ctx.search_dentalsky(q)
                                if result.found:
                                    break

                    # --- All other sites ---
                    else:
                        result = scraper.scrape_product(row)

                except Exception as e:
                    logger.error(f"  [{site_key}] Unhandled error: {e}", exc_info=True)

                if result.found and result.price:
                    row[price_col] = result.price
                    if result.url:
                        row[url_col] = result.url
                    row[variance_col] = _calc_variance(row.get(own_price_col, ""), result.price)
                    row[notes_col] = ""
                    if result.product_name:
                        row[product_name_col] = result.product_name
                        flag, our_qty, comp_qty = _pack_size_flag(row.get("Name", ""), result.product_name)
                        row[pack_flag_col] = flag
                        if our_qty and comp_qty and our_qty != comp_qty:
                            row[adj_variance_col] = _calc_adjusted_variance(
                                row.get(own_price_col, ""), result.price, our_qty, comp_qty
                            )
                            logger.info(f"  [{site_key}] Pack size mismatch: ours={our_qty} theirs={comp_qty} → adjusted variance: {row[adj_variance_col]}")
                    logger.info(f"  [{site_key}] ✓ {result.price}  (variance: {row[variance_col]})")
                else:
                    if not existing_price or existing_price.lower() == "n/a":
                        row[price_col] = "N/A"
                        row[notes_col] = "Not listed on competitor"
                    logger.info(f"  [{site_key}] ✗ Not found")
    finally:
        if pw_ctx and pw_scraper:
            pw_scraper.__exit__(None, None, None)

    # Write output CSV
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=output_headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Output written to: {output_path}")

    # Print summary
    total_products = len(rows)
    for site_key in sites:
        cfg = SITE_CONFIG.get(site_key, {})
        price_col = cfg.get("price_col", "")
        found = sum(
            1 for r in rows
            if r.get(price_col, "N/A").strip() not in ("N/A", "")
        )
        logger.info(f"  {site_key}: {found}/{total_products} prices found")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Dental product price benchmarking scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        default="Price Benchmarking - Top 300 April 2025 - Public Website Prices.csv",
    )
    parser.add_argument("--output", default="output_prices.csv")
    parser.add_argument(
        "--sites",
        nargs="+",
        default=list(SITE_CONFIG.keys()),
        choices=list(SITE_CONFIG.keys()),
        metavar="SITE",
        help=f"Sites to scrape. Choices: {', '.join(SITE_CONFIG.keys())}",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--playwright",
        action="store_true",
        help="Enable Playwright for JS-heavy search (dentalsky, dontalia, henryschein)",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-scrape even when price already exists in input CSV",
    )
    args = parser.parse_args()

    run(
        input_path=args.input,
        output_path=args.output,
        sites=args.sites,
        limit=args.limit,
        skip_existing=not args.no_skip_existing,
        use_playwright=args.playwright,
    )


if __name__ == "__main__":
    main()
