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
import os
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
    match_score: float = 0.0


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


def _clean_product_name(name: str) -> str:
    """Strip internal naming conventions to get a clean product name for searching."""
    name = (name or "").strip()
    # Strip leading # markers used internally (e.g. '#Colgate Duraphat...')
    name = name.lstrip("#").strip()
    # Strip trailing unit suffixes like ' - Each', ' - Pack6', ' - Box100'
    name = re.sub(r"\s*-\s*(Each|Pack\s*\d*|Box\s*\d*|Tube|Roll|Bag|Kit|Set|Pair)\s*$", "", name, flags=re.IGNORECASE).strip()
    # Strip trailing bare ' -' or '- '
    name = re.sub(r"\s*-\s*$", "", name).strip()
    return name


def _build_queries(part_number: str, product_name: str, manufacturer: str, competitor_code: str = "") -> list[str]:
    queries = []
    pn = (part_number or "").strip()
    name = _clean_product_name(product_name)
    mfr = (manufacturer or "").strip()
    cc = (competitor_code or "").strip()

    # Product name is tried first — most descriptive and avoids false code matches
    if name:
        short = name[:60].rsplit(" ", 1)[0].rstrip(" -")
        queries.append(short)
    if mfr and name:
        short = name[:40].rsplit(" ", 1)[0].rstrip(" -")
        queries.append(f"{mfr} {short}")
    # Competitor code and part number as fallback
    if cc:
        queries.append(cc)
        stripped = re.sub(r"[A-Z]$", "", cc).strip()
        if stripped and stripped != cc:
            queries.append(stripped)
    if pn:
        queries.append(pn)
    if mfr and pn:
        queries.append(f"{mfr} {pn}")

    seen: set[str] = set()
    result = []
    for q in queries:
        if q and q not in seen:
            seen.add(q)
            result.append(q)
    return result


# Generic words that appear in many dental product names and should NOT be used
# as the sole discriminating keyword for relevance checks.
_RELEVANCE_STOPWORDS = {
    "each", "pack", "tube", "varnish", "paste", "toothpaste", "fluoride",
    "dental", "product", "with", "from", "size", "type", "mini", "single",
    "standard", "units", "unit", "assorted", "mixed", "prophy",
}


def _relevance_keywords(query: str) -> list[str]:
    """Extract significant (non-generic, >= 4 char, non-numeric) keywords from a search query."""
    words = re.split(r"[\s\-_/()+]+", query.lower())
    return [
        w for w in words
        if len(w) >= 4
        and w not in _RELEVANCE_STOPWORDS
        and not re.fullmatch(r"[\d]+[a-z]*", w)   # skip numbers/quantities like '5000', '10ml', '75g'
    ]


def _href_matches_query(href: str, query: str) -> bool:
    """Return True if at least one significant keyword from query appears in the href slug."""
    keywords = _relevance_keywords(query)
    if not keywords:
        return True  # no filtering possible — allow it
    href_lower = href.lower()
    return any(kw in href_lower for kw in keywords)


def _extract_numeric_tokens(name: str) -> set[str]:
    """
    Extract specific numeric identifiers from a product name using regex.
    These are high-value discriminators: gauges, concentrations, sizes, pack counts.
    e.g. "Needles 27g Long Box100 0.4x35mm" → {'27g', 'box100', '0.4x35mm'}
    """
    tokens = set()
    n = name.lower()
    # Percentages: 16%, 1:100,000
    for m in re.finditer(r'\d+\.?\d*%', n):
        tokens.add(m.group())
    for m in re.finditer(r'1:\d[\d,]+', n):
        tokens.add(m.group().replace(",", ""))
    # Dimensions / measurements: 0.4x35mm, 27g, 2.2ml, 490ml, 30Gx8mm
    for m in re.finditer(r'\d+\.?\d*\s*x\s*\d+\.?\d*\s*(?:mm|cm)?|\d+\.?\d*\s*(?:g|ml|mm|cm|mg)\b', n):
        tokens.add(re.sub(r'\s+', '', m.group()))
    # Pack/box size: Box100, Pack500, 200pk
    for m in re.finditer(r'(?:box|pack|pk|bx)\s*\d+|\d+\s*(?:pack|box|pk)', n):
        tokens.add(re.sub(r'\s+', '', m.group()))
    return tokens


def _name_similarity_score(our_name: str, found_name: str) -> float:
    """
    Return a 0.0–1.0 similarity score between our product name and a found product name.
    Combines keyword overlap (70%) with numeric/spec token matching (30%).
    Numeric tokens (gauges, sizes, concentrations, pack counts) are extracted by regex
    and compared separately — a mismatch here strongly reduces the score.
    """
    our_tokens = set(_relevance_keywords(our_name))
    found_tokens = set(_relevance_keywords(found_name))
    if not our_tokens:
        return 1.0  # can't measure — assume match
    overlap = our_tokens & found_tokens
    keyword_score = len(overlap) / len(our_tokens)

    # Numeric token scoring (concentrations, sizes, gauges, pack counts)
    our_numeric = _extract_numeric_tokens(our_name)
    found_numeric = _extract_numeric_tokens(found_name)
    if our_numeric:
        numeric_overlap = our_numeric & found_numeric
        numeric_score = len(numeric_overlap) / len(our_numeric)
        score = 0.7 * keyword_score + 0.3 * numeric_score
    else:
        score = keyword_score

    logger.debug(
        f"  Name similarity: {score:.2f} "
        f"(keywords={keyword_score:.2f} overlap={overlap}, "
        f"numeric={our_numeric & found_numeric if our_numeric else 'n/a'})"
    )
    return score


def _names_similar(our_name: str, found_name: str, threshold: float = 0.6) -> bool:
    """Return True if found product name is similar enough to ours (score >= threshold)."""
    return _name_similarity_score(our_name, found_name) >= threshold


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
    Product pages: price is in span[class*=price] (LD+JSON lacks price for some products).
    """

    def scrape_price_from_url(self, url: str) -> tuple[Optional[str], Optional[str]]:
        """DMI: extract both LD+JSON and displayed HTML price, prefer the displayed one.
        LD+JSON sometimes contains stale catalogue prices that differ from what's shown on screen."""
        soup = self._soup(url)
        if not soup:
            return None, None

        product_name_el = soup.select_one("h1")
        pname_html = product_name_el.get_text(strip=True) if product_name_el else None
        symbol = "£" if self.CURRENCY == "GBP" else "€"

        # Extract displayed HTML price from span[class*=price]
        html_price: Optional[float] = None
        for el in soup.select("span[class*=price]"):
            text = el.get_text(strip=True)
            m = re.search(r"(\d[\d,]*\.\d{2})", text)
            if m:
                val = float(m.group(1).replace(",", ""))
                if val > 0:
                    html_price = val
                    break

        # Extract LD+JSON price
        ld_price_val, ld_currency, ld_product_name = _extract_ld_json_price(soup)
        ld_price: Optional[float] = float(ld_price_val) if ld_price_val else None
        product_name = ld_product_name or pname_html

        # Prefer the displayed HTML price — it reflects what a customer actually sees
        # Fall back to LD+JSON if no HTML price found
        if html_price:
            if ld_price and abs(html_price - ld_price) > 0.01:
                logger.debug(f"  [{self.SITE_NAME}] Price mismatch — displayed: {symbol}{html_price:.2f}, LD+JSON: {symbol}{ld_price:.2f} — using displayed")
            return f"{symbol}{html_price:.2f}", product_name
        if ld_price:
            return _format_price(ld_price_val, ld_currency or self.CURRENCY), product_name
        return None, None

    def search(self, part_number: str, product_name: str, manufacturer: str, competitor_code: str = "") -> ScrapedPrice:
        for query in _build_queries(part_number, product_name, manufacturer, competitor_code):
            result = self._html_search(query, our_name=product_name, competitor_code=competitor_code)
            if result.found:
                return result
        return ScrapedPrice()

    def scrape_product(self, row: dict) -> ScrapedPrice:
        existing_url = _clean_url(self._get_existing_url(row))
        if existing_url:
            price, product_name = self.scrape_price_from_url(existing_url)
            if price:
                logger.info(f"  [{self.SITE_NAME}] Direct URL ✓ {price}")
                return ScrapedPrice(price=price, url=existing_url, found=True, product_name=product_name)
            logger.info(f"  [{self.SITE_NAME}] Direct URL gave no price, trying search")
        return self.search(
            part_number=row.get("Part Number", ""),
            product_name=row.get("Name", ""),
            manufacturer=row.get("Manufacturer", ""),
            competitor_code=row.get("DMI Code", ""),
        )

    def _html_search(self, query: str, our_name: str = "", competitor_code: str = "") -> ScrapedPrice:
        url = urljoin(self.BASE_URL, "/categories.html")
        soup = self._soup(url, params={"type": "simple", "name": query})
        if not soup:
            return ScrapedPrice()

        # Collect all candidates, scoring each by anchor text similarity
        cc_slug = competitor_code.lower().replace("_", "-") if competitor_code else ""
        candidates = []
        for a in soup.select("a[href*='/products/']"):
            href = a.get("href", "")
            anchor_text = a.get_text(strip=True)
            if not href:
                continue
            # Pre-filter: skip if neither href slug nor anchor text shares any keyword with query
            if not _href_matches_query(href, query) and not _href_matches_query(anchor_text, query):
                logger.debug(f"  [{self.SITE_NAME}] Pre-filter skipped: {href[:60]}")
                continue
            # Base score from name similarity on anchor text
            score = _name_similarity_score(our_name, anchor_text) if our_name else 0.5
            # Boost score when the competitor code appears exactly in the URL slug
            # This prevents variant mix-ups (e.g. PERF-0040046 vs PERF-0040047)
            if cc_slug and cc_slug in href.lower():
                score = min(1.0, score + 0.3)
                logger.debug(f"  [{self.SITE_NAME}] Code match boost for {href[:60]}")
            candidates.append((score, href, anchor_text))

        if not candidates:
            return ScrapedPrice()

        # Sort by relevance score, best first
        candidates.sort(key=lambda x: x[0], reverse=True)
        logger.debug(f"  [{self.SITE_NAME}] {len(candidates)} candidates for '{query}', best: '{candidates[0][2]}' (score={candidates[0][0]:.2f})")

        # Try candidates in order until we get a price from a sufficiently similar product
        for score, href, anchor_text in candidates:
            if score < 0.5:
                break  # remaining candidates are too dissimilar
            product_url = urljoin(self.BASE_URL, href.split("?")[0])
            price, product_name = self.scrape_price_from_url(product_url)
            if not price:
                continue
            # Re-score using the actual product page name (more accurate than anchor text)
            final_name = product_name or anchor_text
            final_score = _name_similarity_score(our_name, final_name) if our_name else score
            if final_score < 0.6:
                logger.info(f"  [{self.SITE_NAME}] ✗ Rejected (score={final_score:.2f}) — '{final_name}'")
                continue
            logger.info(f"  [{self.SITE_NAME}] Search '{query}' ✓ {price} (score={final_score:.2f})")
            return ScrapedPrice(price=price, url=product_url, found=True, product_name=final_name, match_score=final_score)

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


def read_excel(path: str) -> tuple[list[dict], list[str]]:
    """
    Read an Excel (.xlsx/.xls) file as input.
    Maps common column name variants to what the scraper expects.
    Returns (rows_as_dicts, fieldnames).
    """
    import pandas as pd

    df = pd.read_excel(path, dtype=str)
    df = df.fillna("")

    # Column name mapping: Excel name → scraper name
    col_map = {
        "Product Group Description": "Product Group",
        "Stock Unit Name": "Stock Unit",
    }
    df.rename(columns=col_map, inplace=True)

    headers = list(df.columns)
    rows = df.to_dict(orient="records")
    return rows, headers


def read_input(path: str) -> tuple[list[dict], list[str]]:
    """Dispatch to read_csv or read_excel based on file extension."""
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        return read_excel(path)
    return read_csv(path)


def build_output_headers(existing_headers: list[str]) -> list[str]:
    """Add all site price/URL/variance columns and per-site Notes columns if missing."""
    headers = list(existing_headers)
    new_cols = [
        # Price, variance, and URL columns for all sites
        "DMI Sales Price (€)",
        "Variance (DMI IE)",
        "DMI URL (IE)",
        "DMI Sales Price (£)",
        "Variance (DMI UK)",
        "DMI URL (UK)",
        "DentalSky Sales Price (£)",
        "Variance (DentalSky)",
        "DentalSky URL",
        "Dontalia Sales Price (€)",
        "Variance (Dontalia)",
        "Dontalia URL",
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
# Product code mappings — persisted between runs, manually editable
# ---------------------------------------------------------------------------
MAPPINGS_HEADERS = [
    "Our Code", "Our Name", "Site",
    "Competitor URL", "Competitor Product Name",
    "Match Score", "Manual Override",
]


def load_mappings(output_path: str) -> dict:
    """
    Load the 'Product Mappings' sheet from a previous output .xlsx file.
    Returns a dict keyed by (our_code, site). Returns empty dict if file doesn't exist
    or has no mappings sheet.
    """
    import pandas as pd

    mappings: dict[tuple, dict] = {}
    if not os.path.exists(output_path):
        return mappings
    if not output_path.endswith((".xlsx", ".xls")):
        return mappings
    try:
        xl = pd.ExcelFile(output_path)
        if "Product Mappings" not in xl.sheet_names:
            return mappings
        df = pd.read_excel(output_path, sheet_name="Product Mappings", dtype=str).fillna("")
        for _, row in df.iterrows():
            key = (str(row.get("Our Code", "")).strip(), str(row.get("Site", "")).strip())
            mappings[key] = row.to_dict()
        logger.info(f"Loaded {len(mappings)} product mappings from '{output_path}'")
    except Exception as e:
        logger.warning(f"Could not load mappings from '{output_path}': {e}")
    return mappings


CHECKPOINT_EVERY = 5  # write output after every N products processed


def load_progress(output_path: str) -> dict:
    """
    Load previously scraped prices from an existing output file.
    Returns a dict keyed by product Code so they can be merged back into
    the input rows — allowing the run to resume where it left off.
    """
    import pandas as pd

    progress: dict[str, dict] = {}
    if not os.path.exists(output_path):
        return progress
    try:
        if output_path.lower().endswith(".csv"):
            df = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig").fillna("")
        else:
            df = pd.read_excel(output_path, sheet_name="Prices", dtype=str).fillna("")
        for _, row in df.iterrows():
            code = str(row.get("Code", "")).strip()
            if code:
                progress[code] = row.to_dict()
        logger.info(f"Resumed: loaded progress for {len(progress)} products from '{output_path}'")
    except Exception as e:
        logger.warning(f"Could not load progress from '{output_path}': {e}")
    return progress


def write_output(output_path: str, rows: list, output_headers: list, mappings: dict) -> None:
    """Write prices + product mappings to .xlsx (two sheets) or .csv (two files)."""
    import pandas as pd

    prices_df = pd.DataFrame(rows, columns=output_headers)
    # Fill missing columns with empty string
    for col in output_headers:
        if col not in prices_df.columns:
            prices_df[col] = ""

    mapping_rows = sorted(mappings.values(), key=lambda r: (r.get("Our Code", ""), r.get("Site", "")))
    mappings_df = pd.DataFrame(mapping_rows, columns=MAPPINGS_HEADERS) if mapping_rows else pd.DataFrame(columns=MAPPINGS_HEADERS)

    if output_path.lower().endswith(".csv"):
        mappings_path = output_path[:-4] + "_mappings.csv"
        prices_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        mappings_df.to_csv(mappings_path, index=False)
        logger.info(f"Output written to '{output_path}' and '{mappings_path}'")
    else:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            prices_df.to_excel(writer, sheet_name="Prices", index=False)
            mappings_df.to_excel(writer, sheet_name="Product Mappings", index=False)
        logger.info(f"Output written to '{output_path}' (sheets: Prices, Product Mappings)")


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
    rows, headers = read_input(input_path)
    output_headers = build_output_headers(headers)

    if limit:
        rows = rows[:limit]
        logger.info(f"Limited to first {limit} rows")

    # Merge previously scraped prices back into rows so skip_existing works correctly
    progress = load_progress(output_path)
    if progress:
        for row in rows:
            code = str(row.get("Code", "")).strip()
            if code in progress:
                for col, val in progress[code].items():
                    if val and val != "nan" and not row.get(col):
                        row[col] = val

    # Load product code mappings from previous output file (if it exists)
    mappings = load_mappings(output_path)

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
                mapping_key = (str(code), site_key)
                mapping = mappings.get(mapping_key)

                try:
                    # --- Step 1: Check product mappings first ---
                    # Manual override (client-specified URL) takes top priority
                    mapped_url = None
                    if mapping:
                        override = (mapping.get("Manual Override") or "").strip()
                        auto_url = (mapping.get("Competitor URL") or "").strip()
                        if override and override.startswith("http"):
                            mapped_url = override
                            logger.info(f"  [{site_key}] Using manual override URL from mappings")
                        elif auto_url and auto_url.startswith("http"):
                            mapped_url = auto_url
                            logger.info(f"  [{site_key}] Using auto-discovered URL from mappings")

                    if mapped_url:
                        # Scrape directly from mapped URL — no search needed
                        if site_key == "henryschein" and pw_ctx:
                            price = pw_ctx.scrape_henryschein_url(mapped_url)
                            if price:
                                result = ScrapedPrice(price=price, url=mapped_url, found=True, match_score=1.0)
                        else:
                            price, comp_name = scraper.scrape_price_from_url(mapped_url)
                            if price:
                                result = ScrapedPrice(price=price, url=mapped_url, found=True, product_name=comp_name, match_score=1.0)

                    # --- Step 2: Fall back to search if no mapping ---
                    if not result.found:
                        if site_key == "henryschein" and pw_ctx:
                            existing_url = _clean_url(row.get(url_col, ""))
                            if existing_url:
                                price = pw_ctx.scrape_henryschein_url(existing_url)
                                if price:
                                    result = ScrapedPrice(price=price, url=existing_url, found=True, match_score=1.0)
                            if not result.found:
                                for q in _build_queries(
                                    row.get("Part Number", ""),
                                    row.get("Name", ""),
                                    row.get("Manufacturer", ""),
                                    row.get("Schein Code", ""),
                                ):
                                    result = pw_ctx.scrape_henryschein(q, row.get("Part Number", ""))
                                    if result.found:
                                        break

                        elif site_key == "dentalsky" and pw_ctx and not _clean_url(row.get(url_col, "")):
                            result = scraper.scrape_product(row)
                            if not result.found:
                                for q in _build_queries(
                                    row.get("Part Number", ""),
                                    row.get("Name", ""),
                                    row.get("Manufacturer", ""),
                                ):
                                    result = pw_ctx.search_dentalsky(q)
                                    if result.found:
                                        break

                        else:
                            result = scraper.scrape_product(row)

                except Exception as e:
                    logger.error(f"  [{site_key}] Unhandled error: {e}", exc_info=True)

                # --- Step 3: Similarity check (skip if came from a mapping) ---
                if result.found and result.price and not mapped_url:
                    if result.product_name:
                        score = _name_similarity_score(row.get("Name", ""), result.product_name)
                        result.match_score = score
                        if score < 0.6:
                            logger.info(
                                f"  [{site_key}] ✗ Rejected (score={score:.2f}) — "
                                f"found: '{result.product_name}'"
                            )
                            result = ScrapedPrice()

                # --- Step 4: Accept result and update mappings ---
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
                    logger.info(f"  [{site_key}] ✓ {result.price}  (score={result.match_score:.2f}, variance: {row[variance_col]})")

                    # Persist this mapping (don't overwrite a manual override)
                    existing_override = (mapping.get("Manual Override") or "").strip() if mapping else ""
                    mappings[mapping_key] = {
                        "Our Code": str(code),
                        "Our Name": name,
                        "Site": site_key,
                        "Competitor URL": result.url or "",
                        "Competitor Product Name": result.product_name or "",
                        "Match Score": f"{result.match_score:.2f}",
                        "Manual Override": existing_override,
                    }
                else:
                    if not existing_price or existing_price.lower() == "n/a":
                        row[price_col] = "N/A"
                        row[notes_col] = "Not listed on competitor"
                    logger.info(f"  [{site_key}] ✗ Not found")

            # Checkpoint: write output every N products so progress is never lost
            if idx % CHECKPOINT_EVERY == 0:
                write_output(output_path, rows, output_headers, mappings)
                logger.info(f"  [checkpoint] Saved after {idx}/{total} products")
    finally:
        if pw_ctx and pw_scraper:
            pw_scraper.__exit__(None, None, None)

    write_output(output_path, rows, output_headers, mappings)

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
    parser.add_argument("--output", default="output_prices.xlsx")
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
