"""
Live master sheet workflow: read competitor URLs from Google Sheets → scrape prices → write back.

How Alexi uses this:
  1. Open the Google Sheet (already shared with the service account).
  2. Paste competitor product URLs into any of these columns for any product row:
       • "DMI URL (IE)"         → fills "DMI Sales Price (€)"
       • "DMI URL (UK)"         → fills "DMI Sales Price (£)"
       • "DentalSky URL"        → fills "DentalSky Sales Price (£)"
       • "Dontalia URL"         → fills "Dontalia Sales Price (€)"
       • "Henry Schein URL"     → fills "Henry Schein Sales Price (€)"  [needs --playwright]
  3. Run: python scrape_from_sheet.py

Only rows where a URL is present but price is missing/N/A are scraped.
Use --force to re-scrape all rows that have a URL, even if a price exists.
Use --playwright to enable Henry Schein scraping (Playwright must be installed).

Usage:
  python scrape_from_sheet.py
  python scrape_from_sheet.py --worksheet "Benchmarking Data" --force
  python scrape_from_sheet.py --playwright
"""

import argparse
import logging
import os
import re
import sys
import time

import gspread
import requests
from google.oauth2.service_account import Credentials

# Import price extraction logic from scraper.py (same directory)
sys.path.insert(0, os.path.dirname(__file__))
from scraper import (
    DMIScraper,
    DentalSkyScraper,
    DontaliaScraper,
    HenryScheinScraper,
    PlaywrightScraper,
    _extract_ld_json_price,
    _extract_dentalsky_price_from_soup,
    _format_price,
    _clean_url,
    _calc_variance,
)
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────
CREDS_FILE    = os.path.join(os.path.dirname(__file__), "bfm-competitor-price-scraper-60bbef18550e.json")
SHEET_ID      = "1qJCFzTea15Q9HlWqzIEFKHn47PW-Nak-CUHNulZtsjs"
WORKSHEET     = "Benchmarking Data"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Maps: (url_column, price_column, variance_column, product_column, ref_price_column, currency)
SITE_CONFIG = [
    {
        "url_col":      "DMI URL (IE)",
        "price_col":    "DMI Sales Price (€)",
        "variance_col": "Variance (DMI IE)",
        "product_col":  "DMI IE Product",
        "ref_price_col": "Sales Price (€)",
        "site_class":   "dmi_ie",
    },
    {
        "url_col":      "DMI URL (UK)",
        "price_col":    "DMI Sales Price (£)",
        "variance_col": "Variance (DMI UK)",
        "product_col":  "DMI UK Product",
        "ref_price_col": "Sales Price (£)",
        "site_class":   "dmi_uk",
    },
    {
        "url_col":      "DentalSky URL",
        "price_col":    "DentalSky Sales Price (£)",
        "variance_col": "Variance (DentalSky)",
        "product_col":  "DentalSky Product",
        "ref_price_col": "Sales Price (£)",
        "site_class":   "dentalsky",
    },
    {
        "url_col":      "Dontalia URL",
        "price_col":    "Dontalia Sales Price (€)",
        "variance_col": "Variance (Dontalia)",
        "product_col":  "Dontalia Product",
        "ref_price_col": "Sales Price (€)",
        "site_class":   "dontalia",
    },
    {
        "url_col":      "Henry Schein URL",
        "price_col":    "Henry Schein Sales Price (€)",
        "variance_col": "Variance (Henry Schein)",
        "product_col":  "Henry Schein Product",
        "ref_price_col": "Sales Price (€)",
        "site_class":   "henryschein",
        "playwright_required": True,
    },
]

_MISSING = {"", "n/a", "na", "-", "not listed on competitor"}


def _is_missing(val: str) -> bool:
    return str(val or "").strip().lower() in _MISSING


def _build_scrapers(session: requests.Session):
    return {
        "dmi_ie":     DMIScraper(session),
        "dmi_uk":     DMIScraper(session),
        "dentalsky":  DentalSkyScraper(session),
        "dontalia":   DontaliaScraper(session),
    }


def _scrape_henryschein_url(url: str, pw: PlaywrightScraper):
    """Scrape a Henry Schein product URL via Playwright (Angular SPA)."""
    if not pw._available:
        return None, None
    html = pw.get_page_html(url, timeout=20000)
    if not html:
        return None, None
    soup = BeautifulSoup(html, "lxml")
    price_val, currency, product_name = _extract_ld_json_price(soup)
    if price_val:
        return _format_price(price_val, currency or "EUR"), product_name
    # Try visible price selectors
    for sel in ["[class*='price']", "[class*='Price']"]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            m = re.search(r"(\d[\d,]*\.\d{2})", text)
            if m:
                return f"€{float(m.group(1).replace(',', '')):.2f}", None
    return None, None


def _col_letter(idx: int) -> str:
    result = ""
    n = idx + 1
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worksheet",  default=WORKSHEET, help="Worksheet name")
    parser.add_argument("--sheet-id",   default=SHEET_ID,  help="Google Sheet ID")
    parser.add_argument("--force",      action="store_true", help="Re-scrape all URLs, not just missing prices")
    parser.add_argument("--playwright", action="store_true", help="Enable Henry Schein scraping via Playwright")
    args = parser.parse_args()

    # ── auth ──────────────────────────────────────────────────────────────────
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(args.sheet_id)

    try:
        ws = spreadsheet.worksheet(args.worksheet)
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Worksheet '{args.worksheet}' not found. Run push_to_sheets.py first.")
        sys.exit(1)

    logger.info(f"Reading sheet: {args.worksheet}")
    all_values = ws.get_all_values()
    if not all_values:
        logger.error("Sheet is empty.")
        sys.exit(1)

    headers = all_values[0]
    data_rows = all_values[1:]
    col_map = {h: i for i, h in enumerate(headers)}

    logger.info(f"  {len(data_rows)} data rows, {len(headers)} columns")

    # ── scraper session ───────────────────────────────────────────────────────
    session = requests.Session()
    scrapers = _build_scrapers(session)

    # Patch DMI UK site URL
    scrapers["dmi_uk"].BASE_URL  = "https://www.dmi.co.uk"
    scrapers["dmi_uk"].SITE_NAME = "dmi.co.uk"
    scrapers["dmi_uk"].CURRENCY  = "GBP"

    pw = PlaywrightScraper() if args.playwright else None
    if pw:
        pw.__enter__()

    # ── collect updates ───────────────────────────────────────────────────────
    # Each update: {"row": 1-based sheet row, "col": 1-based sheet col, "value": str}
    updates: list[dict] = []

    try:
        for row_idx, row in enumerate(data_rows):
            sheet_row = row_idx + 2  # 1-based, +1 for header

            for cfg in SITE_CONFIG:
                if cfg.get("playwright_required") and not args.playwright:
                    continue

                url_col      = cfg["url_col"]
                price_col    = cfg["price_col"]
                variance_col = cfg["variance_col"]
                product_col  = cfg["product_col"]
                ref_price_col = cfg["ref_price_col"]
                site_key     = cfg["site_class"]

                if url_col not in col_map:
                    continue

                url_idx   = col_map[url_col]
                raw_url   = row[url_idx] if url_idx < len(row) else ""
                clean     = _clean_url(raw_url)
                if not clean:
                    continue

                # Skip if price already populated (unless --force)
                price_idx = col_map.get(price_col)
                if price_idx is not None:
                    existing_price = row[price_idx] if price_idx < len(row) else ""
                    if not args.force and not _is_missing(existing_price):
                        logger.info(f"  Row {sheet_row} [{url_col}] — price already set ({existing_price}), skipping")
                        continue

                logger.info(f"  Row {sheet_row} [{url_col}] → {clean[:60]}")

                # Scrape the price
                price, product_name = None, None
                if site_key == "henryschein" and pw:
                    price, product_name = _scrape_henryschein_url(clean, pw)
                elif site_key in scrapers:
                    scraper = scrapers[site_key]
                    price, product_name = scraper.scrape_price_from_url(clean)

                if not price:
                    logger.warning(f"    No price found for {clean[:60]}")
                    continue

                logger.info(f"    Price: {price}  Product: {product_name or '—'}")

                # Queue price update
                if price_idx is not None:
                    updates.append({"row": sheet_row, "col": price_idx + 1, "value": price})

                # Queue product name update
                prod_idx = col_map.get(product_col)
                if prod_idx is not None and product_name:
                    updates.append({"row": sheet_row, "col": prod_idx + 1, "value": product_name})

                # Queue variance update
                var_idx = col_map.get(variance_col)
                ref_idx = col_map.get(ref_price_col)
                if var_idx is not None and ref_idx is not None:
                    ref_price = row[ref_idx] if ref_idx < len(row) else ""
                    variance = _calc_variance(ref_price, price)
                    updates.append({"row": sheet_row, "col": var_idx + 1, "value": variance})

                time.sleep(0.5)

    finally:
        if pw:
            pw.__exit__(None, None, None)

    if not updates:
        logger.info("No updates needed — all prices already populated.")
        return

    # ── write back to sheet ───────────────────────────────────────────────────
    logger.info(f"\nWriting {len(updates)} cell updates to sheet...")

    # Build batch update request for cell values
    range_data = []
    for u in updates:
        col_letter = _col_letter(u["col"] - 1)
        cell_ref = f"{col_letter}{u['row']}"
        range_data.append({
            "range": f"'{args.worksheet}'!{cell_ref}",
            "values": [[u["value"]]],
        })

    # Write in batches of 500 to avoid quota limits
    BATCH = 500
    for i in range(0, len(range_data), BATCH):
        chunk = range_data[i:i + BATCH]
        ws.spreadsheet.values_batch_update({
            "valueInputOption": "USER_ENTERED",
            "data": chunk,
        })
        logger.info(f"  Wrote batch {i // BATCH + 1} ({len(chunk)} cells)")
        if i + BATCH < len(range_data):
            time.sleep(1)

    # Re-apply colour formatting after price updates
    logger.info("Re-applying colour formatting...")
    _reapply_colours(ws, headers, ws.get_all_values()[1:])

    url = f"https://docs.google.com/spreadsheets/d/{args.sheet_id}/edit"
    logger.info(f"\nDone! View sheet: {url}")


def _reapply_colours(ws, headers, data_rows):
    """Re-run the green/red conditional formatting after a scrape."""
    from push_to_sheets import _resolve_comparisons, build_format_requests
    comparisons = _resolve_comparisons(headers)
    requests_list = build_format_requests(ws.id, headers, data_rows, comparisons)
    if requests_list:
        ws.spreadsheet.batch_update({"requests": requests_list})
        logger.info(f"  Applied {len(requests_list)} colour cells")


if __name__ == "__main__":
    main()
