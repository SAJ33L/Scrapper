# Dental Product Price Benchmarking Scraper

A Python scraper that collects competitor prices for ~300 dental products across 5 websites and outputs a CSV ready for benchmarking/colour-coding in Google Sheets.

---

## What It Does

Reads your input CSV of ~300 dental products and for each product:
1. Uses existing competitor URLs in the CSV to scrape prices directly
2. Falls back to site search when no URL is available
3. Calculates variance (your price vs competitor price)
4. Outputs a new CSV with all prices filled in

---

## Sites Scraped

| Site | Price Currency | Search Method |
|------|---------------|---------------|
| dmi.ie | € (EUR) | HTML search — works without JS |
| dmi.co.uk | £ (GBP) | HTML search — works without JS |
| dentalsky.com | £ (GBP) | Direct URLs only (search needs `--playwright`) |
| dontalia.com | € (EUR) | Direct URLs only (search needs `--playwright`) |
| henryschein.ie | € (EUR) | Requires `--playwright` |

---

## Output Columns

The output CSV contains all original columns plus Henry Schein, which is newly added:

| Column | Description |
|--------|-------------|
| Code | Your internal product code |
| Name | Product name |
| Manufacturer | Brand/manufacturer |
| Part Number | Manufacturer part number |
| Product Group Description | Category |
| Stock Unit Name | Unit of sale (Each, Box 100, etc.) |
| Cost Price | Your cost price |
| Sales Price (£) | Your GBP selling price |
| Sales Price (€) | Your EUR selling price |
| Margin | Your margin % |
| DMI Sales Price (€) | dmi.ie price |
| Variance (DMI IE) | % difference vs your EUR price |
| DMI URL (IE) | dmi.ie product URL |
| DentalSky Sales Price (£) | dentalsky.com price |
| Variance (DentalSky) | % difference vs your GBP price |
| DentalSky URL | dentalsky.com product URL |
| Dontalia Sales Price (€) | dontalia.com price |
| Variance (Dontalia) | % difference vs your EUR price |
| Dontalia URL | dontalia.com product URL |
| DMI Sales Price (£) | dmi.co.uk price |
| Variance (DMI UK) | % difference vs your GBP price |
| DMI URL (UK) | dmi.co.uk product URL |
| Henry Schein Sales Price (€) | henryschein.ie price *(new)* |
| Variance (Henry Schein) | % difference vs your EUR price *(new)* |
| Henry Schein URL | henryschein.ie product URL *(new)* |

> **Variance formula:** `(your price − competitor price) / competitor price × 100`
> Positive = competitor is cheaper than you. Negative = you are cheaper than competitor.

---

## Setup

### Requirements
- Python 3.10+
- pip

### Install

**Option A — run the setup script (Windows):**
```bat
setup.bat
```

**Option B — manual:**
```bash
pip install -r requirements.txt
playwright install chromium   # only needed for --playwright mode
```

---

## Usage

### Basic run (all products, all sites except Henry Schein)
```bash
python scraper.py
```
Reads `Price Benchmarking - Top 300 April 2025 - Public Website Prices.csv` and writes `output_prices.csv`.

---

### Full run including Henry Schein
```bash
python scraper.py --playwright
```
Launches a headless Chromium browser to handle JavaScript-rendered pages (Henry Schein, and search on DentalSky/Dontalia).

---

### Test on a small batch first
```bash
python scraper.py --limit 10
```
Only processes the first 10 rows — useful for testing before a full run.

---

### Specific sites only
```bash
python scraper.py --sites dmi_ie dmi_uk
python scraper.py --sites dentalsky dontalia
python scraper.py --sites henryschein --playwright
```

Available site keys: `dmi_ie`, `dmi_uk`, `dentalsky`, `dontalia`, `henryschein`

---

### Custom input/output files
```bash
python scraper.py --input "my_products.csv" --output "results.csv"
```

---

### Re-scrape products that already have prices
By default the scraper skips any product that already has a price filled in (to save time on re-runs). To force a full re-scrape:
```bash
python scraper.py --no-skip-existing
```

---

### All options
```
python scraper.py --help

  --input FILE        Input CSV file (default: the Price Benchmarking CSV)
  --output FILE       Output CSV file (default: output_prices.csv)
  --sites SITE [...]  Which sites to scrape (default: all 5)
  --limit N           Only process first N rows (for testing)
  --playwright        Enable Playwright for JS-heavy sites
  --no-skip-existing  Re-scrape even where prices already exist
```

---

## How Prices Are Extracted

All five sites embed **schema.org Product structured data** (JSON-LD) in their HTML — this is the primary extraction method and is the most reliable.

```html
<script type="application/ld+json">
{
  "@type": "Product",
  "offers": {
    "price": "27.39",
    "priceCurrency": "EUR"
  }
}
</script>
```

When a product URL is already in the input CSV, the scraper fetches that page and reads the price from this JSON block directly — no fragile CSS selector parsing.

When no URL exists, the scraper falls back to searching each site:

- **dmi.ie / dmi.co.uk** — uses `/categories.html?type=simple&name=QUERY`, takes the first result, then scrapes the product page
- **dentalsky.com / dontalia.com** — search requires JavaScript; without `--playwright` the scraper tries to guess the product URL from the product name slug (e.g. `pegasus-blue-velcro-bib.html`)
- **henryschein.ie** — fully JavaScript-rendered, requires `--playwright` for all operations

---

## Runtime Estimates

The scraper waits 1.5–4 seconds between requests per site to avoid getting blocked.

| Scope | Estimated Time |
|-------|---------------|
| 10 products, 4 sites | ~5–10 minutes |
| 300 products, 4 sites | ~3–6 hours |
| 300 products, all 5 sites (with Playwright) | ~6–10 hours |

**Tip:** Run each site separately in parallel terminal windows to save time:
```bash
# Terminal 1
python scraper.py --sites dmi_ie dmi_uk --output out_dmi.csv

# Terminal 2
python scraper.py --sites dentalsky --output out_dentalsky.csv

# Terminal 3
python scraper.py --sites dontalia --output out_dontalia.csv

# Terminal 4
python scraper.py --sites henryschein --playwright --output out_henryschein.csv
```

Then merge the output CSVs in Excel or Google Sheets.

---

## Logs

Every run writes a `scraper.log` file in the same directory. If something goes wrong or you want to check which products were found/missed, open that file.

Example log output:
```
[1/300] AA009 — Septodont Septoject XL Needles 27g Long
  [dmi.ie] Direct URL ✓ €27.39  (variance: -31.2%)
  [dmi.co.uk] Direct URL ✓ £22.18  (variance: -26.8%)
  [dentalsky] ✗ Not found
  [dontalia] Direct URL ✓ €17.00  (variance: 10.8%)
```

---

## Known Limitations

- **Prices change.** Competitor prices are scraped live at time of running. Re-run regularly to keep data fresh.
- **Wrong variant matches.** If a product URL in the CSV points to a pack (e.g. Box 50) but your product is "Each", the competitor price will look very high. Review rows with variance above ±50% manually.
- **Products not on competitor sites.** Niche or own-brand products may simply not exist on competitor sites — these will show `N/A`.
- **Site changes.** If a competitor redesigns their website, the scraper may need updating. The JSON-LD approach is more robust than CSS selectors, but it can still break.
- **Anti-bot blocking.** Sites occasionally block automated requests. If you see many failures for a previously-working site, try again after a few hours or increase `MIN_DELAY` at the top of `scraper.py`.
