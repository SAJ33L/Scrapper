# Dental Product Price Benchmarking Scraper

A Python scraper that collects competitor prices for ~300 dental products across 5 websites and outputs a CSV ready for benchmarking/colour-coding in Google Sheets.

---

## What It Does

Reads your input CSV of ~300 dental products and for each product:
1. Uses existing competitor URLs in the CSV to scrape prices directly
2. Falls back to site search when no URL is available — using part number, product name, and competitor product codes (`DMI Code`, `Schein Code`) as search queries
3. Calculates variance (your price vs competitor price)
4. Detects pack size mismatches and calculates adjusted per-unit variance
5. Outputs a new CSV with all prices, flags, and competitor product names filled in

A companion **`Competitor Product Code Comparisons.xlsx`** spreadsheet is included in the repository, mapping your products to each competitor's own product codes for reference.

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

## Competitor Product Code Comparisons

The repository includes a **`Competitor Product Code Comparisons.xlsx`** spreadsheet that maps your internal product codes to the equivalent codes used by each competitor site. These codes are embedded in the input CSV and used by the scraper as additional search terms to improve match accuracy:

| Column | Used By |
|--------|---------|
| `DD Code` | Your internal reference code (Dontalia/DentalSky) |
| `DMI Code` | DMI's own product code — passed as a search query on dmi.ie / dmi.co.uk |
| `Schein Code` | Henry Schein's product code — used when searching henryschein.ie |

When the scraper cannot find a product via part number or product name, it falls back to trying the competitor's own code as a search query — significantly reducing "Not found" results.

---

## Output Columns

The output CSV preserves all input columns and adds the following per site:

| Column | Description |
|--------|-------------|
| Code | Your internal product code |
| Name | Product name |
| Product Group | Category |
| Stock Unit | Unit of sale (Each, Box 100, etc.) |
| Part Number | Manufacturer part number |
| DD Code | Dontalia/DentalSky reference code (from input) |
| DMI Code | DMI's product code (from input) |
| Schein Code | Henry Schein's product code (from input) |
| *Site* Sales Price (€/£) | Scraped competitor price |
| Variance (*Site*) | % difference vs your price |
| *Site* URL | Competitor product URL |
| *Site* Product | Competitor product title (for pack size verification) |
| *Site* Pack Flag | `MATCH` / `MISMATCH (ours:100 theirs:50)` / `UNKNOWN` |
| *Site* Adjusted Variance | Per-unit variance when pack sizes differ |
| *Site* Notes | `Not listed on competitor` when product not found |
| *(above repeated for each site: DMI IE, DMI UK, DentalSky, Dontalia, Henry Schein)* | |

> **Variance formula:** `(your price − competitor price) / competitor price × 100`
> Positive = competitor is cheaper than you. Negative = you are cheaper than competitor.

> **Adjusted Variance** is only filled when a pack size mismatch is detected — it recalculates variance on a per-unit basis so you're comparing like for like.

---

## Setup

### Requirements
- Python 3.10+
- pip

### Install

**Linux/Mac:**
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium   # only needed for --playwright mode
```

**Windows:**
```bat
setup.bat
```

---

## Usage

### 1. Activate the virtual environment first
```bash
source .venv/bin/activate       # Linux/Mac
.venv\Scripts\activate          # Windows
```

---

### 2. Basic run (all products, all sites except Henry Schein)
```bash
python scraper.py
```
- Reads the default input CSV
- Skips products that already have prices (faster re-runs)
- Writes results to `output_prices.csv`

---

### 3. Full run including Henry Schein
```bash
python scraper.py --playwright
```
Launches a headless Chromium browser to handle JavaScript-rendered pages (Henry Schein, and proper search on DentalSky/Dontalia).

---

### 4. Test on a small batch first (recommended)
```bash
python scraper.py --limit 10
```
Only processes the first 10 rows. Always do this before a full run to check everything is working.

---

### 5. Re-scrape everything including existing prices
By default the scraper skips rows that already have a price. Use this to force a full fresh scrape:
```bash
python scraper.py --no-skip-existing
```

Combined with a limit (recommended for testing):
```bash
python scraper.py --no-skip-existing --limit 50
```

---

### 6. Specific sites only
```bash
python scraper.py --sites dmi_ie
python scraper.py --sites dmi_ie dmi_uk
python scraper.py --sites dentalsky dontalia
python scraper.py --sites henryschein --playwright
```

Available site keys: `dmi_ie`, `dmi_uk`, `dentalsky`, `dontalia`, `henryschein`

---

### 7. Custom input/output files
```bash
python scraper.py --input "my_products.csv" --output "results.csv"
```

---

### 8. Combining multiple flags
Flags can be combined freely. Examples:

```bash
# Re-scrape first 100 rows on DMI sites only
python scraper.py --sites dmi_ie dmi_uk --no-skip-existing --limit 100

# Full re-scrape of all 5 sites including Henry Schein
python scraper.py --playwright --no-skip-existing

# Test Henry Schein on 5 rows before committing to a full run
python scraper.py --sites henryschein --playwright --limit 5

# Save output to a separate file without overwriting main results
python scraper.py --sites dentalsky --output dentalsky_results.csv --limit 50
```

---

### 9. Run sites in parallel (saves time)
Open 4 terminal windows and run each simultaneously:

```bash
# Terminal 1
python scraper.py --sites dmi_ie dmi_uk --output out_dmi.csv

# Terminal 2
python scraper.py --sites dentalsky --output out_dentalsky.csv

# Terminal 3
python scraper.py --sites dontalia --playwright --output out_dontalia.csv

# Terminal 4
python scraper.py --sites henryschein --playwright --output out_henryschein.csv
```

Then merge the 4 output CSVs in Excel or Google Sheets.

---

### All flags reference

| Flag | Default | Description |
|------|---------|-------------|
| `--input FILE` | Price Benchmarking CSV | Input CSV file to read products from |
| `--output FILE` | `output_prices.csv` | Output CSV file to write results to |
| `--sites SITE [...]` | all 5 sites | Which sites to scrape |
| `--limit N` | all rows | Only process first N rows |
| `--playwright` | off | Enable headless browser (required for Henry Schein; improves DentalSky/Dontalia search) |
| `--no-skip-existing` | off | Re-scrape even where prices already exist |

---

## Understanding the Output

### Variance column
- **Negative** (e.g. `-20%`) = you are cheaper than the competitor
- **Positive** (e.g. `+30%`) = competitor is cheaper than you

### Pack Flag column
| Value | Meaning |
|-------|---------|
| `MATCH` | Both products are the same pack size — variance is a true price difference |
| `MISMATCH (ours:1 theirs:50)` | Different pack sizes — check Adjusted Variance instead |
| `UNKNOWN` | Pack size could not be read from one or both product names — verify manually |

### Adjusted Variance column
Only filled when a `MISMATCH` is detected. Recalculates variance on a per-unit basis.

> **Example:** AA105 BD Venflon Catheter shows `-98%` raw variance (looks like we're massively cheaper), but the competitor is selling a Pack of 50. The Adjusted Variance shows `-0.6%` — prices are almost identical per unit.

---

## How Prices Are Extracted

All five sites embed **schema.org Product structured data** (JSON-LD) in their HTML — this is the primary extraction method and is the most reliable.

```html
<script type="application/ld+json">
{
  "@type": "Product",
  "name": "Septoject XL Needles Box100",
  "offers": {
    "price": "27.39",
    "priceCurrency": "EUR"
  }
}
</script>
```

The scraper reads both the `price` and the `name` from this block — the name is used to detect pack size mismatches.

When no URL exists, the scraper falls back to searching each site:

- **dmi.ie / dmi.co.uk** — uses `/categories.html?type=simple&name=QUERY`, takes the first result
- **dentalsky.com / dontalia.com** — search requires JavaScript; without `--playwright` the scraper tries to guess the URL from the product name slug
- **henryschein.ie** — fully JavaScript-rendered, requires `--playwright` for all operations

---

## Runtime Estimates

The scraper waits 1.5–4 seconds between requests per site to avoid getting blocked.

| Scope | Estimated Time |
|-------|---------------|
| 10 products, 4 sites | ~5–10 minutes |
| 100 products, 4 sites | ~45–90 minutes |
| 300 products, 4 sites | ~3–6 hours |
| 300 products, all 5 sites (with Playwright) | ~6–10 hours |

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
  [dmi_ie] Pack size mismatch: ours=100 theirs=50 → adjusted variance: -5.1%
```

---

## Known Limitations

- **Prices change.** Competitor prices are scraped live at time of running. Re-run regularly to keep data fresh.
- **UNKNOWN pack flags.** If neither product name contains a clear pack size indicator, the flag will show `UNKNOWN` — these need a manual check.
- **Products not on competitor sites.** Niche or own-brand products may simply not exist on competitor sites — these will show `N/A`.
- **Site changes.** If a competitor redesigns their website, the scraper may need updating. The JSON-LD approach is more robust than CSS selectors, but it can still break.
- **Anti-bot blocking.** Sites occasionally block automated requests. If you see many failures for a previously-working site, try again after a few hours or increase `MIN_DELAY` at the top of `scraper.py`.
