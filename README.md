# Dental Product Price Benchmarking Scraper

A Python scraper that collects competitor prices for ~300 dental products across 5 websites, outputs a formatted Excel file, and syncs results to a live Google Sheets master sheet.

---

## What It Does

Reads your input CSV of ~300 dental products and for each product:
1. Uses existing competitor URLs in the CSV to scrape prices directly
2. Falls back to site search when no URL is available — using part number, product name, and competitor product codes (`DMI Code`, `Schein Code`) as search queries
3. Calculates variance (your price vs competitor price)
4. Detects pack size mismatches and calculates adjusted per-unit variance
5. Outputs a formatted `.xlsx` with green/red pricing highlights
6. Optionally uploads results straight to the live Google Sheets master sheet

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

The output CSV preserves all non-site input columns first, then appends site columns in this fixed order (site-by-site blocks):

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
| DMI IE block | `DMI Sales Price (€)`, `Variance (DMI IE)`, `DMI URL (IE)`, `DMI IE Notes`, `DMI IE Product`, `DMI IE Pack Flag`, `DMI IE Adjusted Variance` |
| DMI UK block | `DMI Sales Price (£)`, `Variance (DMI UK)`, `DMI URL (UK)`, `DMI UK Notes`, `DMI UK Product`, `DMI UK Pack Flag`, `DMI UK Adjusted Variance` |
| DentalSky block | `DentalSky Sales Price (£)`, `Variance (DentalSky)`, `DentalSky URL`, `DentalSky Notes`, `DentalSky Product`, `DentalSky Pack Flag`, `DentalSky Adjusted Variance` |
| Dontalia block | `Dontalia Sales Price (€)`, `Variance (Dontalia)`, `Dontalia URL`, `Dontalia Notes`, `Dontalia Product`, `Dontalia Pack Flag`, `Dontalia Adjusted Variance` |
| Henry Schein block | `Henry Schein Sales Price (€)`, `Variance (Henry Schein)`, `Henry Schein URL`, `Henry Schein Notes`, `Henry Schein Product`, `Henry Schein Pack Flag`, `Henry Schein Adjusted Variance` |

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
- Writes results to `output_prices.xlsx`

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
python scraper.py --input "my_products.csv" --output "results.xlsx"
```

---

### 8. Google Sheets integration

The scraper can push results directly to the live Google Sheets master sheet.

**Scrape and upload in one go:**
```bash
python scraper.py --upload-to-sheets
```

**Upload an already-scraped file without re-scraping:**
```bash
python scraper.py --output output_prices.xlsx --upload-only
```

**Upload a specific file:**
```bash
python scraper.py --output my_results.xlsx --upload-only
```

**Upload to a different sheet or worksheet tab:**
```bash
python scraper.py --output output_prices.xlsx --upload-only \
  --sheet-id YOUR_SHEET_ID --worksheet "My Tab Name"
```

> The service account credentials (`bfm-competitor-price-scraper-60bbef18550e.json`) are already configured for the master sheet. No extra auth setup needed.

---

### 9. Live sheet: scraping URLs entered by the team

The team can paste competitor product URLs directly into the Google Sheet (in the `DMI URL (IE)`, `DentalSky URL`, etc. columns). Running the following will scrape any rows where a URL is present but the price is missing:

```bash
python scrape_from_sheet.py
```

**Force re-scrape all URLs even if a price already exists:**
```bash
python scrape_from_sheet.py --force
```

**Include Henry Schein (requires Playwright):**
```bash
python scrape_from_sheet.py --playwright
```

**Target a different sheet or worksheet:**
```bash
python scrape_from_sheet.py --sheet-id YOUR_SHEET_ID --worksheet "My Tab Name"
```

URL columns and the sites they feed:

| URL column | Price column filled |
|------------|-------------------|
| `DMI URL (IE)` | `DMI Sales Price (€)` |
| `DMI URL (UK)` | `DMI Sales Price (£)` |
| `DentalSky URL` | `DentalSky Sales Price (£)` |
| `Dontalia URL` | `Dontalia Sales Price (€)` |
| `Henry Schein URL` | `Henry Schein Sales Price (€)` |

Prices, variances, and green/red highlights are all updated automatically after scraping.

---

### 10. Combining multiple flags
Flags can be combined freely. Examples:

```bash
# Re-scrape first 100 rows on DMI sites only
python scraper.py --sites dmi_ie dmi_uk --no-skip-existing --limit 100

# Full re-scrape of all 5 sites including Henry Schein, then upload
python scraper.py --playwright --no-skip-existing --upload-to-sheets

# Test Henry Schein on 5 rows before committing to a full run
python scraper.py --sites henryschein --playwright --limit 5

# Save output to a separate file without overwriting main results
python scraper.py --sites dentalsky --output dentalsky_results.xlsx --limit 50
```

---

### 11. Run sites in parallel (saves time)
Open 4 terminal windows and run each simultaneously:

```bash
# Terminal 1
python scraper.py --sites dmi_ie dmi_uk --output out_dmi.xlsx

# Terminal 2
python scraper.py --sites dentalsky --output out_dentalsky.xlsx

# Terminal 3
python scraper.py --sites dontalia --playwright --output out_dontalia.xlsx

# Terminal 4
python scraper.py --sites henryschein --playwright --output out_henryschein.xlsx
```

Then merge the 4 output files in Excel or upload each to Google Sheets with `--upload-only`.

---

### All flags reference

#### `scraper.py`

| Flag | Default | Description |
|------|---------|-------------|
| `--input FILE` | Price Benchmarking CSV | Input CSV or XLSX to read products from |
| `--output FILE` | `output_prices.xlsx` | Output file to write results to |
| `--sites SITE [...]` | all 5 sites | Which sites to scrape (`dmi_ie` `dmi_uk` `dentalsky` `dontalia` `henryschein`) |
| `--limit N` | all rows | Only process first N rows |
| `--playwright` | off | Enable headless browser (required for Henry Schein; improves DentalSky/Dontalia search) |
| `--no-skip-existing` | off | Re-scrape even where prices already exist |
| `--upload-to-sheets` | off | Upload output to Google Sheets after scraping finishes |
| `--upload-only` | off | Skip scraping — upload `--output` file directly to Google Sheets |
| `--sheet-id ID` | master sheet ID | Google Sheet ID to upload to |
| `--worksheet NAME` | `Benchmarking Data` | Worksheet tab name inside the Google Sheet |

#### `scrape_from_sheet.py`

| Flag | Default | Description |
|------|---------|-------------|
| `--sheet-id ID` | master sheet ID | Google Sheet ID to read from and write to |
| `--worksheet NAME` | `Benchmarking Data` | Worksheet tab name |
| `--force` | off | Re-scrape all rows with a URL, even if a price already exists |
| `--playwright` | off | Enable Playwright for Henry Schein URL scraping |

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

## Deploying on Railway

Railway runs this project in a containerized runtime. Treat local files as ephemeral: files created during a run (for example `output_prices.csv` and `scraper.log`) may not persist across redeploys/restarts unless you copy them to persistent storage.

### 1) Add deployment files (included in this repo)

- `Procfile` (worker process)
- `railway.toml` (build + start config)

These are set up for this CLI entrypoint:

```bash
python scraper.py --playwright
```

### 2) Create a Railway project from GitHub

1. In Railway, create a new project from `SAJ33L/Scrapper`.
2. Open the service settings and configure it as a **Worker** service (not a web server; no HTTP port required).
3. Confirm start command (or Procfile process) is set to `python scraper.py --playwright`.

If you want explicit file arguments, set the start command to something like:

```bash
python scraper.py --playwright --input "Price Benchmarking - Top 300 April 2025 - Public Website Prices.csv" --output output_prices.csv --sites dmi_ie dmi_uk dentalsky dontalia henryschein --limit 10
```

Remove `--limit` for full production runs.

### 3) Install Playwright browser binaries during build

This project uses Playwright, so Railway must install Chromium and its Linux dependencies at build time:

```bash
python -m playwright install --with-deps chromium
```

`railway.toml` already includes this command in the build step.

### 4) Configure environment variables and secrets

Use Railway Variables/Secrets for any runtime configuration you do not want hardcoded (API keys, webhook URLs, cloud storage credentials, etc.).

- Project/Service → Variables
- Add secret values there, then reference them in code (via `os.environ`) if needed.

This scraper currently works primarily from CLI args (`--input`, `--output`, `--sites`, `--limit`, `--playwright`), so most run configuration is controlled via the Railway start command.

### 5) Scheduling options

For recurring scrapes, use one of these patterns:

- **Railway scheduled trigger / cron-style job** (if available in your Railway plan/workspace): run the scraper command on an interval (hourly, every 6 hours, daily, etc.).
- **Internal loop in worker process**: keep one worker alive and run `scraper.py` repeatedly with a sleep interval.

For long jobs, avoid overlapping runs unless you intentionally want parallel processing.

### 6) Input/output file handling on Railway

- **Input CSV**: ensure the file is available in the container at runtime (commit static input to repo, download from cloud storage at startup, or mount/pull from a persistent source).
- **Output CSV/logs**: do not rely on container local disk for long-term storage. Upload results to a durable destination (for example Google Sheets/Drive, S3-compatible storage, database, or another external store).

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
