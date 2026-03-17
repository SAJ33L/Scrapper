#!/bin/bash
set -e

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo ""
echo "Installing Playwright browser (needed for Henry Schein and JS-heavy sites)..."
playwright install chromium

echo ""
echo "Setup complete! Run the scraper with:"
echo "  python scraper.py --help"
echo ""
echo "Quick start examples:"
echo "  python scraper.py --limit 10                         (test first 10 rows)"
echo "  python scraper.py                                    (full run, all sites)"
echo "  python scraper.py --playwright                       (include Henry Schein)"
echo "  python scraper.py --sites dmi_ie dmi_uk dentalsky   (specific sites only)"
