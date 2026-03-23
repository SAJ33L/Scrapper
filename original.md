The websites to scrape FYI would be:
dmi.ie
henryschein.ie
dentalsky.com
dontalia.com
The list of products is c. 300:
https://docs.google.com/spreadsheets/d/1okOIrz2LV8jDKHyWu7KDxGTf_27_8HKQuAEB9mhn8Hs/edit?usp=sharing

We want to scrape the pricing of 100s of products on 4 competitors' websites, and pull this into a google sheet for colour-coded benchmarking. We have an existing matrix of competitors' product codes to our own products. Is this something you might be able to set up and implement (maybe requires python)?


Yes, I can help you set up a web scraping solution to extract pricing information from the specified websites and populate it into a Google Sheet.
To achieve this, we can use Python with libraries such as `requests` for making HTTP requests, `BeautifulSoup` for parsing HTML content, and `gspread` for interacting with Google Sheets. Below is a high-level outline of the steps involved:
1. **Identify Product URLs**: We will need to identify the URLs for each product on the competitors' websites. This can be done by analyzing the structure of the websites and creating a mapping of product codes to their respective URLs.
2. **Scrape Pricing Information**: For each product URL, we will send a request to the webpage, parse the HTML content, and extract the pricing information.
3. **Update Google Sheet**: Once we have the pricing information, we will use the `gspread` library to update the corresponding cells in the Google Sheet with the scraped prices. We can also apply color-coding based on the price comparison to make it visually easier to benchmark against competitors
4. **Automation**: We can set up a scheduled task (e.g., using `cron` on Linux or Task Scheduler on Windows) to run the scraping script at regular intervals to keep the pricing information up-to-date.
Here is a basic example of how the scraping and Google Sheets integration might look in Python:

```pythonimport requests
from bs4 import BeautifulSoup
import gspread

python scraper.py --input "Top 300 (2).xlsx" --output "test.csv"  --sites dmi_ie --no-skip-existing --limit 100