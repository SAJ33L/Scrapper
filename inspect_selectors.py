from playwright.sync_api import sync_playwright
import urllib.parse
from bs4 import BeautifulSoup

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        
        print("--- DENTALSKY ---")
        try:
            q_ds = "Clinisept"
            url_ds = f"https://www.dentalsky.com/catalogsearch/result/?q={urllib.parse.quote_plus(q_ds)}"
            page.goto(url_ds, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000) # Give scripts time to run
            
            soup_ds = BeautifulSoup(page.content(), "html.parser")
            
            # Print specifically if .product-item-link exists
            item_links = soup_ds.select(".product-item-link")
            print(f"Number of .product-item-link elements: {len(item_links)}")
            
            products = soup_ds.select("li[class*='product-item'], div[class*='product-item-info']")
            if products:
                print(f"Found {len(products)} product containers.")
                for idx, prod in enumerate(products[:3]):
                    link = prod.select_one("a")
                    if link:
                        print(f"  Product {idx} link class: {link.get('class')}")
                        print(f"  Product {idx} link href: {link.get('href')}")
            else:
                print("No general product containers found.")
        except Exception as e:
            print("Failed DentalSky:", e)

        print("\n--- DONTALIA ---")
        try:
            q_dontalia = "Clinisept"
            url_don = f"https://www.dontalia.com/buscar?q={urllib.parse.quote_plus(q_dontalia)}"
            page.goto(url_don, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000)
            
            soup_don = BeautifulSoup(page.content(), "html.parser")
            don_prods = soup_don.select("div[class*='product'], div[data-test='product-card'], article")
            if don_prods:
                print(f"Found {len(don_prods)} product containers on Dontalia.")
                for idx, prod in enumerate(don_prods[:3]):
                    link = prod.select_one("a")
                    if link:
                        print(f"Dontalia Link {idx} class: {link.get('class')}")
                        print(f"Dontalia Link {idx} href: {link.get('href')}")
                        
        except Exception as e:
            print("Failed Dontalia:", e)

main()
