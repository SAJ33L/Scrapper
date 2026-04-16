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
            
            links = []
            for a in soup_ds.select("a"):
                href = a.get("href", "")
                if href and "clinisept" in href.lower() and ".html" in href.lower():
                    links.append((href, a.get("class")))
            print(f"Found {len(links):} links with 'clinisept' and '.html'")
            for link in links[:5]:
                print(link)
        except Exception as e:
            print("Failed DentalSky:", e)

        print("\n--- DONTALIA ---")
        try:
            q_dontalia = "Clinisept"
            url_don = f"https://www.dontalia.com/buscar?q={urllib.parse.quote_plus(q_dontalia)}"
            page.goto(url_don, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(8000)
            
            soup_don = BeautifulSoup(page.content(), "html.parser")
            
            links = []
            for a in soup_don.select("a"):
                href = a.get("href", "")
                if href and "clinisept" in href.lower() and ("/" in href or ".htm" in href):
                    links.append((href, a.get("class")))
            print(f"Found {len(links)} links with 'clinisept' on Dontalia")
            for link in links[:5]:
                print(link)
                        
        except Exception as e:
            print("Failed Dontalia:", e)

main()
