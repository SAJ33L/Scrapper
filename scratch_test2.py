from bs4 import BeautifulSoup
import requests
import json

def test():
    url = "https://www.dentalsky.com/solo-needles-30g-short-100.html"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "lxml")
    
    print("--- Elements containing 'price' in ID or Class ---")
    for el in soup.find_all(lambda t: t.has_attr('class') and any('price' in c.lower() for c in t['class']) or t.has_attr('id') and 'price' in t['id'].lower()):
        print(f"<{el.name} class='{el.get('class', [])}' id='{el.get('id', '')}'> {el.get_text(strip=True)[:100]}")
        
    print("\n--- data-price-amount ---")
    for el in soup.select("[data-price-amount]"):
        print(f"data-price-amount: {el['data-price-amount']} text: {el.get_text(strip=True)[:50]}")

test()
