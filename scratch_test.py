from bs4 import BeautifulSoup
import requests
import re
import json

def test():
    url = "https://www.dentalsky.com/solo-supra-needles-30g-short-100.html"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "lxml")
    
    # Check LD+JSON
    for el in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(el.string)
            if "offers" in data or (isinstance(data, list) and len(data) > 0 and "offers" in data[0]):
                print("LD+JSON:", data)
        except:
            pass

    # Check HTML selectors
    for sel in [
        "[data-price-type='finalPrice'] .price",
        ".special-price .price",
        ".regular-price .price",
        ".price-box .price",
        "[itemprop='price']",
    ]:
        el = soup.select_one(sel)
        if el:
            content = el.get("content") or el.get_text(" ", strip=True)
            print("HTML Selector", sel, "->", content)

test()
