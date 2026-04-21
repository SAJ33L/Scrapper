import requests
from bs4 import BeautifulSoup
from scraper import _extract_dentalsky_price_from_soup

def test():
    url = "https://www.dentalsky.com/solo-needles-30g-short-100.html"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "lxml")
    
    price, name = _extract_dentalsky_price_from_soup(soup)
    print(f"Extracted Price: {price}")
    print(f"Extracted Name: {name}")

test()
