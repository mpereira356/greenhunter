import requests
from bs4 import BeautifulSoup

url = "https://betsapi.com/r/10348446/Young-Lions-vs-Tanjong-Pagar-United"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://betsapi.com/"
}

html = requests.get(url, headers=headers, timeout=15).text
soup = BeautifulSoup(html, "html.parser")

tables = soup.find_all("table")

for t in tables:
    for row in t.find_all("tr"):
        cols = [c.get_text(" ", strip=True) for c in row.find_all("td")]
        if len(cols) == 3:
            print(cols)
