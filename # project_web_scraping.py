from bs4 import BeautifulSoup
import csv
import requests
import time
import random
import numpy as np
import os
import pandas as pd
import glob

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"
}

# --- PRICE RANGES ---
prices_range = np.linspace(0, 1800000, 10).tolist()

# Create session
session = requests.Session()
session.headers.update(headers)


def fetch(url):
    """Safe GET with retry and exponential backoff."""
    for retry in range(7):
        try:
            r = session.get(url, timeout=12)
            if r.status_code == 429:
                wait = 2 ** retry + random.uniform(1, 2)
                print(f"[429] Waiting {wait:.1f}s ...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            wait = 2 ** retry + random.uniform(0.5, 1.5)
            print(f"[ERROR] {e} → retrying in {wait:.1f}s")
            time.sleep(wait)
    print("[FATAL] Page skipped after max retries.")
    return None


# -------------------------
# MAIN LOOP
# -------------------------

output_folder = "C:/Users/Admin/Desktop/scrapped_files"
os.makedirs(output_folder, exist_ok=True)

for price in prices_range:
    minp = int(price)
    maxp = int(price + 200000)

    print(f"\n========== SCRAPING RANGE {minp}–{maxp} ==========\n")

    # List of dictionaries for this price range
    cars_list = []

    page = 1
    while page <= 100:
        url = f"https://auto.drom.ru/mercedes-benz/all/page{page}/?minprice={minp}&maxprice={maxp}"
        print(f"   → Range {minp}-{maxp}, Page {page}")
        response = fetch(url)
        if not response:
            print(f"   × Page {page} failed permanently.\n")
            page += 1
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        car_divs = soup.find_all("div", {"data-ftid": "bulls-list_bull"})
        if not car_divs:
            print("   ✓ No more cars in this range.\n")
            break

        for car_div in car_divs:
            link = car_div.find("a")["href"] if car_div.find("a") else "N/A"

            def get(ftid):
                div = car_div.find(attrs={"data-ftid": ftid})
                if not div:
                    return "N/A"
                return div.get_text(strip=True).replace("\xa0", " ")

            car_data = {
                "Link": link,
                "Title": get("bull_title"),
                "Subtitle": get("bull_subtitle"),
                "Description": get("component_inline-bull-description"),
                "Price": get("bull_price"),
                "Location": get("bull_location")
            }

            cars_list.append(car_data)

        page += 1
        time.sleep(random.uniform(1.0, 3.0))

    # Write this range to CSV
    filename = f"{output_folder}/drom_{minp}_{maxp}.csv"
    df_range = pd.DataFrame(cars_list)
    df_range.to_csv(filename, index=False, encoding="utf-8-sig")  # UTF-8 with BOM for Excel
    print(f"   → Saved {len(cars_list)} cars to {filename}")

# -------------------------
# Merge all CSVs into final file
# -------------------------
files = glob.glob(f"{output_folder}/drom_*.csv")
dfs = [pd.read_csv(f, encoding="utf-8-sig") for f in files]
final = pd.concat(dfs, ignore_index=True)
final.to_csv("C:/Users/Admin/Desktop/drom_final.csv", index=False, encoding="utf-8-sig")

print("\nDONE. Final merged CSV created at Desktop.")