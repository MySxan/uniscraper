
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import csv
import time
import re

URL = "https://www.uniranks.com/ranking/verified-universities"
OUTPUT_MAIN = "uniranks_world.csv"
OUTPUT_CN = "uniranks_cn.csv"
CHINA_REGIONS = ["China", "Hong Kong", "Macau", "Taiwan"]

def get_universities(driver, url, writer_main, writer_cn):
    seen_main = set()
    seen_cn = set()
    page = 1
    main_count = 0
    cn_count = 0
    while True:
        print(f"Fetching {page} pages...")
        driver.get(f"{url}?page={page}")
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        page_main = 0
        page_cn = 0
        for div in soup.find_all("div"):
            text = div.get_text(" ", strip=True)
            if "Rank" in text and "Location" in text:
                m = re.match(r"([A-Za-z0-9 .&'\-()]+?) Rank", text)
                name = m.group(1).strip() if m else ""
                loc_match = re.search(r"Location ([^|]+?)(?:\||Recognized)", text)
                location = loc_match.group(1).strip() if loc_match else ""
                if not name or any(x in name for x in ["Rank", "Loading", "Home", "Verified Universities", "Methodology", "Resources", "Awards", "Contact", "Signup", "Login"]):
                    continue
                if not location or any(x in location for x in ["Loading", "Home", "Verified Universities", "Methodology", "Resources", "Awards", "Contact", "Signup", "Login"]):
                    continue
                location_norm = location.strip().lower().replace(" ", "")
                is_china = any(region.lower().replace(" ", "") in location_norm for region in CHINA_REGIONS)
                
                if is_china:
                    if (name, location) not in seen_cn:
                        writer_cn.writerow([name, location])
                        seen_cn.add((name, location))
                        cn_count += 1
                        page_cn += 1 
                else:
                    if (name, location) not in seen_main:
                        writer_main.writerow([name, location])
                        seen_main.add((name, location))
                        main_count += 1
                        page_main += 1                
        print(f"Found {page_cn} Chinese universities on page {page}, total {cn_count} / {main_count}")
        next_link = soup.find("a", string=re.compile(r"^»$"))
        if not next_link:
            break
        page += 1
    return main_count, cn_count

def main():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)
    try:
        with open(OUTPUT_MAIN, "w", newline="", encoding="utf-8") as f_main, \
             open(OUTPUT_CN, "w", newline="", encoding="utf-8") as f_cn:
            writer_main = csv.writer(f_main)
            writer_cn = csv.writer(f_cn)
            writer_main.writerow(["name", "location"])
            writer_cn.writerow(["name", "location"])
            main_count, cn_count = get_universities(driver, URL, writer_main, writer_cn)
        print(f"Written {main_count} non-Chinese universities to {OUTPUT_MAIN}")
        print(f"Written {cn_count} Chinese universities to {OUTPUT_CN}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
