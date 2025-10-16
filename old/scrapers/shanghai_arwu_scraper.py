from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import csv
import time

URL = "https://www.shanghairanking.com/rankings/arwu/2025"
OUTPUT_FILE = "shanghai_arwu.csv"

def get_universities(driver, url):
    import re
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    
    results = set()
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr[data-v-ae1ab4a8]"))
        )
    except Exception:
        time.sleep(2)
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    page_items = soup.select("ul.ant-pagination li.ant-pagination-item")
    total_pages = 1
    if page_items:
        try:
            total_pages = max(int(item.get_text(strip=True)) for item in page_items if item.get_text(strip=True).isdigit())
        except Exception:
            total_pages = 1

    print(f"Detected total {total_pages} pages")

    for page in range(1, total_pages + 1):
        print(f"Fetching {page}/{total_pages} pages...")
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        rows = soup.select("tr[data-v-ae1ab4a8]")
        
        page_count = 0
        for row in rows:
            name_tag = row.select_one("span.univ-name")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            
            country_div = row.select_one("div.region-img")
            if not country_div:
                continue
            
            style = country_div.get("style", "")
            m = re.search(r'/png100/(\w+)\.png', style)
            country_code = m.group(1) if m else ""
            
            if country_code == "cn":
                continue
            
            results.add(name)
            page_count += 1

        print(f"Found {page_count} non-Chinese universities on page {page}, total {len(results)}")
        
        if page < total_pages:
            try:
                next_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "li.ant-pagination-next"))
                )
                next_button.click()
                time.sleep(1.5)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "tr[data-v-ae1ab4a8]"))
                )
            except Exception as e:
                print(f"click next page button failed: {e}")
                break
    
    return sorted(results)

def main():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)

    try:
        universities = get_universities(driver, URL)
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["name"])
            for name in universities:
                writer.writerow([name])
        print(f"Written {len(universities)} non-Chinese universities to {OUTPUT_FILE}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()