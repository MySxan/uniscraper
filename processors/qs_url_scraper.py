#!/usr/bin/env python3
import json
import time
import re
import logging
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException

BASE_DIR = Path(__file__).parent
OUTPUT_JSON = BASE_DIR / "qs_university_urls.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

class QSUrlScraper:
    BASE_URL = "https://www.topuniversities.com"
    RANKINGS_URL = "https://www.topuniversities.com/world-university-rankings"

    def __init__(self):
        self.driver = None
        self.university_urls = {}

    def init_driver(self):
        """Initialize Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(60)
        self.driver.implicitly_wait(5)
        logger.info("WebDriver initialized")

    def close_driver(self):
        """Close WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    def extract_university_urls_from_page(self, page_num=0):
        """Extract all university names and URLs from a single QS ranking page"""
        urls = {}
        page_url = f"{self.RANKINGS_URL}?page={page_num}&items_per_page=150"
        logger.info(f"Loading page {page_num}: {page_url}")

        try:
            self.driver.get(page_url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.uni-link"))
            )
            time.sleep(2)

            links = self.driver.find_elements(By.CSS_SELECTOR, "a.uni-link")
            logger.info(f"Found {len(links)} universities on page {page_num}")

            for link in links:
                try:
                    href = link.get_attribute("href")
                    name = link.text.strip()
                    if not href or not name:
                        continue
                    if not href.startswith("http"):
                        href = f"{self.BASE_URL}{href}"
                    name = re.sub(r"#.*$", "", name).strip()
                    urls[name] = href
                except Exception:
                    continue

            return urls

        except TimeoutException:
            logger.warning(f"Timeout on page {page_num}")
        except Exception as e:
            logger.warning(f"Failed to extract page {page_num}: {e}")

        return {}

    def extract_all_university_urls(self, max_pages=100):
        """Extract university URLs from all QS ranking pages"""
        all_urls = {}
        for page in range(max_pages):
            logger.info(f"=== Page {page} ===")
            urls = self.extract_university_urls_from_page(page)
            if not urls:
                logger.info("No more universities found. Stopping.")
                break
            all_urls.update(urls)
            time.sleep(2 + (page % 3))  # small delay to be polite
        return all_urls

    def run(self):
        """Main runner"""
        logger.info("Starting QS University URL Scraper...")
        self.init_driver()

        try:
            urls = self.extract_all_university_urls()
            logger.info(f"Total universities extracted: {len(urls)}")

            with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
                json.dump(urls, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved URLs to {OUTPUT_JSON}")
        finally:
            self.close_driver()

def main():
    scraper = QSUrlScraper()
    scraper.run()

if __name__ == "__main__":
    main()
