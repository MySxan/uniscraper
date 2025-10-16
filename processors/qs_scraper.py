#!/usr/bin/env python3

import sys
import json
import time
import logging
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException

BASE_DIR = Path(__file__).parent.parent
CSV_INPUT = BASE_DIR / "output" / "QS_2026_Rankings.csv"
CSV_OUTPUT = BASE_DIR / "output" / "QS_2026_Rankings_with_Coordinates.csv"
URLS_CACHE = Path(__file__).parent / ".qs_university_urls.json"
PROGRESS_FILE = Path(__file__).parent / ".qs_scraper_progress.json"
STATS_FILE = Path(__file__).parent / ".qs_scraper_stats.json"
LOG_FILE = Path(__file__).parent / "qs_scraper.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class QSScraper:
    BASE_URL = "https://www.topuniversities.com"
    RANKINGS_URL = "https://www.topuniversities.com/world-university-rankings"

    def __init__(self):
        self.driver = None
        self.university_urls = self._load_url_cache()
        self.progress = self._load_progress()
        self.stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "start_time": datetime.now().isoformat(),
        }

    def init_driver(self):
        """initialize Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        self.driver = webdriver.Chrome(options=chrome_options)
        # increase timeouts to reduce renderer timeout errors
        self.driver.set_page_load_timeout(90)
        self.driver.implicitly_wait(5)
        logger.info("WebDriver initialized")

    def fetch_university_page(self, url, retries=0, backoff=2):
        """
        Fetch a university page. Single attempt with 90s timeout.
        Returns True if page loaded, False otherwise.
        On failure, logs warning and returns False (page is skipped).
        """
        try:
            logger.debug(f"  Fetching: {url}")
            self.driver.get(url)

            # wait for body to be present (90s set at driver level)
            WebDriverWait(self.driver, 90).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            # small sleep to let renderer finish
            time.sleep(1)
            return True

        except TimeoutException as e:
            logger.warning(f"  Failed to fetch page (timeout after 90s)")
            return False
        except Exception as e:
            logger.warning(f"  Failed to fetch page: {e}")
            return False

    def find_matching_url(self, csv_name, rank):
        """
        match university name from CSV to cached URLs
        """
        if csv_name in self.university_urls:
            return self.university_urls[csv_name]

        urls_list = list(self.university_urls.items())

        # Handle range ranks like "701-710" by extracting the first number
        if isinstance(rank, str) and "-" in rank:
            try:
                rank = int(rank.split("-")[0])
            except (ValueError, IndexError):
                logger.warning(f"  Could not parse rank range: {rank}")
                return None
        else:
            try:
                rank = int(rank)
            except ValueError:
                logger.warning(f"  Could not convert rank to int: {rank}")
                return None

        if 0 <= rank - 1 < len(urls_list):
            name, url = urls_list[rank - 1]
            logger.debug(f"  Using rank-based match: '{csv_name}' -> '{name}'")
            return url

        return None

    def close_driver(self):
        """close WebDriver and all Chrome processes"""
        if not self.driver:
            return

        try:
            # Give WebDriver a moment to wrap up
            self.driver.quit()
            logger.debug("WebDriver quit successfully")
        except Exception as e:
            logger.debug(f"WebDriver quit error (expected): {type(e).__name__}")
            # Ignore errors during quit - Chrome cleanup will happen automatically
        finally:
            self.driver = None

        # Ensure all Chrome processes are cleaned up
        try:
            import subprocess

            result = subprocess.run(
                ["taskkill", "/F", "/IM", "chrome.exe"], capture_output=True, timeout=2
            )
            if result.returncode == 0:
                logger.debug("Chrome processes cleaned up")
        except Exception as e:
            logger.debug(f"Chrome cleanup: {e}")

    def _load_url_cache(self):
        """load cached URLs"""
        if URLS_CACHE.exists():
            with open(URLS_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_url_cache(self):
        """save URL cache"""
        with open(URLS_CACHE, "w", encoding="utf-8") as f:
            json.dump(self.university_urls, f, ensure_ascii=False, indent=2)

    def _load_progress(self):
        """load progress"""
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_progress(self, name, url, latitude, longitude):
        """save progress"""
        self.progress[name] = {
            "url": url,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": datetime.now().isoformat(),
        }
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)

    def _save_stats(self):
        """save stats"""
        self.stats["end_time"] = datetime.now().isoformat()
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(self.stats, f, ensure_ascii=False, indent=2)

    def extract_university_urls_from_page(self, page_num=0):
        """
        fetch university URLs from a rankings page
        """
        urls = {}

        try:
            # 构建 URL，使用 page 参数
            page_url = f"{self.RANKINGS_URL}?page={page_num}&items_per_page=150&tab=indicators&sort_by=rank&order_by=asc"

            logger.info(f"Loading rankings page {page_num}: {page_url}")
            self.driver.get(page_url)

            # 等待页面加载
            time.sleep(5)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.uni-link"))
                )
            except TimeoutException:
                logger.warning(
                    f"Timeout waiting for university links on page {page_num}"
                )
                return urls

            # 提取所有 a.uni-link 元素的 href 和文本
            uni_links = self.driver.find_elements(By.CSS_SELECTOR, "a.uni-link")
            logger.info(f"Found {len(uni_links)} university links on page {page_num}")

            seen_urls = set()

            for link in uni_links:
                try:
                    href = link.get_attribute("href")
                    text = link.text.strip()

                    if href and text and href not in seen_urls:
                        # 完整 URL
                        full_url = (
                            href
                            if href.startswith("http")
                            else f"{self.BASE_URL}{href}"
                        )

                        # 提取大学名称
                        uni_name = re.sub(r"#.*$", "", text).strip()

                        if uni_name:
                            urls[uni_name] = full_url
                            seen_urls.add(href)
                            logger.debug(f"  {uni_name[:50]}: {full_url}")

                except Exception as e:
                    logger.debug(f"Error processing link: {e}")

            logger.info(
                f"Extracted {len(urls)} unique universities from page {page_num}"
            )
            return urls

        except Exception as e:
            logger.error(f"Failed to extract URLs from page {page_num}: {e}")
            return urls

    def extract_all_university_urls(self):
        """
        scrape all university URLs from all ranking pages
        """
        logger.info("=" * 80)
        logger.info("Phase 1: Extracting University URLs from Rankings Pages")
        logger.info("=" * 80)

        all_urls = {}
        page = 0
        max_pages = 100

        while page < max_pages:
            logger.info(f"\n--- Extracting page {page} ---")
            page_urls = self.extract_university_urls_from_page(page)

            if not page_urls:
                logger.info(f"No universities found on page {page}, reached end")
                break

            logger.info(f"Got {len(page_urls)} universities from page {page}")
            all_urls.update(page_urls)
            page += 1
            delay = 3 + (page % 3)
            logger.info(f"Waiting {delay}s before next page...")
            time.sleep(delay)

        logger.info(f"\nTotal unique universities extracted: {len(all_urls)}")

        # URL 缓存
        self.university_urls = all_urls
        self._save_url_cache()

        return all_urls

    def extract_coordinates(self):
        """
        fetch latitude and longitude from university page
        """
        # data-latitude/data-longitude attributes in <option> elements
        try:
            options = self.driver.find_elements(
                By.CSS_SELECTOR, "option[data-latitude]"
            )
            if options:
                lat = options[0].get_attribute("data-latitude")
                lng = options[0].get_attribute("data-longitude")
                if lat and lng:
                    logger.info(f"  [Method 1] Coordinates: {lat}, {lng}")
                    return float(lat), float(lng)
        except Exception as e:
            logger.debug(f"  failed: {e}")

        logger.warning("  No coordinates found")
        return None, None

    def fetch_university_page(self, url):
        """
        load university page with retry and reinit logic
        """
        max_attempts = 3

        for attempt in range(max_attempts):
            try:
                logger.debug(f"  Fetching: {url} (attempt {attempt+1}/{max_attempts})")
                self.driver.get(url)

                time.sleep(2)
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                except:
                    pass

                logger.debug(f"  Page loaded successfully")
                return True

            except Exception as e:
                error_msg = str(e)

                # Check for invalid session or broken connection
                if (
                    "invalid session" in error_msg.lower()
                    or "connection" in error_msg.lower()
                ):
                    logger.debug(f"  WebDriver session lost: {error_msg}")
                    if attempt < max_attempts - 1:
                        logger.info(f"  Reinitializing WebDriver...")
                        try:
                            self._reinit_driver()
                            continue
                        except Exception as reinit_e:
                            logger.warning(f"  Failed to reinit: {reinit_e}")

                logger.warning(
                    f"  Failed to fetch page (attempt {attempt+1}/{max_attempts}): {error_msg}"
                )

        return False

    def save_to_csv(self, rank, name, region, status, latitude, longitude):
        """save a row to CSV"""
        try:
            new_row = pd.DataFrame(
                {
                    "Rank": [rank],
                    "Name": [name],
                    "Region": [region],
                    "Status": [status],
                    "Latitude": [latitude],
                    "Longitude": [longitude],
                }
            )

            if CSV_OUTPUT.exists():
                df = pd.read_csv(CSV_OUTPUT)
                df = pd.concat([df, new_row], ignore_index=True)
            else:
                df = new_row

            df.to_csv(CSV_OUTPUT, index=False)
            logger.info(f"  Saved to CSV")
            return True
        except Exception as e:
            logger.error(f"  Failed to save CSV: {e}")
            return False

    def _reinit_driver(self):
        """reinit WebDriver after errors"""
        try:
            self.close_driver()
        except:
            pass
        time.sleep(1)
        self.init_driver()
        logger.info("WebDriver reinitialized")

    def run(self):
        """scraping workflow"""
        logger.info("=" * 80)
        logger.info("QS University Ranking Scraper")
        logger.info("=" * 80)

        # Phase 1: check cached URLs and extract if not exist
        if len(self.university_urls) == 0:
            logger.info(
                "\nNo cached URLs found, extracting University URLs from all ranking pages..."
            )
            self.extract_all_university_urls()
        else:
            logger.info(
                f"\nUsing cached URLs ({len(self.university_urls)} universities)"
            )

        # load input CSV
        try:
            df_input = pd.read_csv(CSV_INPUT)
            logger.info(f"\nLoaded {len(df_input)} universities from CSV")
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return False

        # Phase 2: scrape coordinates with CSV order
        logger.info("=" * 80)
        logger.info("Phase 2: Scraping Coordinates")
        logger.info("=" * 80)

        for idx, row in df_input.iterrows():
            rank = row["Rank"]
            name = row["Name"]
            region = row["Region"]
            status = row["Status"]

            # check progress
            if name in self.progress:
                logger.info(f"[{idx+1}/{len(df_input)}] SKIP (cached): {name}")
                continue

            logger.info(f"\n[{idx+1}/{len(df_input)}] Rank {rank} - {name}")

            # check URL
            url = self.find_matching_url(name, rank)

            if not url:
                # skip if URL not found in rankings
                logger.warning(f"  URL not found in cache, skipping")
                self.stats["failed"] += 1
                self.stats["processed"] += 1
                continue

            # fetch page
            if not self.fetch_university_page(url):
                logger.warning(f"  Failed to fetch page")
                self.stats["failed"] += 1
                self.stats["processed"] += 1
                continue

            # fetch coordinates
            lat, lng = self.extract_coordinates()

            if lat is not None and lng is not None:
                # save to CSV with coordinates
                if self.save_to_csv(rank, name, region, status, lat, lng):
                    self._save_progress(name, url, lat, lng)
                    self.stats["success"] += 1
                    logger.info(f"  SUCCESS: {lat:.6f}, {lng:.6f}")
                else:
                    self.stats["failed"] += 1
            else:
                # save to CSV with empty coordinates
                logger.warning(f"  No coordinates found")
                if self.save_to_csv(rank, name, region, status, "", ""):
                    self._save_progress(name, url, None, None)
                    self.stats["failed"] += 1
                    logger.info(f"  SAVED WITH UNKNOWN COORDINATES")
                else:
                    self.stats["failed"] += 1

            self.stats["processed"] += 1

            # random delay
            delay = 3 + (hash(name) % 3)
            time.sleep(delay)

        # save stats
        self._save_stats()

        logger.info("\n" + "=" * 80)
        logger.info(f"Scraping complete!")
        logger.info(f"  Processed: {self.stats['processed']}")
        logger.info(f"  Success: {self.stats['success']}")
        logger.info(f"  Failed: {self.stats['failed']}")
        logger.info("=" * 80)

        return True


def main():
    """entry point"""
    scraper = QSScraper()

    try:
        scraper.init_driver()
        success = scraper.run()
        return 0 if success else 1
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1
    finally:
        scraper.close_driver()


if __name__ == "__main__":
    sys.exit(main())
