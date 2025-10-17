#!/usr/bin/env python3

import sys
import json
import time
import logging
import re
from pathlib import Path
from datetime import datetime
from multiprocessing import Process, Queue, current_process
import random
import math

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

BASE_DIR = (
    Path(__file__).parent.parent
    if (Path(__file__).parent.parent.exists())
    else Path(__file__).parent
)
INPUT_CSV = BASE_DIR / "output" / "QS_2026_Rankings.csv"
FINAL_OUTPUT = BASE_DIR / "output" / "QS_2026_Rankings_with_Coordinates.csv"
URLS_CACHE = Path(__file__).parent / ".qs_university_urls.json"

LOG_FILE = Path(__file__).parent / "qs_scraper_parallel.log"
WORKER_OUTPUT_DIR = BASE_DIR / "output"

# Config
NUM_WORKERS = 3  # number of parallel worker processes
WORKER_DELAY_RANGE = (2.0, 5.0)  # seconds, random delay between requests per worker
CHROME_HEADLESS = True
MAX_FETCH_ATTEMPTS = 3
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(processName)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("qs_parallel")


# ---------- helper: load URL cache ----------
def load_url_cache():
    if URLS_CACHE.exists():
        try:
            with open(URLS_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load URL cache: {e}")
            return {}
    return {}


# ---------- worker class ----------
class QSScraperWorker:
    def __init__(self, worker_id, rows, url_cache, output_path, delay_range=(2, 5)):
        self.worker_id = worker_id
        self.rows = rows  # list of (idx, row_series)
        self.url_cache = url_cache
        self.output_path = Path(output_path)
        self.delay_range = delay_range
        self.driver = None
        self._init_driver()

    def _init_driver(self):
        chrome_options = Options()
        if CHROME_HEADLESS:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        # rotate user-agent for this worker
        ua = random.choice(USER_AGENTS)
        chrome_options.add_argument(f"user-agent={ua}")
        # small viewport randomization
        width = random.randint(1000, 1400)
        height = random.randint(700, 1000)
        chrome_options.add_argument(f"--window-size={width},{height}")

        # You can add proxy here per worker if you have proxies:
        # chrome_options.add_argument('--proxy-server=http://<proxy:port>')

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(60)
        self.driver.implicitly_wait(4)
        logger.info(f"[worker {self.worker_id}] WebDriver initialized (UA: {ua})")

    def _close_driver(self):
        if not self.driver:
            return
        try:
            self.driver.quit()
        except Exception as e:
            logger.debug(f"[worker {self.worker_id}] driver quit err: {e}")
        self.driver = None

    def _find_matching_url(self, csv_name, rank):
        # same logic as your original find_matching_url (but simplified)
        if csv_name in self.url_cache:
            return self.url_cache[csv_name]
        # try rank-based fallback
        try:
            if isinstance(rank, str) and "-" in rank:
                rank_num = int(rank.split("-")[0])
            else:
                rank_num = int(rank)
        except Exception:
            return None
        items = list(self.url_cache.items())
        if 0 <= rank_num - 1 < len(items):
            return items[rank_num - 1][1]
        return None

    def fetch_page(self, url):
        for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
            try:
                logger.debug(f"[worker {self.worker_id}] GET {url} (attempt {attempt})")
                self.driver.get(url)
                # small random sleep to mimic human
                time.sleep(random.uniform(1.0, 2.0))
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                return True
            except Exception as e:
                logger.warning(
                    f"[worker {self.worker_id}] fetch attempt {attempt} failed: {e}"
                )
                # try reinit driver on certain failures
                if attempt < MAX_FETCH_ATTEMPTS:
                    try:
                        self._close_driver()
                        time.sleep(1 + random.random())
                        self._init_driver()
                    except Exception as re:
                        logger.warning(f"[worker {self.worker_id}] reinit failed: {re}")
                else:
                    return False
        return False

    def extract_coordinates(self):
        """try methods 1 and 2 (same as your original)"""
        # Method 1
        try:
            links = self.driver.find_elements(By.CSS_SELECTOR, 'a[onclick*="openMap"]')
            for link in links:
                onclick = link.get_attribute("onclick")
                if onclick and "openMap" in onclick:
                    match = re.search(r"openMap\(([-\d.]+),\s*([-\d.]+)\)", onclick)
                    if match:
                        lat, lng = match.groups()
                        return float(lat), float(lng)
        except Exception:
            pass

        # Method 2
        try:
            options = self.driver.find_elements(
                By.CSS_SELECTOR, "option[data-latitude]"
            )
            for opt in options:
                lat = opt.get_attribute("data-latitude")
                lng = opt.get_attribute("data-longitude")
                if lat and lng:
                    return float(lat), float(lng)
        except Exception:
            pass

        return None, None

    def worker_loop(self):
        # prepare output CSV DF
        out_rows = []
        for idx, row in self.rows:
            rank = row.get("Rank")
            name = row.get("Name")
            region = row.get("Region", "")
            status = row.get("Status", "")

            logger.info(f"[worker {self.worker_id}] Processing: {name} (idx {idx})")

            # find url
            url = self._find_matching_url(name, rank)
            if not url:
                logger.warning(
                    f"[worker {self.worker_id}] URL not found for {name}, skipping"
                )
                out_rows.append(
                    {
                        "Rank": rank,
                        "Name": name,
                        "Region": region,
                        "Status": status,
                        "Latitude": "",
                        "Longitude": "",
                    }
                )
                # small delay
                time.sleep(random.uniform(*self.delay_range))
                continue

            ok = self.fetch_page(url)
            if not ok:
                logger.warning(f"[worker {self.worker_id}] Failed to fetch {url}")
                out_rows.append(
                    {
                        "Rank": rank,
                        "Name": name,
                        "Region": region,
                        "Status": status,
                        "Latitude": "",
                        "Longitude": "",
                    }
                )
                time.sleep(random.uniform(*self.delay_range))
                continue

            lat, lng = self.extract_coordinates()
            if lat is not None and lng is not None:
                logger.info(
                    f"[worker {self.worker_id}] Got coords for {name}: {lat:.6f},{lng:.6f}"
                )
                out_rows.append(
                    {
                        "Rank": rank,
                        "Name": name,
                        "Region": region,
                        "Status": status,
                        "Latitude": lat,
                        "Longitude": lng,
                    }
                )
            else:
                logger.warning(f"[worker {self.worker_id}] No coords for {name}")
                out_rows.append(
                    {
                        "Rank": rank,
                        "Name": name,
                        "Region": region,
                        "Status": status,
                        "Latitude": "",
                        "Longitude": "",
                    }
                )

            # random human-like delay
            time.sleep(random.uniform(*self.delay_range))

        # write worker-specific output file
        try:
            if len(out_rows) > 0:
                df_out = pd.DataFrame(out_rows)
                self.output_path.parent.mkdir(parents=True, exist_ok=True)
                df_out.to_csv(self.output_path, index=False)
                logger.info(f"[worker {self.worker_id}] wrote {self.output_path}")
        except Exception as e:
            logger.error(f"[worker {self.worker_id}] failed to write output: {e}")

        # cleanup
        self._close_driver()


# ---------- function: split rows into chunks ----------
def chunk_rows(df, n_chunks):
    rows = list(df.iterrows())
    total = len(rows)
    chunk_size = math.ceil(total / n_chunks)
    chunks = [rows[i * chunk_size : (i + 1) * chunk_size] for i in range(n_chunks)]
    return chunks


# ---------- worker process target ----------
def worker_process(worker_id, rows, url_cache, out_file, delay_range):
    # Convert rows from tuples with index to pandas Series (already are)
    try:
        scraper = QSScraperWorker(
            worker_id, rows, url_cache, out_file, delay_range=delay_range
        )
        scraper.worker_loop()
    except Exception as e:
        logger.exception(f"[worker {worker_id}] unexpected error: {e}")


# ---------- main ----------
def main():
    logger.info("Starting parallel QS scraper")
    if not INPUT_CSV.exists():
        logger.error(f"Input CSV not found: {INPUT_CSV}")
        return 1

    # load CSV
    try:
        df = pd.read_csv(INPUT_CSV)
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}")
        return 1

    # load url cache (extracted previously by your script)
    url_cache = load_url_cache()
    if not url_cache:
        logger.warning(
            "University URL cache empty — consider running extract_all_university_urls first"
        )

    # split rows into chunks
    num_workers = min(NUM_WORKERS, max(1, len(df)))
    chunks = chunk_rows(df, num_workers)

    processes = []
    worker_out_files = []
    for i, chunk in enumerate(chunks):
        worker_id = i + 1
        out_file = WORKER_OUTPUT_DIR / f"qs_out_worker_{worker_id}.csv"
        worker_out_files.append(out_file)
        p = Process(
            target=worker_process,
            args=(worker_id, chunk, url_cache, out_file, WORKER_DELAY_RANGE),
            name=f"Worker-{worker_id}",
        )
        p.start()
        processes.append(p)
        time.sleep(1.0)

    # wait for workers
    for p in processes:
        p.join()

    # merge outputs
    dfs = []
    for f in worker_out_files:
        if f.exists():
            try:
                dfs.append(pd.read_csv(f))
            except Exception as e:
                logger.warning(f"Failed to read {f}: {e}")

    if dfs:
        df_merged = pd.concat(dfs, ignore_index=True)
        df_merged = df_merged.drop_duplicates(subset=["Name"], keep="first")
        FINAL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        df_merged.to_csv(FINAL_OUTPUT, index=False)
        logger.info(f"Final output written to {FINAL_OUTPUT} ({len(df_merged)} rows)")
    else:
        logger.warning("No worker outputs found; nothing to merge")

    logger.info("All done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
