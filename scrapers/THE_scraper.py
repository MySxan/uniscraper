from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import csv
import argparse
import string
import os

BASE_URL = "https://www.timeshighereducation.com"
OUTPUT_FILE = "THE.csv"

def get_names_from_page(driver, url):
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    names = []
    for li in soup.select("li[class*='css-']"):
        a = li.select_one("a.css-4oxae9[href^='/world-university-rankings/']")
        if a and a.get_text(strip=True):
            names.append(a.get_text(strip=True))
    return names

def main(letters):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name"])

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)

    try:
        for letter in letters:
            print(f"=== {letter.upper()} ===")
            page = 1
            last_names = None
            while True:
                url = f"{BASE_URL}/university-directory/{letter}?page={page}"
                names = get_names_from_page(driver, url)
                if not names:
                    break
                if last_names is not None and set(names) == set(last_names):
                    break
                with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    for name in names:
                        writer.writerow([name])
                last_names = names
                page += 1
    finally:
        driver.quit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--letters", type=str, default="A-Z")
    args = parser.parse_args()
    all_letters = list(string.ascii_lowercase)
    if "-" in args.letters:
        start, end = args.letters.split("-")
        start, end = start.lower(), end.lower()
        start_i, end_i = all_letters.index(start), all_letters.index(end)
        letters = all_letters[start_i:end_i + 1]
    else:
        letters = [args.letters.lower()]
    main(letters)
