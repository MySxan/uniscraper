# University Scrapers Project

This project scrapes data from multiple university ranking websites, merges and deduplicates the data, and generates categorized CSV files.

## Usage

### 1. Run Web Scrapers

```bash
# Navigate to scrapers directory
cd scrapers

# Run individual scrapers
python shanghai_arwu_scraper.py
python THE_scraper.py
python uniranks_scraper.py
```

### 2. Merge Data

```bash
cd scripts
python merge_uni.py
```

**Output**: `data/merged_universities.csv`

### 3. Generate Categorized Data

```bash
# Generate non-China region universities
python filter_non_china.py

# Generate universities with unknown location
python filter_unknown.py
```

### merged_universities.csv

Contains complete information for all universities:

- `name`: University name
- `country`: Country/Region
- `sources`: Data sources

### universities_non_china.csv

Contains only non-China region universities (excludes China, Hong Kong, Macau, Taiwan):

- `name`: University name
- `country`: Country/Region

### universities_unknown.csv

Contains only universities with unknown location:

- `name`: University name
- `country`: "Unknown"

## Key Features

### Name Normalization

The merge script includes advanced name normalization to improve deduplication accuracy:

- **Lowercase conversion**: Standardizes all names to lowercase
- **Accent removal**: Converts accented characters (é→e, ü→u, ã→a, ô→o, ç→c)
- **Special character removal**: Removes commas, parentheses, hyphens, etc.
- **Whitespace normalization**: Merges multiple spaces into single space

## Notes

1. Web scrapers require Chrome browser and ChromeDriver
2. Fuzzy matching threshold is set to 90%, adjustable in `merge_uni.py`
3. All paths use relative paths, ensure scripts are run from the `scripts/` directory

## Disclaimer

This project is for educational and research purposes only.
It scrapes only publicly available data from <www.shanghairanking.com>, <www.timeshighereducation.com>, and <www.uniranks.com>.
