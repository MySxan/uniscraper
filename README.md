# University Rankings Data Scraper

Multi-source web scraper for collecting university ranking data and geographic coordinates from various ranking platforms.

## Structure

```
/processors
    merge_rankings.py         # Merge rankings from QS, THE, USNews with deduplication
    qs_url_scraper.py         # Extract URLs from QS rankings
    qs_coordinate_scraper.py  # Extract coordinates for QS universities
    qs_processor.py           # Process QS Excel data to CSV
    the_processor.py          # Process THE rankings
    usnews_lac_processor.py   # Process US News LAC rankings
    .qs_university_urls.json  # Cached QS university URLs
/raw                          # Raw data files
/output                       # Output CSV files
/old                          # Old versions of scripts
requirements.txt              # Python dependencies
README.md
```

## Data Sources

- QS World University Rankings 2026 <https://www.topuniversities.com/world-university-rankings>

- THE World University Rankings 2025 <https://www.timeshighereducation.com/world-university-rankings>

- US News Best National Liberal Arts Colleges Rankings <https://www.usnews.com/best-colleges/rankings/national-liberal-arts-colleges>

## Setup

### Prerequisites

- Python 3.8+
- Chrome browser
- ChromeDriver

### Installation

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### 1. Process QS Rankings Data

```bash
cd processors

# Convert Excel to CSV with country/region data
python qs_processor.py
# Output: output/QS_2026_Rankings.csv

# Extract university URLs from ranking website
python qs_url_scraper.py
# Output: Cached in .qs_university_urls.json

# Extract coordinates for each university
python qs_coordinate_scraper.py
# Output: output/QS_2026_Rankings_with_Coordinates.csv
```

### 2. Process THE Rankings

```bash
python the_processor.py
```

### 3. Process US News LAC Rankings

```bash
python usnews_lac_processor.py
```

### 4. Merge Rankings Data

Combine rankings from all three sources into a unified dataset with intelligent deduplication:

```bash
python merge_rankings.py
# Output:
# output/merged_rankings.csv
# processors/merge.log
```

**Zero False Positives**: Cornell University ≠ Cornell College (kept separate)

**Smart Filtering**: University of South Alabama (USA) ≠ University of Georgia (no false merge)

**"The " Prefix Handling**: "The University of Georgia" matches "University of Georgia"

## Output Files

### QS_2026_Rankings.csv

- `Rank`: University ranking position
- `Name`: University name
- `Region`: Country or Territory
- `Status`: Public or Private institution

### QS_2026_Rankings_with_Coordinates.csv

- `Rank`: University ranking
- `Name`: University name
- `Region`: Country or Territory
- `Status`: Institution type
- `Latitude`: Geographic latitude (empty if not found)
- `Longitude`: Geographic longitude (empty if not found)

### merged_rankings.csv

Combined rankings from QS, THE, and USNews with intelligent deduplication:

- `Name`: University name
- `Country`: Country or Territory
- `QS_Rank`: QS 2026 ranking (if available)
- `THE_Rank`: THE 2025 ranking (if available)
- `USNews_Rank`: US News LAC ranking (if available)
- `Nature_of_Running`: Public or Private institution
- `Latitude`: Geographic latitude (from QS data)
- `Longitude`: Geographic longitude (from QS data)

## Logging

Detailed logs are saved to:

- `processors/qs_scraper.log` - Main scraper operations
- `processors/qs_scraper_parallel.log` - Parallel processing logs
- `processors/merge_log.txt` - Merge and deduplication operations

Logs include:

- URL extraction progress
- Coordinate extraction success/failure
- WebDriver session management
- Data processing statistics
- **Merge Log:**
  - Total input/output records for each merge round
  - Duplicate group count and merging decisions
  - Specific university pairs that were consolidated
  - Field consolidation details (which fields were filled from duplicate records)

## Disclaimer

This project is for educational and research purposes only. It scrapes only publicly available data from:

- <https://www.topuniversities.com/>
- <https://www.timeshighereducation.com/>
- <https://www.usnews.com/>

Please respect website terms of service and robots.txt when using this scraper.
