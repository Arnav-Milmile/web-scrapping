# Carrier HVAC Complaint Scraper

A simple web scraping pipeline built to generate a custom dataset for a promptathon, as there was no existing dataset specifically focused on HVAC complaints.

## Supported Sources
- **MouthShut** (`mouthshut`)
- **ConsumerComplaints.in** (`consumercomplaints`)
- **Google Maps** (`google_maps`)
- **Reddit** (`reddit`)

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```
   *(Note: Set your Reddit API credentials in `config.json` before running the Reddit scraper).*

2. **Run scrapers:**
   ```bash
   # Run a specific source
   python main.py --sources mouthshut
   
   # Run all sources
   python main.py
   ```

3. **Clean duplicate source records:**
   ```bash
   python main.py --clean
   ```
   This will deduplicate each source file and write cleaned files next to the raw outputs.

4. **Merge the final dataset:**
   ```bash
   python main.py --merge
   ```
   This will combine cleaned files (or raw files if cleaned versions are missing) into `data/final/carrier_dataset.jsonl`.

5. **Run all steps in one command:**
   ```bash
   python main.py --sources all --clean --merge
   ```
   This scrapes all sources, cleans duplicates, and merges the dataset.
