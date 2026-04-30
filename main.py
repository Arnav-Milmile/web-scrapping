"""
main.py — Entry point for the Carrier complaint/review scraping pipeline.

Usage:
    python main.py --sources mouthshut
    python main.py --sources mouthshut consumercomplaints
    python main.py --sources all
    python main.py                  # runs all sources
"""

import argparse
import json
import logging
import sys
import os
import re
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.writer import JSONLWriter
from utils.schema import KEYWORDS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            PROJECT_ROOT / "scraper.log",
            mode="a",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)


# Source name → scraper function mapping
AVAILABLE_SOURCES = {
    "mouthshut": "scrapers.mouthshut:scrape_mouthshut",
    "consumercomplaints": "scrapers.consumercomplaints:scrape_consumercomplaints",
    "amazon": "scrapers.amazon:scrape_amazon",
    "flipkart": "scrapers.flipkart:scrape_flipkart",
    "google_maps": "scrapers.google_maps:scrape_google_maps",
    "reddit": "scrapers.reddit:scrape_reddit",
}


def load_config() -> dict:
    """Load configuration from config.json."""
    config_path = PROJECT_ROOT / "config.json"
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    logger.info(f"Loaded config from {config_path}")
    return config


def import_scraper(source_key: str):
    """Dynamically import a scraper function by source key."""
    if source_key not in AVAILABLE_SOURCES:
        logger.error(f"Unknown source: {source_key}")
        return None

    module_path, func_name = AVAILABLE_SOURCES[source_key].split(":")

    try:
        module = __import__(module_path, fromlist=[func_name])
        func = getattr(module, func_name)
        return func
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to import scraper for '{source_key}': {e}")
        return None


def normalize_text_for_dedupe(value: str) -> str:
    """Normalize text for duplicate detection."""
    if not value:
        return ""

    normalized = value.strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"[^\w\s]", "", normalized)
    return normalized


def dedupe_records(records: list[dict]) -> list[dict]:
    """Deduplicate records by normalized text/title/url."""
    seen = set()
    unique = []

    for record in records:
        text_key = normalize_text_for_dedupe(record.get("text", ""))
        title_key = normalize_text_for_dedupe(record.get("title", ""))
        url_key = (record.get("url") or "").strip()

        dedupe_key = (text_key, title_key, url_key)
        if dedupe_key in seen:
            continue

        short_key = (text_key, title_key)
        if short_key in seen:
            continue

        seen.add(dedupe_key)
        seen.add(short_key)
        unique.append(record)

    return unique


def clean_raw_file(source_path: Path, output_path: Path) -> int:
    """Read a raw JSONL file, dedupe its records, and write a cleaned file."""
    if not source_path.exists():
        return 0

    records = []
    with open(source_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning(f"Skipping invalid JSON line in {source_path}: {line[:120]}")

    unique = dedupe_records(records)
    with open(output_path, "w", encoding="utf-8") as f:
        for record in unique:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    return len(unique)


def clean_all_raw_files(config: dict) -> dict[str, int]:
    """Clean all raw JSONL files in the raw directory and write cleaned versions."""
    raw_dir = Path(config["output"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    cleaned_counts = {}
    for raw_file in sorted(raw_dir.glob("*.jsonl")):
        if raw_file.stem.endswith("_cleaned"):
            continue

        cleaned_file = raw_dir / f"{raw_file.stem}_cleaned.jsonl"
        logger.info(f"Cleaning {raw_file.name} → {cleaned_file.name}")
        count = clean_raw_file(raw_file, cleaned_file)
        cleaned_counts[raw_file.stem] = count
        logger.info(f"  {raw_file.name}: {count} unique records")

    if not cleaned_counts:
        logger.info("No raw JSONL files found to clean.")

    return cleaned_counts


def merge_to_final(config: dict):
    """Merge cleaned source files into the final dataset."""
    raw_dir = Path(config["output"]["raw_dir"])
    final_file = Path(config["output"]["final_file"])
    final_file.parent.mkdir(parents=True, exist_ok=True)

    source_files = {}
    for path in sorted(raw_dir.glob("*.jsonl")):
        name = path.stem
        if name.endswith("_cleaned"):
            source_name = name[:-8]
            source_files[source_name] = path
        elif name not in source_files:
            source_files[name] = path

    total = 0
    source_counts = {}
    seen = set()

    with open(final_file, "w", encoding="utf-8") as out:
        for source_name, jsonl_file in source_files.items():
            count = 0
            with open(jsonl_file, "r", encoding="utf-8") as inp:
                for line in inp:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON line in {jsonl_file}")
                        continue

                    merged_key = (
                        normalize_text_for_dedupe(record.get("text", "")),
                        normalize_text_for_dedupe(record.get("title", "")),
                    )
                    if merged_key in seen:
                        continue

                    seen.add(merged_key)
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    total += 1

            source_counts[source_name] = count

    logger.info(f"\nFinal dataset: {final_file}")
    logger.info(f"Total records: {total}")
    for source, count in source_counts.items():
        logger.info(f"  {source}: {count}")

    return total


def log_distribution(source_name: str, output_dir: str):
    """
    Log distribution tracking after a scraper completes.
    Categorises records into AC-related, service-related, other, and unknown.
    """
    filepath = Path(output_dir) / f"{source_name.lower()}.jsonl"
    if not filepath.exists():
        logger.info(f"No output file found for {source_name}")
        return

    total = 0
    ac_related = 0
    service_related = 0
    other = 0
    unknown = 0

    ac_keywords = {"cooling", "gas", "compressor"}
    service_keywords = {"service", "technician", "delay"}

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                total += 1

                kw_set = set(record.get("keywords_detected", []))
                product_type = record.get("product_type", "unknown")

                if kw_set & ac_keywords:
                    ac_related += 1
                elif kw_set & service_keywords:
                    service_related += 1
                elif not kw_set:
                    other += 1

                if product_type == "unknown":
                    unknown += 1

            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON line in {filepath}")

    logger.info(f"\n{'='*50}")
    logger.info(f"DISTRIBUTION — {source_name}")
    logger.info(f"{'='*50}")
    logger.info(f"  Total records:     {total}")
    logger.info(f"  AC-related:        {ac_related} ({ac_related/max(total,1)*100:.1f}%)")
    logger.info(f"  Service-related:   {service_related} ({service_related/max(total,1)*100:.1f}%)")
    logger.info(f"  Other:             {other} ({other/max(total,1)*100:.1f}%)")
    logger.info(f"  Unknown product:   {unknown} ({unknown/max(total,1)*100:.1f}%)")
    logger.info(f"{'='*50}\n")

    # AC bias warning
    if total > 0 and (ac_related / total) > 0.70:
        pct = ac_related / total * 100
        logger.warning(
            f"⚠ AC bias detected ({pct:.1f}%). "
            f"Prioritise Google Maps and Reddit next."
        )


def main():
    parser = argparse.ArgumentParser(
        description="Carrier complaint/review scraping pipeline"
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        default=None,
        help=(
            "Sources to scrape. Options: "
            + ", ".join(AVAILABLE_SOURCES.keys())
            + ", all"
        ),
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean/dedupe individual raw source files before merging",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge raw files into final dataset",
    )

    args = parser.parse_args()

    # Load config
    config = load_config()
    raw_dir = config["output"]["raw_dir"]

    # Ensure output directories exist
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    Path(config["output"]["final_file"]).parent.mkdir(parents=True, exist_ok=True)

    # Handle merge-only mode
    if args.merge and not args.sources and not args.clean:
        merge_to_final(config)
        return

    # Handle clean-only or clean+merge without scraping
    if args.clean and not args.sources:
        clean_all_raw_files(config)
        if args.merge:
            merge_to_final(config)
        return

    # Determine which sources to scrape
    if args.sources is None or "all" in args.sources:
        sources = list(AVAILABLE_SOURCES.keys())
    else:
        sources = [s.lower().strip() for s in args.sources]

    # Validate sources
    for source in sources:
        if source not in AVAILABLE_SOURCES:
            logger.error(f"Unknown source: {source}")
            logger.info(f"Available: {', '.join(AVAILABLE_SOURCES.keys())}")
            sys.exit(1)

    logger.info(f"Pipeline started at {datetime.now().isoformat()}")
    logger.info(f"Sources to scrape: {', '.join(sources)}")

    # Create writer
    writer = JSONLWriter(raw_dir)

    # Run each scraper
    results = {}
    for source in sources:
        logger.info(f"\n{'#'*60}")
        logger.info(f"# Starting source: {source.upper()}")
        logger.info(f"{'#'*60}\n")

        scraper_func = import_scraper(source)
        if scraper_func is None:
            logger.error(f"Skipping {source} — scraper not available")
            results[source] = 0
            continue

        try:
            count = scraper_func(config, writer)
            results[source] = count
            logger.info(f"Source {source} complete: {count} records")

            # Log distribution
            log_distribution(source, raw_dir)

        except Exception as e:
            logger.error(f"Source {source} failed with error: {e}", exc_info=True)
            results[source] = 0

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("PIPELINE SUMMARY")
    logger.info(f"{'='*60}")
    total = 0
    for source, count in results.items():
        logger.info(f"  {source:25s}: {count:>6d} records")
        total += count
    logger.info(f"  {'TOTAL':25s}: {total:>6d} records")
    logger.info(f"{'='*60}")
    logger.info(f"Raw files: {raw_dir}")

    if args.clean:
        logger.info("\nStarting cleanup of raw source files...")
        clean_all_raw_files(config)

    if args.merge:
        logger.info("\nStarting final merge of source files...")
        merge_to_final(config)

    logger.info(f"Pipeline finished at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
