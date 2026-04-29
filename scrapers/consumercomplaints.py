import logging
import random
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

from utils.schema import create_record, passes_inclusion_rule
from utils.writer import JSONLWriter
from utils.rate_limiter import fetch_with_retry, random_delay

logger = logging.getLogger(__name__)

SEARCH_URLS = [
    "https://www.consumercomplaints.in/?search=carrier+ac",
    "https://www.consumercomplaints.in/?search=carrier+air+conditioner",
    "https://www.consumercomplaints.in/?search=ac+not+cooling",
    "https://www.consumercomplaints.in/?search=split+ac+compressor",
    "https://www.consumercomplaints.in/?search=ac+service+complaint",
    "https://www.consumercomplaints.in/?search=inverter+ac+problem+india",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
}


def _create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _get_rate_limits(config: dict) -> tuple[list[int], int]:
    rate_config = config.get("rate_limits", {}).get("consumercomplaints", {})
    delay = rate_config.get("delay_seconds", [2, 4])
    max_pages = rate_config.get("max_pages", 10)
    return delay, max_pages


def _fetch_page(session: requests.Session, url: str, delay_range: list[int]) -> Optional[BeautifulSoup]:
    response = fetch_with_retry(url, session=session, delay_range=tuple(delay_range), headers=HEADERS)
    if response is None:
        logger.warning(f"Failed to fetch consumercomplaints page: {url}")
        return None
    return BeautifulSoup(response.text, "html.parser")


def _extract_complaint_blocks(soup: BeautifulSoup) -> List[BeautifulSoup]:
    selectors = [
        "div.white-box.complaint-box-results",
        "div.complaint-box-results",
        "div[class*='complaint']",
    ]
    for selector in selectors:
        blocks = soup.select(selector)
        if blocks:
            return blocks
    return []


def _parse_complaint_block(block: BeautifulSoup, page_url: str) -> Optional[dict]:
    text_el = block.select_one("div.complaint-box-results__text")
    if not text_el:
        text_el = block.select_one("div[class*='text']")

    if text_el:
        text = text_el.get_text(separator=" ", strip=True)
    else:
        text = block.get_text(separator=" ", strip=True)

    if not text or len(text) < 30:
        return None

    title_el = block.select_one("a.complaint-box-results__title")
    title = title_el.get_text(strip=True) if title_el else None
    if title:
        text = f"{title}. {text}".strip()

    location = None
    date_str = None
    info_el = block.select_one("div.complaint-box-results__info")
    if info_el:
        info_items = [item.get_text(separator=" ", strip=True)
                      for item in info_el.select("div.complaint-box-results__info-item")
                      if item.get_text(strip=True)]
        if len(info_items) >= 2:
            location, date_str = info_items[0], info_items[1]
        elif len(info_items) == 1:
            date_str = info_items[0]

    if not passes_inclusion_rule(text, title):
        return None

    return {
        "text": text,
        "title": title,
        "date": date_str,
        "location": location,
        "url": page_url,
        "raw": str(block)[:2000],
    }


def scrape_consumercomplaints(config: dict, writer: JSONLWriter) -> int:
    session = _create_session()
    delay_range, max_pages = _get_rate_limits(config)

    records_written = 0
    for base_url in SEARCH_URLS:
        for page in range(1, max_pages + 1):
            url = f"{base_url}&page={page}"
            logger.info(f"Fetching ConsumerComplaints search page {page}: {url}")
            soup = _fetch_page(session, url, delay_range)
            if soup is None:
                break

            blocks = _extract_complaint_blocks(soup)
            if not blocks:
                logger.debug(f"No complaint blocks found on page {page} for {base_url}")
                break

            record_count = 0
            for block in blocks:
                parsed = _parse_complaint_block(block, url)
                if not parsed:
                    continue

                record = create_record(
                    source="ConsumerComplaints",
                    platform_type="complaint",
                    text=parsed["text"],
                    url=parsed["url"],
                    title=parsed["title"],
                    rating=None,
                    date=parsed["date"],
                    location=parsed["location"],
                    product_name=None,
                    product_type="unknown",
                    entity_type="service",
                    raw=parsed["raw"],
                    metadata={"complaint_id": None, "status": None},
                )

                if writer.write_record(record):
                    records_written += 1
                    record_count += 1

            logger.info(f"  ConsumerComplaints page={page} → {record_count} records")
            random_delay(delay_range)

    logger.info(f"Completed ConsumerComplaints scrape with {records_written} records")
    return records_written
