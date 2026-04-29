import logging
from typing import List, Optional, Set
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from utils.schema import create_record, passes_inclusion_rule
from utils.writer import JSONLWriter
from utils.rate_limiter import fetch_with_retry, random_delay

logger = logging.getLogger(__name__)

BASE_URL = "https://old.reddit.com"
SEARCH_URL_TEMPLATE = (
    BASE_URL + "/search?q={query}&sort=relevance&t=all&limit=25"
)

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
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

DEFAULT_QUERIES = [
    "carrier ac complaint",
    "carrier air conditioner issue",
    "carrier ac not cooling",
    "carrier ac service complaint",
    "carrier ac installation problem",
]

MAX_REDDIT_POSTS = 20


def _create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _get_rate_limits(config: dict) -> tuple[list[int], int]:
    rate_config = config.get("rate_limits", {}).get("reddit", {})
    delay = rate_config.get("delay_seconds", [2, 4])
    max_posts = rate_config.get("max_posts", MAX_REDDIT_POSTS)
    return delay, max_posts


def _build_search_urls(config: dict) -> List[str]:
    queries = config.get("reddit_queries") or DEFAULT_QUERIES
    return [SEARCH_URL_TEMPLATE.format(query=quote(query)) for query in queries]


def _fetch_page(session: requests.Session, url: str, delay_range: list[int]) -> Optional[BeautifulSoup]:
    response = fetch_with_retry(url, session=session, delay_range=tuple(delay_range), headers=HEADERS)
    if response is None:
        logger.warning(f"Failed to fetch reddit page: {url}")
        return None
    return BeautifulSoup(response.text, "html.parser")


def _extract_search_results(soup: BeautifulSoup) -> List[dict]:
    results = []
    for title_el in soup.select("a.search-title"):
        href = title_el.get("href")
        if not href or "/comments/" not in href:
            continue

        title = title_el.get_text(strip=True)
        url = href if href.startswith("http") else BASE_URL + href

        if any(token in href for token in ["/r/", "/search", "/submit"]):
            # Skip subreddit listings and non-thread links
            if "/comments/" not in href:
                continue

        if url in [result["url"] for result in results]:
            continue

        results.append({"title": title, "url": url})

    return results


def _extract_post_text(soup: BeautifulSoup) -> Optional[str]:
    selectors = [
        "div#siteTable div.thing div.expando .usertext-body div.md",
        "div.expando .usertext-body div.md",
        "div#siteTable div.thing div.usertext-body div.md",
        "div[data-test-id='post-content'] div[data-click-id='text']",
        "div[data-testid='post-content'] div[data-click-id='text']",
        "div.md",
    ]

    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = node.get_text(separator=" ", strip=True)
            if text and len(text) > 60:
                if text.lower().startswith('r/') and 'rules' in text.lower():
                    continue
                return text

    return None


def scrape_reddit(config: dict, writer: JSONLWriter) -> int:
    session = _create_session()
    delay_range, max_posts = _get_rate_limits(config)
    search_urls = _build_search_urls(config)

    records_written = 0
    seen_urls: Set[str] = set()

    for search_url in search_urls:
        if records_written >= max_posts:
            break

        logger.info(f"Fetching Reddit search: {search_url}")
        soup = _fetch_page(session, search_url, delay_range)
        if soup is None:
            continue

        results = _extract_search_results(soup)
        logger.info(f"Found {len(results)} candidate Reddit posts")

        for result in results:
            if records_written >= max_posts:
                break
            if result["url"] in seen_urls:
                continue

            seen_urls.add(result["url"])
            logger.info(f"Fetching Reddit thread: {result['url']}")
            post_soup = _fetch_page(session, result["url"], delay_range)
            if post_soup is None:
                continue

            post_text = _extract_post_text(post_soup)
            if not post_text:
                post_text = result["title"] if len(result["title"]) > 40 else None
            if not post_text:
                logger.debug(f"Skipping Reddit post without enough text: {result['url']}")
                continue

            record = create_record(
                source="Reddit",
                platform_type="social",
                text=post_text,
                url=result["url"],
                title=result["title"],
                rating=None,
                date=None,
                location=None,
                product_name=None,
                product_type="unknown",
                entity_type="service",
                raw=str(post_soup)[:2000],
                metadata={"upvotes": None, "comments_count": None, "verified_purchase": None, "service_center_name": None, "complaint_id": None, "status": None},
            )

            if writer.write_record(record):
                records_written += 1
            random_delay(delay_range)

    logger.info(f"Completed Reddit scrape with {records_written} records")
    return records_written
