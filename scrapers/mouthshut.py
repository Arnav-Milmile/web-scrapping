"""
mouthshut.py — MouthShut.com scraper for Carrier reviews.

Strategy:
1. Search for "carrier" products via the product search endpoint
2. Discover all Carrier product/review page links from search results
3. For each product page, extract individual reviews
4. Paginate through both search results and review pages
5. Apply inclusion rule and write records

MouthShut URL patterns:
- Search (AJAX-loaded): https://www.mouthshut.com/search/prodsrch_loadmore_ajax.aspx?data=carrier&type=&gsearch=0&p=0&currentpage={page}&id=0
- Search products page: https://www.mouthshut.com/search/prodsrch.aspx?data=carrier&type=articles
- Product reviews: https://www.mouthshut.com/{category}/{product-slug}-reviews-{id}
- Review pagination: ?page={n} or ?flt=p_{n}
"""

import re
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse, parse_qs

from utils.schema import (
    CarrierRecord, create_record, passes_inclusion_rule, detect_keywords
)
from utils.writer import JSONLWriter
from utils.rate_limiter import random_delay, fetch_with_retry

logger = logging.getLogger(__name__)

# Browser-like headers to avoid blocks
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

BASE_URL = "https://www.mouthshut.com"
SEARCH_URL = f"{BASE_URL}/search/prodsrch_loadmore_ajax.aspx?data=carrier&type=&gsearch=0&p=0&currentpage=0&id=0"
SEARCH_URL_TEMPLATE = f"{BASE_URL}/search/prodsrch_loadmore_ajax.aspx?data=carrier&type=&gsearch=0&p=0&currentpage={{page}}&id=0"



def _create_session() -> requests.Session:
    """Create a requests session with browser-like headers."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _build_search_url(page_num: int) -> str:
    """Build the MouthShut search AJAX URL for a given search page."""
    if page_num == 0:
        return SEARCH_URL
    return SEARCH_URL_TEMPLATE.format(page=page_num + 1)


def _extract_product_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    """
    Extract all Carrier-related product/review links from a search results page.
    Looks for links that point to product review pages containing 'carrier'.
    """
    links = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        full_url = urljoin(base_url, href)
        href_lower = href.lower()

        # Match review page links that contain 'carrier' (case insensitive)
        if "carrier" in href_lower and any(
            token in href_lower
            for token in [
                "/product-reviews/",
                "/air-conditioners/",
                "/review/",
                "/reviews/",
                "-reviews-",
                "/product-",
                "/products/",
            ]
        ):
            links.add(full_url)

        # Also catch links with review IDs that mention carrier in text
        link_text = a_tag.get_text(strip=True).lower()
        if "carrier" in link_text and any(token in href_lower for token in ["-reviews-", "/review/"]):
            links.add(full_url)

    logger.info(f"Found {len(links)} Carrier product links on page")
    return list(links)


def _find_nearest_text_snippet(tag) -> Optional[str]:
    """Find a nearby textual snippet around a search result link."""
    for candidate in [tag.parent, tag.parent.parent, tag.parent.parent.parent]:
        if candidate is None:
            continue
        for text_node in candidate.find_all(["p", "span", "div"], recursive=False):
            snippet = text_node.get_text(separator=" ", strip=True)
            if len(snippet) > 40 and "carrier" in snippet.lower():
                return snippet
    return None


def _extract_search_results(
    soup: BeautifulSoup, base_url: str
) -> tuple[set[str], List[Dict[str, Any]]]:
    """Extract Carrier review/product links and optional direct review snippets from search pages."""
    product_links = set()
    search_records = []
    seen_urls = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        href_lower = href.lower()
        text = a_tag.get_text(separator=" ", strip=True)
        if "carrier" not in href_lower and "carrier" not in text.lower():
            continue

        if not any(
            token in href_lower
            for token in ["/review/", "/reviews/", "-reviews-", "/product-", "/products/"]
        ):
            continue

        full_url = urljoin(base_url, href)
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)
        product_links.add(full_url)

        snippet = _find_nearest_text_snippet(a_tag) or text
        if not snippet or not passes_inclusion_rule(snippet, text):
            continue

        record_data = {
            "text": snippet,
            "title": text if text else None,
            "rating": None,
            "date": _extract_date_from_text(snippet),
            "url": full_url,
            "raw": str(a_tag)[:2000],
        }
        search_records.append(record_data)

    return product_links, search_records


def _extract_reviews_from_page(
    soup: BeautifulSoup, page_url: str
) -> List[Dict[str, Any]]:
    """
    Extract review data from a MouthShut product review page.
    
    MouthShut uses various CSS structures. We try multiple selectors
    to be resilient to layout changes.
    """
    reviews = []

    # --- Strategy 1: Look for review containers by common class patterns ---
    # MouthShut commonly uses classes like: review-article, row review-article,
    # review-body, reviewdata, col-10
    review_containers = []

    # Try multiple possible selectors for review containers
    selectors = [
        "div.review-article",
        "div.row.review-article",
        "div.reviewdata",
        "div.review-cont",
        "div.review_cont",
        "div.review-box",
        "article.review-card",
        "div.media-body",
        "div.card",
        "div[class*='review']",
        "div.col-10",
    ]

    for selector in selectors:
        containers = soup.select(selector)
        if containers:
            review_containers = containers
            logger.debug(f"Found {len(containers)} reviews with selector: {selector}")
            break

    # If no review containers found via CSS, try finding review-like sections
    if not review_containers:
        # Look for divs that contain both rating and text content
        for div in soup.find_all("div"):
            classes = div.get("class", [])
            class_str = " ".join(classes).lower() if classes else ""
            if "review" in class_str or "comment" in class_str:
                review_containers.append(div)

    # --- Strategy 2: Look for review links (individual review pages) ---
    review_links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        # MouthShut individual review URLs often look like:
        # /review/{product-slug}-review-{reviewid}
        if "/review/" in href and "review" in href.lower():
            full_url = urljoin(BASE_URL, href)
            link_text = a_tag.get_text(strip=True)
            if link_text and len(link_text) > 10:
                review_links.append({
                    "url": full_url,
                    "title": link_text,
                })

    # Process found review containers
    for container in review_containers:
        review_data = _parse_review_container(container, page_url)
        if review_data and review_data.get("text"):
            reviews.append(review_data)

    # If we found review links but no container-based reviews, 
    # those are review title links — we'll include them if useful
    if not reviews and review_links:
        for rl in review_links:
            reviews.append({
                "text": rl.get("title", ""),
                "title": rl.get("title"),
                "rating": None,
                "date": None,
                "url": rl.get("url", page_url),
                "raw": "",
            })

    # --- Strategy 3: Fallback — extract all meaningful text blocks ---
    if not reviews:
        reviews = _fallback_text_extraction(soup, page_url)

    return reviews


def _parse_review_container(
    container, page_url: str
) -> Optional[Dict[str, Any]]:
    """Parse a single review container element."""
    text = ""
    title = None
    rating = None
    date = None
    raw = str(container)[:2000]

    # --- Extract review text ---
    # Try multiple selectors for the review body
    text_selectors = [
        "div.review-body",
        "div.reviewdata",
        "div.review-content",
        "div.review-text",
        "div.review-body-text",
        "div.more",
        "p.review-body",
        "span.review-body",
        "div[class*='body']",
        "div[class*='content']",
        "p",
    ]

    for sel in text_selectors:
        text_el = container.select_one(sel)
        if text_el:
            text = text_el.get_text(separator=" ").strip()
            if len(text) > 20:  # meaningful content
                break

    # If still no text, get all text from the container
    if not text or len(text) < 20:
        text = container.get_text(separator=" ").strip()

    # --- Extract title ---
    title_selectors = [
        "a.review-title",
        "h2 a",
        "h3 a",
        "h4 a",
        "a[class*='title']",
        "strong",
        "b",
    ]
    for sel in title_selectors:
        title_el = container.select_one(sel)
        if title_el:
            title = title_el.get_text(strip=True)
            if title and len(title) > 3:
                break

    # --- Extract rating ---
    # MouthShut uses star-based ratings, often as images or spans
    rating_selectors = [
        "span.rating",
        "div.rating",
        "span[class*='star']",
        "div[class*='star']",
        "span[class*='rating']",
        "i[class*='rated']",
    ]

    for sel in rating_selectors:
        rating_els = container.select(sel)
        if rating_els:
            # Count filled stars or get numeric rating
            rated_count = len([
                el for el in rating_els
                if "rated" in " ".join(el.get("class", [])).lower()
                or "full" in " ".join(el.get("class", [])).lower()
                or "active" in " ".join(el.get("class", [])).lower()
            ])
            if rated_count > 0:
                rating = str(rated_count)
                break

    # Try extracting numeric rating from text
    if not rating:
        rating_match = re.search(r'(\d(?:\.\d)?)\s*/\s*5', container.get_text())
        if rating_match:
            rating = rating_match.group(1)

    # Also look for explicit rating attributes
    if not rating:
        if container.has_attr("data-rating"):
            rating = str(container["data-rating"]).strip()

    # Also check for rating in alt text of images
    if not rating:
        for img in container.find_all("img"):
            alt = img.get("alt", "")
            rating_match = re.search(r'(\d(?:\.\d)?)\s*(?:star|out of)', alt, re.I)
            if rating_match:
                rating = rating_match.group(1)
                break

    # Check aria-label or title attributes for rating information
    if not rating:
        for el in container.find_all(attrs={"aria-label": True}):
            rating_match = re.search(r'(\d(?:\.\d)?)', el["aria-label"])
            if rating_match:
                rating = rating_match.group(1)
                break
        if not rating:
            for el in container.find_all(attrs={"title": True}):
                rating_match = re.search(r'(\d(?:\.\d)?)\s*(?:star|out of)', el["title"], re.I)
                if rating_match:
                    rating = rating_match.group(1)
                    break

    # --- Extract date ---
    date_selectors = [
        "span.date",
        "span.review-date",
        "div.date",
        "time",
        "span[class*='date']",
        "div[class*='date']",
        "span[class*='time']",
    ]

    for sel in date_selectors:
        date_el = container.select_one(sel)
        if date_el:
            date_text = date_el.get_text(strip=True)
            date = _parse_date(date_text)
            if date:
                break

    # Try finding date in text using regex
    if not date:
        text_content = container.get_text()
        date = _extract_date_from_text(text_content)

    # --- Extract review URL ---
    url = page_url
    for a_tag in container.find_all("a", href=True):
        href = a_tag["href"]
        if "/review/" in href:
            url = urljoin(BASE_URL, href)
            break

    return {
        "text": text,
        "title": title,
        "rating": rating,
        "date": date,
        "url": url,
        "raw": raw,
    }


def _fallback_text_extraction(
    soup: BeautifulSoup, page_url: str
) -> List[Dict[str, Any]]:
    """
    Fallback: extract meaningful text blocks from the page.
    Used when standard review selectors don't match.
    """
    reviews = []

    # Look for any substantial text blocks on the page
    # that might be reviews
    for p in soup.find_all(["p", "div"]):
        text = p.get_text(separator=" ").strip()
        # Filter: must be substantial and not navigation/footer
        if (
            len(text) > 100
            and "carrier" in text.lower()
            and not any(
                skip in text.lower()
                for skip in [
                    "copyright", "terms of service", "privacy policy",
                    "sign up", "log in", "forgot password",
                ]
            )
        ):
            reviews.append({
                "text": text,
                "title": None,
                "rating": None,
                "date": None,
                "url": page_url,
                "raw": str(p)[:2000],
            })

    # Deduplicate by text content (keep unique reviews)
    seen = set()
    unique = []
    for r in reviews:
        text_key = r["text"][:100]
        if text_key not in seen:
            seen.add(text_key)
            unique.append(r)

    return unique


def _parse_date(date_text: str) -> Optional[str]:
    """Parse various date formats to YYYY-MM-DD."""
    import re
    from datetime import datetime

    if not date_text:
        return None

    date_text = date_text.strip()

    # Common formats on MouthShut
    formats = [
        "%b %d, %Y",       # Jan 15, 2024
        "%B %d, %Y",       # January 15, 2024
        "%d %b %Y",        # 15 Jan 2024
        "%d %B %Y",        # 15 January 2024
        "%d/%m/%Y",        # 15/01/2024
        "%m/%d/%Y",        # 01/15/2024
        "%Y-%m-%d",        # 2024-01-15
        "%d-%m-%Y",        # 15-01-2024
        "%b %Y",           # Jan 2024
        "%B %Y",           # January 2024
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_text, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def _extract_date_from_text(text: str) -> Optional[str]:
    """Extract date from arbitrary text using regex."""
    # Pattern: Month DD, YYYY or DD Month YYYY
    patterns = [
        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4})',
        r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})',
        r'(\d{1,2}/\d{1,2}/\d{4})',
        r'(\d{4}-\d{2}-\d{2})',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return _parse_date(match.group(1))

    return None


def _get_next_search_page_url(soup: BeautifulSoup, current_page: int) -> Optional[str]:
    """Find the next page URL for search results."""
    next_page = current_page + 1

    # Look for pagination links
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        text = a_tag.get_text(strip=True).lower()

        # Check for explicit "next" link
        if "next" in text or "»" in text or ">" == text:
            return urljoin(BASE_URL, href)

        # Check for page number link
        if f"p={next_page}" in href:
            return urljoin(BASE_URL, href)

    return None


def _get_next_review_page_url(
    soup: BeautifulSoup, current_url: str, current_page: int
) -> Optional[str]:
    """Find the next page URL for product reviews."""
    next_page = current_page + 1

    # Look for pagination
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        text = a_tag.get_text(strip=True).lower()

        if "next" in text or "»" in text:
            return urljoin(BASE_URL, href)

        if f"page={next_page}" in href or f"flt=p_{next_page}" in href:
            return urljoin(BASE_URL, href)

    # Construct URL manually
    parsed = urlparse(current_url)
    if "page=" in current_url:
        new_url = re.sub(r'page=\d+', f'page={next_page}', current_url)
    else:
        separator = "&" if "?" in current_url else "?"
        new_url = f"{current_url}{separator}page={next_page}"

    return new_url


def _infer_product_info(url: str, soup: BeautifulSoup) -> Dict[str, str]:
    """Infer product name and type from URL and page content."""
    product_name = None
    product_type = "unknown"
    entity_type = "product"

    # Extract from URL slug
    url_lower = url.lower()
    if "/air-conditioners/" in url_lower or "ac" in url_lower:
        product_type = "air conditioner"
    elif "/refrigerator" in url_lower:
        product_type = "refrigerator"
    elif "/washing-machine" in url_lower:
        product_type = "washing machine"

    # Extract product name from page title
    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        # Remove common suffixes
        product_name = re.sub(
            r'\s*(?:Reviews?|Price|Features|Ratings?|MouthShut).*$',
            '', title_text, flags=re.IGNORECASE
        ).strip()
        if not product_name:
            product_name = title_text

    # Extract from h1
    if not product_name:
        h1 = soup.find("h1")
        if h1:
            product_name = h1.get_text(strip=True)

    # Infer entity_type
    if any(term in url_lower for term in ["service", "support", "center"]):
        entity_type = "service"
    elif "dealer" in url_lower:
        entity_type = "dealer"

    return {
        "product_name": product_name,
        "product_type": product_type,
        "entity_type": entity_type,
    }


def scrape_mouthshut(config: dict, writer: JSONLWriter) -> int:
    """
    Main entry point for MouthShut scraper.
    
    Args:
        config: Full config dict (we use rate_limits.mouthshut)
        writer: JSONLWriter instance for output

    Returns:
        Total number of records written.
    """
    rate_config = config.get("rate_limits", {}).get("mouthshut", {})
    delay_range = rate_config.get("delay_seconds", [2, 4])
    max_pages = rate_config.get("max_pages", 20)

    session = _create_session()
    total_records = 0
    all_product_urls = set()

    logger.info("=" * 60)
    logger.info("Starting MouthShut scraper")
    logger.info(f"Config: delay={delay_range}, max_pages={max_pages}")
    logger.info("=" * 60)

    # --- Phase 1: Discover Carrier product URLs from search ---
    logger.info("Phase 1: Discovering Carrier product URLs and direct search review snippets...")
    prev_url = None
    for page_num in range(max_pages):
        search_url = _build_search_url(page_num)

        # Pagination loop guard
        if search_url == prev_url:
            logger.info("Pagination loop detected, stopping search")
            break
        prev_url = search_url

        logger.info(f"Fetching search page {page_num}: {search_url}")
        random_delay(delay_range)

        response = fetch_with_retry(
            search_url, session=session, delay_range=delay_range
        )
        if not response:
            logger.warning(f"Failed to fetch search page {page_num}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        product_links, search_records = _extract_search_results(soup, search_url)

        if search_records:
            page_written = 0
            for review_data in search_records:
                record = create_record(
                    source="MouthShut",
                    platform_type="review",
                    text=review_data["text"],
                    url=review_data["url"],
                    title=review_data["title"],
                    rating=review_data["rating"],
                    date=review_data["date"],
                    product_type="unknown",
                    entity_type="unknown",
                    raw=review_data["raw"],
                )
                if writer.write_record(record):
                    page_written += 1
            total_records += page_written
            logger.info(
                f"Search page {page_num}: {len(search_records)} direct snippets found, "
                f"{page_written} written"
            )

        if not product_links:
            logger.info(f"No product links found on search page {page_num}")
            if page_num > 0:
                break
            continue

        before = len(all_product_urls)
        all_product_urls.update(product_links)
        new_count = len(all_product_urls) - before

        logger.info(
            f"Search page {page_num}: {len(product_links)} links found, "
            f"{new_count} new (total: {len(all_product_urls)})"
        )

        # If no new links, we've exhausted search results
        if new_count == 0 and page_num > 0:
            logger.info("No new product links found, stopping search pagination")
            break

    # Add well-known Carrier product URLs to ensure coverage
    known_carrier_urls = _discover_known_carrier_urls(session, delay_range)
    all_product_urls.update(known_carrier_urls)

    logger.info(f"Total product URLs discovered: {len(all_product_urls)}")

    # --- Phase 2: Scrape reviews from each product page ---
    logger.info("Phase 2: Scraping reviews from product pages...")

    for idx, product_url in enumerate(sorted(all_product_urls)):
        logger.info(f"[{idx + 1}/{len(all_product_urls)}] Scraping: {product_url}")

        page_records = _scrape_product_reviews(
            session, product_url, delay_range, max_pages, writer
        )
        total_records += page_records

        logger.info(f"  → {page_records} records from this product")

    # --- Phase 3: Also scrape the search results "articles" tab ---
    logger.info("Phase 3: Scraping search articles/reviews...")
    articles_records = _scrape_search_articles(
        session, delay_range, max_pages, writer
    )
    total_records += articles_records

    logger.info("=" * 60)
    logger.info(f"MouthShut scraper complete. Total records: {total_records}")
    logger.info("=" * 60)

    return total_records


def _discover_known_carrier_urls(
    session: requests.Session, delay_range: list
) -> set:
    """
    Discover Carrier product URLs by navigating category pages.
    """
    known_urls = set()

    # Try the AC category page for Carrier
    category_urls = [
        "https://www.mouthshut.com/air-conditioners/carrier-air-conditioners-reviews-925042452",
    ]

    for cat_url in category_urls:
        random_delay(delay_range)
        response = fetch_with_retry(cat_url, session=session, delay_range=delay_range)
        if response and response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            links = _extract_product_links(soup, cat_url)
            known_urls.update(links)
            logger.info(f"Found {len(links)} product links from category: {cat_url}")

    return known_urls


def _extract_review_query_string(page_text: str) -> Optional[str]:
    """Parse the JS query_string from a MouthShut product page."""
    match = re.search(r"var\s+query_string\s*=\s*['\"]([^'\"]+)['\"]", page_text)
    return match.group(1) if match else None


def _get_review_ajax_url(query_string: str) -> str:
    return f"{BASE_URL}/Review/rar_reviews.aspx?{query_string}"


def _get_product_page_base_url(product_url: str) -> str:
    return re.sub(r'-page-\d+(?=$|[/?#])', '', product_url)


def _get_next_review_page_url_from_ajax(
    soup: BeautifulSoup, current_base_url: str, current_page: int
) -> Optional[str]:
    """Find the next paginated review page URL from AJAX review response HTML."""
    candidate_pages = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "-page-" not in href:
            continue
        full_url = urljoin(BASE_URL, href)
        if current_base_url not in full_url:
            continue
        match = re.search(r'-page-(\d+)', full_url)
        if not match:
            continue
        page_num = int(match.group(1))
        candidate_pages.append((page_num, full_url))

    candidate_pages.sort()
    for page_num, full_url in candidate_pages:
        if page_num > current_page:
            return full_url
    return None


def _scrape_product_reviews(
    session: requests.Session,
    product_url: str,
    delay_range: list,
    max_pages: int,
    writer: JSONLWriter,
) -> int:
    """Scrape all review pages for a single product."""
    records_written = 0
    prev_url = None
    current_url = product_url
    current_page = 1
    base_url = _get_product_page_base_url(product_url)

    for page_num in range(1, max_pages + 1):
        if current_url == prev_url:
            break
        prev_url = current_url

        random_delay(delay_range)
        response = fetch_with_retry(current_url, session=session, delay_range=delay_range)
        if not response:
            logger.warning(f"Failed to fetch review page: {current_url}")
            break

        page_text = response.text
        page_soup = BeautifulSoup(page_text, "html.parser")
        product_info = _infer_product_info(current_url, page_soup)

        query_string = _extract_review_query_string(page_text)
        review_soup = page_soup
        review_url = current_url

        if query_string:
            ajax_url = _get_review_ajax_url(query_string)
            ajax_response = fetch_with_retry(ajax_url, session=session, delay_range=delay_range)
            if ajax_response:
                review_url = ajax_url
                review_soup = BeautifulSoup(ajax_response.text, "html.parser")
            else:
                logger.warning(f"Failed to fetch AJAX reviews for product: {current_url}")

        reviews = _extract_reviews_from_page(review_soup, review_url)
        if not reviews:
            if page_num == 1:
                logger.info(f"No reviews found on first page: {current_url}")
            else:
                logger.info(f"No more reviews found at page {page_num}")
            break

        page_written = 0
        for review_data in reviews:
            text = review_data.get("text", "")
            title = review_data.get("title")

            if not text:
                continue

            if not passes_inclusion_rule(text, title):
                continue

            record = create_record(
                source="MouthShut",
                platform_type="review",
                text=text,
                url=review_data.get("url", current_url),
                title=title,
                rating=review_data.get("rating"),
                date=review_data.get("date"),
                product_name=product_info.get("product_name"),
                product_type=product_info.get("product_type", "unknown"),
                entity_type=product_info.get("entity_type", "product"),
                raw=review_data.get("raw", ""),
            )
            if writer.write_record(record):
                page_written += 1

        records_written += page_written
        logger.info(f"  Page {page_num}: {len(reviews)} reviews found, {page_written} written")

        next_url = _get_next_review_page_url_from_ajax(review_soup, base_url, current_page)
        if not next_url:
            break

        current_url = next_url
        current_page += 1

    return records_written


def _has_next_review_page(soup: BeautifulSoup, current_page: int) -> bool:
    """Check if there's a next page of reviews."""
    next_page = current_page + 1

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        text = a_tag.get_text(strip=True).lower()

        if "next" in text or "»" in text:
            return True
        if f"page={next_page}" in href:
            return True

    return False


def _scrape_search_articles(
    session: requests.Session,
    delay_range: list,
    max_pages: int,
    writer: JSONLWriter,
) -> int:
    """
    Scrape Carrier-related articles/reviews from MouthShut search.
    These are different from product reviews — they're user-written articles.
    """
    records_written = 0
    articles_url = (
        "https://www.mouthshut.com/search/prodsrch.aspx"
        "?data=carrier&type=articles"
    )

    for page_num in range(max_pages):
        url = f"{articles_url}&p={page_num}"
        random_delay(delay_range)

        response = fetch_with_retry(url, session=session, delay_range=delay_range)
        if not response:
            break

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract article links
        article_links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if "/review/" in href:
                full_url = urljoin(BASE_URL, href)
                link_text = a_tag.get_text(strip=True)
                article_links.append((full_url, link_text))

        if not article_links:
            break

        for article_url, article_title in article_links:
            random_delay(delay_range)
            art_response = fetch_with_retry(
                article_url, session=session, delay_range=delay_range
            )

            if not art_response:
                continue

            art_soup = BeautifulSoup(art_response.text, "html.parser")

            # Extract the main article/review text
            text = ""
            for selector in ["div.review-body", "div.reviewdata",
                             "div.review-content", "article", "div.main-content"]:
                el = art_soup.select_one(selector)
                if el:
                    text = el.get_text(separator=" ").strip()
                    if len(text) > 50:
                        break

            if not text:
                # Fallback: get the largest text block
                paragraphs = art_soup.find_all("p")
                texts = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50]
                text = " ".join(texts)

            if not text or not passes_inclusion_rule(text, article_title):
                continue

            record = create_record(
                source="MouthShut",
                platform_type="review",
                text=text,
                url=article_url,
                title=article_title,
                product_type="unknown",
                entity_type="unknown",
                raw=str(art_soup)[:2000] if art_soup else "",
            )

            if writer.write_record(record):
                records_written += 1

        logger.info(f"Articles page {page_num}: {len(article_links)} articles, {records_written} total written")

    return records_written
