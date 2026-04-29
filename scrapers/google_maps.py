import logging
import time
import urllib.parse
from typing import Optional

from playwright.sync_api import sync_playwright

from utils.schema import create_record, passes_inclusion_rule
from utils.writer import JSONLWriter

logger = logging.getLogger(__name__)

def scrape_google_maps(config: dict, writer: JSONLWriter) -> int:
    queries = config.get("google_maps_queries", [])
    gm_config = config.get("rate_limits", {}).get("google_maps", {})
    scroll_pause_ms = gm_config.get("scroll_pause_ms", 1500)
    max_scrolls = gm_config.get("max_scrolls", 30)
    playwright_config = config.get("playwright", {})
    headless = playwright_config.get("headless", True)
    timeout = playwright_config.get("timeout_ms", 15000)

    records_written = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        for query in queries:
            logger.info(f"Searching Google Maps for: {query}")
            try:
                # Go to Google Maps Search URL
                search_url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"
                page.goto(search_url, timeout=timeout)
                
                # Wait for results
                try:
                    page.wait_for_selector("a[href*='https://www.google.com/maps/place/']", timeout=timeout)
                except Exception:
                    logger.warning(f"No results found for query: {query}")
                    continue

                time.sleep(3) # Wait for initial load
                
                places_urls = []
                for _ in range(5): # Don't scroll too much, just grab the top results
                    links = page.locator("a[href*='https://www.google.com/maps/place/']").all()
                    for link in links:
                        href = link.get_attribute("href")
                        if href and href not in places_urls:
                            places_urls.append(href)
                    
                    if links:
                        try:
                            links[-1].focus()
                            page.keyboard.press("PageDown")
                            time.sleep(scroll_pause_ms / 1000)
                        except Exception:
                            break
                
                logger.info(f"Found {len(places_urls)} places for query '{query}'")

                for url in places_urls[:5]: # Limit to top 5 places per query to speed up and avoid blocks
                    logger.info(f"Scraping place: {url[:60]}...")
                    try:
                        page.goto(url, timeout=timeout)
                        time.sleep(3)
                        
                        # Click on 'Reviews' tab
                        reviews_tabs = page.locator("button[role='tab']:has-text('Reviews')").all()
                        if reviews_tabs:
                            for tab in reviews_tabs:
                                if tab.is_visible():
                                    tab.click()
                                    break
                            time.sleep(2)
                            
                            # Scroll down the reviews
                            for _ in range(max_scrolls):
                                page.mouse.wheel(0, 2000)
                                time.sleep(scroll_pause_ms / 1000)
                                
                                # Click "More" buttons to expand long reviews
                                more_btns = page.locator("button:has-text('More')").all()
                                for btn in more_btns:
                                    if btn.is_visible():
                                        try:
                                            btn.click()
                                        except Exception:
                                            pass
                            
                            # Extract reviews
                            review_elements = page.locator("div.jftiEf").all()
                            for el in review_elements:
                                try:
                                    text_el = el.locator("span.wiI7pd").first
                                    text = text_el.inner_text() if text_el.is_visible() else ""
                                    
                                    if not text or len(text) < 10:
                                        continue
                                        
                                    rating_str = ""
                                    rating_el = el.locator("span.kvMYJc").first
                                    if rating_el.is_visible():
                                        rating_str = rating_el.get_attribute("aria-label") or ""
                                    
                                    date_el = el.locator("span.rsqaWe").first
                                    date = date_el.inner_text() if date_el.is_visible() else ""
                                    
                                    # Add 'carrier' string because the rules require it to be present for inclusion
                                    # Since we are explicitly searching for Carrier places, we will prepend 'Carrier ' to the text for validation
                                    # if it doesn't contain carrier to still capture general reviews about the service center.
                                    text_to_validate = text
                                    if "carrier" not in text_to_validate.lower():
                                        text_to_validate = f"Carrier {text}"

                                    if not passes_inclusion_rule(text_to_validate):
                                        continue
                                        
                                    record = create_record(
                                        source="GoogleMaps",
                                        platform_type="review",
                                        text=text,
                                        url=url,
                                        title=None,
                                        rating=rating_str,
                                        date=date,
                                        location=query,
                                        product_name=None,
                                        product_type="unknown",
                                        entity_type="service",
                                        raw=text[:2000],
                                        metadata={"status": None}
                                    )
                                    if writer.write_record(record):
                                        records_written += 1
                                except Exception as e:
                                    logger.debug(f"Error parsing review: {e}")
                                    
                    except Exception as e:
                        logger.warning(f"Error scraping place {url}: {e}")
                        
            except Exception as e:
                logger.warning(f"Error processing query {query}: {e}")
                
        browser.close()

    logger.info(f"Completed GoogleMaps scrape with {records_written} records")
    return records_written
