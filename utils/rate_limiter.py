"""
rate_limiter.py — Shared delay, retry, and rate-limiting logic.

Provides:
- Random delay between requests (configurable per source)
- Exponential backoff for HTTP 429 responses
- Generic retry decorator for transient failures
"""

import time
import random
import logging
import functools
from typing import Tuple, Optional

import requests

logger = logging.getLogger(__name__)


def random_delay(delay_range: list | tuple) -> None:
    """
    Sleep for a random duration between delay_range[0] and delay_range[1] seconds.
    Always adds a 1-3s base delay as per legal constraints.
    """
    base_delay = random.uniform(1, 3)
    if delay_range and len(delay_range) == 2:
        extra_delay = random.uniform(delay_range[0], delay_range[1])
        total = max(base_delay, extra_delay)  # use the larger of the two
    else:
        total = base_delay

    logger.debug(f"Sleeping for {total:.2f}s")
    time.sleep(total)


def exponential_backoff(attempt: int, base_wait: float = 5.0, max_retries: int = 3) -> bool:
    """
    Implement exponential backoff for rate-limited requests (HTTP 429).
    Returns True if should retry, False if max retries exhausted.
    """
    if attempt >= max_retries:
        logger.warning(f"Max retries ({max_retries}) exhausted")
        return False

    wait_time = (2 ** attempt) * base_wait
    logger.info(f"Rate limited — backing off for {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})")
    time.sleep(wait_time)
    return True


def fetch_with_retry(
    url: str,
    session: Optional[requests.Session] = None,
    delay_range: tuple = (2, 4),
    max_retries: int = 3,
    headers: Optional[dict] = None,
    timeout: int = 30,
) -> Optional[requests.Response]:
    """
    Fetch a URL with retry logic and rate-limit handling.

    - Retries on HTTP 429 with exponential backoff
    - Retries on connection errors up to max_retries
    - Applies random delay before each request
    - Returns None if all retries fail
    """
    if session is None:
        session = requests.Session()

    if headers:
        session.headers.update(headers)

    for attempt in range(max_retries):
        try:
            # Apply rate-limiting delay
            if attempt > 0:
                random_delay(delay_range)

            response = session.get(url, timeout=timeout)

            # Handle rate limiting
            if response.status_code == 429:
                if not exponential_backoff(attempt):
                    logger.error(f"Rate limited on {url}, all retries exhausted")
                    return None
                continue

            # Handle other HTTP errors
            if response.status_code >= 400:
                logger.warning(f"HTTP {response.status_code} for {url}")
                if attempt < max_retries - 1:
                    continue
                return None

            return response

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on {url} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return None

        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error on {url}: {e} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            return None

        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return None

    return None
