"""
schema.py — Dataclass + validation for the Carrier complaint/review record schema.

Every scraped record MUST pass through `create_record()` and `validate_record()`
before being written to disk.
"""

import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any


# Keywords to detect in the review/complaint text
KEYWORDS = [
    "cooling", "installation", "delay", "service", "warranty",
    "refund", "noise", "gas", "compressor", "leakage", "tripping",
    "technician", "remote", "breakdown", "vibration",
]

VALID_SOURCES = {
    "MouthShut", "ConsumerComplaints", "Amazon",
    "Flipkart", "GoogleMaps", "Reddit",
}
VALID_PLATFORM_TYPES = {"review", "complaint", "social"}
VALID_ENTITY_TYPES = {"product", "service", "support", "dealer", "unknown"}


@dataclass
class RecordMetadata:
    upvotes: Optional[int] = None
    comments_count: Optional[int] = None
    verified_purchase: Optional[bool] = None
    service_center_name: Optional[str] = None
    complaint_id: Optional[str] = None
    status: Optional[str] = None


@dataclass
class CarrierRecord:
    id: str
    brand: str
    source: str
    platform_type: str
    text: str
    title: Optional[str]
    rating: Optional[str]
    author: None                       # always null
    date: Optional[str]
    location: Optional[str]
    product_name: Optional[str]
    product_type: str
    entity_type: str
    url: str
    keywords_detected: List[str]
    text_length: int
    metadata: Dict[str, Any]
    raw: str


def detect_keywords(text: str) -> List[str]:
    """Return list of keywords found in `text` (case-insensitive)."""
    lower = text.lower()
    return [kw for kw in KEYWORDS if kw in lower]


def passes_inclusion_rule(text: str, title: Optional[str] = None) -> bool:
    """
    Include a record if and only if 'carrier' appears in text or title.
    """
    if "carrier" in text.lower():
        return True
    if title and "carrier" in title.lower():
        return True
    return False


def create_record(
    source: str,
    platform_type: str,
    text: str,
    url: str,
    title: Optional[str] = None,
    rating: Optional[str] = None,
    date: Optional[str] = None,
    location: Optional[str] = None,
    product_name: Optional[str] = None,
    product_type: str = "unknown",
    entity_type: str = "unknown",
    raw: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> CarrierRecord:
    """
    Factory function to build a validated CarrierRecord.
    Truncates `raw` to 2000 chars if longer.
    """
    if metadata is None:
        metadata = {}

    # Ensure metadata has all expected keys
    meta_defaults = {
        "upvotes": None,
        "comments_count": None,
        "verified_purchase": None,
        "service_center_name": None,
        "complaint_id": None,
        "status": None,
    }
    for k, v in meta_defaults.items():
        metadata.setdefault(k, v)

    # Truncate raw to 2000 chars
    if len(raw) > 2000:
        raw = raw[:2000]

    return CarrierRecord(
        id=str(uuid.uuid4()),
        brand="Carrier",
        source=source,
        platform_type=platform_type,
        text=text,
        title=title,
        rating=rating,
        author=None,
        date=date,
        location=location,
        product_name=product_name,
        product_type=product_type,
        entity_type=entity_type,
        url=url,
        keywords_detected=detect_keywords(text),
        text_length=len(text),
        metadata=metadata,
        raw=raw,
    )


def validate_record(record: CarrierRecord) -> List[str]:
    """
    Validate a CarrierRecord against the strict schema.
    Returns a list of validation errors. Empty list = valid.
    """
    errors: List[str] = []

    if not record.text:
        errors.append("text field is empty")
    if not record.url:
        errors.append("url field is empty")
    if record.source not in VALID_SOURCES:
        errors.append(f"invalid source: {record.source}")
    if record.platform_type not in VALID_PLATFORM_TYPES:
        errors.append(f"invalid platform_type: {record.platform_type}")
    if record.entity_type not in VALID_ENTITY_TYPES:
        errors.append(f"invalid entity_type: {record.entity_type}")
    if record.brand != "Carrier":
        errors.append(f"brand must be 'Carrier', got '{record.brand}'")
    if record.author is not None:
        errors.append("author must be null (privacy rule)")
    if record.text_length != len(record.text):
        errors.append(f"text_length mismatch: {record.text_length} != {len(record.text)}")
    if len(record.raw) > 2000:
        errors.append(f"raw exceeds 2000 chars ({len(record.raw)})")

    return errors


def record_to_dict(record: CarrierRecord) -> dict:
    """Convert a CarrierRecord to a JSON-serialisable dictionary."""
    return asdict(record)
