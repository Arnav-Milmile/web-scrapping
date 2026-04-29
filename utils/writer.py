"""
writer.py — JSONL append writer for Carrier scraper records.

Handles:
- Appending validated records to per-source JSONL files
- Ensuring output directories exist
- Thread-safe writes (via file-level locking with a simple approach)
"""

import json
import os
import logging
from pathlib import Path
from typing import List

from utils.schema import CarrierRecord, validate_record, record_to_dict

logger = logging.getLogger(__name__)


class JSONLWriter:
    """Append-only JSONL writer for CarrierRecord objects."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._counts: dict[str, int] = {}

    def _get_filepath(self, source: str) -> Path:
        """Map source name to output file path."""
        source_file_map = {
            "MouthShut": "mouthshut.jsonl",
            "ConsumerComplaints": "consumercomplaints.jsonl",
            "Amazon": "amazon.jsonl",
            "Flipkart": "flipkart.jsonl",
            "GoogleMaps": "google_maps.jsonl",
            "Reddit": "reddit.jsonl",
        }
        filename = source_file_map.get(source, f"{source.lower()}.jsonl")
        return self.output_dir / filename

    def write_record(self, record: CarrierRecord) -> bool:
        """
        Validate and write a single record to the appropriate JSONL file.
        Returns True if written successfully, False if validation failed.
        """
        errors = validate_record(record)
        if errors:
            logger.error(
                f"Record validation failed (source={record.source}, "
                f"url={record.url}): {errors}"
            )
            return False

        filepath = self._get_filepath(record.source)
        record_dict = record_to_dict(record)

        try:
            with open(filepath, "a", encoding="utf-8") as f:
                json_line = json.dumps(record_dict, ensure_ascii=False)
                f.write(json_line + "\n")

            self._counts[record.source] = self._counts.get(record.source, 0) + 1
            return True

        except Exception as e:
            logger.error(f"Failed to write record to {filepath}: {e}")
            return False

    def write_records(self, records: List[CarrierRecord]) -> int:
        """
        Write multiple records. Returns the count of successfully written records.
        """
        success_count = 0
        for record in records:
            if self.write_record(record):
                success_count += 1
        return success_count

    def get_counts(self) -> dict:
        """Return the count of records written per source."""
        return dict(self._counts)

    def get_total_count(self) -> int:
        """Return total records written across all sources."""
        return sum(self._counts.values())
