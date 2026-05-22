"""Watermark management for CDC-based ETL pipeline."""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from .s3_client import S3Client

logger = logging.getLogger(__name__)


class WatermarkStore:
    """Manages watermark state in S3 for change detection."""

    def __init__(self, s3_client: S3Client):
        """Initialize watermark store.

        Args:
            s3_client: S3 client instance.
        """
        self.s3 = s3_client

    def read(self) -> Optional[dict]:
        """Read the current watermark from S3.

        Returns:
            Watermark dict or None if not found.
        """
        try:
            key = f"{self.s3.config.S3_WATERMARK_PREFIX}{self.s3.config.WATERMARK_FILE}"
            response = self.s3.client.get_object(
                Bucket=self.s3.config.S3_BUCKET,
                Key=key,
            )
            data = response["Body"].read().decode("utf-8")
            return json.loads(data)
        except self.s3.client.exceptions.NoSuchKey:
            logger.info("Watermark file not found, returning None")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse watermark JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to read watermark: {e}")
            return None

    def write(
        self,
        last_successful_run: datetime,
        last_processed_file: str,
        records_processed: int,
        temp_key: Optional[str] = None,
    ) -> bool:
        """Write watermark atomically using copy-then-rename pattern.

        Args:
            last_successful_run: Timestamp of successful run completion.
            last_processed_file: Last processed file path.
            records_processed: Total records processed in this run.
            temp_key: Optional temporary key for atomic update.

        Returns:
            True if successful.
        """
        watermark_data = {
            "last_successful_run": last_successful_run.isoformat(),
            "last_processed_file": last_processed_file,
            "records_processed": records_processed,
            "watermark_id": str(uuid.uuid4()),
        }

        watermark_json = json.dumps(watermark_data, indent=2)

        # Use atomic update via copy-then-rename if temp_key provided
        if temp_key:
            # Write to temp location
            if not self.s3.write_object(temp_key, watermark_json, "application/json"):
                return False

            # Copy over original
            dest_key = f"{self.s3.config.S3_WATERMARK_PREFIX}{self.s3.config.WATERMARK_FILE}"
            if not self.s3.copy_object(temp_key, dest_key):
                return False

            # Delete temp
            self.s3.delete_object(temp_key)
            return True
        else:
            # Direct write (non-atomic)
            key = f"{self.s3.config.S3_WATERMARK_PREFIX}{self.s3.config.WATERMARK_FILE}"
            return self.s3.write_object(key, watermark_json, "application/json")

    def update(
        self,
        last_processed_file: str,
        records_processed: int,
    ) -> bool:
        """Update watermark after successful processing.

        Args:
            last_processed_file: Last processed file path.
            records_processed: Total records processed.

        Returns:
            True if successful.
        """
        now = datetime.now(timezone.utc)
        temp_key = f"{self.s3.config.S3_WATERMARK_PREFIX}temp_{uuid.uuid4().hex}"

        return self.write(
            last_successful_run=now,
            last_processed_file=last_processed_file,
            records_processed=records_processed,
            temp_key=temp_key,
        )

    def create_initial(self) -> bool:
        """Create initial watermark for first run.

        Returns:
            True if successful.
        """
        now = datetime.now(timezone.utc)
        key = f"{self.s3.config.S3_WATERMARK_PREFIX}{self.s3.config.WATERMARK_FILE}"

        initial_watermark = {
            "last_successful_run": now.isoformat(),
            "last_processed_file": "",
            "records_processed": 0,
            "watermark_id": str(uuid.uuid4()),
        }

        return self.s3.write_object(
            key,
            json.dumps(initial_watermark, indent=2),
            "application/json",
        )