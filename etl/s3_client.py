"""S3 client for ETL pipeline operations."""

import json
import logging
from datetime import datetime
from typing import Any, Generator, Optional

import boto3
from botocore.exceptions import ClientError

from .config import Config

logger = logging.getLogger(__name__)


class S3Client:
    """S3 operations for ETL pipeline."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize S3 client.

        Args:
            config: ETL configuration. Uses default if not provided.
        """
        self.config = config or Config()
        self.client = boto3.client(
            "s3",
            region_name=self.config.AWS_REGION,
        )
        self.resource = boto3.resource("s3", region_name=self.config.AWS_REGION)

    def list_objects_paginated(
        self,
        prefix: str,
        start_after: Optional[str] = None,
        max_keys: int = 1000,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """List objects with pagination support.

        Args:
            prefix: S3 prefix to list objects under.
            start_after: Key to start listing after.
            max_keys: Maximum keys per page.

        Yields:
            Pages of S3 object summaries.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        operation_inputs = {
            "Bucket": self.config.S3_BUCKET,
            "Prefix": prefix,
            "MaxKeys": max_keys,
        }
        if start_after:
            operation_inputs["StartAfter"] = start_after

        page_iterator = paginator.paginate(**operation_inputs)
        for page in page_iterator:
            if "Contents" in page:
                yield page["Contents"]

    def list_files_since_watermark(
        self,
        watermark: Optional[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """List new/updated files since last watermark.

        Args:
            watermark: Watermark dict containing last_successful_run timestamp.

        Returns:
            List of S3 object summaries for files newer than watermark.
        """
        files = []
        prefix = self.config.raw_prefix

        last_processed_file = watermark.get("last_processed_file") if watermark else None

        for page in self.list_objects_paginated(prefix):
            for obj in page:
                key = obj["Key"]
                last_modified = obj["LastModified"]

                # Skip the watermark file itself
                if self.config.S3_WATERMARK_PREFIX in key:
                    continue

                # If we have a last processed file, skip files up to and including it
                if last_processed_file and key <= last_processed_file:
                    continue

                # Filter by LastModified if watermark exists
                if watermark and watermark.get("last_successful_run"):
                    watermark_time = watermark["last_successful_run"]
                    if isinstance(watermark_time, str):
                        watermark_dt = datetime.fromisoformat(watermark_time.replace("Z", "+00:00"))
                    else:
                        watermark_dt = watermark_time

                    if last_modified <= watermark_dt:
                        continue

                files.append(obj)

        return files

    def read_parquet_bytes(self, key: str) -> bytes:
        """Read Parquet file bytes from S3.

        Args:
            key: S3 object key.

        Returns:
            Parquet file bytes.
        """
        response = self.client.get_object(
            Bucket=self.config.S3_BUCKET,
            Key=key,
        )
        return response["Body"].read()

    def write_object(
        self,
        key: str,
        data: bytes | str,
        content_type: Optional[str] = None,
    ) -> bool:
        """Write data to S3.

        Args:
            key: S3 object key.
            data: Data to write.
            content_type: Optional content type.

        Returns:
            True if successful.
        """
        try:
            kwargs: dict[str, Any] = {
                "Bucket": self.config.S3_BUCKET,
                "Key": key,
                "Body": data,
            }
            if content_type:
                kwargs["ContentType"] = content_type

            self.client.put_object(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to write to S3: {e}")
            return False

    def copy_object(self, source_key: str, dest_key: str) -> bool:
        """Copy object within the same bucket.

        Args:
            source_key: Source object key.
            dest_key: Destination object key.

        Returns:
            True if successful.
        """
        try:
            self.resource.Object(
                self.config.S3_BUCKET,
                dest_key,
            ).copy_from(
                CopySource={"Bucket": self.config.S3_BUCKET, "Key": source_key}
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to copy S3 object: {e}")
            return False

    def delete_object(self, key: str) -> bool:
        """Delete object from S3.

        Args:
            key: S3 object key to delete.

        Returns:
            True if successful.
        """
        try:
            self.client.delete_object(
                Bucket=self.config.S3_BUCKET,
                Key=key,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete S3 object: {e}")
            return False