"""Error handling for failed ETL files."""

import json
import logging
from datetime import date, datetime, timezone
from typing import Optional

from .config import Config
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class FailedFileHandler:
    """Handles logging and storage of failed processing files."""

    def __init__(self, s3_client: Optional[S3Client] = None, config: Optional[Config] = None):
        """Initialize failed file handler.

        Args:
            s3_client: S3 client instance.
            config: ETL configuration.
        """
        self.config = config or Config()
        self.s3 = s3_client or S3Client(self.config)

    def _get_failed_key(self, filename: str, error_date: Optional[date] = None) -> str:
        """Generate S3 key for failed file log.

        Args:
            filename: Original filename that failed.
            error_date: Date of failure. Uses today if not provided.

        Returns:
            S3 key for failed file log.
        """
        error_date = error_date or date.today()
        date_str = error_date.isoformat()

        # Create safe key from filename
        safe_filename = filename.replace("/", "_").replace("\\", "_")
        if not safe_filename.endswith(".error"):
            safe_filename += ".error"

        return f"{self.config.failed_prefix}{date_str}/{safe_filename}"

    def write_error_log(
        self,
        filename: str,
        error_type: str,
        error_message: str,
        record_count: int,
        additional_data: Optional[dict] = None,
    ) -> bool:
        """Write error log to S3 dead-letter location.

        Args:
            filename: Original filename that failed.
            error_type: Type/category of error.
            error_message: Error message details.
            record_count: Number of records in failed file.
            additional_data: Optional additional context.

        Returns:
            True if successfully written.
        """
        error_log = {
            "error_type": error_type,
            "error_message": error_message,
            "record_count": record_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "original_filename": filename,
        }

        if additional_data:
            error_log["additional_context"] = additional_data

        key = self._get_failed_key(filename)

        try:
            json_data = json.dumps(error_log, indent=2)
            return self.s3.write_object(key, json_data, "application/json")
        except Exception as e:
            logger.error(f"Failed to write error log to S3: {e}")
            return False

    def handle_processing_failure(
        self,
        filename: str,
        error: Exception,
        record_count: int,
        file_key: Optional[str] = None,
    ) -> bool:
        """Handle a processing failure with full context.

        Args:
            filename: Name of file that failed.
            error: Exception that occurred.
            record_count: Number of records in file.
            file_key: Optional S3 key for the file.

        Returns:
            True if error was logged successfully.
        """
        error_type = type(error).__name__
        error_message = str(error)[:1000]  # Truncate long messages

        additional = {
            "file_key": file_key,
            "exception_type": error_type,
        }

        return self.write_error_log(
            filename=filename,
            error_type=error_type,
            error_message=error_message,
            record_count=record_count,
            additional_data=additional,
        )

    def list_failed_files(
        self,
        date_prefix: Optional[str] = None,
    ) -> list[dict]:
        """List failed files from S3.

        Args:
            date_prefix: Optional date string (YYYY-MM-DD) to filter by.

        Returns:
            List of failed file objects.
        """
        prefix = self.config.failed_prefix
        if date_prefix:
            prefix = f"{prefix}{date_prefix}/"

        files = []
        for page in self.s3.list_objects_paginated(prefix):
            files.extend(page)

        return files

    def get_failed_file_content(self, key: str) -> Optional[dict]:
        """Read a failed file error log.

        Args:
            key: S3 key of the error log.

        Returns:
            Parsed error log dict or None.
        """
        try:
            response = self.s3.client.get_object(
                Bucket=self.s3.config.S3_BUCKET,
                Key=key,
            )
            data = response["Body"].read().decode("utf-8")
            return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to read error log: {e}")
            return None


# Import Optional for type hints
from typing import Optional