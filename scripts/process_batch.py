"""Standalone batch processor script for S3 Parquet ETL."""

import argparse
import logging
import sys
from datetime import datetime, timezone

from etl.config import Config
from etl.database import DatabaseUpserter
from etl.failed_handler import FailedFileHandler
from etl.parquet_processor import ParquetProcessor
from etl.s3_client import S3Client
from etl.watermark import WatermarkStore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def process_files(
    config: Config,
    file_keys: list[str],
    update_watermark: bool = True,
) -> dict:
    """Process a list of S3 Parquet files.

    Args:
        config: ETL configuration.
        file_keys: List of S3 keys to process.
        update_watermark: Whether to update watermark after processing.

    Returns:
        Processing statistics.
    """
    s3_client = S3Client(config)
    processor = ParquetProcessor(config)
    db_upsert = DatabaseUpserter(config)
    failed_handler = FailedFileHandler(s3_client, config)
    watermark_store = WatermarkStore(s3_client)

    stats = {
        "files_processed": 0,
        "files_failed": 0,
        "records_processed": 0,
        "records_failed": 0,
        "start_time": datetime.now(timezone.utc).isoformat(),
        "end_time": None,
    }

    for key in file_keys:
        logger.info(f"Processing {key}")
        try:
            # Read Parquet bytes from S3
            parquet_bytes = s3_client.read_parquet_bytes(key)

            # Process in batches
            batch_count = 0
            for batch_df, batch_stats in processor.process_file_batched(
                parquet_bytes, config.BATCH_SIZE
            ):
                batch_count += 1
                logger.info(f"  Batch {batch_count}: {batch_stats['output_rows']} rows")

                # Upsert to database
                upsert_stats = db_upsert.upsert_batch(batch_df)
                stats["records_processed"] += upsert_stats["rows_upserted"]
                stats["records_failed"] += upsert_stats["rows_failed"]

            stats["files_processed"] += 1
            logger.info(f"Successfully processed {key} in {batch_count} batches")

        except Exception as e:
            stats["files_failed"] += 1
            logger.error(f"Failed to process {key}: {e}")
            failed_handler.handle_processing_failure(
                filename=key,
                error=e,
                record_count=0,
                file_key=key,
            )

    stats["end_time"] = datetime.now(timezone.utc).isoformat()

    # Update watermark if enabled
    if update_watermark and file_keys:
        last_key = file_keys[-1]
        watermark_store.update(last_key, stats["records_processed"])
        logger.info(f"Watermark updated: last_file={last_key}")

    return stats


def main():
    """Main entry point for batch processor."""
    parser = argparse.ArgumentParser(description="Process S3 Parquet files")
    parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="S3 bucket name (overrides config)",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="raw/",
        help="S3 prefix to list files from",
    )
    parser.add_argument(
        "--since-watermark",
        action="store_true",
        help="Process only files since last watermark",
    )
    parser.add_argument(
        "--no-watermark-update",
        action="store_true",
        help="Don't update watermark after processing",
    )
    parser.add_argument(
        "--file-keys",
        type=str,
        nargs="+",
        help="Specific file keys to process",
    )

    args = parser.parse_args()

    # Load configuration
    config = Config()
    if args.bucket:
        config.S3_BUCKET = args.bucket

    s3_client = S3Client(config)

    # Determine files to process
    if args.file_keys:
        file_keys = args.file_keys
    elif args.since_watermark:
        watermark_store = WatermarkStore(s3_client)
        watermark = watermark_store.read()
        files = s3_client.list_files_since_watermark(watermark)
        file_keys = [f["Key"] for f in files]
        logger.info(f"Found {len(file_keys)} files since watermark")
    else:
        # List all files in prefix
        files = []
        for page in s3_client.list_objects_paginated(args.prefix):
            for obj in page:
                if obj["Key"].endswith(".parquet"):
                    files.append(obj)
        file_keys = [f["Key"] for f in files]
        logger.info(f"Found {len(file_keys)} files in {args.prefix}")

    if not file_keys:
        logger.info("No files to process")
        return 0

    # Process files
    logger.info(f"Processing {len(file_keys)} files")
    stats = process_files(
        config=config,
        file_keys=file_keys,
        update_watermark=not args.no_watermark_update,
    )

    # Print summary
    logger.info("=" * 50)
    logger.info("Processing Summary:")
    logger.info(f"  Files processed: {stats['files_processed']}")
    logger.info(f"  Files failed: {stats['files_failed']}")
    logger.info(f"  Records processed: {stats['records_processed']}")
    logger.info(f"  Records failed: {stats['records_failed']}")
    logger.info(f"  Start: {stats['start_time']}")
    logger.info(f"  End: {stats['end_time']}")
    logger.info("=" * 50)

    return 0 if stats["files_failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())