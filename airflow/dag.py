"""Airflow DAG for S3 Parquet ETL pipeline."""

import logging
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago

from etl.config import Config
from etl.database import DatabaseUpserter
from etl.failed_handler import FailedFileHandler
from etl.parquet_processor import ParquetProcessor
from etl.s3_client import S3Client
from etl.watermark import WatermarkStore

logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    "owner": "etl_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def create_etl_dag(
    dag_id: str = "s3_parquet_etl",
    schedule: str = "0 2 * * *",
    bucket: Optional[str] = None,
) -> DAG:
    """Create ETL DAG with tasks.

    Args:
        dag_id: DAG identifier.
        schedule: Cron schedule expression.
        bucket: S3 bucket name.

    Returns:
        Configured DAG instance.
    """
    config = Config()
    if bucket:
        config.S3_BUCKET = bucket

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule=schedule,
        start_date=days_ago(1),
        max_active_runs=config.DAG_MAX_ACTIVE_RUNS,
        catchup=False,
        tags=["etl", "parquet", "s3"],
    ) as dag:

        @task(task_id="list_new_files")
        def list_new_files_task():
            """List new files since last watermark."""
            s3_client = S3Client(config)
            watermark_store = WatermarkStore(s3_client)
            watermark = watermark_store.read()

            files = s3_client.list_files_since_watermark(watermark)
            logger.info(f"Found {len(files)} new files to process")

            # Store files list in XCom for next task
            return [
                {"key": f["Key"], "size": f["Size"], "last_modified": str(f["LastModified"])}
                for f in files
            ]

        @task(task_id="process_files")
        def process_files_task(files: list):
            """Process each file and upsert to database."""
            s3_client = S3Client(config)
            processor = ParquetProcessor(config)
            db_upsert = DatabaseUpserter(config)
            failed_handler = FailedFileHandler(s3_client, config)

            total_stats = {
                "files_processed": 0,
                "files_failed": 0,
                "records_processed": 0,
                "records_failed": 0,
            }

            for file_info in files:
                key = file_info["key"]
                try:
                    # Read Parquet bytes
                    parquet_bytes = s3_client.read_parquet_bytes(key)

                    # Process in batches
                    for batch_df, batch_stats in processor.process_file_batched(
                        parquet_bytes, config.BATCH_SIZE
                    ):
                        # Upsert to database
                        upsert_stats = db_upsert.upsert_batch(batch_df)
                        total_stats["records_processed"] += upsert_stats["rows_upserted"]
                        total_stats["records_failed"] += upsert_stats["rows_failed"]

                    total_stats["files_processed"] += 1
                    logger.info(f"Successfully processed {key}")

                except Exception as e:
                    total_stats["files_failed"] += 1
                    logger.error(f"Failed to process {key}: {e}")
                    failed_handler.handle_processing_failure(
                        filename=key,
                        error=e,
                        record_count=0,
                        file_key=key,
                    )

            return total_stats

        @task(task_id="update_watermark")
        def update_watermark_task(stats: dict, files: list):
            """Update watermark after successful processing."""
            if not files:
                logger.info("No files to update watermark")
                return

            s3_client = S3Client(config)
            watermark_store = WatermarkStore(s3_client)

            last_file = files[-1]["key"] if files else ""
            records = stats.get("records_processed", 0)

            success = watermark_store.update(last_file, records)
            if success:
                logger.info(f"Watermark updated: last_file={last_file}, records={records}")
            else:
                logger.error("Failed to update watermark")

            return success

        @task(task_id="cleanup")
        def cleanup_task():
            """Cleanup old processed files (optional)."""
            logger.info("Cleanup task - no-op for now")
            return True

        # Task dependencies
        files = list_new_files_task()
        stats = process_files_task(files)
        update_watermark_task(stats, files)
        cleanup_task()

        return dag


# Create default DAG instance
etl_dag = create_etl_dag()