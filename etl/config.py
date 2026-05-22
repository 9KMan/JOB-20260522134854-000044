"""Configuration settings for S3 Parquet ETL pipeline."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """ETL pipeline configuration."""

    # S3 Settings
    S3_BUCKET: str = os.getenv("S3_BUCKET", "etl-data-lake")
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    S3_RAW_PREFIX: str = "raw/"
    S3_PROCESSED_PREFIX: str = "processed/"
    S3_WATERMARK_PREFIX: str = ".watermark/"
    S3_FAILED_PREFIX: str = ".failed/"

    # Watermark file name
    WATERMARK_FILE: str = "last_run.json"

    # Processing Settings
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100000"))  # 100k rows per batch
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY_SECONDS: int = int(os.getenv("RETRY_DELAY_SECONDS", "5"))

    # PostgreSQL Settings
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "etl_db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "etl_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_TABLE: str = os.getenv("POSTGRES_TABLE", "records")

    # Lambda-style settings
    LAMBDA_TIMEOUT: int = int(os.getenv("LAMBDA_TIMEOUT", "900"))  # 15 minutes
    LAMBDA_MEMORY_MB: int = int(os.getenv("LAMBDA_MEMORY_MB", "2048"))

    # Airflow settings
    DAG_SCHEDULE: str = os.getenv("DAG_SCHEDULE", "0 2 * * *")  # Daily at 2 AM
    DAG_MAX_ACTIVE_RUNS: int = int(os.getenv("DAG_MAX_ACTIVE_RUNS", "1"))
    TASK_CONCURRENCY: int = int(os.getenv("TASK_CONCURRENCY", "4"))

    @property
    def watermark_path(self) -> str:
        """Get the full S3 path for the watermark file."""
        return f"s3://{self.S3_BUCKET}/{self.S3_WATERMARK_PREFIX}{self.WATERMARK_FILE}"

    @property
    def failed_prefix(self) -> str:
        """Get the S3 prefix for failed files."""
        return self.S3_FAILED_PREFIX

    @property
    def processed_prefix(self) -> str:
        """Get the S3 prefix for processed files."""
        return self.S3_PROCESSED_PREFIX

    @property
    def raw_prefix(self) -> str:
        """Get the S3 prefix for raw files."""
        return self.S3_RAW_PREFIX

    def get_postgres_dsn(self) -> dict:
        """Get PostgreSQL connection parameters as a dict."""
        return {
            "host": self.POSTGRES_HOST,
            "port": self.POSTGRES_PORT,
            "database": self.POSTGRES_DB,
            "user": self.POSTGRES_USER,
            "password": self.POSTGRES_PASSWORD,
        }


# Global config instance
config = Config()